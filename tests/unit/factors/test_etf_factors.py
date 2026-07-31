"""ETF market factors over point-in-time backward-adjusted close histories."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from math import sqrt

import numpy as np
import polars as pl
import pytest

from quant_core.data.adjustments import AdjustmentMode
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors import FACTOR_OUTPUT_SCHEMA, FactorContext, FactorRegistry
from quant_core.factors.builtin import register_etf_factors
from quant_core.factors.builtin.momentum import ReturnFactor, Trend120dFactor
from quant_core.factors.builtin.risk import Volatility60dFactor

_SSE = InstrumentId.parse("SSE:510300")
_SZSE = InstrumentId.parse("SZSE:159919")
_SNAPSHOT = SnapshotId.parse("00000000-0000-0000-0000-000000000006")
_UNIVERSE_HASH = "6" * 64


class RecordingPriceService:
    """Small deterministic PriceAdjustmentService-compatible boundary fake."""

    def __init__(self, bars: pl.DataFrame) -> None:
        self._bars = bars
        self.calls: list[
            tuple[
                SnapshotId,
                tuple[InstrumentId, ...],
                date,
                date,
                AdjustmentMode,
                date,
            ]
        ] = []

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        mode: AdjustmentMode,
        as_of: date,
    ) -> pl.LazyFrame:
        self.calls.append((snapshot_id, tuple(instruments), start, end, mode, as_of))
        instrument_ids = [instrument.canonical() for instrument in instruments]
        return self._bars.filter(
            pl.col("instrument_id").is_in(instrument_ids)
            & pl.col("trade_date").is_between(start, end, closed="both")
        ).lazy()


def test_return_factors_use_exact_lagged_close_formula() -> None:
    """Using n observations instead of n lags would produce an off-by-one return."""
    closes = [100.0 + float(index) for index in range(121)]
    bars = _bars(_SSE, closes)
    ctx = _context(bars["trade_date"][-1], bars["trade_date"][-1])

    for window in (20, 60, 120):
        result = (
            ReturnFactor(RecordingPriceService(bars), [_SSE], window)
            .compute(ctx)
            .collect()
        )

        assert result["value"].item() == pytest.approx(
            closes[-1] / closes[-1 - window] - 1.0
        )
        assert result["is_valid"].item() is True


def test_trend_uses_log_price_ols_slope_normalized_by_mean() -> None:
    """Replacing the selected formula with P/MA or raw slope must change this result."""
    closes = np.exp(2.0 + 0.004 * np.arange(120) + 0.03 * np.sin(np.arange(120)))
    bars = _bars(_SSE, closes.tolist())
    ctx = _context(bars["trade_date"][-1], bars["trade_date"][-1])

    result = Trend120dFactor(RecordingPriceService(bars), [_SSE]).compute(ctx).collect()

    x = np.arange(120, dtype=np.float64)
    y = np.log(closes)
    expected_slope = np.linalg.lstsq(np.column_stack((np.ones(120), x)), y, rcond=None)[
        0
    ][1]
    assert result["value"].item() == pytest.approx(expected_slope / np.mean(y))
    assert result["is_valid"].item() is True


def test_volatility_uses_60_log_returns_sample_std_annualized() -> None:
    """Using price levels, population std, or only 60 prices must fail this reference."""
    log_returns = np.linspace(-0.012, 0.019, 60)
    closes = (40.0 * np.exp(np.r_[0.0, np.cumsum(log_returns)])).tolist()
    bars = _bars(_SSE, closes)
    ctx = _context(bars["trade_date"][-1], bars["trade_date"][-1])

    result = (
        Volatility60dFactor(RecordingPriceService(bars), [_SSE]).compute(ctx).collect()
    )

    expected = np.std(log_returns, ddof=1) * sqrt(252.0)
    assert result["value"].item() == pytest.approx(expected)
    assert result["is_valid"].item() is True


def test_constant_prices_have_zero_valid_volatility() -> None:
    """Treating a genuine zero-risk window as missing would discard valid data."""
    bars = _bars(_SSE, [25.0] * 61)
    ctx = _context(bars["trade_date"][-1], bars["trade_date"][-1])

    result = (
        Volatility60dFactor(RecordingPriceService(bars), [_SSE]).compute(ctx).collect()
    )

    assert result["value"].item() == pytest.approx(0.0)
    assert result["is_valid"].item() is True


@pytest.mark.parametrize(
    ("factor", "required"),
    [
        (lambda service: ReturnFactor(service, [_SSE], 20), 21),
        (lambda service: ReturnFactor(service, [_SSE], 60), 61),
        (lambda service: ReturnFactor(service, [_SSE], 120), 121),
        (lambda service: Trend120dFactor(service, [_SSE]), 120),
        (lambda service: Volatility60dFactor(service, [_SSE]), 61),
    ],
)
def test_one_observation_short_is_invalid_not_zero(
    factor: object, required: int
) -> None:
    """Lowering any minimum-history boundary would fabricate an early signal."""
    bars = _bars(_SSE, [10.0 + index for index in range(required - 1)])
    ctx = _context(bars["trade_date"][-1], bars["trade_date"][-1])
    service = RecordingPriceService(bars)

    result = factor(service).compute(ctx).collect()  # type: ignore[operator]

    assert result["value"].item() is None
    assert result["is_valid"].item() is False


@pytest.mark.parametrize(
    ("factor", "count", "bad_index"),
    [
        (lambda service: ReturnFactor(service, [_SSE], 20), 21, 10),
        (lambda service: Trend120dFactor(service, [_SSE]), 120, 60),
        (lambda service: Volatility60dFactor(service, [_SSE]), 61, 30),
    ],
)
@pytest.mark.parametrize("bad_close", [None, 0.0, -1.0, float("nan"), float("inf")])
def test_invalid_price_anywhere_in_window_invalidates_signal(
    factor: object, count: int, bad_index: int, bad_close: float | None
) -> None:
    """Skipping a bad interior price would silently change observed-session semantics."""
    closes: list[float | None] = [10.0 + index for index in range(count)]
    closes[bad_index] = bad_close
    bars = _bars(_SSE, closes)
    ctx = _context(bars["trade_date"][-1], bars["trade_date"][-1])

    result = factor(RecordingPriceService(bars)).compute(ctx).collect()  # type: ignore[operator]

    assert result["value"].item() is None
    assert result["is_valid"].item() is False


def test_trend_zero_log_mean_is_invalid() -> None:
    """Dividing a zero normalization mean must not publish infinity or NaN."""
    closes = np.exp(np.linspace(-1.0, 1.0, 120)).tolist()
    bars = _bars(_SSE, closes)
    ctx = _context(bars["trade_date"][-1], bars["trade_date"][-1])

    result = Trend120dFactor(RecordingPriceService(bars), [_SSE]).compute(ctx).collect()

    assert result["value"].item() is None
    assert result["is_valid"].item() is False


def test_windows_are_isolated_by_instrument_and_input_order_is_normalized() -> None:
    """A global shift over interleaved securities would mix one ETF into another."""
    first = _bars(_SSE, [100.0 + index for index in range(21)])
    second = _bars(_SZSE, [200.0 - 2.0 * index for index in range(21)])
    shuffled = pl.concat([first, second]).sample(fraction=1.0, shuffle=True, seed=6)
    service = RecordingPriceService(shuffled)
    last_day = first["trade_date"][-1]

    result = (
        ReturnFactor(service, [_SZSE, _SSE], 20)
        .compute(_context(last_day, last_day))
        .collect()
    )

    assert result.select("instrument_id", "value").rows() == [
        (_SSE.canonical(), pytest.approx(120.0 / 100.0 - 1.0)),
        (_SZSE.canonical(), pytest.approx(160.0 / 200.0 - 1.0)),
    ]


def test_factor_requests_backward_adjusted_history_and_clips_output_scope() -> None:
    """Starting at ctx.start or returning warm-up rows would violate window scope."""
    bars = _bars(_SSE, [50.0 + index for index in range(25)])
    start = bars["trade_date"][-2]
    end = bars["trade_date"][-1]
    service = RecordingPriceService(bars)

    result = ReturnFactor(service, [_SSE], 20).compute(_context(start, end)).collect()

    assert result["trade_date"].to_list() == [start, end]
    snapshot, instruments, history_start, requested_end, mode, as_of = service.calls[0]
    assert snapshot == _SNAPSHOT
    assert instruments == (_SSE,)
    assert history_start < start
    assert requested_end == end
    assert mode is AdjustmentMode.BACKWARD
    assert as_of == end


def test_long_trading_gap_falls_back_to_older_observed_sessions() -> None:
    """A calendar-day heuristic alone would lose valid pre-suspension observations."""
    old = _bars(_SSE, [100.0 + index for index in range(20)])
    current_day = date(2026, 1, 5)
    current = pl.DataFrame(
        {
            "instrument_id": [_SSE.canonical()],
            "trade_date": [current_day],
            "close": [150.0],
            "available_at": [datetime(2026, 1, 5, 8, tzinfo=UTC)],
        },
        schema=old.schema,
    )
    service = RecordingPriceService(pl.concat([old, current]))

    result = (
        ReturnFactor(service, [_SSE], 20)
        .compute(_context(current_day, current_day))
        .collect()
    )

    assert result["value"].item() == pytest.approx(150.0 / 100.0 - 1.0)
    assert result["is_valid"].item() is True
    assert len(service.calls) == 2
    assert service.calls[-1][2] == date.min


def test_signal_never_reads_or_changes_from_prices_after_context_end() -> None:
    """Including a future close would introduce look-ahead into a completed signal."""
    bars = _bars(_SSE, [100.0 + index for index in range(22)])
    signal_day = bars["trade_date"][-2]
    baseline_service = RecordingPriceService(bars)
    mutated = bars.with_columns(
        pl.when(pl.col("trade_date") > signal_day)
        .then(pl.lit(1_000_000.0))
        .otherwise(pl.col("close"))
        .alias("close")
    )

    baseline = (
        ReturnFactor(baseline_service, [_SSE], 20)
        .compute(_context(signal_day, signal_day))
        .collect()
    )
    changed_future = (
        ReturnFactor(RecordingPriceService(mutated), [_SSE], 20)
        .compute(_context(signal_day, signal_day))
        .collect()
    )

    assert baseline["value"].item() == changed_future["value"].item()
    assert baseline_service.calls[0][3] == signal_day


def test_available_at_is_latest_required_input_availability() -> None:
    """Using only the signal row timestamp can predate a delayed window constituent."""
    bars = _bars(_SSE, [100.0 + index for index in range(21)])
    delayed = datetime(2024, 5, 1, 12, tzinfo=UTC)
    bars = bars.with_columns(
        pl.when(pl.int_range(pl.len()) == 7)
        .then(pl.lit(delayed))
        .otherwise(pl.col("available_at"))
        .alias("available_at")
    )
    signal_day = bars["trade_date"][-1]

    result = (
        ReturnFactor(RecordingPriceService(bars), [_SSE], 20)
        .compute(_context(signal_day, signal_day))
        .collect()
    )

    assert result["available_at"].item() == delayed


def test_duplicate_bar_key_is_rejected() -> None:
    """Silently selecting one duplicate makes a factor depend on input row order."""
    bars = _bars(_SSE, [100.0 + index for index in range(21)])
    bars = pl.concat([bars, bars.slice(10, 1)])
    signal_day = bars["trade_date"].max()

    with pytest.raises(ValueError, match="duplicate adjusted bar key"):
        ReturnFactor(RecordingPriceService(bars), [_SSE], 20).compute(
            _context(signal_day, signal_day)
        )


def test_empty_input_returns_exact_factor_schema() -> None:
    """A schema-less empty result cannot be materialized by FeatureCache."""
    empty_bars = _bars(_SSE, []).clear()
    day = date(2024, 1, 31)

    result = (
        ReturnFactor(RecordingPriceService(empty_bars), [_SSE], 20)
        .compute(_context(day, day))
        .collect()
    )

    assert result.is_empty()
    assert result.schema == FACTOR_OUTPUT_SCHEMA


def test_output_schema_sorting_identity_and_availability_are_exact() -> None:
    """Wrong dtypes, identity columns, or sort keys make cache content unstable."""
    first = _bars(_SSE, [100.0 + index for index in range(22)])
    second = _bars(_SZSE, [200.0 + index for index in range(22)])
    bars = pl.concat([second.reverse(), first.reverse()])
    start = first["trade_date"][-2]
    end = first["trade_date"][-1]

    result = (
        ReturnFactor(RecordingPriceService(bars), [_SZSE, _SSE], 20)
        .compute(_context(start, end))
        .collect()
    )

    assert result.schema == FACTOR_OUTPUT_SCHEMA
    assert result.select("trade_date", "instrument_id").rows() == sorted(
        result.select("trade_date", "instrument_id").rows()
    )
    assert result["factor_id"].unique().to_list() == ["return_20d_v1"]
    assert result["factor_version"].unique().to_list() == ["1.0.0"]
    assert result["available_at"].null_count() == 0


def test_builtin_registration_exposes_exact_specs_and_stable_code_hashes() -> None:
    """Omitting a factor or registering wrong metadata breaks engine resolution."""
    bars = _bars(_SSE, [10.0])
    registry = FactorRegistry()

    register_etf_factors(registry, RecordingPriceService(bars), [_SSE])

    expected = {
        "return_20d_v1@1.0.0": (20, 1),
        "return_60d_v1@1.0.0": (60, 1),
        "return_120d_v1@1.0.0": (120, 1),
        "trend_120d_v1@1.0.0": (120, 1),
        "volatility_60d_v1@1.0.0": (60, -1),
    }
    for reference, (lookback, direction) in expected.items():
        spec = registry.spec(reference)
        assert spec.frequency == "daily"
        assert spec.lookback_sessions == lookback
        assert spec.direction == direction
        assert spec.dependencies == ()
        assert spec.parameters["adjustment_mode"] == "BACKWARD"
        assert len(registry.code_hash(reference)) == 64


@pytest.mark.parametrize("window", [0, -1, 21, 59, 61, 119, 121, True])
def test_return_factor_rejects_unsupported_windows(window: int) -> None:
    """Ad-hoc windows would publish unregistered identities and unclear metadata."""
    service = RecordingPriceService(_bars(_SSE, [10.0]))

    with pytest.raises(ValueError, match="window must be one of 20, 60, 120"):
        ReturnFactor(service, [_SSE], window)


def _context(start: date, end: date) -> FactorContext:
    return FactorContext(
        snapshot_id=_SNAPSHOT,
        universe_hash=_UNIVERSE_HASH,
        start=start,
        end=end,
    )


def _bars(instrument: InstrumentId, closes: Sequence[float | None]) -> pl.DataFrame:
    start = date(2024, 1, 1)
    days = [start + timedelta(days=index) for index in range(len(closes))]
    available = [
        datetime.combine(day, datetime.min.time(), tzinfo=UTC) + timedelta(hours=8)
        for day in days
    ]
    return pl.DataFrame(
        {
            "instrument_id": [instrument.canonical()] * len(closes),
            "trade_date": days,
            "close": closes,
            "available_at": available,
        },
        schema={
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "close": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
        },
    )
