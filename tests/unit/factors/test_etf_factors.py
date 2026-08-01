"""ETF market factors over point-in-time row log-return histories."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from inspect import getsource
from math import sqrt

import numpy as np
import polars as pl
import pytest

from quant_core.data.adjustments import (
    ADJUSTMENT_EVENT_COMPONENTS_DTYPE,
    FORWARD_LOG_RETURN_COLUMN,
    FORWARD_RETURN_INDEX_COLUMN,
    AdjustmentMode,
)
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors import (
    FACTOR_OUTPUT_SCHEMA,
    FactorContext,
    FactorRegistry,
)
from quant_core.factors.base import factor_table_content_hash
from quant_core.factors.builtin import register_etf_factors
from quant_core.factors.builtin.momentum import (
    ReturnFactor,
    Trend120dFactor,
    _MarketFactor,
)
from quant_core.factors.builtin.risk import Volatility60dFactor

_SSE = InstrumentId.parse("SSE:510300")
_SZSE = InstrumentId.parse("SZSE:159919")
_SNAPSHOT = SnapshotId.parse("00000000-0000-0000-0000-000000000006")
_UNIVERSE_HASH = "6" * 64


def test_market_factor_execution_does_not_materialize_row_dictionaries() -> None:
    """Long histories must not allocate a Python mapping for every market row."""
    assert "to_dicts" not in getsource(_MarketFactor.compute)


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
        result = self._bars.filter(
            pl.col("instrument_id").is_in(instrument_ids)
            & pl.col("trade_date").is_between(start, end, closed="both")
        )
        if "adjustment_factor" not in result.columns:
            result = result.with_columns(
                pl.lit(1.0, dtype=pl.Float64).alias("adjustment_factor"),
                pl.lit(1.0, dtype=pl.Float64).alias("adjustment_event_factor"),
                pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias(
                    "adjustment_event_available_at"
                ),
                pl.lit([], dtype=ADJUSTMENT_EVENT_COMPONENTS_DTYPE).alias(
                    "adjustment_event_components"
                ),
            )
        if FORWARD_RETURN_INDEX_COLUMN not in result.columns:
            result = result.with_columns(
                pl.col("close").alias(FORWARD_RETURN_INDEX_COLUMN)
            )
        if FORWARD_LOG_RETURN_COLUMN not in result.columns:
            result = result.with_columns(
                pl.when(pl.col("preclose").is_null() | (pl.col("preclose") == 0))
                .then(pl.lit(None, dtype=pl.Float64))
                .otherwise(pl.col("close").log() - pl.col("preclose").log())
                .cast(pl.Float64)
                .alias(FORWARD_LOG_RETURN_COLUMN)
            )
        return result.lazy()


class ActionAwarePriceService:
    """Apply only actions whose ex-date and availability are known by ``as_of``."""

    def __init__(
        self,
        raw_bars: pl.DataFrame,
        actions: Sequence[tuple[date, float, datetime]],
    ) -> None:
        self._raw_bars = raw_bars
        self._actions = actions

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        mode: AdjustmentMode,
        as_of: date,
    ) -> pl.LazyFrame:
        assert mode is AdjustmentMode.FORWARD
        instrument_ids = [instrument.canonical() for instrument in instruments]
        applicable = [
            action
            for action in self._actions
            if action[0] <= as_of and action[2].date() <= as_of
        ]
        rows: list[dict[str, object]] = []
        return_indices: dict[str, float] = {}
        for row in (
            self._raw_bars.filter(
                pl.col("instrument_id").is_in(instrument_ids)
                & pl.col("trade_date").is_between(start, end, closed="both")
            )
            .sort("instrument_id", "trade_date")
            .to_dicts()
        ):
            trade_date = row["trade_date"]
            instrument_id = row["instrument_id"]
            assert isinstance(trade_date, date)
            assert isinstance(instrument_id, str)
            adjustment_factor = np.prod(
                [factor for ex_date, factor, _ in applicable if trade_date < ex_date],
                dtype=np.float64,
            ).item()
            events = [action for action in applicable if action[0] == trade_date]
            raw_close = float(row["close"])
            raw_preclose = float(row["preclose"])
            previous_index = return_indices.get(instrument_id)
            return_index = (
                raw_close
                if previous_index is None
                else previous_index * raw_close / float(row["preclose"])
            )
            return_indices[instrument_id] = return_index
            row[FORWARD_RETURN_INDEX_COLUMN] = return_index
            row[FORWARD_LOG_RETURN_COLUMN] = float(
                np.log(raw_close) - np.log(raw_preclose)
            )
            row["close"] = raw_close * adjustment_factor
            row["preclose"] = raw_preclose * adjustment_factor
            row["adjustment_factor"] = adjustment_factor
            row["adjustment_event_factor"] = (
                np.prod([event[1] for event in events], dtype=np.float64).item()
                if events
                else 1.0
            )
            row["adjustment_event_available_at"] = (
                max(event[2] for event in events) if events else None
            )
            row["adjustment_event_components"] = [
                {
                    "action_type": "synthetic",
                    "cash_per_share": 0.0,
                    "share_ratio": 1.0 / event[1] - 1.0,
                    "available_at": event[2],
                }
                for event in events
            ]
            rows.append(row)
        schema = {
            **self._raw_bars.schema,
            "adjustment_factor": pl.Float64,
            "adjustment_event_factor": pl.Float64,
            "adjustment_event_available_at": pl.Datetime("us", "UTC"),
            "adjustment_event_components": ADJUSTMENT_EVENT_COMPONENTS_DTYPE,
            FORWARD_RETURN_INDEX_COLUMN: pl.Float64,
            FORWARD_LOG_RETURN_COLUMN: pl.Float64,
        }
        return pl.DataFrame(rows, schema=schema).lazy()


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


def test_trend_uses_scale_invariant_log_price_ols_slope() -> None:
    """Normalizing by the log-price mean would make the trend level-dependent."""
    closes = np.exp(2.0 + 0.004 * np.arange(120) + 0.03 * np.sin(np.arange(120)))
    bars = _bars(_SSE, closes.tolist())
    ctx = _context(bars["trade_date"][-1], bars["trade_date"][-1])

    result = Trend120dFactor(RecordingPriceService(bars), [_SSE]).compute(ctx).collect()

    x = np.arange(120, dtype=np.float64)
    y = np.log(closes)
    expected_slope = np.linalg.lstsq(np.column_stack((np.ones(120), x)), y, rcond=None)[
        0
    ][1]
    assert result["value"].item() == pytest.approx(expected_slope)
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


def test_volatility_fails_closed_when_finite_returns_overflow_second_moment() -> None:
    """Finite returns and path must not let an unrepresentable variance escape."""
    returns = [1e308, -1e308, *([0.0] * 58)]
    bars = _bars(_SSE, [100.0] * 61).with_columns(
        pl.Series(
            FORWARD_LOG_RETURN_COLUMN,
            [None, *returns],
            dtype=pl.Float64,
        )
    )
    signal_day = bars["trade_date"][-1]

    result = (
        Volatility60dFactor(RecordingPriceService(bars), [_SSE])
        .compute(_context(signal_day, signal_day))
        .collect()
    )

    assert result["value"].item() is None
    assert result["is_valid"].item() is False


def test_volatility_preserves_finite_near_zero_second_moment() -> None:
    """Squaring tiny returns directly must not underflow a representable result to zero."""
    tiny = 1e-300
    returns = [tiny, -tiny] * 30
    bars = _bars(_SSE, [100.0] * 61).with_columns(
        pl.Series(
            FORWARD_LOG_RETURN_COLUMN,
            [None, *returns],
            dtype=pl.Float64,
        )
    )
    signal_day = bars["trade_date"][-1]

    result = (
        Volatility60dFactor(RecordingPriceService(bars), [_SSE])
        .compute(_context(signal_day, signal_day))
        .collect()
    )
    expected = tiny * sqrt(60.0 / 59.0) * sqrt(252.0)

    assert result["value"].item() == pytest.approx(expected, rel=1e-12, abs=0.0)
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


@pytest.mark.parametrize("bad_return", [None, float("nan"), float("inf")])
def test_malformed_forward_log_return_invalidates_signal(
    bad_return: float | None,
) -> None:
    bars = _bars(_SSE, [10.0 + index for index in range(21)]).with_columns(
        pl.Series(
            FORWARD_LOG_RETURN_COLUMN,
            [0.0] * 10 + [bad_return] + [0.0] * 10,
            dtype=pl.Float64,
        )
    )
    day = bars["trade_date"][-1]

    result = (
        ReturnFactor(RecordingPriceService(bars), [_SSE], 20)
        .compute(_context(day, day))
        .collect()
    )

    assert result["value"].item() is None
    assert result["is_valid"].item() is False


@pytest.mark.parametrize("first_return", [None, float("nan"), float("inf")])
def test_first_window_log_return_is_ignored(first_return: float | None) -> None:
    """The first row's return precedes the factor window and must not invalidate it."""
    log_return = float(np.log(1.01))
    bars = _bars(_SSE, [100.0 * 1.01**index for index in range(21)]).with_columns(
        pl.Series(
            FORWARD_LOG_RETURN_COLUMN,
            [first_return, *([log_return] * 20)],
            dtype=pl.Float64,
        )
    )
    signal_day = bars["trade_date"][-1]

    result = (
        ReturnFactor(RecordingPriceService(bars), [_SSE], 20)
        .compute(_context(signal_day, signal_day))
        .collect()
    )

    assert result["value"].item() == pytest.approx(1.01**20 - 1.0)
    assert result["is_valid"].item() is True


def test_market_factor_ignores_unstable_return_index_values() -> None:
    """Reintroducing cumulative-index consumption must fail this result contract."""
    log_return = float(np.log(1.01))
    bars = _bars(_SSE, [100.0] * 21).with_columns(
        pl.Series(
            FORWARD_LOG_RETURN_COLUMN,
            [None, *([log_return] * 20)],
            dtype=pl.Float64,
        ),
        pl.Series(
            FORWARD_RETURN_INDEX_COLUMN,
            [100.0, *([100.0] * 20)],
            dtype=pl.Float64,
        ),
    )
    signal_day = bars["trade_date"][-1]

    result = (
        ReturnFactor(RecordingPriceService(bars), [_SSE], 20)
        .compute(_context(signal_day, signal_day))
        .collect()
    )

    assert result["value"].item() == pytest.approx(1.01**20 - 1.0)
    assert result["is_valid"].item() is True


def test_constant_trend_is_exact_zero_and_valid() -> None:
    """A flat valid price window is a zero trend, not missing data."""
    closes = [1.0] * 120
    bars = _bars(_SSE, closes)
    ctx = _context(bars["trade_date"][-1], bars["trade_date"][-1])

    result = Trend120dFactor(RecordingPriceService(bars), [_SSE]).compute(ctx).collect()

    assert result["value"].item() == 0.0
    assert result["is_valid"].item() is True


def test_near_flat_trend_remains_finite_and_valid() -> None:
    """A representable near-zero slope must not be rejected as a zero denominator."""
    closes = [1.0 + index * 1e-15 for index in range(120)]
    bars = _bars(_SSE, closes)
    signal_day = bars["trade_date"][-1]

    result = (
        Trend120dFactor(RecordingPriceService(bars), [_SSE])
        .compute(_context(signal_day, signal_day))
        .collect()
    )

    assert result["value"].item() is not None
    assert np.isfinite(result["value"].item())
    assert result["is_valid"].item() is True


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


def test_factor_requests_forward_adjusted_history_and_clips_output_scope() -> None:
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
    assert mode is AdjustmentMode.FORWARD
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
            "preclose": [119.0],
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


def test_signal_uses_context_end_as_forward_anchor() -> None:
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
    assert baseline_service.calls[0][5] == signal_day


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


def test_unknown_window_availability_invalidates_signal() -> None:
    bars = _bars(_SSE, [100.0 + index for index in range(21)]).with_columns(
        pl.when(pl.col("trade_date") == date(2024, 1, 8))
        .then(pl.lit(None, dtype=pl.Datetime("us", "UTC")))
        .otherwise(pl.col("available_at"))
        .alias("available_at")
    )
    day = bars["trade_date"][-1]

    result = (
        ReturnFactor(RecordingPriceService(bars), [_SSE], 20)
        .compute(_context(day, day))
        .collect()
    )

    assert result["available_at"].item() is None
    assert result["value"].item() is None
    assert result["is_valid"].item() is False


@pytest.mark.parametrize(
    ("make_factor", "factor_id"),
    [
        (lambda service: ReturnFactor(service, [_SSE], 20), "return_20d_v1"),
        (lambda service: ReturnFactor(service, [_SSE], 60), "return_60d_v1"),
        (lambda service: ReturnFactor(service, [_SSE], 120), "return_120d_v1"),
        (lambda service: Trend120dFactor(service, [_SSE]), "trend_120d_v1"),
        (lambda service: Volatility60dFactor(service, [_SSE]), "volatility_60d_v1"),
    ],
)
def test_row_log_returns_keep_all_etf_market_factors_stable_after_future_jump(
    make_factor: object, factor_id: str
) -> None:
    """Every market factor must consume request-stable row log returns."""
    closes = np.exp(2.0 + 0.003 * np.arange(125)).tolist()
    bars = _bars(_SSE, closes)
    signal_day = bars["trade_date"][119]
    extended_end = bars["trade_date"][124]

    service = ActionAwarePriceService(
        bars,
        [(bars["trade_date"][120], 0.5, datetime(2024, 4, 30, tzinfo=UTC))],
    )
    short = make_factor(service).compute(_context(signal_day, signal_day)).collect()  # type: ignore[operator]
    extended = (
        make_factor(service).compute(_context(signal_day, extended_end)).collect()
    )  # type: ignore[operator]
    early = extended.filter(pl.col("trade_date") == signal_day)

    assert short["factor_id"].item() == factor_id
    assert early["value"].item() == short["value"].item()
    assert early["available_at"].item() == short["available_at"].item()
    assert factor_table_content_hash(early.to_arrow()) == factor_table_content_hash(
        short.to_arrow()
    )


def test_trend_rejects_the_old_global_forward_price_formula() -> None:
    """Using globally adjusted price levels would publish the wrong trend."""
    closes = np.exp(2.0 + 0.003 * np.arange(125)).tolist()
    bars = _bars(_SSE, closes)
    signal_day = bars["trade_date"][119]
    extended_end = bars["trade_date"][124]
    service = ActionAwarePriceService(
        bars,
        [(bars["trade_date"][120], 0.5, datetime(2024, 4, 30, tzinfo=UTC))],
    )

    actual = (
        Trend120dFactor(service, [_SSE])
        .compute(_context(signal_day, extended_end))
        .collect()
        .filter(pl.col("trade_date") == signal_day)["value"]
        .item()
    )
    x = np.arange(120, dtype=np.float64)
    global_log_closes = np.log(np.asarray(closes[:120]) * 0.5)
    global_slope = np.linalg.lstsq(
        np.column_stack((np.ones(120), x)), global_log_closes, rcond=None
    )[0][1]
    global_value = global_slope / np.mean(global_log_closes)

    assert actual != pytest.approx(global_value)


@pytest.mark.parametrize(
    "make_factor",
    [
        lambda service: ReturnFactor(service, [_SSE], 20),
        lambda service: ReturnFactor(service, [_SSE], 60),
        lambda service: ReturnFactor(service, [_SSE], 120),
        lambda service: Trend120dFactor(service, [_SSE]),
        lambda service: Volatility60dFactor(service, [_SSE]),
    ],
)
def test_row_log_returns_have_exact_stable_values_for_nonbinary_future_scale(
    make_factor: object,
) -> None:
    """A 0.7 forward factor must not perturb bytes of earlier factor observations."""
    bars = _bars(_SSE, [100.0 + index for index in range(122)])
    signal_day = bars["trade_date"][120]
    service = ActionAwarePriceService(
        bars,
        [(bars["trade_date"][121], 0.7, datetime(2024, 5, 1, tzinfo=UTC))],
    )

    short = make_factor(service).compute(_context(signal_day, signal_day)).collect()  # type: ignore[operator]
    extended = (
        make_factor(service)
        .compute(_context(signal_day, bars["trade_date"][121]))
        .collect()
    )  # type: ignore[operator]
    early = extended.filter(pl.col("trade_date") == signal_day)

    assert early["value"].item() == short["value"].item()
    assert early["available_at"].item() == short["available_at"].item()
    assert factor_table_content_hash(early.to_arrow()) == factor_table_content_hash(
        short.to_arrow()
    )


@pytest.mark.parametrize(
    "make_factor",
    [
        lambda service: ReturnFactor(service, [_SSE], 20),
        lambda service: ReturnFactor(service, [_SSE], 60),
        lambda service: ReturnFactor(service, [_SSE], 120),
        lambda service: Trend120dFactor(service, [_SSE]),
        lambda service: Volatility60dFactor(service, [_SSE]),
    ],
)
def test_etf_market_factor_is_byte_stable_across_request_anchors(
    make_factor: object,
) -> None:
    """A 0.7 boundary before the window must not leak request-anchor rounding."""
    closes = [100.0 * 1.001**index for index in range(700)]
    bars = _bars(_SSE, closes)
    jump_index = 250
    bars = bars.with_columns(
        pl.when(pl.int_range(pl.len()) == jump_index)
        .then(pl.col("preclose") * 0.7)
        .otherwise(pl.col("preclose"))
        .alias("preclose")
    )
    signal_day = bars["trade_date"][-1]
    wider_start = bars["trade_date"][500]
    service = ActionAwarePriceService(bars, [])

    narrow = make_factor(service).compute(_context(signal_day, signal_day)).collect()  # type: ignore[operator]
    wide = (
        make_factor(service)
        .compute(_context(wider_start, signal_day))
        .collect()
        .filter(pl.col("trade_date") == signal_day)
    )  # type: ignore[operator]

    assert wide["value"].item() == narrow["value"].item()
    assert wide["available_at"].item() == narrow["available_at"].item()
    assert factor_table_content_hash(wide.to_arrow()) == factor_table_content_hash(
        narrow.to_arrow()
    )


@pytest.mark.parametrize(
    "make_factor",
    [
        lambda service, scope: ReturnFactor(service, scope, 20),
        lambda service, scope: ReturnFactor(service, scope, 60),
        lambda service, scope: ReturnFactor(service, scope, 120),
        lambda service, scope: Trend120dFactor(service, scope),
        lambda service, scope: Volatility60dFactor(service, scope),
    ],
)
def test_row_log_returns_are_byte_stable_for_wide_multi_instrument_histories(
    make_factor: object,
) -> None:
    first = _bars(_SSE, [1e-8 * 1.01**index for index in range(122)])
    second = _bars(_SZSE, [1e8 * 0.999**index for index in range(122)])
    bars = pl.concat([first, second])
    signal_day = first["trade_date"][120]
    future_day = first["trade_date"][121]
    service = ActionAwarePriceService(
        bars,
        [(future_day, 0.7, datetime(2024, 5, 1, tzinfo=UTC))],
    )
    scope = [_SSE, _SZSE]

    short = (
        make_factor(service, scope).compute(_context(signal_day, signal_day)).collect()
    )  # type: ignore[operator]
    extended = (
        make_factor(service, scope)
        .compute(_context(signal_day, future_day))
        .collect()
        .filter(pl.col("trade_date") == signal_day)
    )  # type: ignore[operator]

    assert extended.rows() == short.rows()
    assert factor_table_content_hash(extended.to_arrow()) == factor_table_content_hash(
        short.to_arrow()
    )


def test_nonfinite_row_log_return_invalidates_volatility() -> None:
    """A provider boundary that exposes an infinite row return must fail closed."""
    bars = _bars(_SSE, [100.0] * 61).with_columns(
        pl.Series(
            FORWARD_LOG_RETURN_COLUMN,
            [0.0] * 60 + [float("inf")],
            dtype=pl.Float64,
        )
    )
    signal_day = bars["trade_date"][-1]

    result = (
        Volatility60dFactor(RecordingPriceService(bars), [_SSE])
        .compute(_context(signal_day, signal_day))
        .collect()
    )

    assert result["value"].item() is None
    assert result["is_valid"].item() is False


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
    assert result["factor_version"].unique().to_list() == ["2.1.0"]
    assert result["available_at"].null_count() == 0


def test_builtin_registration_exposes_exact_specs_and_stable_code_hashes() -> None:
    """Omitting a factor or registering wrong metadata breaks engine resolution."""
    bars = _bars(_SSE, [10.0])
    registry = FactorRegistry()

    register_etf_factors(registry, RecordingPriceService(bars), [_SSE])

    expected = {
        "return_20d_v1@2.1.0": (20, 1),
        "return_60d_v1@2.1.0": (60, 1),
        "return_120d_v1@2.1.0": (120, 1),
        "trend_120d_v1@2.1.0": (120, 1),
        "volatility_60d_v1@2.1.0": (60, -1),
    }
    for reference, (lookback, direction) in expected.items():
        spec = registry.spec(reference)
        assert spec.frequency == "daily"
        assert spec.lookback_sessions == lookback
        assert spec.direction == direction
        assert spec.dependencies == ()
        assert spec.parameters["adjustment_mode"] == "FORWARD"
        assert spec.parameters["price_basis"] == "baostock_forward_log_return_v2"
        assert spec.parameters["price_field"] == FORWARD_LOG_RETURN_COLUMN
        assert (
            spec.parameters["log_return_formula"] == "log_close_minus_log_preclose_v2"
        )
        assert spec.parameters["path_construction"] == "window_forward_cumsum_v1"
        assert len(registry.code_hash(reference)) == 64


def test_builtin_registration_rejects_same_ref_with_a_different_hash() -> None:
    bars = _bars(_SSE, [10.0])
    registry = FactorRegistry()
    existing = Volatility60dFactor(RecordingPriceService(bars), [_SSE])
    registry.register(existing, code_hash="0" * 64)

    with pytest.raises(ValueError, match="conflicting built-in implementation"):
        register_etf_factors(registry, RecordingPriceService(bars), [_SSE])


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
    precloses = list(closes[:1]) + list(closes[:-1])
    return pl.DataFrame(
        {
            "instrument_id": [instrument.canonical()] * len(closes),
            "trade_date": days,
            "close": closes,
            "preclose": precloses,
            "available_at": available,
        },
        schema={
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "close": pl.Float64,
            "preclose": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
        },
    )
