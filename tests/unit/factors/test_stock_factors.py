"""Stock alpha and auxiliary factors with point-in-time boundaries."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta

import numpy as np
import polars as pl
import pytest

from quant_core.data.adjustments import (
    FORWARD_LOG_RETURN_COLUMN,
    FORWARD_RETURN_INDEX_COLUMN,
    AdjustmentMode,
)
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors import FactorContext, FactorRegistry
from quant_core.factors.base import factor_table_content_hash
from quant_core.factors.builtin import register_etf_factors, register_stock_factors
from quant_core.factors.builtin.auxiliary import (
    AvgAmount20dFactor,
    IndustryCodePitFactor,
    LogMarketCapFactor,
    assert_alpha_eligible,
)
from quant_core.factors.builtin.momentum import Momentum12020Factor
from quant_core.factors.builtin.quality import CfoToNetProfitFactor, RoeAvgPitFactor
from quant_core.factors.builtin.risk import (
    DownsideVolatility60dFactor,
    MaxDrawdown120dFactor,
    Volatility60dFactor,
)
from quant_core.factors.builtin.valuation import BookToPriceFactor, EarningsYieldFactor

_ID = InstrumentId.parse("SSE:600000")
_SNAPSHOT = SnapshotId.parse("00000000-0000-0000-0000-000000000007")
_HASH = "7" * 64


class BarService:
    def __init__(self, bars: pl.DataFrame) -> None:
        self.bars_frame = bars
        self.calls = 0
        self.starts: list[date] = []

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        mode: AdjustmentMode = AdjustmentMode.RAW,
        as_of: date | None = None,
    ) -> pl.LazyFrame:
        self.calls += 1
        self.starts.append(start)
        ids = [item.canonical() for item in instruments]
        result = self.bars_frame.filter(
            pl.col("instrument_id").is_in(ids)
            & pl.col("trade_date").is_between(start, end, closed="both")
        )
        if mode is AdjustmentMode.FORWARD:
            additions: list[pl.Expr] = []
            if "adjustment_factor" not in result.columns:
                additions.append(pl.lit(1.0).alias("adjustment_factor"))
            if FORWARD_RETURN_INDEX_COLUMN not in result.columns:
                additions.append(pl.col("close").alias(FORWARD_RETURN_INDEX_COLUMN))
            if FORWARD_LOG_RETURN_COLUMN not in result.columns:
                additions.append(
                    pl.when(pl.col("preclose").is_null() | (pl.col("preclose") == 0))
                    .then(pl.lit(None, dtype=pl.Float64))
                    .otherwise((pl.col("close") / pl.col("preclose")).log())
                    .cast(pl.Float64)
                    .alias(FORWARD_LOG_RETURN_COLUMN)
                )
            result = result.with_columns(*additions)
        return result.lazy()


class RawOnlyBars:
    """Production-shaped raw repository without adjustment-service arguments."""

    def __init__(self, bars: pl.DataFrame) -> None:
        self.bars_frame = bars

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        return self.bars_frame.filter(
            pl.col("instrument_id").is_in(
                [instrument.canonical() for instrument in instruments]
            )
            & pl.col("trade_date").is_between(start, end, closed="both")
        ).lazy()


class Financials:
    def __init__(
        self, frame: pl.DataFrame, trading_days: Sequence[date] | None = None
    ) -> None:
        self.frame = frame
        self.calls: list[date] = []
        self.trading_days = tuple(trading_days) if trading_days is not None else None

    def trade_calendar(
        self, snapshot_id: SnapshotId, start: date, end: date
    ) -> pl.LazyFrame:
        days = self.trading_days or tuple(
            start + timedelta(days=index)
            for index in range((end - start).days + 1)
            if (start + timedelta(days=index)).weekday() < 5
        )
        return pl.DataFrame(
            {
                "trade_date": [day for day in days if start <= day <= end],
                "is_trading_day": [True for day in days if start <= day <= end],
            },
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()

    def financials_as_of(
        self,
        snapshot_id: SnapshotId,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        self.calls.append(as_of)
        ids = (
            None if instruments is None else [item.canonical() for item in instruments]
        )
        result = self.frame.filter(
            pl.col("metric").is_in(field_ids)
            & (
                pl.col("available_at")
                <= datetime.combine(as_of, datetime.max.time(), UTC)
            )
        )
        if ids is not None:
            result = result.filter(pl.col("instrument_id").is_in(ids))
        return result.lazy()


class PitValues:
    def __init__(self, frame: pl.DataFrame) -> None:
        self.frame = frame

    def values_as_of(
        self, snapshot_id: SnapshotId, as_of: date, instruments: Sequence[InstrumentId]
    ) -> pl.LazyFrame:
        return self.frame.filter(
            (pl.col("signal_date") == as_of)
            & pl.col("instrument_id").is_in([item.canonical() for item in instruments])
        ).lazy()


class RevisionFinancials(Financials):
    """Test repository that exposes only the latest known revision per PIT key."""

    def financials_as_of(
        self,
        snapshot_id: SnapshotId,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        self.calls.append(as_of)
        ids = (
            None if instruments is None else [item.canonical() for item in instruments]
        )
        cutoff = datetime.combine(as_of, datetime.max.time(), UTC)
        result = self.frame.filter(
            pl.col("metric").is_in(field_ids)
            & pl.col("available_at").is_not_null()
            & (pl.col("available_at") <= cutoff)
        )
        if ids is not None:
            result = result.filter(pl.col("instrument_id").is_in(ids))
        return (
            result.sort("available_at")
            .unique(subset=["instrument_id", "report_period", "metric"], keep="last")
            .lazy()
        )


def test_valuation_factors_use_positive_finite_snapshot_bar_multiples() -> None:
    bars = _bars([10.0], pe=[5.0], pb=[2.0])
    ctx = _ctx(bars["trade_date"].item(), bars["trade_date"].item())
    assert EarningsYieldFactor(BarService(bars), [_ID]).compute(ctx).collect()[
        "value"
    ].item() == pytest.approx(0.2)
    assert BookToPriceFactor(BarService(bars), [_ID]).compute(ctx).collect()[
        "value"
    ].item() == pytest.approx(0.5)
    invalid = _bars([10.0, 10.0], pe=[0.0, -1.0], pb=[float("inf"), None])
    result = (
        EarningsYieldFactor(BarService(invalid), [_ID])
        .compute(_ctx(invalid["trade_date"][0], invalid["trade_date"][-1]))
        .collect()
    )
    assert result["value"].to_list() == [None, None]
    assert result["is_valid"].to_list() == [False, False]


def test_valuation_with_unknown_availability_is_invalid_without_fabricated_time() -> (
    None
):
    bars = _bars([10.0], pe=[5.0]).with_columns(
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("available_at")
    )
    day = bars["trade_date"].item()

    result = (
        EarningsYieldFactor(BarService(bars), [_ID]).compute(_ctx(day, day)).collect()
    )

    assert result["value"].item() is None
    assert result["available_at"].item() is None
    assert result["is_valid"].item() is False


def test_financial_factors_query_each_signal_date_and_match_report_period() -> None:
    days = [date(2024, 4, 29), date(2024, 4, 30)]
    frame = pl.DataFrame(
        {
            "instrument_id": [_ID.canonical()] * 5,
            "report_period": [date(2023, 12, 31)] * 3 + [date(2024, 3, 31)] * 2,
            "metric": [
                "roe_avg",
                "operating_cash_flow",
                "net_profit",
                "operating_cash_flow",
                "net_profit",
            ],
            "value": [0.10, 30.0, 10.0, 100.0, 0.0],
            "available_at": [datetime(2024, 4, 29, 8, tzinfo=UTC)] * 3
            + [datetime(2024, 4, 30, 8, tzinfo=UTC)] * 2,
        }
    )
    provider = Financials(frame)
    roe = RoeAvgPitFactor(provider, [_ID]).compute(_ctx(days[0], days[1])).collect()
    assert provider.calls == days
    assert roe["value"].to_list() == [0.10, 0.10]
    provider.calls.clear()
    ratio = (
        CfoToNetProfitFactor(provider, [_ID]).compute(_ctx(days[0], days[1])).collect()
    )
    assert ratio["value"].to_list() == [3.0, None]
    assert provider.calls == days


def test_cfo_ratio_uses_latest_common_report_period_from_shuffled_rows() -> None:
    signal_date = date(2024, 5, 1)
    frame = pl.DataFrame(
        {
            "instrument_id": [_ID.canonical()] * 3,
            "report_period": [
                date(2024, 3, 31),
                date(2023, 12, 31),
                date(2023, 12, 31),
            ],
            "metric": ["operating_cash_flow", "net_profit", "operating_cash_flow"],
            "value": [100.0, 10.0, 30.0],
            "available_at": [
                datetime(2024, 4, 30, 8, tzinfo=UTC),
                datetime(2024, 4, 29, 9, tzinfo=UTC),
                datetime(2024, 4, 29, 8, tzinfo=UTC),
            ],
        }
    )

    result = (
        CfoToNetProfitFactor(Financials(frame), [_ID])
        .compute(_ctx(signal_date, signal_date))
        .collect()
    )

    assert result["value"].item() == pytest.approx(3.0)
    assert result["available_at"].item() == datetime(2024, 4, 29, 9, tzinfo=UTC)


def test_financial_factors_reject_duplicate_metric_report_keys() -> None:
    signal_date = date(2024, 5, 1)
    frame = pl.DataFrame(
        {
            "instrument_id": [_ID.canonical()] * 2,
            "report_period": [date(2023, 12, 31)] * 2,
            "metric": ["roe_avg"] * 2,
            "value": [0.10, 0.11],
            "available_at": [datetime(2024, 4, 29, 8, tzinfo=UTC)] * 2,
        }
    )

    with pytest.raises(ValueError, match="duplicate financial metric key"):
        RoeAvgPitFactor(Financials(frame), [_ID]).compute(
            _ctx(signal_date, signal_date)
        )


def test_financial_factor_queries_each_signal_date_without_future_or_unknown_revision() -> (
    None
):
    first, second = date(2024, 4, 29), date(2024, 4, 30)
    frame = pl.DataFrame(
        {
            "instrument_id": [_ID.canonical()] * 3,
            "report_period": [date(2023, 12, 31)] * 3,
            "metric": ["roe_avg"] * 3,
            "value": [0.10, 0.20, 9.99],
            "available_at": [
                datetime(2024, 4, 29, 8, tzinfo=UTC),
                datetime(2024, 4, 30, 8, tzinfo=UTC),
                None,
            ],
        },
        schema={
            "instrument_id": pl.String,
            "report_period": pl.Date,
            "metric": pl.String,
            "value": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
        },
    )
    provider = RevisionFinancials(frame)

    result = RoeAvgPitFactor(provider, [_ID]).compute(_ctx(first, second)).collect()

    assert provider.calls == [first, second]
    assert result["value"].to_list() == pytest.approx([0.10, 0.20])
    assert result["available_at"].to_list() == [
        datetime(2024, 4, 29, 8, tzinfo=UTC),
        datetime(2024, 4, 30, 8, tzinfo=UTC),
    ]


def test_pit_quality_uses_only_explicit_open_sessions() -> None:
    friday, monday = date(2024, 4, 26), date(2024, 4, 29)
    provider = Financials(_empty_financials(), [friday, monday])

    result = RoeAvgPitFactor(provider, [_ID]).compute(_ctx(friday, monday)).collect()

    assert provider.calls == [friday, monday]
    assert result["trade_date"].to_list() == [friday, monday]


def test_cfo_ratio_requires_finite_positive_threshold() -> None:
    provider = Financials(
        pl.DataFrame(
            {
                "instrument_id": [],
                "report_period": [],
                "metric": [],
                "value": [],
                "available_at": [],
            },
            schema={
                "instrument_id": pl.String,
                "report_period": pl.Date,
                "metric": pl.String,
                "value": pl.Float64,
                "available_at": pl.Datetime("us", "UTC"),
            },
        )
    )
    for bad in (0.0, -1.0, float("inf"), True):
        with pytest.raises(ValueError, match="finite positive"):
            CfoToNetProfitFactor(provider, [_ID], min_abs_net_profit=bad)  # type: ignore[arg-type]


def test_market_formulas_and_exact_history_boundaries() -> None:
    prices = (
        100.0 * np.exp(np.r_[0.0, np.cumsum(np.linspace(-0.02, 0.03, 120))])
    ).tolist()
    bars = _bars(prices)
    day = bars["trade_date"][-1]
    ctx = _ctx(day, day)
    momentum = Momentum12020Factor(BarService(bars), [_ID]).compute(ctx).collect()
    assert momentum["value"].item() == pytest.approx(prices[-21] / prices[-121] - 1.0)
    downside = (
        DownsideVolatility60dFactor(BarService(bars), [_ID]).compute(ctx).collect()
    )
    returns = np.diff(np.log(np.asarray(prices[-61:])))
    assert downside["value"].item() == pytest.approx(
        np.sqrt(np.mean(np.minimum(returns, 0.0) ** 2)) * np.sqrt(252.0)
    )
    drawdown = MaxDrawdown120dFactor(BarService(bars), [_ID]).compute(ctx).collect()
    window = np.asarray(prices[-120:])
    assert drawdown["value"].item() == pytest.approx(
        np.max(1.0 - window / np.maximum.accumulate(window))
    )
    short = _bars(prices[:-1])
    short_day = short["trade_date"][-1]
    assert (
        Momentum12020Factor(BarService(short), [_ID])
        .compute(_ctx(short_day, short_day))
        .collect()["is_valid"]
        .item()
        is False
    )


def test_downside_volatility_fails_closed_when_finite_returns_overflow_squares() -> (
    None
):
    """Finite returns and path must not let an unrepresentable RMS escape."""
    returns = [1e308, -1e308, *([0.0] * 58)]
    bars = _bars([100.0] * 61).with_columns(
        pl.Series(
            FORWARD_LOG_RETURN_COLUMN,
            [None, *returns],
            dtype=pl.Float64,
        )
    )
    signal_day = bars["trade_date"][-1]

    result = (
        DownsideVolatility60dFactor(BarService(bars), [_ID])
        .compute(_ctx(signal_day, signal_day))
        .collect()
    )

    assert result["value"].item() is None
    assert result["is_valid"].item() is False


def test_flat_downside_volatility_is_zero_and_valid() -> None:
    bars = _bars([100.0] * 61).with_columns(
        pl.Series(
            FORWARD_LOG_RETURN_COLUMN,
            [None, *([0.0] * 60)],
            dtype=pl.Float64,
        )
    )
    signal_day = bars["trade_date"][-1]

    result = (
        DownsideVolatility60dFactor(BarService(bars), [_ID])
        .compute(_ctx(signal_day, signal_day))
        .collect()
    )

    assert result["value"].item() == 0.0
    assert result["is_valid"].item() is True


def test_downside_volatility_preserves_finite_near_zero_result() -> None:
    tiny = -1e-300
    bars = _bars([100.0] * 61).with_columns(
        pl.Series(
            FORWARD_LOG_RETURN_COLUMN,
            [None, tiny, *([0.0] * 59)],
            dtype=pl.Float64,
        )
    )
    signal_day = bars["trade_date"][-1]

    result = (
        DownsideVolatility60dFactor(BarService(bars), [_ID])
        .compute(_ctx(signal_day, signal_day))
        .collect()
    )
    expected = abs(tiny) / np.sqrt(60.0) * np.sqrt(252.0)

    assert result["value"].item() == pytest.approx(expected, rel=1e-12, abs=0.0)
    assert result["is_valid"].item() is True


def test_nonfinite_downside_log_return_invalidates_without_evaluator_error() -> None:
    bars = _bars([100.0] * 61).with_columns(
        pl.Series(
            FORWARD_LOG_RETURN_COLUMN,
            [None, float("inf"), *([0.0] * 59)],
            dtype=pl.Float64,
        )
    )
    signal_day = bars["trade_date"][-1]

    result = (
        DownsideVolatility60dFactor(BarService(bars), [_ID])
        .compute(_ctx(signal_day, signal_day))
        .collect()
    )

    assert result["value"].item() is None
    assert result["is_valid"].item() is False


@pytest.mark.parametrize(
    "make_factor",
    [
        lambda service: Momentum12020Factor(service, [_ID]),
        lambda service: Volatility60dFactor(service, [_ID]),
        lambda service: DownsideVolatility60dFactor(service, [_ID]),
        lambda service: MaxDrawdown120dFactor(service, [_ID]),
    ],
)
def test_stock_market_factors_are_byte_stable_when_return_index_changes(
    make_factor: object,
) -> None:
    """Stock market factors must use row returns, not cumulative index levels."""
    log_returns = np.linspace(-0.02, 0.03, 120).tolist()
    bars = _bars([100.0] * 121).with_columns(
        pl.lit(1.0, dtype=pl.Float64).alias("adjustment_factor"),
        pl.Series(
            FORWARD_LOG_RETURN_COLUMN,
            [None, *log_returns],
            dtype=pl.Float64,
        ),
        pl.Series(
            FORWARD_RETURN_INDEX_COLUMN,
            [100.0 + index for index in range(121)],
            dtype=pl.Float64,
        ),
    )
    changed_index = bars.with_columns(
        pl.Series(
            FORWARD_RETURN_INDEX_COLUMN,
            [1e-8 * 1.01**index for index in range(121)],
            dtype=pl.Float64,
        )
    )
    signal_day = bars["trade_date"][-1]

    baseline = (
        make_factor(BarService(bars)).compute(_ctx(signal_day, signal_day)).collect()
    )  # type: ignore[operator]
    changed = (
        make_factor(BarService(changed_index))
        .compute(_ctx(signal_day, signal_day))
        .collect()
    )  # type: ignore[operator]

    assert changed["value"].item() == baseline["value"].item()
    assert changed["available_at"].item() == baseline["available_at"].item()
    assert factor_table_content_hash(changed.to_arrow()) == factor_table_content_hash(
        baseline.to_arrow()
    )


def test_auxiliary_factors_use_raw_close_pit_providers_and_cannot_form_alpha() -> None:
    bars = _bars([10.0] * 20, amounts=[float(i) for i in range(1, 21)])
    day = bars["trade_date"][-1]
    ctx = _ctx(day, day)
    avg = AvgAmount20dFactor(BarService(bars), [_ID]).compute(ctx).collect()
    assert avg["value"].item() == pytest.approx(10.5)
    values = pl.DataFrame(
        {
            "signal_date": [day],
            "instrument_id": [_ID.canonical()],
            "value": [1_000.0],
            "available_at": [datetime(2024, 1, 1, tzinfo=UTC)],
        }
    )
    market_cap = (
        LogMarketCapFactor(
            BarService(bars),
            [_ID],
            PitValues(values),
            calendar_provider=Financials(_empty_financials(), [day]),
        )
        .compute(ctx)
        .collect()
    )
    assert market_cap["value"].item() == pytest.approx(np.log(10_000.0))
    missing = (
        LogMarketCapFactor(
            BarService(bars),
            [_ID],
            calendar_provider=Financials(_empty_financials(), [day]),
        )
        .compute(ctx)
        .collect()
    )
    assert missing["value"].item() is None and missing["is_valid"].item() is False
    industry = (
        IndustryCodePitFactor(
            [_ID],
            PitValues(values),
            calendar_provider=Financials(_empty_financials(), [day]),
        )
        .compute(ctx)
        .collect()
    )
    assert industry["value"].item() == 1_000.0
    with pytest.raises(ValueError, match="auxiliary"):
        assert_alpha_eligible(
            [
                avg_factor.spec
                for avg_factor in [AvgAmount20dFactor(BarService(bars), [_ID])]
            ]
        )


def test_avg_amount_unknown_availability_invalidates_the_window() -> None:
    bars = _bars([10.0] * 20, amounts=[float(index) for index in range(1, 21)])
    bars = bars.with_columns(
        pl.when(pl.col("trade_date") == bars["trade_date"][-1])
        .then(pl.lit(None, dtype=pl.Datetime("us", "UTC")))
        .otherwise(pl.col("available_at"))
        .alias("available_at")
    )
    day = bars["trade_date"][-1]

    result = (
        AvgAmount20dFactor(BarService(bars), [_ID]).compute(_ctx(day, day)).collect()
    )

    assert result["value"].item() is None
    assert result["available_at"].item() is None
    assert result["is_valid"].item() is False


def test_avg_amount_falls_back_to_full_history_across_long_suspensions() -> None:
    days = [date(2023, 1, 1) + timedelta(days=index * 10) for index in range(20)]
    bars = _bars(
        [10.0] * 20, amounts=[float(index) for index in range(1, 21)]
    ).with_columns(
        pl.Series("trade_date", days, dtype=pl.Date),
        pl.Series(
            "available_at",
            [datetime.combine(day, datetime.min.time(), UTC) for day in days],
            dtype=pl.Datetime("us", "UTC"),
        ),
    )
    service = BarService(bars)

    result = (
        AvgAmount20dFactor(service, [_ID]).compute(_ctx(days[-1], days[-1])).collect()
    )

    assert result["value"].item() == pytest.approx(10.5)
    assert service.starts == [days[-1] - timedelta(days=60), date.min]


def test_log_market_cap_rejects_duplicate_raw_bar_keys() -> None:
    bars = pl.concat([_bars([10.0]), _bars([11.0])])
    day = bars["trade_date"].item(0)
    values = pl.DataFrame(
        {
            "signal_date": [day],
            "instrument_id": [_ID.canonical()],
            "value": [1_000.0],
            "available_at": [datetime(2024, 1, 1, tzinfo=UTC)],
        }
    )

    with pytest.raises(ValueError, match="duplicate raw market-cap bar key"):
        LogMarketCapFactor(
            BarService(bars),
            [_ID],
            PitValues(values),
            calendar_provider=Financials(_empty_financials(), [day]),
        ).compute(_ctx(day, day))


def test_log_market_cap_uses_only_explicit_open_sessions() -> None:
    friday, monday = date(2024, 4, 26), date(2024, 4, 29)
    bars = _bars([10.0] * 4).with_columns(
        pl.Series(
            "trade_date",
            [friday + timedelta(days=index) for index in range(4)],
            dtype=pl.Date,
        )
    )
    values = pl.DataFrame(
        {
            "signal_date": [friday + timedelta(days=index) for index in range(4)],
            "instrument_id": [_ID.canonical()] * 4,
            "value": [1_000.0] * 4,
            "available_at": [datetime(2024, 4, 1, tzinfo=UTC)] * 4,
        }
    )

    result = (
        LogMarketCapFactor(
            BarService(bars),
            [_ID],
            PitValues(values),
            calendar_provider=Financials(_empty_financials(), [friday, monday]),
        )
        .compute(_ctx(friday, monday))
        .collect()
    )

    assert result["trade_date"].to_list() == [friday, monday]


def test_industry_codes_accept_finite_zero_and_negative_taxonomy_values() -> None:
    days = [date(2024, 4, 26), date(2024, 4, 29)]
    values = pl.DataFrame(
        {
            "signal_date": days,
            "instrument_id": [_ID.canonical()] * 2,
            "value": [0.0, -2.0],
            "available_at": [datetime(2024, 4, 1, tzinfo=UTC)] * 2,
        }
    )

    result = (
        IndustryCodePitFactor(
            [_ID],
            PitValues(values),
            calendar_provider=Financials(_empty_financials(), days),
        )
        .compute(_ctx(days[0], days[1]))
        .collect()
    )

    assert result["value"].to_list() == [0.0, -2.0]
    assert result["is_valid"].to_list() == [True, True]


def test_stock_registration_requires_explicit_adjusted_price_service() -> None:
    bars = _bars([10.0] * 121)

    with pytest.raises(TypeError, match="price_service"):
        register_stock_factors(
            FactorRegistry(), RawOnlyBars(bars), Financials(_empty_financials()), [_ID]
        )


def test_all_stock_factors_register_once_with_alpha_metadata() -> None:
    bars = _bars([10.0] * 121)
    registry = FactorRegistry()
    register_stock_factors(
        registry,
        BarService(bars),
        Financials(_empty_financials()),
        [_ID],
        price_service=BarService(bars),
    )
    expected = {
        "earnings_yield_ttm_v1",
        "book_to_price_mrq_v1",
        "roe_avg_pit_v1",
        "cfo_to_np_pit_v1",
        "momentum_120_20_v1",
        "volatility_60d_v1",
        "downside_volatility_60d_v1",
        "max_drawdown_120d_v1",
        "avg_amount_20d_v1",
        "log_market_cap_v1",
        "industry_code_pit_v1",
    }
    for factor_id in expected:
        spec = registry.spec(factor_id)
        if factor_id in {
            "avg_amount_20d_v1",
            "log_market_cap_v1",
            "industry_code_pit_v1",
        }:
            assert spec.parameters["role"] == "auxiliary"
            assert spec.parameters["eligible_for_alpha"] is False
        else:
            assert spec.parameters.get("eligible_for_alpha", True) is True
            if factor_id in {
                "momentum_120_20_v1",
                "volatility_60d_v1",
                "downside_volatility_60d_v1",
                "max_drawdown_120d_v1",
            }:
                assert spec.parameters["adjustment_mode"] == "FORWARD"
                assert (
                    spec.parameters["price_basis"] == "baostock_forward_log_return_v1"
                )
                assert spec.parameters["price_field"] == FORWARD_LOG_RETURN_COLUMN
                assert (
                    spec.parameters["path_construction"] == "window_forward_cumsum_v1"
                )


@pytest.mark.parametrize("etf_first", [True, False])
def test_shared_market_factor_registration_is_idempotent_for_equivalent_runtime(
    etf_first: bool,
) -> None:
    bars = _bars([10.0] * 121)
    registry = FactorRegistry()
    service = BarService(bars)
    financials = Financials(_empty_financials())
    etf = lambda: register_etf_factors(registry, service, [_ID])
    stock = lambda: register_stock_factors(
        registry,
        service,
        financials,
        [_ID],
        price_service=service,
    )

    (etf if etf_first else stock)()
    (stock if etf_first else etf)()

    assert registry.code_hash("volatility_60d_v1@2.0.0") == registry.code_hash(
        "volatility_60d_v1"
    )


@pytest.mark.parametrize("etf_first", [True, False])
def test_shared_market_factor_registration_rejects_different_service_same_domain(
    etf_first: bool,
) -> None:
    stock_bars = _bars([100.0 + float(index % 5) for index in range(121)])
    etf_bars = _bars([10.0] * 121)
    stock_service = BarService(stock_bars)
    etf_service = BarService(etf_bars)
    financials = Financials(_empty_financials())
    registry = FactorRegistry()
    etf = lambda: register_etf_factors(registry, etf_service, [_ID])
    stock = lambda: register_stock_factors(
        registry,
        stock_service,
        financials,
        [_ID],
        price_service=stock_service,
    )

    (etf if etf_first else stock)()
    with pytest.raises(ValueError, match="conflicting built-in runtime dependencies"):
        (stock if etf_first else etf)()

    day = stock_bars["trade_date"][-1]
    result = registry.factor("volatility_60d_v1").compute(_ctx(day, day)).collect()
    assert result["instrument_id"].to_list() == [_ID.canonical()]
    if etf_first:
        assert result["value"].item() == 0.0
    else:
        assert result["value"].item() > 0.0


@pytest.mark.parametrize("etf_first", [True, False])
def test_shared_market_factor_registration_rejects_different_domain_same_service(
    etf_first: bool,
) -> None:
    etf_id = InstrumentId.parse("SSE:510300")
    stock_bars = _bars([10.0] * 121)
    etf_bars = _bars([100.0 + float(index % 5) for index in range(121)]).with_columns(
        pl.lit(etf_id.canonical()).alias("instrument_id")
    )
    shared_service = BarService(pl.concat([stock_bars, etf_bars]))
    financials = Financials(_empty_financials())
    registry = FactorRegistry()
    etf = lambda: register_etf_factors(registry, shared_service, [etf_id])
    stock = lambda: register_stock_factors(
        registry,
        shared_service,
        financials,
        [_ID],
        price_service=shared_service,
    )

    (etf if etf_first else stock)()
    with pytest.raises(ValueError, match="conflicting built-in runtime dependencies"):
        (stock if etf_first else etf)()

    day = stock_bars["trade_date"][-1]
    result = registry.factor("volatility_60d_v1").compute(_ctx(day, day)).collect()
    expected_instrument = etf_id.canonical() if etf_first else _ID.canonical()
    assert result["instrument_id"].to_list() == [expected_instrument]
    if etf_first:
        assert result["value"].item() > 0.0
    else:
        assert result["value"].item() == 0.0


def _ctx(start: date, end: date) -> FactorContext:
    return FactorContext(_SNAPSHOT, _HASH, start, end)


def _bars(
    closes: Sequence[float],
    *,
    pe: Sequence[float | None] | None = None,
    pb: Sequence[float | None] | None = None,
    amounts: Sequence[float | None] | None = None,
) -> pl.DataFrame:
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(len(closes))]
    n = len(closes)
    return pl.DataFrame(
        {
            "instrument_id": [_ID.canonical()] * n,
            "trade_date": days,
            "close": closes,
            "preclose": list(closes[:1]) + list(closes[:-1]),
            "amount": list(amounts or [100.0] * n),
            "pe_ttm": list(pe or [10.0] * n),
            "pb_mrq": list(pb or [2.0] * n),
            "available_at": [
                datetime.combine(day, datetime.min.time(), UTC) for day in days
            ],
        },
        schema={
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "close": pl.Float64,
            "preclose": pl.Float64,
            "amount": pl.Float64,
            "pe_ttm": pl.Float64,
            "pb_mrq": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
        },
    )


def _empty_financials() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "instrument_id": pl.String,
            "report_period": pl.Date,
            "metric": pl.String,
            "value": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
        }
    )
