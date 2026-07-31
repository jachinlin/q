"""Stock alpha and auxiliary factors with point-in-time boundaries."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta

import numpy as np
import polars as pl
import pytest

from quant_core.data.adjustments import (
    ADJUSTMENT_EVENT_COMPONENTS_DTYPE,
    AdjustmentMode,
)
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors import FactorContext, FactorRegistry
from quant_core.factors.builtin import register_stock_factors
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
)
from quant_core.factors.builtin.valuation import BookToPriceFactor, EarningsYieldFactor

_ID = InstrumentId.parse("SSE:600000")
_SNAPSHOT = SnapshotId.parse("00000000-0000-0000-0000-000000000007")
_HASH = "7" * 64


class BarService:
    def __init__(self, bars: pl.DataFrame) -> None:
        self.bars_frame = bars
        self.calls = 0

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
        ids = [item.canonical() for item in instruments]
        result = self.bars_frame.filter(
            pl.col("instrument_id").is_in(ids)
            & pl.col("trade_date").is_between(start, end, closed="both")
        )
        if (
            mode is AdjustmentMode.BACKWARD
            and "adjustment_factor" not in result.columns
        ):
            result = result.with_columns(
                pl.lit(1.0).alias("adjustment_factor"),
                pl.lit(1.0).alias("adjustment_event_factor"),
                pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias(
                    "adjustment_event_available_at"
                ),
                pl.lit([], dtype=ADJUSTMENT_EVENT_COMPONENTS_DTYPE).alias(
                    "adjustment_event_components"
                ),
            )
        return result.lazy()


class Financials:
    def __init__(self, frame: pl.DataFrame) -> None:
        self.frame = frame
        self.calls: list[date] = []

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
        LogMarketCapFactor(BarService(bars), [_ID], PitValues(values))
        .compute(ctx)
        .collect()
    )
    assert market_cap["value"].item() == pytest.approx(np.log(10_000.0))
    missing = LogMarketCapFactor(BarService(bars), [_ID]).compute(ctx).collect()
    assert missing["value"].item() is None and missing["is_valid"].item() is False
    industry = IndustryCodePitFactor([_ID], PitValues(values)).compute(ctx).collect()
    assert industry["value"].item() == 1_000.0
    with pytest.raises(ValueError, match="auxiliary"):
        assert_alpha_eligible(
            [
                avg_factor.spec
                for avg_factor in [AvgAmount20dFactor(BarService(bars), [_ID])]
            ]
        )


def test_all_stock_factors_register_once_with_alpha_metadata() -> None:
    bars = _bars([10.0] * 121)
    registry = FactorRegistry()
    register_stock_factors(
        registry, BarService(bars), Financials(_empty_financials()), [_ID]
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
