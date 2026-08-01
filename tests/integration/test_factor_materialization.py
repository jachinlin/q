"""End-to-end immutable materialization of every stock MVP factor."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from quant_core.data.adjustments import (
    FORWARD_LOG_RETURN_COLUMN,
    FORWARD_RETURN_INDEX_COLUMN,
    AdjustmentMode,
)
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors import FactorContext, FactorEngine, FactorRegistry, FeatureCache
from quant_core.factors.builtin import register_etf_factors, register_stock_factors

_ID = InstrumentId.parse("SSE:600000")


class CountingBars:
    def __init__(self) -> None:
        self.calls = 0
        days = [date(2024, 1, 1) + timedelta(days=index) for index in range(121)]
        self.frame = pl.DataFrame(
            {
                "instrument_id": [_ID.canonical()] * 121,
                "trade_date": days,
                "close": [100.0 + index for index in range(121)],
                "preclose": [100.0] + [100.0 + index for index in range(120)],
                "amount": [1_000_000.0] * 121,
                "pe_ttm": [10.0] * 121,
                "pb_mrq": [2.0] * 121,
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
        result = self.frame.filter(
            pl.col("trade_date").is_between(start, end, closed="both")
        )
        if mode is AdjustmentMode.FORWARD:
            result = result.with_columns(
                pl.lit(1.0).alias("adjustment_factor"),
                pl.col("close").alias(FORWARD_RETURN_INDEX_COLUMN),
                pl.when(pl.col("preclose").is_null() | (pl.col("preclose") == 0))
                .then(pl.lit(None, dtype=pl.Float64))
                .otherwise(pl.col("close").log() - pl.col("preclose").log())
                .cast(pl.Float64)
                .alias(FORWARD_LOG_RETURN_COLUMN),
            )
        return result.lazy()


class CountingFinancials:
    def __init__(self) -> None:
        self.calls = 0

    def financials_as_of(
        self,
        snapshot_id: SnapshotId,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        self.calls += 1
        return (
            pl.DataFrame(
                {
                    "instrument_id": [_ID.canonical()] * 3,
                    "report_period": [date(2023, 12, 31)] * 3,
                    "metric": ["roe_avg", "operating_cash_flow", "net_profit"],
                    "value": [0.12, 30.0, 10.0],
                    "available_at": [datetime(2024, 4, 30, tzinfo=UTC)] * 3,
                }
            )
            .filter(pl.col("metric").is_in(field_ids))
            .lazy()
        )

    def trade_calendar(
        self, snapshot_id: SnapshotId, start: date, end: date
    ) -> pl.LazyFrame:
        return pl.DataFrame(
            {"trade_date": [end], "is_trading_day": [True]},
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()


def test_second_identical_materialization_hits_cache_without_provider_or_rewrite(
    tmp_path: Path,
) -> None:
    bars, financials = CountingBars(), CountingFinancials()
    registry = FactorRegistry()
    register_stock_factors(registry, bars, financials, [_ID], price_service=bars)
    cache = FeatureCache(tmp_path / "features")
    engine = FactorEngine(registry, cache)
    day = bars.frame["trade_date"][-1]
    ctx = FactorContext(
        SnapshotId.parse("00000000-0000-0000-0000-000000000077"), "a" * 64, day, day
    )
    requested = (
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
    )

    first = engine.compute(requested, ctx)
    calls = (bars.calls, financials.calls)
    state = _cache_state(cache.root)
    second = engine.compute(requested, ctx)

    assert set(first) == {
        "earnings_yield_ttm_v1@1.0.0",
        "book_to_price_mrq_v1@1.0.0",
        "roe_avg_pit_v1@1.0.0",
        "cfo_to_np_pit_v1@1.0.0",
        "momentum_120_20_v1@2.1.0",
        "volatility_60d_v1@2.1.0",
        "downside_volatility_60d_v1@2.1.0",
        "max_drawdown_120d_v1@2.1.0",
        "avg_amount_20d_v1@1.0.0",
        "log_market_cap_v1@1.0.0",
        "industry_code_pit_v1@1.0.0",
    }
    assert {key: item.content_hash for key, item in second.items()} == {
        key: item.content_hash for key, item in first.items()
    }
    assert (bars.calls, financials.calls) == calls
    assert _cache_state(cache.root) == state

    changed_universe = FactorContext(ctx.snapshot_id, "b" * 64, day, day)
    third = engine.compute(requested, changed_universe)

    assert bars.calls > calls[0]
    assert financials.calls > calls[1]
    assert all(third[key].cache_key != first[key].cache_key for key in first)


def test_etf_forward_factors_materialize_once_and_record_forward_price_contract(
    tmp_path: Path,
) -> None:
    """Changing a market factor back to BACKWARD must miss this cache contract."""
    bars = CountingBars()
    registry = FactorRegistry()
    register_etf_factors(registry, bars, [_ID])
    cache = FeatureCache(tmp_path / "features")
    engine = FactorEngine(registry, cache)
    day = bars.frame["trade_date"][-1]
    ctx = FactorContext(
        SnapshotId.parse("00000000-0000-0000-0000-000000000078"), "c" * 64, day, day
    )
    requested = (
        "return_20d_v1",
        "return_60d_v1",
        "return_120d_v1",
        "trend_120d_v1",
        "volatility_60d_v1",
    )

    first = engine.compute(requested, ctx)
    cache_state = _cache_state(cache.root)
    calls = bars.calls
    second = engine.compute(requested, ctx)

    assert set(first) == {
        "return_20d_v1@2.1.0",
        "return_60d_v1@2.1.0",
        "return_120d_v1@2.1.0",
        "trend_120d_v1@2.1.0",
        "volatility_60d_v1@2.1.0",
    }
    assert bars.calls == calls
    assert _cache_state(cache.root) == cache_state
    assert {key: artifact.content_hash for key, artifact in second.items()} == {
        key: artifact.content_hash for key, artifact in first.items()
    }
    assert all(
        json.loads((cache.root / artifact.cache_key / "manifest.json").read_text())[
            "parameters"
        ]["adjustment_mode"]
        == AdjustmentMode.FORWARD.value
        for artifact in first.values()
    )
    assert all(
        json.loads((cache.root / artifact.cache_key / "manifest.json").read_text())[
            "parameters"
        ]["price_basis"]
        == "baostock_forward_log_return_v2"
        for artifact in first.values()
    )
    assert all(
        json.loads((cache.root / artifact.cache_key / "manifest.json").read_text())[
            "parameters"
        ]["log_return_formula"]
        == "log_close_minus_log_preclose_v2"
        for artifact in first.values()
    )


def _cache_state(root: Path) -> dict[str, tuple[int, int, int]]:
    return {
        str(path.relative_to(root)): (
            path.stat().st_ino,
            path.stat().st_mtime_ns,
            path.stat().st_size,
        )
        for path in root.rglob("*")
        if path.is_file()
    }
