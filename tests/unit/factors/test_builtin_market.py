from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from math import expm1, log, sqrt

import numpy as np
import polars as pl
import pytest

from quant_research.data.canonical.adjustments import FORWARD_LOG_RETURN_COLUMN
from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FactorContext, factor_table_content_hash
from quant_research.factors.builtin.momentum import (
    MarketBarsCache,
    Momentum12020Factor,
    ReturnFactor,
    Trend120dFactor,
)
from quant_research.factors.builtin.risk import (
    DownsideVolatility60dFactor,
    MaxDrawdown120dFactor,
    Volatility60dFactor,
)

_SCHEMA = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    FORWARD_LOG_RETURN_COLUMN: pl.Float64,
    "available_at": pl.Datetime("us", "UTC"),
}


class _PriceService:
    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame
        self.calls = 0
        self.lookbacks: list[int] = []

    def log_returns(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        self.calls += 1
        self.lookbacks.append(lookback_sessions)
        ids = [instrument.canonical() for instrument in instruments]
        return self._frame.filter(pl.col("instrument_id").is_in(ids)).lazy()


def _frame(returns: list[float | None]) -> pl.DataFrame:
    first = date(2026, 1, 1)
    days = [first + timedelta(days=index) for index in range(len(returns))]
    return pl.DataFrame(
        {
            "trade_date": days,
            "instrument_id": ["000001.SZ"] * len(days),
            FORWARD_LOG_RETURN_COLUMN: returns,
            "available_at": [
                datetime(day.year, day.month, day.day, 8, tzinfo=UTC) for day in days
            ],
        },
        schema=_SCHEMA,
    )


def _context(frame: pl.DataFrame) -> FactorContext:
    day = frame["trade_date"][-1]
    return FactorContext("a" * 64, "b" * 64, day, day)


def test_market_factors_match_hard_coded_window_oracles_and_share_one_read() -> None:
    returns = [0.0] + [0.01] * 100 + [0.5] * 20
    frame = _frame(returns)
    service = _PriceService(frame)
    instrument = InstrumentId.parse("000001.SZ")
    cache = MarketBarsCache(service, (instrument,), max_lookback_sessions=120)
    factors = (
        Momentum12020Factor(service, (instrument,), market_bars=cache),
        Volatility60dFactor(service, (instrument,), market_bars=cache),
        DownsideVolatility60dFactor(service, (instrument,), market_bars=cache),
        MaxDrawdown120dFactor(service, (instrument,), market_bars=cache),
    )

    results = [factor.compute(_context(frame)).collect() for factor in factors]

    assert results[0].item(0, "value") == pytest.approx(expm1(1.0))
    assert results[1].item(0, "value") == pytest.approx(
        float(np.std(returns[-60:], ddof=1) * sqrt(252.0))
    )
    assert results[2].item(0, "value") == pytest.approx(0.0)
    assert results[3].item(0, "value") == pytest.approx(0.0)
    assert service.calls == 1


def test_market_bars_cache_recomputes_only_when_context_changes() -> None:
    frame = _frame([0.0, *([0.01] * 120)])
    service = _PriceService(frame)
    instrument = InstrumentId.parse("000001.SZ")
    cache = MarketBarsCache(service, (instrument,), max_lookback_sessions=120)
    first_context = _context(frame)
    second_context = FactorContext(
        "a" * 64,
        "c" * 64,
        first_context.start,
        first_context.end,
    )

    first_bars = cache.load(first_context)
    assert cache.load(first_context) is first_bars
    assert service.calls == 1

    second_bars = cache.load(second_context)
    assert second_bars is not first_bars
    assert cache.load(second_context) is second_bars
    assert service.calls == 2


def test_downside_volatility_uses_all_sessions_in_denominator() -> None:
    trailing = [-0.02 if index % 2 == 0 else 0.03 for index in range(60)]
    frame = _frame([0.0, *([0.0] * 60), *trailing])
    service = _PriceService(frame)
    instrument = InstrumentId.parse("000001.SZ")
    factor = DownsideVolatility60dFactor(service, (instrument,))

    result = factor.compute(_context(frame)).collect()

    expected = sqrt(sum(min(value, 0.0) ** 2 for value in trailing) / 60) * sqrt(252.0)
    assert result.item(0, "value") == pytest.approx(expected)


def test_max_drawdown_vector_kernel_matches_peak_to_later_trough() -> None:
    returns = [0.0] * 120
    returns[1] = log(1.2)
    returns[2] = log(0.75)
    returns[3] = log(110.0 / 90.0)
    frame = _frame(returns)
    service = _PriceService(frame)
    instrument = InstrumentId.parse("000001.SZ")
    factor = MaxDrawdown120dFactor(service, (instrument,))

    result = factor.compute(_context(frame)).collect()

    assert result.item(0, "value") == pytest.approx(0.25)


def test_missing_canonical_session_invalidates_market_windows() -> None:
    returns: list[float | None] = [0.0] * 121
    returns[-10] = None
    frame = _frame(returns).with_columns(
        pl.when(pl.int_range(pl.len()) == 111)
        .then(pl.lit(None, dtype=pl.Datetime("us", "UTC")))
        .otherwise(pl.col("available_at"))
        .alias("available_at")
    )
    service = _PriceService(frame)
    instrument = InstrumentId.parse("000001.SZ")
    cache = MarketBarsCache(service, (instrument,), max_lookback_sessions=120)
    factors = (
        Momentum12020Factor(service, (instrument,), market_bars=cache),
        Volatility60dFactor(service, (instrument,), market_bars=cache),
        DownsideVolatility60dFactor(service, (instrument,), market_bars=cache),
        MaxDrawdown120dFactor(service, (instrument,), market_bars=cache),
    )

    results = [factor.compute(_context(frame)).collect() for factor in factors]

    assert all(result.item(0, "value") is None for result in results)
    assert all(result.item(0, "is_valid") is False for result in results)


def test_market_factor_output_order_and_hash_ignore_input_row_order() -> None:
    first = _frame([0.0] * 120)
    second = first.with_columns(pl.lit("600000.SH").alias("instrument_id"))
    ordered = pl.concat([first, second]).sort("instrument_id", "trade_date")
    shuffled = ordered.sample(fraction=1.0, shuffle=True, seed=7)
    instruments = (
        InstrumentId.parse("000001.SZ"),
        InstrumentId.parse("600000.SH"),
    )
    ordered_factor = MaxDrawdown120dFactor(_PriceService(ordered), instruments)
    shuffled_factor = MaxDrawdown120dFactor(_PriceService(shuffled), instruments)
    context = _context(first)

    ordered_result = ordered_factor.compute(context).collect()
    shuffled_result = shuffled_factor.compute(context).collect()

    assert ordered_result.equals(shuffled_result)
    assert factor_table_content_hash(
        ordered_result.to_arrow()
    ) == factor_table_content_hash(shuffled_result.to_arrow())


def test_etf_return_and_trend_share_session_complete_market_input() -> None:
    frame = _frame([0.0, *([0.01] * 120)])
    service = _PriceService(frame)
    instrument = InstrumentId.parse("000001.SZ")
    cache = MarketBarsCache(service, (instrument,), max_lookback_sessions=120)
    return_factor = ReturnFactor(service, (instrument,), 20, market_bars=cache)
    trend_factor = Trend120dFactor(service, (instrument,), market_bars=cache)

    return_result = return_factor.compute(_context(frame)).collect()
    trend_result = trend_factor.compute(_context(frame)).collect()

    assert return_result.item(0, "value") == pytest.approx(expm1(0.2))
    assert trend_result.item(0, "value") == pytest.approx(0.01)
    assert service.calls == 1
