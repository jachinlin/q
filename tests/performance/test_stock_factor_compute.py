"""Opt-in 20-year maximum-partition stock-factor performance evidence."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from quant_research.data.adjustments import FORWARD_LOG_RETURN_COLUMN
from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FactorContext
from quant_research.factors.builtin.auxiliary import AvgAmount20dFactor
from quant_research.factors.builtin.momentum import MarketBarsCache, Momentum12020Factor
from quant_research.factors.builtin.quality import RoePitFactor
from quant_research.factors.builtin.risk import (
    DownsideVolatility60dFactor,
    MaxDrawdown120dFactor,
    Volatility60dFactor,
)
from quant_research.factors.builtin.valuation import (
    BookToPriceFactor,
    DailyBasicsCache,
    EarningsYieldFactor,
)
from tests.performance._process_memory import process_peak_rss_bytes

pytestmark = pytest.mark.performance

_PARTITION_SIZE = 100


class _SyntheticInputs:
    """Serve deterministic in-memory inputs while counting boundary reads."""

    def __init__(
        self,
        sessions: tuple[date, ...],
        market: pl.DataFrame,
        basics: pl.DataFrame,
        financials: pl.DataFrame,
    ) -> None:
        self._sessions = sessions
        self._market = market
        self._basics = basics
        self._financials = financials
        self.market_reads = 0
        self.bar_reads = 0
        self.basics_reads = 0
        self.calendar_reads = 0
        self.financial_reads = 0

    def log_returns(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        del instruments, start
        assert lookback_sessions == 120
        self.market_reads += 1
        return self._market.lazy()

    def daily_basics(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        del instruments, start, end
        self.basics_reads += 1
        return self._basics.lazy()

    def bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        del instruments
        self.bar_reads += 1
        return self._market.filter(
            pl.col("trade_date").is_between(start, end, closed="both")
        ).select("instrument_id", "trade_date", "amount", "available_at").lazy()

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        self.calendar_reads += 1
        sessions = [day for day in self._sessions if start <= day <= end]
        return pl.DataFrame(
            {"trade_date": sessions, "is_trading_day": [True] * len(sessions)},
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()

    def financial_history(
        self,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        del field_ids, as_of, instruments
        self.financial_reads += 1
        return self._financials.lazy()


def test_twenty_year_max_partition_stock_factors_record_evidence() -> None:
    """Study factors and the liquidity auxiliary use bounded native reads."""
    current = date(2006, 1, 2)
    end = date(2025, 12, 31)
    session_values: list[date] = []
    while current <= end:
        if current.weekday() < 5:
            session_values.append(current)
        current += timedelta(days=1)
    sessions = tuple(session_values)
    instruments = tuple(
        InstrumentId.parse(f"{600_000 + index:06d}.SH")
        for index in range(_PARTITION_SIZE)
    )
    scope = pl.DataFrame(
        {"instrument_id": [instrument.canonical() for instrument in instruments]},
        schema={"instrument_id": pl.String},
    )
    session_frame = pl.DataFrame(
        {
            "trade_date": sessions,
            FORWARD_LOG_RETURN_COLUMN: [
                ((index % 31) - 15) / 10_000.0 for index in range(len(sessions))
            ],
            "available_at": [
                datetime(day.year, day.month, day.day, 8, tzinfo=UTC)
                for day in sessions
            ],
        },
        schema={
            "trade_date": pl.Date,
            FORWARD_LOG_RETURN_COLUMN: pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
        },
    )
    market = (
        scope.join(session_frame, how="cross")
        .with_columns(pl.lit(1_000_000.0).alias("amount"))
        .select(
            "trade_date",
            "instrument_id",
            FORWARD_LOG_RETURN_COLUMN,
            "amount",
            "available_at",
        )
        .sort("instrument_id", "trade_date")
    )
    basics = market.select(
        "trade_date",
        "instrument_id",
        pl.lit(10.0).alias("pe_ttm"),
        pl.lit(2.0).alias("pb_mrq"),
        "available_at",
    )
    financial_rows = []
    for instrument in instruments:
        for revision, session in enumerate(sessions[::63]):
            financial_rows.append(
                {
                    "instrument_id": instrument.canonical(),
                    "report_period": session,
                    "metric": "dupont_roe",
                    "value": 0.10 + (revision % 5) / 100.0,
                    "revision": revision,
                    "available_at": datetime(
                        session.year, session.month, session.day, 8, tzinfo=UTC
                    ),
                }
            )
    financials = pl.DataFrame(
        financial_rows,
        schema={
            "instrument_id": pl.String,
            "report_period": pl.Date,
            "metric": pl.String,
            "value": pl.Float64,
            "revision": pl.Int64,
            "available_at": pl.Datetime("us", "UTC"),
        },
    )
    inputs = _SyntheticInputs(sessions, market, basics, financials)
    context = FactorContext("a" * 64, "b" * 64, sessions[0], sessions[-1])
    market_cache = MarketBarsCache(inputs, instruments, max_lookback_sessions=120)
    basics_cache = DailyBasicsCache(inputs, instruments)
    factors = (
        EarningsYieldFactor(inputs, instruments, daily_basics=basics_cache),
        BookToPriceFactor(inputs, instruments, daily_basics=basics_cache),
        RoePitFactor(inputs, instruments),
        Momentum12020Factor(inputs, instruments, market_bars=market_cache),
        Volatility60dFactor(inputs, instruments, market_bars=market_cache),
        DownsideVolatility60dFactor(inputs, instruments, market_bars=market_cache),
        MaxDrawdown120dFactor(inputs, instruments, market_bars=market_cache),
        AvgAmount20dFactor(inputs, instruments),
    )

    stage_seconds: dict[str, float] = {}
    expected_rows = len(sessions) * len(instruments)
    for factor in factors:
        started = time.perf_counter()
        result = factor.compute(context).collect()
        stage_seconds[factor.spec.factor_id] = time.perf_counter() - started
        assert result.height == expected_rows

    evidence = {
        "workload": "SYNTHETIC_MAX_FACTOR_PARTITION",
        "sessions": len(sessions),
        "instruments": len(instruments),
        "rows_per_factor": expected_rows,
        "peak_memory_bytes": process_peak_rss_bytes(),
        "stage_seconds": stage_seconds,
    }
    print(f"stock_factor_performance={json.dumps(evidence, sort_keys=True)}")
    assert inputs.market_reads == 1
    assert inputs.bar_reads == 2
    assert inputs.basics_reads == 1
    assert inputs.calendar_reads == 1
    assert inputs.financial_reads == 1
