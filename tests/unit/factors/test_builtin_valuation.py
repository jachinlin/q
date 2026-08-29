from __future__ import annotations

from datetime import UTC, date, datetime
from math import e

import polars as pl
import pytest

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext
from quant_research.factors.builtin.valuation import (
    BookToPriceFactor,
    DailyBasicsCache,
    DividendYieldFactor,
    EarningsYieldFactor,
    LogTotalMarketCapFactor,
    SalesYieldFactor,
)

_SCHEMA = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "pe_ttm": pl.Float64,
    "pb": pl.Float64,
    "ps_ttm": pl.Float64,
    "dividend_yield_ttm": pl.Float64,
    "total_market_value": pl.Float64,
    "available_at": pl.Datetime("us", "UTC"),
}


class _Repository:
    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame
        self.calls = 0

    def stock_daily_basics(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        self.calls += 1
        ids = [instrument.canonical() for instrument in instruments]
        return self._frame.filter(
            pl.col("instrument_id").is_in(ids)
            & pl.col("trade_date").is_between(start, end, closed="both")
        ).lazy()

    def stock_bars(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        raise AssertionError((instruments, start, end))


def _available(day: date, hour: int = 8) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


def test_daily_basic_factors_share_one_input_read() -> None:
    days = [date(2026, 4, 27), date(2026, 4, 28), date(2026, 4, 29)]
    frame = pl.DataFrame(
        {
            "trade_date": days,
            "instrument_id": ["000001.SZ"] * 3,
            "pe_ttm": [10.0, -5.0, 0.0],
            "pb": [2.0, -4.0, float("inf")],
            "ps_ttm": [4.0, -2.0, float("nan")],
            "dividend_yield_ttm": [0.02, 0.0, -0.01],
            "total_market_value": [e, e**2, e**3],
            "available_at": [_available(day) for day in days],
        },
        schema=_SCHEMA,
    )
    repository = _Repository(frame)
    instrument = InstrumentId.parse("000001.SZ")
    cache = DailyBasicsCache(repository, (instrument,))
    earnings = EarningsYieldFactor(repository, (instrument,), daily_basics=cache)
    book = BookToPriceFactor(repository, (instrument,), daily_basics=cache)
    sales = SalesYieldFactor(repository, (instrument,), daily_basics=cache)
    dividend = DividendYieldFactor(repository, (instrument,), daily_basics=cache)
    market_cap = LogTotalMarketCapFactor(
        repository, (instrument,), daily_basics=cache
    )
    context = FactorContext("a" * 64, "b" * 64, days[0], days[-1])

    earnings_result = earnings.compute(context).collect()
    book_result = book.compute(context).collect()
    sales_result = sales.compute(context).collect()
    dividend_result = dividend.compute(context).collect()
    market_cap_result = market_cap.compute(context).collect()

    assert earnings_result["value"].to_list() == [0.1, -0.2, None]
    assert book_result["value"].to_list() == [0.5, -0.25, None]
    assert sales_result["value"].to_list() == [0.25, -0.5, None]
    assert dividend_result["value"].to_list() == [0.02, 0.0, None]
    assert earnings_result["is_valid"].to_list() == [True, True, False]
    assert book_result["is_valid"].to_list() == [True, True, False]
    assert sales_result["is_valid"].to_list() == [True, True, False]
    assert dividend_result["is_valid"].to_list() == [True, True, False]
    assert sales.spec.parameters["measurement"] == "ttm"
    assert sales.spec.parameters["source_field"] == "ps_ttm"
    assert dividend.spec.parameters["value_domain"] == "nonnegative_finite"
    assert dividend.spec.parameters["direction"] == 1
    assert market_cap_result["value"].to_list() == pytest.approx([1.0, 2.0, 3.0])
    assert market_cap.spec.direction == -1
    assert market_cap.spec.parameters == {
        "source_field": "total_market_value",
        "formula": "ln(total_market_value)",
        "positive_input_required": True,
        "eligible_for_alpha": True,
    }
    assert repository.calls == 1


def test_valuation_rejects_nonfinite_and_future_availability() -> None:
    days = [date(2026, 4, 27), date(2026, 4, 28)]
    frame = pl.DataFrame(
        {
            "trade_date": days,
            "instrument_id": ["000001.SZ"] * 2,
            "pe_ttm": [float("nan"), 10.0],
            "pb": [1.0, 1.0],
            "ps_ttm": [1.0, 1.0],
            "dividend_yield_ttm": [0.0, 0.0],
            "total_market_value": [1.0, 1.0],
            "available_at": [_available(days[0]), _available(date(2026, 4, 29))],
        },
        schema=_SCHEMA,
    )
    repository = _Repository(frame)
    instrument = InstrumentId.parse("000001.SZ")
    factor = EarningsYieldFactor(repository, (instrument,))

    result = factor.compute(
        FactorContext("a" * 64, "b" * 64, days[0], days[-1])
    ).collect()

    assert result["value"].to_list() == [None, None]
    assert result["is_valid"].to_list() == [False, False]


def test_valuation_cache_rejects_duplicate_keys() -> None:
    day = date(2026, 4, 27)
    frame = pl.DataFrame(
        {
            "trade_date": [day, day],
            "instrument_id": ["000001.SZ", "000001.SZ"],
            "pe_ttm": [10.0, 11.0],
            "pb": [2.0, 2.0],
            "ps_ttm": [3.0, 3.0],
            "dividend_yield_ttm": [0.0, 0.0],
            "total_market_value": [1.0, 1.0],
            "available_at": [_available(day), _available(day)],
        },
        schema=_SCHEMA,
    )
    repository = _Repository(frame)
    instrument = InstrumentId.parse("000001.SZ")

    with pytest.raises(ValueError, match="duplicate valuation bar key"):
        EarningsYieldFactor(repository, (instrument,)).compute(
            FactorContext("a" * 64, "b" * 64, day, day)
        )


def test_log_total_market_cap_rejects_invalid_or_future_values() -> None:
    days = [date(2026, 4, 27)] * 6
    frame = pl.DataFrame(
        {
            "trade_date": days,
            "instrument_id": [
                "000006.SZ",
                "000001.SZ",
                "000005.SZ",
                "000002.SZ",
                "000004.SZ",
                "000003.SZ",
            ],
            "pe_ttm": [1.0] * 6,
            "pb": [1.0] * 6,
            "ps_ttm": [1.0] * 6,
            "dividend_yield_ttm": [0.0] * 6,
            "total_market_value": [0.0, -1.0, float("nan"), float("inf"), None, e],
            "available_at": [
                _available(days[0]),
                _available(days[0]),
                _available(days[0]),
                _available(days[0]),
                _available(days[0]),
                _available(date(2026, 4, 28)),
            ],
        },
        schema=_SCHEMA,
    )
    repository = _Repository(frame)
    instruments = tuple(
        InstrumentId.parse(value) for value in frame["instrument_id"].to_list()
    )
    factor = LogTotalMarketCapFactor(repository, instruments)

    result = factor.compute(
        FactorContext("a" * 64, "b" * 64, days[0], days[0])
    ).collect()

    assert result.schema == FACTOR_OUTPUT_SCHEMA
    assert result["instrument_id"].to_list() == sorted(
        frame["instrument_id"].to_list()
    )
    assert result["value"].to_list() == [None] * 6
    assert result["is_valid"].to_list() == [False] * 6
