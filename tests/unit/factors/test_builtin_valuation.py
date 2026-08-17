from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl
import pytest

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FactorContext
from quant_research.factors.builtin.valuation import (
    BookToPriceFactor,
    DailyBasicsCache,
    EarningsYieldFactor,
)

_SCHEMA = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "pe_ttm": pl.Float64,
    "pb_mrq": pl.Float64,
    "available_at": pl.Datetime("us", "UTC"),
}


class _Repository:
    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame
        self.calls = 0

    def daily_basics(
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

    def bars(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        raise AssertionError((instruments, start, end))


def _available(day: date, hour: int = 8) -> datetime:
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


def test_signed_valuation_reciprocals_share_one_input_read() -> None:
    days = [date(2026, 4, 27), date(2026, 4, 28), date(2026, 4, 29)]
    frame = pl.DataFrame(
        {
            "trade_date": days,
            "instrument_id": ["000001.SZ"] * 3,
            "pe_ttm": [10.0, -5.0, 0.0],
            "pb_mrq": [2.0, -4.0, float("inf")],
            "available_at": [_available(day) for day in days],
        },
        schema=_SCHEMA,
    )
    repository = _Repository(frame)
    instrument = InstrumentId.parse("000001.SZ")
    cache = DailyBasicsCache(repository, (instrument,))
    earnings = EarningsYieldFactor(repository, (instrument,), daily_basics=cache)
    book = BookToPriceFactor(repository, (instrument,), daily_basics=cache)
    context = FactorContext("a" * 64, "b" * 64, days[0], days[-1])

    earnings_result = earnings.compute(context).collect()
    book_result = book.compute(context).collect()

    assert earnings_result["value"].to_list() == [0.1, -0.2, None]
    assert book_result["value"].to_list() == [0.5, -0.25, None]
    assert earnings_result["is_valid"].to_list() == [True, True, False]
    assert book_result["is_valid"].to_list() == [True, True, False]
    assert repository.calls == 1


def test_valuation_rejects_nonfinite_and_future_availability() -> None:
    days = [date(2026, 4, 27), date(2026, 4, 28)]
    frame = pl.DataFrame(
        {
            "trade_date": days,
            "instrument_id": ["000001.SZ"] * 2,
            "pe_ttm": [float("nan"), 10.0],
            "pb_mrq": [1.0, 1.0],
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
            "pb_mrq": [2.0, 2.0],
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
