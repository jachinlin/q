from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext
from quant_research.factors.builtin.auxiliary import AvgAmount20dFactor

_BAR_SCHEMA = {
    "instrument_id": pl.String,
    "trade_date": pl.Date,
    "amount": pl.Float64,
    "available_at": pl.Datetime("us", "UTC"),
}


class _Repository:
    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame
        self.calls = 0

    def stock_bars(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        self.calls += 1
        identifiers = [instrument.canonical() for instrument in instruments]
        return self._frame.filter(
            pl.col("instrument_id").is_in(identifiers)
            & pl.col("trade_date").is_between(start, end, closed="both")
        ).lazy()

    def stock_daily_basics(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        raise AssertionError((instruments, start, end))


def _frame(
    amounts: list[float], *, future_last_availability: bool = False
) -> pl.DataFrame:
    first = date(2026, 1, 1)
    days = [first + timedelta(days=index) for index in range(len(amounts))]
    availability = [
        datetime(day.year, day.month, day.day, 8, tzinfo=UTC) for day in days
    ]
    if future_last_availability:
        last = days[-1] + timedelta(days=1)
        availability[-1] = datetime(last.year, last.month, last.day, 8, tzinfo=UTC)
    return pl.DataFrame(
        {
            "instrument_id": ["000001.SZ"] * len(days),
            "trade_date": days,
            "amount": amounts,
            "available_at": availability,
        },
        schema=_BAR_SCHEMA,
    )


def _compute(frame: pl.DataFrame, start: date, end: date) -> tuple[pl.DataFrame, int]:
    repository = _Repository(frame)
    factor = AvgAmount20dFactor(repository, (InstrumentId.parse("000001.SZ"),))
    result = factor.compute(FactorContext("a" * 64, "b" * 64, start, end)).collect()
    assert result.schema == FACTOR_OUTPUT_SCHEMA
    return result, repository.calls


def test_avg_amount_20d_uses_native_rolling_window() -> None:
    frame = _frame([float(index) for index in range(1, 22)])
    days = frame["trade_date"].to_list()

    result, calls = _compute(frame, days[19], days[20])

    assert result.select("trade_date", "value", "is_valid").rows() == [
        (days[19], pytest.approx(10.5), True),
        (days[20], pytest.approx(11.5), True),
    ]
    assert calls == 1


def test_avg_amount_20d_rejects_bad_window_and_future_availability() -> None:
    negative = _frame([-1.0, *[float(index) for index in range(2, 22)]])
    days = negative["trade_date"].to_list()

    negative_result, _ = _compute(negative, days[19], days[20])
    future_result, _ = _compute(
        _frame([float(index) for index in range(1, 21)], future_last_availability=True),
        days[19],
        days[19],
    )

    assert negative_result.select("value", "is_valid").rows() == [
        (None, False),
        (pytest.approx(11.5), True),
    ]
    assert future_result.select("value", "is_valid").row(0) == (None, False)
    assert future_result["available_at"].item() is not None
