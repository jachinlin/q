from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FACTOR_OUTPUT_SCHEMA, FactorContext
from quant_research.factors.builtin.turnover import Turnover20dFactor

_SCHEMA = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "turnover_rate_free_float": pl.Float64,
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
        identities = [instrument.canonical() for instrument in instruments]
        return self._frame.filter(
            pl.col("instrument_id").is_in(identities)
            & pl.col("trade_date").is_between(start, end, closed="both")
        ).lazy()

    def stock_bars(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        raise AssertionError((instruments, start, end))


def _frame(
    values: list[float | None], *, future_at: int | None = None
) -> pl.DataFrame:
    first = date(2026, 4, 1)
    days = [first + timedelta(days=index) for index in range(len(values))]
    available = [
        datetime(day.year, day.month, day.day, 8, tzinfo=UTC) for day in days
    ]
    if future_at is not None:
        future = days[-1] + timedelta(days=1)
        available[future_at] = datetime(
            future.year, future.month, future.day, 8, tzinfo=UTC
        )
    return pl.DataFrame(
        {
            "trade_date": days,
            "instrument_id": ["000001.SZ"] * len(days),
            "turnover_rate_free_float": values,
            "available_at": available,
        },
        schema=_SCHEMA,
    )


def _compute(frame: pl.DataFrame) -> tuple[pl.DataFrame, Turnover20dFactor, int]:
    repository = _Repository(frame)
    factor = Turnover20dFactor(
        repository, (InstrumentId.parse("000001.SZ"),)
    )
    signal = frame["trade_date"].to_list()[-1]
    result = factor.compute(
        FactorContext("a" * 64, "b" * 64, signal, signal)
    ).collect()
    return result, factor, repository.calls


def test_turnover_20d_uses_literal_mean_and_complete_window() -> None:
    result, factor, calls = _compute(_frame([float(value) for value in range(1, 21)]))

    assert result.schema == FACTOR_OUTPUT_SCHEMA
    assert result.select("value", "is_valid").rows() == [(10.5, True)]
    assert factor.spec.direction == -1
    assert factor.spec.lookback_sessions == 19
    assert factor.spec.parameters == {
        "source_field": "turnover_rate_free_float",
        "formula": "mean(turnover_rate_free_float)",
        "window_observations": 20,
        "window_basis": "observed_daily_basic_rows",
        "value_domain": "nonnegative_finite",
        "full_window_required": True,
        "direction": -1,
        "eligible_for_alpha": True,
    }
    assert calls <= 2


@pytest.mark.parametrize(
    "bad_value",
    [None, -0.01, float("nan"), float("inf")],
)
def test_turnover_20d_rejects_invalid_window_value(bad_value: float | None) -> None:
    values: list[float | None] = [1.0] * 20
    values[3] = bad_value

    result, _, _ = _compute(_frame(values))

    assert result.select("value", "is_valid").rows() == [(None, False)]


def test_turnover_20d_rejects_future_availability() -> None:
    result, _, _ = _compute(_frame([1.0] * 20, future_at=2))

    assert result.select("value", "is_valid").rows() == [(None, False)]
