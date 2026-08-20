from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl
import pytest

from quant_research.data.canonical.adjustments import (
    AdjustmentMode,
    _PriceAdjustmentEngine,
)
from quant_research.domain.identifiers import InstrumentId


class _BarsRepository:
    def __init__(
        self, bars: pl.DataFrame, sessions: tuple[date, ...] | None = None
    ) -> None:
        self._bars = bars
        self._sessions = sessions or tuple(bars["trade_date"].to_list())
        self.bar_requests: list[tuple[date, date]] = []

    def bars(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        del instruments
        self.bar_requests.append((start, end))
        return self._bars.filter(pl.col("trade_date").is_between(start, end)).lazy()

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        sessions = [day for day in self._sessions if start <= day <= end]
        return pl.DataFrame(
            {
                "trade_date": sessions,
                "is_trading_day": [True] * len(sessions),
            },
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()


def test_adjustment_mode_excludes_backward_prices() -> None:
    assert {mode.value for mode in AdjustmentMode} == {"RAW", "FORWARD"}
    with pytest.raises(ValueError, match="BACKWARD"):
        AdjustmentMode("BACKWARD")


def test_forward_adjustment_omits_untraded_suspension_placeholders() -> None:
    bars = pl.DataFrame(
        {
            "instrument_id": ["300114.SZ"] * 3,
            "trade_date": [date(2025, 2, 14), date(2025, 2, 17), date(2025, 2, 18)],
            "open": [10.0, None, 10.2],
            "high": [10.5, None, 10.6],
            "low": [9.9, None, 10.1],
            "close": [10.3, None, 10.5],
            "preclose": [10.0, None, 10.3],
            "volume": [1000, None, 1200],
            "amount": [10_200.0, None, 12_500.0],
        }
    )
    repository = _BarsRepository(bars)
    service = _PriceAdjustmentEngine(repository)

    result = service.adjusted_bars(
        (InstrumentId.parse("300114.SZ"),),
        date(2025, 2, 14),
        date(2025, 2, 18),
    ).collect()

    assert result["trade_date"].to_list() == [date(2025, 2, 14), date(2025, 2, 18)]
    assert result["close"].null_count() == 0
    assert result["adjustment_as_of"].to_list() == [
        date(2025, 2, 18),
        date(2025, 2, 18),
    ]
    assert repository.bar_requests == [(date(2025, 2, 14), date(2025, 2, 18))]


def test_log_returns_distinguish_suspension_from_missing_session() -> None:
    sessions = tuple(date(2025, 2, day) for day in range(14, 18))
    available = [datetime(2025, 2, day, 8, tzinfo=UTC) for day in (14, 15, 16)]
    bars = pl.DataFrame(
        {
            "instrument_id": ["300114.SZ"] * 3,
            "trade_date": list(sessions[:3]),
            "open": [10.0, None, 10.0],
            "high": [10.0, None, 11.0],
            "low": [10.0, None, 10.0],
            "close": [10.0, None, 11.0],
            "preclose": [10.0, None, 10.0],
            "volume": [1000, None, 1200],
            "amount": [10_000.0, None, 12_000.0],
            "available_at": available,
        },
        schema={
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "preclose": pl.Float64,
            "volume": pl.Int64,
            "amount": pl.Float64,
            "available_at": pl.Datetime("us", "UTC"),
        },
    )
    repository = _BarsRepository(bars, sessions)
    service = _PriceAdjustmentEngine(repository)

    result = service.log_returns(
        (InstrumentId.parse("300114.SZ"),),
        sessions[2],
        sessions[3],
        lookback_sessions=2,
    ).collect()

    assert result["trade_date"].to_list() == list(sessions)
    returns = result["forward_log_return"].to_list()
    assert returns[:3] == pytest.approx([0.0, 0.0, 0.09531017980432493])
    assert returns[3] is None
    assert result["available_at"].to_list() == [*available, None]
    assert repository.bar_requests == [(sessions[0], sessions[3])]
