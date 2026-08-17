from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import polars as pl
import pytest

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import FactorContext
from quant_research.factors.builtin.quality import RoePitFactor

_FINANCIAL_SCHEMA = {
    "instrument_id": pl.String,
    "report_period": pl.Date,
    "metric": pl.String,
    "value": pl.Float64,
    "revision": pl.Int64,
    "available_at": pl.Datetime("us", "UTC"),
}


class _Provider:
    def __init__(self, rows: list[dict[str, object]], signals: list[date]) -> None:
        self._history = pl.DataFrame(rows, schema=_FINANCIAL_SCHEMA)
        self._signals = signals
        self.history_calls = 0
        self.calendar_calls = 0

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        self.calendar_calls += 1
        days = [day for day in self._signals if start <= day <= end]
        return pl.DataFrame(
            {"trade_date": days, "is_trading_day": [True] * len(days)},
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()

    def financial_history(
        self,
        field_ids: tuple[str, ...],
        as_of: date,
        instruments: tuple[InstrumentId, ...] | None = None,
    ) -> pl.LazyFrame:
        self.history_calls += 1
        assert field_ids == ("dupont_roe",)
        assert instruments is not None
        instrument_ids = [instrument.canonical() for instrument in instruments]
        return self._history.filter(
            pl.col("instrument_id").is_in(instrument_ids)
            & (pl.col("available_at").dt.date() <= as_of)
        ).lazy()


def _row(
    *,
    period: date,
    value: float,
    available: date,
    revision: int = 0,
    instrument: str = "000001.SZ",
) -> dict[str, object]:
    return {
        "instrument_id": instrument,
        "report_period": period,
        "metric": "dupont_roe",
        "value": value,
        "revision": revision,
        "available_at": datetime.combine(available, datetime.min.time(), tzinfo=UTC),
    }


def _context(start: date, end: date) -> FactorContext:
    return FactorContext("a" * 64, "b" * 64, start, end)


def test_roe_pit_uses_event_history_without_daily_queries() -> None:
    signals = [date(2026, 4, 29), date(2026, 4, 30), date(2026, 5, 1)]
    provider = _Provider(
        [
            _row(period=date(2025, 9, 30), value=0.10, available=date(2026, 1, 1)),
            _row(period=date(2025, 12, 31), value=0.12, available=date(2026, 4, 29)),
            _row(
                period=date(2025, 9, 30),
                value=0.90,
                available=date(2026, 4, 30),
                revision=1,
            ),
            _row(
                period=date(2025, 12, 31),
                value=0.13,
                available=date(2026, 5, 1),
                revision=1,
            ),
        ],
        signals,
    )
    factor = RoePitFactor(provider, (InstrumentId.parse("000001.SZ"),))

    result = factor.compute(_context(signals[0], signals[-1])).collect()

    assert result.select("trade_date", "value", "is_valid").rows() == [
        (signals[0], 0.12, True),
        (signals[1], 0.12, True),
        (signals[2], 0.13, True),
    ]
    assert provider.calendar_calls == 1
    assert provider.history_calls == 1
    assert factor.spec.parameters["staleness_calendar_days"] == 190


def test_roe_pit_staleness_boundary_is_inclusive() -> None:
    available = date(2026, 1, 1)
    signals = [available + timedelta(days=190), available + timedelta(days=191)]
    provider = _Provider(
        [
            _row(
                period=date(2025, 12, 31),
                value=0.12,
                available=available,
            )
        ],
        signals,
    )
    factor = RoePitFactor(provider, (InstrumentId.parse("000001.SZ"),))

    result = factor.compute(_context(signals[0], signals[-1])).collect()

    assert result["value"].to_list() == [0.12, None]
    assert result["is_valid"].to_list() == [True, False]


def test_roe_pit_nonfinite_latest_report_does_not_fall_back() -> None:
    signal = date(2026, 4, 30)
    provider = _Provider(
        [
            _row(period=date(2025, 9, 30), value=0.10, available=date(2026, 1, 1)),
            _row(
                period=date(2025, 12, 31),
                value=float("nan"),
                available=date(2026, 4, 29),
            ),
        ],
        [signal],
    )
    factor = RoePitFactor(provider, (InstrumentId.parse("000001.SZ"),))

    result = factor.compute(_context(signal, signal)).collect()

    assert result.select("value", "is_valid").rows() == [(None, False)]


def test_roe_pit_rejects_duplicate_revision_keys() -> None:
    signal = date(2026, 4, 30)
    row = _row(period=date(2025, 12, 31), value=0.12, available=date(2026, 4, 29))
    factor = RoePitFactor(
        _Provider([row, dict(row)], [signal]),
        (InstrumentId.parse("000001.SZ"),),
    )

    with pytest.raises(ValueError, match="duplicate financial revision key"):
        factor.compute(_context(signal, signal))
