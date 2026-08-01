"""Snapshot-bound trading sessions with fail-closed calendar validation."""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import polars as pl

from quant_core.data.repository import ResearchDataRepository
from quant_core.domain.identifiers import SnapshotId


@dataclass(frozen=True, slots=True)
class TradingCalendar:
    """The open sessions returned for one explicit snapshot and date coverage."""

    snapshot_id: SnapshotId
    start: date
    end: date
    _sessions: tuple[date, ...]

    @classmethod
    def load(
        cls,
        repository: ResearchDataRepository,
        snapshot_id: SnapshotId,
        start: date,
        end: date,
    ) -> TradingCalendar:
        """Load and validate one inclusive calendar range from ``snapshot_id``."""
        _validate_range(start, end)
        frame = repository.trade_calendar(snapshot_id, start, end).collect()
        _validate_calendar_columns(frame)
        rows = frame.select("trade_date", "is_trading_day").rows()
        if not rows:
            raise ValueError("trade calendar interval must not be empty")
        observed: set[date] = set()
        sessions: list[date] = []
        for trade_date, is_trading_day in rows:
            if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
                raise ValueError("trade_date must contain date values")  # noqa: TRY004
            if type(is_trading_day) is not bool:
                raise ValueError("is_trading_day must contain bool values")
            if trade_date in observed:
                raise ValueError("trade calendar contains duplicate trade_date")
            if trade_date < start or trade_date > end:
                raise ValueError("trade calendar row is outside requested range")
            observed.add(trade_date)
            if is_trading_day:
                sessions.append(trade_date)
        sessions.sort()
        return cls(snapshot_id, start, end, tuple(sessions))

    def sessions(self, start: date, end: date) -> tuple[date, ...]:
        """Return open sessions in the inclusive loaded subrange."""
        _validate_range(start, end)
        if start < self.start or end > self.end:
            raise ValueError("requested sessions are outside loaded coverage")
        left = bisect_right(self._sessions, start - _ONE_DAY)
        right = bisect_right(self._sessions, end)
        return self._sessions[left:right]

    def next_session(self, trade_date: date) -> date:
        """Return the first loaded session strictly after ``trade_date``."""
        if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
            raise TypeError("trade_date must be a date")
        index = bisect_right(self._sessions, trade_date)
        if index == len(self._sessions):
            raise ValueError("no later trading session in loaded coverage")
        return self._sessions[index]


def _validate_range(start: date, end: date) -> None:
    if (
        not isinstance(start, date)
        or isinstance(start, datetime)
        or not isinstance(end, date)
        or isinstance(end, datetime)
    ):
        raise TypeError("start and end must be date values")
    if start > end:
        raise ValueError("start must not follow end")


def _validate_calendar_columns(frame: pl.DataFrame) -> None:
    required = {"trade_date", "is_trading_day"}
    if not required.issubset(frame.columns):
        raise ValueError("trade calendar is missing required columns")


_ONE_DAY = timedelta(days=1)
