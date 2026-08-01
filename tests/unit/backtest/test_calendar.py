"""Trading-calendar behavior bound to one immutable research snapshot."""

from datetime import date

import polars as pl
import pytest

from quant_core.backtest.calendar import TradingCalendar
from quant_core.domain.identifiers import SnapshotId


class CalendarRepository:
    """A complete in-memory implementation of the calendar repository boundary."""

    def __init__(self, frame: pl.DataFrame) -> None:
        self.frame = frame
        self.calls: list[tuple[SnapshotId, date, date]] = []

    def trade_calendar(
        self, snapshot_id: SnapshotId, start: date, end: date
    ) -> pl.LazyFrame:
        self.calls.append((snapshot_id, start, end))
        return self.frame.lazy()


_SNAPSHOT = SnapshotId.parse("00000000-0000-0000-0000-000000000101")
_FRIDAY = date(2024, 2, 9)
_SPRING_FESTIVAL = date(2024, 2, 12)
_MONDAY = date(2024, 2, 19)


def test_calendar_uses_explicit_snapshot_and_skips_weekend_and_spring_festival() -> (
    None
):
    repository = CalendarRepository(
        _calendar_frame(
            [_FRIDAY, date(2024, 2, 10), _SPRING_FESTIVAL, _MONDAY],
            [True, False, False, True],
        )
    )

    calendar = TradingCalendar.load(repository, _SNAPSHOT, _FRIDAY, _MONDAY)

    assert calendar.snapshot_id == _SNAPSHOT
    assert repository.calls == [(_SNAPSHOT, _FRIDAY, _MONDAY)]
    assert calendar.sessions(_FRIDAY, _MONDAY) == (_FRIDAY, _MONDAY)
    assert calendar.next_session(date(2024, 2, 10)) == _MONDAY


def test_calendar_rejects_last_session_next_session_request() -> None:
    calendar = TradingCalendar.load(
        CalendarRepository(_calendar_frame([_FRIDAY, _MONDAY], [True, True])),
        _SNAPSHOT,
        _FRIDAY,
        _MONDAY,
    )

    with pytest.raises(ValueError, match="no later trading session"):
        calendar.next_session(_MONDAY)


@pytest.mark.parametrize(
    ("dates", "flags", "message"),
    [
        ([], [], "empty"),
        ([_FRIDAY, _FRIDAY], [True, False], "duplicate"),
        ([_FRIDAY], [1], "bool"),
        ([_FRIDAY, date(2024, 2, 20)], [True, False], "outside requested range"),
    ],
)
def test_calendar_fails_closed_for_invalid_repository_rows(
    dates: list[date], flags: list[object], message: str
) -> None:
    repository = CalendarRepository(_calendar_frame(dates, flags))

    with pytest.raises(ValueError, match=message):
        TradingCalendar.load(repository, _SNAPSHOT, _FRIDAY, _MONDAY)


def test_calendar_rejects_missing_columns_and_invalid_ranges() -> None:
    missing_flag = CalendarRepository(pl.DataFrame({"trade_date": [_FRIDAY]}))
    with pytest.raises(ValueError, match="required columns"):
        TradingCalendar.load(missing_flag, _SNAPSHOT, _FRIDAY, _MONDAY)

    with pytest.raises(ValueError, match="start must not follow end"):
        TradingCalendar.load(missing_flag, _SNAPSHOT, _MONDAY, _FRIDAY)

    calendar = TradingCalendar.load(
        CalendarRepository(_calendar_frame([_FRIDAY, _MONDAY], [True, True])),
        _SNAPSHOT,
        _FRIDAY,
        _MONDAY,
    )
    with pytest.raises(ValueError, match="start must not follow end"):
        calendar.sessions(_MONDAY, _FRIDAY)
    with pytest.raises(ValueError, match="outside loaded coverage"):
        calendar.sessions(date(2024, 2, 8), _FRIDAY)


def _calendar_frame(dates: list[date], flags: list[object]) -> pl.DataFrame:
    return pl.DataFrame({"trade_date": dates, "is_trading_day": flags})
