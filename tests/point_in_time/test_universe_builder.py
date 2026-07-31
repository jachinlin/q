"""Truth-table coverage for audited, snapshot-bound historical universes."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from quant_core.data.repository import SnapshotResearchRepository
from quant_core.domain.enums import DatasetKind
from quant_core.domain.identifiers import SnapshotId
from quant_core.universe import UniverseBuilder, UniverseRules
from tests.fixtures.point_in_time import (
    FixtureSnapshotRepository,
    _write_dataset,
    point_in_time_fixture,
)

_AS_OF = date(2024, 4, 5)
_TRADING_DAYS = [
    date(2024, 3, 11) + timedelta(days=offset)
    for offset in range(26)
    if (date(2024, 3, 11) + timedelta(days=offset)).weekday() < 5
]
_FUTURE_DAYS = [date(2024, 4, 8), date(2024, 4, 9)]
_IDS = {
    "not_listed": "SSE:600000",
    "delisted": "SSE:600001",
    "new": "SSE:600002",
    "st": "SSE:600003",
    "suspended": "SSE:600004",
    "board": "SSE:600005",
    "illiquid": "SSE:600006",
    "missing_status": "SSE:600007",
    "eligible": "SSE:600008",
    "missing_history": "SSE:600009",
    "missing_amount": "SSE:600010",
    "status_delisted": "SSE:600011",
    "late_status": "SSE:600012",
    "late_bar": "SSE:600013",
}


def test_build_returns_one_auditable_row_per_instrument_with_reason_priority(
    tmp_path: Path,
) -> None:
    """Dropping a rule, changing priority, or reading future data changes this table."""
    repository, snapshot_id = _universe_fixture(tmp_path)

    result = UniverseBuilder(SnapshotResearchRepository(repository)).build(
        snapshot_id,
        _AS_OF,
        UniverseRules(min_listing_days=3, min_avg_amount_20d=100.0),
    )

    assert result.select(
        "instrument_id", "as_of", "eligible", "reason_codes"
    ).rows() == [
        (_IDS["not_listed"], _AS_OF, False, ["NOT_LISTED_YET", "RISK_WARNING"]),
        (_IDS["delisted"], _AS_OF, False, ["DELISTED"]),
        (_IDS["new"], _AS_OF, False, ["INSUFFICIENT_LISTING_DAYS"]),
        (_IDS["st"], _AS_OF, False, ["RISK_WARNING"]),
        (_IDS["suspended"], _AS_OF, False, ["SUSPENDED"]),
        (_IDS["board"], _AS_OF, False, ["BOARD_NOT_ALLOWED"]),
        (_IDS["illiquid"], _AS_OF, False, ["INSUFFICIENT_AVERAGE_AMOUNT"]),
        (_IDS["missing_status"], _AS_OF, False, ["STATUS_MISSING"]),
        (_IDS["eligible"], _AS_OF, True, []),
        (_IDS["missing_history"], _AS_OF, False, ["INSUFFICIENT_LIQUIDITY_HISTORY"]),
        (_IDS["missing_amount"], _AS_OF, False, ["LIQUIDITY_AMOUNT_MISSING"]),
        (_IDS["status_delisted"], _AS_OF, False, ["DELISTED"]),
        (_IDS["late_status"], _AS_OF, False, ["STATUS_MISSING"]),
        (_IDS["late_bar"], _AS_OF, False, ["INSUFFICIENT_LIQUIDITY_HISTORY"]),
    ]
    assert result.schema == pl.Schema(
        {
            "instrument_id": pl.String,
            "as_of": pl.Date,
            "eligible": pl.Boolean,
            "reason_codes": pl.List(pl.String),
        }
    )


def test_build_fail_closes_each_row_when_as_of_is_not_a_trading_day(
    tmp_path: Path,
) -> None:
    """Treating a natural-calendar date as open would fabricate a historical pool."""
    repository, snapshot_id = _universe_fixture(tmp_path)

    result = UniverseBuilder(SnapshotResearchRepository(repository)).build(
        snapshot_id,
        date(2024, 4, 6),
        UniverseRules(min_listing_days=3),
    )

    assert result["eligible"].to_list() == [False] * len(_IDS)
    assert result["reason_codes"].to_list() == [["AS_OF_NOT_TRADING_DAY"]] * len(_IDS)


@pytest.mark.parametrize(
    "rules",
    [
        {"min_listing_days": -1},
        {"allowed_boards": frozenset()},
        {"min_avg_amount_20d": -0.1},
        {"min_avg_amount_20d": float("inf")},
    ],
)
def test_rules_reject_invalid_configuration(rules: dict[str, object]) -> None:
    """Permitting invalid thresholds or board sets makes eligibility ambiguous."""
    with pytest.raises(ValueError):
        UniverseRules(**rules)  # type: ignore[arg-type]


def test_snapshot_repository_reads_instruments_and_calendar_from_the_snapshot(
    tmp_path: Path,
) -> None:
    """Substituting any unbound dataset version would change this selected evidence."""
    repository, snapshot_id = _universe_fixture(tmp_path)
    research = SnapshotResearchRepository(repository)

    instruments = research.instruments(snapshot_id).collect()
    calendar = research.trade_calendar(snapshot_id, _TRADING_DAYS[0], _AS_OF).collect()

    assert instruments["instrument_id"].to_list() == list(_IDS.values())
    assert calendar["trade_date"].to_list() == _TRADING_DAYS


def _universe_fixture(
    tmp_path: Path,
) -> tuple[FixtureSnapshotRepository, SnapshotId]:
    base = point_in_time_fixture(tmp_path)
    instruments = _write_dataset(
        tmp_path,
        "universe-instruments",
        DatasetKind.INSTRUMENT,
        [_instrument_row(identifier, key) for key, identifier in _IDS.items()],
    )
    calendar = _write_dataset(
        tmp_path,
        "universe-calendar",
        DatasetKind.TRADE_CALENDAR,
        [_calendar_row(day) for day in _TRADING_DAYS],
    )
    statuses = _write_dataset(
        tmp_path,
        "universe-status",
        DatasetKind.SECURITY_STATUS,
        [
            _status_row(identifier, key, _AS_OF)
            for key, identifier in _IDS.items()
            if key != "missing_status"
        ]
        + [_status_row(_IDS["missing_status"], "missing_status", _TRADING_DAYS[-2])],
    )
    bars = _write_dataset(
        tmp_path,
        "universe-bars",
        DatasetKind.DAILY_BAR,
        [
            _bar_row(
                identifier,
                day,
                None
                if key == "missing_amount" and day == _TRADING_DAYS[-1]
                else 50.0
                if key == "illiquid"
                else 200.0,
            )
            for key, identifier in _IDS.items()
            for day in _TRADING_DAYS
            if not (key == "missing_history" and day == _TRADING_DAYS[-1])
        ]
        + [_bar_row(_IDS["illiquid"], day, 1_000.0) for day in _FUTURE_DAYS],
    )
    with_instruments = base.repository.bind_dataset(
        base.early_snapshot_id, DatasetKind.INSTRUMENT, instruments
    )
    with_calendar = base.repository.bind_dataset(
        with_instruments, DatasetKind.TRADE_CALENDAR, calendar
    )
    with_status = base.repository.bind_dataset(
        with_calendar, DatasetKind.SECURITY_STATUS, statuses
    )
    snapshot_id = base.repository.bind_dataset(with_status, DatasetKind.DAILY_BAR, bars)
    return base.repository, snapshot_id


def _instrument_row(identifier: str, key: str) -> dict[str, object]:
    list_date = _TRADING_DAYS[0]
    delist_date: date | None = None
    if key == "not_listed":
        list_date = _AS_OF + timedelta(days=1)
    elif key == "delisted":
        delist_date = _TRADING_DAYS[-2]
    elif key == "new":
        list_date = _TRADING_DAYS[-2]
    return {
        "instrument_id": identifier,
        "exchange": "SSE",
        "board": "MAIN",
        "name": key,
        "instrument_type": "EQUITY",
        "listing_status": "LISTED",
        "list_date": list_date,
        "delist_date": delist_date,
        **_audit(),
    }


def _calendar_row(day: date) -> dict[str, object]:
    return {"trade_date": day, "is_trading_day": True, **_audit()}


def _status_row(identifier: str, key: str, day: date) -> dict[str, object]:
    return {
        "instrument_id": identifier,
        "trade_date": day,
        "is_listed": key != "status_delisted",
        "is_suspended": key == "suspended",
        "is_risk_warning": key in {"not_listed", "st"},
        "board": "OTHER" if key == "board" else "MAIN",
        "price_limit_rule_id": "main",
        "tradable_reason": "normal",
        **_audit(datetime(2024, 4, 6, tzinfo=UTC) if key == "late_status" else None),
    }


def _bar_row(identifier: str, day: date, amount: float | None) -> dict[str, object]:
    return {
        "instrument_id": identifier,
        "trade_date": day,
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.0,
        "preclose": 10.0,
        "volume": 100,
        "amount": amount,
        "adjustment_flag": "none",
        "turnover": 1.0,
        "pct_change": 0.0,
        "pe_ttm": 10.0,
        "pb_mrq": 1.0,
        "ps_ttm": 2.0,
        "pcf_ncf_ttm": 3.0,
        **_audit(
            datetime(2024, 4, 6, tzinfo=UTC)
            if identifier == _IDS["late_bar"] and day == _TRADING_DAYS[-1]
            else None
        ),
    }


def _audit(available_at: datetime | None = None) -> dict[str, object]:
    return {
        "source": "fixture",
        "source_version": "v1",
        "available_at": available_at or datetime(2024, 4, 5, tzinfo=UTC),
        "availability_source": "announcement",
        "pit_usable": True,
        "ingested_at": datetime(2024, 4, 5, tzinfo=UTC),
    }
