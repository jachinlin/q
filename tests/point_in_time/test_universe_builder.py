"""Truth-table coverage for audited, snapshot-bound historical universes."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from quant_core.data.repository import SnapshotResearchRepository
from quant_core.domain.enums import Board, DatasetKind
from quant_core.domain.identifiers import InstrumentId, SnapshotId
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
        (
            _IDS["missing_history"],
            _AS_OF,
            False,
            ["MISSING_LIQUIDITY_OBSERVATIONS"],
        ),
        (_IDS["missing_amount"], _AS_OF, False, ["LIQUIDITY_AMOUNT_MISSING"]),
        (_IDS["status_delisted"], _AS_OF, False, ["DELISTED"]),
        (_IDS["late_status"], _AS_OF, False, ["STATUS_MISSING"]),
        (_IDS["late_bar"], _AS_OF, False, ["MISSING_LIQUIDITY_OBSERVATIONS"]),
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


def test_liquidity_distinguishes_short_calendar_from_missing_window_bars(
    tmp_path: Path,
) -> None:
    """A missing bar must not be hidden behind the distinct short-calendar code."""
    repository, snapshot_id = _universe_fixture(tmp_path)
    frames = _frames(repository, snapshot_id)
    short_calendar = frames.calendar.tail(19)
    alternate = _AlternateRepository(
        frames.instruments,
        short_calendar,
        frames.statuses,
        frames.bars,
    )

    result = UniverseBuilder(alternate).build(
        snapshot_id, _AS_OF, UniverseRules(min_listing_days=3, min_avg_amount_20d=100.0)
    )

    assert result.filter(pl.col("instrument_id") == _IDS["eligible"]).get_column(
        "reason_codes"
    ).to_list() == [["INSUFFICIENT_LIQUIDITY_HISTORY"]]
    assert result.filter(pl.col("instrument_id") == _IDS["missing_history"]).get_column(
        "reason_codes"
    ).to_list() == [["INSUFFICIENT_LIQUIDITY_HISTORY"]]


def test_liquidity_uses_only_the_most_recent_twenty_open_days(tmp_path: Path) -> None:
    """Including older low-amount days would falsely exclude this otherwise liquid stock."""
    repository, snapshot_id = _universe_fixture(tmp_path)
    frames = _frames(repository, snapshot_id)
    earlier_days = [date(2024, 3, day) for day in range(4, 9)]
    older_calendar = pl.DataFrame([_calendar_row(day) for day in earlier_days])
    older_bars = pl.DataFrame(
        [_bar_row(_IDS["eligible"], day, 0.0) for day in earlier_days]
    )
    alternate = _AlternateRepository(
        frames.instruments.filter(pl.col("instrument_id") == _IDS["eligible"]),
        pl.concat([older_calendar, frames.calendar]),
        frames.statuses.filter(pl.col("instrument_id") == _IDS["eligible"]),
        pl.concat(
            [
                older_bars,
                frames.bars.filter(pl.col("instrument_id") == _IDS["eligible"]),
            ]
        ),
    )

    result = UniverseBuilder(alternate).build(
        snapshot_id, _AS_OF, UniverseRules(min_listing_days=3, min_avg_amount_20d=180.0)
    )

    assert result.rows() == [(_IDS["eligible"], _AS_OF, True, [])]


def test_non_stock_instruments_remain_auditable_but_ineligible(tmp_path: Path) -> None:
    """Allowing any non-STOCK type would contaminate the equity factor universe."""
    repository, snapshot_id = _universe_fixture(tmp_path)
    frames = _frames(repository, snapshot_id)
    identifiers = list(_IDS.values())[:5]
    types = ["STOCK", "ETF", "INDEX", "CONVERTIBLE_BOND", "UNKNOWN"]
    instruments = frames.instruments.filter(pl.col("instrument_id").is_in(identifiers))
    instruments = instruments.with_columns(pl.Series("instrument_type", types))
    alternate = _AlternateRepository(
        instruments,
        frames.calendar,
        frames.statuses.filter(pl.col("instrument_id").is_in(identifiers)),
        frames.bars.filter(pl.col("instrument_id").is_in(identifiers)),
    )

    result = UniverseBuilder(alternate).build(
        snapshot_id, _AS_OF, UniverseRules(min_listing_days=3)
    )

    assert result.select("instrument_id", "reason_codes").rows() == [
        (identifiers[0], ["NOT_LISTED_YET", "RISK_WARNING"]),
        (identifiers[1], ["INSTRUMENT_TYPE_NOT_ALLOWED", "DELISTED"]),
        (identifiers[2], ["INSTRUMENT_TYPE_NOT_ALLOWED", "INSUFFICIENT_LISTING_DAYS"]),
        (identifiers[3], ["INSTRUMENT_TYPE_NOT_ALLOWED", "RISK_WARNING"]),
        (identifiers[4], ["INSTRUMENT_TYPE_NOT_ALLOWED", "SUSPENDED"]),
    ]


def test_rules_normalize_mutable_boards_and_numeric_threshold() -> None:
    """Retaining a caller-owned board set or integer threshold breaks immutability."""
    boards = {Board.MAIN}
    rules = UniverseRules(allowed_boards=boards, min_avg_amount_20d=100)
    boards.clear()

    assert rules.allowed_boards == frozenset({Board.MAIN})
    assert isinstance(rules.allowed_boards, frozenset)
    assert rules.min_avg_amount_20d == 100.0
    with pytest.raises(AttributeError):
        rules.allowed_boards.add(Board.STAR)  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "rules",
    [
        {"min_listing_days": 1.5},
        {"min_listing_days": True},
        {"exclude_st": 1},
        {"exclude_suspended": "false"},
        {"min_avg_amount_20d": True},
        {"min_avg_amount_20d": "100"},
        {"allowed_boards": {Board.MAIN, "UNKNOWN"}},
    ],
)
def test_rules_reject_non_exact_runtime_values(rules: dict[str, object]) -> None:
    """Truthiness, fractions, and unknown boards must not silently alter a universe."""
    with pytest.raises(ValueError):
        UniverseRules(**rules)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("duplicate_instrument", "duplicate instrument_id"),
        ("duplicate_calendar", "duplicate trade_date"),
        ("duplicate_status", "duplicate status primary key"),
        ("foreign_status", "unexpected status observation"),
        ("wrong_date_status", "unexpected status observation"),
        ("duplicate_bar", "duplicate bar primary key"),
        ("foreign_bar", "unexpected bar observation"),
        ("wrong_date_bar", "unexpected bar observation"),
    ],
)
def test_builder_fail_closes_invalid_alternate_repository_inputs(
    tmp_path: Path, kind: str, message: str
) -> None:
    """Last-write-wins or ignored foreign evidence would make an audit non-reproducible."""
    repository, snapshot_id = _universe_fixture(tmp_path)
    frames = _frames(repository, snapshot_id)
    alternate = _corrupt_alternate(frames, kind)

    with pytest.raises(ValueError, match="UNIVERSE_INPUT_INVALID: " + message):
        UniverseBuilder(alternate).build(
            snapshot_id,
            _AS_OF,
            UniverseRules(min_listing_days=3, min_avg_amount_20d=100.0),
        )


def test_non_trading_and_empty_universes_are_stably_sorted(tmp_path: Path) -> None:
    """Input order must not affect fail-closed output, including an empty snapshot."""
    repository, snapshot_id = _universe_fixture(tmp_path)
    frames = _frames(repository, snapshot_id)
    reversed_instruments = frames.instruments.sort("instrument_id", descending=True)
    non_trading = UniverseBuilder(
        _AlternateRepository(
            reversed_instruments, frames.calendar, frames.statuses, frames.bars
        )
    ).build(snapshot_id, date(2024, 4, 6), UniverseRules())
    empty = UniverseBuilder(
        _AlternateRepository(
            frames.instruments.head(0), frames.calendar, frames.statuses, frames.bars
        )
    ).build(snapshot_id, _AS_OF, UniverseRules())

    assert non_trading["instrument_id"].to_list() == sorted(_IDS.values())
    assert empty.schema == pl.Schema(
        {
            "instrument_id": pl.String,
            "as_of": pl.Date,
            "eligible": pl.Boolean,
            "reason_codes": pl.List(pl.String),
        }
    )
    assert empty.is_empty()


def test_reversed_calendar_produces_the_same_historical_universe(
    tmp_path: Path,
) -> None:
    """Repository row order must not change the selected 20-day evidence window."""
    repository, snapshot_id = _universe_fixture(tmp_path)
    frames = _frames(repository, snapshot_id)
    earlier_days = [date(2024, 3, day) for day in range(4, 9)]
    calendar = pl.concat(
        [pl.DataFrame([_calendar_row(day) for day in earlier_days]), frames.calendar]
    )
    bars = pl.concat(
        [
            pl.DataFrame(
                [_bar_row(_IDS["eligible"], day, 0.0) for day in earlier_days]
            ),
            frames.bars.filter(pl.col("instrument_id") == _IDS["eligible"]),
        ]
    )
    instruments = frames.instruments.filter(pl.col("instrument_id") == _IDS["eligible"])
    statuses = frames.statuses.filter(pl.col("instrument_id") == _IDS["eligible"])
    forward = UniverseBuilder(
        _AlternateRepository(instruments, calendar, statuses, bars)
    ).build(
        snapshot_id, _AS_OF, UniverseRules(min_listing_days=3, min_avg_amount_20d=180.0)
    )
    reversed_ = UniverseBuilder(
        _AlternateRepository(instruments, calendar.reverse(), statuses, bars)
    ).build(
        snapshot_id, _AS_OF, UniverseRules(min_listing_days=3, min_avg_amount_20d=180.0)
    )

    assert reversed_.rows() == forward.rows()


@pytest.mark.parametrize(
    ("kind", "message"),
    [
        ("future", "unexpected trade_date"),
        ("null", "invalid trade_date"),
        ("string", "invalid trade_date"),
    ],
)
def test_builder_fail_closes_invalid_alternate_calendar_rows(
    tmp_path: Path, kind: str, message: str
) -> None:
    """Out-of-request or malformed calendar dates cannot silently redefine history."""
    repository, snapshot_id = _universe_fixture(tmp_path)
    frames = _frames(repository, snapshot_id)
    calendar = frames.calendar
    if kind == "future":
        calendar = pl.concat(
            [calendar, pl.DataFrame([_calendar_row(_AS_OF + timedelta(days=1))])]
        )
    elif kind == "null":
        calendar = pl.concat(
            [
                calendar,
                calendar.head(1).with_columns(
                    pl.lit(None).cast(pl.Date).alias("trade_date")
                ),
            ]
        )
    elif kind == "string":
        calendar = calendar.with_columns(pl.col("trade_date").cast(pl.String))
    else:
        raise AssertionError(f"unknown calendar corruption: {kind}")

    with pytest.raises(ValueError, match="UNIVERSE_INPUT_INVALID: " + message):
        UniverseBuilder(
            _AlternateRepository(
                frames.instruments,
                calendar,
                frames.statuses,
                frames.bars,
                ignore_calendar_bounds=True,
            )
        ).build(snapshot_id, _AS_OF, UniverseRules())


class _Frames:
    def __init__(
        self,
        instruments: pl.DataFrame,
        calendar: pl.DataFrame,
        statuses: pl.DataFrame,
        bars: pl.DataFrame,
    ) -> None:
        self.instruments = instruments
        self.calendar = calendar
        self.statuses = statuses
        self.bars = bars


class _AlternateRepository:
    """Complete in-memory frames for boundary validation tests."""

    def __init__(
        self,
        instruments: pl.DataFrame,
        calendar: pl.DataFrame,
        statuses: pl.DataFrame,
        bars: pl.DataFrame,
        *,
        ignore_bar_bounds: bool = False,
        ignore_calendar_bounds: bool = False,
    ) -> None:
        self._frames = _Frames(instruments, calendar, statuses, bars)
        self._ignore_bar_bounds = ignore_bar_bounds
        self._ignore_calendar_bounds = ignore_calendar_bounds

    def instruments(self, snapshot_id: SnapshotId) -> pl.LazyFrame:
        return self._frames.instruments.lazy()

    def trade_calendar(
        self, snapshot_id: SnapshotId, start: date, end: date
    ) -> pl.LazyFrame:
        frame = self._frames.calendar
        if not self._ignore_calendar_bounds:
            frame = frame.filter(pl.col("trade_date").is_between(start, end))
        return frame.lazy()

    def security_status(
        self,
        snapshot_id: SnapshotId,
        as_of: date,
        instruments: list[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        return self._frames.statuses.lazy()

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: list[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        frame = self._frames.bars
        if not self._ignore_bar_bounds:
            frame = frame.filter(pl.col("trade_date").is_between(start, end))
        return frame.lazy()


def _frames(repository: FixtureSnapshotRepository, snapshot_id: SnapshotId) -> _Frames:
    research = SnapshotResearchRepository(repository)
    instruments = research.instruments(snapshot_id).collect()
    identifiers = [InstrumentId.parse(value) for value in instruments["instrument_id"]]
    return _Frames(
        instruments,
        research.trade_calendar(snapshot_id, date.min, _AS_OF).collect(),
        research.security_status(snapshot_id, _AS_OF).collect(),
        research.bars(snapshot_id, identifiers, date.min, _AS_OF).collect(),
    )


def _corrupt_alternate(frames: _Frames, kind: str) -> _AlternateRepository:
    instruments = frames.instruments
    calendar = frames.calendar
    statuses = frames.statuses
    bars = frames.bars
    if kind == "duplicate_instrument":
        instruments = pl.concat([instruments, instruments.head(1)])
    elif kind == "duplicate_calendar":
        calendar = pl.concat([calendar, calendar.head(1)])
    elif kind == "duplicate_status":
        statuses = pl.concat([statuses, statuses.head(1)])
    elif kind == "foreign_status":
        statuses = pl.concat(
            [
                statuses,
                statuses.head(1).with_columns(
                    pl.lit("SSE:699999").alias("instrument_id")
                ),
            ]
        )
    elif kind == "wrong_date_status":
        statuses = pl.concat(
            [
                statuses,
                statuses.head(1).with_columns(
                    pl.lit(_AS_OF - timedelta(days=1)).cast(pl.Date).alias("trade_date")
                ),
            ]
        )
    elif kind == "duplicate_bar":
        bars = pl.concat([bars, bars.head(1)])
    elif kind == "foreign_bar":
        bars = pl.concat(
            [
                bars,
                bars.head(1).with_columns(pl.lit("SSE:699999").alias("instrument_id")),
            ]
        )
    elif kind == "wrong_date_bar":
        bars = pl.concat(
            [
                bars,
                bars.head(1).with_columns(
                    pl.lit(_AS_OF + timedelta(days=1)).cast(pl.Date).alias("trade_date")
                ),
            ]
        )
    else:
        raise AssertionError(f"unknown corruption: {kind}")
    return _AlternateRepository(
        instruments,
        calendar,
        statuses,
        bars,
        ignore_bar_bounds=kind == "wrong_date_bar",
    )


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
        "instrument_type": "STOCK",
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
