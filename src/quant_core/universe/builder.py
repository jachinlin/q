"""Build one deterministic, audit-ready stock universe from a snapshot."""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from math import isfinite
from typing import cast
from zoneinfo import ZoneInfo

import polars as pl

from quant_core.data.repository import ResearchDataRepository
from quant_core.data.schemas import PolarsDataType
from quant_core.domain.enums import Board
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.universe.rules import UniverseRules

_OUTPUT_COLUMNS: dict[str, PolarsDataType] = {
    "instrument_id": pl.String,
    "as_of": pl.Date,
    "eligible": pl.Boolean,
    "reason_codes": cast(pl.DataType, pl.List(pl.String)),
}
_OUTPUT_SCHEMA = pl.Schema(_OUTPUT_COLUMNS)
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_REASON_PRIORITY = (
    "AS_OF_NOT_TRADING_DAY",
    "INSTRUMENT_HISTORY_MISSING",
    "NOT_LISTED_YET",
    "DELISTED",
    "INSUFFICIENT_LISTING_DAYS",
    "STATUS_MISSING",
    "RISK_WARNING",
    "SUSPENDED",
    "BOARD_NOT_ALLOWED",
    "INSUFFICIENT_LIQUIDITY_HISTORY",
    "LIQUIDITY_AMOUNT_MISSING",
    "INSUFFICIENT_AVERAGE_AMOUNT",
)


class UniverseBuilder:
    """Combine snapshot-bound evidence into stable per-instrument eligibility."""

    def __init__(self, repository: ResearchDataRepository) -> None:
        self._repository = repository

    def build(
        self, snapshot_id: SnapshotId, as_of: date, rules: UniverseRules
    ) -> pl.DataFrame:
        """Return one row per snapshot instrument, sorted by ``instrument_id``."""
        instruments = self._repository.instruments(snapshot_id).collect()
        calendar = self._repository.trade_calendar(
            snapshot_id, date.min, as_of
        ).collect()
        identifiers = _instrument_ids(instruments)
        if not _is_trading_day(calendar, as_of):
            return _all_closed(identifiers, as_of, "AS_OF_NOT_TRADING_DAY")

        statuses = _known_by_as_of(
            self._repository.security_status(snapshot_id, as_of), as_of
        ).collect()
        bars = (
            _known_by_as_of(
                self._repository.bars(snapshot_id, identifiers, date.min, as_of), as_of
            ).collect()
            if identifiers
            else pl.DataFrame()
        )
        trading_days = _trading_days(calendar, as_of)
        status_by_instrument = {
            row["instrument_id"]: row for row in statuses.to_dicts()
        }
        amounts_by_instrument = _amounts_by_instrument(bars)
        rows = [
            _eligibility_row(
                instrument,
                as_of,
                rules,
                trading_days,
                status_by_instrument.get(instrument["instrument_id"]),
                amounts_by_instrument.get(instrument["instrument_id"], {}),
            )
            for instrument in instruments.sort("instrument_id").to_dicts()
        ]
        return pl.DataFrame(rows, schema=_OUTPUT_SCHEMA, strict=False)


def _instrument_ids(instruments: pl.DataFrame) -> list[InstrumentId]:
    return [
        InstrumentId.parse(value) for value in instruments["instrument_id"].to_list()
    ]


def _is_trading_day(calendar: pl.DataFrame, as_of: date) -> bool:
    return bool(
        calendar.filter(
            (pl.col("trade_date") == as_of) & pl.col("is_trading_day")
        ).height
    )


def _trading_days(calendar: pl.DataFrame, as_of: date) -> list[date]:
    return calendar.filter((pl.col("trade_date") <= as_of) & pl.col("is_trading_day"))[
        "trade_date"
    ].to_list()


def _amounts_by_instrument(bars: pl.DataFrame) -> dict[str, dict[date, float | None]]:
    amounts: dict[str, dict[date, float | None]] = {}
    for row in bars.to_dicts():
        amounts.setdefault(row["instrument_id"], {})[row["trade_date"]] = row["amount"]
    return amounts


def _known_by_as_of(frame: pl.LazyFrame, as_of: date) -> pl.LazyFrame:
    return frame.filter(
        pl.col("pit_usable")
        & pl.col("available_at").is_not_null()
        & (pl.col("available_at") <= _shanghai_close_utc(as_of))
    )


def _shanghai_close_utc(value: date) -> datetime:
    return datetime.combine(value, time.max, tzinfo=_SHANGHAI).astimezone(UTC)


def _all_closed(
    identifiers: list[InstrumentId], as_of: date, reason: str
) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "instrument_id": identifier.canonical(),
                "as_of": as_of,
                "eligible": False,
                "reason_codes": [reason],
            }
            for identifier in identifiers
        ],
        schema=_OUTPUT_SCHEMA,
        strict=False,
    )


def _eligibility_row(
    instrument: dict[str, object],
    as_of: date,
    rules: UniverseRules,
    trading_days: list[date],
    status: dict[str, object] | None,
    amounts: dict[date, float | None],
) -> dict[str, object]:
    reasons: set[str] = set()
    list_date = instrument["list_date"]
    delist_date = instrument["delist_date"]
    if not isinstance(list_date, date):
        reasons.add("INSTRUMENT_HISTORY_MISSING")
    elif list_date > as_of:
        reasons.add("NOT_LISTED_YET")
    elif (
        sum(list_date <= day <= as_of for day in trading_days) < rules.min_listing_days
    ):
        reasons.add("INSUFFICIENT_LISTING_DAYS")
    if isinstance(delist_date, date) and delist_date <= as_of:
        reasons.add("DELISTED")
    if status is None:
        reasons.add("STATUS_MISSING")
    else:
        _add_status_reasons(reasons, status, rules, list_date, as_of)
    if rules.min_avg_amount_20d is not None:
        _add_liquidity_reasons(reasons, trading_days, amounts, rules.min_avg_amount_20d)
    ordered_reasons = [reason for reason in _REASON_PRIORITY if reason in reasons]
    return {
        "instrument_id": instrument["instrument_id"],
        "as_of": as_of,
        "eligible": not ordered_reasons,
        "reason_codes": ordered_reasons,
    }


def _add_status_reasons(
    reasons: set[str],
    status: dict[str, object],
    rules: UniverseRules,
    list_date: object,
    as_of: date,
) -> None:
    if status["is_listed"] is False:
        if isinstance(list_date, date) and list_date > as_of:
            reasons.add("NOT_LISTED_YET")
        elif isinstance(list_date, date):
            reasons.add("DELISTED")
    if rules.exclude_st and status["is_risk_warning"] is True:
        reasons.add("RISK_WARNING")
    if rules.exclude_suspended and status["is_suspended"] is True:
        reasons.add("SUSPENDED")
    try:
        board = Board(str(status["board"]))
    except ValueError:
        reasons.add("BOARD_NOT_ALLOWED")
    else:
        if board not in rules.allowed_boards:
            reasons.add("BOARD_NOT_ALLOWED")


def _add_liquidity_reasons(
    reasons: set[str],
    trading_days: list[date],
    amounts: dict[date, float | None],
    minimum: float,
) -> None:
    window = trading_days[-20:]
    if len(window) < 20:
        reasons.add("INSUFFICIENT_LIQUIDITY_HISTORY")
        return
    if any(day not in amounts for day in window):
        reasons.add("INSUFFICIENT_LIQUIDITY_HISTORY")
        return
    values = [amounts[day] for day in window]
    if any(value is None or not isfinite(value) for value in values):
        reasons.add("LIQUIDITY_AMOUNT_MISSING")
        return
    average = sum(value for value in values if value is not None) / len(values)
    if average < minimum:
        reasons.add("INSUFFICIENT_AVERAGE_AMOUNT")
