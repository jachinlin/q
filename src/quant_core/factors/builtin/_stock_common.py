"""Shared deterministic helpers for stock factors."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date, datetime
from math import isfinite
from typing import Protocol

import polars as pl

from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors.base import FACTOR_OUTPUT_SCHEMA, FactorSpec


class BarRepository(Protocol):
    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame: ...


class TradeCalendarProvider(Protocol):
    def trade_calendar(
        self, snapshot_id: SnapshotId, start: date, end: date
    ) -> pl.LazyFrame: ...


def output_frame(
    spec: FactorSpec, rows: Iterable[tuple[date, str, float | None, datetime | None]]
) -> pl.LazyFrame:
    materialized = []
    for day, instrument, value, available_at in rows:
        valid = (
            value is not None and isfinite(value) and _known_availability(available_at)
        )
        materialized.append(
            {
                "trade_date": day,
                "instrument_id": instrument,
                "factor_id": spec.factor_id,
                "factor_version": spec.version,
                "value": value if valid else None,
                "available_at": available_at
                if _known_availability(available_at)
                else None,
                "is_valid": valid,
            }
        )
    return (
        pl.DataFrame(materialized, schema=FACTOR_OUTPUT_SCHEMA)
        .sort("trade_date", "instrument_id")
        .lazy()
    )


def trading_signal_dates(
    provider: TradeCalendarProvider, snapshot_id: SnapshotId, start: date, end: date
) -> tuple[date, ...]:
    frame = provider.trade_calendar(snapshot_id, start, end).collect()
    required = {"trade_date", "is_trading_day"}
    if not required.issubset(frame.columns):
        raise ValueError("trade calendar missing required columns")
    if frame["trade_date"].is_duplicated().any():
        raise ValueError("duplicate trade calendar date")
    calendar_dates = frame["trade_date"].to_list()
    if any(type(day) is not date or day < start or day > end for day in calendar_dates):
        raise ValueError("trade calendar date is outside requested range")
    days = frame.filter(pl.col("is_trading_day"))["trade_date"].to_list()
    return tuple(sorted(days))


def _known_availability(value: datetime | None) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() is not None
    )


def canonical_scope(instruments: Sequence[InstrumentId]) -> tuple[InstrumentId, ...]:
    scope = tuple(instruments)
    if any(not isinstance(item, InstrumentId) for item in scope):
        raise TypeError("instruments must contain InstrumentId values")
    identities = [item.canonical() for item in scope]
    if len(set(identities)) != len(identities):
        raise ValueError("instrument scope contains duplicates")
    return scope
