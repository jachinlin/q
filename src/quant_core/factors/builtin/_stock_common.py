"""Shared deterministic helpers for stock factors."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime, time, timedelta
from math import isfinite
from typing import Protocol
from zoneinfo import ZoneInfo

import polars as pl

from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.factors.base import FACTOR_OUTPUT_SCHEMA, FactorSpec

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class BarRepository(Protocol):
    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame: ...


def output_frame(
    spec: FactorSpec, rows: Iterable[tuple[date, str, float | None, datetime | None]]
) -> pl.LazyFrame:
    materialized = [
        {
            "trade_date": day,
            "instrument_id": instrument,
            "factor_id": spec.factor_id,
            "factor_version": spec.version,
            "value": value if value is not None and isfinite(value) else None,
            "available_at": available_at or signal_close(day),
            "is_valid": value is not None and isfinite(value),
        }
        for day, instrument, value, available_at in rows
    ]
    return (
        pl.DataFrame(materialized, schema=FACTOR_OUTPUT_SCHEMA)
        .sort("trade_date", "instrument_id")
        .lazy()
    )


def signal_dates(start: date, end: date) -> tuple[date, ...]:
    return tuple(
        start + timedelta(days=index) for index in range((end - start).days + 1)
    )


def signal_close(value: date) -> datetime:
    return datetime.combine(value, time.max, _SHANGHAI).astimezone(UTC)


def canonical_scope(instruments: Sequence[InstrumentId]) -> tuple[InstrumentId, ...]:
    scope = tuple(instruments)
    if any(not isinstance(item, InstrumentId) for item in scope):
        raise TypeError("instruments must contain InstrumentId values")
    identities = [item.canonical() for item in scope]
    if len(set(identities)) != len(identities):
        raise ValueError("instrument scope contains duplicates")
    return scope
