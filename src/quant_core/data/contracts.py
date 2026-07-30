"""Vendor-neutral contracts at the source and canonical data boundaries."""

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Protocol

import polars as pl

from quant_core.domain.enums import DatasetKind
from quant_core.domain.identifiers import InstrumentId

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | list[JsonValue] | Mapping[str, JsonValue]


def canonical_json_bytes(value: JsonValue) -> bytes:
    """Serialize a JSON value in its stable UTF-8 representation."""
    try:
        serialized = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as error:
        raise ValueError("value must be JSON serializable") from error
    return serialized.encode("utf-8")


@dataclass(frozen=True, slots=True)
class ProviderCapabilities:
    """The source datasets a provider can supply faithfully."""

    daily_bars: bool
    trade_calendar: bool
    instruments: bool
    security_status: bool
    financials_with_announcement_date: bool
    corporate_actions: bool
    adjustment_factors: bool


@dataclass(frozen=True, slots=True)
class RawBatch:
    """A provider-neutral, reproducible batch retrieved from a source."""

    provider: str
    dataset: str
    request: Mapping[str, JsonValue]
    retrieved_at: datetime
    schema: tuple[str, ...]
    rows: Sequence[Mapping[str, JsonValue]]

    def __post_init__(self) -> None:
        """Reject timestamps or requests that cannot be reproduced."""
        if self.retrieved_at.tzinfo is None or self.retrieved_at.utcoffset() is None:
            raise ValueError("retrieved_at must be timezone-aware")
        canonical_json_bytes(self.request)


@dataclass(frozen=True, slots=True)
class PublishedPartition:
    """A visible immutable raw partition and its integrity metadata."""

    provider: str
    dataset: str
    request: Mapping[str, JsonValue]
    retrieved_at: datetime
    data_path: Path
    manifest_path: Path
    request_hash: str
    content_hash: str
    schema_fingerprint: str
    row_count: int


@dataclass(frozen=True, slots=True)
class CanonicalBatch:
    """A normalized canonical frame derived from published raw evidence."""

    dataset: DatasetKind
    frame: pl.DataFrame
    source_content_hashes: tuple[str, ...]


class SourceClient(Protocol):
    """Minimal acquisition interface shared by all source providers."""

    def fetch_daily_bars(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> Iterable[RawBatch]:
        """Fetch daily-bar batches for the requested canonical instruments."""


class CanonicalMapper(Protocol):
    """Maps a visible raw partition into canonical batches."""

    def normalize(self, raw_partition: PublishedPartition) -> Iterable[CanonicalBatch]:
        """Read and normalize the already-published raw partition."""
