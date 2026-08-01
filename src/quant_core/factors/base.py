"""Immutable public contracts for versioned point-in-time factors."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from math import isfinite
from types import MappingProxyType
from typing import Protocol, cast

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

from quant_core.data.contracts import JsonScalar, JsonValue, canonical_json_bytes
from quant_core.data.schemas import PolarsDataType
from quant_core.domain.identifiers import SnapshotId

type FrozenJsonValue = (
    JsonScalar | tuple["FrozenJsonValue", ...] | Mapping[str, "FrozenJsonValue"]
)

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

_FACTOR_OUTPUT_COLUMNS: dict[str, PolarsDataType] = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "factor_id": pl.String,
    "factor_version": pl.String,
    "value": pl.Float64,
    "available_at": pl.Datetime("us", "UTC"),
    "is_valid": pl.Boolean,
}
FACTOR_OUTPUT_SCHEMA = pl.Schema(_FACTOR_OUTPUT_COLUMNS)


@dataclass(frozen=True, slots=True)
class FactorSpec:
    """Stable logical identity and computation controls for one factor version."""

    factor_id: str
    version: str
    frequency: str
    lookback_sessions: int
    dependencies: tuple[str, ...]
    direction: int
    parameters: Mapping[str, JsonValue]

    def __post_init__(self) -> None:
        _validate_identifier(self.factor_id, "factor_id")
        _validate_identifier(self.version, "version")
        _validate_identifier(self.frequency, "frequency")
        if type(self.lookback_sessions) is not int or self.lookback_sessions < 0:
            raise ValueError("lookback_sessions must be a nonnegative integer")
        if type(self.direction) is not int or self.direction not in {-1, 1}:
            raise ValueError("direction must be -1 or +1")
        if not isinstance(self.dependencies, tuple):
            raise TypeError("dependencies must be a tuple")
        dependencies = tuple(
            canonical_factor_ref(dependency) for dependency in self.dependencies
        )
        if len(set(dependencies)) != len(dependencies):
            raise ValueError("duplicate factor dependency")
        if not isinstance(self.parameters, Mapping):
            raise TypeError("parameters must be a mapping")
        canonical_json_bytes(cast(JsonValue, self.parameters))
        frozen = _freeze_json(self.parameters)
        if not isinstance(frozen, Mapping):
            raise TypeError("parameters must be a mapping")
        object.__setattr__(self, "dependencies", dependencies)
        object.__setattr__(self, "parameters", cast(Mapping[str, JsonValue], frozen))

    @property
    def canonical_ref(self) -> str:
        """Return the unambiguous ``factor_id@version`` logical reference."""
        return f"{self.factor_id}@{self.version}"


@dataclass(frozen=True, slots=True)
class FactorContext:
    """Exact immutable point-in-time scope supplied to a factor computation."""

    snapshot_id: SnapshotId
    universe_hash: str
    start: date
    end: date

    def __post_init__(self) -> None:
        if not isinstance(self.snapshot_id, SnapshotId):
            raise TypeError("snapshot_id must be a SnapshotId")
        validate_sha256(self.universe_hash, "universe_hash")
        if type(self.start) is not date:
            raise TypeError("start must be a date")
        if type(self.end) is not date:
            raise TypeError("end must be a date")
        if self.start > self.end:
            raise ValueError("start must not follow end")


@dataclass(frozen=True, slots=True)
class FactorArtifact:
    """One verified immutable feature cache artifact and its PIT binding."""

    factor_ref: str
    cache_key: str
    content_hash: str
    row_count: int
    snapshot_id: SnapshotId
    universe_hash: str
    start: date
    end: date
    table: pa.Table

    def __post_init__(self) -> None:
        canonical_factor_ref(self.factor_ref)
        validate_sha256(self.cache_key, "cache_key")
        validate_sha256(self.content_hash, "content_hash")
        validate_sha256(self.universe_hash, "universe_hash")
        if type(self.row_count) is not int or self.row_count < 0:
            raise ValueError("row_count must be a nonnegative integer")
        if not isinstance(self.table, pa.Table):
            raise TypeError("artifact table must be a pyarrow Table")
        table = _owned_read_only_table(self.table)
        if table.num_rows != self.row_count:
            raise ValueError("artifact table row count does not match metadata")
        frame = cast(pl.DataFrame, pl.from_arrow(table))
        if frame.schema != FACTOR_OUTPUT_SCHEMA:
            raise ValueError("artifact table schema is invalid")
        if factor_table_content_hash(table) != self.content_hash:
            raise ValueError("artifact table content hash does not match metadata")
        if not isinstance(self.snapshot_id, SnapshotId):
            raise TypeError("snapshot_id must be a SnapshotId")
        if type(self.start) is not date or type(self.end) is not date:
            raise TypeError("artifact start and end must be dates")
        if self.start > self.end:
            raise ValueError("artifact start must not follow end")
        object.__setattr__(self, "table", table)

    def lazy_frame(self) -> pl.LazyFrame:
        """Return a lazy view over this artifact's owned immutable Arrow content."""
        return cast(pl.DataFrame, pl.from_arrow(self.table)).lazy()


class Factor(Protocol):
    """A versioned factor implementation with injected data dependencies."""

    @property
    def spec(self) -> FactorSpec:
        """Return this implementation's immutable logical contract."""

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """Compute exact-schema observations for the supplied PIT scope."""


def canonical_factor_ref(value: str) -> str:
    """Validate and return an explicit ``factor_id@version`` reference."""
    if not isinstance(value, str):
        raise TypeError("factor reference must be a string")
    factor_id, separator, version = value.partition("@")
    if separator != "@" or "@" in version:
        raise ValueError("dependency must use factor_id@version")
    _validate_identifier(factor_id, "factor_id")
    _validate_identifier(version, "version")
    return f"{factor_id}@{version}"


def validate_sha256(value: str, field: str) -> str:
    """Validate a lowercase hexadecimal SHA-256 digest."""
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field} must be a 64-character lowercase SHA-256 hash")
    return value


def factor_table_content_hash(table: pa.Table) -> str:
    """Hash factor content after canonical Arrow chunk normalization."""
    return hashlib.sha256(_factor_table_ipc_bytes(table)).hexdigest()


def _factor_table_ipc_bytes(table: pa.Table) -> bytes:
    combined = table.combine_chunks()
    # Arrow does not define bytes beneath null validity bits.  Parquet is free to
    # rewrite those invisible bytes, so replace just those physical values with a
    # fixed Arrow scalar while retaining the original null bitmap.  This stays in
    # Arrow instead of allocating one Python object per research-scale table cell.
    canonical = pa.table(
        [
            _canonical_null_buffers(combined.column(index).combine_chunks(), field)
            for index, field in enumerate(combined.schema)
        ],
        schema=combined.schema,
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, canonical.schema) as writer:
        writer.write_table(canonical)
    return cast(bytes, sink.getvalue().to_pybytes())


def _canonical_null_buffers(array: pa.Array, field: pa.Field) -> pa.Array:
    if array.null_count == 0:
        return array
    filled = pc.fill_null(array, _null_fill_scalar(field.type))
    return pa.Array.from_buffers(
        field.type,
        len(array),
        [array.buffers()[0], *filled.buffers()[1:]],
        null_count=array.null_count,
    )


def _null_fill_scalar(data_type: pa.DataType) -> pa.Scalar:
    if pa.types.is_string(data_type):
        value: object = ""
    elif pa.types.is_boolean(data_type):
        value = False
    elif pa.types.is_floating(data_type):
        value = 0.0
    elif pa.types.is_integer(data_type):
        value = 0
    elif pa.types.is_date(data_type):
        value = date(1970, 1, 1)
    elif pa.types.is_timestamp(data_type):
        value = datetime(1970, 1, 1, tzinfo=UTC)
    else:
        raise TypeError(f"unsupported factor hash dtype: {data_type}")
    return pa.scalar(value, type=data_type)


def _owned_read_only_table(table: pa.Table) -> pa.Table:
    payload = _factor_table_ipc_bytes(table)
    with pa.ipc.open_stream(pa.py_buffer(payload)) as reader:
        return reader.read_all()


def thaw_json(value: object) -> JsonValue:
    """Copy immutable factor parameters into canonical JSON containers."""
    if isinstance(value, Mapping):
        return {str(key): thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [thaw_json(item) for item in value]
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if type(value) is float and isfinite(value):
        return value
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _freeze_json(value: object) -> FrozenJsonValue:
    if isinstance(value, Mapping):
        frozen: dict[str, FrozenJsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("JSON mapping keys must be strings")
            frozen[key] = _freeze_json(item)
        return MappingProxyType(dict(sorted(frozen.items())))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if type(value) is float and isfinite(value):
        return value
    if type(value) is float:
        raise ValueError("value must be JSON serializable")
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _validate_identifier(value: str, field: str) -> None:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"{field} must be a nonempty identifier")
