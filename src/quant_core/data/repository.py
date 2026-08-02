"""Snapshot-bound, point-in-time research reads over canonical Parquet data."""

from __future__ import annotations

import hashlib
import io
import shutil
import stat
import tempfile
import threading
import weakref
from collections.abc import Buffer, Callable, Sequence
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Never, Protocol, cast
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.data.safe_files import open_verified_file
from quant_core.data.schemas import CANONICAL_SCHEMAS, CanonicalSchema
from quant_core.domain.enums import DatasetKind, Severity, SnapshotStatus
from quant_core.domain.identifiers import DatasetVersionId, InstrumentId, SnapshotId
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.repositories import (
    DatasetPartitionRecord,
    DatasetVersionRecord,
    SnapshotRecord,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_PARTITION_FILE_BYTES = 8 * 1024 * 1024 * 1024
_MAX_DATASET_FILE_BYTES = 64 * 1024 * 1024 * 1024
_MAX_ROW_GROUP_BYTES = 512 * 1024 * 1024
_COPY_CHUNK_BYTES = 64 * 1024


class ResearchDataRepository(Protocol):
    """Read research data from one immutable snapshot at a time."""

    def instruments(self, snapshot_id: SnapshotId) -> pl.LazyFrame: ...

    def trade_calendar(
        self, snapshot_id: SnapshotId, start: date, end: date
    ) -> pl.LazyFrame: ...

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame: ...

    def corporate_actions_as_of(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId] | None,
        as_of: date,
    ) -> pl.LazyFrame: ...

    def financials_as_of(
        self,
        snapshot_id: SnapshotId,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame: ...

    def security_status(
        self,
        snapshot_id: SnapshotId,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame: ...


class SnapshotCatalog(Protocol):
    """The immutable catalog queries needed to resolve snapshot partitions."""

    def get_snapshot(self, identifier: SnapshotId) -> SnapshotRecord: ...

    def get_dataset_version(
        self, identifier: DatasetVersionId
    ) -> DatasetVersionRecord: ...


class SnapshotDatasetMissing(QuantError):
    """A requested canonical dataset is absent from an immutable snapshot."""

    def __init__(self, snapshot_id: SnapshotId, dataset: DatasetKind) -> None:
        super().__init__(
            ErrorDetail(
                code="SNAPSHOT_DATASET_MISSING",
                severity=Severity.FATAL,
                message="snapshot does not contain the requested dataset",
                context={"dataset": dataset.value, "snapshot_id": str(snapshot_id)},
                remediation="select a snapshot published with the required dataset",
                retryable=False,
            )
        )


def verify_published_dataset(
    catalog: SnapshotCatalog,
    snapshot_id: SnapshotId,
    dataset: DatasetKind,
) -> DatasetVersionRecord:
    """Read-verify every physical partition bound to one published dataset."""
    record = _published_dataset_record(catalog, snapshot_id, dataset)
    verified_bytes = 0
    for partition in record.partitions:
        remaining = _MAX_DATASET_FILE_BYTES - verified_bytes
        if remaining < 0:
            raise ValueError("published dataset exceeds the configured size limit")
        verified_bytes += _verify_owned_partition(
            partition.path,
            partition,
            trusted_root=partition.path.absolute().parent,
            max_bytes=min(_MAX_PARTITION_FILE_BYTES, remaining),
        )
    return record


class SnapshotResearchRepository:
    """Resolve every Parquet input through one explicit immutable snapshot."""

    def __init__(self, catalog: SnapshotCatalog) -> None:
        self._catalog = catalog
        self._partition_leases = _SnapshotPartitionLeasePool()

    def instruments(self, snapshot_id: SnapshotId) -> pl.LazyFrame:
        """Return every canonical instrument bound to ``snapshot_id``."""
        return self._read(snapshot_id, DatasetKind.INSTRUMENT, "TRUE", [])

    def trade_calendar(
        self, snapshot_id: SnapshotId, start: date, end: date
    ) -> pl.LazyFrame:
        """Return the inclusive canonical calendar range bound to ``snapshot_id``."""
        if start > end:
            raise ValueError("start must not follow end")
        return self._read(
            snapshot_id,
            DatasetKind.TRADE_CALENDAR,
            "trade_date >= ? AND trade_date <= ?",
            [start, end],
        )

    def bars(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """Return inclusive daily bars as one catalog-bound lazy Parquet scan."""
        if start > end:
            raise ValueError("start must not follow end")
        record = self._dataset_record(snapshot_id, DatasetKind.DAILY_BAR)
        definition = CANONICAL_SCHEMAS[DatasetKind.DAILY_BAR]
        instrument_ids = [instrument.canonical() for instrument in instruments]
        scope = (
            pl.col("instrument_id").is_in(instrument_ids)
            if instrument_ids
            else pl.lit(False)
        )
        leases = tuple(
            self._partition_leases.acquire(partition) for partition in record.partitions
        )
        return (
            pl.scan_parquet([lease.path for lease in leases])
            .select(list(definition.columns))
            .filter(scope & pl.col("trade_date").is_between(start, end, closed="both"))
            .cast(definition.columns)
            .sort(list(definition.sort_key))
            .map_batches(_retain_partition_leases(leases))
        )

    def financials_as_of(
        self,
        snapshot_id: SnapshotId,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """Return the latest usable revision known by Shanghai's end of ``as_of``."""
        predicates, parameters = _instrument_predicate(instruments)
        field_predicate, field_parameters = _value_predicate("metric", field_ids)
        predicates.extend(field_predicate)
        predicates.extend(
            (
                "pit_usable = TRUE",
                "available_at IS NOT NULL",
                "available_at <= ?",
            )
        )
        parameters.extend(field_parameters)
        parameters.append(_shanghai_close_utc(as_of))
        definition = CANONICAL_SCHEMAS[DatasetKind.FINANCIAL_OBSERVATION]
        columns = _columns(definition)
        order = _order(definition)
        query = (
            "SELECT "
            + columns
            + " FROM (SELECT "
            + columns
            + ", ROW_NUMBER() OVER (PARTITION BY instrument_id, report_period, metric "
            "ORDER BY available_at DESC, revision DESC) AS _pit_rank FROM data WHERE "
            + " AND ".join(predicates)
            + ") WHERE _pit_rank = 1 ORDER BY "
            + order
        )
        return self._read_query(
            snapshot_id,
            DatasetKind.FINANCIAL_OBSERVATION,
            query,
            parameters,
        )

    def corporate_actions_as_of(
        self,
        snapshot_id: SnapshotId,
        instruments: Sequence[InstrumentId] | None,
        as_of: date,
    ) -> pl.LazyFrame:
        """Return only PIT-usable corporate actions known by Shanghai close."""
        predicates, parameters = _instrument_predicate(instruments)
        predicates.extend(
            (
                "pit_usable = TRUE",
                "available_at IS NOT NULL",
                "available_at <= ?",
                "ex_date IS NOT NULL",
                "ex_date <= ?",
            )
        )
        parameters.extend((_shanghai_close_utc(as_of), as_of))
        return self._read(
            snapshot_id,
            DatasetKind.CORPORATE_ACTION,
            " AND ".join(predicates),
            parameters,
        )

    def security_status(
        self,
        snapshot_id: SnapshotId,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """Return the canonical status observations recorded for ``as_of``."""
        predicates, parameters = _instrument_predicate(instruments)
        predicates.extend(
            (
                "trade_date = ?",
                "pit_usable = TRUE",
                "available_at IS NOT NULL",
                "available_at <= ?",
            )
        )
        parameters.extend((as_of, _shanghai_close_utc(as_of)))
        return self._read(
            snapshot_id,
            DatasetKind.SECURITY_STATUS,
            " AND ".join(predicates),
            parameters,
        )

    def _read(
        self,
        snapshot_id: SnapshotId,
        dataset: DatasetKind,
        predicate: str,
        parameters: Sequence[object],
    ) -> pl.LazyFrame:
        definition = CANONICAL_SCHEMAS[dataset]
        query = (
            "SELECT "
            + _columns(definition)
            + " FROM data WHERE "
            + predicate
            + " ORDER BY "
            + _order(definition)
        )
        return self._read_query(snapshot_id, dataset, query, parameters)

    def _read_query(
        self,
        snapshot_id: SnapshotId,
        dataset: DatasetKind,
        query: str,
        parameters: Sequence[object],
    ) -> pl.LazyFrame:
        record = self._dataset_record(snapshot_id, dataset)
        leases = tuple(
            self._partition_leases.acquire(partition) for partition in record.partitions
        )
        source_query, source_parameters = _parquet_sources(
            [lease.path for lease in leases]
        )
        connection = duckdb.connect(":memory:")
        try:
            result = connection.execute(
                "WITH data AS (" + source_query + ") " + query,
                [*source_parameters, *parameters],
            ).to_arrow_table()
        finally:
            connection.close()
        frame = cast(pl.DataFrame, pl.from_arrow(result))
        return frame.cast(CANONICAL_SCHEMAS[dataset].columns).lazy()

    def _dataset_record(
        self, snapshot_id: SnapshotId, dataset: DatasetKind
    ) -> DatasetVersionRecord:
        return _published_dataset_record(self._catalog, snapshot_id, dataset)


def _published_dataset_record(
    catalog: SnapshotCatalog,
    snapshot_id: SnapshotId,
    dataset: DatasetKind,
) -> DatasetVersionRecord:
    snapshot = catalog.get_snapshot(snapshot_id)
    if snapshot.status is not SnapshotStatus.PUBLISHED or snapshot.published_at is None:
        _raise_snapshot_not_published(snapshot_id)
    version_id = snapshot.dataset_versions.get(dataset.value)
    if version_id is None:
        raise SnapshotDatasetMissing(snapshot_id, dataset)
    record = catalog.get_dataset_version(version_id)
    if record.dataset is not dataset or record.status != SnapshotStatus.PUBLISHED.value:
        _raise_catalog_error(snapshot_id, dataset, "dataset version is not published")
    _validate_catalog_partition_identities(snapshot_id, dataset, record)
    return record


def _instrument_predicate(
    instruments: Sequence[InstrumentId] | None,
) -> tuple[list[str], list[object]]:
    if instruments is None:
        return [], []
    return _value_predicate("instrument_id", [item.canonical() for item in instruments])


def _value_predicate(
    column: str, values: Sequence[str]
) -> tuple[list[str], list[object]]:
    _validate_column(column)
    if not values:
        return ["FALSE"], []
    return [column + " IN (" + ", ".join("?" for _ in values) + ")"], list(values)


def _parquet_sources(paths: Sequence[Path]) -> tuple[str, list[object]]:
    if not paths:
        raise ValueError("dataset version must contain at least one partition")
    return (
        " UNION ALL ".join("SELECT * FROM read_parquet(?)" for _ in paths),
        [path.as_posix() for path in paths],
    )


def _columns(definition: CanonicalSchema) -> str:
    return ", ".join(_quoted(column) for column in definition.columns)


def _order(definition: CanonicalSchema) -> str:
    return ", ".join(_quoted(column) for column in definition.sort_key)


def _quoted(column: str) -> str:
    _validate_column(column)
    return f'"{column}"'


def _validate_column(column: str) -> None:
    if not any(
        column in definition.columns for definition in CANONICAL_SCHEMAS.values()
    ):
        raise ValueError("column is not in the canonical schema allowlist")


def _shanghai_close_utc(value: date) -> datetime:
    return datetime.combine(value, time.max, tzinfo=_SHANGHAI).astimezone(UTC)


def _validate_catalog_partition_identities(
    snapshot_id: SnapshotId,
    dataset: DatasetKind,
    record: DatasetVersionRecord,
) -> None:
    paths: set[Path] = set()
    content_hashes: set[str] = set()
    for partition in record.partitions:
        path = partition.path.resolve()
        if path in paths or partition.content_hash in content_hashes:
            _raise_catalog_error(
                snapshot_id,
                dataset,
                "dataset version contains duplicate partition identity",
            )
        paths.add(path)
        content_hashes.add(partition.content_hash)


class _SnapshotPartitionLease:
    """Own one immutable scan input for the lifetime of a lazy daily-bar plan."""

    def __init__(self, partition: DatasetPartitionRecord) -> None:
        self._directory = tempfile.TemporaryDirectory(
            prefix=".snapshot-scan-", dir=str(partition.path.absolute().parent)
        )
        self.path = Path(self._directory.name) / f"{partition.content_hash}.parquet"
        try:
            try:
                with (
                    open_verified_file(
                        partition.path,
                        trusted_root=partition.path.absolute().parent,
                        max_bytes=_MAX_PARTITION_FILE_BYTES,
                    ) as source,
                    self.path.open("xb") as target,
                ):
                    shutil.copyfileobj(source.file, target, length=_COPY_CHUNK_BYTES)
            except ValueError as error:
                if "link or reparse point" in str(error):
                    raise
                raise ValueError("published partition is unavailable") from error
            _verify_owned_partition(
                self.path,
                partition,
                trusted_root=self.path.parent,
                max_bytes=_MAX_PARTITION_FILE_BYTES,
            )
            self.path.chmod(stat.S_IREAD)
        except BaseException:
            self._directory.cleanup()
            raise


class _SnapshotPartitionLeasePool:
    """Single-flight weak pool of verified content-addressed scan copies."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._leases: weakref.WeakValueDictionary[
            tuple[str, str, int], _SnapshotPartitionLease
        ] = weakref.WeakValueDictionary()

    def acquire(self, partition: DatasetPartitionRecord) -> _SnapshotPartitionLease:
        key = (
            partition.content_hash,
            partition.schema_fingerprint,
            partition.row_count,
        )
        with self._lock:
            lease = self._leases.get(key)
            if lease is None:
                lease = _SnapshotPartitionLease(partition)
                self._leases[key] = lease
            return lease


def _retain_partition_leases(
    leases: tuple[_SnapshotPartitionLease, ...],
) -> Callable[[pl.DataFrame], pl.DataFrame]:
    """Keep verified owned files alive for every execution of the lazy plan."""

    def retain(frame: pl.DataFrame) -> pl.DataFrame:
        if not leases:
            raise ValueError("daily-bar dataset must contain a partition")
        return frame

    return retain


def _verify_owned_partition(
    path: Path,
    partition: DatasetPartitionRecord,
    *,
    trusted_root: Path,
    max_bytes: int,
) -> int:
    """Bind copied bytes to all published logical catalog metadata."""
    message = "published partition fails catalog integrity checks"
    try:
        with open_verified_file(
            path, trusted_root=trusted_root, max_bytes=max_bytes
        ) as opened:
            parquet = pq.ParquetFile(opened.file)
            metadata = parquet.metadata
            schema = parquet.schema_arrow
            if metadata.num_rows != partition.row_count:
                raise ValueError(message)
            for index in range(metadata.num_row_groups):
                if metadata.row_group(index).total_byte_size > _MAX_ROW_GROUP_BYTES:
                    raise ValueError(
                        "published partition row group exceeds the configured size limit"
                    )
            content_hash = _parquet_content_hash(parquet, schema)
            file_size = opened.size
        schema_fingerprint = hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
    except ValueError as error:
        if "size limit" in str(error):
            raise
        raise ValueError(message) from error
    except Exception as error:
        raise ValueError(message) from error
    if (
        content_hash != partition.content_hash
        or schema_fingerprint != partition.schema_fingerprint
    ):
        raise ValueError(message)
    return file_size


def _parquet_content_hash(parquet: pq.ParquetFile, schema: pa.Schema) -> str:
    sink = _HashingSink()
    output = pa.PythonFile(sink, mode="w")
    try:
        with pa.ipc.new_stream(output, schema) as writer:
            for index in range(parquet.metadata.num_row_groups):
                writer.write_table(parquet.read_row_group(index))
    finally:
        output.close()
    return sink.hexdigest()


class _HashingSink(io.RawIOBase):
    """Minimal Arrow output stream that retains only the SHA-256 state."""

    def __init__(self) -> None:
        super().__init__()
        self._digest = hashlib.sha256()
        self._position = 0

    def writable(self) -> bool:
        return True

    def write(self, value: Buffer, /) -> int:
        size = memoryview(value).nbytes
        self._digest.update(value)
        self._position += size
        return size

    def tell(self) -> int:
        return self._position

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _raise_snapshot_not_published(snapshot_id: SnapshotId) -> Never:
    raise QuantError(
        ErrorDetail(
            code="SNAP_NOT_PUBLISHED",
            severity=Severity.FATAL,
            message="snapshot is not published",
            context={"snapshot_id": str(snapshot_id)},
            remediation="select a published immutable snapshot",
            retryable=False,
        )
    )


def _raise_catalog_error(
    snapshot_id: SnapshotId, dataset: DatasetKind, message: str
) -> Never:
    raise QuantError(
        ErrorDetail(
            code="SNAPSHOT_CATALOG_INVALID",
            severity=Severity.FATAL,
            message=message,
            context={"dataset": dataset.value, "snapshot_id": str(snapshot_id)},
            remediation="inspect the immutable snapshot catalog",
            retryable=False,
        )
    )
