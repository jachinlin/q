"""Snapshot-bound, point-in-time research reads over canonical Parquet data."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Never, Protocol, cast
from zoneinfo import ZoneInfo

import duckdb
import polars as pl

from quant_core.data.schemas import CANONICAL_SCHEMAS, CanonicalSchema
from quant_core.domain.enums import DatasetKind, Severity, SnapshotStatus
from quant_core.domain.identifiers import DatasetVersionId, InstrumentId, SnapshotId
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.repositories import DatasetVersionRecord, SnapshotRecord

_SHANGHAI = ZoneInfo("Asia/Shanghai")


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


class SnapshotResearchRepository:
    """Resolve every Parquet input through one explicit immutable snapshot."""

    def __init__(self, catalog: SnapshotCatalog) -> None:
        self._catalog = catalog

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
        return (
            pl.scan_parquet([partition.path for partition in record.partitions])
            .select(list(definition.columns))
            .filter(scope & pl.col("trade_date").is_between(start, end, closed="both"))
            .cast(definition.columns)
            .sort(list(definition.sort_key))
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
        source_query, source_parameters = _parquet_sources(record)
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
        snapshot = self._catalog.get_snapshot(snapshot_id)
        if (
            snapshot.status is not SnapshotStatus.PUBLISHED
            or snapshot.published_at is None
        ):
            _raise_snapshot_not_published(snapshot_id)
        version_id = snapshot.dataset_versions.get(dataset.value)
        if version_id is None:
            raise SnapshotDatasetMissing(snapshot_id, dataset)
        record = self._catalog.get_dataset_version(version_id)
        if (
            record.dataset is not dataset
            or record.status != SnapshotStatus.PUBLISHED.value
        ):
            _raise_catalog_error(
                snapshot_id, dataset, "dataset version is not published"
            )
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


def _parquet_sources(record: DatasetVersionRecord) -> tuple[str, list[object]]:
    if not record.partitions:
        raise ValueError("dataset version must contain at least one partition")
    return (
        " UNION ALL ".join("SELECT * FROM read_parquet(?)" for _ in record.partitions),
        [partition.path.as_posix() for partition in record.partitions],
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
