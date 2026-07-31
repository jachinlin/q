"""Immutable canonical Parquet publication and incremental merging."""

from __future__ import annotations

import hashlib
import re
import uuid
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import cast

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.data.contracts import CanonicalBatch
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.data.storage import resolved_storage_root, validate_storage_path
from quant_core.domain.enums import DatasetKind, Severity
from quant_core.domain.identifiers import DatasetVersionId
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.repositories import (
    DatasetPartitionRecord,
    DatasetPartitionSpec,
    DatasetVersionRecord,
    DatasetVersionSpec,
    MetadataRepository,
)


@dataclass(frozen=True, slots=True)
class CuratedResult:
    dataset_versions: Mapping[str, DatasetVersionId]
    frames: Mapping[DatasetKind, tuple[pl.LazyFrame, ...]]


class CuratedPartitionStore:
    """Publish complete canonical versions as verified content-addressed files."""

    def __init__(self, root: Path) -> None:
        self._root = resolved_storage_root(root)

    @property
    def root(self) -> Path:
        return self._root

    def publish(
        self,
        batches: Iterable[CanonicalBatch],
        *,
        previous_versions: Mapping[str, DatasetVersionRecord],
        run_id: str,
        source: str,
        start: date,
        end: date,
        repository: MetadataRepository,
    ) -> CuratedResult:
        grouped: dict[DatasetKind, dict[str, list[pl.DataFrame]]] = defaultdict(
            lambda: defaultdict(list)
        )
        seen_datasets: set[DatasetKind] = set()
        for batch in batches:
            seen_datasets.add(batch.dataset)
            for key, frame in self._partition(batch.dataset, batch.frame):
                grouped[batch.dataset][key].append(frame)
        new_partitions: dict[DatasetKind, dict[str, pl.DataFrame]] = {}
        for dataset, partitions in grouped.items():
            definition = CANONICAL_SCHEMAS[dataset]
            new_partitions[dataset] = {}
            for key, frames in partitions.items():
                added = pl.concat(frames, how="vertical").cast(definition.columns)
                duplicate_count = (
                    added.group_by(list(definition.primary_key))
                    .len()
                    .filter(pl.col("len") > 1)
                    .height
                )
                if duplicate_count:
                    raise QuantError(
                        ErrorDetail(
                            code="DATA_CANONICAL_PRIMARY_KEY_DUPLICATE",
                            severity=Severity.FATAL,
                            message=(
                                "new canonical batches contain duplicate primary keys"
                            ),
                            context={
                                "dataset": dataset.value,
                                "duplicate_keys": duplicate_count,
                            },
                            remediation="repair batch boundaries or canonical mapping",
                            retryable=False,
                        )
                    )
                new_partitions[dataset][key] = added
        all_datasets = seen_datasets | {
            record.dataset for record in previous_versions.values()
        }
        versions: dict[str, DatasetVersionId] = {}
        frames_by_dataset: dict[DatasetKind, tuple[pl.LazyFrame, ...]] = {}
        for dataset in sorted(all_datasets, key=lambda item: item.value):
            previous = previous_versions.get(dataset.value)
            additions = new_partitions.get(dataset, {})
            if not additions and previous is not None:
                versions[dataset.value] = previous.id
                frames_by_dataset[dataset] = self.scan_version(previous)
                continue
            definition = CANONICAL_SCHEMAS[dataset]
            previous_by_key = (
                {
                    self._partition_key(dataset, partition): partition
                    for partition in previous.partitions
                }
                if previous is not None
                else {}
            )
            specs_by_key: dict[str, DatasetPartitionSpec] = {
                key: DatasetPartitionSpec(
                    content_hash=partition.content_hash,
                    path=partition.path,
                    schema_fingerprint=partition.schema_fingerprint,
                    row_count=partition.row_count,
                )
                for key, partition in previous_by_key.items()
                if key not in additions
            }
            for key, added in additions.items():
                frames = [added]
                old = previous_by_key.get(key)
                if old is not None:
                    frames.insert(0, self.read_partition(dataset, old))
                complete = (
                    pl.concat(frames, how="vertical")
                    .unique(
                        subset=list(definition.primary_key),
                        keep="last",
                        maintain_order=True,
                    )
                    .sort(list(definition.sort_key))
                    .cast(definition.columns)
                )
                specs_by_key[key] = self._publish_partition(dataset, key, complete)
            specs = tuple(specs_by_key[key] for key in sorted(specs_by_key))
            version_start = (
                min(start, previous.start_date)
                if previous is not None and previous.start_date is not None
                else start
            )
            version_end = (
                max(end, previous.end_date)
                if previous is not None and previous.end_date is not None
                else end
            )
            version = repository.register_dataset_version(
                DatasetVersionSpec(
                    dataset=dataset,
                    source=source,
                    partitions=specs,
                    start_date=version_start,
                    end_date=version_end,
                    created_run_id=run_id,
                )
            )
            versions[dataset.value] = version.id
            frames_by_dataset[dataset] = tuple(
                pl.scan_parquet(spec.path) for spec in specs
            )
        return CuratedResult(versions, frames_by_dataset)

    def read_version(self, record: DatasetVersionRecord) -> tuple[pl.DataFrame, ...]:
        return tuple(
            self.read_partition(record.dataset, partition)
            for partition in record.partitions
        )

    def scan_version(self, record: DatasetVersionRecord) -> tuple[pl.LazyFrame, ...]:
        return tuple(
            pl.scan_parquet(self._validated_catalog_path(record.dataset, partition))
            for partition in record.partitions
        )

    def verify_version(self, record: DatasetVersionRecord) -> None:
        for partition in record.partitions:
            self.read_partition(record.dataset, partition)

    def read_partition(
        self, dataset: DatasetKind, partition: DatasetPartitionRecord
    ) -> pl.DataFrame:
        path = self._validated_catalog_path(dataset, partition)
        table = pq.read_table(path)
        if (
            _content_hash(table) != partition.content_hash
            or _schema_fingerprint(table.schema) != partition.schema_fingerprint
            or table.num_rows != partition.row_count
        ):
            raise ValueError("curated partition fails catalog integrity checks")
        return cast(pl.DataFrame, pl.from_arrow(table))

    def _partition_key(
        self, dataset: DatasetKind, partition: DatasetPartitionRecord
    ) -> str:
        self._validated_catalog_path(dataset, partition)
        return partition.path.parent.name

    def _validated_catalog_path(
        self, dataset: DatasetKind, partition: DatasetPartitionRecord
    ) -> Path:
        key = partition.path.parent.name
        if not self._valid_partition_key(dataset, key):
            raise ValueError("curated catalog path is outside curated root")
        expected = (
            self._root
            / f"dataset={dataset.value}"
            / key
            / f"{partition.content_hash}.parquet"
        )
        if partition.path.absolute() != expected:
            raise ValueError("curated catalog path is outside curated root")
        try:
            return validate_storage_path(self._root, expected, require_file=True)
        except ValueError as error:
            raise ValueError("curated catalog path is outside curated root") from error

    @staticmethod
    def _valid_partition_key(dataset: DatasetKind, key: str) -> bool:
        if dataset in {DatasetKind.DAILY_BAR, DatasetKind.SECURITY_STATUS}:
            return re.fullmatch(r"year=\d{4}", key) is not None
        if dataset is DatasetKind.FINANCIAL_OBSERVATION:
            return re.fullmatch(r"report_year=\d{4}", key) is not None
        return key == "all"

    @staticmethod
    def _partition(
        dataset: DatasetKind, frame: pl.DataFrame
    ) -> Sequence[tuple[str, pl.DataFrame]]:
        column = None
        label = "partition"
        if dataset in {DatasetKind.DAILY_BAR, DatasetKind.SECURITY_STATUS}:
            column, label = "trade_date", "year"
        elif dataset is DatasetKind.FINANCIAL_OBSERVATION:
            column, label = "report_period", "report_year"
        if column is None:
            return (("all", frame),)
        if frame.is_empty():
            return ((f"{label}=0000", frame),)
        years = sorted(set(frame.get_column(column).dt.year().to_list()))
        return tuple(
            (
                f"{label}={year}",
                frame.filter(pl.col(column).dt.year() == year),
            )
            for year in years
        )

    def _publish_partition(
        self, dataset: DatasetKind, partition_key: str, frame: pl.DataFrame
    ) -> DatasetPartitionSpec:
        table = frame.to_arrow()
        directory = self._root / f"dataset={dataset.value}" / partition_key
        validate_storage_path(self._root, directory)
        directory.mkdir(parents=True, exist_ok=True)
        validate_storage_path(self._root, directory)
        temporary = directory / f".{uuid.uuid4().hex}.parquet.tmp"
        try:
            validate_storage_path(self._root, temporary)
            pq.write_table(table, temporary, compression="zstd")
            validate_storage_path(self._root, temporary, require_file=True)
            written = pq.read_table(temporary)
            round_trip = cast(pl.DataFrame, pl.from_arrow(written))
            if written.num_rows != table.num_rows or not round_trip.equals(frame):
                raise ValueError("written curated partition failed verification")
            content_hash = _content_hash(written)
            schema_fingerprint = _schema_fingerprint(written.schema)
            path = directory / f"{content_hash}.parquet"
            validate_storage_path(self._root, path)
            if path.exists():
                self._verify_existing(
                    path, content_hash, schema_fingerprint, written.num_rows
                )
            else:
                try:
                    validate_storage_path(self._root, temporary, require_file=True)
                    validate_storage_path(self._root, path)
                    temporary.rename(path)
                    validate_storage_path(self._root, path, require_file=True)
                except FileExistsError:
                    self._verify_existing(
                        path, content_hash, schema_fingerprint, written.num_rows
                    )
        finally:
            temporary.unlink(missing_ok=True)
        return DatasetPartitionSpec(
            content_hash=content_hash,
            path=path,
            schema_fingerprint=schema_fingerprint,
            row_count=table.num_rows,
        )

    def _verify_existing(
        self,
        path: Path,
        content_hash: str,
        schema_fingerprint: str,
        row_count: int,
    ) -> None:
        validate_storage_path(self._root, path, require_file=True)
        existing = pq.read_table(path)
        if (
            _content_hash(existing) != content_hash
            or _schema_fingerprint(existing.schema) != schema_fingerprint
            or existing.num_rows != row_count
        ):
            raise ValueError("curated content-addressed path is corrupt")


def _content_hash(table: pa.Table) -> str:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def _schema_fingerprint(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
