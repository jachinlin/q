"""Immutable canonical Parquet publication and incremental merging."""

from __future__ import annotations

import hashlib
import uuid
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import cast

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.data.contracts import CanonicalBatch
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.domain.enums import DatasetKind
from quant_core.domain.identifiers import DatasetVersionId
from quant_core.persistence.repositories import (
    DatasetPartitionSpec,
    DatasetVersionRecord,
    DatasetVersionSpec,
    MetadataRepository,
)


@dataclass(frozen=True, slots=True)
class CuratedResult:
    dataset_versions: Mapping[str, DatasetVersionId]
    frames: Mapping[DatasetKind, tuple[pl.DataFrame, ...]]


class CuratedPartitionStore:
    """Publish complete canonical versions as verified content-addressed files."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    def publish(
        self,
        batches: Sequence[CanonicalBatch],
        *,
        previous_versions: Mapping[str, DatasetVersionRecord],
        run_id: str,
        source: str,
        start: date,
        end: date,
        repository: MetadataRepository,
    ) -> CuratedResult:
        grouped: dict[DatasetKind, list[pl.DataFrame]] = defaultdict(list)
        for batch in batches:
            grouped[batch.dataset].append(batch.frame)
        all_datasets = set(grouped) | {
            record.dataset for record in previous_versions.values()
        }
        versions: dict[str, DatasetVersionId] = {}
        frames_by_dataset: dict[DatasetKind, tuple[pl.DataFrame, ...]] = {}
        for dataset in sorted(all_datasets, key=lambda item: item.value):
            previous = previous_versions.get(dataset.value)
            old_frames = list(self.read_version(previous)) if previous else []
            new_frames = grouped.get(dataset, [])
            if not new_frames and previous is not None:
                versions[dataset.value] = previous.id
                frames_by_dataset[dataset] = tuple(old_frames)
                continue
            definition = CANONICAL_SCHEMAS[dataset]
            combined = pl.concat([*old_frames, *new_frames], how="vertical")
            complete = (
                combined.unique(
                    subset=list(definition.primary_key),
                    keep="last",
                    maintain_order=True,
                )
                .sort(list(definition.sort_key))
                .cast(definition.columns)
            )
            partitions = tuple(self._partition(dataset, complete))
            specs = tuple(
                self._publish_partition(dataset, key, frame)
                for key, frame in partitions
            )
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
            frames_by_dataset[dataset] = tuple(frame for _, frame in partitions)
        return CuratedResult(versions, frames_by_dataset)

    def read_version(self, record: DatasetVersionRecord) -> tuple[pl.DataFrame, ...]:
        frames: list[pl.DataFrame] = []
        for partition in record.partitions:
            path = self._validated_catalog_path(partition.path)
            table = pq.read_table(path)
            if (
                _content_hash(table) != partition.content_hash
                or _schema_fingerprint(table.schema) != partition.schema_fingerprint
                or table.num_rows != partition.row_count
            ):
                raise ValueError("curated partition fails catalog integrity checks")
            frames.append(cast(pl.DataFrame, pl.from_arrow(table)))
        return tuple(frames)

    def _validated_catalog_path(self, path: Path) -> Path:
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(self._root)
        except (OSError, ValueError) as error:
            raise ValueError("curated catalog path is outside curated root") from error
        if path.absolute() != resolved or not resolved.is_file():
            raise ValueError("curated catalog path is outside curated root")
        return resolved

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
        if column is None or frame.is_empty():
            return (("all", frame),)
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
        directory.mkdir(parents=True, exist_ok=True)
        temporary = directory / f".{uuid.uuid4().hex}.parquet.tmp"
        try:
            pq.write_table(table, temporary, compression="zstd")
            written = pq.read_table(temporary)
            round_trip = cast(pl.DataFrame, pl.from_arrow(written))
            if written.num_rows != table.num_rows or not round_trip.equals(frame):
                raise ValueError("written curated partition failed verification")
            content_hash = _content_hash(written)
            schema_fingerprint = _schema_fingerprint(written.schema)
            path = directory / f"{content_hash}.parquet"
            if path.exists():
                self._verify_existing(
                    path, content_hash, schema_fingerprint, written.num_rows
                )
            else:
                try:
                    temporary.rename(path)
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

    @staticmethod
    def _verify_existing(
        path: Path, content_hash: str, schema_fingerprint: str, row_count: int
    ) -> None:
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
