"""负责 Canonical Parquet 的不可变发布与增量合并。"""

from __future__ import annotations

import hashlib
import re
import uuid
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_research.data.contracts import (
    CanonicalBatch,
    JsonValue,
    canonical_json_bytes,
)
from quant_research.data.schemas import CANONICAL_SCHEMAS
from quant_research.data.storage import resolved_storage_root, validate_storage_path
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
    CanonicalDatasetSpec,
    CanonicalPartitionRecord,
    CanonicalPartitionSpec,
    MetadataRepository,
    RawHeadSnapshot,
)
from quant_research.logging import StructuredLogger


@dataclass(frozen=True, slots=True)
class CuratedResult:
    """描述一次 Canonical 分区发布的内容身份与物理结果。

    入参：
        datasets：构造对象所需的同名字段，约束见类型标注。
        frames：构造对象所需的同名字段，约束见类型标注。
    返回值：
        构造并返回 ``CuratedResult`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    datasets: Mapping[str, CanonicalDatasetRecord]
    frames: Mapping[DatasetKind, tuple[pl.LazyFrame, ...]]


@dataclass(frozen=True, slots=True)
class CanonicalPartitionReplacement:
    """描述要在同一事务中替换的完整 Canonical 分区。

    入参：
        partition_key：构造对象所需的同名字段，约束见类型标注。
        frame：待校验或转换的数据帧。
        input_hash：构造对象所需的同名字段，约束见类型标注。
        raw_input_count：构造对象所需的同名字段，约束见类型标注。
        rebuild_reason：构造对象所需的同名字段，约束见类型标注。
    返回值：
        构造并返回 ``CanonicalPartitionReplacement`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    partition_key: str
    frame: pl.DataFrame
    input_hash: str
    raw_input_count: int
    rebuild_reason: str


class CuratedPartitionStore:
    """发布并读取经过完整性校验的当前 Canonical 状态。

    入参：
        root：配置的可信存储根目录。
    返回值：
        构造并返回 ``CuratedPartitionStore`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def __init__(self, root: Path) -> None:
        self._root = resolved_storage_root(root)

    @property
    def root(self) -> Path:
        """返回该存储实例受信任的绝对根目录。

        入参：
            无。
        返回值：
            返回可信根目录（``Path``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return self._root

    @staticmethod
    def transform_hash(
        dataset: DatasetKind,
        *,
        mapper_hash: str,
        partitioning: str,
        reuse: str,
    ) -> str:
        """返回映射代码与目标 Canonical 契约的确定性身份。

        入参：
            dataset：目标 Canonical 数据集标识。
            mapper_hash：调用接口所需的同名参数，具体约束见类型标注。
            partitioning：调用接口所需的同名参数，具体约束见类型标注。
            reuse：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回哈希（``str``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        payload = cast(
            JsonValue,
            {
                "dataset": dataset.value,
                "mapper_hash": mapper_hash,
                "partitioning": partitioning,
                "reuse": reuse,
            },
        )
        digest = hashlib.sha256()
        digest.update(Path(__file__).read_bytes())
        digest.update(canonical_json_bytes(payload))
        return digest.hexdigest()

    def publish_replacements(
        self,
        dataset: DatasetKind,
        replacements: Sequence[CanonicalPartitionReplacement],
        *,
        removed_keys: Sequence[str],
        previous: CanonicalDatasetRecord | None,
        run_id: str,
        source: str,
        start: date,
        end: date,
        repository: MetadataRepository,
        expected_raw_heads: RawHeadSnapshot,
        logger: StructuredLogger | None = None,
    ) -> CanonicalDatasetRecord:
        """在同一事务中原子替换全部受影响 Canonical 分区。

        入参：
            dataset：目标 Canonical 数据集标识。
            replacements：调用接口所需的同名参数，具体约束见类型标注。
            removed_keys：调用接口所需的同名参数，具体约束见类型标注。
            previous：调用接口所需的同名参数，具体约束见类型标注。
            run_id：调用接口所需的同名参数，具体约束见类型标注。
            source：供应商标识。
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
            repository：调用接口所需的同名参数，具体约束见类型标注。
            expected_raw_heads：调用接口所需的同名参数，具体约束见类型标注。
            logger：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回校验并原子发布``replacements``后的``replacements``（``CanonicalDatasetRecord``）。
        异常：
            QuantError、ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        replacement_keys = [item.partition_key for item in replacements]
        if len(replacement_keys) != len(set(replacement_keys)):
            raise ValueError("canonical replacement partition keys must be unique")
        removed = set(removed_keys)
        if removed.intersection(replacement_keys):
            raise ValueError("a canonical partition cannot be replaced and removed")
        previous_by_key = (
            {item.partition_key: item for item in previous.partitions}
            if previous is not None
            else {}
        )
        specs_by_key: dict[str, CanonicalPartitionSpec] = {}
        for key, partition in previous_by_key.items():
            if key in removed or key in replacement_keys:
                continue
            self._validated_catalog_path(dataset, partition)
            specs_by_key[key] = CanonicalPartitionSpec(
                partition_key=key,
                content_hash=partition.content_hash,
                path=partition.path,
                schema_fingerprint=partition.schema_fingerprint,
                input_hash=partition.input_hash,
                row_count=partition.row_count,
            )
        events: list[dict[str, object]] = []
        definition = CANONICAL_SCHEMAS[dataset]
        for replacement in replacements:
            frame = replacement.frame.cast(definition.columns)
            duplicate_count = (
                frame.group_by(list(definition.primary_key))
                .len()
                .filter(pl.col("len") > 1)
                .height
                if frame.height
                else 0
            )
            if duplicate_count:
                raise QuantError(
                    ErrorDetail(
                        code="DATA_CANONICAL_PRIMARY_KEY_DUPLICATE",
                        severity=Severity.FATAL,
                        message="rebuilt canonical partition contains duplicate keys",
                        context={
                            "dataset": dataset.value,
                            "partition_key": replacement.partition_key,
                            "duplicate_keys": duplicate_count,
                        },
                        remediation="repair Raw precedence or canonical mapping",
                        retryable=False,
                    )
                )
            complete = frame.sort(list(definition.sort_key))
            spec, written = self._publish_partition(
                dataset, replacement.partition_key, complete
            )
            spec = CanonicalPartitionSpec(
                partition_key=spec.partition_key,
                content_hash=spec.content_hash,
                path=spec.path,
                schema_fingerprint=spec.schema_fingerprint,
                input_hash=replacement.input_hash,
                row_count=spec.row_count,
            )
            specs_by_key[replacement.partition_key] = spec
            old = previous_by_key.get(replacement.partition_key)
            events.append(
                {
                    "dataset": dataset.value,
                    "source": source,
                    "run_id": run_id,
                    "partition": {
                        **_CanonicalCurateSupport._partition_spec_context(spec),
                        "disposition": (
                            "rebuilt_file_written" if written else "rebuilt_file_reused"
                        ),
                        "rebuild_reason": replacement.rebuild_reason,
                        "raw_input_count": replacement.raw_input_count,
                        "previous_content_hash": (
                            None if old is None else old.content_hash
                        ),
                        "previous_row_count": None if old is None else old.row_count,
                    },
                }
            )
        specs = tuple(specs_by_key[key] for key in sorted(specs_by_key))
        if not specs:
            raise QuantError(
                ErrorDetail(
                    code="DATA_CURATE_INPUT_MISSING",
                    severity=Severity.SEVERE,
                    message=f"no canonical input remains for {dataset.value}",
                    context={"dataset": dataset.value},
                    remediation="localize valid Raw inputs before Curate",
                    retryable=False,
                )
            )
        published = repository.replace_canonical_dataset(
            CanonicalDatasetSpec(
                dataset=dataset,
                source=source,
                partitions=specs,
                start_date=start,
                end_date=end,
            ),
            updated_at=datetime.now(UTC),
            expected_raw_heads=expected_raw_heads,
        )
        self._cleanup_orphans(published.orphan_paths)
        if logger is not None:
            for event in events:
                event["pointer_changed"] = published.changed
                logger.emit(
                    "INFO",
                    "curate.partition_completed",
                    stage="CURATE",
                    context=event,
                )
            for key in sorted(removed):
                old = previous_by_key.get(key)
                logger.emit(
                    "INFO",
                    "curate.partition_completed",
                    stage="CURATE",
                    context={
                        "dataset": dataset.value,
                        "source": source,
                        "run_id": run_id,
                        "pointer_changed": published.changed,
                        "partition": {
                            "partition_key": key,
                            "disposition": "removed",
                            "rebuild_reason": "raw_input_removed",
                            "previous_content_hash": (
                                None if old is None else old.content_hash
                            ),
                            "previous_row_count": None
                            if old is None
                            else old.row_count,
                        },
                    },
                )
        self._cleanup_unreferenced(repository.list_canonical_datasets())
        return published.record

    def publish(
        self,
        batches: Iterable[CanonicalBatch],
        *,
        previous_datasets: Mapping[str, CanonicalDatasetRecord],
        run_id: str,
        source: str,
        start: date,
        end: date,
        repository: MetadataRepository,
        heartbeat: Callable[[], None] = lambda: None,
        logger: StructuredLogger | None = None,
    ) -> CuratedResult:
        """发布不可变内容，并在内容相同时复用已有对象。

        入参：
            batches：调用接口所需的同名参数，具体约束见类型标注。
            previous_datasets：调用接口所需的同名参数，具体约束见类型标注。
            run_id：调用接口所需的同名参数，具体约束见类型标注。
            source：供应商标识。
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
            repository：调用接口所需的同名参数，具体约束见类型标注。
            heartbeat：调用接口所需的同名参数，具体约束见类型标注。
            logger：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回校验并原子发布Canonical 数据后的``publish``（``CuratedResult``）。
        异常：
            QuantError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        grouped: dict[DatasetKind, dict[str, list[pl.DataFrame]]] = defaultdict(
            lambda: defaultdict(list)
        )
        seen_datasets: set[DatasetKind] = set()
        for batch in batches:
            heartbeat()
            seen_datasets.add(batch.dataset)
            for key, frame in self._partition(batch.dataset, batch.frame):
                grouped[batch.dataset][key].append(frame)
        usable_previous = dict(previous_datasets)
        for name, record in previous_datasets.items():
            try:
                for partition in record.partitions:
                    self._validated_catalog_path(record.dataset, partition)
            except FileNotFoundError:
                if record.dataset not in seen_datasets:
                    raise
                usable_previous.pop(name)
                if logger is not None:
                    logger.emit(
                        "WARNING",
                        "curate.dataset_rebuild_started",
                        stage="CURATE",
                        context={
                            "dataset": record.dataset.value,
                            "source": source,
                            "run_id": run_id,
                            "reason": "canonical_partition_missing",
                            "partitions": [
                                _CanonicalCurateSupport._partition_record_context(
                                    partition
                                )
                                for partition in record.partitions
                                if not partition.path.is_file()
                            ],
                        },
                    )
        new_partitions: dict[DatasetKind, dict[str, pl.DataFrame]] = {}
        for dataset, partitions in grouped.items():
            heartbeat()
            definition = CANONICAL_SCHEMAS[dataset]
            new_partitions[dataset] = {}
            for key, frames in partitions.items():
                heartbeat()
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
            record.dataset for record in usable_previous.values()
        }
        records: dict[str, CanonicalDatasetRecord] = {}
        frames_by_dataset: dict[DatasetKind, tuple[pl.LazyFrame, ...]] = {}
        for dataset in sorted(all_datasets, key=lambda item: item.value):
            heartbeat()
            previous = usable_previous.get(dataset.value)
            additions = new_partitions.get(dataset, {})
            if not additions and previous is not None:
                records[dataset.value] = previous
                frames_by_dataset[dataset] = self.scan_dataset(previous)
                if logger is not None:
                    logger.emit(
                        "INFO",
                        "curate.dataset_reused",
                        stage="CURATE",
                        context={
                            "dataset": dataset.value,
                            "source": source,
                            "run_id": run_id,
                            "dataset_content_hash": previous.content_hash,
                            "partitions": [
                                _CanonicalCurateSupport._partition_record_context(
                                    partition
                                )
                                for partition in previous.partitions
                            ],
                        },
                    )
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
            specs_by_key: dict[str, CanonicalPartitionSpec] = {
                key: CanonicalPartitionSpec(
                    partition_key=key,
                    content_hash=partition.content_hash,
                    path=partition.path,
                    schema_fingerprint=partition.schema_fingerprint,
                    input_hash=partition.input_hash,
                    row_count=partition.row_count,
                )
                for key, partition in previous_by_key.items()
                if key not in additions
            }
            partition_events: list[dict[str, object]] = []
            for key, added in additions.items():
                heartbeat()
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
                spec, written = self._publish_partition(dataset, key, complete)
                specs_by_key[key] = spec
                partition_events.append(
                    {
                        "dataset": dataset.value,
                        "source": source,
                        "run_id": run_id,
                        "partition": {
                            **_CanonicalCurateSupport._partition_spec_context(spec),
                            "disposition": (
                                "file_written" if written else "file_reused"
                            ),
                            "input_row_count": added.height,
                            "previous_content_hash": (
                                None if old is None else old.content_hash
                            ),
                            "previous_row_count": (
                                None if old is None else old.row_count
                            ),
                        },
                    }
                )
            specs = tuple(specs_by_key[key] for key in sorted(specs_by_key))
            dataset_start = (
                min(start, previous.start_date)
                if previous is not None and previous.start_date is not None
                else start
            )
            dataset_end = (
                max(end, previous.end_date)
                if previous is not None and previous.end_date is not None
                else end
            )
            published = repository.replace_canonical_dataset(
                CanonicalDatasetSpec(
                    dataset=dataset,
                    source=source,
                    partitions=specs,
                    start_date=dataset_start,
                    end_date=dataset_end,
                ),
                updated_at=datetime.now(UTC),
            )
            record = published.record
            records[dataset.value] = record
            self._cleanup_orphans(published.orphan_paths)
            frames_by_dataset[dataset] = tuple(
                pl.scan_parquet(spec.path) for spec in specs
            )
            if logger is not None:
                for partition_event in partition_events:
                    partition_event["pointer_changed"] = published.changed
                    logger.emit(
                        "INFO",
                        "curate.partition_completed",
                        stage="CURATE",
                        context=partition_event,
                    )
                logger.emit(
                    "INFO",
                    "curate.dataset_registered",
                    stage="CURATE",
                    context={
                        "dataset": dataset.value,
                        "source": source,
                        "run_id": run_id,
                        "dataset_content_hash": record.content_hash,
                        "changed": published.changed,
                        "partition_count": len(record.partitions),
                        "row_count": sum(
                            partition.row_count for partition in record.partitions
                        ),
                        "partitions": [
                            _CanonicalCurateSupport._partition_record_context(partition)
                            for partition in record.partitions
                        ],
                        "orphan_paths": [str(path) for path in published.orphan_paths],
                        "updated_at": record.updated_at.isoformat(),
                    },
                )
        heartbeat()
        self._cleanup_unreferenced(repository.list_canonical_datasets())
        return CuratedResult(records, frames_by_dataset)

    def read_dataset(self, record: CanonicalDatasetRecord) -> tuple[pl.DataFrame, ...]:
        """读取指定 Canonical 数据集的当前完整数据帧。

        入参：
            record：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回读取并校验数据集后的数据集（``tuple[pl.DataFrame, ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return tuple(
            self.read_partition(record.dataset, partition)
            for partition in record.partitions
        )

    @classmethod
    def partition_frame(
        cls, dataset: DatasetKind, frame: pl.DataFrame
    ) -> tuple[tuple[str, pl.DataFrame], ...]:
        """返回指定 Canonical 分区的惰性数据帧。

        入参：
            dataset：目标 Canonical 数据集标识。
            frame：待校验或转换的数据帧。
        返回值：
            返回``frame``（``tuple[tuple[str, pl.DataFrame], ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return tuple(cls._partition(dataset, frame))

    def scan_dataset(self, record: CanonicalDatasetRecord) -> tuple[pl.LazyFrame, ...]:
        """返回指定 Canonical 数据集全部当前分区的惰性扫描。

        入参：
            record：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回数据集（``tuple[pl.LazyFrame, ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return tuple(
            pl.scan_parquet(self._validated_catalog_path(record.dataset, partition))
            for partition in record.partitions
        )

    def verify_dataset(self, record: CanonicalDatasetRecord) -> None:
        """校验指定 Canonical 数据集的所有当前分区。

        入参：
            record：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            无。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        for partition in record.partitions:
            self.read_partition(record.dataset, partition)

    def read_partition(
        self, dataset: DatasetKind, partition: CanonicalPartitionRecord
    ) -> pl.DataFrame:
        """读取并校验一个 Canonical 分区。

        入参：
            dataset：目标 Canonical 数据集标识。
            partition：待读取、校验或映射的分区。
        返回值：
            返回读取并校验分区后的分区（``pl.DataFrame``）。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        path = self._validated_catalog_path(dataset, partition)
        table = pq.read_table(path)
        if (
            _CanonicalCurateSupport._content_hash(table) != partition.content_hash
            or _CanonicalCurateSupport._schema_fingerprint(table.schema)
            != partition.schema_fingerprint
            or table.num_rows != partition.row_count
        ):
            raise ValueError("curated partition fails catalog integrity checks")
        return cast(pl.DataFrame, pl.from_arrow(table))

    def _partition_key(
        self, dataset: DatasetKind, partition: CanonicalPartitionRecord
    ) -> str:
        self._validated_catalog_path(dataset, partition)
        return partition.path.parent.name

    def _validated_catalog_path(
        self, dataset: DatasetKind, partition: CanonicalPartitionRecord
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
            if not expected.is_file():
                raise FileNotFoundError(
                    f"curated catalog file is missing: {expected}"
                ) from error
            raise ValueError("curated catalog path is outside curated root") from error

    @staticmethod
    def _valid_partition_key(dataset: DatasetKind, key: str) -> bool:
        if dataset in {
            DatasetKind.DAILY_BAR,
            DatasetKind.DAILY_BASIC,
            DatasetKind.SECURITY_STATUS,
            DatasetKind.INDEX_BAR,
            DatasetKind.INDUSTRY_CLASSIFICATION,
        }:
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
        if dataset in {
            DatasetKind.DAILY_BAR,
            DatasetKind.DAILY_BASIC,
            DatasetKind.SECURITY_STATUS,
            DatasetKind.INDEX_BAR,
            DatasetKind.INDUSTRY_CLASSIFICATION,
        }:
            column, label = (
                ("as_of_date", "year")
                if dataset is DatasetKind.INDUSTRY_CLASSIFICATION
                else ("trade_date", "year")
            )
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
    ) -> tuple[CanonicalPartitionSpec, bool]:
        """Publish one content-addressed partition; return (spec, was_written)."""
        table = frame.to_arrow()
        directory = self._root / f"dataset={dataset.value}" / partition_key
        validate_storage_path(self._root, directory)
        directory.mkdir(parents=True, exist_ok=True)
        validate_storage_path(self._root, directory)
        temporary = directory / f".{uuid.uuid4().hex}.parquet.tmp"
        written_new = False
        try:
            validate_storage_path(self._root, temporary)
            pq.write_table(table, temporary, compression="zstd")
            validate_storage_path(self._root, temporary, require_file=True)
            written = pq.read_table(temporary)
            round_trip = cast(pl.DataFrame, pl.from_arrow(written))
            if written.num_rows != table.num_rows or not round_trip.equals(frame):
                raise ValueError("written curated partition failed verification")
            content_hash = _CanonicalCurateSupport._content_hash(written)
            schema_fingerprint = _CanonicalCurateSupport._schema_fingerprint(
                written.schema
            )
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
                    written_new = True
                    validate_storage_path(self._root, path, require_file=True)
                except FileExistsError:
                    self._verify_existing(
                        path, content_hash, schema_fingerprint, written.num_rows
                    )
        finally:
            temporary.unlink(missing_ok=True)
        return (
            CanonicalPartitionSpec(
                partition_key=partition_key,
                content_hash=content_hash,
                path=path,
                schema_fingerprint=schema_fingerprint,
                input_hash="0" * 64,
                row_count=table.num_rows,
            ),
            written_new,
        )

    def _cleanup_orphans(self, paths: Sequence[Path]) -> None:
        for path in paths:
            try:
                managed_path = validate_storage_path(self._root, path)
                managed_path.unlink(missing_ok=True)
            except (OSError, ValueError):
                # Windows can keep a Parquet file open while a LazyFrame is alive.
                # A later Curate pass will make another cleanup attempt.
                continue

    def _cleanup_unreferenced(self, records: Iterable[CanonicalDatasetRecord]) -> None:
        referenced = {
            partition.path.resolve()
            for record in records
            for partition in record.partitions
        }
        for path in self._root.rglob("*.parquet"):
            try:
                resolved = path.resolve(strict=True)
            except OSError:
                continue
            if resolved in referenced:
                continue
            self._cleanup_orphans((resolved,))

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
            _CanonicalCurateSupport._content_hash(existing) != content_hash
            or _CanonicalCurateSupport._schema_fingerprint(existing.schema)
            != schema_fingerprint
            or existing.num_rows != row_count
        ):
            raise ValueError("curated content-addressed path is corrupt")


class _CanonicalCurateSupport:
    """集中承载 Canonical 发布流程内部的上下文与哈希计算逻辑。"""

    @staticmethod
    def _partition_record_context(
        partition: CanonicalPartitionRecord,
    ) -> dict[str, object]:
        return {
            "partition_key": partition.partition_key,
            "content_hash": partition.content_hash,
            "schema_fingerprint": partition.schema_fingerprint,
            "row_count": partition.row_count,
            "path": str(partition.path),
        }

    @staticmethod
    def _partition_spec_context(partition: CanonicalPartitionSpec) -> dict[str, object]:
        return {
            "partition_key": partition.partition_key,
            "content_hash": partition.content_hash,
            "schema_fingerprint": partition.schema_fingerprint,
            "row_count": partition.row_count,
            "path": str(partition.path),
        }

    @staticmethod
    def _content_hash(table: pa.Table) -> str:
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()

    @staticmethod
    def _schema_fingerprint(schema: pa.Schema) -> str:
        return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()
