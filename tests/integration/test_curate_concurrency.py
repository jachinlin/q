"""CURATE 数据集级并发与串行发布集成测试。"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pyarrow as pa  # type: ignore[import-untyped]

from quant_research.data.contracts import PublishedPartition, RawBatch
from quant_research.data.pipeline.curate import CuratedPartitionStore
from quant_research.data.pipeline.dataset import DataPipeline
from quant_research.data.quality.runner import QualityRunner
from quant_research.data.storage.partitions import RawPartitionStore
from quant_research.domain.enums import DatasetKind
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.repositories import (
    MetadataRepository,
    RawPartitionSpec,
)
from quant_research.infrastructure.tushare.client import _FIELDS
from quant_research.infrastructure.tushare.mapper import TushareMapper
from quant_research.infrastructure.tushare.routing import TUSHARE_ROUTES


class _ConcurrentMapper(TushareMapper):
    """要求两个数据集同时进入规范化阶段。"""

    def __init__(self) -> None:
        self._barrier = threading.Barrier(2, timeout=5)

    def normalize(
        self,
        raw_partition: PublishedPartition,
        raw_table: pa.Table,
    ) -> tuple[Any, ...]:
        self._barrier.wait()
        return super().normalize(raw_partition, raw_table)


class _RecordingCuratedStore(CuratedPartitionStore):
    """记录真实发布区是否发生重叠。"""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self._guard = threading.Lock()
        self._active = 0
        self.maximum_active = 0

    def publish_replacements(self, *args: Any, **kwargs: Any) -> Any:
        with self._guard:
            self._active += 1
            self.maximum_active = max(self.maximum_active, self._active)
        try:
            time.sleep(0.02)
            return super().publish_replacements(*args, **kwargs)
        finally:
            with self._guard:
                self._active -= 1


class _RecordingRawStore(RawPartitionStore):
    """记录 CURATE 对完整 Raw 内容的读取次数。"""

    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.read_calls = 0

    def read(
        self,
        partition: PublishedPartition,
        *,
        verify: bool = True,
    ) -> pa.Table:
        self.read_calls += 1
        return super().read(partition, verify=verify)


def _register_raw(
    raw_store: RawPartitionStore,
    repository: MetadataRepository,
    *,
    endpoint: str,
    row: dict[str, object],
    request_discriminator: str | None = None,
) -> None:
    fields = _FIELDS[endpoint]
    request = {"endpoint": endpoint, "fields": ",".join(fields)}
    if request_discriminator is not None:
        request["scope"] = request_discriminator
    published = raw_store.publish(
        RawBatch(
            source="tushare",
            endpoint=endpoint,
            request=request,
            retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
            schema=fields,
            rows=(dict.fromkeys(fields) | row,),
        )
    )
    repository.register_raw_partition(
        RawPartitionSpec(
            source=published.source,
            endpoint=published.endpoint,
            request=published.request,
            request_hash=published.request_hash,
            content_hash=published.content_hash,
            data_path=published.data_path,
            manifest_path=published.manifest_path,
            schema_fingerprint=published.schema_fingerprint,
            row_count=published.row_count,
            retrieved_at=published.retrieved_at,
        )
    )


def test_curate_builds_datasets_concurrently_and_publishes_serially(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    raw_store = _RecordingRawStore(tmp_path / "raw")
    curated_store = _RecordingCuratedStore(tmp_path / "canonical")
    _register_raw(
        raw_store,
        repository,
        endpoint="stock_basic",
        row={
            "ts_code": "600000.SH",
            "symbol": "600000",
            "name": "浦发银行",
            "market": "主板",
            "exchange": "SSE",
            "list_status": "L",
            "list_date": "19991110",
        },
    )
    _register_raw(
        raw_store,
        repository,
        endpoint="fund_basic",
        row={
            "ts_code": "510300.SH",
            "name": "沪深300ETF",
            "fund_type": "股票型",
            "list_date": "20120528",
            "market": "E",
            "status": "L",
        },
    )
    datasets = (DatasetKind.STOCK_MASTER, DatasetKind.FUND_MASTER)
    pipeline = DataPipeline(
        source=SimpleNamespace(provider="tushare"),  # type: ignore[arg-type]
        mapper=_ConcurrentMapper(),
        calendar=object(),  # type: ignore[arg-type]
        raw_store=raw_store,
        curated_store=curated_store,
        repository=repository,
        quality_runner=QualityRunner(),
        routes=TUSHARE_ROUTES,
        max_concurrent_curate_datasets=2,
    )

    first = pipeline._curate_datasets(
        datasets,
        windows={dataset: (None, None) for dataset in datasets},
        observer=None,
    )

    assert tuple(result.dataset for result in first) == datasets
    assert curated_store.maximum_active == 1
    assert all(result.raw_inputs_read == 1 for result in first)
    records = repository.list_canonical_datasets()
    assert tuple(record.dataset for record in records) == tuple(
        sorted(datasets, key=lambda dataset: dataset.value)
    )
    assert all(partition.path.is_file() for record in records for partition in record.partitions)
    assert len(repository.catalog_state().catalog_hash) == 64
    assert raw_store.read_calls == 2

    retry = DataPipeline(
        source=SimpleNamespace(provider="tushare"),  # type: ignore[arg-type]
        mapper=TushareMapper(),
        calendar=object(),  # type: ignore[arg-type]
        raw_store=raw_store,
        curated_store=curated_store,
        repository=repository,
        quality_runner=QualityRunner(),
        routes=TUSHARE_ROUTES,
        max_concurrent_curate_datasets=1,
    )._curate_datasets(
        datasets,
        windows={dataset: (None, None) for dataset in datasets},
        observer=None,
    )

    assert tuple(result.content_hash for result in retry) == tuple(
        result.content_hash for result in first
    )
    assert all(result.rebuilt_partitions == 0 for result in retry)
    assert all(result.raw_inputs_read == 0 for result in retry)
    assert raw_store.read_calls == 2
    engine.dispose()


def test_curate_content_is_identical_at_one_four_and_eight_workers(
    tmp_path: Path,
) -> None:
    def build(concurrency: int) -> tuple[str, ...]:
        root = tmp_path / str(concurrency)
        database = root / "state" / "quant.db"
        upgrade_database(database)
        engine = create_sqlite_engine(database)
        repository = MetadataRepository(engine)
        raw_store = RawPartitionStore(root / "raw")
        _register_raw(
            raw_store,
            repository,
            endpoint="stock_basic",
            row={
                "ts_code": "600000.SH",
                "symbol": "600000",
                "name": "浦发银行",
                "market": "主板",
                "exchange": "SSE",
                "list_status": "L",
                "list_date": "19991110",
            },
        )
        _register_raw(
            raw_store,
            repository,
            endpoint="fund_basic",
            row={
                "ts_code": "510300.SH",
                "name": "沪深300ETF",
                "fund_type": "股票型",
                "list_date": "20120528",
                "market": "E",
                "status": "L",
            },
        )
        datasets = (DatasetKind.STOCK_MASTER, DatasetKind.FUND_MASTER)
        results = DataPipeline(
            source=SimpleNamespace(provider="tushare"),  # type: ignore[arg-type]
            mapper=TushareMapper(),
            calendar=object(),  # type: ignore[arg-type]
            raw_store=raw_store,
            curated_store=CuratedPartitionStore(root / "canonical"),
            repository=repository,
            quality_runner=QualityRunner(),
            routes=TUSHARE_ROUTES,
            max_concurrent_curate_datasets=concurrency,
        )._curate_datasets(
            datasets,
            windows={dataset: (None, None) for dataset in datasets},
            observer=None,
        )
        hashes = tuple(result.content_hash for result in results)
        engine.dispose()
        return hashes

    assert build(1) == build(4) == build(8)


def test_curate_raw_progress_is_throttled_and_finishes_with_exact_count(
    tmp_path: Path,
) -> None:
    class Observer:
        def __init__(self) -> None:
            self.boundaries: list[tuple[str, dict[str, Any]]] = []

        def stage_started(self, stage: str, total: int) -> None:
            del stage, total

        def dataset_completed(
            self,
            stage: str,
            dataset: DatasetKind,
            completed: int,
            total: int,
            details: Any,
        ) -> None:
            del stage, dataset, completed, total, details

        def boundary(
            self,
            stage: str,
            dataset: DatasetKind,
            kind: str,
            details: Any,
        ) -> None:
            del stage, dataset
            self.boundaries.append((kind, dict(details)))

        def is_cancelled(self) -> bool:
            return False

    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    raw_store = RawPartitionStore(tmp_path / "raw")
    for index in range(3):
        _register_raw(
            raw_store,
            repository,
            endpoint="stock_basic",
            request_discriminator=str(index),
            row={
                "ts_code": f"60000{index}.SH",
                "symbol": f"60000{index}",
                "name": f"测试{index}",
                "market": "主板",
                "exchange": "SSE",
                "list_status": "L",
                "list_date": "19991110",
            },
        )
    observer = Observer()
    pipeline = DataPipeline(
        source=SimpleNamespace(provider="tushare"),  # type: ignore[arg-type]
        mapper=TushareMapper(),
        calendar=object(),  # type: ignore[arg-type]
        raw_store=raw_store,
        curated_store=CuratedPartitionStore(tmp_path / "canonical"),
        repository=repository,
        quality_runner=QualityRunner(),
        routes=TUSHARE_ROUTES,
        max_concurrent_curate_datasets=1,
    )

    with patch(
        "quant_research.data.pipeline.dataset.time.monotonic",
        return_value=0.0,
    ):
        pipeline._curate_datasets(
            (DatasetKind.STOCK_MASTER,),
            windows={DatasetKind.STOCK_MASTER: (None, None)},
            observer=observer,
        )

    raw_events = [
        details for kind, details in observer.boundaries if kind == "raw_input"
    ]
    assert [event["raw_index"] for event in raw_events] == [1, 3]
    assert raw_events[-1]["aggregate_completed"] == 3
    assert raw_events[-1]["aggregate_total"] == 3
    engine.dispose()
