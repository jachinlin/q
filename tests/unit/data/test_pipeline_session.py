"""Provider session ownership for dataset localization."""

import hashlib
import json
import threading
import time
from collections import defaultdict
from datetime import UTC, date, datetime
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest

from quant_research.data.contracts import (
    PublishedPartition,
    RawBatch,
    canonical_json_bytes,
)
from quant_research.data.pipeline.dataset import (
    DataPipeline,
    DataPipelineCancelled,
    DatasetCurateResult,
    DataUpdatePlan,
    DataUpdateWindow,
    DataUpdateWindowBasis,
    LocalizeResult,
)
from quant_research.domain.enums import DatasetKind
from quant_research.domain.identifiers import QualityRunId
from quant_research.infrastructure.tushare.routing import TUSHARE_ROUTES
from quant_research.logging import StructuredLogger


class _Source:
    provider = "tushare"

    def __init__(self) -> None:
        self.login_calls = 0
        self.close_calls = 0

    def login(self) -> None:
        self.login_calls += 1

    def close(self) -> None:
        self.close_calls += 1

    def requests(
        self, endpoint: str, start: date, end: date
    ) -> tuple[dict[str, object], ...]:
        del endpoint, start, end
        return ()

    def fetch(self, endpoint: str, request: dict[str, object]) -> tuple[RawBatch, ...]:
        return (
            RawBatch(
                source=self.provider,
                endpoint=endpoint,
                request=request,
                retrieved_at=datetime(2026, 8, 15, tzinfo=UTC),
                schema=("ts_code",),
                rows=({"ts_code": "600000.SH"},),
            ),
        )


class _RequestSource(_Source):
    def requests(
        self, endpoint: str, start: date, end: date
    ) -> tuple[dict[str, object], ...]:
        del start, end
        return (
            {
                "endpoint": endpoint,
                "list_status": "L",
                "fields": "ts_code",
            },
        )


class _ConcurrentRequestSource(_Source):
    def __init__(self, request_count: int = 8) -> None:
        super().__init__()
        self.request_count = request_count
        self.active = 0
        self.max_active = 0
        self.fetch_order: list[int] = []
        self._lock = threading.Lock()
        self._all_started = threading.Event()

    def requests(
        self, endpoint: str, start: date, end: date
    ) -> tuple[dict[str, object], ...]:
        del start, end
        return tuple(
            {"endpoint": endpoint, "ordinal": index, "fields": "ts_code"}
            for index in range(self.request_count)
        )

    def fetch(self, endpoint: str, request: dict[str, object]) -> tuple[RawBatch, ...]:
        ordinal = int(request["ordinal"])
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active == min(4, self.request_count):
                self._all_started.set()
        assert self._all_started.wait(timeout=2)
        time.sleep((self.request_count - ordinal) * 0.003)
        with self._lock:
            self.fetch_order.append(ordinal)
            self.active -= 1
        return super().fetch(endpoint, request)


class _FailingConcurrentSource(_ConcurrentRequestSource):
    def fetch(self, endpoint: str, request: dict[str, object]) -> tuple[RawBatch, ...]:
        ordinal = int(request["ordinal"])
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active == 4:
                self._all_started.set()
        assert self._all_started.wait(timeout=2)
        if ordinal == 1:
            raise ConnectionError("planned request failure")
        if ordinal == 0:
            time.sleep(0.02)
        with self._lock:
            self.fetch_order.append(ordinal)
            self.active -= 1
        return _Source.fetch(self, endpoint, request)


class _DelayedRequestSource(_ConcurrentRequestSource):
    def fetch(self, endpoint: str, request: dict[str, object]) -> tuple[RawBatch, ...]:
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(0.04)
        with self._lock:
            self.active -= 1
        return _Source.fetch(self, endpoint, request)


class _Calendar:
    def explicit_window(self, start: date, end: date) -> tuple[date, date]:
        return start, end


class _Repository:
    def record_dataset_stage(self, *_: object, **__: object) -> None:
        """接收测试不关心的运营阶段证据。"""

    def catalog_state(self) -> Any:
        """返回执行结果所需的稳定目录身份。"""
        return SimpleNamespace(catalog_hash="a" * 64)

    def find_raw_partition(self, *_: object, **__: object) -> None:
        return None

    def list_raw_partitions(self, *_: object, **__: object) -> tuple[()]:
        return ()

    def register_raw_partition(self, *_: object, **__: object) -> None:
        """接收测试发布的 Raw 登记。"""


class _RawStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def publish(self, batch: RawBatch) -> PublishedPartition:
        request_hash = hashlib.sha256(
            canonical_json_bytes(dict(batch.request))
        ).hexdigest()
        return PublishedPartition(
            source=batch.source,
            endpoint=batch.endpoint,
            request=batch.request,
            retrieved_at=batch.retrieved_at,
            data_path=self.root / f"{request_hash}.parquet",
            manifest_path=self.root / "request.json",
            request_hash=request_hash,
            content_hash="b" * 64,
            schema_fingerprint="c" * 64,
            row_count=len(batch.rows),
        )

    def find_metadata_by_request(self, *_: object, **__: object) -> None:
        return None


class _RecordingRawStore(_RawStore):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.published: list[int] = []

    def publish(self, batch: RawBatch) -> PublishedPartition:
        self.published.append(int(batch.request["ordinal"]))
        return super().publish(batch)


class _Observer:
    def __init__(self) -> None:
        self.stages: list[tuple[str, int]] = []
        self.boundaries: list[tuple[str, DatasetKind, str, dict[str, object]]] = []

    def stage_started(self, stage: str, total: int) -> None:
        self.stages.append((stage, total))

    def dataset_completed(self, *_: object, **__: object) -> None:
        """接收测试不关心的单数据集进度。"""

    def boundary(
        self,
        stage: str,
        dataset: DatasetKind,
        kind: str,
        details: dict[str, object],
    ) -> None:
        self.boundaries.append((stage, dataset, kind, dict(details)))

    def is_cancelled(self) -> bool:
        return False


class _CancelAfterFirstObserver(_Observer):
    def __init__(self) -> None:
        super().__init__()
        self.cancelled = False

    def boundary(
        self,
        stage: str,
        dataset: DatasetKind,
        kind: str,
        details: dict[str, object],
    ) -> None:
        super().boundary(stage, dataset, kind, details)
        if kind == "raw_request" and details.get("status") == "COMPLETED":
            self.cancelled = True

    def is_cancelled(self) -> bool:
        return self.cancelled


class _RecordingCurateObserver(_Observer):
    def __init__(self) -> None:
        super().__init__()
        self.completed: list[tuple[DatasetKind, int, int]] = []

    def dataset_completed(
        self,
        stage: str,
        dataset: DatasetKind,
        completed: int,
        total: int,
        details: dict[str, object],
    ) -> None:
        assert stage == "CURATE"
        assert details["max_concurrency"] in range(1, 9)
        self.completed.append((dataset, completed, total))


def _partial_plan() -> DataUpdatePlan:
    windows = tuple(
        DataUpdateWindow(
            dataset=dataset,
            basis=DataUpdateWindowBasis.INCREMENTAL,
            start=date(2026, 8, 10),
            end=date(2026, 8, 14),
            overlap_days=4,
            current_watermark=date(2026, 8, 13),
        )
        for dataset in (DatasetKind.STOCK_DAILY_BAR, DatasetKind.STOCK_DAILY_BASIC)
    )
    return DataUpdatePlan(
        window_mode="AUTO_INCREMENTAL",
        planned_at=datetime(2026, 8, 15, tzinfo=UTC),
        start=date(2026, 8, 10),
        end=date(2026, 8, 14),
        dataset_windows=windows,
    )


def _pipeline(
    source: _Source,
    data_root: Path,
    logger: StructuredLogger | None = None,
) -> DataPipeline:
    return DataPipeline(
        source=source,  # type: ignore[arg-type]
        mapper=object(),  # type: ignore[arg-type]
        calendar=_Calendar(),  # type: ignore[arg-type]
        raw_store=SimpleNamespace(root=data_root / "raw"),  # type: ignore[arg-type]
        curated_store=SimpleNamespace(  # type: ignore[arg-type]
            root=data_root / "canonical"
        ),
        repository=_Repository(),  # type: ignore[arg-type]
        quality_runner=object(),  # type: ignore[arg-type]
        routes=TUSHARE_ROUTES,
        logger=logger,
    )


def test_localize_reuses_an_active_outer_source_session(tmp_path: Path) -> None:
    source = _Source()
    pipeline = _pipeline(source, tmp_path)
    pipeline._source_session_active = True

    result = pipeline.localize(
        DatasetKind.STOCK_MASTER,
        start=date(2026, 8, 11),
        end=date(2026, 8, 11),
    )

    assert result == LocalizeResult(DatasetKind.STOCK_MASTER, 0, 0, 0)
    assert source.login_calls == 0
    assert source.close_calls == 0


def test_localize_all_opens_one_session_around_every_dataset(tmp_path: Path) -> None:
    source = _Source()
    pipeline = _pipeline(source, tmp_path)
    observed: list[DatasetKind] = []
    windows = tuple(
        DataUpdateWindow(
            dataset=dataset,
            basis=DataUpdateWindowBasis.EXPLICIT,
            start=date(2026, 8, 10),
            end=date(2026, 8, 14),
            overlap_days=0,
        )
        for dataset in sorted(TUSHARE_ROUTES, key=lambda item: item.value)
    )

    def localized(
        instance: DataPipeline,
        dataset: DatasetKind,
        *,
        start: date,
        end: date,
        observer: object | None = None,
    ) -> LocalizeResult:
        assert observer is not None
        assert instance._source_session_active
        assert start == date(2026, 8, 10) and end == date(2026, 8, 14)
        observed.append(dataset)
        return LocalizeResult(dataset, 0, 1, 1)

    with patch.object(DataPipeline, "localize", autospec=True, side_effect=localized):
        results = pipeline.localize_all(windows=windows)

    assert len(results) == len(observed) == len(DatasetKind)
    assert source.login_calls == 1
    assert source.close_calls == 1
    assert pipeline._source_session_active is False


def test_localize_plan_only_visits_the_frozen_dataset_subset(tmp_path: Path) -> None:
    source = _Source()
    pipeline = _pipeline(source, tmp_path)
    plan = _partial_plan()
    observed: list[tuple[DatasetKind, date, date]] = []

    def localized(
        instance: DataPipeline,
        dataset: DatasetKind,
        *,
        start: date,
        end: date,
        observer: object | None = None,
    ) -> LocalizeResult:
        assert observer is not None
        assert instance._source_session_active
        observed.append((dataset, start, end))
        return LocalizeResult(dataset, 0, 1, 1)

    with patch.object(DataPipeline, "localize", autospec=True, side_effect=localized):
        results = pipeline.localize_all(windows=plan.dataset_windows)

    assert tuple(item.dataset for item in results) == (
        DatasetKind.STOCK_DAILY_BAR,
        DatasetKind.STOCK_DAILY_BASIC,
    )
    assert observed == [
        (DatasetKind.STOCK_DAILY_BAR, date(2026, 8, 10), date(2026, 8, 14)),
        (DatasetKind.STOCK_DAILY_BASIC, date(2026, 8, 10), date(2026, 8, 14)),
    ]
    assert source.login_calls == source.close_calls == 1


def test_execute_partial_plan_curates_selection_then_validates_full_catalog(
    tmp_path: Path,
) -> None:
    pipeline = _pipeline(_Source(), tmp_path)
    plan = _partial_plan()
    observer = _Observer()
    quality_run_id = QualityRunId(uuid4())

    with (
        patch.object(
            DataPipeline, "localize_all", autospec=True, return_value=()
        ) as localize_all,
        patch.object(
            DataPipeline, "_curate_many", autospec=True, return_value=()
        ) as curate_many,
        patch.object(
            DataPipeline,
            "validate",
            autospec=True,
            return_value=quality_run_id,
        ) as validate,
    ):
        result = pipeline.execute_update_plan(plan, observer=observer)

    localize_all.assert_called_once_with(
        pipeline, windows=plan.dataset_windows, observer=observer
    )
    curate_many.assert_called_once_with(
        pipeline,
        (DatasetKind.STOCK_DAILY_BAR, DatasetKind.STOCK_DAILY_BASIC),
        observer=observer,
    )
    validate.assert_called_once_with(pipeline, observer=observer)
    assert result.quality_run_id == quality_run_id


def test_localize_logs_current_request_before_fetch_and_reports_its_slice(
    tmp_path: Path,
) -> None:
    stream = StringIO()
    observer = _Observer()
    pipeline = DataPipeline(
        source=_RequestSource(),  # type: ignore[arg-type]
        mapper=object(),  # type: ignore[arg-type]
        calendar=_Calendar(),  # type: ignore[arg-type]
        raw_store=_RawStore(tmp_path / "raw"),  # type: ignore[arg-type]
        curated_store=SimpleNamespace(root=tmp_path / "canonical"),
        repository=_Repository(),  # type: ignore[arg-type]
        quality_runner=object(),  # type: ignore[arg-type]
        routes=TUSHARE_ROUTES,
        logger=StructuredLogger(stream),
    )

    result = pipeline.localize(
        DatasetKind.STOCK_MASTER,
        start=date(2026, 8, 15),
        end=date(2026, 8, 15),
        observer=observer,
    )

    assert result == LocalizeResult(DatasetKind.STOCK_MASTER, 1, 0, 1)
    request_events = [
        details
        for _, _, kind, details in observer.boundaries
        if kind == "raw_request"
    ]
    assert [item["status"] for item in request_events] == ["STARTED", "COMPLETED"]
    assert request_events[0]["request"] == {
        "endpoint": "stock_basic",
        "list_status": "L",
        "fields": "ts_code",
    }
    assert request_events[0]["request_index"] == 1
    assert request_events[0]["request_total"] == 1
    assert request_events[1]["completed_requests"] == 1
    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    started_index = next(
        index for index, item in enumerate(records) if item["event"] == "localize.raw_started"
    )
    completed_index = next(
        index
        for index, item in enumerate(records)
        if item["event"] == "localize.raw_completed"
    )
    assert started_index < completed_index
    assert records[started_index]["context"]["request"]["list_status"] == "L"


def test_localize_fetches_with_bounded_concurrency_and_publishes_in_order(
    tmp_path: Path,
) -> None:
    source = _ConcurrentRequestSource()
    raw_store = _RecordingRawStore(tmp_path / "raw")
    pipeline = DataPipeline(
        source=source,  # type: ignore[arg-type]
        mapper=object(),  # type: ignore[arg-type]
        calendar=_Calendar(),  # type: ignore[arg-type]
        raw_store=raw_store,  # type: ignore[arg-type]
        curated_store=SimpleNamespace(root=tmp_path / "canonical"),
        repository=_Repository(),  # type: ignore[arg-type]
        quality_runner=object(),  # type: ignore[arg-type]
        routes=TUSHARE_ROUTES,
        max_concurrent_requests=lambda: 4,
    )

    result = pipeline.localize(
        DatasetKind.STOCK_MASTER,
        start=date(2026, 8, 15),
        end=date(2026, 8, 15),
    )

    assert result.fetched == source.request_count
    assert source.max_active == 4
    assert source.fetch_order != list(range(source.request_count))
    assert raw_store.published == list(range(source.request_count))


def test_localize_concurrency_one_preserves_serial_fetching(tmp_path: Path) -> None:
    source = _ConcurrentRequestSource(request_count=1)
    pipeline = DataPipeline(
        source=source,  # type: ignore[arg-type]
        mapper=object(),  # type: ignore[arg-type]
        calendar=_Calendar(),  # type: ignore[arg-type]
        raw_store=_RecordingRawStore(tmp_path / "raw"),  # type: ignore[arg-type]
        curated_store=SimpleNamespace(root=tmp_path / "canonical"),
        repository=_Repository(),  # type: ignore[arg-type]
        quality_runner=object(),  # type: ignore[arg-type]
        routes=TUSHARE_ROUTES,
        max_concurrent_requests=lambda: 1,
    )

    pipeline.localize(
        DatasetKind.STOCK_MASTER,
        start=date(2026, 8, 15),
        end=date(2026, 8, 15),
    )

    assert source.max_active == 1


def test_localize_failure_keeps_only_the_ordered_published_prefix(
    tmp_path: Path,
) -> None:
    source = _FailingConcurrentSource()
    raw_store = _RecordingRawStore(tmp_path / "raw")
    pipeline = DataPipeline(
        source=source,  # type: ignore[arg-type]
        mapper=object(),  # type: ignore[arg-type]
        calendar=_Calendar(),  # type: ignore[arg-type]
        raw_store=raw_store,  # type: ignore[arg-type]
        curated_store=SimpleNamespace(root=tmp_path / "canonical"),
        repository=_Repository(),  # type: ignore[arg-type]
        quality_runner=object(),  # type: ignore[arg-type]
        routes=TUSHARE_ROUTES,
        max_concurrent_requests=lambda: 4,
    )

    with pytest.raises(ConnectionError, match="planned request failure"):
        pipeline.localize(
            DatasetKind.STOCK_MASTER,
            start=date(2026, 8, 15),
            end=date(2026, 8, 15),
        )

    assert raw_store.published == [0]
    assert 4 not in source.fetch_order


def test_localize_cancellation_discards_results_after_published_prefix(
    tmp_path: Path,
) -> None:
    source = _ConcurrentRequestSource()
    raw_store = _RecordingRawStore(tmp_path / "raw")
    observer = _CancelAfterFirstObserver()
    pipeline = DataPipeline(
        source=source,  # type: ignore[arg-type]
        mapper=object(),  # type: ignore[arg-type]
        calendar=_Calendar(),  # type: ignore[arg-type]
        raw_store=raw_store,  # type: ignore[arg-type]
        curated_store=SimpleNamespace(root=tmp_path / "canonical"),
        repository=_Repository(),  # type: ignore[arg-type]
        quality_runner=object(),  # type: ignore[arg-type]
        routes=TUSHARE_ROUTES,
        max_concurrent_requests=lambda: 4,
    )

    with pytest.raises(RuntimeError, match="cancellation requested"):
        pipeline.localize(
            DatasetKind.STOCK_MASTER,
            start=date(2026, 8, 15),
            end=date(2026, 8, 15),
            observer=observer,
        )

    assert raw_store.published == [0]


@pytest.mark.performance
def test_four_way_localize_concurrency_improves_network_throughput(
    tmp_path: Path,
) -> None:
    def elapsed(concurrency: int, name: str) -> float:
        source = _DelayedRequestSource(request_count=12)
        pipeline = DataPipeline(
            source=source,  # type: ignore[arg-type]
            mapper=object(),  # type: ignore[arg-type]
            calendar=_Calendar(),  # type: ignore[arg-type]
            raw_store=_RecordingRawStore(tmp_path / name / "raw"),  # type: ignore[arg-type]
            curated_store=SimpleNamespace(root=tmp_path / name / "canonical"),
            repository=_Repository(),  # type: ignore[arg-type]
            quality_runner=object(),  # type: ignore[arg-type]
            routes=TUSHARE_ROUTES,
            max_concurrent_requests=lambda: concurrency,
        )
        started = time.monotonic()
        pipeline.localize(
            DatasetKind.STOCK_MASTER,
            start=date(2026, 8, 15),
            end=date(2026, 8, 15),
        )
        return time.monotonic() - started

    serial = elapsed(1, "serial")
    concurrent = elapsed(4, "concurrent")

    assert concurrent < serial * 0.45


def test_curate_runs_one_dataset_per_thread_with_bounded_serial_publication(
    tmp_path: Path,
) -> None:
    datasets = tuple(sorted(TUSHARE_ROUTES, key=lambda item: item.value))[:10]
    pipeline = DataPipeline(
        source=_Source(),  # type: ignore[arg-type]
        mapper=object(),  # type: ignore[arg-type]
        calendar=_Calendar(),  # type: ignore[arg-type]
        raw_store=SimpleNamespace(root=tmp_path / "raw"),  # type: ignore[arg-type]
        curated_store=SimpleNamespace(root=tmp_path / "canonical"),
        repository=_Repository(),  # type: ignore[arg-type]
        quality_runner=object(),  # type: ignore[arg-type]
        routes=TUSHARE_ROUTES,
        max_concurrent_curate_datasets=8,
    )
    observer = _RecordingCurateObserver()
    guard = threading.Lock()
    active = 0
    maximum_active = 0
    publishing = 0
    maximum_publishing = 0
    worker_threads: dict[DatasetKind, set[int]] = defaultdict(set)

    def curated(
        instance: DataPipeline,
        selected: tuple[DatasetKind, ...],
        *,
        windows: dict[DatasetKind, tuple[date | None, date | None]],
        observer: Any,
        publish_lock: threading.Lock,
    ) -> tuple[DatasetCurateResult, ...]:
        del instance
        nonlocal active, maximum_active, publishing, maximum_publishing
        dataset = selected[0]
        assert windows == {dataset: (None, None)}
        with guard:
            active += 1
            maximum_active = max(maximum_active, active)
            worker_threads[dataset].add(threading.get_ident())
        observer.boundary(
            "CURATE",
            dataset,
            "raw_input",
            {"status": "STARTED", "raw_index": 1, "raw_total": 1},
        )
        time.sleep(0.04)
        observer.boundary(
            "CURATE",
            dataset,
            "raw_input",
            {"status": "COMPLETED", "raw_index": 1, "raw_total": 1},
        )
        with publish_lock:
            with guard:
                publishing += 1
                maximum_publishing = max(maximum_publishing, publishing)
            time.sleep(0.005)
            with guard:
                publishing -= 1
        with guard:
            active -= 1
        return (
            DatasetCurateResult(dataset, "a" * 64, 1, 1, 1, 0, 1),
        )

    with patch.object(
        DataPipeline, "_curate_dataset", autospec=True, side_effect=curated
    ):
        results = pipeline._curate_datasets(
            datasets,
            windows={dataset: (None, None) for dataset in datasets},
            observer=observer,
        )

    assert tuple(result.dataset for result in results) == datasets
    assert maximum_active == 8
    assert maximum_publishing == 1
    assert all(len(threads) == 1 for threads in worker_threads.values())
    assert [completed for _, completed, _ in observer.completed] == list(
        range(1, len(datasets) + 1)
    )
    raw_completed = [
        int(details["aggregate_completed"])
        for _, _, kind, details in observer.boundaries
        if kind == "raw_input" and details["status"] == "COMPLETED"
    ]
    assert raw_completed == sorted(raw_completed)
    assert max(
        int(details["active_concurrency"])
        for _, _, _, details in observer.boundaries
    ) <= 8


def test_curate_preserves_root_failure_while_stopping_peer_datasets(
    tmp_path: Path,
) -> None:
    datasets = tuple(sorted(TUSHARE_ROUTES, key=lambda item: item.value))[:4]
    pipeline = _pipeline(_Source(), tmp_path)
    started = threading.Barrier(len(datasets), timeout=2)

    def curated(
        instance: DataPipeline,
        selected: tuple[DatasetKind, ...],
        *,
        windows: dict[DatasetKind, tuple[date | None, date | None]],
        observer: Any,
        publish_lock: threading.Lock,
    ) -> tuple[DatasetCurateResult, ...]:
        del instance, windows, publish_lock
        dataset = selected[0]
        started.wait()
        if dataset is datasets[1]:
            raise RuntimeError("planned CURATE failure")
        deadline = time.monotonic() + 2
        while not observer.is_cancelled() and time.monotonic() < deadline:
            time.sleep(0.005)
        raise DataPipelineCancelled("peer CURATE stopped")

    with (
        patch.object(
            DataPipeline, "_curate_dataset", autospec=True, side_effect=curated
        ),
        pytest.raises(RuntimeError, match="planned CURATE failure"),
    ):
        pipeline._curate_datasets(
            datasets,
            windows={dataset: (None, None) for dataset in datasets},
            observer=_Observer(),
        )


@pytest.mark.performance
def test_eight_way_curate_concurrency_improves_dataset_throughput(
    tmp_path: Path,
) -> None:
    datasets = tuple(sorted(TUSHARE_ROUTES, key=lambda item: item.value))[:8]

    def elapsed(concurrency: int, name: str) -> float:
        pipeline = DataPipeline(
            source=_Source(),  # type: ignore[arg-type]
            mapper=object(),  # type: ignore[arg-type]
            calendar=_Calendar(),  # type: ignore[arg-type]
            raw_store=SimpleNamespace(root=tmp_path / name / "raw"),  # type: ignore[arg-type]
            curated_store=SimpleNamespace(root=tmp_path / name / "canonical"),
            repository=_Repository(),  # type: ignore[arg-type]
            quality_runner=object(),  # type: ignore[arg-type]
            routes=TUSHARE_ROUTES,
            max_concurrent_curate_datasets=concurrency,
        )

        def curated(
            instance: DataPipeline,
            selected: tuple[DatasetKind, ...],
            *,
            windows: dict[DatasetKind, tuple[date | None, date | None]],
            observer: Any,
            publish_lock: threading.Lock,
        ) -> tuple[DatasetCurateResult, ...]:
            del instance, windows, publish_lock
            dataset = selected[0]
            time.sleep(0.04)
            return (DatasetCurateResult(dataset, "a" * 64, 1, 1, 1, 0, 1),)

        started_at = time.monotonic()
        with patch.object(
            DataPipeline, "_curate_dataset", autospec=True, side_effect=curated
        ):
            pipeline._curate_datasets(
                datasets,
                windows={dataset: (None, None) for dataset in datasets},
                observer=_Observer(),
            )
        return time.monotonic() - started_at

    assert elapsed(8, "concurrent") < elapsed(1, "serial") * 0.45


@pytest.mark.parametrize("value", (0, 9, True))
def test_curate_dataset_concurrency_requires_integer_from_one_through_eight(
    tmp_path: Path,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="CURATE datasets"):
        DataPipeline(
            source=_Source(),  # type: ignore[arg-type]
            mapper=object(),  # type: ignore[arg-type]
            calendar=_Calendar(),  # type: ignore[arg-type]
            raw_store=SimpleNamespace(root=tmp_path / "raw"),  # type: ignore[arg-type]
            curated_store=SimpleNamespace(root=tmp_path / "canonical"),
            repository=_Repository(),  # type: ignore[arg-type]
            quality_runner=object(),  # type: ignore[arg-type]
            routes=TUSHARE_ROUTES,
            max_concurrent_curate_datasets=value,  # type: ignore[arg-type]
        )


def test_pipeline_stages_use_independent_business_log_contexts(
    tmp_path: Path,
) -> None:
    stream = StringIO()
    pipeline = _pipeline(_Source(), tmp_path, StructuredLogger(stream))

    pipeline._localize_log(
        "localize.raw_completed",
        request={"api": "query_trade_dates", "from": "2026-08-11"},
        source="baostock",
        endpoint="query_trade_dates",
        disposition="fetched",
        request_hash="a" * 64,
        content_hash="b" * 64,
        row_count=1,
    )
    pipeline._curate_log(
        "curate.partition_completed",
        dataset="trade_calendar",
        partition={
            "partition_key": "all",
            "content_hash": "c" * 64,
            "schema_fingerprint": "d" * 64,
            "row_count": 1,
            "path": "canonical/dataset=trade_calendar/all/c.parquet",
        },
    )
    pipeline._validate_log(
        "validate.issue_detected",
        scope="ALL",
        catalog_hash="e" * 64,
        issue={
            "rule_id": "primary_key",
            "dataset": "trade_calendar",
            "actual": 1,
            "threshold": 0,
        },
    )

    localize, curate, validate = [
        json.loads(line) for line in stream.getvalue().splitlines()
    ]
    assert localize["stage"] == "LOCALIZE"
    assert "task_id" not in localize
    assert "worker_id" not in localize
    assert localize["context"]["request"]["api"] == "query_trade_dates"
    assert curate["stage"] == "CURATE"
    assert "request" not in curate["context"]
    assert curate["context"]["partition"]["partition_key"] == "all"
    assert validate["stage"] == "VALIDATE"
    assert "request" not in validate["context"]
    assert validate["context"]["scope"] == "ALL"
    assert validate["context"]["issue"]["rule_id"] == "primary_key"
