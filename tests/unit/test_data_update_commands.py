"""Dashboard 数据更新计划预览、入库和历史重试约束。"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any, cast

import pytest

from quant_research.application.operations import OperationalCommandService
from quant_research.data.contracts import JsonValue
from quant_research.data.pipeline.publish import (
    DataUpdatePlan,
    DataUpdateSkip,
    DataUpdateWindow,
    DataUpdateWindowBasis,
)
from quant_research.domain.enums import DatasetKind
from quant_research.domain.errors import QuantError
from quant_research.tasks.models import TaskRecord, TaskStatus

NOW = datetime(2026, 8, 15, 3, tzinfo=UTC)


class _Planner:
    def __init__(self, plan: DataUpdatePlan) -> None:
        self.plan_value = plan
        self.datasets: list[tuple[DatasetKind, ...] | None] = []

    def plan(
        self,
        *,
        start: date | None,
        end: date | None,
        datasets: tuple[DatasetKind, ...] | None = None,
    ) -> DataUpdatePlan:
        assert start is None and end is None
        self.datasets.append(datasets)
        return self.plan_value


class _Queue:
    def __init__(self, task: TaskRecord | None = None) -> None:
        self.task = task
        self.enqueued: tuple[Mapping[str, JsonValue], str | None] | None = None
        self.retry_called = False

    def enqueue(
        self,
        task_type: str,
        payload: Mapping[str, JsonValue],
        priority: int,
        *,
        idempotency_key: str | None = None,
        available_at: datetime | None = None,
        actor: str = "system",
        request_id: str | None = None,
        subject_kind: str | None = None,
        subject_id: str | None = None,
    ) -> str:
        del available_at, subject_kind, subject_id
        assert task_type == "DATA_UPDATE"
        assert priority == 0
        assert actor == "dashboard"
        assert request_id == "request-1"
        self.enqueued = payload, idempotency_key
        self.task = _task(dict(payload))
        return self.task.id

    def get(self, task_id: str) -> TaskRecord:
        assert self.task is not None and self.task.id == task_id
        return self.task

    def retry(self, *_: object, **__: object) -> str:
        self.retry_called = True
        return "retried-task"


class _ValidationQueue:
    def __init__(self) -> None:
        self.enqueued: tuple[str, Mapping[str, JsonValue], str | None] | None = None
        self.task: TaskRecord | None = None

    def enqueue(
        self,
        task_type: str,
        payload: Mapping[str, JsonValue],
        priority: int,
        *,
        idempotency_key: str | None = None,
        available_at: datetime | None = None,
        actor: str = "system",
        request_id: str | None = None,
        subject_kind: str | None = None,
        subject_id: str | None = None,
    ) -> str:
        del available_at, subject_kind, subject_id
        assert priority == 0
        assert actor == "dashboard"
        assert request_id == "request-1"
        self.enqueued = task_type, payload, idempotency_key
        self.task = TaskRecord(
            id="quality-task-1",
            task_type=task_type,
            payload=dict(payload),
            status=TaskStatus.QUEUED,
            priority=0,
            progress={},
            created_at=NOW,
            available_at=NOW,
            updated_at=NOW,
            heartbeat_at=None,
            completed_at=None,
        )
        return self.task.id

    def get(self, task_id: str) -> TaskRecord:
        assert self.task is not None and self.task.id == task_id
        return self.task


def _plan() -> DataUpdatePlan:
    return DataUpdatePlan(
        window_mode="AUTO_INCREMENTAL",
        planned_at=NOW,
        start=date(2026, 8, 10),
        end=date(2026, 8, 14),
        dataset_windows=(
            DataUpdateWindow(
                dataset=DatasetKind.DAILY_BAR,
                basis=DataUpdateWindowBasis.INCREMENTAL,
                start=date(2026, 8, 10),
                end=date(2026, 8, 14),
                overlap_days=4,
                current_watermark=date(2026, 8, 13),
            ),
        ),
    )


def _task(payload: dict[str, JsonValue]) -> TaskRecord:
    return TaskRecord(
        id="task-1",
        task_type="DATA_UPDATE",
        payload=payload,
        status=TaskStatus.FAILED,
        priority=0,
        progress={},
        created_at=NOW,
        available_at=NOW,
        updated_at=NOW,
        heartbeat_at=None,
        completed_at=NOW,
    )


def _service(queue: _Queue, planner: _Planner) -> OperationalCommandService:
    return OperationalCommandService(cast(Any, queue), planner, cast(Any, object()))


def test_preview_and_enqueue_persist_the_same_complete_plan() -> None:
    plan = _plan()
    queue = _Queue()
    planner = _Planner(plan)
    service = _service(queue, planner)
    selected = (DatasetKind.DAILY_BAR,)

    assert (
        service.preview_data_update(
            start=None,
            end=None,
            datasets=selected,
        )
        == plan.to_payload()
    )
    result = service.enqueue_data_update(
        start=None,
        end=None,
        datasets=selected,
        expected_plan_hash=plan.plan_hash,
        request_id="request-1",
    )

    assert result["plan_hash"] == plan.plan_hash
    assert queue.enqueued is not None
    payload, key = queue.enqueued
    assert payload == plan.to_payload()
    assert key == f"dashboard-data-update-{plan.plan_hash[:24]}"
    assert "null" not in str(payload).lower()
    assert planner.datasets == [selected, selected]


def test_stale_preview_is_rejected_before_task_creation() -> None:
    queue = _Queue()
    service = _service(queue, _Planner(_plan()))

    with pytest.raises(QuantError) as stale:
        service.enqueue_data_update(
            start=None,
            end=None,
            expected_plan_hash="f" * 64,
            request_id="request-1",
        )

    assert stale.value.detail.code == "DATA_UPDATE_PLAN_STALE"
    assert queue.enqueued is None


def test_disclosure_pending_plan_cannot_create_an_empty_update_task() -> None:
    plan = DataUpdatePlan(
        window_mode="AUTO_INCREMENTAL",
        planned_at=NOW,
        start=date(2026, 8, 15),
        end=date(2026, 8, 15),
        dataset_windows=(),
        skipped_datasets=(
            DataUpdateSkip(
                dataset=DatasetKind.FINANCIAL_OBSERVATION,
                reason="DISCLOSURE_DEADLINE_PENDING",
                trigger_date=date(2026, 8, 31),
            ),
        ),
    )
    queue = _Queue()
    service = _service(queue, _Planner(plan))

    with pytest.raises(QuantError) as not_required:
        service.enqueue_data_update(
            start=None,
            end=None,
            datasets=(DatasetKind.FINANCIAL_OBSERVATION,),
            expected_plan_hash=plan.plan_hash,
            request_id="request-1",
        )

    assert not_required.value.detail.code == "DATA_UPDATE_NOT_REQUIRED"
    assert queue.enqueued is None


@pytest.mark.parametrize(
    ("dataset", "expected_payload", "expected_key", "expected_scope"),
    (
        (
            None,
            {"scope": "ALL"},
            "dashboard-data-validation-all",
            "ALL",
        ),
        (
            DatasetKind.DAILY_BAR,
            {"scope": "DATASET", "dataset": "daily_bar"},
            "dashboard-data-validation-daily_bar",
            "DATASET",
        ),
    ),
)
def test_quality_run_enqueue_uses_one_strict_scope(
    dataset: DatasetKind | None,
    expected_payload: dict[str, JsonValue],
    expected_key: str,
    expected_scope: str,
) -> None:
    queue = _ValidationQueue()
    service = _service(cast(Any, queue), _Planner(_plan()))

    result = service.enqueue_data_validation(
        dataset=dataset,
        request_id="request-1",
    )

    assert queue.enqueued == (
        "DATA_VALIDATION",
        expected_payload,
        expected_key,
    )
    assert result["task_id"] == "quality-task-1"
    assert result["scope"] == expected_scope
    assert result.get("dataset") == (None if dataset is None else dataset.value)


def test_legacy_dynamic_update_task_cannot_be_retried() -> None:
    queue = _Queue(_task({"start": None, "end": None}))
    service = _service(queue, _Planner(_plan()))

    with pytest.raises(QuantError) as legacy:
        service.retry_task("task-1", confirm_orphaned=False, request_id="retry-request")

    assert legacy.value.detail.code == "DATA_UPDATE_LEGACY_PLAN"
    assert queue.retry_called is False
