"""数据更新 Worker 必须执行任务中已固化的计划。"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import cast
from uuid import uuid4

from quant_research.application.data import DataUpdateHandler
from quant_research.data.pipeline.publish import (
    DataPipeline,
    DataUpdatePlan,
    DataUpdateWindow,
    DataUpdateWindowBasis,
    PipelineResult,
)
from quant_research.domain.enums import DatasetKind
from quant_research.domain.identifiers import QualityRunId
from quant_research.tasks.handlers import CancellationToken, ProgressSink
from quant_research.tasks.models import ClaimedTask, TaskProgress, TaskStatus


class _Pipeline:
    def __init__(self) -> None:
        self.executed: DataUpdatePlan | None = None

    def execute_update_plan(
        self, plan: DataUpdatePlan, *, observer: object | None = None
    ) -> PipelineResult:
        assert observer is not None
        self.executed = plan
        return PipelineResult("run-1", QualityRunId(uuid4()), "a" * 64)


class _Progress:
    def __init__(self) -> None:
        self.values: list[TaskProgress] = []

    def update(self, progress: TaskProgress) -> None:
        self.values.append(progress)


class _Cancellation:
    def is_cancelled(self) -> bool:
        return False


def test_handler_executes_frozen_plan_without_resolving_dates_again() -> None:
    plan = DataUpdatePlan(
        window_mode="AUTO_INCREMENTAL",
        planned_at=datetime(2026, 8, 15, tzinfo=UTC),
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
    task = ClaimedTask(
        id="task-1",
        attempt_id="attempt-1",
        attempt_no=1,
        task_type="DATA_UPDATE",
        payload=plan.to_payload(),
        priority=0,
        worker_id="worker-1",
        progress=TaskProgress(
            stage="QUEUED", completed=0, total=0, message="queued", context={}
        ),
        claimed_at=datetime(2026, 8, 15, tzinfo=UTC),
    )
    pipeline = _Pipeline()
    progress = _Progress()

    outcome = DataUpdateHandler(cast(DataPipeline, pipeline)).run(
        task,
        cast(ProgressSink, progress),
        cast(CancellationToken, _Cancellation()),
    )

    assert pipeline.executed == plan
    assert outcome.status is TaskStatus.SUCCEEDED
    assert progress.values[-1].stage == "COMPLETE"
