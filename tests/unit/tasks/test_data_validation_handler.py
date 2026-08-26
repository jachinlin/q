"""后台数据质量运行处理器的范围、结果与取消语义。"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import cast
from uuid import uuid4

import pytest

from quant_research.application.data import DataValidationHandler
from quant_research.data.pipeline.publish import DataPipeline
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.domain.identifiers import QualityRunId
from quant_research.tasks.handlers import CancellationToken, ProgressSink
from quant_research.tasks.models import ClaimedTask, TaskProgress, TaskStatus


class _Pipeline:
    def __init__(self) -> None:
        self.dataset: DatasetKind | None = None
        self.cancel_during_run = False
        self.failure: QuantError | None = None

    def validate(
        self,
        dataset: DatasetKind | None = None,
        *,
        heartbeat: Callable[[], None] = lambda: None,
    ) -> QualityRunId:
        self.dataset = dataset
        heartbeat()
        if self.cancel_during_run:
            heartbeat()
        if self.failure is not None:
            raise self.failure
        return QualityRunId(uuid4())


class _Progress:
    def __init__(self) -> None:
        self.values: list[TaskProgress] = []

    def update(self, progress: TaskProgress) -> None:
        self.values.append(progress)


class _Cancellation:
    def __init__(self, *, cancel_on_check: int | None = None) -> None:
        self.cancel_on_check = cancel_on_check
        self.checks = 0

    def is_cancelled(self) -> bool:
        self.checks += 1
        return self.cancel_on_check == self.checks


def _task(payload: dict[str, str]) -> ClaimedTask:
    return ClaimedTask(
        id="task-1",
        attempt_id="attempt-1",
        attempt_no=1,
        task_type="DATA_VALIDATION",
        payload=payload,
        priority=0,
        worker_id="worker-1",
        progress=TaskProgress(
            stage="QUEUED", completed=0, total=0, message="queued", context={}
        ),
        claimed_at=datetime(2026, 8, 16, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("payload", "expected_dataset"),
    (
        ({"scope": "ALL"}, None),
        (
            {"scope": "DATASET", "dataset": "daily_bar"},
            DatasetKind.STOCK_DAILY_BAR,
        ),
    ),
)
def test_handler_dispatches_all_or_one_dataset(
    payload: dict[str, str], expected_dataset: DatasetKind | None
) -> None:
    pipeline = _Pipeline()
    progress = _Progress()

    outcome = DataValidationHandler(cast(DataPipeline, pipeline)).run(
        _task(payload),
        cast(ProgressSink, progress),
        cast(CancellationToken, _Cancellation()),
    )

    assert pipeline.dataset is expected_dataset
    assert outcome.status is TaskStatus.SUCCEEDED
    assert outcome.result is not None
    assert outcome.result["scope"] == ("ALL" if expected_dataset is None else "DATASET")
    assert progress.values[0].stage == "VALIDATE"
    assert progress.values[-1].stage == "COMPLETE"


def test_handler_honors_cancellation_at_validation_boundary() -> None:
    pipeline = _Pipeline()
    pipeline.cancel_during_run = True

    outcome = DataValidationHandler(cast(DataPipeline, pipeline)).run(
        _task({"scope": "ALL"}),
        cast(ProgressSink, _Progress()),
        cast(CancellationToken, _Cancellation(cancel_on_check=1)),
    )

    assert outcome.status is TaskStatus.CANCELLED


def test_all_scope_propagates_blocking_quality_failure_to_worker() -> None:
    pipeline = _Pipeline()
    pipeline.failure = QuantError(
        ErrorDetail(
            code="DATA_VALIDATION_FAILED",
            severity=Severity.SEVERE,
            message="validate-all found blocking quality issues",
            context={},
            remediation="inspect quality run",
            retryable=False,
        )
    )
    progress = _Progress()

    with pytest.raises(QuantError, match="blocking quality issues"):
        DataValidationHandler(cast(DataPipeline, pipeline)).run(
            _task({"scope": "ALL"}),
            cast(ProgressSink, progress),
            cast(CancellationToken, _Cancellation()),
        )

    assert [item.stage for item in progress.values] == ["VALIDATE"]


@pytest.mark.parametrize(
    "payload",
    (
        {},
        {"scope": "DATASET"},
        {"scope": "ALL", "dataset": "daily_bar"},
        {"scope": "DATASET", "dataset": "not-a-dataset"},
    ),
)
def test_handler_rejects_noncanonical_payloads(payload: dict[str, str]) -> None:
    with pytest.raises(ValueError, match="DATA_VALIDATION"):
        DataValidationHandler(cast(DataPipeline, _Pipeline())).run(
            _task(payload),
            cast(ProgressSink, _Progress()),
            cast(CancellationToken, _Cancellation()),
        )
