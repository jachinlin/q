"""验证策略研究 Worker 的固定四阶段链和失败清理。"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from quant_research.strategy_studies.config import StrategyStudyConfigParser
from quant_research.strategy_studies.models import (
    STRATEGY_STUDY_STAGES,
    StrategyStudyRecord,
    StrategyStudyStage,
    StrategyStudyStatus,
)
from quant_research.strategy_studies.runner import StrategyStudyHandler
from quant_research.tasks.models import ClaimedTask, TaskProgress, TaskStatus
from tests.unit.strategy_studies.test_models import strategy_study_yaml


def _study() -> StrategyStudyRecord:
    now = datetime(2026, 8, 23, tzinfo=UTC)
    resolved = StrategyStudyConfigParser().parse(strategy_study_yaml())
    return StrategyStudyRecord(
        id="study-1",
        definition=resolved.definition,
        config_hash=resolved.config_hash,
        catalog_hash="a" * 64,
        status=StrategyStudyStatus.QUEUED,
        stage=StrategyStudyStage.VALIDATE,
        task_id="task-1",
        artifact_dir=None,
        manifest_hash=None,
        error=None,
        created_at=now,
        started_at=None,
        completed_at=None,
    )


def _task() -> ClaimedTask:
    return ClaimedTask(
        id="task-1", attempt_id="attempt-1", attempt_no=1,
        task_type="STRATEGY_STUDY", payload={"strategy_study_id": "study-1"},
        priority=0, worker_id="worker-1",
        progress=TaskProgress(stage="QUEUED", completed=0, total=0, message="queued"),
        claimed_at=datetime(2026, 8, 23, tzinfo=UTC),
        subject_kind="STRATEGY_STUDY", subject_id="study-1",
    )


class _Registry:
    def __init__(self) -> None:
        self.study = _study()
        self.transitions: list[StrategyStudyStatus] = []

    def get(self, study_id: str) -> StrategyStudyRecord:
        assert study_id == self.study.id
        return self.study

    def update_stage(self, study_id: str, stage: StrategyStudyStage) -> None:
        assert study_id == self.study.id
        self.study = self.study.model_copy(update={"stage": stage})

    def transition(self, study_id: str, expected: StrategyStudyStatus, target: StrategyStudyStatus, *, stage: StrategyStudyStage, error: dict[str, Any] | None = None, artifact_dir: str | None = None, manifest_hash: str | None = None) -> None:
        assert study_id == self.study.id and self.study.status is expected
        self.transitions.append(target)
        self.study = self.study.model_copy(update={"status": target, "stage": stage, "error": error, "artifact_dir": artifact_dir, "manifest_hash": manifest_hash})

    def discard_outputs(self, study_id: str) -> None:
        assert study_id == self.study.id


class _Catalog:
    def __init__(self, fail_after: int | None = None) -> None:
        self.calls = 0
        self.fail_after = fail_after

    def assert_unchanged(self, catalog_hash: str) -> None:
        assert catalog_hash == "a" * 64
        self.calls += 1
        if self.fail_after == self.calls:
            raise ValueError("catalog drift")


class _Progress:
    def update(self, progress: TaskProgress) -> None:
        del progress


class _Cancellation:
    def __init__(self, cancelled: bool = False) -> None:
        self.cancelled = cancelled

    def is_cancelled(self) -> bool:
        return self.cancelled


class _Session:
    def __init__(self, fail_at: StrategyStudyStage | None = None) -> None:
        self.stages: list[StrategyStudyStage] = []
        self.fail_at = fail_at
        self.aborted = 0

    def execute(self, stage: StrategyStudyStage, progress: _Progress, cancellation: _Cancellation) -> dict[str, Any]:
        del progress, cancellation
        self.stages.append(stage)
        if stage is self.fail_at:
            raise RuntimeError("stage failed")
        return {"artifact_dir": "strategy-studies/study-1", "manifest_hash": "b" * 64} if stage is StrategyStudyStage.PUBLISH else {}

    def abort(self) -> None:
        self.aborted += 1


class _Executor:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def create(self, study: StrategyStudyRecord) -> _Session:
        assert study.id == "study-1"
        return self.session


def test_executes_exact_four_stage_order() -> None:
    registry, catalog, session = _Registry(), _Catalog(), _Session()
    outcome = StrategyStudyHandler(registry, catalog, _Executor(session)).run(_task(), _Progress(), _Cancellation())
    assert outcome.status is TaskStatus.SUCCEEDED
    assert session.stages == list(STRATEGY_STUDY_STAGES)
    assert registry.transitions == [StrategyStudyStatus.RUNNING, StrategyStudyStatus.SUCCEEDED]
    assert catalog.calls == len(STRATEGY_STUDY_STAGES) * 2


@pytest.mark.parametrize("failure", [StrategyStudyStage.BACKTEST, StrategyStudyStage.ANALYTICS])
def test_failure_aborts_and_never_publishes(failure: StrategyStudyStage) -> None:
    registry, session = _Registry(), _Session(failure)
    with pytest.raises(RuntimeError, match="stage failed"):
        StrategyStudyHandler(registry, _Catalog(), _Executor(session)).run(_task(), _Progress(), _Cancellation())
    assert StrategyStudyStage.PUBLISH not in session.stages
    assert session.aborted == 1
    assert registry.transitions[-1] is StrategyStudyStatus.FAILED


def test_catalog_drift_fails_before_next_stage_and_cleans_session() -> None:
    registry, session = _Registry(), _Session()
    with pytest.raises(ValueError, match="catalog drift"):
        StrategyStudyHandler(registry, _Catalog(fail_after=2), _Executor(session)).run(_task(), _Progress(), _Cancellation())
    assert session.stages == [StrategyStudyStage.VALIDATE]
    assert session.aborted == 1


def test_cancellation_converges_to_cancelled_without_execution() -> None:
    registry, session = _Registry(), _Session()
    outcome = StrategyStudyHandler(registry, _Catalog(), _Executor(session)).run(_task(), _Progress(), _Cancellation(True))
    assert outcome.status is TaskStatus.CANCELLED
    assert session.stages == []
    assert session.aborted == 1
    assert registry.transitions[-1] is StrategyStudyStatus.CANCELLED
