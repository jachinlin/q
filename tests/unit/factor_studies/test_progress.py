"""验证因子研究四阶段进度、确定性采样和失败子步骤。"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from quant_research.domain.enums import MultipleTestingMethod
from quant_research.factor_studies.models import (
    FACTOR_STUDY_STAGES,
    FactorStudyDefinition,
    FactorStudyRecord,
    FactorStudyStage,
    FactorStudyStatus,
    FactorStudyUniverse,
)
from quant_research.factor_studies.progress import FactorStudyProgressReporter
from quant_research.factor_studies.runner import FactorStudyHandler
from quant_research.tasks.models import ClaimedTask, TaskProgress, TaskStatus


class _Progress:
    def __init__(self) -> None:
        self.values: list[TaskProgress] = []

    def update(self, progress: TaskProgress) -> None:
        self.values.append(progress)


class _Cancellation:
    def is_cancelled(self) -> bool:
        return False


class _Catalog:
    def assert_unchanged(self, catalog_hash: str) -> None:
        assert catalog_hash == "a" * 64


class _Registry:
    def __init__(self, study: FactorStudyRecord) -> None:
        self.study = study
        self.transitions: list[FactorStudyStatus] = []

    def get(self, study_id: str) -> FactorStudyRecord:
        assert study_id == self.study.id
        return self.study

    def update_stage(self, study_id: str, stage: FactorStudyStage) -> None:
        assert study_id == self.study.id
        self.study = self.study.model_copy(update={"stage": stage})

    def transition(
        self,
        study_id: str,
        expected: FactorStudyStatus,
        target: FactorStudyStatus,
        *,
        stage: FactorStudyStage,
        error: dict[str, object] | None = None,
        artifact_dir: str | None = None,
        manifest_hash: str | None = None,
    ) -> None:
        assert study_id == self.study.id
        assert self.study.status is expected
        self.transitions.append(target)
        self.study = self.study.model_copy(
            update={
                "status": target,
                "stage": stage,
                "error": error,
                "artifact_dir": artifact_dir,
                "manifest_hash": manifest_hash,
            }
        )


class _Session:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.stages: list[FactorStudyStage] = []
        self.abort_calls = 0

    def execute(
        self,
        stage: FactorStudyStage,
        progress: FactorStudyProgressReporter,
        cancellation: _Cancellation,
    ) -> dict[str, object]:
        del cancellation
        self.stages.append(stage)
        if stage is FactorStudyStage.ANALYZE_FACTORS and self.fail:
            progress.substage_started(
                "COMPUTE_FACTORS",
                "正在重新计算研究因子",
            )
            raise RuntimeError("factor compute failed")
        if stage is FactorStudyStage.PUBLISH:
            return {
                "artifact_dir": "factor-studies/study-1",
                "manifest_hash": "b" * 64,
            }
        return {}

    def abort(self) -> None:
        self.abort_calls += 1


class _Factory:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def create(self, study: FactorStudyRecord) -> _Session:
        assert study.id == "study-1"
        return self.session


def _study() -> FactorStudyRecord:
    now = datetime(2026, 8, 28, tzinfo=UTC)
    return FactorStudyRecord(
        id="study-1",
        definition=FactorStudyDefinition(
            name="progress",
            start_date=date(2026, 1, 1),
            end_date=date(2026, 1, 31),
            correction=MultipleTestingMethod.BH_FDR,
            factor_ids=("book_to_price_mrq",),
            universe=FactorStudyUniverse(name="CN_STOCK_STANDARD"),
            horizons=(5,),
        ),
        config_hash="c" * 64,
        catalog_hash="a" * 64,
        status=FactorStudyStatus.QUEUED,
        stage=FactorStudyStage.VALIDATE,
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
        id="task-1",
        attempt_id="attempt-1",
        attempt_no=1,
        task_type="FACTOR_STUDY",
        payload={"factor_study_id": "study-1"},
        priority=0,
        worker_id="worker-1",
        progress=TaskProgress(stage="QUEUED", completed=0, total=0, message=""),
        claimed_at=datetime(2026, 8, 28, tzinfo=UTC),
        subject_kind="FACTOR_STUDY",
        subject_id="study-1",
    )


def test_universe_progress_uses_deterministic_five_percent_sampling() -> None:
    sink = _Progress()
    reporter = FactorStudyProgressReporter(sink)
    reporter.stage_started(FactorStudyStage.ANALYZE_FACTORS)
    reporter.substage_started("BUILD_UNIVERSE", "开始")

    published = [
        completed
        for completed in range(1, 1_001)
        if reporter.substage_progress(
            "BUILD_UNIVERSE",
            "准备中",
            item_completed=completed,
            item_total=1_000,
        )
    ]

    assert published == [1, *range(50, 1_001, 50)]
    sampled = [
        item
        for item in sink.values
        if item.context.get("substage_state") == "PROGRESS"
    ]
    assert len(sampled) == 21
    assert all((item.completed, item.total) == (2, 4) for item in sampled)


def test_handler_keeps_four_stage_progress_and_finishes_at_four_of_four() -> None:
    registry = _Registry(_study())
    session = _Session()
    sink = _Progress()

    outcome = FactorStudyHandler(
        registry,
        _Catalog(),
        _Factory(session),
    ).run(_task(), sink, _Cancellation())

    assert outcome.status is TaskStatus.SUCCEEDED
    assert session.stages == list(FACTOR_STUDY_STAGES)
    assert [(item.stage, item.completed, item.total) for item in sink.values] == [
        ("VALIDATE", 0, 4),
        ("VALIDATE", 1, 4),
        ("PREPARE_INPUTS", 1, 4),
        ("PREPARE_INPUTS", 2, 4),
        ("ANALYZE_FACTORS", 2, 4),
        ("ANALYZE_FACTORS", 3, 4),
        ("PUBLISH", 3, 4),
        ("PUBLISH", 4, 4),
    ]
    assert sink.values[-1].context == {
        "stage_state": "COMPLETED",
        "manifest_hash": "b" * 64,
    }


def test_handler_persists_the_active_substage_when_analysis_fails() -> None:
    registry = _Registry(_study())
    session = _Session(fail=True)

    with pytest.raises(RuntimeError, match="factor compute failed"):
        FactorStudyHandler(
            registry,
            _Catalog(),
            _Factory(session),
        ).run(_task(), _Progress(), _Cancellation())

    assert session.abort_calls == 1
    assert registry.study.status is FactorStudyStatus.FAILED
    assert registry.study.error == {
        "code": "FACTOR_STUDY_STAGE_FAILED",
        "error_type": "RuntimeError",
        "substage": "COMPUTE_FACTORS",
    }
