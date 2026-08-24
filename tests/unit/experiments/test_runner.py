"""验证实验 Worker 的唯一阶段链和失败终态。"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from quant_research.experiments.config import ExperimentConfigParser
from quant_research.experiments.models import (
    STRATEGY_STAGES,
    ResearchMark,
    RunRecord,
    RunStage,
    RunStatus,
)
from quant_research.experiments.runner import ExperimentRunHandler
from quant_research.tasks.models import ClaimedTask, TaskProgress, TaskStatus


def _run() -> RunRecord:
    definition = ExperimentConfigParser().parse_experiment(
        """name: runner
sample_windows:
  train: {start: 2020-01-01, end: 2020-12-31}
  validation: {start: 2021-01-01, end: 2021-12-31}
  test: {start: 2022-01-01, end: 2022-12-31}
governance: {test_budget: 1, correction: BONFERRONI}
initial_run:
  start_date: 2020-01-01
  end_date: 2021-12-31
  strategy:
    strategy_id: dual_ma_trend
    parameters: {instrument_id: 510300.SH, short_window: 5, long_window: 20}
  benchmark: 000300.SH
  initial_cash_fen: 1000000
  execution: {reference_price: OPEN, slippage_bps: 0.0, max_volume_participation: 0.1, limit_order_policy: REJECT}
"""
    )
    now = datetime(2026, 8, 23, tzinfo=UTC)
    return RunRecord(
        id="run-1",
        experiment_id="experiment-1",
        config=definition.definition.initial_run,
        config_hash=definition.config_hash,
        catalog_hash="a" * 64,
        status=RunStatus.QUEUED,
        stage=RunStage.VALIDATE,
        research_mark=ResearchMark.UNREVIEWED,
        uses_test_region=False,
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
        task_type="EXPERIMENT_RUN",
        payload={},
        priority=0,
        worker_id="worker-1",
        progress=TaskProgress(stage="QUEUED", completed=0, total=0, message="queued"),
        claimed_at=datetime(2026, 8, 23, tzinfo=UTC),
        subject_kind="EXPERIMENT_RUN",
        subject_id="run-1",
    )


class _Registry:
    def __init__(self, run: RunRecord, *, fail_success: bool = False) -> None:
        self.run = run
        self.transitions: list[RunStatus] = []
        self.discard_calls = 0
        self.fail_success = fail_success

    def get_run(self, run_id: str) -> RunRecord:
        assert run_id == self.run.id
        return self.run

    def update_stage(self, run_id: str, stage: RunStage) -> None:
        assert run_id == self.run.id
        self.run = self.run.model_copy(update={"stage": stage})

    def transition(
        self,
        run_id: str,
        expected: RunStatus,
        target: RunStatus,
        *,
        stage: RunStage,
        error: dict[str, object] | None = None,
        artifact_dir: str | None = None,
        manifest_hash: str | None = None,
    ) -> None:
        assert run_id == self.run.id
        assert self.run.status is expected
        if target is RunStatus.SUCCEEDED and self.fail_success:
            raise RuntimeError("success transition failed")
        self.transitions.append(target)
        self.run = self.run.model_copy(
            update={
                "status": target,
                "stage": stage,
                "error": error,
                "artifact_dir": artifact_dir,
                "manifest_hash": manifest_hash,
            }
        )

    def discard_outputs(self, run_id: str) -> None:
        assert run_id == self.run.id
        self.discard_calls += 1


class _Catalog:
    def __init__(self) -> None:
        self.calls = 0

    def assert_unchanged(self, catalog_hash: str) -> None:
        assert catalog_hash == "a" * 64
        self.calls += 1


class _Progress:
    def __init__(self) -> None:
        self.values: list[TaskProgress] = []

    def update(self, progress: TaskProgress) -> None:
        self.values.append(progress)


class _Cancellation:
    def is_cancelled(self) -> bool:
        return False


class _Session:
    def __init__(self, *, fail_at: RunStage | None = None) -> None:
        self.stages: list[RunStage] = []
        self.fail_at = fail_at
        self.abort_calls = 0

    def execute(
        self, stage: RunStage, progress: _Progress, cancellation: _Cancellation
    ) -> dict[str, object]:
        del progress, cancellation
        self.stages.append(stage)
        if stage is self.fail_at:
            raise RuntimeError("stage failed")
        if stage is RunStage.PERSIST:
            return {"artifact_dir": "runs/run-1", "manifest_hash": "b" * 64}
        return {}

    def abort(self) -> None:
        self.abort_calls += 1


class _Factory:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def create(self, run: RunRecord) -> _Session:
        assert run.id == "run-1"
        return self.session


def test_strategy_run_executes_the_fixed_stage_order() -> None:
    registry = _Registry(_run())
    catalog = _Catalog()
    session = _Session()
    handler = ExperimentRunHandler(
        registry, catalog, _Factory(session)
    )

    outcome = handler.run(_task(), _Progress(), _Cancellation())

    assert outcome.status is TaskStatus.SUCCEEDED
    assert session.stages == list(STRATEGY_STAGES)
    assert registry.transitions == [RunStatus.RUNNING, RunStatus.SUCCEEDED]
    assert registry.run.artifact_dir == "runs/run-1"
    assert catalog.calls == len(STRATEGY_STAGES) * 2


def test_analysis_failure_never_executes_persist() -> None:
    registry = _Registry(_run())
    session = _Session(fail_at=RunStage.ANALYTICS)
    handler = ExperimentRunHandler(
        registry, _Catalog(), _Factory(session)
    )

    with pytest.raises(RuntimeError, match="stage failed"):
        handler.run(_task(), _Progress(), _Cancellation())

    assert RunStage.PERSIST not in session.stages
    assert session.abort_calls == 1
    assert registry.transitions == [RunStatus.RUNNING, RunStatus.FAILED]
    assert registry.run.artifact_dir is None


def test_success_transition_failure_aborts_published_session() -> None:
    registry = _Registry(_run(), fail_success=True)
    session = _Session()
    handler = ExperimentRunHandler(
        registry, _Catalog(), _Factory(session)
    )

    with pytest.raises(RuntimeError, match="success transition failed"):
        handler.run(_task(), _Progress(), _Cancellation())

    assert RunStage.PERSIST in session.stages
    assert session.abort_calls == 1
    assert registry.transitions == [RunStatus.RUNNING, RunStatus.FAILED]
