"""验证非阻塞研究任务链、验证集选型与 TEST 隔离。"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from quant_research.application.research_platform import (
    ResearchCommandService,
    ResearchExecutionIdentity,
    ResearchExpandHandler,
    ResearchRegisterHandler,
    ResearchRunHandler,
    ResearchRunResult,
    ResearchSelectHandler,
)
from quant_research.application.worker import Worker
from quant_research.experiments.research import (
    ResearchMetricRecord,
    ResearchPhase,
    ResearchStatus,
)
from quant_research.experiments.research_artifacts import ResearchArtifactPublisher
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.research_registry import ResearchRegistry
from quant_research.infrastructure.persistence.research_task_queue import (
    ResearchTaskQueue,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.research_protocols import ResearchConfigResolver
from quant_research.strategies.definitions import ComponentRegistry


class _Identities:
    def capture(self) -> ResearchExecutionIdentity:
        return ResearchExecutionIdentity(*("a" * 64,) * 5)


class _Runtime:
    def __init__(self, publisher: ResearchArtifactPublisher) -> None:
        self.publisher = publisher

    def execute(self, family, execution, variant, run, progress, cancellation):
        del progress, cancellation
        short = float(variant.config["signal"]["short_window_sessions"])
        long = float(variant.config["signal"]["long_window_sessions"])
        if run.phase is ResearchPhase.TEST:
            values = (("TEST", 999.0),)
        else:
            values = (("TRAIN", 1000.0 - short - long), ("VALIDATION", short + long / 1000.0))
        metrics = tuple(
            metric
            for split, value in values
            for metric in (
                ResearchMetricRecord(
                    run_id=run.id,
                    split=split,
                    category="PERFORMANCE",
                    name="calmar",
                    value=value,
                    unit=None,
                    p_value=0.001,
                    adjusted_p_value=None,
                ),
                ResearchMetricRecord(
                    run_id=run.id,
                    split=split,
                    category="PERFORMANCE",
                    name="sharpe",
                    value=value,
                    unit=None,
                    p_value=0.001,
                    adjusted_p_value=None,
                ),
                ResearchMetricRecord(
                    run_id=run.id,
                    split=split,
                    category="PERFORMANCE",
                    name="max_drawdown",
                    value=-0.1,
                    unit=None,
                    p_value=None,
                    adjusted_p_value=None,
                ),
            )
        )
        path, digest = self.publisher.publish(
            family_id=family.id,
            execution_id=execution.id,
            run_id=run.id,
            frames={"analytics/nav.parquet": pl.DataFrame({"trade_date": [], "nav": []})},
            documents={"runtime.json": {"phase": run.phase.value}},
            identity={
                "family_id": family.id,
                "execution_id": execution.id,
                "run_id": run.id,
                "variant_id": variant.id,
                "phase": run.phase.value,
                "catalog_hash": execution.catalog_hash,
                "composition_hash": variant.composition_hash,
            },
        )
        return ResearchRunResult(
            str(path), digest, metrics, {"REGISTER": "SUCCEEDED"}
        )


def test_workflow_selects_only_validation_then_runs_one_test(tmp_path: Path) -> None:
    database = tmp_path / "state.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    queue = TaskQueue(engine)
    research_queue = ResearchTaskQueue(engine, queue)
    registry = ResearchRegistry(engine)
    publisher = ResearchArtifactPublisher(tmp_path / "artifacts")
    commands = ResearchCommandService(
        resolver=ResearchConfigResolver(),
        components=ComponentRegistry(),
        registry=registry,
        queue=research_queue,
        identities=_Identities(),
    )
    runtime = _Runtime(publisher)
    worker = Worker(
        queue,
        worker_id="workflow-worker",
        handlers=(
            ResearchExpandHandler(registry, research_queue),
            ResearchRunHandler(registry, research_queue, runtime),
            ResearchSelectHandler(registry, research_queue, publisher),
            ResearchRegisterHandler(registry),
        ),
        heartbeat_interval=0.05,
    )
    config = (
        Path(__file__).parents[2]
        / "configs"
        / "research"
        / "examples"
        / "dual_ma_trend.yaml"
    ).read_text(encoding="utf-8")
    submitted = commands.submit(config, request_id="workflow-test")

    for _ in range(30):
        if not worker.run_once():
            break
    else:
        raise AssertionError("research workflow did not drain")

    execution = registry.get_execution(str(submitted["execution_id"]))
    variants = registry.list_variants(execution.id)
    runs = registry.list_runs(execution.id)
    tests = [item for item in runs if item.phase is ResearchPhase.TEST]
    selected = next(item for item in variants if item.id == execution.selected_variant_id)
    assert execution.status is ResearchStatus.SUCCEEDED
    assert len(tests) == 1
    assert selected.parameters == {
        "signal.long_window_sessions": 200,
        "signal.short_window_sessions": 40,
    }
    assert all(
        metric.split == "VALIDATION"
        for run in runs
        if run.phase is ResearchPhase.TRAIN_VALIDATION
        for metric in registry.list_metrics(run.id)
        if metric.adjusted_p_value is not None
    )
    assert registry.list_metrics(tests[0].id)[-1].value == 999.0
    assert (
        tmp_path
        / "artifacts"
        / "research"
        / str(submitted["family_id"])
        / execution.id
        / "selection.json"
    ).is_file()
    engine.dispose()
