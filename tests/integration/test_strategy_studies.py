"""验证单一 StrategyStudy 与任务、指标和产物的事务主脊。"""

from pathlib import Path

import pytest
from sqlalchemy import inspect

from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.strategy_studies import (
    StrategyStudyRegistry,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.strategy_studies.config import StrategyStudyConfigParser
from quant_research.strategy_studies.models import (
    StrategyStudyStage,
    StrategyStudyStatus,
)
from tests.unit.strategy_studies.test_models import strategy_study_yaml


def _registry(tmp_path: Path) -> tuple[StrategyStudyRegistry, TaskQueue, object]:
    database = tmp_path / "state.sqlite3"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    return (
        StrategyStudyRegistry(engine),
        TaskQueue(engine, task_log_root=tmp_path / "logs"),
        engine,
    )


def test_empty_database_contains_only_final_strategy_study_tables(
    tmp_path: Path,
) -> None:
    """空库升级后应存在策略研究表且旧 Experiment/Run 表消失。"""
    database = tmp_path / "state.sqlite3"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    tables = set(inspect(engine).get_table_names())
    assert {
        "strategy_study",
        "strategy_study_tag",
        "strategy_study_metric",
        "strategy_study_artifact",
        "task",
    }.issubset(tables)
    assert not {"experiment", "experiment_tag", "run", "run_metric", "run_artifact"}.intersection(tables)
    assert "run_id" not in {item["name"] for item in inspect(engine).get_columns("audit_event")}
    engine.dispose()


def test_create_is_atomic_and_binds_one_strategy_study_task(tmp_path: Path) -> None:
    """一次创建必须原子产生一个研究和一个同身份任务。"""
    registry, queue, engine = _registry(tmp_path)
    resolved = StrategyStudyConfigParser().parse(strategy_study_yaml())
    study_id, task_id = registry.create(
        resolved.definition, resolved.config_hash, "a" * 64, actor="test"
    )
    study = registry.get(study_id)
    task = queue.get(task_id)
    assert study.task_id == task_id
    assert (task.task_type, task.subject_kind, task.subject_id) == (
        "STRATEGY_STUDY",
        "STRATEGY_STUDY",
        study_id,
    )
    assert task.payload == {"strategy_study_id": study_id}
    engine.dispose()


def test_outputs_round_trip_without_p_values_and_failure_cleanup(
    tmp_path: Path,
) -> None:
    """指标不含显著性字段，失败清理删除全部未发布登记。"""
    registry, _, engine = _registry(tmp_path)
    resolved = StrategyStudyConfigParser().parse(strategy_study_yaml())
    study_id, _ = registry.create(
        resolved.definition, resolved.config_hash, "a" * 64, actor="test"
    )
    registry.transition(
        study_id,
        StrategyStudyStatus.QUEUED,
        StrategyStudyStatus.RUNNING,
        stage=StrategyStudyStage.VALIDATE,
    )
    registry.register_outputs(
        study_id,
        {"annualized_return": (0.1, "ratio")},
        ({"artifact_type": "metrics", "relative_path": "metrics.json", "content_hash": "b" * 64, "byte_count": 10, "row_count": None, "schema": None},),
    )
    metric = registry.get(study_id).metrics[0]
    assert metric.model_dump() == {"name": "annualized_return", "value": 0.1, "unit": "ratio"}
    registry.discard_outputs(study_id)
    assert registry.get(study_id).metrics == ()
    assert registry.get(study_id).artifacts == ()
    engine.dispose()


def test_delete_rejects_active_and_preserves_detached_task(tmp_path: Path) -> None:
    """仅终态研究可删除，保留任务但解除其研究关联。"""
    registry, queue, engine = _registry(tmp_path)
    resolved = StrategyStudyConfigParser().parse(strategy_study_yaml())
    study_id, task_id = registry.create(
        resolved.definition, resolved.config_hash, "a" * 64, actor="test"
    )
    with pytest.raises(ValueError, match="active strategy study"):
        registry.delete(study_id, actor="test")
    registry.transition(
        study_id,
        StrategyStudyStatus.QUEUED,
        StrategyStudyStatus.CANCELLED,
        stage=StrategyStudyStage.VALIDATE,
    )
    registry.delete(study_id, actor="test")
    with pytest.raises(KeyError, match="strategy study does not exist"):
        registry.get(study_id)
    task = queue.get(task_id)
    assert (task.subject_kind, task.subject_id) == (None, None)
    engine.dispose()
