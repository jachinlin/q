"""验证独立 FactorStudy、Task、多 attempt 和人工结论持久化。"""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import inspect, text

from quant_research.factor_studies.config import FactorStudyConfigParser
from quant_research.factor_studies.models import (
    FactorDecisionMark,
    FactorStudyDecisionKey,
    FactorStudyDefinition,
    FactorStudyStage,
    FactorStudyStatus,
)
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.factor_studies import FactorStudyRegistry
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.tasks.models import TaskOutcome, TaskStatus


def _definition() -> FactorStudyDefinition:
    return FactorStudyConfigParser().parse_file(
        Path("configs/factor_studies/examples/factor_study.yaml")
    ).definition


def test_empty_database_contains_independent_factor_study_tables(tmp_path: Path) -> None:
    """空库升级后应同时具有纯策略实验表和五张独立研究表。"""
    database = tmp_path / "state.sqlite3"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    tables = set(inspect(engine).get_table_names())
    assert {
        "factor_study",
        "factor_study_tag",
        "factor_study_metric",
        "factor_study_artifact",
        "factor_study_decision",
    }.issubset(tables)
    assert "kind" not in {item["name"] for item in inspect(engine).get_columns("experiment")}
    engine.dispose()


def test_existing_strategy_database_upgrade_drops_legacy_factor_experiments(
    tmp_path: Path,
) -> None:
    """现有策略记录应去除 kind，旧因子实验和任务引用应直接丢弃。"""
    database = tmp_path / "legacy.sqlite3"
    engine = create_sqlite_engine(database)
    strategy_definition = '{"initial_run":{"kind":"STRATEGY_BACKTEST"},"kind":"STRATEGY_BACKTEST","name":"strategy"}'
    factor_definition = '{"initial_run":{"kind":"FACTOR_STUDY"},"kind":"FACTOR_STUDY","name":"factor"}'
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(64) NOT NULL)"))
        connection.execute(text("INSERT INTO alembic_version VALUES ('experiment_runs')"))
        connection.execute(text("CREATE TABLE experiment (id VARCHAR(36) PRIMARY KEY, name VARCHAR(128) NOT NULL, description TEXT NOT NULL, kind VARCHAR(32) NOT NULL, definition_json TEXT NOT NULL, definition_hash VARCHAR(64) NOT NULL, baseline_run_id VARCHAR(36), created_at VARCHAR(32) NOT NULL, CONSTRAINT ck_experiment_kind CHECK (kind IN ('STRATEGY_BACKTEST','FACTOR_STUDY')))"))
        connection.execute(text("CREATE TABLE run (id VARCHAR(36) PRIMARY KEY, experiment_id VARCHAR(36) NOT NULL REFERENCES experiment(id) ON DELETE CASCADE, config_json TEXT NOT NULL, config_hash VARCHAR(64) NOT NULL)"))
        connection.execute(text("CREATE TABLE task (id VARCHAR(36) PRIMARY KEY, subject_kind VARCHAR(32), subject_id VARCHAR(64), task_type VARCHAR(64))"))
        connection.execute(
            text("INSERT INTO experiment VALUES ('strategy','strategy','', 'STRATEGY_BACKTEST', :definition, :hash, NULL, '2026-01-01T00:00:00+00:00')"),
            {"definition": strategy_definition, "hash": "a" * 64},
        )
        connection.execute(
            text("INSERT INTO experiment VALUES ('factor','factor','', 'FACTOR_STUDY', :definition, :hash, NULL, '2026-01-01T00:00:00+00:00')"),
            {"definition": factor_definition, "hash": "b" * 64},
        )
        connection.execute(text("INSERT INTO run VALUES ('strategy-run','strategy','{\"kind\":\"STRATEGY_BACKTEST\"}','c')"))
        connection.execute(text("INSERT INTO run VALUES ('factor-run','factor','{\"kind\":\"FACTOR_STUDY\"}','d')"))
        connection.execute(text("INSERT INTO task VALUES ('factor-task','EXPERIMENT_RUN','factor-run','EXPERIMENT_RUN')"))
    engine.dispose()

    upgrade_database(database)
    upgraded = create_sqlite_engine(database)
    with upgraded.connect() as connection:
        assert connection.execute(text("SELECT id FROM experiment")).scalars().all() == ["strategy"]
        assert connection.execute(text("SELECT id FROM task")).scalars().all() == []
        definition_json = connection.execute(text("SELECT definition_json FROM experiment")).scalar_one()
        config_json = connection.execute(text("SELECT config_json FROM run")).scalar_one()
    assert '"kind"' not in definition_json
    assert '"kind"' not in config_json
    assert "kind" not in {item["name"] for item in inspect(upgraded).get_columns("experiment")}
    upgraded.dispose()


def test_atomic_create_retry_reuses_task_and_creates_attempt(tmp_path: Path) -> None:
    """失败重试复用任务和冻结配置，并由下一次 claim 创建新 attempt。"""
    database = tmp_path / "state.sqlite3"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    registry = FactorStudyRegistry(engine)
    queue = TaskQueue(engine, task_log_root=tmp_path / "logs")
    study_id, task_id = registry.create(
        _definition(), "b" * 64, "a" * 64, actor="test"
    )
    task = queue.get(task_id)
    assert (task.task_type, task.subject_kind, task.subject_id) == (
        "FACTOR_STUDY",
        "FACTOR_STUDY",
        study_id,
    )
    first = queue.claim("worker", datetime(2099, 1, 1, tzinfo=UTC))
    assert first is not None and first.attempt_no == 1
    registry.transition(
        study_id,
        FactorStudyStatus.QUEUED,
        FactorStudyStatus.RUNNING,
        stage=FactorStudyStage.VALIDATE,
    )
    registry.transition(
        study_id,
        FactorStudyStatus.RUNNING,
        FactorStudyStatus.FAILED,
        stage=FactorStudyStage.ANALYZE_FACTORS,
        error={"code": "TEST"},
    )
    queue.finish(
        first.attempt_id,
        "worker",
        TaskOutcome(status=TaskStatus.FAILED, error={"code": "TEST"}),
    )

    assert registry.retry(study_id, actor="test") == task_id
    second = queue.claim("worker", datetime(2099, 1, 2, tzinfo=UTC))
    assert second is not None and second.id == task_id and second.attempt_no == 2
    assert registry.get(study_id).config_hash == "b" * 64
    engine.dispose()


def test_decisions_are_idempotent_and_validate_dimensions(tmp_path: Path) -> None:
    """四维决策键应支持更新、清除并拒绝配置外维度。"""
    database = tmp_path / "state.sqlite3"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    registry = FactorStudyRegistry(engine)
    study_id, _ = registry.create(_definition(), "b" * 64, "a" * 64, actor="test")
    registry.transition(
        study_id,
        FactorStudyStatus.QUEUED,
        FactorStudyStatus.RUNNING,
        stage=FactorStudyStage.VALIDATE,
    )
    registry.transition(
        study_id,
        FactorStudyStatus.RUNNING,
        FactorStudyStatus.SUCCEEDED,
        stage=FactorStudyStage.PUBLISH,
        artifact_dir=str(tmp_path / "artifacts"),
        manifest_hash="c" * 64,
    )
    key = FactorStudyDecisionKey(
        signal_variant="DIRECTION_ADJUSTED",
        label_kind="THEORETICAL_FORWARD_RETURN",
        factor_ref="book_to_price_mrq",
        horizon=5,
    )
    registry.decide(study_id, key, FactorDecisionMark.CANDIDATE, "first", actor="a")
    registry.decide(study_id, key, FactorDecisionMark.DISCARDED, "second", actor="b")
    assert [(item.mark, item.note) for item in registry.get(study_id).decisions] == [
        (FactorDecisionMark.DISCARDED, "second")
    ]
    registry.decide(study_id, key, FactorDecisionMark.UNREVIEWED, "", actor="b")
    assert registry.get(study_id).decisions == ()
    with pytest.raises(ValueError, match="horizon"):
        registry.decide(
            study_id,
            key.model_copy(update={"horizon": 999}),
            FactorDecisionMark.CANDIDATE,
            "",
            actor="b",
        )
    engine.dispose()
