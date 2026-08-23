"""验证统一 Experiment → Run → Task 的 SQLite 事务和不可覆盖重跑。"""

from pathlib import Path

from sqlalchemy import inspect

from quant_research.experiments.config import ExperimentConfigParser
from quant_research.experiments.models import ResearchMark
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.experiment_runs import (
    ExperimentRunRegistry,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from tests.unit.experiments.test_unified_models import experiment_yaml


def test_create_mark_and_rerun_use_distinct_run_and_task(tmp_path: Path) -> None:
    database = tmp_path / "state.sqlite3"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    registry = ExperimentRunRegistry(engine)
    definition = ExperimentConfigParser().parse_experiment(experiment_yaml()).definition
    experiment_id, run_id, task_id = registry.create(definition, "a" * 64, actor="test")
    aggregate = registry.get_experiment(experiment_id)
    assert aggregate.runs[0].task_id == task_id
    task = TaskQueue(engine, task_log_root=tmp_path / "logs").get(task_id)
    assert (task.task_type, task.subject_kind, task.subject_id) == (
        "EXPERIMENT_RUN",
        "EXPERIMENT_RUN",
        run_id,
    )

    registry.mark(run_id, ResearchMark.BASELINE, actor="test")
    new_run_id, new_task_id = registry.rerun(run_id, "b" * 64, actor="test")
    assert new_run_id != run_id and new_task_id != task_id
    assert registry.get_experiment(experiment_id).experiment.baseline_run_id == run_id
    assert (
        registry.get_run(new_run_id).config_hash == registry.get_run(run_id).config_hash
    )
    engine.dispose()


def test_empty_database_migration_contains_only_unified_research_tables(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state.sqlite3"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    tables = set(inspect(engine).get_table_names())
    assert {"experiment", "run", "run_metric", "run_artifact", "task"}.issubset(tables)
    assert not {"research_family", "research_variant", "factor_study"}.intersection(
        tables
    )
    engine.dispose()


def test_failed_run_output_cleanup_is_transactional(tmp_path: Path) -> None:
    database = tmp_path / "state.sqlite3"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    registry = ExperimentRunRegistry(engine)
    definition = ExperimentConfigParser().parse_experiment(experiment_yaml()).definition
    _, run_id, _ = registry.create(definition, "a" * 64, actor="test")
    registry.register_outputs(
        run_id,
        {"annualized_return": (0.1, "ratio", None, None)},
        (
            {
                "artifact_type": "metrics",
                "relative_path": "metrics.json",
                "content_hash": "b" * 64,
                "byte_count": 10,
                "row_count": None,
                "schema": None,
            },
        ),
    )
    assert registry.get_run(run_id).metrics
    assert registry.get_run(run_id).artifacts

    registry.discard_outputs(run_id)

    assert registry.get_run(run_id).metrics == ()
    assert registry.get_run(run_id).artifacts == ()
    engine.dispose()
