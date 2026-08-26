"""验证数据中心运营证据和任务结果的持久化契约。"""

from datetime import UTC, date, datetime
from pathlib import Path

from sqlalchemy import inspect, text

from quant_research.data.quality.models import (
    QualityRuleResult,
    QualityRuleStatus,
    QualityRunSpec,
)
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.identifiers import QualityRunId
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.repositories import MetadataRepository
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.tasks.models import TaskOutcome, TaskStatus


def test_migration_and_operational_stage_updates_preserve_prior_evidence(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    first = datetime(2026, 8, 14, 10, tzinfo=UTC)
    second = datetime(2026, 8, 14, 11, tzinfo=UTC)

    repository.record_dataset_stage(
        DatasetKind.STOCK_DAILY_BAR,
        "LOCALIZE",
        completed_at=first,
        localized_through=date(2026, 8, 13),
    )
    repository.record_dataset_stage(
        DatasetKind.STOCK_DAILY_BAR, "CURATE", completed_at=second
    )

    state = repository.list_dataset_operational_states()[0]
    assert state.last_localized_at == first
    assert state.localized_through == date(2026, 8, 13)
    assert state.last_curated_at == second
    assert state.last_validated_at is None
    inspector = inspect(engine)
    assert {column["name"] for column in inspector.get_columns("task")} >= {
        "result_json"
    }
    assert {column["name"] for column in inspector.get_columns("task_attempt")} >= {
        "result_json"
    }
    engine.dispose()


def test_data_initialization_state_freezes_and_completes(tmp_path: Path) -> None:
    database = tmp_path / "initialization.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    started = datetime(2026, 8, 21, 1, tzinfo=UTC)

    initial = repository.begin_data_initialization(
        years=20,
        start_date=date(2006, 8, 20),
        end_date=date(2026, 8, 20),
        started_at=started,
    )
    resumed = repository.begin_data_initialization(
        years=20,
        start_date=date(2006, 8, 20),
        end_date=date(2026, 8, 20),
        started_at=started,
    )

    assert initial == resumed
    assert initial.status == "IN_PROGRESS"
    quality_run_id = QualityRunId.new()
    completed = repository.complete_data_initialization(
        catalog_hash="a" * 64,
        quality_run_id=quality_run_id,
        completed_at=started,
    )
    assert completed.status == "COMPLETED"
    assert completed.quality_run_id == quality_run_id
    assert repository.find_data_initialization() == completed
    engine.dispose()


def test_task_result_is_bounded_and_visible_on_task_and_attempt(tmp_path: Path) -> None:
    database = tmp_path / "tasks.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    now = datetime(2026, 8, 14, tzinfo=UTC)
    queue = TaskQueue(engine, clock=lambda: now)
    task_id = queue.enqueue("DATA_UPDATE", {}, 0)
    claimed = queue.claim("worker-1", now)
    assert claimed is not None

    result = {"run_id": "run-1", "data_hash": "a" * 64, "datasets": {}}
    queue.finish(
        claimed.attempt_id,
        "worker-1",
        TaskOutcome(status=TaskStatus.SUCCEEDED, result=result),
    )

    assert queue.get(task_id).result == result
    assert queue.list_attempts(task_id)[0].result == result
    engine.dispose()


def test_existing_database_upgrade_marks_old_quality_runs_incomplete(
    tmp_path: Path,
) -> None:
    database = tmp_path / "legacy.db"
    engine = create_sqlite_engine(database)
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE alembic_version (version_num VARCHAR(64) NOT NULL)")
        )
        connection.execute(
            text("INSERT INTO alembic_version VALUES ('data_center_operations')")
        )
        connection.execute(
            text(
                "CREATE TABLE quality_run ("
                "id VARCHAR(36) PRIMARY KEY, scope VARCHAR(16) NOT NULL, "
                "input_hash VARCHAR(64) NOT NULL, status VARCHAR(16) NOT NULL, "
                "started_at VARCHAR(32) NOT NULL, completed_at VARCHAR(32), "
                "created_at VARCHAR(32) NOT NULL)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO quality_run VALUES ("
                "'old-run', 'ALL', :hash, 'PASSED', :at, :at, :at)"
            ),
            {"hash": "a" * 64, "at": "2026-08-14T10:00:00+00:00"},
        )
    engine.dispose()

    upgrade_database(database)
    upgraded = create_sqlite_engine(database)
    with upgraded.connect() as connection:
        complete = connection.execute(
            text("SELECT results_complete FROM quality_run WHERE id = 'old-run'")
        ).scalar_one()
    assert complete == 0
    assert "quality_rule_result" in inspect(upgraded).get_table_names()
    upgraded.dispose()


def test_complete_quality_rule_results_round_trip_atomically(tmp_path: Path) -> None:
    database = tmp_path / "quality-results.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    now = datetime(2026, 8, 14, 10, tzinfo=UTC)
    result = QualityRuleResult(
        rule_id="canonical_schema",
        dataset=DatasetKind.STOCK_DAILY_BAR,
        status=QualityRuleStatus.PASS,
        severity=Severity.FATAL,
        title="Canonical Schema 一致",
        description="检查分区 Schema。",
        pass_criterion="不匹配分区数为 0。",
        scope={"partition_count": 1},
        actual=0,
        threshold=0,
    )

    record = repository.register_quality_run(
        QualityRunSpec(
            dataset_hashes={DatasetKind.STOCK_DAILY_BAR.value: "b" * 64},
            input_hash="a" * 64,
            scope="DATASET",
            started_at=now,
            completed_at=now,
            issues=(),
            rule_results=(result,),
            results_complete=True,
        )
    )

    loaded = repository.get_quality_run(record.id)
    assert loaded.results_complete is True
    assert loaded.rule_results == (result,)
    assert loaded.status == "PASSED"
    engine.dispose()
