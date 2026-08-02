"""Migration coverage for durable experiments and background tasks."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import IntegrityError

from quant_core.persistence.database import create_sqlite_engine, upgrade_database

EXPECTED_TABLES = {
    "audit_event",
    "experiment",
    "experiment_artifact",
    "experiment_metric",
    "experiment_tag",
    "task",
    "task_attempt",
}
UTC_TIMESTAMP = "2026-07-30T08:00:00+00:00"


def _config(engine: Engine) -> Config:
    config = Config()
    migrations = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "quant_core"
        / "persistence"
        / "migrations"
    )
    config.set_main_option("script_location", str(migrations))
    config.attributes["connection"] = engine
    return config


def _insert_experiment(engine: Engine, identifier: str = "experiment-1") -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO experiment ("
                "id, strategy_id, strategy_version, config_json, config_hash, "
                "snapshot_id, snapshot_manifest_hash, source_tree_hash, "
                "git_commit_hash, lockfile_hash, rulebook_version, fingerprint, "
                "status, research_mark, created_at"
                ") VALUES ("
                ":id, 'momentum', '1.0.0', '{}', :hash, NULL, :hash, :hash, "
                "NULL, :hash, 'cn-a-v1', :hash, 'CREATED', 'UNREVIEWED', :created_at"
                ")"
            ),
            {"id": identifier, "hash": "a" * 64, "created_at": UTC_TIMESTAMP},
        )


def _insert_task(engine: Engine, identifier: str = "task-1") -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO task ("
                "id, experiment_id, task_type, payload_json, status, priority, "
                "progress_json, created_at, available_at, updated_at"
                ") VALUES ("
                ":id, 'experiment-1', 'BACKTEST', '{}', 'QUEUED', 0, '{}', "
                ":created_at, :created_at, :created_at"
                ")"
            ),
            {"id": identifier, "created_at": UTC_TIMESTAMP},
        )


def test_base_to_head_creates_experiment_and_task_schema(tmp_path: Path) -> None:
    """Removing migration 0004 would leave the durable control tables absent."""
    database = tmp_path / "base-to-head.db"

    upgrade_database(database)

    engine = create_sqlite_engine(database)
    assert EXPECTED_TABLES.issubset(inspect(engine).get_table_names())
    with engine.connect() as connection:
        assert connection.execute(text("PRAGMA foreign_keys")).scalar_one() == 1
        assert connection.execute(text("PRAGMA journal_mode")).scalar_one() == "wal"
    engine.dispose()


def test_revision_0003_upgrades_linearly_to_0004(tmp_path: Path) -> None:
    """A wrong down_revision would break upgrades of existing 0003 databases."""
    engine = create_sqlite_engine(tmp_path / "incremental.db")
    config = _config(engine)
    command.upgrade(config, "0003_pipeline_stage_leases")
    assert EXPECTED_TABLES.isdisjoint(inspect(engine).get_table_names())

    command.upgrade(config, "0004_experiments_tasks")

    assert EXPECTED_TABLES.issubset(inspect(engine).get_table_names())
    with engine.connect() as connection:
        assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == (
            "0004_experiments_tasks"
        )
    engine.dispose()


def test_foreign_keys_exist_and_are_enforced(tmp_path: Path) -> None:
    """Dropping experiment/task ownership foreign keys would permit orphan rows."""
    database = tmp_path / "foreign-keys.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)

    foreign_keys = inspect(engine).get_foreign_keys("task_attempt")
    assert any(
        key["referred_table"] == "task" and key["constrained_columns"] == ["task_id"]
        for key in foreign_keys
    )
    with engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(
            text(
                "INSERT INTO task_attempt ("
                "id, task_id, attempt_no, status, started_at, progress_json"
                ") VALUES ('attempt-orphan', 'missing', 1, 'RUNNING', :started_at, '{}')"
            ),
            {"started_at": UTC_TIMESTAMP},
        )
    engine.dispose()


def test_status_checks_reject_unknown_enum_strings(tmp_path: Path) -> None:
    """Removing status CHECK constraints would allow unreadable persisted states."""
    database = tmp_path / "status-checks.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)

    for statement in (
        (
            "INSERT INTO experiment ("
            "id, strategy_id, strategy_version, config_json, config_hash, "
            "snapshot_manifest_hash, source_tree_hash, lockfile_hash, "
            "rulebook_version, fingerprint, status, research_mark, created_at"
            ") VALUES ("
            "'invalid-experiment', 'strategy', '1', '{}', :hash, :hash, :hash, "
            ":hash, 'rules', :hash, 'UNKNOWN', 'UNREVIEWED', :created_at)"
        ),
        (
            "INSERT INTO experiment ("
            "id, strategy_id, strategy_version, config_json, config_hash, "
            "snapshot_manifest_hash, source_tree_hash, lockfile_hash, "
            "rulebook_version, fingerprint, status, research_mark, created_at"
            ") VALUES ("
            "'invalid-mark', 'strategy', '1', '{}', :hash, :hash, :hash, :hash, "
            "'rules', :hash, 'CREATED', 'UNKNOWN', :created_at)"
        ),
    ):
        with engine.begin() as connection, pytest.raises(IntegrityError):
            connection.execute(
                text(statement), {"hash": "b" * 64, "created_at": UTC_TIMESTAMP}
            )

    _insert_experiment(engine)
    with engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(
            text(
                "INSERT INTO task ("
                "id, experiment_id, task_type, payload_json, status, priority, "
                "progress_json, created_at, available_at, updated_at"
                ") VALUES ("
                "'invalid-task', 'experiment-1', 'BACKTEST', '{}', 'UNKNOWN', 0, "
                "'{}', :created_at, :created_at, :created_at)"
            ),
            {"created_at": UTC_TIMESTAMP},
        )
    engine.dispose()


def test_attempt_numbers_are_unique_but_fingerprints_can_repeat(
    tmp_path: Path,
) -> None:
    """Duplicate attempt numbers are ambiguous, while repeated research is allowed."""
    database = tmp_path / "unique-keys.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    _insert_experiment(engine)
    _insert_experiment(engine, "experiment-2")
    _insert_task(engine)

    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO task_attempt ("
                "id, task_id, attempt_no, status, started_at, progress_json"
                ") VALUES ('attempt-1', 'task-1', 1, 'RUNNING', :started_at, '{}')"
            ),
            {"started_at": UTC_TIMESTAMP},
        )
    with engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(
            text(
                "INSERT INTO task_attempt ("
                "id, task_id, attempt_no, status, started_at, progress_json"
                ") VALUES ('attempt-2', 'task-1', 1, 'RUNNING', :started_at, '{}')"
            ),
            {"started_at": UTC_TIMESTAMP},
        )
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM experiment")).scalar_one() == 2
    engine.dispose()


def test_timestamp_columns_use_existing_utc_text_convention(tmp_path: Path) -> None:
    """Changing timestamps away from bounded ISO text would break repository parsing."""
    database = tmp_path / "timestamps.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)

    for table, required in {
        "experiment": {"created_at", "queued_at", "started_at", "completed_at"},
        "task": {
            "created_at",
            "available_at",
            "updated_at",
            "heartbeat_at",
            "completed_at",
        },
        "task_attempt": {"started_at", "heartbeat_at", "completed_at"},
        "audit_event": {"created_at"},
    }.items():
        columns = {column["name"]: column["type"] for column in inspect(engine).get_columns(table)}
        assert required.issubset(columns)
        for name in required:
            assert str(columns[name]) == "VARCHAR(32)"
    engine.dispose()


def test_domain_models_validate_fingerprint_metadata_and_exact_statuses() -> None:
    """Weak DTO validation would let malformed hashes or status drift reach storage."""
    from quant_core.experiments.models import (
        ExperimentSpec,
        ExperimentStatus,
        ResearchMark,
    )
    from quant_core.tasks.models import TaskStatus

    assert tuple(status.value for status in ExperimentStatus) == (
        "CREATED",
        "QUEUED",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
        "CANCELLED",
    )
    assert tuple(mark.value for mark in ResearchMark) == (
        "UNREVIEWED",
        "BASELINE",
        "CANDIDATE",
        "DISCARDED",
    )
    assert tuple(status.value for status in TaskStatus) == (
        "QUEUED",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
        "CANCEL_REQUESTED",
        "CANCELLED",
        "ORPHANED",
    )
    with pytest.raises(ValueError, match="config_hash"):
        ExperimentSpec(
            strategy_id="momentum",
            strategy_version="1",
            config={},
            config_hash="not-a-hash",
            snapshot_manifest_hash="b" * 64,
            source_tree_hash="c" * 64,
            lockfile_hash="d" * 64,
            rulebook_version="cn-a-v1",
            fingerprint="e" * 64,
            created_at=datetime(2026, 7, 30, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="config_hash must match canonical config"):
        ExperimentSpec(
            strategy_id="momentum",
            strategy_version="1",
            config={"window": 20},
            config_hash="a" * 64,
            snapshot_manifest_hash="b" * 64,
            source_tree_hash="c" * 64,
            lockfile_hash="d" * 64,
            rulebook_version="cn-a-v1",
            fingerprint="e" * 64,
            created_at=datetime(2026, 7, 30, tzinfo=UTC),
        )


def test_repository_persists_duplicate_experiments_and_utc_task_records(
    tmp_path: Path,
) -> None:
    """A uniqueness shortcut must not reuse research, and timestamps must round-trip UTC."""
    from quant_core.experiments.models import ExperimentSpec
    from quant_core.persistence.repositories import MetadataRepository
    from quant_core.tasks.models import TaskAttemptSpec, TaskSpec, TaskStatus

    database = tmp_path / "repository.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    created_at = datetime(2026, 7, 30, 16, tzinfo=UTC)
    experiment_spec = ExperimentSpec(
        strategy_id="momentum",
        strategy_version="1.0.0",
        config={"window": 20, "universe": ["SSE:600000"]},
        config_hash=hashlib.sha256(
            b'{"universe":["SSE:600000"],"window":20}'
        ).hexdigest(),
        snapshot_manifest_hash="b" * 64,
        source_tree_hash="c" * 64,
        lockfile_hash="d" * 64,
        rulebook_version="cn-a-v1",
        fingerprint="e" * 64,
        created_at=created_at,
    )

    first = repository.register_experiment(experiment_spec)
    repeated = repository.register_experiment(experiment_spec)
    task = repository.create_task(
        TaskSpec(
            experiment_id=first.id,
            task_type="BACKTEST",
            payload={"experiment_id": first.id},
            created_at=created_at,
            available_at=created_at + timedelta(seconds=2),
        )
    )
    attempt = repository.create_task_attempt(
        TaskAttemptSpec(
            task_id=task.id,
            attempt_no=1,
            worker_id="worker-1",
            started_at=created_at + timedelta(seconds=3),
            log_path="logs/task-1.ndjson",
        )
    )

    assert first.id != repeated.id
    assert repository.count_experiments_by_fingerprint("e" * 64) == 2
    assert first.config == {"universe": ["SSE:600000"], "window": 20}
    assert first.created_at == created_at
    assert task.status is TaskStatus.QUEUED
    assert task.available_at == created_at + timedelta(seconds=2)
    assert attempt.status is TaskStatus.RUNNING
    assert attempt.started_at == created_at + timedelta(seconds=3)
    with engine.connect() as connection:
        persisted_config = connection.execute(
            text("SELECT config_json FROM experiment WHERE id = :id"),
            {"id": first.id},
        ).scalar_one()
        persisted_time = connection.execute(
            text("SELECT created_at FROM experiment WHERE id = :id"),
            {"id": first.id},
        ).scalar_one()
    assert persisted_config == '{"universe":["SSE:600000"],"window":20}'
    assert persisted_time == "2026-07-30T16:00:00+00:00"
    engine.dispose()


def test_auxiliary_dtos_reject_noncanonical_persistent_values() -> None:
    """Artifact/audit DTOs must not bypass hash, JSON, or UTC invariants."""
    from quant_core.experiments.models import ExperimentArtifact
    from quant_core.tasks.models import AuditEventSpec

    naive_timestamp = datetime(2026, 7, 30, tzinfo=UTC).replace(tzinfo=None)
    with pytest.raises(ValueError, match="content_hash"):
        ExperimentArtifact(
            experiment_id="experiment-1",
            name="summary",
            artifact_type="BACKTEST_SUMMARY",
            path="artifacts/summary.json",
            content_hash="bad-hash",
            metadata={},
            created_at=datetime(2026, 7, 30, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="created_at must be timezone-aware"):
        AuditEventSpec(
            experiment_id="experiment-1",
            event_type="EXPERIMENT_CREATED",
            details={},
            created_at=naive_timestamp,
        )
