"""Integration coverage for the durable SQLite task queue."""

from __future__ import annotations

import importlib
import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from pydantic import ValidationError
from sqlalchemy import Engine, inspect, text

from quant_core.errors import QuantError
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.tasks.models import TaskOutcome, TaskProgress, TaskStatus

NOW = datetime(2026, 7, 30, 8, tzinfo=UTC)
NOW_TEXT = NOW.isoformat()


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
                "NULL, :hash, 'cn-a-v1', :hash, 'CREATED', 'UNREVIEWED', :now"
                ")"
            ),
            {"id": identifier, "hash": "a" * 64, "now": NOW_TEXT},
        )


def _insert_0004_queue_history(engine: Engine) -> None:
    _insert_experiment(engine)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO task ("
                "id, experiment_id, task_type, payload_json, status, priority, "
                "progress_json, created_at, available_at, updated_at, heartbeat_at"
                ") VALUES ("
                "'task-1', 'experiment-1', 'BACKTEST', :payload, "
                "'RUNNING', 7, :progress, :now, :now, :now, :now"
                ")"
            ),
            {
                "now": NOW_TEXT,
                "payload": '{"window":20}',
                "progress": '{"stage":"run"}',
            },
        )
        connection.execute(
            text(
                "INSERT INTO task_attempt ("
                "id, task_id, attempt_no, status, worker_id, started_at, "
                "heartbeat_at, progress_json"
                ") VALUES ("
                "'attempt-1', 'task-1', 1, 'RUNNING', 'worker-1', :now, :now, "
                ":progress"
                ")"
            ),
            {"now": NOW_TEXT, "progress": '{"stage":"run"}'},
        )
        connection.execute(
            text(
                "INSERT INTO audit_event ("
                "experiment_id, task_id, event_type, actor, details_json, created_at"
                ") VALUES ("
                "'experiment-1', 'task-1', 'TASK_CLAIMED', 'worker-1', '{}', :now"
                ")"
            ),
            {"now": NOW_TEXT},
        )


def _task_columns(engine: Engine) -> dict[str, dict[str, Any]]:
    return {column["name"]: column for column in inspect(engine).get_columns("task")}


def _database_snapshot(
    engine: Engine,
) -> tuple[tuple[tuple[Any, ...], ...], dict[str, tuple[tuple[Any, ...], ...]]]:
    """Capture all user schema objects and rows around a fail-closed migration."""
    with engine.connect() as connection:
        schema = tuple(
            tuple(row)
            for row in connection.execute(
                text(
                    "SELECT type, name, tbl_name, sql FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
                )
            )
        )
        table_names = [
            row[1] for row in schema if row[0] == "table"
        ]
        rows = {
            table_name: tuple(
                tuple(row)
                for row in connection.exec_driver_sql(
                    f'SELECT * FROM "{table_name}" ORDER BY rowid'
                )
            )
            for table_name in table_names
        }
    return schema, rows


def _assert_task_relational_contract(engine: Engine) -> None:
    """Verify rebuilt queue tables retain foreign keys, constraints, and indexes."""
    with engine.connect() as connection:
        foreign_keys = {
            table_name: {
                (row[3], row[2], row[4], row[6])
                for row in connection.exec_driver_sql(
                    f'PRAGMA foreign_key_list("{table_name}")'
                )
            }
            for table_name in ("task", "task_attempt", "audit_event")
        }
        index_names = {
            row[0]
            for row in connection.execute(
                text(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'index' AND name NOT LIKE 'sqlite_%'"
                )
            )
        }
        table_sql = {
            row.name: row.sql.upper()
            for row in connection.execute(
                text(
                    "SELECT name, sql FROM sqlite_master "
                    "WHERE type = 'table' AND name IN ('task', 'task_attempt')"
                )
            )
        }
        violations = connection.execute(text("PRAGMA foreign_key_check")).all()

    assert foreign_keys == {
        "task": {("experiment_id", "experiment", "id", "CASCADE")},
        "task_attempt": {("task_id", "task", "id", "CASCADE")},
        "audit_event": {
            ("experiment_id", "experiment", "id", "SET NULL"),
            ("task_id", "task", "id", "SET NULL"),
        },
    }
    assert {
        "ix_task_experiment",
        "ix_task_queue",
        "ix_task_attempt_task",
        "ix_audit_event_experiment_created",
        "ix_audit_event_task_created",
    } <= index_names
    assert "CK_TASK_STATUS" in table_sql["task"]
    assert "CK_TASK_ATTEMPT_STATUS" in table_sql["task_attempt"]
    assert "CK_TASK_ATTEMPT_POSITIVE" in table_sql["task_attempt"]
    assert "UQ_TASK_ATTEMPT_NO" in table_sql["task_attempt"]
    assert violations == []


def test_0004_to_0005_preserves_queue_history_and_adds_runtime_schema(
    tmp_path: Path,
) -> None:
    """Rebuilding the parent task table must retain task, attempt, and audit rows."""
    engine = create_sqlite_engine(tmp_path / "incremental.db")
    config = _config(engine)
    command.upgrade(config, "0004_experiments_tasks")
    _insert_0004_queue_history(engine)

    command.upgrade(config, "head")

    columns = _task_columns(engine)
    _assert_task_relational_contract(engine)
    assert {
        name: (str(columns[name]["type"]), columns[name]["nullable"])
        for name in (
            "experiment_id",
            "idempotency_key",
            "worker_id",
            "locked_at",
            "error_json",
        )
    } == {
        "experiment_id": ("VARCHAR(36)", True),
        "idempotency_key": ("VARCHAR(128)", True),
        "worker_id": ("VARCHAR(128)", True),
        "locked_at": ("VARCHAR(32)", True),
        "error_json": ("TEXT", True),
    }
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one() == "0005_task_queue_runtime"
        assert connection.execute(
            text("SELECT id, experiment_id, priority FROM task")
        ).one() == ("task-1", "experiment-1", 7)
        assert connection.execute(
            text("SELECT id, task_id, attempt_no FROM task_attempt")
        ).one() == ("attempt-1", "task-1", 1)
        assert connection.execute(
            text("SELECT task_id, event_type FROM audit_event")
        ).one() == ("task-1", "TASK_CLAIMED")
    engine.dispose()


def test_0005_indexes_encode_claim_order_and_active_idempotency(
    tmp_path: Path,
) -> None:
    """Changing either index would permit duplicate work or slower wrong-order claims."""
    database = tmp_path / "indexes.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)

    with engine.connect() as connection:
        index_sql = {
            row.name: " ".join(row.sql.upper().split())
            for row in connection.execute(
                text(
                    "SELECT name, sql FROM sqlite_master "
                    "WHERE type = 'index' AND tbl_name = 'task' AND sql IS NOT NULL"
                )
            )
        }
        queue_columns = [
            (row.name, row.descending)
            for row in connection.execute(
                text(
                    "SELECT name, [desc] AS descending "
                    "FROM pragma_index_xinfo('ix_task_queue') "
                    "WHERE key = 1 ORDER BY seqno"
                )
            )
        ]

    assert queue_columns == [
        ("status", 0),
        ("available_at", 0),
        ("priority", 1),
        ("created_at", 0),
        ("id", 0),
    ]
    unique_sql = index_sql["uq_task_active_idempotency"]
    assert "UNIQUE INDEX" in unique_sql
    assert "TASK_TYPE, COALESCE(EXPERIMENT_ID, ''), IDEMPOTENCY_KEY" in unique_sql
    assert "IDEMPOTENCY_KEY IS NOT NULL" in unique_sql
    assert "STATUS IN ('QUEUED', 'RUNNING', 'CANCEL_REQUESTED')" in unique_sql
    engine.dispose()


def test_0005_downgrade_upgrade_round_trip_preserves_non_null_queue_history(
    tmp_path: Path,
) -> None:
    """The linear migration must be reversible without discarding existing history."""
    engine = create_sqlite_engine(tmp_path / "round-trip.db")
    config = _config(engine)
    command.upgrade(config, "0004_experiments_tasks")
    _insert_0004_queue_history(engine)
    command.upgrade(config, "0005_task_queue_runtime")

    command.downgrade(config, "0004_experiments_tasks")

    downgraded = _task_columns(engine)
    _assert_task_relational_contract(engine)
    assert downgraded["experiment_id"]["nullable"] is False
    assert {
        "idempotency_key",
        "worker_id",
        "locked_at",
        "error_json",
    }.isdisjoint(downgraded)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM task")).scalar_one() == 1
        assert connection.execute(
            text("SELECT count(*) FROM task_attempt")
        ).scalar_one() == 1
        assert connection.execute(
            text("SELECT count(*) FROM audit_event")
        ).scalar_one() == 1

    command.upgrade(config, "head")

    assert _task_columns(engine)["experiment_id"]["nullable"] is True
    _assert_task_relational_contract(engine)
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM task")).scalar_one() == 1
        assert connection.execute(
            text("SELECT count(*) FROM task_attempt")
        ).scalar_one() == 1
    engine.dispose()


def test_0005_downgrade_fails_closed_before_ddl_for_standalone_tasks(
    tmp_path: Path,
) -> None:
    """0004 cannot encode NULL ownership, so rejection must precede every DDL write."""
    engine = create_sqlite_engine(tmp_path / "standalone-downgrade.db")
    config = _config(engine)
    command.upgrade(config, "head")
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO task ("
                "id, experiment_id, task_type, payload_json, status, priority, "
                "progress_json, created_at, available_at, updated_at"
                ") VALUES ("
                "'standalone-task', NULL, 'BACKTEST', '{}', 'QUEUED', 0, "
                "'{}', :now, :now, :now"
                ")"
            ),
            {"now": NOW_TEXT},
        )
        connection.execute(
            text(
                "INSERT INTO audit_event ("
                "experiment_id, task_id, event_type, actor, details_json, created_at"
                ") VALUES ("
                "NULL, 'standalone-task', 'TASK_ENQUEUED', 'system', '{}', :now"
                ")"
            ),
            {"now": NOW_TEXT},
        )

    before = _database_snapshot(engine)

    with pytest.raises(RuntimeError, match="standalone task"):
        command.downgrade(config, "0004_experiments_tasks")

    assert _database_snapshot(engine) == before
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one() == "0005_task_queue_runtime"
        assert connection.execute(text("PRAGMA foreign_key_check")).all() == []
    engine.dispose()


def _queue_model(name: str) -> type[Any]:
    models = importlib.import_module("quant_core.tasks.models")
    model = getattr(models, name, None)
    assert model is not None, f"quant_core.tasks.models.{name} is missing"
    return model


@pytest.mark.parametrize(
    "values",
    [
        {"stage": "", "completed": 0, "total": 0, "message": ""},
        {"stage": "run", "completed": -1, "total": 1, "message": ""},
        {"stage": "run", "completed": 2, "total": 1, "message": ""},
        {"stage": "run", "completed": 0, "total": -1, "message": ""},
        {"stage": "run", "completed": True, "total": 1, "message": ""},
        {"stage": "run", "completed": 0, "total": 1, "message": "x" * 2049},
        {
            "stage": "run",
            "completed": 0,
            "total": 1,
            "message": "",
            "unexpected": "field",
        },
    ],
)
def test_task_progress_rejects_invalid_bounds_and_shape(values: dict[str, Any]) -> None:
    """Malformed progress must never reach task and attempt rows."""
    task_progress = _queue_model("TaskProgress")

    with pytest.raises(ValidationError):
        task_progress.model_validate(values)


def test_task_progress_is_strict_frozen_and_has_exact_fields() -> None:
    """Queue callers must receive one immutable, drift-resistant progress contract."""
    task_progress = _queue_model("TaskProgress")
    progress = task_progress(stage="queued", completed=0, total=0, message="")

    assert progress.model_dump() == {
        "stage": "queued",
        "completed": 0,
        "total": 0,
        "message": "",
    }
    with pytest.raises(ValidationError):
        progress.completed = 1
    with pytest.raises(ValidationError):
        task_progress(stage="queued", completed="0", total=0, message="")


def test_task_outcome_enforces_terminal_status_error_contract() -> None:
    """Success/cancellation cannot hide errors and failure cannot omit one."""
    models = importlib.import_module("quant_core.tasks.models")
    task_outcome = _queue_model("TaskOutcome")

    failed = task_outcome(
        status=models.TaskStatus.FAILED,
        error={"code": "BACKTEST_FAILED"},
    )
    assert failed.error == {"code": "BACKTEST_FAILED"}
    for status, error in (
        (models.TaskStatus.RUNNING, None),
        (models.TaskStatus.FAILED, None),
        (models.TaskStatus.SUCCEEDED, {"code": "unexpected"}),
        (models.TaskStatus.CANCELLED, {"code": "unexpected"}),
    ):
        with pytest.raises(ValidationError):
            task_outcome(status=status, error=error)


def test_claimed_task_normalizes_aware_timestamps_to_utc() -> None:
    """A claim crossing a timezone boundary must expose one UTC instant."""
    claimed_task = _queue_model("ClaimedTask")
    progress = _queue_model("TaskProgress")(
        stage="queued", completed=0, total=0, message=""
    )

    claimed = claimed_task(
        id="task-1",
        attempt_id="attempt-1",
        attempt_no=1,
        experiment_id=None,
        task_type="BACKTEST",
        payload={"window": 20},
        priority=7,
        worker_id="worker-1",
        progress=progress,
        claimed_at=datetime(
            2026,
            7,
            30,
            16,
            tzinfo=timezone(timedelta(hours=8)),
        ),
    )

    assert claimed.claimed_at == NOW
    assert claimed.claimed_at.tzinfo is UTC
    with pytest.raises(ValidationError, match="timezone-aware"):
        claimed_task(**{**claimed.model_dump(), "claimed_at": NOW.replace(tzinfo=None)})


@pytest.fixture
def engine(tmp_path: Path) -> Engine:
    database = tmp_path / "queue.db"
    upgrade_database(database)
    value = create_sqlite_engine(database)
    try:
        yield value
    finally:
        value.dispose()


def _queue(engine: Engine, clock: Any = None) -> Any:
    module = importlib.import_module("quant_core.tasks.queue")
    queue_type = getattr(module, "TaskQueue", None)
    assert queue_type is not None, "quant_core.tasks.queue.TaskQueue is missing"
    return queue_type(engine, clock=clock or (lambda: NOW))


def _task_row(engine: Engine, task_id: str) -> dict[str, Any]:
    with engine.connect() as connection:
        row = connection.execute(
            text("SELECT * FROM task WHERE id = :task_id"), {"task_id": task_id}
        ).mappings().one()
    return dict(row)


def _attempt_rows(engine: Engine, task_id: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT * FROM task_attempt WHERE task_id = :task_id "
                "ORDER BY attempt_no"
            ),
            {"task_id": task_id},
        ).mappings()
        return [dict(row) for row in rows]


def _task_audit(engine: Engine, task_id: str) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT event_type, actor, details_json, created_at "
                "FROM audit_event WHERE task_id = :task_id ORDER BY id"
            ),
            {"task_id": task_id},
        ).mappings()
        return [
            {
                **dict(row),
                "details": json.loads(row["details_json"]),
            }
            for row in rows
        ]


def test_enqueue_without_key_creates_fresh_canonical_standalone_tasks(
    engine: Engine,
) -> None:
    """Absent a key, equal standalone requests must remain distinct durable work."""
    queue = _queue(engine)
    payload = MappingProxyType({"z": 2, "a": 1})

    first = queue.enqueue("BACKTEST", payload, 7)
    second = queue.enqueue("BACKTEST", payload, 7)

    assert first != second
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT experiment_id, payload_json, status, priority, "
                "progress_json, available_at FROM task ORDER BY id"
            )
        ).all()
    assert rows == [
        (
            None,
            '{"a":1,"z":2}',
            "QUEUED",
            7,
            '{"completed":0,"message":"","stage":"queued","total":0}',
            NOW_TEXT,
        ),
        (
            None,
            '{"a":1,"z":2}',
            "QUEUED",
            7,
            '{"completed":0,"message":"","stage":"queued","total":0}',
            NOW_TEXT,
        ),
    ]
    assert [event["event_type"] for event in _task_audit(engine, first)] == [
        "TASK_ENQUEUED"
    ]


def test_enqueue_rejects_invalid_inputs_before_any_write(engine: Engine) -> None:
    """Invalid identities, times, priorities, and bounded JSON cannot leave rows."""
    queue = _queue(engine)
    naive = NOW.replace(tzinfo=None)

    invalid_calls = (
        lambda: queue.enqueue("", {}, 0),
        lambda: queue.enqueue("BACKTEST", {}, True),
        lambda: queue.enqueue("BACKTEST", {}, 0, available_at=naive),
        lambda: queue.enqueue("BACKTEST", {}, 0, idempotency_key=""),
        lambda: queue.enqueue("BACKTEST", {}, 0, idempotency_key="k" * 129),
        lambda: queue.enqueue("BACKTEST", {"blob": "x" * 1_048_577}, 0),
    )
    for invalid in invalid_calls:
        with pytest.raises((TypeError, ValueError)):
            invalid()

    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM task")).scalar_one() == 0
        assert connection.execute(
            text("SELECT count(*) FROM audit_event")
        ).scalar_one() == 0


def test_enqueue_requires_existing_optional_experiment(engine: Engine) -> None:
    """A supplied experiment identity must be checked in the same write transaction."""
    queue = _queue(engine)

    with pytest.raises(QuantError) as captured:
        queue.enqueue("BACKTEST", {}, 0, experiment_id="missing")

    assert captured.value.detail.code == "TASK_EXPERIMENT_NOT_FOUND"
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM task")).scalar_one() == 0

    _insert_experiment(engine)
    task_id = queue.enqueue("BACKTEST", {}, 0, experiment_id="experiment-1")
    assert _task_row(engine, task_id)["experiment_id"] == "experiment-1"


def test_active_idempotency_returns_same_task_and_audits_canonical_hit(
    engine: Engine,
) -> None:
    """Equivalent map order must deduplicate without creating another task or attempt."""
    queue = _queue(engine)

    first = queue.enqueue(
        "BACKTEST",
        {"window": 20, "symbols": ["SSE:600000"]},
        5,
        idempotency_key="request-1",
        actor="scheduler",
        request_id="enqueue-1",
    )
    duplicate = queue.enqueue(
        "BACKTEST",
        {"symbols": ["SSE:600000"], "window": 20},
        99,
        idempotency_key="request-1",
        actor="scheduler",
        request_id="enqueue-2",
    )

    assert duplicate == first
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM task")).scalar_one() == 1
        assert connection.execute(
            text("SELECT count(*) FROM task_attempt")
        ).scalar_one() == 0
    audit = _task_audit(engine, first)
    assert [event["event_type"] for event in audit] == [
        "TASK_ENQUEUED",
        "TASK_ENQUEUE_DEDUPLICATED",
    ]
    assert audit[-1]["details"]["request_id"] == "enqueue-2"
    assert _task_row(engine, first)["priority"] == 5


def test_active_idempotency_rejects_payload_mismatch_without_mutation(
    engine: Engine,
) -> None:
    """One namespace cannot silently reinterpret an active task's payload."""
    queue = _queue(engine)
    task_id = queue.enqueue(
        "BACKTEST", {"window": 20}, 0, idempotency_key="request-1"
    )

    with pytest.raises(QuantError) as captured:
        queue.enqueue(
            "BACKTEST", {"window": 60}, 0, idempotency_key="request-1"
        )

    assert captured.value.detail.code == "TASK_IDEMPOTENCY_CONFLICT"
    assert _task_row(engine, task_id)["payload_json"] == '{"window":20}'
    assert [event["event_type"] for event in _task_audit(engine, task_id)] == [
        "TASK_ENQUEUED"
    ]


def test_terminal_idempotency_key_creates_new_task_instead_of_restarting_old(
    engine: Engine,
) -> None:
    """A terminal key no longer reserves the namespace, but enqueue cannot mutate it."""
    queue = _queue(engine)
    old_id = queue.enqueue("BACKTEST", {}, 0, idempotency_key="request-1")
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE task SET status = 'FAILED', completed_at = :now "
                "WHERE id = :task_id"
            ),
            {"now": NOW_TEXT, "task_id": old_id},
        )

    new_id = queue.enqueue("BACKTEST", {}, 0, idempotency_key="request-1")

    assert new_id != old_id
    assert _task_row(engine, old_id)["status"] == "FAILED"
    assert _task_row(engine, new_id)["status"] == "QUEUED"


def test_claim_orders_by_priority_created_at_id_and_available_time(
    engine: Engine,
) -> None:
    """Mutating any ORDER BY term or availability predicate changes claim order."""
    clock = [NOW]
    queue = _queue(engine, clock=lambda: clock[0])
    low = queue.enqueue("LOW", {}, 1)
    clock[0] = NOW + timedelta(seconds=1)
    equal_a = queue.enqueue("EQUAL", {"slot": "a"}, 5)
    equal_b = queue.enqueue("EQUAL", {"slot": "b"}, 5)
    future = queue.enqueue(
        "FUTURE",
        {},
        100,
        available_at=NOW + timedelta(minutes=1),
    )

    claimed = [
        queue.claim("worker-1", NOW + timedelta(seconds=2)),
        queue.claim("worker-1", NOW + timedelta(seconds=2)),
        queue.claim("worker-1", NOW + timedelta(seconds=2)),
    ]

    assert [item.id for item in claimed if item is not None] == [
        *sorted((equal_a, equal_b)),
        low,
    ]
    assert queue.claim("worker-1", NOW + timedelta(seconds=2)) is None
    available = queue.claim("worker-1", NOW + timedelta(minutes=1))
    assert available is not None
    assert available.id == future


def test_claim_atomically_persists_attempt_ownership_progress_and_audit(
    engine: Engine,
) -> None:
    """A returned claim must already be a complete durable ownership record."""
    queue = _queue(engine)
    task_id = queue.enqueue("BACKTEST", {"window": 20}, 7)

    claimed = queue.claim("worker-1", NOW + timedelta(seconds=1))

    assert claimed is not None
    assert claimed.id == task_id
    assert claimed.attempt_no == 1
    assert claimed.worker_id == "worker-1"
    assert claimed.progress == TaskProgress(
        stage="queued", completed=0, total=0, message=""
    )
    task = _task_row(engine, task_id)
    attempts = _attempt_rows(engine, task_id)
    assert task["status"] == attempts[0]["status"] == "RUNNING"
    assert task["worker_id"] == attempts[0]["worker_id"] == "worker-1"
    assert task["locked_at"] == NOW_TEXT.replace(
        "08:00:00", "08:00:01"
    )
    assert task["heartbeat_at"] == attempts[0]["heartbeat_at"]
    assert task["progress_json"] == attempts[0]["progress_json"]
    assert [event["event_type"] for event in _task_audit(engine, task_id)] == [
        "TASK_ENQUEUED",
        "TASK_CLAIMED",
    ]


def test_heartbeat_is_owner_fenced_and_synchronizes_task_attempt(
    engine: Engine,
) -> None:
    """Only the current attempt owner can persist matching task/attempt progress."""
    queue = _queue(engine)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    claimed = queue.claim("worker-1", NOW)
    assert claimed is not None
    progress = TaskProgress(stage="factor", completed=2, total=5, message="working")
    heartbeat_at = datetime(
        2026, 7, 30, 16, 0, 10, tzinfo=timezone(timedelta(hours=8))
    )

    queue.heartbeat(claimed.attempt_id, "worker-1", progress, heartbeat_at)

    task = _task_row(engine, task_id)
    attempt = _attempt_rows(engine, task_id)[0]
    assert task["heartbeat_at"] == attempt["heartbeat_at"] == (
        "2026-07-30T08:00:10+00:00"
    )
    assert json.loads(task["progress_json"]) == progress.model_dump()
    assert task["progress_json"] == attempt["progress_json"]

    with pytest.raises(QuantError) as wrong_owner:
        queue.heartbeat(claimed.attempt_id, "worker-2", progress, heartbeat_at)
    with pytest.raises(QuantError) as missing:
        queue.heartbeat("missing", "worker-1", progress, heartbeat_at)
    assert wrong_owner.value.detail.code == "TASK_OWNERSHIP_CONFLICT"
    assert missing.value.detail.code == "TASK_ATTEMPT_NOT_FOUND"


def test_queued_cancel_is_terminal_idempotent_and_has_no_attempt(
    engine: Engine,
) -> None:
    """Cancelling queued work must complete it directly without inventing an attempt."""
    clock = [NOW]
    queue = _queue(engine, clock=lambda: clock[0])
    task_id = queue.enqueue("BACKTEST", {}, 0)
    clock[0] = NOW + timedelta(seconds=5)

    queue.request_cancel(task_id, "user-1")
    queue.request_cancel(task_id, "user-1")

    task = _task_row(engine, task_id)
    assert task["status"] == "CANCELLED"
    assert task["completed_at"] == "2026-07-30T08:00:05+00:00"
    assert _attempt_rows(engine, task_id) == []
    assert [event["event_type"] for event in _task_audit(engine, task_id)] == [
        "TASK_ENQUEUED",
        "TASK_CANCELLED",
    ]


def test_running_cancel_request_preserves_owner_until_cancel_finish(
    engine: Engine,
) -> None:
    """Cooperative cancellation must fence the attempt until its owner acknowledges it."""
    queue = _queue(engine)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    claimed = queue.claim("worker-1", NOW)
    assert claimed is not None

    queue.request_cancel(task_id, "user-1")
    queue.request_cancel(task_id, "user-1")

    task = _task_row(engine, task_id)
    attempt = _attempt_rows(engine, task_id)[0]
    assert task["status"] == attempt["status"] == "CANCEL_REQUESTED"
    assert task["worker_id"] == attempt["worker_id"] == "worker-1"
    with pytest.raises(QuantError) as invalid_finish:
        queue.finish(
            claimed.attempt_id,
            "worker-1",
            TaskOutcome(status=TaskStatus.SUCCEEDED),
        )
    assert invalid_finish.value.detail.code == "TASK_STATE_CONFLICT"

    queue.finish(
        claimed.attempt_id,
        "worker-1",
        TaskOutcome(status=TaskStatus.CANCELLED),
    )
    queue.request_cancel(task_id, "user-1")

    assert _task_row(engine, task_id)["status"] == "CANCELLED"
    assert _attempt_rows(engine, task_id)[0]["status"] == "CANCELLED"
    assert [event["event_type"] for event in _task_audit(engine, task_id)] == [
        "TASK_ENQUEUED",
        "TASK_CLAIMED",
        "TASK_CANCEL_REQUESTED",
        "TASK_FINISHED",
    ]


def test_registered_backtest_success_wins_cancel_request_before_task_finish(
    engine: Engine,
) -> None:
    _insert_experiment(engine)
    queue = _queue(engine)
    task_id = queue.enqueue(
        "BACKTEST",
        {},
        0,
        experiment_id="experiment-1",
    )
    claimed = queue.claim("worker-1", NOW)
    assert claimed is not None
    queue.request_cancel(task_id, "user-1")
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE experiment SET status = 'SUCCEEDED', completed_at = :now "
                "WHERE id = 'experiment-1'"
            ),
            {"now": NOW_TEXT},
        )

    queue.finish(
        claimed.attempt_id,
        "worker-1",
        TaskOutcome(status=TaskStatus.SUCCEEDED),
    )

    task = _task_row(engine, task_id)
    attempt = _attempt_rows(engine, task_id)[0]
    assert task["status"] == attempt["status"] == "SUCCEEDED"


def test_finish_is_owner_fenced_idempotent_and_persists_canonical_error(
    engine: Engine,
) -> None:
    """Only an identical owner/outcome replay may repeat a terminal finish."""
    queue = _queue(engine)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    claimed = queue.claim("worker-1", NOW)
    assert claimed is not None
    progress = TaskProgress(stage="run", completed=1, total=2, message="half")
    queue.heartbeat(claimed.attempt_id, "worker-1", progress, NOW)
    failed = TaskOutcome(
        status=TaskStatus.FAILED,
        error={"message": "boom", "code": "BACKTEST_FAILED"},
    )

    with pytest.raises(QuantError) as wrong_owner:
        queue.finish(claimed.attempt_id, "worker-2", failed)
    assert wrong_owner.value.detail.code == "TASK_OWNERSHIP_CONFLICT"

    queue.finish(claimed.attempt_id, "worker-1", failed)
    queue.finish(claimed.attempt_id, "worker-1", failed)

    task = _task_row(engine, task_id)
    attempt = _attempt_rows(engine, task_id)[0]
    expected_error = '{"code":"BACKTEST_FAILED","message":"boom"}'
    assert task["status"] == attempt["status"] == "FAILED"
    assert task["error_json"] == attempt["error_json"] == expected_error
    assert task["progress_json"] == attempt["progress_json"]
    assert [event["event_type"] for event in _task_audit(engine, task_id)].count(
        "TASK_FINISHED"
    ) == 1

    with pytest.raises(QuantError) as changed:
        queue.finish(
            claimed.attempt_id,
            "worker-1",
            TaskOutcome(status=TaskStatus.CANCELLED),
        )
    with pytest.raises(QuantError) as terminal_heartbeat:
        queue.heartbeat(claimed.attempt_id, "worker-1", progress, NOW)
    assert changed.value.detail.code == "TASK_STATE_CONFLICT"
    assert terminal_heartbeat.value.detail.code == "TASK_STATE_CONFLICT"


def test_orphan_threshold_is_strict_and_does_not_requeue(engine: Engine) -> None:
    """Exactly 60 seconds remains active; only a later heartbeat age is orphaned."""
    queue = _queue(engine)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    claimed = queue.claim("worker-1", NOW)
    assert claimed is not None

    assert queue.mark_orphans(NOW + timedelta(seconds=59), timedelta(seconds=60)) == 0
    assert queue.mark_orphans(NOW + timedelta(seconds=60), timedelta(seconds=60)) == 0
    assert queue.mark_orphans(
        NOW + timedelta(seconds=60, microseconds=1), timedelta(seconds=60)
    ) == 1

    task = _task_row(engine, task_id)
    attempt = _attempt_rows(engine, task_id)[0]
    assert task["status"] == attempt["status"] == "ORPHANED"
    assert json.loads(task["error_json"])["code"] == "TASK_ORPHANED"
    assert queue.claim("worker-2", NOW + timedelta(minutes=2)) is None
    assert [event["event_type"] for event in _task_audit(engine, task_id)][-1] == (
        "TASK_ORPHANED"
    )
    with pytest.raises(ValueError):
        queue.mark_orphans(NOW, timedelta(0))
    with pytest.raises(ValueError, match="timezone-aware"):
        queue.mark_orphans(NOW.replace(tzinfo=None), timedelta(seconds=60))


def test_orphan_scan_uses_newer_task_or_attempt_heartbeat(engine: Engine) -> None:
    """A fresh heartbeat in either synchronized row prevents a false orphan."""
    queue = _queue(engine)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    queue.claim("worker-1", NOW)
    with engine.begin() as connection:
        connection.execute(
            text("UPDATE task SET heartbeat_at = :old WHERE id = :task_id"),
            {"old": (NOW - timedelta(minutes=5)).isoformat(), "task_id": task_id},
        )
        connection.execute(
            text("UPDATE task_attempt SET heartbeat_at = :fresh WHERE task_id = :task_id"),
            {"fresh": NOW.isoformat(), "task_id": task_id},
        )

    assert queue.mark_orphans(NOW + timedelta(seconds=60), timedelta(seconds=60)) == 0
    assert _task_row(engine, task_id)["status"] == "RUNNING"


def test_retry_reuses_task_preserves_history_and_increments_attempt(
    engine: Engine,
) -> None:
    """Explicit retry resets runtime state but next claim creates attempt N+1."""
    clock = [NOW]
    queue = _queue(engine, clock=lambda: clock[0])
    task_id = queue.enqueue(
        "BACKTEST", {}, 0, idempotency_key="request-1", request_id="enqueue"
    )
    first = queue.claim("worker-1", NOW)
    assert first is not None
    queue.finish(
        first.attempt_id,
        "worker-1",
        TaskOutcome(status=TaskStatus.FAILED, error={"code": "FAILED"}),
    )
    clock[0] = NOW + timedelta(minutes=1)

    retried = queue.retry(task_id, "user-1", request_id="retry-1")

    assert retried == task_id
    reset = _task_row(engine, task_id)
    assert reset["status"] == "QUEUED"
    assert reset["worker_id"] is None
    assert reset["locked_at"] is None
    assert reset["heartbeat_at"] is None
    assert reset["completed_at"] is None
    assert reset["error_json"] is None
    assert json.loads(reset["progress_json"]) == {
        "completed": 0,
        "message": "",
        "stage": "queued",
        "total": 0,
    }
    assert len(_attempt_rows(engine, task_id)) == 1

    second = queue.claim("worker-2", NOW + timedelta(minutes=1))
    assert second is not None
    assert second.id == task_id
    assert second.attempt_no == 2
    assert second.attempt_id != first.attempt_id
    assert [row["attempt_no"] for row in _attempt_rows(engine, task_id)] == [1, 2]
    assert [event["event_type"] for event in _task_audit(engine, task_id)] == [
        "TASK_ENQUEUED",
        "TASK_CLAIMED",
        "TASK_FINISHED",
        "TASK_RETRIED",
        "TASK_CLAIMED",
    ]


@pytest.mark.parametrize(
    "status",
    [
        TaskStatus.QUEUED,
        TaskStatus.RUNNING,
        TaskStatus.CANCEL_REQUESTED,
        TaskStatus.SUCCEEDED,
    ],
)
def test_retry_rejects_non_retryable_states(engine: Engine, status: TaskStatus) -> None:
    """Retry cannot bypass active ownership or repeat successful work."""
    queue = _queue(engine)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    if status is not TaskStatus.QUEUED:
        claimed = queue.claim("worker-1", NOW)
        assert claimed is not None
        if status is TaskStatus.CANCEL_REQUESTED:
            queue.request_cancel(task_id, "user-1")
        elif status is TaskStatus.SUCCEEDED:
            queue.finish(
                claimed.attempt_id,
                "worker-1",
                TaskOutcome(status=TaskStatus.SUCCEEDED),
            )

    with pytest.raises(QuantError) as captured:
        queue.retry(task_id, "user-1")

    assert captured.value.detail.code == "TASK_STATE_CONFLICT"


def test_finish_audit_failure_rolls_back_task_and_attempt(engine: Engine) -> None:
    """A last-step constraint failure must roll back both preceding state updates."""
    queue = _queue(engine)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    claimed = queue.claim("worker-1", NOW)
    assert claimed is not None
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TRIGGER reject_task_finish BEFORE INSERT ON audit_event "
                "WHEN NEW.event_type = 'TASK_FINISHED' "
                "BEGIN SELECT RAISE(ABORT, 'reject finish audit'); END"
            )
        )

    with pytest.raises(Exception, match="reject finish audit"):
        queue.finish(
            claimed.attempt_id,
            "worker-1",
            TaskOutcome(status=TaskStatus.SUCCEEDED),
        )

    task = _task_row(engine, task_id)
    attempt = _attempt_rows(engine, task_id)[0]
    assert task["status"] == attempt["status"] == "RUNNING"
    assert task["completed_at"] is attempt["completed_at"] is None
    assert task["error_json"] is attempt["error_json"] is None
    assert "TASK_FINISHED" not in {
        event["event_type"] for event in _task_audit(engine, task_id)
    }
