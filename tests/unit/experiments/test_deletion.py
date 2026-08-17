"""验证实验硬删除的数据库、文件和恢复边界。"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import Engine, insert, text

from quant_research.domain.errors import QuantError
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.experiment_deletion import (
    SqliteExperimentDeletion,
)
from quant_research.infrastructure.persistence.orm import (
    AuditEventORM,
    ExperimentArtifactORM,
    ExperimentMetricORM,
    ExperimentORM,
    ExperimentTagORM,
    TaskAttemptORM,
    TaskORM,
)

EXPERIMENT_ID = "11111111-1111-4111-8111-111111111111"
TASK_ID = "22222222-2222-4222-8222-222222222222"
ATTEMPT_ID = "33333333-3333-4333-8333-333333333333"
NOW = datetime(2026, 8, 15, 4, tzinfo=UTC)
NOW_TEXT = NOW.isoformat()


def _engine(tmp_path: Path) -> Engine:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    return create_sqlite_engine(database)


def _insert_experiment_graph(engine: Engine, status: str) -> None:
    with engine.begin() as connection:
        connection.execute(
            insert(ExperimentORM).values(
                id=EXPERIMENT_ID,
                strategy_id="etf_rotation",
                config_json="{}",
                config_hash="a" * 64,
                data_hash="b" * 64,
                source_tree_hash="c" * 64,
                git_commit_hash=None,
                lockfile_hash="d" * 64,
                rulebook_hash="e" * 64,
                fingerprint="f" * 64,
                status=status,
                research_mark="UNREVIEWED",
                created_at=NOW_TEXT,
                queued_at=NOW_TEXT if status != "CREATED" else None,
                started_at=NOW_TEXT if status not in {"CREATED", "QUEUED"} else None,
                completed_at=(
                    NOW_TEXT if status in {"SUCCEEDED", "FAILED", "CANCELLED"} else None
                ),
            )
        )
        connection.execute(
            insert(ExperimentTagORM).values(
                experiment_id=EXPERIMENT_ID,
                tag="candidate",
            )
        )
        connection.execute(
            insert(ExperimentMetricORM).values(
                experiment_id=EXPERIMENT_ID,
                name="sharpe_ratio",
                value=1.2,
                unit=None,
                created_at=NOW_TEXT,
            )
        )
        connection.execute(
            insert(ExperimentArtifactORM).values(
                experiment_id=EXPERIMENT_ID,
                name="manifest.json",
                artifact_type="JSON",
                path="manifest.json",
                content_hash="0" * 64,
                metadata_json="{}",
                created_at=NOW_TEXT,
            )
        )
        connection.execute(
            insert(TaskORM).values(
                id=TASK_ID,
                experiment_id=EXPERIMENT_ID,
                task_type="BACKTEST",
                payload_json="{}",
                status="SUCCEEDED",
                priority=0,
                progress_json='{"completed":7,"message":"","stage":"done","total":7}',
                created_at=NOW_TEXT,
                available_at=NOW_TEXT,
                updated_at=NOW_TEXT,
                completed_at=NOW_TEXT,
            )
        )
        connection.execute(
            insert(TaskAttemptORM).values(
                id=ATTEMPT_ID,
                task_id=TASK_ID,
                attempt_no=1,
                status="SUCCEEDED",
                worker_id="worker-1",
                started_at=NOW_TEXT,
                completed_at=NOW_TEXT,
                log_path="run.log",
                progress_json='{"completed":7,"message":"","stage":"done","total":7}',
            )
        )
        connection.execute(
            insert(AuditEventORM).values(
                experiment_id=EXPERIMENT_ID,
                task_id=TASK_ID,
                event_type="EXPERIMENT_SUCCEEDED",
                actor="worker",
                details_json="{}",
                created_at=NOW_TEXT,
            )
        )


def _service(engine: Engine, data_root: Path) -> SqliteExperimentDeletion:
    return SqliteExperimentDeletion(
        engine,
        data_root=data_root,
        artifact_root=data_root / "artifacts",
        task_log_root=data_root / "state" / "task-logs",
        clock=lambda: NOW,
    )


def _write_resources(data_root: Path) -> tuple[Path, Path, Path]:
    published = data_root / "artifacts" / f"experiment_id={EXPERIMENT_ID}"
    staging = (
        data_root
        / "artifacts"
        / ".experiment-staging"
        / f"experiment_id={EXPERIMENT_ID}"
    )
    task_log = data_root / "state" / "task-logs" / f"task_id={TASK_ID}"
    for root, name in (
        (published, "manifest.json"),
        (staging, "checkpoint.json"),
        (task_log, "run.log"),
    ):
        root.mkdir(parents=True)
        (root / name).write_text(name, encoding="utf-8")
    return published, staging, task_log


def test_delete_removes_graph_and_files_but_preserves_audit(tmp_path: Path) -> None:
    engine = _engine(tmp_path)
    _insert_experiment_graph(engine, "SUCCEEDED")
    resources = _write_resources(tmp_path)
    unrelated = tmp_path / "artifacts" / "factor-studies" / "keep.txt"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text("keep", encoding="utf-8")

    _service(engine, tmp_path).delete(
        EXPERIMENT_ID,
        "dashboard",
        request_id="delete-request",
    )

    with engine.connect() as connection:
        for table in (
            "experiment",
            "experiment_tag",
            "experiment_metric",
            "experiment_artifact",
            "task",
            "task_attempt",
        ):
            assert connection.scalar(text(f"SELECT COUNT(*) FROM {table}")) == 0
        audits = (
            connection.execute(
                text(
                    "SELECT experiment_id, task_id, event_type, details_json "
                    "FROM audit_event ORDER BY id"
                )
            )
            .mappings()
            .all()
        )
    assert [row["event_type"] for row in audits] == [
        "EXPERIMENT_SUCCEEDED",
        "EXPERIMENT_DELETED",
    ]
    assert all(
        row["experiment_id"] is None and row["task_id"] is None for row in audits
    )
    assert json.loads(audits[-1]["details_json"]) == {
        "action": "delete",
        "actor": "dashboard",
        "artifact_count": 1,
        "attempt_count": 1,
        "experiment_id": EXPERIMENT_ID,
        "metric_count": 1,
        "request_id": "delete-request",
        "status": "SUCCEEDED",
        "tag_count": 1,
        "task_count": 1,
    }
    assert all(not path.exists() for path in resources)
    assert unrelated.read_text("utf-8") == "keep"
    engine.dispose()


@pytest.mark.parametrize("status", ["CREATED", "SUCCEEDED", "FAILED", "CANCELLED"])
def test_all_non_active_experiment_statuses_are_deletable(
    tmp_path: Path, status: str
) -> None:
    engine = _engine(tmp_path)
    _insert_experiment_graph(engine, status)
    _service(engine, tmp_path).delete(EXPERIMENT_ID, "dashboard", request_id="delete")
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM experiment")) == 0
    engine.dispose()


@pytest.mark.parametrize("status", ["QUEUED", "RUNNING"])
def test_active_experiment_is_not_partially_deleted(
    tmp_path: Path, status: str
) -> None:
    engine = _engine(tmp_path)
    _insert_experiment_graph(engine, status)
    resources = _write_resources(tmp_path)

    with pytest.raises(QuantError) as captured:
        _service(engine, tmp_path).delete(
            EXPERIMENT_ID,
            "dashboard",
            request_id="delete",
        )

    assert captured.value.detail.code == "EXPERIMENT_DELETE_CONFLICT"
    assert captured.value.detail.retryable is True
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM experiment")) == 1
    assert all(path.exists() for path in resources)
    engine.dispose()


def test_stage_failure_restores_already_moved_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _engine(tmp_path)
    _insert_experiment_graph(engine, "FAILED")
    resources = _write_resources(tmp_path)
    real_replace = os.replace
    calls = 0

    def fail_second_replace(source: Path, target: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise PermissionError("locked")
        real_replace(source, target)

    monkeypatch.setattr(os, "replace", fail_second_replace)
    with pytest.raises(QuantError) as captured:
        _service(engine, tmp_path).delete(
            EXPERIMENT_ID,
            "dashboard",
            request_id="delete",
        )

    assert captured.value.detail.code == "EXPERIMENT_DELETE_STORAGE_FAILED"
    assert all(path.exists() for path in resources)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM experiment")) == 1
    engine.dispose()


def test_recovery_restores_staged_files_when_database_delete_did_not_commit(
    tmp_path: Path,
) -> None:
    engine = _engine(tmp_path)
    _insert_experiment_graph(engine, "FAILED")
    published, _, _ = _write_resources(tmp_path)
    operation = (
        tmp_path / "state" / ".experiment-deletions" / f"experiment_id={EXPERIMENT_ID}"
    )
    operation.mkdir(parents=True)
    (operation / "manifest.json").write_text(
        json.dumps({"experiment_id": EXPERIMENT_ID, "task_ids": [TASK_ID]}),
        encoding="utf-8",
    )
    os.replace(published, operation / "published")

    _service(engine, tmp_path).recover()

    assert published.is_dir()
    assert not operation.exists()
    engine.dispose()
