"""Real-process crash recovery evidence for the durable worker."""

from __future__ import annotations

import multiprocessing
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text

from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.tasks.models import TaskOutcome, TaskStatus
from quant_core.tasks.queue import TaskQueue

CRASH_TIME = datetime(2026, 7, 30, 8, tzinfo=UTC)


class _BlockingArtifactHandler:
    task_type = "BACKTEST"

    def __init__(
        self,
        temporary_path: str,
        published_path: str,
        started: Any,
    ) -> None:
        self._temporary_path = Path(temporary_path)
        self._published_path = Path(published_path)
        self._started = started

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, progress
        self._temporary_path.write_text("partial", encoding="utf-8")
        self._started.set()
        while not cancellation.is_cancelled():
            time.sleep(0.1)
        self._published_path.write_text("published", encoding="utf-8")
        return TaskOutcome(status=TaskStatus.SUCCEEDED)


def _run_blocking_worker(
    database: str,
    temporary_path: str,
    published_path: str,
    started: Any,
) -> None:
    from quant_core.tasks import Worker

    engine = create_sqlite_engine(Path(database))
    try:
        queue = TaskQueue(engine, clock=lambda: CRASH_TIME)
        worker = Worker(
            queue,
            worker_id="crash-worker",
            handlers=(
                _BlockingArtifactHandler(
                    temporary_path,
                    published_path,
                    started,
                ),
            ),
            clock=lambda: CRASH_TIME,
            heartbeat_interval=3600.0,
        )
        worker.run_once()
    finally:
        engine.dispose()


def _remaining(deadline: float) -> float:
    return max(0.001, deadline - time.monotonic())


def _wait_until_handler_started(
    process: multiprocessing.Process,
    started: Any,
    deadline: float,
) -> None:
    while time.monotonic() < deadline:
        if started.wait(timeout=min(0.05, _remaining(deadline))):
            return
        if not process.is_alive():
            break
    raise AssertionError(
        f"worker exited before blocking handler started: {process.exitcode}"
    )


def test_forced_worker_termination_requires_explicit_retry_with_new_attempt(
    tmp_path: Path,
) -> None:
    """A killed owner must orphan, never publish or rerun, then retain attempt history."""
    database = tmp_path / "worker-crash.db"
    temporary = tmp_path / "staging.tmp"
    published = tmp_path / "published.marker"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    try:
        queue = TaskQueue(engine, clock=lambda: CRASH_TIME)
        task_id = queue.enqueue("BACKTEST", {}, 0)
    finally:
        engine.dispose()

    context = multiprocessing.get_context("spawn")
    started = context.Event()
    process = context.Process(
        target=_run_blocking_worker,
        args=(str(database), str(temporary), str(published), started),
        name="worker-recovery-crash-test",
    )
    deadline = time.monotonic() + 10.0
    process.start()
    try:
        _wait_until_handler_started(process, started, deadline)
        assert temporary.read_text(encoding="utf-8") == "partial"
        assert not published.exists()
        process.terminate()
        process.join(timeout=_remaining(deadline))
        if process.is_alive():
            process.kill()
            process.join(timeout=_remaining(deadline))
        assert not process.is_alive()
        assert process.exitcode not in (None, 0)
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=0.5)
        if process.is_alive():
            process.kill()
            process.join(timeout=0.5)

    assert not published.exists()
    recovery_engine = create_sqlite_engine(database)
    try:
        queue = TaskQueue(recovery_engine, clock=lambda: CRASH_TIME)
        stale_after = timedelta(seconds=60)
        assert queue.mark_orphans(CRASH_TIME + stale_after, stale_after) == 0
        orphaned_at = CRASH_TIME + stale_after + timedelta(microseconds=1)
        assert queue.mark_orphans(orphaned_at, stale_after) == 1
        assert queue.claim("recovery-worker", orphaned_at) is None

        with recovery_engine.connect() as connection:
            task_status = connection.execute(
                text("SELECT status FROM task WHERE id = :task_id"),
                {"task_id": task_id},
            ).scalar_one()
            old_attempt = connection.execute(
                text(
                    "SELECT id, attempt_no, status FROM task_attempt "
                    "WHERE task_id = :task_id"
                ),
                {"task_id": task_id},
            ).one()
        assert task_status == "ORPHANED"
        assert old_attempt.attempt_no == 1
        assert old_attempt.status == "ORPHANED"

        assert queue.retry(task_id, "user-1", available_at=orphaned_at) == task_id
        reclaimed = queue.claim("recovery-worker", orphaned_at)
        assert reclaimed is not None
        assert reclaimed.id == task_id
        assert reclaimed.attempt_id != old_attempt.id
        assert reclaimed.attempt_no == 2
        with recovery_engine.connect() as connection:
            attempts = connection.execute(
                text(
                    "SELECT id, attempt_no, status FROM task_attempt "
                    "WHERE task_id = :task_id ORDER BY attempt_no"
                ),
                {"task_id": task_id},
            ).all()
        assert [(row.id, row.attempt_no, row.status) for row in attempts] == [
            (old_attempt.id, 1, "ORPHANED"),
            (reclaimed.attempt_id, 2, "RUNNING"),
        ]
        queue.finish(
            reclaimed.attempt_id,
            "recovery-worker",
            TaskOutcome(status=TaskStatus.CANCELLED),
        )
    finally:
        recovery_engine.dispose()
