"""Real file-backed concurrency coverage for the durable SQLite task queue."""

from __future__ import annotations

import multiprocessing
import queue as queue_module
import threading
import time
import traceback
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import Connection, Engine, create_engine, event, text
from sqlalchemy.engine import URL

from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.task_queue import (
    TaskQueue,
    TaskQueueBusy,
)

NOW = datetime(2026, 7, 30, 8, tzinfo=UTC)
type _Operation = tuple[str, dict[str, object]]


def _engines(database: Path) -> tuple[Engine, Engine]:
    return create_sqlite_engine(database), create_sqlite_engine(database)


def _run_operation(
    database: str,
    barrier: Any,
    output: Any,
    position: int,
    operation: _Operation,
) -> None:
    engine = create_sqlite_engine(Path(database))
    try:
        barrier.wait(timeout=5)
        kind, arguments = operation
        queue = TaskQueue(engine, clock=lambda: NOW)
        if kind == "claim":
            result: object = queue.claim(cast(str, arguments["worker_id"]), NOW)
        elif kind == "enqueue":
            result = queue.enqueue(
                cast(str, arguments["task_type"]),
                cast(dict[str, Any], arguments["payload"]),
                cast(int, arguments["priority"]),
                idempotency_key=cast(str | None, arguments.get("idempotency_key")),
            )
        elif kind == "sleep":
            time.sleep(cast(float, arguments["seconds"]))
            result = None
        else:
            raise AssertionError(f"unknown parallel test operation: {kind}")
        output.put((position, "ok", result))
    except Exception:  # noqa: BLE001 - child must transmit every test failure
        output.put((position, "error", traceback.format_exc()))
    finally:
        engine.dispose()


def _run_together(
    database: Path,
    first: _Operation,
    second: _Operation,
    *,
    timeout: float = 15,
) -> tuple[object, object]:
    """Run two spawn processes and forcibly reap either one after a deadline."""
    if timeout <= 0:
        raise ValueError("parallel operation timeout must be positive")
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(3, timeout=min(5, timeout))
    output = context.Queue()
    processes = tuple(
        context.Process(
            target=_run_operation,
            args=(str(database), barrier, output, position, operation),
            name=f"task-queue-test-{position}",
        )
        for position, operation in enumerate((first, second))
    )
    results: dict[int, object] = {}
    deadline = time.monotonic() + timeout
    for process in processes:
        process.start()
    try:
        try:
            barrier.wait(timeout=max(0.001, deadline - time.monotonic()))
        except threading.BrokenBarrierError as error:
            raise TimeoutError("parallel queue operations timed out") from error
        for _ in processes:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("parallel queue operations timed out")
            try:
                position, status, value = output.get(timeout=remaining)
            except queue_module.Empty as error:
                raise TimeoutError("parallel queue operations timed out") from error
            if status == "error":
                raise AssertionError(f"parallel queue operation failed:\n{value}")
            results[cast(int, position)] = value
        for process in processes:
            process.join(timeout=max(0, deadline - time.monotonic()))
            if process.is_alive():
                raise TimeoutError("parallel queue operations timed out")
            if process.exitcode != 0:
                raise AssertionError(
                    f"parallel queue process exited with code {process.exitcode}"
                )
        return results[0], results[1]
    finally:
        barrier.abort()
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=0.5)
            if process.is_alive():
                process.kill()
                process.join(timeout=0.5)
        output.cancel_join_thread()
        output.close()


def _zero_timeout_engine(database: Path) -> Engine:
    return create_engine(
        URL.create("sqlite+pysqlite", database=str(database.resolve())),
        future=True,
        connect_args={"timeout": 0.0},
    )


def test_parallel_harness_terminates_timed_out_workers(tmp_path: Path) -> None:
    """A broken queue operation must not leave a thread, process, or SQLite lock."""
    started_at = time.monotonic()

    with pytest.raises(TimeoutError, match="parallel queue operations timed out"):
        _run_together(
            database=tmp_path / "timeout.db",
            first=("sleep", {"seconds": 60.0}),
            second=("sleep", {"seconds": 60.0}),
            timeout=0.1,
        )

    assert time.monotonic() - started_at < 5
    assert not any(
        child.name.startswith("task-queue-test-")
        for child in multiprocessing.active_children()
    )


def test_two_independent_connections_claim_single_task_once(tmp_path: Path) -> None:
    """Removing BEGIN IMMEDIATE can let both sessions observe the same queued row."""
    database = tmp_path / "single-claim.db"
    upgrade_database(database)
    first_engine, second_engine = _engines(database)
    try:
        first_queue = TaskQueue(first_engine, clock=lambda: NOW)
        task_id = first_queue.enqueue("BACKTEST", {}, 0)

        results = _run_together(
            database,
            ("claim", {"worker_id": "worker-1"}),
            ("claim", {"worker_id": "worker-2"}),
        )

        claimed = [result for result in results if result is not None]
        assert [result.id for result in claimed] == [task_id]
        with first_engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT count(*) FROM task_attempt WHERE task_id = :task_id"),
                    {"task_id": task_id},
                ).scalar_one()
                == 1
            )
    finally:
        first_engine.dispose()
        second_engine.dispose()


def test_concurrent_claim_rounds_preserve_priority_without_duplicates(
    tmp_path: Path,
) -> None:
    """Each serialized round must allocate the next priority band exactly once."""
    database = tmp_path / "ordered-claims.db"
    upgrade_database(database)
    first_engine, second_engine = _engines(database)
    try:
        first_queue = TaskQueue(first_engine, clock=lambda: NOW)
        task_ids = {
            priority: first_queue.enqueue(f"TASK_{priority}", {}, priority)
            for priority in (1, 2, 3, 4)
        }

        first_round = _run_together(
            database,
            ("claim", {"worker_id": "worker-1"}),
            ("claim", {"worker_id": "worker-2"}),
        )
        second_round = _run_together(
            database,
            ("claim", {"worker_id": "worker-1"}),
            ("claim", {"worker_id": "worker-2"}),
        )

        assert {result.id for result in first_round if result is not None} == {
            task_ids[4],
            task_ids[3],
        }
        assert {result.id for result in second_round if result is not None} == {
            task_ids[2],
            task_ids[1],
        }
        claimed_ids = [
            result.id for result in (*first_round, *second_round) if result is not None
        ]
        assert len(claimed_ids) == len(set(claimed_ids)) == 4
        with first_engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT count(*) FROM task_attempt")
                ).scalar_one()
                == 4
            )
    finally:
        first_engine.dispose()
        second_engine.dispose()


def test_claim_releases_write_lock_before_return(tmp_path: Path) -> None:
    """A caller doing work after claim must not retain the queue write transaction."""
    database = tmp_path / "released-claim.db"
    upgrade_database(database)
    claim_engine, writer_engine = _engines(database)
    try:
        queue = TaskQueue(claim_engine, clock=lambda: NOW)
        task_id = queue.enqueue("BACKTEST", {}, 0)

        claimed = queue.claim("worker-1", NOW)

        assert claimed is not None
        with writer_engine.connect() as writer:
            writer.exec_driver_sql("PRAGMA busy_timeout=0")
            writer.exec_driver_sql("BEGIN IMMEDIATE")
            writer.execute(
                text("UPDATE task SET updated_at = :now WHERE id = :task_id"),
                {"now": NOW.isoformat(), "task_id": task_id},
            )
            writer.commit()
    finally:
        claim_engine.dispose()
        writer_engine.dispose()


def test_orphan_candidate_scan_does_not_hold_write_lock(tmp_path: Path) -> None:
    """The unbounded active-task read must finish before any short write lock."""
    database = tmp_path / "orphan-scan-lock.db"
    upgrade_database(database)
    scan_engine, writer_engine = _engines(database)
    scan_observed = threading.Event()
    release_scan = threading.Event()
    results: list[int] = []
    failures: list[Exception] = []
    try:
        queue = TaskQueue(scan_engine, clock=lambda: NOW)
        queue.enqueue("BACKTEST", {}, 0)
        claimed = queue.claim("worker-1", NOW)
        assert claimed is not None

        @event.listens_for(scan_engine, "after_cursor_execute")
        def pause_candidate_scan(
            _connection: object,
            _cursor: object,
            statement: str,
            _parameters: object,
            _context: object,
            _executemany: bool,
        ) -> None:
            normalized = " ".join(statement.upper().split())
            if (
                not scan_observed.is_set()
                and normalized.startswith("SELECT")
                and "FROM TASK" in normalized
                and "TASK.STATUS IN" in normalized
            ):
                scan_observed.set()
                if not release_scan.wait(timeout=5):
                    raise AssertionError("orphan candidate scan was not released")

        def scan() -> None:
            try:
                results.append(
                    queue.mark_orphans(
                        NOW + timedelta(seconds=61), timedelta(seconds=60)
                    )
                )
            except Exception as error:  # noqa: BLE001 - assert background failure
                failures.append(error)

        thread = threading.Thread(target=scan, daemon=True)
        thread.start()
        try:
            assert scan_observed.wait(timeout=5)
            with writer_engine.connect() as writer:
                writer.exec_driver_sql("PRAGMA busy_timeout=0")
                writer.exec_driver_sql("BEGIN IMMEDIATE")
                writer.rollback()
        finally:
            release_scan.set()
            thread.join(timeout=5)

        assert not thread.is_alive()
        assert failures == []
        assert results == [1]
    finally:
        release_scan.set()
        scan_engine.dispose()
        writer_engine.dispose()


def test_concurrent_idempotent_enqueue_creates_one_active_row(tmp_path: Path) -> None:
    """The partial unique namespace and transaction must linearize equal producers."""
    database = tmp_path / "idempotent-enqueue.db"
    upgrade_database(database)
    first_engine, second_engine = _engines(database)
    try:
        identifiers = _run_together(
            database,
            (
                "enqueue",
                {
                    "task_type": "BACKTEST",
                    "payload": {"window": 20},
                    "priority": 1,
                    "idempotency_key": "request-1",
                },
            ),
            (
                "enqueue",
                {
                    "task_type": "BACKTEST",
                    "payload": {"window": 20},
                    "priority": 99,
                    "idempotency_key": "request-1",
                },
            ),
        )

        assert identifiers[0] == identifiers[1]
        with first_engine.connect() as connection:
            assert (
                connection.execute(text("SELECT count(*) FROM task")).scalar_one() == 1
            )
            assert (
                connection.execute(
                    text(
                        "SELECT count(*) FROM audit_event "
                        "WHERE event_type IN "
                        "('TASK_ENQUEUED', 'TASK_ENQUEUE_DEDUPLICATED')"
                    )
                ).scalar_one()
                == 2
            )
    finally:
        first_engine.dispose()
        second_engine.dispose()


def test_lock_retries_use_declared_delays_then_succeed(tmp_path: Path) -> None:
    """A transient writer must trigger deterministic bounded delays before claim."""
    database = tmp_path / "lock-retry-success.db"
    upgrade_database(database)
    queue_engine = _zero_timeout_engine(database)
    blocker_engine = create_sqlite_engine(database)
    delays: list[float] = []
    begin_attempts: list[str] = []

    @event.listens_for(queue_engine, "before_cursor_execute")
    def observe_begin(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if statement.strip().upper() == "BEGIN IMMEDIATE":
            begin_attempts.append(statement)

    blocker = blocker_engine.connect()
    try:
        queue = TaskQueue(
            queue_engine,
            clock=lambda: NOW,
            sleeper=lambda delay: _release_after_second(
                delay, delays=delays, blocker=blocker
            ),
            lock_retry_delays=(0.001, 0.002, 0.003),
        )
        task_id = queue.enqueue("BACKTEST", {}, 0)
        begin_attempts.clear()
        blocker.exec_driver_sql("BEGIN IMMEDIATE")

        claimed = queue.claim("worker-1", NOW)

        assert claimed is not None
        assert claimed.id == task_id
        assert delays == [0.001, 0.002]
        assert len(begin_attempts) == 3
    finally:
        if blocker.in_transaction():
            blocker.rollback()
        blocker.close()
        queue_engine.dispose()
        blocker_engine.dispose()


def _release_after_second(
    delay: float, *, delays: list[float], blocker: Connection
) -> None:
    delays.append(delay)
    if len(delays) == 2:
        blocker.commit()


def test_lock_retry_exhaustion_is_structured_and_bounded(tmp_path: Path) -> None:
    """A permanent writer must stop after three retries with retryable metadata."""
    database = tmp_path / "lock-retry-exhausted.db"
    upgrade_database(database)
    queue_engine = _zero_timeout_engine(database)
    blocker_engine = create_sqlite_engine(database)
    delays: list[float] = []
    begin_attempts: list[str] = []

    @event.listens_for(queue_engine, "before_cursor_execute")
    def observe_begin(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        if statement.strip().upper() == "BEGIN IMMEDIATE":
            begin_attempts.append(statement)

    blocker = blocker_engine.connect()
    try:
        queue = TaskQueue(
            queue_engine,
            clock=lambda: NOW,
            sleeper=delays.append,
            lock_retry_delays=(0.001, 0.002, 0.003),
        )
        queue.enqueue("BACKTEST", {}, 0)
        begin_attempts.clear()
        blocker.exec_driver_sql("BEGIN IMMEDIATE")

        with pytest.raises(TaskQueueBusy) as captured:
            queue.claim("worker-1", NOW)

        assert captured.value.detail.code == "TASK_QUEUE_BUSY"
        assert captured.value.detail.retryable is True
        assert delays == [0.001, 0.002, 0.003]
        assert len(begin_attempts) == 4
    finally:
        if blocker.in_transaction():
            blocker.rollback()
        blocker.close()
        queue_engine.dispose()
        blocker_engine.dispose()
