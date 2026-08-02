"""Behavioral tests for the single-task durable worker runtime."""

from __future__ import annotations

import importlib
import json
import logging
import threading
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, text

from quant_core.domain.enums import Severity
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.tasks.models import TaskOutcome, TaskProgress, TaskStatus
from quant_core.tasks.queue import TaskQueue

NOW = datetime(2026, 7, 30, 8, tzinfo=UTC)
LATEST_PROGRESS = TaskProgress(
    stage="backtest",
    completed=3,
    total=7,
    message="three sessions complete",
)


def _worker_type() -> Any:
    try:
        module = importlib.import_module("quant_core.tasks.worker")
    except ModuleNotFoundError:
        pytest.fail("quant_core.tasks.worker is missing", pytrace=False)
    worker_type = getattr(module, "Worker", None)
    assert worker_type is not None, "quant_core.tasks.worker.Worker is missing"
    return worker_type


def _handlers_module() -> Any:
    return importlib.import_module("quant_core.tasks.handlers")


@pytest.fixture
def engine(tmp_path: Path) -> Iterator[Engine]:
    database = tmp_path / "worker.db"
    upgrade_database(database)
    value = create_sqlite_engine(database)
    try:
        yield value
    finally:
        value.dispose()


def _statuses(engine: Engine) -> list[str]:
    with engine.connect() as connection:
        return list(
            connection.execute(text("SELECT status FROM task ORDER BY created_at, id"))
            .scalars()
            .all()
        )


def _runtime_rows(engine: Engine, task_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    with engine.connect() as connection:
        task = dict(
            connection.execute(
                text("SELECT * FROM task WHERE id = :task_id"),
                {"task_id": task_id},
            )
            .mappings()
            .one()
        )
        attempt = dict(
            connection.execute(
                text("SELECT * FROM task_attempt WHERE task_id = :task_id"),
                {"task_id": task_id},
            )
            .mappings()
            .one()
        )
    return task, attempt


def _finished_audit(engine: Engine, task_id: str) -> dict[str, Any]:
    with engine.connect() as connection:
        raw = connection.execute(
            text(
                "SELECT details_json FROM audit_event "
                "WHERE task_id = :task_id AND event_type = 'TASK_FINISHED'"
            ),
            {"task_id": task_id},
        ).scalar_one()
    parsed = json.loads(raw)
    assert isinstance(parsed, dict)
    return parsed


class _SuccessHandler:
    task_type = "BACKTEST"

    def __init__(self) -> None:
        self.task_ids: list[str] = []

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del progress, cancellation
        self.task_ids.append(task.id)
        return TaskOutcome(status=TaskStatus.SUCCEEDED)


class _TypedSuccessHandler(_SuccessHandler):
    def __init__(self, task_type: str) -> None:
        super().__init__()
        self.task_type = task_type


class _BoundaryCancellationHandler:
    task_type = "BACKTEST"

    def __init__(self) -> None:
        self.at_boundary = threading.Event()
        self.continue_from_boundary = threading.Event()
        self.observations: list[bool] = []

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, progress
        self.observations.append(cancellation.is_cancelled())
        self.at_boundary.set()
        self.continue_from_boundary.wait(timeout=2)
        self.observations.append(cancellation.is_cancelled())
        status = (
            TaskStatus.CANCELLED
            if self.observations[-1]
            else TaskStatus.SUCCEEDED
        )
        return TaskOutcome(status=status)


class _RecordingQueue:
    def __init__(self, delegate: TaskQueue, periodic_seen: threading.Event) -> None:
        self._delegate = delegate
        self._periodic_seen = periodic_seen
        self.heartbeats: list[TaskProgress] = []
        self.heartbeat_thread: threading.Thread | None = None
        self.heartbeat_stopped_before_finish: bool | None = None

    def claim(self, worker_id: str, now: datetime) -> Any:
        return self._delegate.claim(worker_id, now)

    def heartbeat(
        self,
        attempt_id: str,
        worker_id: str,
        progress: TaskProgress,
        now: datetime,
    ) -> None:
        self.heartbeats.append(progress)
        if len(self.heartbeats) == 2:
            self.heartbeat_thread = threading.current_thread()
            self._periodic_seen.set()
        self._delegate.heartbeat(attempt_id, worker_id, progress, now)

    def is_cancel_requested(self, attempt_id: str, worker_id: str) -> bool:
        return self._delegate.is_cancel_requested(attempt_id, worker_id)

    def finish(
        self, attempt_id: str, worker_id: str, outcome: TaskOutcome
    ) -> None:
        thread = self.heartbeat_thread
        self.heartbeat_stopped_before_finish = thread is None or not thread.is_alive()
        self._delegate.finish(attempt_id, worker_id, outcome)


class _HeartbeatHandler:
    task_type = "BACKTEST"

    def __init__(
        self, progress_updated: threading.Event, periodic_seen: threading.Event
    ) -> None:
        self._progress_updated = progress_updated
        self._periodic_seen = periodic_seen

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, cancellation
        progress.update(LATEST_PROGRESS)
        self._progress_updated.set()
        observed = self._periodic_seen.wait(timeout=2)
        if not observed:
            return TaskOutcome(
                status=TaskStatus.FAILED,
                error={"code": "TEST_HEARTBEAT_MISSING", "retryable": False},
            )
        return TaskOutcome(status=TaskStatus.SUCCEEDED)


class _RaisingHandler:
    task_type = "BACKTEST"

    def __init__(self, error: Exception) -> None:
        self._error = error

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, progress, cancellation
        raise self._error


class _UnsafeFailureHandler:
    task_type = "BACKTEST"

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, progress, cancellation
        return TaskOutcome(
            status=TaskStatus.FAILED,
            error={
                "code": "HANDLER_FAILED",
                "retryable": True,
                "message": "returned-message-secret",
                "context": {
                    "stage": "publish",
                    "api_key": "returned-api-key-secret",
                    "opaque": "returned-opaque-secret",
                },
            },
        )


class _HeartbeatFailureQueue:
    def __init__(self, delegate: TaskQueue) -> None:
        self._delegate = delegate
        self.failed = threading.Event()
        self.finish_calls = 0

    def claim(self, worker_id: str, now: datetime) -> Any:
        return self._delegate.claim(worker_id, now)

    def heartbeat(
        self,
        attempt_id: str,
        worker_id: str,
        progress: TaskProgress,
        now: datetime,
    ) -> None:
        del worker_id
        self.failed.set()
        self._delegate.heartbeat(attempt_id, "former-owner", progress, now)

    def is_cancel_requested(self, attempt_id: str, worker_id: str) -> bool:
        return self._delegate.is_cancel_requested(attempt_id, worker_id)

    def finish(
        self, attempt_id: str, worker_id: str, outcome: TaskOutcome
    ) -> None:
        self.finish_calls += 1
        self._delegate.finish(attempt_id, worker_id, outcome)


class _IgnoresHeartbeatFailureHandler:
    task_type = "BACKTEST"

    def __init__(self, failed: threading.Event) -> None:
        self._failed = failed

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, progress, cancellation
        self._failed.wait(timeout=2)
        return TaskOutcome(status=TaskStatus.SUCCEEDED)


def _run_in_thread(operation: Callable[[], bool]) -> tuple[threading.Thread, list[Any]]:
    results: list[Any] = []

    def run() -> None:
        try:
            results.append(operation())
        except BaseException as error:  # noqa: BLE001 - surfaced in test thread
            results.append(error)

    thread = threading.Thread(target=run, name="worker-test-runner", daemon=True)
    thread.start()
    return thread, results


def test_run_once_claims_only_one_task_and_shutdown_prevents_another_claim(
    engine: Engine,
) -> None:
    """Looping inside run_once or claiming after shutdown would drain both tasks."""
    queue = TaskQueue(engine, clock=lambda: NOW)
    first = queue.enqueue("BACKTEST", {}, 0)
    second = queue.enqueue("BACKTEST", {}, 0)
    handler = _SuccessHandler()
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
    )

    assert worker.run_once() is True
    assert len(handler.task_ids) == 1
    assert handler.task_ids[0] in {first, second}
    assert sorted(_statuses(engine)) == ["QUEUED", "SUCCEEDED"]

    worker.request_shutdown()
    assert worker.run_once() is False
    assert len(handler.task_ids) == 1
    assert sorted(_statuses(engine)) == ["QUEUED", "SUCCEEDED"]


def test_run_forever_uses_default_two_second_idle_poll(engine: Engine) -> None:
    """Changing the default idle delay or busy-spinning must change this observation."""
    queue = TaskQueue(engine, clock=lambda: NOW)
    waits: list[float] = []
    worker: Any

    def poll_waiter(shutdown: threading.Event, timeout: float) -> bool:
        waits.append(timeout)
        worker.request_shutdown()
        return shutdown.is_set()

    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(),
        clock=lambda: NOW,
        poll_waiter=poll_waiter,
    )

    worker.run_forever()

    assert waits == [2.0]


def test_progress_is_immediate_and_periodic_heartbeat_reuses_latest_value(
    engine: Engine,
) -> None:
    """Buffering progress or heartbeating stale progress breaks durable observability."""
    delegate = TaskQueue(engine, clock=lambda: NOW)
    task_id = delegate.enqueue("BACKTEST", {}, 0)
    progress_updated = threading.Event()
    periodic_seen = threading.Event()
    queue = _RecordingQueue(delegate, periodic_seen)
    handler = _HeartbeatHandler(progress_updated, periodic_seen)
    intervals: list[float] = []
    waiter_calls = 0

    def heartbeat_waiter(stop: threading.Event, timeout: float) -> bool:
        nonlocal waiter_calls
        waiter_calls += 1
        intervals.append(timeout)
        if waiter_calls == 1:
            progress_updated.wait(timeout=2)
            return stop.is_set()
        return stop.wait(timeout=2)

    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
        heartbeat_waiter=heartbeat_waiter,
    )

    assert worker.run_once() is True

    task, attempt = _runtime_rows(engine, task_id)
    assert intervals and intervals[0] == 10.0
    assert queue.heartbeats == [LATEST_PROGRESS, LATEST_PROGRESS]
    assert task["progress_json"] == attempt["progress_json"]
    assert '"completed":3' in task["progress_json"]
    assert queue.heartbeat_stopped_before_finish is True


def test_queue_cancellation_is_observed_at_handler_boundary(engine: Engine) -> None:
    """Returning False after CANCEL_REQUESTED would let owned work continue."""
    queue = TaskQueue(engine, clock=lambda: NOW)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    handler = _BoundaryCancellationHandler()
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
    )
    thread, results = _run_in_thread(worker.run_once)
    try:
        assert handler.at_boundary.wait(timeout=2)
        queue.request_cancel(task_id, "user-1")
        handler.continue_from_boundary.set()
        thread.join(timeout=2)
    finally:
        handler.continue_from_boundary.set()

    assert not thread.is_alive()
    assert results == [True]
    assert handler.observations == [False, True]
    task, attempt = _runtime_rows(engine, task_id)
    assert task["status"] == attempt["status"] == "CANCELLED"


def test_shutdown_cancels_current_boundary_and_leaves_next_task_queued(
    engine: Engine,
) -> None:
    """Graceful shutdown must signal current work without claiming queued work."""
    queue = TaskQueue(engine, clock=lambda: NOW)
    current = queue.enqueue("BACKTEST", {}, 1)
    queue.enqueue("BACKTEST", {}, 0)
    handler = _BoundaryCancellationHandler()
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
    )
    thread, results = _run_in_thread(worker.run_once)
    try:
        assert handler.at_boundary.wait(timeout=2)
        worker.request_shutdown()
        handler.continue_from_boundary.set()
        thread.join(timeout=2)
    finally:
        handler.continue_from_boundary.set()

    assert not thread.is_alive()
    assert results == [True]
    assert handler.observations == [False, True]
    task, attempt = _runtime_rows(engine, current)
    assert task["status"] == attempt["status"] == "CANCELLED"
    assert sorted(_statuses(engine)) == ["CANCELLED", "QUEUED"]
    assert worker.run_once() is False


def test_cancel_query_is_owner_fenced_and_rejects_inconsistent_or_terminal_state(
    engine: Engine,
) -> None:
    """A stale owner or mismatched pair must never be mistaken for no cancellation."""
    queue = TaskQueue(engine, clock=lambda: NOW)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    claimed = queue.claim("worker-1", NOW)
    assert claimed is not None

    assert queue.is_cancel_requested(claimed.attempt_id, "worker-1") is False
    with pytest.raises(QuantError) as wrong_owner:
        queue.is_cancel_requested(claimed.attempt_id, "worker-2")
    assert wrong_owner.value.detail.code == "TASK_OWNERSHIP_CONFLICT"

    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE task_attempt SET status = 'CANCEL_REQUESTED' "
                "WHERE id = :attempt_id"
            ),
            {"attempt_id": claimed.attempt_id},
        )
    with pytest.raises(QuantError) as inconsistent:
        queue.is_cancel_requested(claimed.attempt_id, "worker-1")
    assert inconsistent.value.detail.code == "TASK_STATE_CONFLICT"

    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE task SET status = 'CANCEL_REQUESTED' WHERE id = :task_id"
            ),
            {"task_id": task_id},
        )
    assert queue.is_cancel_requested(claimed.attempt_id, "worker-1") is True
    queue.finish(
        claimed.attempt_id,
        "worker-1",
        TaskOutcome(status=TaskStatus.CANCELLED),
    )
    with pytest.raises(QuantError) as terminal:
        queue.is_cancel_requested(claimed.attempt_id, "worker-1")
    assert terminal.value.detail.code == "TASK_STATE_CONFLICT"


def test_handler_registry_names_standard_types_and_rejects_duplicate_registration() -> None:
    """A missing standard type or silent duplicate would make dispatch ambiguous."""
    module = _handlers_module()
    registry_type = getattr(module, "HandlerRegistry", None)
    assert registry_type is not None, "HandlerRegistry is missing"
    standard = {
        "DATA_UPDATE",
        "FACTOR_COMPUTE",
        "BACKTEST",
        "REPORT",
    }
    assert getattr(module, "STANDARD_TASK_TYPES", None) == frozenset(standard)
    registry = registry_type()
    handlers = [_TypedSuccessHandler(task_type) for task_type in sorted(standard)]

    for handler in handlers:
        registry.register(handler)

    assert [registry.get(name) for name in sorted(standard)] == handlers
    with pytest.raises(ValueError, match="already registered"):
        registry.register(_TypedSuccessHandler("BACKTEST"))


@pytest.mark.parametrize("task_type", ["BACKTEST", "UNKNOWN_TASK"])
def test_unregistered_or_unknown_task_type_finishes_nonretryable_failure(
    engine: Engine,
    task_type: str,
) -> None:
    """Missing dispatch must never strand a claimed task in RUNNING."""
    queue = TaskQueue(engine, clock=lambda: NOW)
    task_id = queue.enqueue(task_type, {}, 0)
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(),
        clock=lambda: NOW,
    )

    assert worker.run_once() is True

    task, attempt = _runtime_rows(engine, task_id)
    expected = {"code": "WORKER_UNKNOWN_TASK_TYPE", "retryable": False}
    assert task["status"] == attempt["status"] == "FAILED"
    assert json.loads(task["error_json"]) == expected
    assert json.loads(attempt["error_json"]) == expected
    assert _finished_audit(engine, task_id)["error"] == expected


def test_quant_error_keeps_machine_fields_and_redacts_every_persistent_surface(
    engine: Engine,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Copying free text or secret context into any failure surface leaks credentials."""
    secrets = {
        "quant-message-secret",
        "quant-remediation-secret",
        "quant-api-key-secret",
        "quant-password-secret",
        "quant-environment-secret",
        "quant-opaque-secret",
    }
    detail = ErrorDetail(
        code="DATA_PROVIDER_TIMEOUT",
        severity=Severity.SEVERE,
        message="provider failed with quant-message-secret",
        context={
            "dataset": "daily_prices",
            "stage": "download",
            "api_key": "quant-api-key-secret",
            "connection": {
                "host": "localhost",
                "password": "quant-password-secret",
            },
            "environment": {"DATABASE_URL": "quant-environment-secret"},
            "opaque": "quant-opaque-secret",
        },
        remediation="rotate quant-remediation-secret",
        retryable=True,
    )
    queue = TaskQueue(engine, clock=lambda: NOW)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    logger = logging.getLogger("tests.worker.quant-error")
    caplog.set_level(logging.ERROR, logger=logger.name)
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(_RaisingHandler(QuantError(detail)),),
        clock=lambda: NOW,
        logger=logger,
    )

    assert worker.run_once() is True

    task, attempt = _runtime_rows(engine, task_id)
    expected = {
        "code": "DATA_PROVIDER_TIMEOUT",
        "context": {
            "api_key": "[REDACTED]",
            "connection": {
                "host": "localhost",
                "password": "[REDACTED]",
            },
            "dataset": "daily_prices",
            "environment": "[REDACTED]",
            "stage": "download",
        },
        "retryable": True,
    }
    task_error = json.loads(task["error_json"])
    attempt_error = json.loads(attempt["error_json"])
    audit = _finished_audit(engine, task_id)
    assert task_error == attempt_error == audit["error"] == expected
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.exc_info is None
    assert record.exception_type == "quant_core.errors.QuantError"
    assert record.frames
    assert all(set(frame) == {"file", "function", "line"} for frame in record.frames)
    disclosed = " ".join(
        (
            task["error_json"],
            attempt["error_json"],
            json.dumps(audit, sort_keys=True),
            caplog.text,
            repr(record.__dict__),
        )
    )
    assert all(secret not in disclosed for secret in secrets)


def test_unknown_exception_maps_without_message_but_logs_sanitized_frames(
    engine: Engine,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unknown exception text must not survive while frame evidence remains locatable."""
    secret = "runtime-token-secret"
    queue = TaskQueue(engine, clock=lambda: NOW)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    logger = logging.getLogger("tests.worker.unhandled")
    caplog.set_level(logging.ERROR, logger=logger.name)
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(_RaisingHandler(RuntimeError(f"token={secret}")),),
        clock=lambda: NOW,
        logger=logger,
    )

    assert worker.run_once() is True

    task, attempt = _runtime_rows(engine, task_id)
    expected = {"code": "WORKER_UNHANDLED_ERROR", "retryable": False}
    assert json.loads(task["error_json"]) == expected
    assert json.loads(attempt["error_json"]) == expected
    assert _finished_audit(engine, task_id)["error"] == expected
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.exc_info is None
    assert record.exception_type == "builtins.RuntimeError"
    assert record.frames
    disclosed = " ".join(
        (
            task["error_json"],
            attempt["error_json"],
            json.dumps(_finished_audit(engine, task_id), sort_keys=True),
            caplog.text,
            repr(record.__dict__),
        )
    )
    assert secret not in disclosed


def test_returned_failed_outcome_is_normalized_before_persistence(
    engine: Engine,
) -> None:
    """Trusting arbitrary handler error JSON would bypass the worker disclosure boundary."""
    queue = TaskQueue(engine, clock=lambda: NOW)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(_UnsafeFailureHandler(),),
        clock=lambda: NOW,
    )

    assert worker.run_once() is True

    task, attempt = _runtime_rows(engine, task_id)
    expected = {
        "code": "HANDLER_FAILED",
        "context": {
            "api_key": "[REDACTED]",
            "stage": "publish",
        },
        "retryable": True,
    }
    assert json.loads(task["error_json"]) == expected
    assert json.loads(attempt["error_json"]) == expected
    assert _finished_audit(engine, task_id)["error"] == expected
    serialized = " ".join(
        (task["error_json"], attempt["error_json"], json.dumps(expected))
    )
    assert "returned-message-secret" not in serialized
    assert "returned-api-key-secret" not in serialized
    assert "returned-opaque-secret" not in serialized


def test_background_heartbeat_failure_propagates_without_success_finish(
    engine: Engine,
) -> None:
    """Losing the heartbeat fence must make a later handler success non-committable."""
    delegate = TaskQueue(engine, clock=lambda: NOW)
    task_id = delegate.enqueue("BACKTEST", {}, 0)
    queue = _HeartbeatFailureQueue(delegate)
    handler = _IgnoresHeartbeatFailureHandler(queue.failed)

    def heartbeat_waiter(stop: threading.Event, timeout: float) -> bool:
        del timeout
        return stop.is_set()

    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
        heartbeat_waiter=heartbeat_waiter,
    )

    with pytest.raises(QuantError) as captured:
        worker.run_once()

    assert captured.value.detail.code == "TASK_OWNERSHIP_CONFLICT"
    assert queue.finish_calls == 0
    task, attempt = _runtime_rows(engine, task_id)
    assert task["status"] == attempt["status"] == "RUNNING"
