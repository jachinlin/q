"""Behavioral tests for the single-task durable worker runtime."""

from __future__ import annotations

import importlib
import json
import logging
import multiprocessing
import threading
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, event, text

from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.tasks.models import TaskOutcome, TaskProgress, TaskStatus

NOW = datetime(2026, 7, 30, 8, tzinfo=UTC)
LATEST_PROGRESS = TaskProgress(
    stage="backtest",
    completed=3,
    total=7,
    message="three sessions complete",
)


def _worker_type() -> Any:
    try:
        module = importlib.import_module("quant_research.application.worker")
    except ModuleNotFoundError:
        pytest.fail("quant_research.application.worker is missing", pytrace=False)
    worker_type = getattr(module, "Worker", None)
    assert worker_type is not None, (
        "quant_research.application.worker.Worker is missing"
    )
    return worker_type


def _handlers_module() -> Any:
    return importlib.import_module("quant_research.tasks.handlers")


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


def _runtime_rows(
    engine: Engine, task_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
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


class _DetailedProgressHandler:
    task_type = "BACKTEST"

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, cancellation
        progress.update(
            TaskProgress(
                stage="LOCALIZE",
                completed=16,
                total=20,
                message="正在下载 stock_daily_bar / daily · trade_date=20260825",
                context={
                    "dataset": "stock_daily_bar",
                    "endpoint": "daily",
                    "request": {"trade_date": "20260825"},
                },
            )
        )
        return TaskOutcome(status=TaskStatus.SUCCEEDED)


class _BurstProgressHandler:
    task_type = "BACKTEST"

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, cancellation
        for completed in range(10):
            progress.update(
                TaskProgress(
                    stage="LOCALIZE",
                    completed=completed,
                    total=10,
                    message=f"request {completed}",
                    context={"boundary": "raw_request"},
                )
            )
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
        status = TaskStatus.CANCELLED if self.observations[-1] else TaskStatus.SUCCEEDED
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

    def finish(self, attempt_id: str, worker_id: str, outcome: TaskOutcome) -> None:
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


class _ProgressThenRaisingHandler:
    task_type = "BACKTEST"

    def __init__(self, error: Exception) -> None:
        self._error = error

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, cancellation
        progress.update(
            TaskProgress(
                stage="ANALYZE_FACTORS",
                completed=2,
                total=4,
                message="正在重新计算研究因子",
                context={
                    "substage": "COMPUTE_FACTORS",
                    "substage_state": "STARTED",
                },
            )
        )
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
                    "stage": "returned-stage-secret",
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

    def finish(self, attempt_id: str, worker_id: str, outcome: TaskOutcome) -> None:
        self.finish_calls += 1
        self._delegate.finish(attempt_id, worker_id, outcome)


class _ProgressRaceQueue:
    def __init__(self, delegate: TaskQueue) -> None:
        self._delegate = delegate
        self.periodic_captured = threading.Event()
        self.release_periodic = threading.Event()
        self.immediate_persisted = threading.Event()

    def claim(self, worker_id: str, now: datetime) -> Any:
        return self._delegate.claim(worker_id, now)

    def heartbeat(
        self,
        attempt_id: str,
        worker_id: str,
        progress: TaskProgress,
        now: datetime,
    ) -> None:
        if threading.current_thread().name.startswith("quant-worker-heartbeat-"):
            self.periodic_captured.set()
            if not self.release_periodic.wait(timeout=2):
                raise TimeoutError("periodic heartbeat test barrier was not released")
            self._delegate.heartbeat(attempt_id, worker_id, progress, now)
            return
        self._delegate.heartbeat(attempt_id, worker_id, progress, now)
        self.immediate_persisted.set()

    def is_cancel_requested(self, attempt_id: str, worker_id: str) -> bool:
        return self._delegate.is_cancel_requested(attempt_id, worker_id)

    def finish(self, attempt_id: str, worker_id: str, outcome: TaskOutcome) -> None:
        self._delegate.finish(attempt_id, worker_id, outcome)


class _ProgressRaceHandler:
    task_type = "BACKTEST"

    def __init__(self) -> None:
        self.update_started = threading.Event()

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, cancellation
        self.update_started.set()
        progress.update(LATEST_PROGRESS)
        return TaskOutcome(status=TaskStatus.SUCCEEDED)


class _FinishCancellationRaceQueue:
    def __init__(
        self,
        delegate: TaskQueue,
        blocked_status: TaskStatus = TaskStatus.SUCCEEDED,
    ) -> None:
        self._delegate = delegate
        self._blocked_status = blocked_status
        self.finish_entered = threading.Event()
        self.release_finish = threading.Event()
        self.finish_statuses: list[TaskStatus] = []

    def claim(self, worker_id: str, now: datetime) -> Any:
        return self._delegate.claim(worker_id, now)

    def heartbeat(
        self,
        attempt_id: str,
        worker_id: str,
        progress: TaskProgress,
        now: datetime,
    ) -> None:
        self._delegate.heartbeat(attempt_id, worker_id, progress, now)

    def is_cancel_requested(self, attempt_id: str, worker_id: str) -> bool:
        return self._delegate.is_cancel_requested(attempt_id, worker_id)

    def finish(self, attempt_id: str, worker_id: str, outcome: TaskOutcome) -> None:
        self.finish_statuses.append(outcome.status)
        if outcome.status is self._blocked_status:
            self.finish_entered.set()
            if not self.release_finish.wait(timeout=2):
                raise TimeoutError("finish test barrier was not released")
        self._delegate.finish(attempt_id, worker_id, outcome)


class _FinalBoundarySuccessHandler:
    task_type = "BACKTEST"

    def __init__(self) -> None:
        self.observed: bool | None = None

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, progress
        self.observed = cancellation.is_cancelled()
        return TaskOutcome(status=TaskStatus.SUCCEEDED)


class _FinalBoundaryFailedHandler:
    task_type = "BACKTEST"

    def __init__(self) -> None:
        self.observed: bool | None = None

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, progress
        self.observed = cancellation.is_cancelled()
        return TaskOutcome(
            status=TaskStatus.FAILED,
            error={"code": "HANDLER_FAILED", "retryable": False},
        )


class _IgnoresHeartbeatFailureHandler:
    task_type = "BACKTEST"

    def __init__(self, failed: threading.Event) -> None:
        self._failed = failed

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, progress, cancellation
        self._failed.wait(timeout=2)
        return TaskOutcome(status=TaskStatus.SUCCEEDED)


class _ConcurrentHandler:
    task_type = "BACKTEST"

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._rendezvous = threading.Barrier(2)
        self.active = 0
        self.max_active = 0

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, progress, cancellation
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        try:
            self._rendezvous.wait(timeout=1)
        except threading.BrokenBarrierError:
            pass
        finally:
            with self._lock:
                self.active -= 1
        return TaskOutcome(status=TaskStatus.SUCCEEDED)


class _BlockingClaimQueue:
    def __init__(self, delegate: TaskQueue) -> None:
        self._delegate = delegate
        self.claim_entered = threading.Event()
        self.release_claim = threading.Event()

    def claim(self, worker_id: str, now: datetime) -> Any:
        self.claim_entered.set()
        if not self.release_claim.wait(timeout=2):
            raise TimeoutError("claim test barrier was not released")
        return self._delegate.claim(worker_id, now)

    def heartbeat(
        self,
        attempt_id: str,
        worker_id: str,
        progress: TaskProgress,
        now: datetime,
    ) -> None:
        self._delegate.heartbeat(attempt_id, worker_id, progress, now)

    def is_cancel_requested(self, attempt_id: str, worker_id: str) -> bool:
        return self._delegate.is_cancel_requested(attempt_id, worker_id)

    def finish(self, attempt_id: str, worker_id: str, outcome: TaskOutcome) -> None:
        self._delegate.finish(attempt_id, worker_id, outcome)


class _DelayedCancellationHandler:
    task_type = "BACKTEST"

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.allow_check = threading.Event()
        self.observed: bool | None = None

    def run(self, task: Any, progress: Any, cancellation: Any) -> TaskOutcome:
        del task, progress
        self.entered.set()
        self.allow_check.wait(timeout=2)
        self.observed = cancellation.is_cancelled()
        return TaskOutcome(
            status=(TaskStatus.CANCELLED if self.observed else TaskStatus.SUCCEEDED)
        )


class _TraversalBombMapping(Mapping[str, object]):
    def __init__(self) -> None:
        self.visits = 0

    def __getitem__(self, key: str) -> object:
        return f"value-for-{key}"

    def __iter__(self) -> Iterator[str]:
        while True:
            self.visits += 1
            if self.visits > 50:
                raise RuntimeError("normalizer traversed beyond its input cap")
            yield f"opaque_{self.visits}"

    def __len__(self) -> int:
        return 1_000_000

    def items(self) -> Any:
        raise RuntimeError("normalizer invoked an untrusted eager items method")


def _run_in_thread(operation: Callable[[], bool]) -> tuple[threading.Thread, list[Any]]:
    results: list[Any] = []

    def run() -> None:
        try:
            results.append(operation())
        except BaseException as error:  # noqa: BLE001 - surfaced in test thread
            results.append(error)

    thread = threading.Thread(target=run, name="worker-test-runner", daemon=False)
    thread.start()
    return thread, results


def _run_shutdown_from_claim(
    claim_entered: Any,
    shutdown_returned: Any,
    output: Any,
) -> None:
    from quant_research.application.worker import Worker

    class ShutdownFromClaimQueue:
        worker: Worker

        def claim(self, worker_id: str, now: datetime) -> None:
            del worker_id, now
            claim_entered.set()
            self.worker.request_shutdown()
            shutdown_returned.set()

    queue = ShutdownFromClaimQueue()
    worker = Worker(
        queue,  # type: ignore[arg-type] - claim is the only reachable operation
        worker_id="worker-1",
        handlers=(),
        clock=lambda: NOW,
    )
    queue.worker = worker
    output.put(worker.run_once())


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


def test_run_once_recovers_orphans_on_start_and_then_throttles_scans(
    engine: Engine,
) -> None:
    """首次轮询必须恢复，后续轮询只在扫描间隔到期后再次恢复。"""
    queue = TaskQueue(engine, clock=lambda: NOW)
    clock = [NOW]
    recoveries: list[datetime] = []

    def recover(now: datetime) -> int:
        recoveries.append(now)
        return 0

    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(),
        clock=lambda: clock[0],
        orphan_recovery=recover,
        orphan_recovery_interval=30.0,
    )

    assert worker.run_once() is False
    clock[0] = NOW + timedelta(seconds=29)
    assert worker.run_once() is False
    clock[0] = NOW + timedelta(seconds=30)
    assert worker.run_once() is False

    assert recoveries == [NOW, NOW + timedelta(seconds=30)]


def test_same_worker_serializes_concurrent_run_once_claim_through_finish(
    engine: Engine,
) -> None:
    """Removing the instance execution guard lets two handlers overlap."""
    queue = TaskQueue(engine, clock=lambda: NOW)
    queue.enqueue("BACKTEST", {}, 0)
    queue.enqueue("BACKTEST", {}, 0)
    handler = _ConcurrentHandler()
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
    )
    start = threading.Barrier(3)
    results: list[bool] = []
    failures: list[BaseException] = []

    def call() -> None:
        try:
            start.wait(timeout=2)
            results.append(worker.run_once())
        except BaseException as error:  # noqa: BLE001 - surfaced below
            failures.append(error)

    threads = [threading.Thread(target=call) for _ in range(2)]
    for thread in threads:
        thread.start()
    try:
        start.wait(timeout=2)
    finally:
        for thread in threads:
            thread.join(timeout=3)

    assert not any(thread.is_alive() for thread in threads)
    assert failures == []
    assert results == [True, True]
    assert handler.max_active == 1
    assert _statuses(engine) == ["SUCCEEDED", "SUCCEEDED"]


def test_claim_can_request_shutdown_reentrantly_without_deadlock() -> None:
    """Replacing the claim gate with a non-reentrant lock deadlocks this call."""
    context = multiprocessing.get_context("spawn")
    claim_entered = context.Event()
    shutdown_returned = context.Event()
    output = context.Queue()
    process = context.Process(
        target=_run_shutdown_from_claim,
        args=(claim_entered, shutdown_returned, output),
        name="worker-reentrant-shutdown-test",
    )
    process.start()
    try:
        # Windows ``spawn`` imports the test module in a fresh interpreter;
        # loaded scientific dependencies can exceed the old two-second budget.
        assert claim_entered.wait(timeout=10)
        assert shutdown_returned.wait(timeout=2)
        process.join(timeout=10)
        assert not process.is_alive()
        assert process.exitcode == 0
        assert output.get(timeout=2) is False
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=0.5)
        if process.is_alive():
            process.kill()
            process.join(timeout=0.5)
        output.cancel_join_thread()
        output.close()


def test_claim_started_before_shutdown_holds_the_shared_linearization_gate(
    engine: Engine,
) -> None:
    """A shutdown call cannot linearize between the pre-claim check and claim."""
    delegate = TaskQueue(engine, clock=lambda: NOW)
    task_id = delegate.enqueue("BACKTEST", {}, 0)
    queue = _BlockingClaimQueue(delegate)
    handler = _DelayedCancellationHandler()
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
    )
    run_thread, results = _run_in_thread(worker.run_once)
    shutdown_invoked = threading.Event()
    shutdown_completed = threading.Event()

    def request_shutdown() -> None:
        shutdown_invoked.set()
        worker.request_shutdown()
        shutdown_completed.set()

    shutdown_thread = threading.Thread(target=request_shutdown)
    completed_before_claim = False
    try:
        assert queue.claim_entered.wait(timeout=2)
        shutdown_thread.start()
        assert shutdown_invoked.wait(timeout=2)
        completed_before_claim = shutdown_completed.wait(timeout=0.2)
        queue.release_claim.set()
        assert shutdown_completed.wait(timeout=2)
        assert handler.entered.wait(timeout=2)
        handler.allow_check.set()
    finally:
        queue.release_claim.set()
        handler.allow_check.set()
        run_thread.join(timeout=3)
        if shutdown_thread.ident is not None:
            shutdown_thread.join(timeout=3)

    assert completed_before_claim is False
    assert not run_thread.is_alive()
    assert not shutdown_thread.is_alive()
    assert results == [True]
    assert handler.observed is True
    task, attempt = _runtime_rows(engine, task_id)
    assert task["status"] == attempt["status"] == "CANCELLED"


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


@pytest.mark.parametrize(
    "parameter",
    ["poll_interval", "heartbeat_interval", "heartbeat_join_timeout"],
)
@pytest.mark.parametrize(
    "value",
    [True, 0.0, -0.1, float("nan"), float("inf")],
)
def test_worker_rejects_invalid_timing_before_claim(
    engine: Engine,
    parameter: str,
    value: float,
) -> None:
    """Invalid waits must fail during construction, before durable work is claimed."""
    queue = TaskQueue(engine, clock=lambda: NOW)
    queue.enqueue("BACKTEST", {}, 0)

    with pytest.raises(ValueError, match="finite positive number"):
        _worker_type()(
            queue,
            worker_id="worker-1",
            handlers=(_SuccessHandler(),),
            clock=lambda: NOW,
            **{parameter: value},
        )

    assert _statuses(engine) == ["QUEUED"]


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


def test_rapid_request_progress_is_coalesced_and_terminal_value_is_flushed(
    engine: Engine,
) -> None:
    delegate = TaskQueue(engine, clock=lambda: NOW)
    task_id = delegate.enqueue("BACKTEST", {}, 0)
    queue = _RecordingQueue(delegate, threading.Event())
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(_BurstProgressHandler(),),
        clock=lambda: NOW,
    )

    assert worker.run_once() is True

    assert [item.completed for item in queue.heartbeats] == [0, 9]
    task, attempt = _runtime_rows(engine, task_id)
    assert json.loads(task["progress_json"])["completed"] == 9
    assert task["progress_json"] == attempt["progress_json"]


def test_periodic_heartbeat_cannot_overwrite_newer_immediate_progress(
    engine: Engine,
) -> None:
    """Releasing progress locking before persistence permits stale overwrite."""
    delegate = TaskQueue(engine, clock=lambda: NOW)
    task_id = delegate.enqueue("BACKTEST", {}, 0)
    queue = _ProgressRaceQueue(delegate)
    handler = _ProgressRaceHandler()
    waiter_calls = 0

    def heartbeat_waiter(stop: threading.Event, timeout: float) -> bool:
        nonlocal waiter_calls
        waiter_calls += 1
        if waiter_calls == 1:
            return False
        return stop.wait(timeout)

    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
        heartbeat_waiter=heartbeat_waiter,
    )
    thread, results = _run_in_thread(worker.run_once)
    immediate_wrote_while_old_blocked = False
    try:
        assert queue.periodic_captured.wait(timeout=2)
        assert handler.update_started.wait(timeout=2)
        immediate_wrote_while_old_blocked = queue.immediate_persisted.wait(timeout=0.2)
        queue.release_periodic.set()
    finally:
        queue.release_periodic.set()
        thread.join(timeout=3)

    assert not thread.is_alive()
    assert results == [True]
    assert immediate_wrote_while_old_blocked is False
    task, attempt = _runtime_rows(engine, task_id)
    assert json.loads(task["progress_json"]) == LATEST_PROGRESS.model_dump()
    assert task["progress_json"] == attempt["progress_json"]


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
    finally:
        handler.continue_from_boundary.set()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert results == [True]
    assert handler.observations == [False, True]
    task, attempt = _runtime_rows(engine, task_id)
    assert task["status"] == attempt["status"] == "CANCELLED"


def test_cancel_winning_after_last_boundary_converges_success_to_cancelled(
    engine: Engine,
) -> None:
    """Letting the success conflict escape strands a valid CANCEL_REQUESTED pair."""
    delegate = TaskQueue(engine, clock=lambda: NOW)
    task_id = delegate.enqueue("BACKTEST", {}, 0)
    queue = _FinishCancellationRaceQueue(delegate)
    handler = _FinalBoundarySuccessHandler()
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
    )
    thread, results = _run_in_thread(worker.run_once)
    try:
        assert queue.finish_entered.wait(timeout=2)
        delegate.request_cancel(task_id, "user-1")
        queue.release_finish.set()
    finally:
        queue.release_finish.set()
        thread.join(timeout=3)

    assert not thread.is_alive()
    assert results == [True]
    assert handler.observed is False
    assert queue.finish_statuses == [TaskStatus.SUCCEEDED, TaskStatus.CANCELLED]
    task, attempt = _runtime_rows(engine, task_id)
    assert task["status"] == attempt["status"] == "CANCELLED"


def test_cancel_winning_after_final_boundary_converges_handler_failed_outcome(
    engine: Engine,
) -> None:
    """Restricting cancellation convergence to success strands failed work."""
    delegate = TaskQueue(engine, clock=lambda: NOW)
    task_id = delegate.enqueue("BACKTEST", {}, 0)
    queue = _FinishCancellationRaceQueue(delegate, TaskStatus.FAILED)
    handler = _FinalBoundaryFailedHandler()
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
    )
    thread, results = _run_in_thread(worker.run_once)
    try:
        assert queue.finish_entered.wait(timeout=2)
        delegate.request_cancel(task_id, "user-1")
        queue.release_finish.set()
    finally:
        queue.release_finish.set()
        thread.join(timeout=3)

    assert not thread.is_alive()
    assert results == [True]
    assert handler.observed is False
    assert queue.finish_statuses == [TaskStatus.FAILED, TaskStatus.CANCELLED]
    task, attempt = _runtime_rows(engine, task_id)
    assert task["status"] == attempt["status"] == "CANCELLED"


def test_cancel_winning_unknown_task_finish_converges_to_cancelled(
    engine: Engine,
) -> None:
    """Bypassing the shared finish policy strands an unknown claimed task."""
    delegate = TaskQueue(engine, clock=lambda: NOW)
    task_id = delegate.enqueue("UNKNOWN_TASK", {}, 0)
    queue = _FinishCancellationRaceQueue(delegate, TaskStatus.FAILED)
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(),
        clock=lambda: NOW,
    )
    thread, results = _run_in_thread(worker.run_once)
    try:
        assert queue.finish_entered.wait(timeout=2)
        delegate.request_cancel(task_id, "user-1")
        queue.release_finish.set()
    finally:
        queue.release_finish.set()
        thread.join(timeout=3)

    assert not thread.is_alive()
    assert results == [True]
    assert queue.finish_statuses == [TaskStatus.FAILED, TaskStatus.CANCELLED]
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
    finally:
        handler.continue_from_boundary.set()
        thread.join(timeout=2)

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
            text("UPDATE task SET status = 'CANCEL_REQUESTED' WHERE id = :task_id"),
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


def test_delete_removes_only_terminal_task_records_and_preserves_audit(
    engine: Engine,
) -> None:
    queue = TaskQueue(engine, clock=lambda: NOW)
    active_id = queue.enqueue("DATA_UPDATE", {"start": None, "end": None}, 0)
    with pytest.raises(QuantError) as active:
        queue.delete(active_id, "dashboard", request_id="delete-active")
    assert active.value.detail.code == "TASK_STATE_CONFLICT"
    assert queue.get(active_id).status is TaskStatus.QUEUED

    terminal_id = queue.enqueue("BACKTEST", {"experiment_id": "example"}, 1)
    claimed = queue.claim("worker-1", NOW)
    assert claimed is not None
    assert claimed.id == terminal_id
    queue.finish(
        claimed.attempt_id,
        claimed.worker_id,
        TaskOutcome(status=TaskStatus.SUCCEEDED, result={"artifact": "kept"}),
    )

    queue.delete(terminal_id, "dashboard", request_id="delete-terminal")

    with pytest.raises(QuantError):
        queue.get(terminal_id)
    with engine.connect() as connection:
        assert (
            connection.scalar(
                text("SELECT COUNT(*) FROM task_attempt WHERE task_id = :task_id"),
                {"task_id": terminal_id},
            )
            == 0
        )
        event_row = (
            connection.execute(
                text(
                    "SELECT task_id, details_json FROM audit_event "
                    "WHERE event_type = 'TASK_DELETED' ORDER BY id DESC LIMIT 1"
                )
            )
            .mappings()
            .one()
        )
    assert event_row["task_id"] is None
    assert json.loads(event_row["details_json"]) == {
        "request_id": "delete-terminal",
        "status": "SUCCEEDED",
        "task_id": terminal_id,
        "task_type": "BACKTEST",
    }


def test_cancel_query_reads_task_and_attempt_from_one_concurrent_snapshot(
    engine: Engine,
) -> None:
    """Separate SELECTs can invent a state mismatch during valid cancellation."""
    queue = TaskQueue(engine, clock=lambda: NOW)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    claimed = queue.claim("worker-1", NOW)
    assert claimed is not None
    read_completed = threading.Event()
    release_read = threading.Event()

    @event.listens_for(engine, "after_cursor_execute")
    def pause_after_attempt_read(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        normalized = " ".join(statement.lower().split())
        if (
            not read_completed.is_set()
            and "from task_attempt" in normalized
            and "task_attempt.id" in normalized
        ):
            read_completed.set()
            if not release_read.wait(timeout=2):
                raise TimeoutError("cancellation read barrier was not released")

    thread, results = _run_in_thread(
        lambda: queue.is_cancel_requested(claimed.attempt_id, "worker-1")
    )
    try:
        assert read_completed.wait(timeout=2)
        queue.request_cancel(task_id, "user-1")
    finally:
        release_read.set()
        thread.join(timeout=3)
        event.remove(engine, "after_cursor_execute", pause_after_attempt_read)

    assert not thread.is_alive()
    assert results == [False]
    task, attempt = _runtime_rows(engine, task_id)
    assert task["status"] == attempt["status"] == "CANCEL_REQUESTED"
    queue.finish(
        claimed.attempt_id,
        "worker-1",
        TaskOutcome(status=TaskStatus.CANCELLED),
    )


def test_handler_registry_names_standard_types_and_rejects_duplicate_registration() -> (
    None
):
    """A missing standard type or silent duplicate would make dispatch ambiguous."""
    module = _handlers_module()
    registry_type = getattr(module, "HandlerRegistry", None)
    assert registry_type is not None, "HandlerRegistry is missing"
    standard = {
        "DATA_BOOTSTRAP",
        "DATA_UPDATE",
        "DATA_VALIDATION",
        "STRATEGY_STUDY",
        "FACTOR_STUDY",
    }
    assert getattr(module, "STANDARD_TASK_TYPES", None) == frozenset(standard)
    registry = registry_type()
    handlers = [_TypedSuccessHandler(task_type) for task_type in sorted(standard)]

    for handler in handlers:
        registry.register(handler)

    assert [registry.get(name) for name in sorted(standard)] == handlers
    with pytest.raises(ValueError, match="already registered"):
        registry.register(_TypedSuccessHandler("STRATEGY_STUDY"))


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
        "quant-dataset-secret",
        "quant-stage-secret",
        "quant-host-secret",
        "quant-list-secret",
        "quant-nested-secret",
        "quant-key-name-secret",
    }
    detail = ErrorDetail(
        code="DATA_PROVIDER_TIMEOUT",
        severity=Severity.SEVERE,
        message="provider failed with quant-message-secret",
        context={
            "dataset": "quant-dataset-secret",
            "stage": "quant-stage-secret",
            "api_key": "quant-api-key-secret",
            "api_key_quant-key-name-secret": "already redacted",
            "connection": {
                "host": "quant-host-secret",
                "password": "quant-password-secret",
            },
            "environment": {"DATABASE_URL": "quant-environment-secret"},
            "opaque": "quant-opaque-secret",
            "target": [
                "quant-list-secret",
                {"provider": "quant-nested-secret"},
            ],
            "attempt": 3,
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
            "attempt": 3,
            "api_key": "[REDACTED]",
            "connection": {
                "host": "[REDACTED]",
                "password": "[REDACTED]",
            },
            "dataset": "[REDACTED]",
            "environment": "[REDACTED]",
            "redacted": "[REDACTED]",
            "stage": "[REDACTED]",
            "target": [
                "[REDACTED]",
                {"provider": "[REDACTED]"},
            ],
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
    assert record.exception_type == "quant_research.domain.errors.QuantError"
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


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        (
            ["x" * 1_024],
            {
                "code": "BUDGETED_CONTEXT",
                "context": {"target": ["[REDACTED]"]},
                "retryable": False,
            },
        ),
        (
            ["x" * 2_048 for _ in range(50)],
            {
                "code": "WORKER_ERROR_NORMALIZATION_FAILED",
                "retryable": False,
            },
        ),
    ],
)
def test_error_normalization_has_one_global_utf8_budget_and_safe_fallback(
    engine: Engine,
    target: list[str],
    expected: dict[str, Any],
) -> None:
    """Per-value caps can exceed TaskOutcome's bound and strand RUNNING work."""
    detail = ErrorDetail(
        code="BUDGETED_CONTEXT",
        severity=Severity.SEVERE,
        message="bounded failure",
        context={"target": target},
        remediation="inspect bounded context",
        retryable=False,
    )
    queue = TaskQueue(engine, clock=lambda: NOW)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(_RaisingHandler(QuantError(detail)),),
        clock=lambda: NOW,
    )

    assert worker.run_once() is True

    task, attempt = _runtime_rows(engine, task_id)
    assert task["status"] == attempt["status"] == "FAILED"
    assert json.loads(task["error_json"]) == expected
    assert json.loads(attempt["error_json"]) == expected
    assert _finished_audit(engine, task_id)["error"] == expected


def test_error_normalization_stops_mapping_input_at_global_traversal_cap(
    engine: Engine,
) -> None:
    """Eagerly enumerating a Mapping lets untrusted context make work unbounded."""
    context = _TraversalBombMapping()
    detail = ErrorDetail(
        code="BOUNDED_CONTEXT",
        severity=Severity.SEVERE,
        message="bounded traversal",
        context=context,
        remediation="inspect bounded context",
        retryable=False,
    )
    queue = TaskQueue(engine, clock=lambda: NOW)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(_RaisingHandler(QuantError(detail)),),
        clock=lambda: NOW,
    )

    assert worker.run_once() is True

    expected = {"code": "BOUNDED_CONTEXT", "retryable": False}
    assert context.visits == 50
    task, attempt = _runtime_rows(engine, task_id)
    assert json.loads(task["error_json"]) == expected
    assert json.loads(attempt["error_json"]) == expected
    assert _finished_audit(engine, task_id)["error"] == expected


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


def test_task_diagnostic_log_records_complete_exception_without_redaction(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """任务专属日志应保留完整异常消息和 traceback，同时持久化错误仍保持有界。"""
    logging_module = importlib.import_module("quant_research.logging")
    diagnostic_root = tmp_path / "state" / "task-logs"
    artifact_root = tmp_path / "artifacts"
    manager = logging_module.TaskLogManager(
        diagnostic_root=diagnostic_root,
        artifact_root=artifact_root,
        sensitive_values=(),
    )
    queue = TaskQueue(
        engine,
        clock=lambda: NOW,
        task_log_root=diagnostic_root,
    )
    task_id = queue.enqueue("BACKTEST", {}, 0)
    message = "complete worker diagnostic message"
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(_ProgressThenRaisingHandler(RuntimeError(message)),),
        clock=lambda: NOW,
        task_logs=manager,
    )

    assert worker.run_once() is True

    task, attempt = _runtime_rows(engine, task_id)
    assert json.loads(task["error_json"]) == {
        "code": "WORKER_UNHANDLED_ERROR",
        "retryable": False,
    }
    log_text = Path(attempt["log_path"]).read_text(encoding="utf-8")
    records = [json.loads(line) for line in log_text.splitlines()]
    failure = next(item for item in records if item["event"] == "task.handler_failed")
    assert failure["context"]["exception_message"] == message
    assert message in failure["context"]["traceback"]
    assert "RuntimeError" in failure["context"]["traceback"]
    assert failure["context"]["retryable"] is False
    assert failure["context"]["remediation"] == (
        "inspect the traceback and task inputs before retrying"
    )
    assert failure["context"]["last_progress"] == {
        "stage": "ANALYZE_FACTORS",
        "completed": 2,
        "total": 4,
        "message": "正在重新计算研究因子",
        "context": {
            "substage": "COMPUTE_FACTORS",
            "substage_state": "STARTED",
        },
    }


def test_task_diagnostic_log_keeps_structured_progress_details(
    engine: Engine,
    tmp_path: Path,
) -> None:
    logging_module = importlib.import_module("quant_research.logging")
    diagnostic_root = tmp_path / "state" / "task-logs"
    manager = logging_module.TaskLogManager(
        diagnostic_root=diagnostic_root,
        artifact_root=tmp_path / "artifacts",
    )
    queue = TaskQueue(engine, clock=lambda: NOW, task_log_root=diagnostic_root)
    task_id = queue.enqueue("BACKTEST", {}, 0)
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(_DetailedProgressHandler(),),
        clock=lambda: NOW,
        task_logs=manager,
    )

    assert worker.run_once() is True

    _, attempt = _runtime_rows(engine, task_id)
    records = [
        json.loads(line)
        for line in Path(attempt["log_path"]).read_text(encoding="utf-8").splitlines()
    ]
    event = next(item for item in records if item["event"] == "task.progress")
    assert event["stage"] == "LOCALIZE"
    assert event["context"]["completed"] == 16
    assert event["context"]["total"] == 20
    assert event["context"]["details"] == {
        "dataset": "stock_daily_bar",
        "endpoint": "daily",
        "request": {"trade_date": "20260825"},
    }


def test_task_log_open_failure_does_not_block_the_task(
    engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A diagnostic file open failure must leave task execution authoritative."""
    logging_module = importlib.import_module("quant_research.logging")
    diagnostic_root = tmp_path / "state" / "task-logs"
    manager = logging_module.TaskLogManager(
        diagnostic_root=diagnostic_root,
        artifact_root=tmp_path / "artifacts",
    )
    queue = TaskQueue(
        engine,
        clock=lambda: NOW,
        task_log_root=diagnostic_root,
    )
    task_id = queue.enqueue("BACKTEST", {}, 0)
    handler = _SuccessHandler()

    def fail_open(_context: object) -> None:
        raise logging_module.StructuredLogWriteError(
            "open", OSError("diagnostic volume unavailable")
        )

    monkeypatch.setattr(manager, "open", fail_open)
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
        task_logs=manager,
    )

    assert worker.run_once() is True

    task, attempt = _runtime_rows(engine, task_id)
    assert task["status"] == TaskStatus.SUCCEEDED.value
    assert attempt["status"] == TaskStatus.SUCCEEDED.value
    assert attempt["log_path"] is None
    assert handler.task_ids == [task_id]


def test_task_log_binding_failure_does_not_strand_claimed_task(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """日志根装配错误不得让已认领任务永久停留在 RUNNING。"""
    logging_module = importlib.import_module("quant_research.logging")
    manager = logging_module.TaskLogManager(
        diagnostic_root=tmp_path / "manager-task-logs",
        artifact_root=tmp_path / "artifacts",
    )
    queue = TaskQueue(
        engine,
        clock=lambda: NOW,
        task_log_root=tmp_path / "queue-task-logs",
    )
    task_id = queue.enqueue("BACKTEST", {}, 0)
    handler = _SuccessHandler()
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(handler,),
        clock=lambda: NOW,
        task_logs=manager,
    )

    assert worker.run_once() is True

    task, attempt = _runtime_rows(engine, task_id)
    assert task["status"] == TaskStatus.SUCCEEDED.value
    assert attempt["status"] == TaskStatus.SUCCEEDED.value
    assert attempt["log_path"] is None
    assert handler.task_ids == [task_id]


def test_task_diagnostic_log_keeps_quant_error_action_fields(
    engine: Engine,
    tmp_path: Path,
) -> None:
    """领域异常的严重度、可重试性和处理建议应进入受控诊断日志。"""
    logging_module = importlib.import_module("quant_research.logging")
    diagnostic_root = tmp_path / "state" / "task-logs"
    manager = logging_module.TaskLogManager(
        diagnostic_root=diagnostic_root,
        artifact_root=tmp_path / "artifacts",
        sensitive_values=(),
    )
    queue = TaskQueue(
        engine,
        clock=lambda: NOW,
        task_log_root=diagnostic_root,
    )
    task_id = queue.enqueue("BACKTEST", {}, 0)
    detail = ErrorDetail(
        code="DATA_HASH_DRIFT",
        severity=Severity.SEVERE,
        message="validated catalog is stale",
        context={"stage": "VALIDATE"},
        remediation="run validate-all before retrying",
        retryable=False,
    )
    worker = _worker_type()(
        queue,
        worker_id="worker-1",
        handlers=(_RaisingHandler(QuantError(detail)),),
        clock=lambda: NOW,
        task_logs=manager,
    )

    assert worker.run_once() is True

    _, attempt = _runtime_rows(engine, task_id)
    records = [
        json.loads(line)
        for line in Path(attempt["log_path"]).read_text(encoding="utf-8").splitlines()
    ]
    failure = next(item for item in records if item["event"] == "task.handler_failed")
    assert failure["error_code"] == "DATA_HASH_DRIFT"
    assert failure["context"]["severity"] == "SEVERE"
    assert failure["context"]["retryable"] is False
    assert failure["context"]["remediation"] == "run validate-all before retrying"


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
            "stage": "[REDACTED]",
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
    assert "returned-stage-secret" not in serialized


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
