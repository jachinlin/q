"""Single-task worker runtime over the durable SQLite queue."""

from __future__ import annotations

import logging
import math
import re
import threading
import traceback
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

from quant_core.data.contracts import JsonValue
from quant_core.errors import ErrorDetail, QuantError
from quant_core.tasks.handlers import HandlerRegistry, TaskHandler
from quant_core.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskProgress,
    TaskStatus,
)

type _Waiter = Callable[[threading.Event, float], bool]

_SAFE_CONTEXT_FIELDS = frozenset(
    {
        "actual",
        "attempt",
        "attempt_id",
        "connection",
        "dataset",
        "error_code",
        "expected",
        "experiment_id",
        "host",
        "operation",
        "partition",
        "provider",
        "stage",
        "status",
        "target",
        "task_id",
        "trade_date",
        "worker_id",
    }
)
_SECRET_KEY_MARKERS = (
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "credential",
    "password",
    "secret",
    "token",
)
_SECRET_CONTAINER_KEYS = frozenset({"env", "environ", "environment"})
_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}")


class _Queue(Protocol):
    def claim(self, worker_id: str, now: datetime) -> ClaimedTask | None: ...

    def heartbeat(
        self,
        attempt_id: str,
        worker_id: str,
        progress: TaskProgress,
        now: datetime,
    ) -> None: ...

    def is_cancel_requested(self, attempt_id: str, worker_id: str) -> bool: ...

    def finish(
        self,
        attempt_id: str,
        worker_id: str,
        outcome: TaskOutcome,
    ) -> None: ...


class _LatestProgress:
    def __init__(self, initial: TaskProgress) -> None:
        self._value = initial
        self._lock = threading.Lock()

    def get(self) -> TaskProgress:
        with self._lock:
            return self._value

    def set(self, progress: TaskProgress) -> None:
        with self._lock:
            self._value = progress


class _CoordinationFailure:
    def __init__(self) -> None:
        self._error: Exception | None = None
        self._lock = threading.Lock()
        self._failed = threading.Event()

    def record(self, error: Exception) -> None:
        with self._lock:
            if self._error is None:
                self._error = error
                self._failed.set()

    def get(self) -> Exception | None:
        with self._lock:
            return self._error

    def is_set(self) -> bool:
        return self._failed.is_set()


class _DurableProgressSink:
    def __init__(
        self,
        queue: _Queue,
        task: ClaimedTask,
        latest: _LatestProgress,
        clock: Callable[[], datetime],
        failure: _CoordinationFailure,
    ) -> None:
        self._queue = queue
        self._task = task
        self._latest = latest
        self._clock = clock
        self._failure = failure

    def update(self, progress: TaskProgress) -> None:
        if not isinstance(progress, TaskProgress):
            raise TypeError("progress must be a TaskProgress")
        self._latest.set(progress)
        try:
            self._queue.heartbeat(
                self._task.attempt_id,
                self._task.worker_id,
                progress,
                self._clock(),
            )
        except Exception as error:
            self._failure.record(error)
            raise


class _QueueCancellationToken:
    def __init__(
        self,
        queue: _Queue,
        task: ClaimedTask,
        shutdown: threading.Event,
        failure: _CoordinationFailure,
    ) -> None:
        self._queue = queue
        self._task = task
        self._shutdown = shutdown
        self._failure = failure

    def is_cancelled(self) -> bool:
        if self._shutdown.is_set() or self._failure.is_set():
            return True
        try:
            return self._queue.is_cancel_requested(
                self._task.attempt_id,
                self._task.worker_id,
            )
        except Exception as error:  # noqa: BLE001 - cancellation fails closed
            self._failure.record(error)
            return True


class Worker:
    """Claim and execute at most one durable task at a time."""

    def __init__(
        self,
        queue: _Queue,
        *,
        worker_id: str,
        handlers: Iterable[TaskHandler],
        clock: Callable[[], datetime] | None = None,
        poll_interval: float = 2.0,
        heartbeat_interval: float = 10.0,
        poll_waiter: _Waiter | None = None,
        heartbeat_waiter: _Waiter | None = None,
        heartbeat_join_timeout: float = 5.0,
        logger: logging.Logger | None = None,
    ) -> None:
        self._queue = queue
        self._worker_id = worker_id
        self._handlers = HandlerRegistry(handlers)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._poll_interval = poll_interval
        self._heartbeat_interval = heartbeat_interval
        self._poll_waiter = poll_waiter or _wait
        self._heartbeat_waiter = heartbeat_waiter or _wait
        self._heartbeat_join_timeout = heartbeat_join_timeout
        self._logger = logger or logging.getLogger(__name__)
        self._shutdown = threading.Event()

    def request_shutdown(self) -> None:
        """Stop polling and ask active cooperative work to cancel."""
        self._shutdown.set()

    def run_forever(self) -> None:
        """Poll until shutdown is requested."""
        while not self._shutdown.is_set():
            processed = self.run_once()
            if (
                not processed
                and not self._shutdown.is_set()
                and self._poll_waiter(self._shutdown, self._poll_interval)
            ):
                return

    def run_once(self) -> bool:
        """Claim and finish at most one task, returning whether one was claimed."""
        if self._shutdown.is_set():
            return False
        task = self._queue.claim(self._worker_id, self._clock())
        if task is None:
            return False

        handler = self._handlers.get(task.task_type)
        if handler is None:
            self._queue.finish(
                task.attempt_id,
                self._worker_id,
                TaskOutcome(
                    status=TaskStatus.FAILED,
                    error={
                        "code": "WORKER_UNKNOWN_TASK_TYPE",
                        "retryable": False,
                    },
                ),
            )
            return True

        latest = _LatestProgress(task.progress)
        failure = _CoordinationFailure()
        progress = _DurableProgressSink(
            self._queue,
            task,
            latest,
            self._clock,
            failure,
        )
        cancellation = _QueueCancellationToken(
            self._queue,
            task,
            self._shutdown,
            failure,
        )
        stop_heartbeat = threading.Event()
        heartbeat = threading.Thread(
            target=self._heartbeat,
            args=(task, latest, stop_heartbeat, failure),
            name=f"quant-worker-heartbeat-{task.attempt_id}",
            daemon=True,
        )
        heartbeat.start()
        outcome: TaskOutcome | None = None
        handler_error: Exception | None = None
        try:
            outcome = handler.run(task, progress, cancellation)
            if not isinstance(outcome, TaskOutcome):
                raise TypeError("handler must return a TaskOutcome")
        except Exception as error:  # noqa: BLE001 - sanitize handler boundary
            handler_error = error
        finally:
            stop_heartbeat.set()
            heartbeat.join(timeout=self._heartbeat_join_timeout)
        if heartbeat.is_alive():
            raise RuntimeError("heartbeat thread did not stop before deadline")
        coordination_error = failure.get()
        if coordination_error is not None:
            raise coordination_error
        if handler_error is not None:
            self._log_handler_failure(task, handler_error)
            outcome = _failure_outcome(handler_error)
        if outcome is None:
            raise AssertionError("handler completed without outcome or error")
        outcome = _normalize_outcome(outcome)
        self._queue.finish(task.attempt_id, self._worker_id, outcome)
        return True

    def _heartbeat(
        self,
        task: ClaimedTask,
        latest: _LatestProgress,
        stop: threading.Event,
        failure: _CoordinationFailure,
    ) -> None:
        try:
            while not self._heartbeat_waiter(stop, self._heartbeat_interval):
                self._queue.heartbeat(
                    task.attempt_id,
                    self._worker_id,
                    latest.get(),
                    self._clock(),
                )
        except Exception as error:  # noqa: BLE001 - transfer from heartbeat thread
            failure.record(error)

    def _log_handler_failure(
        self, task: ClaimedTask, error: Exception
    ) -> None:
        self._logger.error(
            "task handler failed",
            extra={
                "attempt_id": task.attempt_id,
                "error_code": _failure_code(error),
                "exception_type": _exception_type(error),
                "frames": _safe_frames(error),
                "task_id": task.id,
                "worker_id": self._worker_id,
            },
        )


def _wait(event: threading.Event, timeout: float) -> bool:
    return event.wait(timeout)


def _failure_outcome(error: Exception) -> TaskOutcome:
    if isinstance(error, QuantError):
        persisted = _quant_error(error.detail)
    else:
        persisted = {
            "code": "WORKER_UNHANDLED_ERROR",
            "retryable": False,
        }
    return TaskOutcome(status=TaskStatus.FAILED, error=persisted)


def _normalize_outcome(outcome: TaskOutcome) -> TaskOutcome:
    if outcome.status is not TaskStatus.FAILED:
        return outcome
    if outcome.error is None:
        raise AssertionError("validated FAILED outcome is missing error")
    return TaskOutcome(
        status=TaskStatus.FAILED,
        error=_normalized_error(outcome.error),
    )


def _quant_error(detail: ErrorDetail) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {
        "code": _safe_code(detail.code),
        "retryable": detail.retryable,
    }
    context = _safe_mapping(detail.context)
    if context:
        result["context"] = context
    return result


def _normalized_error(error: dict[str, JsonValue]) -> dict[str, JsonValue]:
    raw_code = error.get("code")
    result: dict[str, JsonValue] = {
        "code": _safe_code(raw_code),
        "retryable": error.get("retryable")
        if isinstance(error.get("retryable"), bool)
        else False,
    }
    raw_context = error.get("context")
    if isinstance(raw_context, dict):
        context = _safe_mapping(cast(dict[str, object], raw_context))
        if context:
            result["context"] = context
    return result


def _safe_code(value: object) -> str:
    if isinstance(value, str) and _ERROR_CODE.fullmatch(value) is not None:
        return value
    return "WORKER_INVALID_ERROR_CODE"


def _safe_mapping(value: Mapping[str, object]) -> dict[str, JsonValue]:
    pairs = [(key, item) for key, item in value.items() if isinstance(key, str)]
    return _safe_mapping_pairs(pairs, depth=0)


def _safe_mapping_pairs(
    pairs: list[tuple[str, object]], *, depth: int
) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for index, (key, item) in enumerate(sorted(pairs, key=lambda pair: pair[0])):
        if index >= 50:
            break
        name = key[:128]
        normalized = name.casefold().replace("-", "_")
        if normalized in _SECRET_CONTAINER_KEYS or any(
            marker in normalized for marker in _SECRET_KEY_MARKERS
        ):
            result[name] = "[REDACTED]"
        elif normalized in _SAFE_CONTEXT_FIELDS:
            result[name] = _safe_value(item, depth=depth)
    return result


def _safe_value(value: object, *, depth: int) -> JsonValue:
    if depth >= 4:
        return "[TRUNCATED]"
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        return value[:2048]
    if isinstance(value, float):
        return value if math.isfinite(value) else "[NONFINITE]"
    if isinstance(value, Mapping):
        pairs = [
            (key, item) for key, item in value.items() if isinstance(key, str)
        ]
        return _safe_mapping_pairs(pairs, depth=depth + 1)
    if isinstance(value, (list, tuple)):
        return [_safe_value(item, depth=depth + 1) for item in value[:50]]
    return f"[UNSERIALIZABLE:{type(value).__name__}]"


def _failure_code(error: Exception) -> str:
    if isinstance(error, QuantError):
        return _safe_code(error.detail.code)
    return "WORKER_UNHANDLED_ERROR"


def _exception_type(error: Exception) -> str:
    kind = type(error)
    return f"{kind.__module__}.{kind.__qualname__}"[:256]


def _safe_frames(error: Exception) -> list[dict[str, JsonValue]]:
    frames = traceback.extract_tb(error.__traceback__)[-16:]
    return [
        {
            "file": Path(frame.filename).name[:256],
            "function": frame.name[:256],
            "line": frame.lineno,
        }
        for frame in frames
    ]
