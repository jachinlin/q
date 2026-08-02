"""Single-task worker runtime over the durable SQLite queue."""

from __future__ import annotations

import logging
import math
import re
import threading
import traceback
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path
from typing import Protocol, cast

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.errors import ErrorDetail, QuantError
from quant_core.tasks.handlers import HandlerRegistry, TaskHandler
from quant_core.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskProgress,
    TaskStatus,
)
from quant_core.tasks.queue import TaskQueueConflict

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
_SECRET_CONTAINER_KEYS = frozenset(
    {"env", "environ", "environment", "redacted"}
)
_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,127}")
_MAX_CONTEXT_ITEMS = 50
_MAX_CONTEXT_DEPTH = 4
_MAX_NORMALIZATION_NODES = 128
_MAX_NORMALIZATION_INPUT_BYTES = 16_384
_MAX_NORMALIZED_ERROR_BYTES = 16_384
_NORMALIZATION_FAILED_ERROR: dict[str, JsonValue] = {
    "code": "WORKER_ERROR_NORMALIZATION_FAILED",
    "retryable": False,
}


class _NormalizationBudget:
    def __init__(self) -> None:
        self._nodes = 0
        self._input_bytes = 0

    def visit(self) -> None:
        self._nodes += 1
        if self._nodes > _MAX_NORMALIZATION_NODES:
            raise ValueError("error normalization node budget exceeded")

    def consume_text(self, value: str) -> None:
        remaining = _MAX_NORMALIZATION_INPUT_BYTES - self._input_bytes
        if len(value) > remaining:
            raise ValueError("error normalization UTF-8 budget exceeded")
        size = len(value.encode("utf-8"))
        if size > remaining:
            raise ValueError("error normalization UTF-8 budget exceeded")
        self._input_bytes += size


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

    def update_and_persist(
        self,
        progress: TaskProgress,
        persist: Callable[[TaskProgress], None],
    ) -> None:
        with self._lock:
            self._value = progress
            persist(progress)

    def persist_latest(self, persist: Callable[[TaskProgress], None]) -> None:
        with self._lock:
            persist(self._value)


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
        try:
            self._latest.update_and_persist(
                progress,
                self._persist,
            )
        except Exception as error:
            self._failure.record(error)
            raise

    def _persist(self, progress: TaskProgress) -> None:
        self._queue.heartbeat(
            self._task.attempt_id,
            self._task.worker_id,
            progress,
            self._clock(),
        )


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
        self._poll_interval = _positive_seconds(poll_interval, "poll_interval")
        self._heartbeat_interval = _positive_seconds(
            heartbeat_interval,
            "heartbeat_interval",
        )
        self._poll_waiter = poll_waiter or _wait
        self._heartbeat_waiter = heartbeat_waiter or _wait
        self._heartbeat_join_timeout = _positive_seconds(
            heartbeat_join_timeout,
            "heartbeat_join_timeout",
        )
        self._logger = logger or logging.getLogger(__name__)
        self._shutdown = threading.Event()
        self._execution_lock = threading.Lock()
        self._claim_gate = threading.RLock()

    def request_shutdown(self) -> None:
        """Stop polling and ask active cooperative work to cancel."""
        with self._claim_gate:
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
        with self._execution_lock:
            return self._run_once_serialized()

    def _run_once_serialized(self) -> bool:
        with self._claim_gate:
            if self._shutdown.is_set():
                return False
            task = self._queue.claim(self._worker_id, self._clock())
        if task is None:
            return False

        handler = self._handlers.get(task.task_type)
        if handler is None:
            self._finish(
                task,
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
        self._finish(task, outcome)
        return True

    def _finish(self, task: ClaimedTask, outcome: TaskOutcome) -> None:
        try:
            self._queue.finish(task.attempt_id, self._worker_id, outcome)
        except TaskQueueConflict:
            if (
                outcome.status is not TaskStatus.CANCELLED
                and self._queue.is_cancel_requested(
                    task.attempt_id,
                    self._worker_id,
                )
            ):
                self._queue.finish(
                    task.attempt_id,
                    self._worker_id,
                    TaskOutcome(status=TaskStatus.CANCELLED),
                )
                return
            raise

    def _heartbeat(
        self,
        task: ClaimedTask,
        latest: _LatestProgress,
        stop: threading.Event,
        failure: _CoordinationFailure,
    ) -> None:
        try:
            while not self._heartbeat_waiter(stop, self._heartbeat_interval):
                latest.persist_latest(
                    lambda progress: self._queue.heartbeat(
                        task.attempt_id,
                        self._worker_id,
                        progress,
                        self._clock(),
                    )
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


def _positive_seconds(value: float, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{label} must be a finite positive number")
    return float(value)


def _failure_outcome(error: Exception) -> TaskOutcome:
    try:
        if isinstance(error, QuantError):
            persisted = _quant_error(error.detail)
        else:
            persisted = _validated_error(
                {
                    "code": "WORKER_UNHANDLED_ERROR",
                    "retryable": False,
                }
            )
        return TaskOutcome(status=TaskStatus.FAILED, error=persisted)
    except Exception:  # noqa: BLE001 - normalization must fail to fixed JSON
        return _normalization_failed_outcome()


def _normalize_outcome(outcome: TaskOutcome) -> TaskOutcome:
    if outcome.status is not TaskStatus.FAILED:
        return outcome
    if outcome.error is None:
        raise AssertionError("validated FAILED outcome is missing error")
    try:
        return TaskOutcome(
            status=TaskStatus.FAILED,
            error=_normalized_error(outcome.error),
        )
    except Exception:  # noqa: BLE001 - normalization must fail to fixed JSON
        return _normalization_failed_outcome()


def _normalization_failed_outcome() -> TaskOutcome:
    return TaskOutcome(
        status=TaskStatus.FAILED,
        error=dict(_NORMALIZATION_FAILED_ERROR),
    )


def _quant_error(detail: ErrorDetail) -> dict[str, JsonValue]:
    budget = _NormalizationBudget()
    budget.visit()
    code = _safe_code(detail.code)
    budget.consume_text(code)
    budget.visit()
    result: dict[str, JsonValue] = {
        "code": code,
        "retryable": detail.retryable,
    }
    context = _safe_mapping(detail.context, budget=budget, depth=0)
    if context:
        result["context"] = context
    return _validated_error(result)


def _normalized_error(error: dict[str, JsonValue]) -> dict[str, JsonValue]:
    budget = _NormalizationBudget()
    budget.visit()
    raw_code = error.get("code")
    code = _safe_code(raw_code)
    budget.consume_text(code)
    budget.visit()
    result: dict[str, JsonValue] = {
        "code": code,
        "retryable": error.get("retryable")
        if isinstance(error.get("retryable"), bool)
        else False,
    }
    raw_context = error.get("context")
    if isinstance(raw_context, dict):
        context = _safe_mapping(
            cast(dict[str, object], raw_context),
            budget=budget,
            depth=0,
        )
        if context:
            result["context"] = context
    return _validated_error(result)


def _safe_code(value: object) -> str:
    if (
        isinstance(value, str)
        and len(value) <= 128
        and _ERROR_CODE.fullmatch(value) is not None
    ):
        return value
    return "WORKER_INVALID_ERROR_CODE"


def _safe_mapping(
    value: Mapping[str, object],
    *,
    budget: _NormalizationBudget,
    depth: int,
) -> dict[str, JsonValue]:
    budget.visit()
    pairs: list[tuple[str, object]] = []
    for key in islice(value, _MAX_CONTEXT_ITEMS):
        if not isinstance(key, str):
            continue
        budget.consume_text(key)
        pairs.append((key, value[key]))
    result: dict[str, JsonValue] = {}
    for raw_name, item in sorted(pairs, key=lambda pair: pair[0]):
        normalized = raw_name.casefold().replace("-", "_")
        if (
            normalized in _SECRET_CONTAINER_KEYS
            or normalized in _SECRET_KEY_MARKERS
        ):
            result[normalized] = "[REDACTED]"
        elif any(marker in normalized for marker in _SECRET_KEY_MARKERS):
            result["redacted"] = "[REDACTED]"
        elif normalized in _SAFE_CONTEXT_FIELDS:
            result[normalized] = _safe_value(
                item,
                budget=budget,
                depth=depth,
            )
    return result


def _safe_value(
    value: object,
    *,
    budget: _NormalizationBudget,
    depth: int,
) -> JsonValue:
    budget.visit()
    if depth >= _MAX_CONTEXT_DEPTH:
        return "[TRUNCATED]"
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        if abs(value) <= 9_007_199_254_740_991:
            return value
        return "[OUT_OF_RANGE]"
    if isinstance(value, str):
        budget.consume_text(value)
        return "[REDACTED]"
    if isinstance(value, float):
        return value if math.isfinite(value) else "[NONFINITE]"
    if isinstance(value, Mapping):
        return _safe_mapping(value, budget=budget, depth=depth + 1)
    if isinstance(value, (list, tuple)):
        return [
            _safe_value(item, budget=budget, depth=depth + 1)
            for item in islice(value, _MAX_CONTEXT_ITEMS)
        ]
    kind = type(value).__name__[:128]
    return f"[UNSERIALIZABLE:{kind}]"


def _validated_error(error: dict[str, JsonValue]) -> dict[str, JsonValue]:
    encoded = canonical_json_bytes(cast(JsonValue, error))
    if len(encoded) > _MAX_NORMALIZED_ERROR_BYTES:
        raise ValueError("normalized error JSON exceeds worker budget")
    return error


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
