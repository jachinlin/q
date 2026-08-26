"""提供任务与任务执行器相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import logging
import math
import re
import threading
import traceback
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path
from typing import Protocol, cast

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.logging import (
    LogContext,
    StructuredLogger,
    StructuredLogWriteError,
    TaskLogManager,
)
from quant_research.tasks.errors import TaskQueueConflict
from quant_research.tasks.handlers import HandlerRegistry, TaskHandler
from quant_research.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskProgress,
    TaskStatus,
)

type _Waiter = Callable[[threading.Event, float], bool]
type _OrphanRecovery = Callable[[datetime], int]

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
_SECRET_CONTAINER_KEYS = frozenset({"env", "environ", "environment", "redacted"})
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


@dataclass(frozen=True, slots=True)
class WorkerRunResult:
    """记录一次应用用例操作的结果、业务指标和审计身份。

    入参：
        task_id：目标任务标识，类型为 ``str``。
        experiment_id：目标实验标识，类型为 ``str | None``。
        task_status：最近一次 Worker 处理后任务的持久化状态。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Final durable identity produced by the most recent ``run_once`` call.
    """

    task_id: str
    subject_kind: str | None
    subject_id: str | None
    task_status: TaskStatus


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

    def bind_log_path(
        self,
        attempt_id: str,
        worker_id: str,
        expected_path: str,
    ) -> str: ...

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

    def current(self) -> TaskProgress:
        with self._lock:
            return self._value


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
        logger: StructuredLogger | None,
    ) -> None:
        self._queue = queue
        self._task = task
        self._latest = latest
        self._clock = clock
        self._failure = failure
        self._logger = logger

    def update(self, progress: TaskProgress) -> None:
        if not isinstance(progress, TaskProgress):
            raise TypeError("progress must be a TaskProgress")
        try:
            self._latest.update_and_persist(
                progress,
                self._persist,
            )
            if self._logger is not None:
                self._logger.emit(
                    "INFO",
                    "task.progress",
                    stage=progress.stage,
                    context={
                        "completed": progress.completed,
                        "details": progress.context,
                        "message": progress.message,
                        "total": progress.total,
                    },
                )
        except Exception as error:
            self._failure.record(error)
            raise

    def event(self, event: str, context: Mapping[str, object]) -> None:
        if not isinstance(event, str) or not event:
            raise ValueError("event must be a nonempty string")
        if not isinstance(context, Mapping):
            raise TypeError("context must be a mapping")
        if self._logger is None:
            return
        try:
            self._logger.emit(
                "INFO",
                event,
                stage=self._latest.current().stage,
                context=context,
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
    """认领并执行持久化应用用例任务，同时维护心跳与终态。

    入参：
        queue：持久化任务状态、认领和重试的任务队列。
        worker_id：当前 Worker 实例的稳定所有者标识。
        handlers：按任务类型分派且不得重复登记的处理器集合。
        clock：用于产生可复现 UTC 时间戳的可注入时钟。
        poll_interval：没有可运行任务时两次认领尝试之间的等待秒数。
        heartbeat_interval：活跃任务两次所有权续租之间的秒数。
        poll_waiter：可注入的空闲等待器，用于测试时避免真实休眠。
        heartbeat_waiter：可注入的心跳等待器，用于可控地唤醒续租线程。
        heartbeat_join_timeout：关闭 Worker 时等待心跳线程退出的最长秒数。
        orphan_recovery：按当前时点回收失联任务的持久化回调。
        orphan_recovery_interval：两次失联任务扫描之间的最短秒数。
        logger：接收结构化事件的日志器，类型为 ``logging.Logger | None``。
        task_logs：为任务尝试创建、封存并固化 JSONL 日志的管理器。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError`` 或 ``ValueError``。
    Claim and execute at most one durable task at a time.
    """

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
        orphan_recovery: _OrphanRecovery | None = None,
        orphan_recovery_interval: float = 30.0,
        logger: logging.Logger | None = None,
        task_logs: TaskLogManager | None = None,
    ) -> None:
        self._queue = queue
        self._worker_id = worker_id
        self._handlers = HandlerRegistry(handlers)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._poll_interval = _WorkerSupport._positive_seconds(
            poll_interval, "poll_interval"
        )
        self._heartbeat_interval = _WorkerSupport._positive_seconds(
            heartbeat_interval,
            "heartbeat_interval",
        )
        self._poll_waiter = poll_waiter or _WorkerSupport._wait
        self._heartbeat_waiter = heartbeat_waiter or _WorkerSupport._wait
        self._heartbeat_join_timeout = _WorkerSupport._positive_seconds(
            heartbeat_join_timeout,
            "heartbeat_join_timeout",
        )
        if orphan_recovery is not None and not callable(orphan_recovery):
            raise TypeError("orphan_recovery must be callable or None")
        self._orphan_recovery = orphan_recovery
        self._orphan_recovery_interval = _WorkerSupport._positive_seconds(
            orphan_recovery_interval,
            "orphan_recovery_interval",
        )
        self._last_orphan_recovery_at: datetime | None = None
        self._logger = logger or logging.getLogger(__name__)
        if task_logs is not None and not isinstance(task_logs, TaskLogManager):
            raise TypeError("task_logs must be a TaskLogManager or None")
        self._task_logs = task_logs
        self._shutdown = threading.Event()
        self._execution_lock = threading.Lock()
        self._claim_gate = threading.RLock()
        self._last_result: WorkerRunResult | None = None

    @property
    def last_result(self) -> WorkerRunResult | None:
        """处理应用用例中的``last``结果。

        入参：
            无。
        返回值：
            返回结果（``WorkerRunResult | None``）。
        异常：
            无。
        """
        return self._last_result

    def request_shutdown(self) -> None:
        """请求``shutdown``。

        入参：
            无。
        返回值：
            无。
        异常：
            无。
        Stop polling and ask active cooperative work to cancel.
        """
        with self._claim_gate:
            self._shutdown.set()

    def run_forever(self) -> None:
        """执行完整处理流程。

        入参：
            无。
        返回值：
            无。
        异常：
            无。
        Poll until shutdown is requested.
        """
        while not self._shutdown.is_set():
            processed = self.run_once()
            if (
                not processed
                and not self._shutdown.is_set()
                and self._poll_waiter(self._shutdown, self._poll_interval)
            ):
                return

    def run_once(self) -> bool:
        """执行完整处理流程。

        入参：
            无。
        返回值：
            返回是否执行``once``。
        异常：
            无。
        Claim and finish at most one task, returning whether one was claimed.
        """
        with self._execution_lock:
            return self._run_once_serialized()

    def _run_once_serialized(self) -> bool:
        self._last_result = None
        with self._claim_gate:
            if self._shutdown.is_set():
                return False
            now = self._clock()
            self._recover_orphans(now)
            task = self._queue.claim(self._worker_id, now)
        if task is None:
            return False

        if self._task_logs is None:
            outcome, _final_progress = self._run_claimed(task, logger=None)
        else:
            outcome, _final_progress = self._run_with_task_log(task)
        final_status = self._finish(task, outcome)
        self._last_result = WorkerRunResult(
            task_id=task.id,
            subject_kind=task.subject_kind,
            subject_id=task.subject_id,
            task_status=final_status,
        )
        return True

    def _recover_orphans(self, now: datetime) -> None:
        recovery = self._orphan_recovery
        if recovery is None:
            return
        previous = self._last_orphan_recovery_at
        if previous is not None and now >= previous:
            elapsed = (now - previous).total_seconds()
            if elapsed < self._orphan_recovery_interval:
                return
        recovered = recovery(now)
        if type(recovered) is not int:
            raise TypeError("orphan_recovery must return an integer")
        if recovered < 0:
            raise ValueError("orphan_recovery count must be nonnegative")
        self._last_orphan_recovery_at = now
        if recovered:
            self._logger.warning("recovered %d orphaned task(s)", recovered)

    def _run_with_task_log(self, task: ClaimedTask) -> tuple[TaskOutcome, TaskProgress]:
        assert self._task_logs is not None
        context = LogContext(
            request_id=task.attempt_id,
            experiment_id=(
                task.subject_id if task.subject_kind == "EXPERIMENT_RUN" else None
            ),
            task_id=task.id,
            attempt_id=task.attempt_id,
            worker_id=task.worker_id,
        )
        try:
            session = self._task_logs.open(context)
        except (StructuredLogWriteError, TypeError, ValueError):
            return self._run_claimed(task, logger=None)
        with session:
            try:
                bound_path = self._queue.bind_log_path(
                    task.attempt_id,
                    task.worker_id,
                    str(session.path),
                )
                if Path(bound_path) != session.path:
                    raise ValueError("task log manager and queue roots do not match")
            except (TaskQueueConflict, TypeError, ValueError):
                return self._run_claimed(task, logger=None)
            session.logger.emit(
                "INFO",
                "task.claimed",
                context={
                    "task": {
                        "task_type": task.task_type,
                        "payload": task.payload,
                        "priority": task.priority,
                        "claimed_at": task.claimed_at.isoformat(),
                    },
                    "attempt": {
                        "attempt_no": task.attempt_no,
                        "initial_progress": task.progress.model_dump(mode="json"),
                    },
                },
            )
            outcome, final_progress = self._run_claimed(task, logger=session.logger)
            session.logger.emit(
                "INFO",
                "task.outcome_ready",
                context={
                    "task_type": task.task_type,
                    "outcome": outcome.model_dump(mode="json"),
                    "final_progress": final_progress.model_dump(mode="json"),
                },
                error_code=(
                    cast(str, outcome.error["code"])
                    if outcome.error is not None
                    else None
                ),
                stage=final_progress.stage or None,
            )
            return outcome, final_progress

    def _run_claimed(
        self,
        task: ClaimedTask,
        *,
        logger: StructuredLogger | None,
    ) -> tuple[TaskOutcome, TaskProgress]:

        handler = self._handlers.get(task.task_type)
        if handler is None:
            return (
                TaskOutcome(
                    status=TaskStatus.FAILED,
                    error={
                        "code": "WORKER_UNKNOWN_TASK_TYPE",
                        "retryable": False,
                    },
                ),
                task.progress,
            )

        latest = _LatestProgress(task.progress)
        failure = _CoordinationFailure()
        progress = _DurableProgressSink(
            self._queue,
            task,
            latest,
            self._clock,
            failure,
            logger,
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
            if logger is not None:
                logger.emit(
                    "ERROR",
                    "task.handler_failed",
                    context=_WorkerSupport._failure_log_context(task, handler_error),
                    error_code=_WorkerSupport._failure_code(handler_error),
                    stage=latest.current().stage or None,
                )
            outcome = _WorkerSupport._failure_outcome(handler_error)
        if outcome is None:
            raise AssertionError("handler completed without outcome or error")
        outcome = _WorkerSupport._normalize_outcome(outcome)
        return outcome, latest.current()

    def _finish(self, task: ClaimedTask, outcome: TaskOutcome) -> TaskStatus:
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
                return TaskStatus.CANCELLED
            raise
        return outcome.status

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

    def _log_handler_failure(self, task: ClaimedTask, error: Exception) -> None:
        self._logger.error(
            "task handler failed",
            extra={
                "attempt_id": task.attempt_id,
                "error_code": _WorkerSupport._failure_code(error),
                "exception_type": _WorkerSupport._exception_type(error),
                "frames": _WorkerSupport._safe_frames(error),
                "task_id": task.id,
                "worker_id": self._worker_id,
            },
        )


class _WorkerSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _wait(event: threading.Event, timeout: float) -> bool:
        return event.wait(timeout)

    @staticmethod
    def _positive_seconds(value: float, label: str) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{label} must be a finite positive number")
        return float(value)

    @staticmethod
    def _failure_outcome(error: Exception) -> TaskOutcome:
        try:
            if isinstance(error, QuantError):
                persisted = _WorkerSupport._quant_error(error.detail)
            else:
                persisted = _WorkerSupport._validated_error(
                    {
                        "code": "WORKER_UNHANDLED_ERROR",
                        "retryable": False,
                    }
                )
            return TaskOutcome(status=TaskStatus.FAILED, error=persisted)
        except Exception:  # noqa: BLE001 - normalization must fail to fixed JSON
            return _WorkerSupport._normalization_failed_outcome()

    @staticmethod
    def _failure_log_context(
        task: ClaimedTask,
        error: Exception,
    ) -> dict[str, object]:
        context: dict[str, object] = {
            "task_type": task.task_type,
            "payload": task.payload,
            "exception_type": _WorkerSupport._exception_type(error),
            "exception_message": str(error),
            "traceback": _WorkerSupport._full_traceback(error),
            "frames": _WorkerSupport._safe_frames(error),
            "retryable": False,
            "remediation": "inspect the traceback and task inputs before retrying",
        }
        if isinstance(error, QuantError):
            context.update(
                {
                    "severity": error.detail.severity.value,
                    "retryable": error.detail.retryable,
                    "remediation": error.detail.remediation,
                }
            )
        return context

    @staticmethod
    def _normalize_outcome(outcome: TaskOutcome) -> TaskOutcome:
        if outcome.status is not TaskStatus.FAILED:
            return outcome
        if outcome.error is None:
            raise AssertionError("validated FAILED outcome is missing error")
        try:
            return TaskOutcome(
                status=TaskStatus.FAILED,
                error=_WorkerSupport._normalized_error(outcome.error),
            )
        except Exception:  # noqa: BLE001 - normalization must fail to fixed JSON
            return _WorkerSupport._normalization_failed_outcome()

    @staticmethod
    def _normalization_failed_outcome() -> TaskOutcome:
        return TaskOutcome(
            status=TaskStatus.FAILED,
            error=dict(_NORMALIZATION_FAILED_ERROR),
        )

    @staticmethod
    def _quant_error(detail: ErrorDetail) -> dict[str, JsonValue]:
        budget = _NormalizationBudget()
        budget.visit()
        code = _WorkerSupport._safe_code(detail.code)
        budget.consume_text(code)
        budget.visit()
        result: dict[str, JsonValue] = {
            "code": code,
            "retryable": detail.retryable,
        }
        context = _WorkerSupport._safe_mapping(detail.context, budget=budget, depth=0)
        if context:
            result["context"] = context
        return _WorkerSupport._validated_error(result)

    @staticmethod
    def _normalized_error(error: dict[str, JsonValue]) -> dict[str, JsonValue]:
        budget = _NormalizationBudget()
        budget.visit()
        raw_code = error.get("code")
        code = _WorkerSupport._safe_code(raw_code)
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
            context = _WorkerSupport._safe_mapping(
                cast(dict[str, object], raw_context),
                budget=budget,
                depth=0,
            )
            if context:
                result["context"] = context
        return _WorkerSupport._validated_error(result)

    @staticmethod
    def _safe_code(value: object) -> str:
        if (
            isinstance(value, str)
            and len(value) <= 128
            and _ERROR_CODE.fullmatch(value) is not None
        ):
            return value
        return "WORKER_INVALID_ERROR_CODE"

    @staticmethod
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
                result[normalized] = _WorkerSupport._safe_value(
                    item,
                    budget=budget,
                    depth=depth,
                )
        return result

    @staticmethod
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
            return _WorkerSupport._safe_mapping(value, budget=budget, depth=depth + 1)
        if isinstance(value, (list, tuple)):
            return [
                _WorkerSupport._safe_value(item, budget=budget, depth=depth + 1)
                for item in islice(value, _MAX_CONTEXT_ITEMS)
            ]
        kind = type(value).__name__[:128]
        return f"[UNSERIALIZABLE:{kind}]"

    @staticmethod
    def _validated_error(error: dict[str, JsonValue]) -> dict[str, JsonValue]:
        encoded = canonical_json_bytes(cast(JsonValue, error))
        if len(encoded) > _MAX_NORMALIZED_ERROR_BYTES:
            raise ValueError("normalized error JSON exceeds worker budget")
        return error

    @staticmethod
    def _failure_code(error: Exception) -> str:
        if isinstance(error, QuantError):
            return _WorkerSupport._safe_code(error.detail.code)
        return "WORKER_UNHANDLED_ERROR"

    @staticmethod
    def _exception_type(error: Exception) -> str:
        kind = type(error)
        return f"{kind.__module__}.{kind.__qualname__}"[:256]

    @staticmethod
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

    @staticmethod
    def _full_traceback(error: Exception) -> str:
        """返回任务专属诊断日志使用的完整异常链。

        入参：
            error：处理器边界捕获的异常。
        返回值：
            包含 cause/context 链、文件路径、源码行和异常消息的 traceback 文本。
        异常：
            无。
        """
        return "".join(traceback.format_exception(error))
