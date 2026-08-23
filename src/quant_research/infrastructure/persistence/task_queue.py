"""实现仅使用通用主体关联的 SQLite 持久化任务队列。"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Never, TypeVar, cast
from uuid import uuid4

from sqlalchemy import Engine, and_, delete, func, insert, select, update
from sqlalchemy.engine import Connection, RowMapping
from sqlalchemy.exc import OperationalError

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail
from quant_research.infrastructure.persistence.orm import (
    AuditEventORM,
    TaskAttemptORM,
    TaskORM,
)
from quant_research.tasks.errors import (
    TaskQueueBusy,
    TaskQueueConflict,
    TaskQueueNotFound,
)
from quant_research.tasks.models import (
    ClaimedTask,
    TaskAttemptRecord,
    TaskOutcome,
    TaskProgress,
    TaskRecord,
    TaskStatus,
)

MAX_PAYLOAD_BYTES = 1_048_576
MAX_ERROR_BYTES = 65_536
DEFAULT_PROGRESS = TaskProgress(stage="queued", completed=0, total=0, message="")
_ACTIVE = ("QUEUED", "RUNNING", "CANCEL_REQUESTED")
_TERMINAL = ("SUCCEEDED", "FAILED", "CANCELLED", "ORPHANED")
_RETRYABLE = ("FAILED", "CANCELLED", "ORPHANED")
_T = TypeVar("_T")


class TaskQueue:
    """以短事务提供入队、认领、心跳、取消、重试和终态提交。

    入参：
        engine：SQLite 引擎；clock、sleeper：可注入时间依赖；
        lock_retry_delays：锁冲突退避；task_log_root：可信日志根目录。
    返回值：
        创建支持多 Worker 竞争和所有权围栏的任务队列。
    异常：
        ValueError：退避参数非法时抛出。
    """

    def __init__(
        self,
        engine: Engine,
        *,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] | None = None,
        lock_retry_delays: tuple[float, ...] = (0.01, 0.02, 0.04),
        task_log_root: Path | None = None,
    ) -> None:
        if len(lock_retry_delays) > 3 or any(
            not math.isfinite(value) or value < 0 for value in lock_retry_delays
        ):
            raise ValueError("lock retry delays are invalid")
        self._engine = engine
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleeper = sleeper or time.sleep
        self._delays = lock_retry_delays
        self._log_root = task_log_root.resolve() if task_log_root is not None else None

    def enqueue(
        self,
        task_type: str,
        payload: Mapping[str, JsonValue],
        priority: int = 0,
        *,
        idempotency_key: str | None = None,
        available_at: datetime | None = None,
        actor: str = "system",
        request_id: str | None = None,
        subject_kind: str | None = None,
        subject_id: str | None = None,
    ) -> str:
        """创建通用主体任务，或返回同主体上的活动幂等任务。

        入参：
            task_type、payload、priority：任务类型、冻结载荷和优先级；其余参数
            定义幂等键、可运行时间、审计操作者和通用主体。
        返回值：
            返回新任务标识；命中相同活动幂等任务时返回已有标识。
        异常：
            ValueError、TypeError：身份、主体、优先级或载荷非法时抛出；
            TaskQueueConflict：幂等键对应不同载荷时抛出。
        """
        task_type = self._identity(task_type, "task_type", 64)
        actor = self._identity(actor, "actor", 128)
        key = self._optional(idempotency_key, "idempotency_key", 128)
        subject_kind = self._optional(subject_kind, "subject_kind", 32)
        subject_id = self._optional(subject_id, "subject_id", 64)
        if (subject_kind is None) != (subject_id is None):
            raise ValueError("subject_kind and subject_id must be jointly present")
        if type(priority) is not int:
            raise TypeError("priority must be an integer")
        payload_json = self._json(payload, MAX_PAYLOAD_BYTES, "payload")
        now = self._utc(self._clock())
        available = self._utc(available_at) if available_at is not None else now

        def operation(connection: Connection) -> str:
            if key is not None:
                row = (
                    connection.execute(
                        select(TaskORM.__table__).where(
                            TaskORM.task_type == task_type,
                            func.coalesce(TaskORM.subject_kind, "")
                            == (subject_kind or ""),
                            func.coalesce(TaskORM.subject_id, "") == (subject_id or ""),
                            TaskORM.idempotency_key == key,
                            TaskORM.status.in_(_ACTIVE),
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is not None:
                    if row["payload_json"] != payload_json:
                        self._conflict(
                            "TASK_IDEMPOTENCY_CONFLICT",
                            "idempotency key has different payload",
                        )
                    existing_id = cast(str, row["id"])
                    self._audit(
                        connection,
                        existing_id,
                        "TASK_ENQUEUE_DEDUPLICATED",
                        actor,
                        {"request_id": request_id},
                        now,
                    )
                    return existing_id
            task_id = str(uuid4())
            stamp = self._stamp(now)
            connection.execute(
                insert(TaskORM).values(
                    id=task_id,
                    subject_kind=subject_kind,
                    subject_id=subject_id,
                    task_type=task_type,
                    payload_json=payload_json,
                    status=TaskStatus.QUEUED.value,
                    priority=priority,
                    progress_json=self._progress(DEFAULT_PROGRESS),
                    created_at=stamp,
                    available_at=self._stamp(available),
                    updated_at=stamp,
                    heartbeat_at=None,
                    completed_at=None,
                    idempotency_key=key,
                    worker_id=None,
                    locked_at=None,
                    error_json=None,
                    result_json=None,
                )
            )
            self._audit(
                connection,
                task_id,
                "TASK_ENQUEUED",
                actor,
                {
                    "request_id": request_id,
                    "subject_kind": subject_kind,
                    "subject_id": subject_id,
                },
                now,
            )
            return task_id

        return self._write(operation)

    def get(self, task_id: str) -> TaskRecord:
        """读取一个任务快照。

        入参：
            task_id：任务标识。
        返回值：
            返回当前任务状态、关联主体、进度和结果的不可变快照。
        异常：
            TaskQueueNotFound：任务不存在时抛出。
        """
        with self._engine.connect() as connection:
            row = (
                connection.execute(
                    select(TaskORM.__table__).where(TaskORM.id == task_id)
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            self._not_found(task_id)
        return self._record(cast(RowMapping, row))

    def list(
        self,
        *,
        status: TaskStatus | None = None,
        task_type: str | None = None,
        subject_kind: str | None = None,
        subject_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[TaskRecord, ...]:
        """按稳定倒序分页读取任务。

        入参：
            status、task_type、subject_kind、subject_id：可选过滤条件；
            limit、offset：分页大小与偏移。
        返回值：
            返回按创建时间和任务标识倒序排列的任务元组。
        异常：
            数据库读取失败时传播 SQLAlchemy 异常。
        """
        statement = select(TaskORM.__table__)
        conditions = []
        if status is not None:
            conditions.append(TaskORM.status == status.value)
        if task_type is not None:
            conditions.append(TaskORM.task_type == task_type)
        if subject_kind is not None:
            conditions.append(TaskORM.subject_kind == subject_kind)
        if subject_id is not None:
            conditions.append(TaskORM.subject_id == subject_id)
        if conditions:
            statement = statement.where(and_(*conditions))
        statement = (
            statement.order_by(TaskORM.created_at.desc(), TaskORM.id.desc())
            .limit(limit)
            .offset(offset)
        )
        with self._engine.connect() as connection:
            rows = connection.execute(statement).mappings().all()
        return tuple(self._record(row) for row in rows)

    def list_attempts(
        self, task_id: str, *, limit: int = 100
    ) -> tuple[TaskAttemptRecord, ...]:
        """读取任务的执行尝试。

        入参：
            task_id：任务标识；limit：最多返回的尝试数。
        返回值：
            返回按尝试序号升序排列的执行尝试元组。
        异常：
            数据库读取失败时传播 SQLAlchemy 异常。
        """
        with self._engine.connect() as connection:
            rows = (
                connection.execute(
                    select(TaskAttemptORM.__table__)
                    .where(TaskAttemptORM.task_id == task_id)
                    .order_by(TaskAttemptORM.attempt_no)
                    .limit(limit)
                )
                .mappings()
                .all()
            )
        return tuple(self._attempt(row) for row in rows)

    def claim(self, worker_id: str, now: datetime) -> ClaimedTask | None:
        """原子认领当前最高优先级可运行任务。

        入参：
            worker_id：Worker 身份；now：本次认领的 UTC 时间基准。
        返回值：
            成功时返回含唯一 attempt_id 的任务；没有可运行任务时返回 None。
        异常：
            ValueError：Worker 身份或时间非法时抛出；TaskQueueBusy：SQLite
            写锁在有限退避后仍不可用时抛出。
        """
        worker_id = self._identity(worker_id, "worker_id", 128)
        instant = self._utc(now)

        def operation(connection: Connection) -> ClaimedTask | None:
            task = (
                connection.execute(
                    select(TaskORM.__table__)
                    .where(
                        TaskORM.status == TaskStatus.QUEUED.value,
                        TaskORM.available_at <= self._stamp(instant),
                    )
                    .order_by(TaskORM.priority.desc(), TaskORM.created_at, TaskORM.id)
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )
            if task is None:
                return None
            changed = connection.execute(
                update(TaskORM)
                .where(
                    TaskORM.id == task["id"], TaskORM.status == TaskStatus.QUEUED.value
                )
                .values(
                    status=TaskStatus.RUNNING.value,
                    worker_id=worker_id,
                    locked_at=self._stamp(instant),
                    heartbeat_at=self._stamp(instant),
                    updated_at=self._stamp(instant),
                )
            ).rowcount
            if changed != 1:
                return None
            attempt_no = (
                cast(
                    int,
                    connection.scalar(
                        select(func.count())
                        .select_from(TaskAttemptORM)
                        .where(TaskAttemptORM.task_id == task["id"])
                    ),
                )
                + 1
            )
            attempt_id = str(uuid4())
            connection.execute(
                insert(TaskAttemptORM).values(
                    id=attempt_id,
                    task_id=task["id"],
                    attempt_no=attempt_no,
                    status=TaskStatus.RUNNING.value,
                    worker_id=worker_id,
                    started_at=self._stamp(instant),
                    heartbeat_at=self._stamp(instant),
                    completed_at=None,
                    log_path=None,
                    progress_json=task["progress_json"],
                    error_json=None,
                    result_json=None,
                )
            )
            self._audit(
                connection,
                cast(str, task["id"]),
                "TASK_CLAIMED",
                worker_id,
                {"attempt_id": attempt_id, "attempt_no": attempt_no},
                instant,
            )
            return ClaimedTask(
                id=cast(str, task["id"]),
                attempt_id=attempt_id,
                attempt_no=attempt_no,
                task_type=cast(str, task["task_type"]),
                payload=self._object(cast(str, task["payload_json"])),
                priority=cast(int, task["priority"]),
                worker_id=worker_id,
                progress=TaskProgress.model_validate_json(
                    cast(str, task["progress_json"]), strict=True
                ),
                claimed_at=instant,
                subject_kind=cast(str | None, task["subject_kind"]),
                subject_id=cast(str | None, task["subject_id"]),
            )

        return self._write(operation)

    def heartbeat(
        self, attempt_id: str, worker_id: str, progress: TaskProgress, now: datetime
    ) -> None:
        """续租当前 Worker 所有权并提交最新进度。

        入参：
            attempt_id、worker_id：尝试与所有者身份；progress：最新进度；
            now：心跳时间。
        返回值：
            任务与尝试的心跳和进度同时写入后返回 None。
        异常：
            TaskQueueConflict：所有权丢失或任务不再活动时抛出。
        """
        instant = self._utc(now)
        progress_json = self._progress(progress)

        def operation(connection: Connection) -> None:
            attempt = self._owned(connection, attempt_id, worker_id)
            task_id = cast(str, attempt["task_id"])
            changed = connection.execute(
                update(TaskORM)
                .where(
                    TaskORM.id == task_id,
                    TaskORM.worker_id == worker_id,
                    TaskORM.status.in_(("RUNNING", "CANCEL_REQUESTED")),
                )
                .values(
                    progress_json=progress_json,
                    heartbeat_at=self._stamp(instant),
                    updated_at=self._stamp(instant),
                )
            ).rowcount
            if changed != 1:
                self._conflict("TASK_OWNERSHIP_LOST", "task ownership was lost")
            connection.execute(
                update(TaskAttemptORM)
                .where(TaskAttemptORM.id == attempt_id)
                .values(heartbeat_at=self._stamp(instant), progress_json=progress_json)
            )

        self._write(operation)

    def is_cancel_requested(self, attempt_id: str, worker_id: str) -> bool:
        """查询当前尝试是否已收到协作取消请求。

        入参：
            attempt_id、worker_id：尝试标识和预期所有者。
        返回值：
            任务处于 CANCEL_REQUESTED 时返回 True，否则返回 False。
        异常：
            TaskQueueConflict：尝试不存在、所有权不符或任务与尝试状态分叉时抛出。
        """
        with self._engine.connect() as connection:
            row = (
                connection.execute(
                    select(
                        TaskAttemptORM.worker_id.label("attempt_worker_id"),
                        TaskAttemptORM.status.label("attempt_status"),
                        TaskORM.worker_id.label("task_worker_id"),
                        TaskORM.status.label("task_status"),
                    )
                    .select_from(TaskAttemptORM)
                    .join(TaskORM, TaskORM.id == TaskAttemptORM.task_id)
                    .where(TaskAttemptORM.id == attempt_id)
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            self._conflict("TASK_OWNERSHIP_LOST", "attempt ownership was lost")
        snapshot = row
        if (
            snapshot["attempt_worker_id"] != worker_id
            or snapshot["task_worker_id"] != worker_id
        ):
            self._conflict(
                "TASK_OWNERSHIP_CONFLICT", "task attempt is owned by another worker"
            )
        if (
            snapshot["attempt_status"] not in ("RUNNING", "CANCEL_REQUESTED")
            or snapshot["task_status"] not in ("RUNNING", "CANCEL_REQUESTED")
            or snapshot["attempt_status"] != snapshot["task_status"]
        ):
            self._conflict(
                "TASK_STATE_CONFLICT", "task and attempt are not one active pair"
            )
        return cast(str, snapshot["task_status"]) == TaskStatus.CANCEL_REQUESTED.value

    def request_cancel(
        self,
        task_id: str,
        actor: str = "system",
        *,
        request_id: str | None = None,
        strict: bool = False,
    ) -> TaskStatus:
        """取消排队任务或向运行任务发出协作取消请求。

        入参：
            task_id：任务标识；actor、request_id：审计信息；strict：是否将重复
            取消视为冲突。
        返回值：
            返回 CANCELLED 或 CANCEL_REQUESTED 的最新状态。
        异常：
            TypeError：strict 不是布尔值时抛出；TaskQueueNotFound 或
            TaskQueueConflict：任务不存在或当前状态不可取消时抛出。
        """
        if type(strict) is not bool:
            raise TypeError("strict must be a bool")
        now = self._utc(self._clock())

        def operation(connection: Connection) -> TaskStatus:
            task = self._task(connection, task_id)
            status = cast(str, task["status"])
            if status == "QUEUED":
                target = TaskStatus.CANCELLED
                completed = self._stamp(now)
            elif status == "RUNNING":
                target = TaskStatus.CANCEL_REQUESTED
                completed = None
                attempts = list(
                    connection.execute(
                        select(TaskAttemptORM.__table__).where(
                            TaskAttemptORM.task_id == task_id,
                            TaskAttemptORM.status == TaskStatus.RUNNING.value,
                        )
                    ).mappings()
                )
                if len(attempts) != 1:
                    self._conflict(
                        "TASK_STATE_CONFLICT", "running task has no unique attempt"
                    )
                connection.execute(
                    update(TaskAttemptORM)
                    .where(TaskAttemptORM.id == attempts[0]["id"])
                    .values(status=TaskStatus.CANCEL_REQUESTED.value)
                )
            elif status in ("CANCEL_REQUESTED", "CANCELLED"):
                if strict:
                    self._conflict(
                        "TASK_STATE_CONFLICT", f"cannot cancel task in {status}"
                    )
                return TaskStatus(status)
            else:
                self._conflict("TASK_STATE_CONFLICT", f"cannot cancel task in {status}")
            connection.execute(
                update(TaskORM)
                .where(TaskORM.id == task_id)
                .values(
                    status=target.value,
                    updated_at=self._stamp(now),
                    completed_at=completed,
                )
            )
            self._audit(
                connection,
                task_id,
                "TASK_CANCELLED"
                if target is TaskStatus.CANCELLED
                else "TASK_CANCEL_REQUESTED",
                actor,
                {"request_id": request_id},
                now,
            )
            return target

        return self._write(operation)

    def finish(self, attempt_id: str, worker_id: str, outcome: TaskOutcome) -> None:
        """由持有所有权的 Worker 原子提交任务终态。

        入参：
            attempt_id、worker_id：尝试与所有者身份；outcome：成功、失败或取消结果。
        返回值：
            任务和尝试一致进入终态后返回 None；相同终态重放也返回 None。
        异常：
            TaskQueueConflict：所有权、活动状态或终态重放内容不一致时抛出。
        """
        now = self._utc(self._clock())
        error_json = (
            self._json(outcome.error, MAX_ERROR_BYTES, "error")
            if outcome.error is not None
            else None
        )
        result_json = (
            self._json(outcome.result, MAX_ERROR_BYTES, "result")
            if outcome.result is not None
            else None
        )

        def operation(connection: Connection) -> None:
            raw_attempt = (
                connection.execute(
                    select(TaskAttemptORM.__table__).where(
                        TaskAttemptORM.id == attempt_id
                    )
                )
                .mappings()
                .one_or_none()
            )
            if raw_attempt is None:
                self._conflict("TASK_ATTEMPT_NOT_FOUND", "task attempt does not exist")
            attempt = raw_attempt
            task_id = cast(str, attempt["task_id"])
            task = self._task(connection, task_id)
            if attempt["worker_id"] != worker_id or task["worker_id"] != worker_id:
                self._conflict(
                    "TASK_OWNERSHIP_CONFLICT",
                    "task attempt is owned by another worker",
                )
            if attempt["status"] in _TERMINAL:
                if (
                    attempt["status"] == task["status"] == outcome.status.value
                    and attempt["error_json"] == task["error_json"] == error_json
                    and attempt["result_json"] == task["result_json"] == result_json
                ):
                    return
                self._conflict(
                    "TASK_STATE_CONFLICT", "terminal finish replay changed outcome"
                )
            if (
                attempt["status"] not in ("RUNNING", "CANCEL_REQUESTED")
                or task["status"] not in ("RUNNING", "CANCEL_REQUESTED")
                or attempt["status"] != task["status"]
            ):
                self._conflict(
                    "TASK_STATE_CONFLICT", "task and attempt are not one active pair"
                )
            if task["status"] not in ("RUNNING", "CANCEL_REQUESTED"):
                self._conflict("TASK_STATE_CONFLICT", "task is not active")
            if (
                task["status"] == TaskStatus.CANCEL_REQUESTED.value
                and outcome.status is not TaskStatus.CANCELLED
            ):
                self._conflict(
                    "TASK_STATE_CONFLICT",
                    "cancel-requested task must finish as cancelled",
                )
            status = outcome.status
            result = result_json
            stamp = self._stamp(now)
            connection.execute(
                update(TaskORM)
                .where(TaskORM.id == task_id)
                .values(
                    status=status.value,
                    heartbeat_at=stamp,
                    completed_at=stamp,
                    updated_at=stamp,
                    error_json=error_json,
                    result_json=result,
                )
            )
            connection.execute(
                update(TaskAttemptORM)
                .where(TaskAttemptORM.id == attempt_id)
                .values(
                    status=status.value,
                    heartbeat_at=stamp,
                    completed_at=stamp,
                    error_json=error_json,
                    result_json=result,
                )
            )
            self._audit(
                connection,
                task_id,
                "TASK_FINISHED",
                worker_id,
                {"status": status.value, "error": outcome.error},
                now,
            )

        self._write(operation)

    def bind_log_path(
        self, attempt_id: str, worker_id: str, expected_path: str
    ) -> str:
        """为当前尝试登记可信任务日志路径。

        入参：
            attempt_id、worker_id：活动尝试与预期所有者；expected_path：日志管理器
            从同一可信根推导出的绝对路径。
        返回值：
            返回位于配置日志根内的绝对日志文件路径。
        异常：
            TaskQueueConflict：日志根未配置、所有权不符或路径已绑定为其他值时抛出；
            ValueError：解析后的路径逃逸可信根时抛出。
        """
        root = self._log_root
        if root is None:
            self._conflict(
                "TASK_LOG_ROOT_UNCONFIGURED", "task_log_root is not configured"
            )
        with self._engine.connect() as connection:
            attempt = self._owned(connection, attempt_id, worker_id)
            task_id = cast(str, attempt["task_id"])
        target = (
            root / f"task_id={task_id}" / f"attempt_id={attempt_id}" / "run.log"
        ).resolve()
        if root not in target.parents:
            raise ValueError("task log path escaped trusted root")
        if not isinstance(expected_path, str) or Path(expected_path).resolve() != target:
            self._conflict(
                "TASK_LOG_PATH_MISMATCH",
                "task log manager and queue roots do not match",
            )

        def operation(connection: Connection) -> str:
            attempt = self._owned(connection, attempt_id, worker_id)
            persisted = cast(str | None, attempt["log_path"])
            if persisted == str(target):
                return str(target)
            if persisted is not None:
                self._conflict(
                    "TASK_LOG_PATH_CONFLICT",
                    "task attempt already has a different log path",
                )
            connection.execute(
                update(TaskAttemptORM)
                .where(TaskAttemptORM.id == attempt_id)
                .values(log_path=str(target))
            )
            self._audit(
                connection,
                task_id,
                "TASK_LOG_BOUND",
                worker_id,
                {"attempt_id": attempt_id},
                self._utc(self._clock()),
            )
            return str(target)

        return self._write(operation)

    def retry(
        self,
        task_id: str,
        actor: str = "system",
        *,
        available_at: datetime | None = None,
        request_id: str | None = None,
    ) -> str:
        """仅为非 Experiment Run 任务复用同一任务标识重新排队。

        入参：
            task_id：终态任务标识；actor、request_id：审计信息；available_at：
            新一轮可认领时间。
        返回值：
            返回重新排队的原任务标识。
        异常：
            TaskQueueConflict：任务不是可重试终态，或其主体是 Experiment Run 时抛出。
        """
        now = self._utc(self._clock())
        available = self._utc(available_at) if available_at is not None else now

        def operation(connection: Connection) -> str:
            task = self._task(connection, task_id)
            if task["subject_kind"] == "EXPERIMENT_RUN":
                self._conflict(
                    "TASK_RETRY_REQUIRES_NEW_RUN",
                    "experiment retries must create a new Run",
                )
            if task["status"] not in _RETRYABLE:
                self._conflict(
                    "TASK_STATE_CONFLICT", "only terminal tasks can be retried"
                )
            connection.execute(
                update(TaskORM)
                .where(TaskORM.id == task_id)
                .values(
                    status="QUEUED",
                    available_at=self._stamp(available),
                    updated_at=self._stamp(now),
                    completed_at=None,
                    heartbeat_at=None,
                    worker_id=None,
                    locked_at=None,
                    error_json=None,
                    result_json=None,
                    progress_json=self._progress(DEFAULT_PROGRESS),
                )
            )
            self._audit(
                connection,
                task_id,
                "TASK_RETRIED",
                actor,
                {"request_id": request_id},
                now,
            )
            return task_id

        return self._write(operation)

    def delete(
        self,
        task_id: str,
        actor: str = "system",
        *,
        request_id: str | None = None,
    ) -> None:
        """删除终态任务及其尝试；活动任务不能删除。

        入参：
            task_id：终态任务标识；actor、request_id：删除审计信息。
        返回值：
            任务、级联尝试删除完成后返回 None。
        异常：
            TaskQueueNotFound：任务不存在时抛出；TaskQueueConflict：任务仍活动时抛出。
        """
        actor = self._identity(actor, "actor", 128)

        def operation(connection: Connection) -> None:
            task = self._task(connection, task_id)
            if task["status"] not in _TERMINAL:
                self._conflict("TASK_STATE_CONFLICT", "active task cannot be deleted")
            self._audit(
                connection,
                task_id,
                "TASK_DELETED",
                actor,
                {
                    "request_id": request_id,
                    "status": cast(str, task["status"]),
                    "task_id": task_id,
                    "task_type": cast(str, task["task_type"]),
                },
                self._utc(self._clock()),
            )
            connection.execute(delete(TaskORM).where(TaskORM.id == task_id))

        self._write(operation)

    def mark_orphans(self, now: datetime, stale_after: timedelta) -> int:
        """将心跳超时的活动任务和尝试标记为 ORPHANED。

        入参：
            now：扫描时间；stale_after：允许的最大无心跳时长。
        返回值：
            返回本轮实际转为 ORPHANED 的任务数量。
        异常：
            TypeError、ValueError：超时参数类型错误或非正时抛出；
            TaskQueueBusy：并发写锁持续冲突时抛出。
        """
        if not isinstance(stale_after, timedelta):
            raise TypeError("stale_after must be a timedelta")
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        instant = self._utc(now)
        cutoff = self._stamp(instant - stale_after)
        with self._engine.connect() as connection:
            candidate_ids = tuple(
                cast(
                    str,
                    row[0],
                )
                for row in connection.execute(
                    select(TaskORM.id)
                    .join(TaskAttemptORM, TaskAttemptORM.task_id == TaskORM.id)
                    .where(
                        TaskORM.status.in_(("RUNNING", "CANCEL_REQUESTED")),
                        TaskAttemptORM.status.in_(("RUNNING", "CANCEL_REQUESTED")),
                        TaskORM.heartbeat_at < cutoff,
                        TaskAttemptORM.heartbeat_at < cutoff,
                    )
                    .order_by(TaskORM.id)
                )
            )
        error: dict[str, JsonValue] = {
            "code": "TASK_ORPHANED",
            "message": "task heartbeat exceeded the stale threshold",
            "stale_after_seconds": stale_after.total_seconds(),
        }
        error_json = self._json(error, MAX_ERROR_BYTES, "error")
        count = 0
        for candidate_id in candidate_ids:

            def operation(
                connection: Connection, *, task_id: str = candidate_id
            ) -> bool:
                pair = (
                    connection.execute(
                        select(
                            TaskORM.status.label("task_status"),
                            TaskORM.heartbeat_at.label("task_heartbeat"),
                            TaskAttemptORM.id.label("attempt_id"),
                            TaskAttemptORM.status.label("attempt_status"),
                            TaskAttemptORM.heartbeat_at.label("attempt_heartbeat"),
                        )
                        .select_from(TaskORM)
                        .join(TaskAttemptORM, TaskAttemptORM.task_id == TaskORM.id)
                        .where(
                            TaskORM.id == task_id,
                            TaskORM.status.in_(("RUNNING", "CANCEL_REQUESTED")),
                            TaskAttemptORM.status.in_(("RUNNING", "CANCEL_REQUESTED")),
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if pair is None or not (
                    cast(str, pair["task_heartbeat"]) < cutoff
                    and cast(str, pair["attempt_heartbeat"]) < cutoff
                ):
                    return False
                stamp = self._stamp(instant)
                connection.execute(
                    update(TaskORM)
                    .where(TaskORM.id == task_id)
                    .values(
                        status="ORPHANED",
                        completed_at=stamp,
                        updated_at=stamp,
                        error_json=error_json,
                    )
                )
                connection.execute(
                    update(TaskAttemptORM)
                    .where(TaskAttemptORM.id == pair["attempt_id"])
                    .values(
                        status="ORPHANED", completed_at=stamp, error_json=error_json
                    )
                )
                self._audit(
                    connection,
                    task_id,
                    "TASK_ORPHANED",
                    "system",
                    {"error": error},
                    instant,
                )
                return True

            count += int(self._write(operation))
        return count

    def _write(self, operation: Callable[[Connection], _T]) -> _T:
        delays = (*self._delays, None)
        for delay in delays:
            try:
                with self._engine.connect() as connection:
                    connection.exec_driver_sql("BEGIN IMMEDIATE")
                    try:
                        result = operation(connection)
                    except BaseException:
                        connection.rollback()
                        raise
                    connection.commit()
                    return result
            except OperationalError as error:
                if "locked" not in str(error).lower() or delay is None:
                    if delay is None and "locked" in str(error).lower():
                        raise TaskQueueBusy(
                            ErrorDetail(
                                code="TASK_QUEUE_BUSY",
                                severity=Severity.SEVERE,
                                message="task queue is busy",
                                context={},
                                remediation="retry later",
                                retryable=True,
                            )
                        ) from error
                    raise
                self._sleeper(delay)
        raise RuntimeError("unreachable")

    @staticmethod
    def _task(connection: Connection, task_id: str) -> RowMapping:
        row = (
            connection.execute(select(TaskORM.__table__).where(TaskORM.id == task_id))
            .mappings()
            .one_or_none()
        )
        if row is None:
            TaskQueue._not_found(task_id)
        return cast(RowMapping, row)

    @staticmethod
    def _owned(connection: Connection, attempt_id: str, worker_id: str) -> RowMapping:
        row = (
            connection.execute(
                select(TaskAttemptORM.__table__).where(TaskAttemptORM.id == attempt_id)
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            TaskQueue._conflict("TASK_ATTEMPT_NOT_FOUND", "task attempt does not exist")
        attempt = row
        task = TaskQueue._task(connection, cast(str, attempt["task_id"]))
        if attempt["worker_id"] != worker_id or task["worker_id"] != worker_id:
            TaskQueue._conflict(
                "TASK_OWNERSHIP_CONFLICT", "task attempt is owned by another worker"
            )
        if (
            attempt["status"] not in ("RUNNING", "CANCEL_REQUESTED")
            or task["status"] not in ("RUNNING", "CANCEL_REQUESTED")
            or attempt["status"] != task["status"]
        ):
            TaskQueue._conflict(
                "TASK_STATE_CONFLICT", "task and attempt are not one active pair"
            )
        return attempt

    @staticmethod
    def _record(row: RowMapping) -> TaskRecord:
        return TaskRecord(
            id=cast(str, row["id"]),
            task_type=cast(str, row["task_type"]),
            payload=TaskQueue._object(cast(str, row["payload_json"])),
            status=TaskStatus(cast(str, row["status"])),
            priority=cast(int, row["priority"]),
            progress=TaskQueue._object(cast(str, row["progress_json"])),
            created_at=TaskQueue._parse(cast(str, row["created_at"])),
            available_at=TaskQueue._parse(cast(str, row["available_at"])),
            updated_at=TaskQueue._parse(cast(str, row["updated_at"])),
            heartbeat_at=TaskQueue._parse_optional(
                cast(str | None, row["heartbeat_at"])
            ),
            completed_at=TaskQueue._parse_optional(
                cast(str | None, row["completed_at"])
            ),
            idempotency_key=cast(str | None, row["idempotency_key"]),
            worker_id=cast(str | None, row["worker_id"]),
            locked_at=TaskQueue._parse_optional(cast(str | None, row["locked_at"])),
            error=TaskQueue._object_optional(cast(str | None, row["error_json"])),
            result=TaskQueue._object_optional(cast(str | None, row["result_json"])),
            subject_kind=cast(str | None, row["subject_kind"]),
            subject_id=cast(str | None, row["subject_id"]),
        )

    @staticmethod
    def _attempt(row: RowMapping) -> TaskAttemptRecord:
        return TaskAttemptRecord(
            id=cast(str, row["id"]),
            task_id=cast(str, row["task_id"]),
            attempt_no=cast(int, row["attempt_no"]),
            status=TaskStatus(cast(str, row["status"])),
            worker_id=cast(str | None, row["worker_id"]),
            started_at=TaskQueue._parse(cast(str, row["started_at"])),
            heartbeat_at=TaskQueue._parse_optional(
                cast(str | None, row["heartbeat_at"])
            ),
            completed_at=TaskQueue._parse_optional(
                cast(str | None, row["completed_at"])
            ),
            log_path=cast(str | None, row["log_path"]),
            progress=TaskQueue._object(cast(str, row["progress_json"])),
            error=TaskQueue._object_optional(cast(str | None, row["error_json"])),
            result=TaskQueue._object_optional(cast(str | None, row["result_json"])),
        )

    @staticmethod
    def _audit(
        connection: Connection,
        task_id: str,
        event: str,
        actor: str,
        details: Mapping[str, JsonValue],
        now: datetime,
    ) -> None:
        task = TaskQueue._task(connection, task_id)
        connection.execute(
            insert(AuditEventORM).values(
                run_id=task["subject_id"]
                if task["subject_kind"] == "EXPERIMENT_RUN"
                else None,
                subject_kind=task["subject_kind"],
                subject_id=task["subject_id"],
                task_id=task_id,
                event_type=event,
                actor=actor,
                details_json=TaskQueue._json(details, MAX_ERROR_BYTES, "audit"),
                created_at=TaskQueue._stamp(now),
            )
        )

    @staticmethod
    def _json(value: object, limit: int, label: str) -> str:
        encoded = canonical_json_bytes(TaskQueue._plain_json(value))
        if len(encoded) > limit:
            raise ValueError(f"{label} JSON exceeds {limit} bytes")
        return encoded.decode("utf-8")

    @staticmethod
    def _plain_json(value: object) -> JsonValue:
        if isinstance(value, Mapping):
            if any(not isinstance(key, str) for key in value):
                raise TypeError("JSON object keys must be strings")
            return {
                cast(str, key): TaskQueue._plain_json(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple)):
            return [TaskQueue._plain_json(item) for item in value]
        return cast(JsonValue, value)

    @staticmethod
    def _progress(value: TaskProgress) -> str:
        return TaskQueue._json(
            value.model_dump(mode="json"), MAX_ERROR_BYTES, "progress"
        )

    @staticmethod
    def _object(value: str) -> dict[str, JsonValue]:
        result = json.loads(value)
        if not isinstance(result, dict):
            raise TypeError("stored JSON must be an object")
        return cast(dict[str, JsonValue], result)

    @staticmethod
    def _object_optional(value: str | None) -> dict[str, JsonValue] | None:
        return None if value is None else TaskQueue._object(value)

    @staticmethod
    def _identity(value: str, name: str, limit: int) -> str:
        if not isinstance(value, str) or not value or len(value) > limit:
            raise ValueError(f"{name} must be nonempty and at most {limit} characters")
        return value

    @staticmethod
    def _optional(value: str | None, name: str, limit: int) -> str | None:
        return None if value is None else TaskQueue._identity(value, name, limit)

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
        ):
            raise ValueError("timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @staticmethod
    def _stamp(value: datetime) -> str:
        return TaskQueue._utc(value).isoformat()

    @staticmethod
    def _parse(value: str) -> datetime:
        return TaskQueue._utc(datetime.fromisoformat(value))

    @staticmethod
    def _parse_optional(value: str | None) -> datetime | None:
        return None if value is None else TaskQueue._parse(value)

    @staticmethod
    def _not_found(task_id: str) -> None:
        raise TaskQueueNotFound(
            ErrorDetail(
                code="TASK_NOT_FOUND",
                severity=Severity.SEVERE,
                message=f"task does not exist: {task_id}",
                context={"task_id": task_id},
                remediation="refresh the task list",
                retryable=False,
            )
        )

    @staticmethod
    def _conflict(code: str, message: str) -> Never:
        raise TaskQueueConflict(
            ErrorDetail(
                code=code,
                severity=Severity.SEVERE,
                message=message,
                context={},
                remediation="refresh state and retry",
                retryable=False,
            )
        )


__all__ = ["TaskQueue"]
