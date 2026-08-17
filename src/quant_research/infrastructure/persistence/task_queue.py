"""提供任务与任务队列相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Never, TypeVar, cast
from uuid import uuid4

from sqlalchemy import (
    Connection,
    Engine,
    RowMapping,
    and_,
    delete,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.exc import OperationalError

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail
from quant_research.experiments.models import (
    ExperimentSpec,
    ExperimentStatus,
    ResearchMark,
)
from quant_research.infrastructure.persistence.orm import (
    AuditEventORM,
    ExperimentORM,
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
MAX_AUDIT_BYTES = 65_536
DEFAULT_PROGRESS = TaskProgress(
    stage="queued",
    completed=0,
    total=0,
    message="",
)
_ACTIVE_STATUSES = (
    TaskStatus.QUEUED.value,
    TaskStatus.RUNNING.value,
    TaskStatus.CANCEL_REQUESTED.value,
)
_EXECUTION_STATUSES = (
    TaskStatus.RUNNING.value,
    TaskStatus.CANCEL_REQUESTED.value,
)
_TERMINAL_STATUSES = (
    TaskStatus.SUCCEEDED.value,
    TaskStatus.FAILED.value,
    TaskStatus.CANCELLED.value,
    TaskStatus.ORPHANED.value,
)
_RETRYABLE_STATUSES = (
    TaskStatus.FAILED.value,
    TaskStatus.CANCELLED.value,
    TaskStatus.ORPHANED.value,
)
_BACKTEST_IDEMPOTENCY_KEY = "experiment-backtest-v1"

_T = TypeVar("_T")


class TaskQueue:
    """在短 SQLite 写事务中持久化任务入队、认领、心跳、取消与终态。

    入参：
        engine：引擎。
        clock：用于产生可复现 UTC 时间戳的可注入时钟。
        sleeper：由组合根注入、用于隔离外部副作用的``sleeper``端口。
        lock_retry_delays：参与本次处理的``lock``重试``delays``；调用方不得依赖未声明的顺序。
        task_log_root：所有派生路径必须位于其中的任务日志可信根目录。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Persist queue state in short explicit SQLite write transactions.
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
        if len(lock_retry_delays) > 3:
            raise ValueError("lock_retry_delays permits at most 3 retries")
        if any(
            isinstance(delay, bool)
            or not isinstance(delay, (int, float))
            or not math.isfinite(delay)
            or delay < 0
            for delay in lock_retry_delays
        ):
            raise ValueError("lock retry delays must be finite non-negative numbers")
        self._engine = engine
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleeper = sleeper or time.sleep
        self._lock_retry_delays = tuple(float(delay) for delay in lock_retry_delays)
        if task_log_root is not None and not isinstance(task_log_root, Path):
            raise TypeError("task_log_root must be a Path or None")
        self._task_log_root = (
            task_log_root.resolve() if task_log_root is not None else None
        )

    def enqueue(
        self,
        task_type: str,
        payload: Mapping[str, JsonValue],
        priority: int,
        experiment_id: str | None = None,
        *,
        idempotency_key: str | None = None,
        available_at: datetime | None = None,
        actor: str = "system",
        request_id: str | None = None,
    ) -> str:
        """幂等入队基础设施。

        入参：
            task_type：任务类型。
            payload：参与本次处理的任务载荷；调用方不得依赖未声明的顺序。
            priority：任务在同一可运行集合中的调度优先级。
            experiment_id：目标实验标识，类型为 ``str | None``。
            idempotency_key：幂等键``key``。
            available_at：该条观测首次可供研究使用的带时区时间戳。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回``enqueue``（``str``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``。
        Create queued work or return its active canonical idempotency match.
        """
        task_kind = _QueueSupport._bounded_identity(task_type, "task_type", 64)
        subject = _QueueSupport._bounded_identity(actor, "actor", 128)
        experiment = _QueueSupport._optional_identity(
            experiment_id, "experiment_id", 36
        )
        key = _QueueSupport._optional_identity(idempotency_key, "idempotency_key", 128)
        request = _QueueSupport._optional_identity(request_id, "request_id", 128)
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise TypeError("priority must be an int and not bool")
        payload_json = _QueueSupport._mapping_json_text(
            payload, "payload", MAX_PAYLOAD_BYTES
        )
        created_at = self._time()
        available = self._time(available_at) if available_at is not None else created_at
        progress_json = _QueueSupport._progress_json(DEFAULT_PROGRESS)

        def write(connection: Connection) -> str:
            if experiment is not None:
                exists = connection.scalar(
                    select(ExperimentORM.id).where(ExperimentORM.id == experiment)
                )
                if exists is None:
                    _QueueSupport._raise_not_found(
                        "TASK_EXPERIMENT_NOT_FOUND",
                        f"experiment does not exist: {experiment}",
                        {"experiment_id": experiment},
                    )
            if key is not None:
                existing = (
                    connection.execute(
                        select(TaskORM.__table__).where(
                            TaskORM.task_type == task_kind,
                            func.coalesce(TaskORM.experiment_id, "")
                            == (experiment or ""),
                            TaskORM.idempotency_key == key,
                            TaskORM.status.in_(_ACTIVE_STATUSES),
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if existing is not None:
                    if existing["payload_json"] != payload_json:
                        _QueueSupport._raise_idempotency_conflict(
                            cast(str, existing["id"]), task_kind, experiment, key
                        )
                    _QueueSupport._add_audit(
                        connection,
                        experiment_id=cast(str | None, existing["experiment_id"]),
                        task_id=cast(str, existing["id"]),
                        event_type="TASK_ENQUEUE_DEDUPLICATED",
                        actor=subject,
                        details={
                            "request_id": request,
                            "idempotency_key": key,
                            "status": cast(str, existing["status"]),
                        },
                        created_at=created_at,
                    )
                    return cast(str, existing["id"])

            identifier = str(uuid4())
            connection.execute(
                insert(TaskORM).values(
                    id=identifier,
                    experiment_id=experiment,
                    task_type=task_kind,
                    payload_json=payload_json,
                    status=TaskStatus.QUEUED.value,
                    priority=priority,
                    progress_json=progress_json,
                    created_at=_QueueSupport._timestamp(created_at),
                    available_at=_QueueSupport._timestamp(available),
                    updated_at=_QueueSupport._timestamp(created_at),
                    heartbeat_at=None,
                    completed_at=None,
                    idempotency_key=key,
                    worker_id=None,
                    locked_at=None,
                    error_json=None,
                    result_json=None,
                )
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=experiment,
                task_id=identifier,
                event_type="TASK_ENQUEUED",
                actor=subject,
                details={
                    "request_id": request,
                    "idempotency_key": key,
                    "status": TaskStatus.QUEUED.value,
                },
                created_at=created_at,
            )
            return identifier

        return self._immediate(write)

    def submit_backtest(
        self,
        experiment_id: str,
        config_hash: str,
        *,
        priority: int = 0,
        actor: str = "notebook",
        request_id: str | None = None,
    ) -> str:
        """提交并登记约定任务。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
            config_hash：确定性序列化后的实验或策略配置身份。
            priority：任务在同一可运行集合中的调度优先级。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回回测（``str``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``。
        Atomically queue one immutable experiment or return its active task.
        """
        experiment = _QueueSupport._bounded_identity(experiment_id, "experiment_id", 36)
        expected_hash = _QueueSupport._sha256(config_hash, "config_hash")
        subject = _QueueSupport._bounded_identity(actor, "actor", 128)
        request = _QueueSupport._optional_identity(request_id, "request_id", 128)
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise TypeError("priority must be an int and not bool")
        payload: dict[str, JsonValue] = {
            "experiment_id": experiment,
            "config_hash": expected_hash,
        }
        payload_json = _QueueSupport._mapping_json_text(
            payload, "payload", MAX_PAYLOAD_BYTES
        )
        submitted_at = self._time()
        timestamp = _QueueSupport._timestamp(submitted_at)
        progress_json = _QueueSupport._progress_json(DEFAULT_PROGRESS)

        def write(connection: Connection) -> str:
            record = (
                connection.execute(
                    select(ExperimentORM.__table__).where(
                        ExperimentORM.id == experiment
                    )
                )
                .mappings()
                .one_or_none()
            )
            if record is None:
                _QueueSupport._raise_not_found(
                    "TASK_EXPERIMENT_NOT_FOUND",
                    f"experiment does not exist: {experiment}",
                    {"experiment_id": experiment},
                )
            if record["config_hash"] != expected_hash:
                _QueueSupport._raise_conflict(
                    "EXPERIMENT_CONFIG_HASH_CONFLICT",
                    "experiment config hash changed before submission",
                    {"experiment_id": experiment},
                )

            existing = (
                connection.execute(
                    select(TaskORM.__table__).where(
                        TaskORM.task_type == "BACKTEST",
                        TaskORM.experiment_id == experiment,
                        TaskORM.idempotency_key == _BACKTEST_IDEMPOTENCY_KEY,
                        TaskORM.status.in_(_ACTIVE_STATUSES),
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if existing["payload_json"] != payload_json or record["status"] not in {
                    "QUEUED",
                    "RUNNING",
                }:
                    _QueueSupport._raise_idempotency_conflict(
                        cast(str, existing["id"]),
                        "BACKTEST",
                        experiment,
                        _BACKTEST_IDEMPOTENCY_KEY,
                    )
                _QueueSupport._add_audit(
                    connection,
                    experiment_id=experiment,
                    task_id=cast(str, existing["id"]),
                    event_type="TASK_ENQUEUE_DEDUPLICATED",
                    actor=subject,
                    details={
                        "request_id": request,
                        "idempotency_key": _BACKTEST_IDEMPOTENCY_KEY,
                        "status": cast(str, existing["status"]),
                    },
                    created_at=submitted_at,
                )
                return cast(str, existing["id"])

            status = cast(str, record["status"])
            if status != "CREATED":
                _QueueSupport._raise_conflict(
                    "EXPERIMENT_SUBMIT_CONFLICT",
                    "experiment cannot be submitted from its current state",
                    {"experiment_id": experiment, "status": status},
                )

            identifier = str(uuid4())
            connection.execute(
                insert(TaskORM).values(
                    id=identifier,
                    experiment_id=experiment,
                    task_type="BACKTEST",
                    payload_json=payload_json,
                    status=TaskStatus.QUEUED.value,
                    priority=priority,
                    progress_json=progress_json,
                    created_at=timestamp,
                    available_at=timestamp,
                    updated_at=timestamp,
                    heartbeat_at=None,
                    completed_at=None,
                    idempotency_key=_BACKTEST_IDEMPOTENCY_KEY,
                    worker_id=None,
                    locked_at=None,
                    error_json=None,
                    result_json=None,
                )
            )
            transitioned = connection.execute(
                update(ExperimentORM)
                .where(
                    ExperimentORM.id == experiment,
                    ExperimentORM.status == "CREATED",
                )
                .values(status="QUEUED", queued_at=timestamp)
            )
            if transitioned.rowcount != 1:
                _QueueSupport._raise_conflict(
                    "EXPERIMENT_SUBMIT_CONFLICT",
                    "experiment changed state during submission",
                    {"experiment_id": experiment, "status": status},
                )
            _QueueSupport._add_audit(
                connection,
                experiment_id=experiment,
                task_id=None,
                event_type="EXPERIMENT_STATE_TRANSITIONED",
                actor=subject,
                details={
                    "subject": subject,
                    "action": "submit",
                    "object": {"type": "experiment", "id": experiment},
                    "old_value": {"status": "CREATED"},
                    "new_value": {
                        "status": "QUEUED",
                        "queued_at": timestamp,
                    },
                    "request_id": request,
                },
                created_at=submitted_at,
            )
            # Keep this audit last so every earlier mutation is covered by the
            # same rollback boundary if durable observability cannot be written.
            _QueueSupport._add_audit(
                connection,
                experiment_id=experiment,
                task_id=identifier,
                event_type="TASK_ENQUEUED",
                actor=subject,
                details={
                    "request_id": request,
                    "idempotency_key": _BACKTEST_IDEMPOTENCY_KEY,
                    "status": TaskStatus.QUEUED.value,
                },
                created_at=submitted_at,
            )
            return identifier

        return self._immediate(write)

    def create_experiment_and_submit(
        self,
        spec: ExperimentSpec,
        *,
        priority: int = 0,
        actor: str = "cli",
        request_id: str | None = None,
    ) -> tuple[str, str]:
        """创建并返回约定对象。

        入参：
            spec：不可变规格。
            priority：任务在同一可运行集合中的调度优先级。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回创建实验并``submit``后的实验并``submit``（``tuple[str, str]``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        Atomically create one immutable experiment and its queued task.
        """
        if not isinstance(spec, ExperimentSpec):
            raise TypeError("spec must be an ExperimentSpec")
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise TypeError("priority must be an int and not bool")
        subject = _QueueSupport._bounded_identity(actor, "actor", 128)
        request = _QueueSupport._optional_identity(request_id, "request_id", 128)
        config_bytes = canonical_json_bytes(spec.config)
        if hashlib.sha256(config_bytes).hexdigest() != spec.config_hash:
            raise ValueError("config_hash must match canonical config")
        submitted_at = self._time()
        submitted_text = _QueueSupport._timestamp(submitted_at)
        created_text = _QueueSupport._timestamp(spec.created_at)
        progress_json = _QueueSupport._progress_json(DEFAULT_PROGRESS)
        experiment_id = str(uuid4())
        task_id = str(uuid4())
        payload: dict[str, JsonValue] = {
            "experiment_id": experiment_id,
            "config_hash": spec.config_hash,
        }
        payload_json = _QueueSupport._mapping_json_text(
            payload, "payload", MAX_PAYLOAD_BYTES
        )

        def write(connection: Connection) -> tuple[str, str]:
            duplicate_count = int(
                connection.scalar(
                    select(func.count())
                    .select_from(ExperimentORM)
                    .where(ExperimentORM.fingerprint == spec.fingerprint)
                )
                or 0
            )
            connection.execute(
                insert(ExperimentORM).values(
                    id=experiment_id,
                    strategy_id=spec.strategy_id,
                    config_json=config_bytes.decode("utf-8"),
                    config_hash=spec.config_hash,
                    data_hash=spec.data_hash,
                    source_tree_hash=spec.source_tree_hash,
                    git_commit_hash=spec.git_commit_hash,
                    lockfile_hash=spec.lockfile_hash,
                    rulebook_hash=spec.rulebook_hash,
                    fingerprint=spec.fingerprint,
                    status=ExperimentStatus.QUEUED.value,
                    research_mark=ResearchMark.UNREVIEWED.value,
                    created_at=created_text,
                    queued_at=submitted_text,
                    started_at=None,
                    completed_at=None,
                )
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=experiment_id,
                task_id=None,
                event_type="EXPERIMENT_CREATED",
                actor=subject,
                details={
                    "duplicate_count": duplicate_count,
                    "fingerprint": spec.fingerprint,
                    "request_id": request,
                    "status": ExperimentStatus.CREATED.value,
                },
                created_at=submitted_at,
            )
            connection.execute(
                insert(TaskORM).values(
                    id=task_id,
                    experiment_id=experiment_id,
                    task_type="BACKTEST",
                    payload_json=payload_json,
                    status=TaskStatus.QUEUED.value,
                    priority=priority,
                    progress_json=progress_json,
                    created_at=submitted_text,
                    available_at=submitted_text,
                    updated_at=submitted_text,
                    heartbeat_at=None,
                    completed_at=None,
                    idempotency_key=_BACKTEST_IDEMPOTENCY_KEY,
                    worker_id=None,
                    locked_at=None,
                    error_json=None,
                    result_json=None,
                )
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=experiment_id,
                task_id=None,
                event_type="EXPERIMENT_STATE_TRANSITIONED",
                actor=subject,
                details={
                    "new_status": ExperimentStatus.QUEUED.value,
                    "old_status": ExperimentStatus.CREATED.value,
                    "request_id": request,
                },
                created_at=submitted_at,
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=experiment_id,
                task_id=task_id,
                event_type="TASK_ENQUEUED",
                actor=subject,
                details={
                    "idempotency_key": _BACKTEST_IDEMPOTENCY_KEY,
                    "request_id": request,
                    "status": TaskStatus.QUEUED.value,
                },
                created_at=submitted_at,
            )
            return experiment_id, task_id

        return self._immediate(write)

    def get(self, task_id: str) -> TaskRecord:
        """读取并返回约定对象。

        入参：
            task_id：目标任务标识，类型为 ``str``。
        返回值：
            返回读取基础设施后的``get``（``TaskRecord``）。
        异常：
            无。
        Read one immutable task record without claiming or mutating it.
        """
        identifier = _QueueSupport._bounded_identity(task_id, "task_id", 36)
        with self._engine.connect() as connection:
            return _QueueSupport._task_record(
                _QueueSupport._task(connection, identifier)
            )

    def list(
        self,
        *,
        status: TaskStatus | None = None,
        task_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[TaskRecord, ...]:
        """列出符合条件的记录。

        入参：
            status：当前记录所处的受控生命周期状态。
            task_type：可选任务类型筛选。
            limit：单次查询允许返回的最大记录数。
            offset：分页查询跳过的记录数。
        返回值：
            返回按确定性顺序列出基础设施后的``list``（``tuple[TaskRecord, ...]``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        Read one bounded stable page without returning ORM instances.
        """
        if status is not None and not isinstance(status, TaskStatus):
            raise TypeError("status must be a TaskStatus or None")
        task_kind = (
            _QueueSupport._bounded_identity(task_type, "task_type", 64)
            if task_type is not None
            else None
        )
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("limit must be an integer from 1 through 500")
        if type(offset) is not int or offset < 0:
            raise ValueError("offset must be a nonnegative integer")
        statement = select(TaskORM.__table__)
        if status is not None:
            statement = statement.where(TaskORM.status == status.value)
        if task_kind is not None:
            statement = statement.where(TaskORM.task_type == task_kind)
        statement = (
            statement.order_by(
                TaskORM.created_at.desc(),
                TaskORM.id.desc(),
            )
            .limit(limit)
            .offset(offset)
        )
        with self._engine.connect() as connection:
            return tuple(
                _QueueSupport._task_record(row)
                for row in connection.execute(statement).mappings()
            )

    def list_for_experiment(
        self,
        experiment_id: str,
        *,
        limit: int = 100,
    ) -> tuple[TaskRecord, ...]:
        """列出符合条件的记录。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
            limit：单次查询允许返回的最大记录数。
        返回值：
            返回按确定性顺序列出``for``实验后的``for``实验（``tuple[TaskRecord, ...]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        Read a bounded newest-first task page for one experiment identity.
        """
        identifier = _QueueSupport._bounded_identity(experiment_id, "experiment_id", 36)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer from 1 through 100")
        statement = (
            select(TaskORM.__table__)
            .where(TaskORM.experiment_id == identifier)
            .order_by(TaskORM.created_at.desc(), TaskORM.id.desc())
            .limit(limit)
        )
        with self._engine.connect() as connection:
            return tuple(
                _QueueSupport._task_record(row)
                for row in connection.execute(statement).mappings()
            )

    def list_attempts(
        self,
        task_id: str,
        *,
        limit: int = 100,
    ) -> tuple[TaskAttemptRecord, ...]:
        """列出符合条件的记录。

        入参：
            task_id：目标任务标识，类型为 ``str``。
            limit：单次查询允许返回的最大记录数。
        返回值：
            返回按确定性顺序列出``attempts``后的``attempts``（``tuple[TaskAttemptRecord, ...]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        Read the latest bounded attempt history without exposing ORM rows.
        """
        identifier = _QueueSupport._bounded_identity(task_id, "task_id", 36)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("limit must be an integer from 1 through 100")
        statement = (
            select(TaskAttemptORM.__table__)
            .where(TaskAttemptORM.task_id == identifier)
            .order_by(TaskAttemptORM.attempt_no.desc(), TaskAttemptORM.id.desc())
            .limit(limit)
        )
        with self._engine.connect() as connection:
            _QueueSupport._task(connection, identifier)
            return tuple(
                _QueueSupport._task_attempt_record(row)
                for row in connection.execute(statement).mappings()
            )

    def bind_log_path(
        self,
        attempt_id: str,
        worker_id: str,
    ) -> str:
        """为活动任务尝试绑定由可信根和数据库身份推导的日志路径。

        入参：
            attempt_id：一次任务执行尝试的 UUID 标识。
            worker_id：当前 Worker 实例的稳定所有者标识。
        返回值：
            返回日志路径（``str``）。
        异常：
            任务日志根未配置、Worker 所有权或已持久化路径冲突时抛出 ``TaskQueueConflict``。
        """
        attempt_identifier = _QueueSupport._bounded_identity(
            attempt_id, "attempt_id", 36
        )
        worker = _QueueSupport._bounded_identity(worker_id, "worker_id", 128)
        root = self._task_log_root
        if root is None:
            _QueueSupport._raise_conflict(
                "TASK_LOG_ROOT_UNCONFIGURED",
                "task log path requires a configured trusted root",
                {"attempt_id": attempt_identifier},
            )
        bound_at = self._time()

        def write(connection: Connection) -> str:
            attempt, task = _QueueSupport._attempt_and_task(
                connection, attempt_identifier
            )
            _QueueSupport._require_owner(attempt, task, worker, attempt_identifier)
            _QueueSupport._require_active_pair(attempt, task, "bind_log_path")
            task_id = cast(str, task["id"])
            expected = (
                root
                / f"task_id={task_id}"
                / f"attempt_id={attempt_identifier}"
                / "run.log"
            )
            persisted = cast(str | None, attempt["log_path"])
            serialized = str(expected)
            if persisted == serialized:
                return serialized
            if persisted is not None:
                _QueueSupport._raise_conflict(
                    "TASK_LOG_PATH_CONFLICT",
                    "the task attempt already has a different log path",
                    {"attempt_id": attempt_identifier, "task_id": task_id},
                )
            connection.execute(
                update(TaskAttemptORM)
                .where(TaskAttemptORM.id == attempt_identifier)
                .values(log_path=serialized)
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=cast(str | None, task["experiment_id"]),
                task_id=task_id,
                event_type="TASK_LOG_BOUND",
                actor=worker,
                details={
                    "attempt_id": attempt_identifier,
                    "log_ref": (
                        f"task_id={task_id}/attempt_id={attempt_identifier}/run.log"
                    ),
                    "worker_id": worker,
                },
                created_at=bound_at,
            )
            return serialized

        return self._immediate(write)

    def claim(self, worker_id: str, now: datetime) -> ClaimedTask | None:
        """原子认领基础设施。

        入参：
            worker_id：当前 Worker 实例的稳定所有者标识。
            now：当前 UTC 时间。
        返回值：
            返回``claim``（``ClaimedTask | None``）。
        异常：
            无。
        Atomically claim the first available task under SQLite BEGIN IMMEDIATE.
        """
        worker = _QueueSupport._bounded_identity(worker_id, "worker_id", 128)
        claimed_at = self._time(now)
        timestamp = _QueueSupport._timestamp(claimed_at)

        def write(connection: Connection) -> ClaimedTask | None:
            task = (
                connection.execute(
                    select(TaskORM.__table__)
                    .where(
                        TaskORM.status == TaskStatus.QUEUED.value,
                        TaskORM.available_at <= timestamp,
                    )
                    .order_by(
                        TaskORM.priority.desc(),
                        TaskORM.created_at.asc(),
                        TaskORM.id.asc(),
                    )
                    .limit(1)
                )
                .mappings()
                .one_or_none()
            )
            if task is None:
                return None

            task_id = cast(str, task["id"])
            attempt_no = (
                int(
                    connection.scalar(
                        select(func.max(TaskAttemptORM.attempt_no)).where(
                            TaskAttemptORM.task_id == task_id
                        )
                    )
                    or 0
                )
                + 1
            )
            attempt_id = str(uuid4())
            progress = _QueueSupport._parse_progress(cast(str, task["progress_json"]))
            progress_json = _QueueSupport._progress_json(progress)
            updated = connection.execute(
                update(TaskORM)
                .where(
                    TaskORM.id == task_id,
                    TaskORM.status == TaskStatus.QUEUED.value,
                )
                .values(
                    status=TaskStatus.RUNNING.value,
                    worker_id=worker,
                    locked_at=timestamp,
                    heartbeat_at=timestamp,
                    updated_at=timestamp,
                    progress_json=progress_json,
                )
            )
            if updated.rowcount != 1:
                _QueueSupport._raise_state_conflict(
                    task_id, "claim", cast(str, task["status"])
                )
            connection.execute(
                insert(TaskAttemptORM).values(
                    id=attempt_id,
                    task_id=task_id,
                    attempt_no=attempt_no,
                    status=TaskStatus.RUNNING.value,
                    worker_id=worker,
                    started_at=timestamp,
                    heartbeat_at=timestamp,
                    completed_at=None,
                    log_path=None,
                    progress_json=progress_json,
                    error_json=None,
                    result_json=None,
                )
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=cast(str | None, task["experiment_id"]),
                task_id=task_id,
                event_type="TASK_CLAIMED",
                actor=worker,
                details={
                    "attempt_id": attempt_id,
                    "attempt_no": attempt_no,
                    "status": TaskStatus.RUNNING.value,
                    "worker_id": worker,
                },
                created_at=claimed_at,
            )
            payload = _QueueSupport._parse_json_object(
                cast(str, task["payload_json"]), "payload"
            )
            return ClaimedTask(
                id=task_id,
                attempt_id=attempt_id,
                attempt_no=attempt_no,
                experiment_id=cast(str | None, task["experiment_id"]),
                task_type=cast(str, task["task_type"]),
                payload=payload,
                priority=cast(int, task["priority"]),
                worker_id=worker,
                progress=progress,
                claimed_at=claimed_at,
            )

        return self._immediate(write)

    def heartbeat(
        self,
        attempt_id: str,
        worker_id: str,
        progress: TaskProgress,
        now: datetime,
    ) -> None:
        """续租任务所有权并持久化进度基础设施。

        入参：
            attempt_id：一次任务执行尝试的 UUID 标识。
            worker_id：当前 Worker 实例的稳定所有者标识。
            progress：当前尝试已完成量、总量和阶段说明。
            now：当前 UTC 时间。
        返回值：
            无。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``。
        Persist owner-fenced progress to both active runtime rows.
        """
        attempt_identifier = _QueueSupport._bounded_identity(
            attempt_id, "attempt_id", 36
        )
        worker = _QueueSupport._bounded_identity(worker_id, "worker_id", 128)
        if not isinstance(progress, TaskProgress):
            raise TypeError("progress must be a TaskProgress")
        heartbeat_at = self._time(now)
        timestamp = _QueueSupport._timestamp(heartbeat_at)
        progress_json = _QueueSupport._progress_json(progress)

        def write(connection: Connection) -> None:
            attempt, task = _QueueSupport._attempt_and_task(
                connection, attempt_identifier
            )
            _QueueSupport._require_owner(attempt, task, worker, attempt_identifier)
            _QueueSupport._require_active_pair(attempt, task, "heartbeat")
            connection.execute(
                update(TaskAttemptORM)
                .where(TaskAttemptORM.id == attempt_identifier)
                .values(heartbeat_at=timestamp, progress_json=progress_json)
            )
            connection.execute(
                update(TaskORM)
                .where(TaskORM.id == cast(str, task["id"]))
                .values(
                    heartbeat_at=timestamp,
                    progress_json=progress_json,
                    updated_at=timestamp,
                )
            )

        self._immediate(write)

    def is_cancel_requested(self, attempt_id: str, worker_id: str) -> bool:
        """判断取消请求值。

        入参：
            attempt_id：一次任务执行尝试的 UUID 标识。
            worker_id：当前 Worker 实例的稳定所有者标识。
        返回值：
            返回是否取消请求值。
        异常：
            无。
        Read cooperative-cancellation state under the current owner fence.
        """
        attempt_identifier = _QueueSupport._bounded_identity(
            attempt_id, "attempt_id", 36
        )
        worker = _QueueSupport._bounded_identity(worker_id, "worker_id", 128)
        with self._engine.connect() as connection:
            row = (
                connection.execute(
                    select(
                        TaskAttemptORM.id.label("attempt_id"),
                        TaskAttemptORM.task_id.label("attempt_task_id"),
                        TaskAttemptORM.worker_id.label("attempt_worker_id"),
                        TaskAttemptORM.status.label("attempt_status"),
                        TaskORM.id.label("task_id"),
                        TaskORM.worker_id.label("task_worker_id"),
                        TaskORM.status.label("task_status"),
                    )
                    .select_from(TaskAttemptORM)
                    .join(TaskORM, TaskORM.id == TaskAttemptORM.task_id)
                    .where(TaskAttemptORM.id == attempt_identifier)
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                _QueueSupport._raise_not_found(
                    "TASK_ATTEMPT_NOT_FOUND",
                    f"task attempt does not exist: {attempt_identifier}",
                    {"attempt_id": attempt_identifier},
                )
            attempt: dict[str, object] = {
                "id": row["attempt_id"],
                "task_id": row["attempt_task_id"],
                "worker_id": row["attempt_worker_id"],
                "status": row["attempt_status"],
            }
            task: dict[str, object] = {
                "id": row["task_id"],
                "worker_id": row["task_worker_id"],
                "status": row["task_status"],
            }
            _QueueSupport._require_owner(attempt, task, worker, attempt_identifier)
            _QueueSupport._require_active_pair(attempt, task, "is_cancel_requested")
            status = cast(str, task["status"])
            return status == TaskStatus.CANCEL_REQUESTED.value

    def request_cancel(
        self,
        task_id: str,
        actor: str,
        *,
        request_id: str | None = None,
        strict: bool = False,
    ) -> None:
        """请求取消。

        入参：
            task_id：目标任务标识，类型为 ``str``。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
            strict：控制是否启用``strict``规则的布尔开关。
        返回值：
            无。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``。
        Cancel queued work or request cooperative cancellation from its owner.
        """
        identifier = _QueueSupport._bounded_identity(task_id, "task_id", 36)
        subject = _QueueSupport._bounded_identity(actor, "actor", 128)
        request = _QueueSupport._optional_identity(request_id, "request_id", 128)
        if type(strict) is not bool:
            raise TypeError("strict must be a bool")
        cancelled_at = self._time()
        timestamp = _QueueSupport._timestamp(cancelled_at)

        def write(connection: Connection) -> None:
            task = _QueueSupport._task(connection, identifier)
            status = cast(str, task["status"])
            if status in (
                TaskStatus.CANCEL_REQUESTED.value,
                TaskStatus.CANCELLED.value,
            ):
                if strict:
                    _QueueSupport._raise_state_conflict(
                        identifier, "request_cancel", status
                    )
                return
            if status == TaskStatus.QUEUED.value:
                connection.execute(
                    update(TaskORM)
                    .where(TaskORM.id == identifier)
                    .values(
                        status=TaskStatus.CANCELLED.value,
                        completed_at=timestamp,
                        updated_at=timestamp,
                    )
                )
                event_type = "TASK_CANCELLED"
                new_status = TaskStatus.CANCELLED.value
            elif status == TaskStatus.RUNNING.value:
                attempts = list(
                    connection.execute(
                        select(TaskAttemptORM.__table__).where(
                            TaskAttemptORM.task_id == identifier,
                            TaskAttemptORM.status == TaskStatus.RUNNING.value,
                        )
                    ).mappings()
                )
                if len(attempts) != 1:
                    _QueueSupport._raise_state_conflict(
                        identifier, "request_cancel", status
                    )
                attempt = attempts[0]
                if task["worker_id"] is None or (
                    task["worker_id"] != attempt["worker_id"]
                ):
                    _QueueSupport._raise_state_conflict(
                        identifier, "request_cancel", status
                    )
                connection.execute(
                    update(TaskAttemptORM)
                    .where(TaskAttemptORM.id == cast(str, attempt["id"]))
                    .values(status=TaskStatus.CANCEL_REQUESTED.value)
                )
                connection.execute(
                    update(TaskORM)
                    .where(TaskORM.id == identifier)
                    .values(
                        status=TaskStatus.CANCEL_REQUESTED.value,
                        updated_at=timestamp,
                    )
                )
                event_type = "TASK_CANCEL_REQUESTED"
                new_status = TaskStatus.CANCEL_REQUESTED.value
            else:
                _QueueSupport._raise_state_conflict(
                    identifier, "request_cancel", status
                )

            _QueueSupport._add_audit(
                connection,
                experiment_id=cast(str | None, task["experiment_id"]),
                task_id=identifier,
                event_type=event_type,
                actor=subject,
                details={
                    "old_status": status,
                    "status": new_status,
                    "request_id": request,
                },
                created_at=cancelled_at,
            )

        self._immediate(write)

    def delete(
        self,
        task_id: str,
        actor: str,
        *,
        request_id: str | None = None,
    ) -> None:
        """删除一个终态任务及其尝试索引，同时保留外部产物与审计证据。

        入参：
            task_id：目标任务标识。
            actor：执行删除的主体标识。
            request_id：可选的请求关联标识。
        返回值：
            无。
        异常：
            任务不存在时抛出 ``TaskQueueNotFound``；任务尚未进入终态时抛出
            ``TaskQueueConflict``。
        """
        identifier = _QueueSupport._bounded_identity(task_id, "task_id", 36)
        subject = _QueueSupport._bounded_identity(actor, "actor", 128)
        request = _QueueSupport._optional_identity(request_id, "request_id", 128)
        deleted_at = self._time()

        def write(connection: Connection) -> None:
            task = _QueueSupport._task(connection, identifier)
            status = cast(str, task["status"])
            if status not in _TERMINAL_STATUSES:
                _QueueSupport._raise_state_conflict(identifier, "delete", status)
            _QueueSupport._add_audit(
                connection,
                experiment_id=cast(str | None, task["experiment_id"]),
                task_id=identifier,
                event_type="TASK_DELETED",
                actor=subject,
                details={
                    "task_id": identifier,
                    "task_type": cast(str, task["task_type"]),
                    "status": status,
                    "request_id": request,
                },
                created_at=deleted_at,
            )
            connection.execute(delete(TaskORM).where(TaskORM.id == identifier))

        self._immediate(write)

    def finish(self, attempt_id: str, worker_id: str, outcome: TaskOutcome) -> None:
        """将活跃尝试原子推进到终态基础设施。

        入参：
            attempt_id：一次任务执行尝试的 UUID 标识。
            worker_id：当前 Worker 实例的稳定所有者标识。
            outcome：执行结果。
        返回值：
            无。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``。
        Atomically finish one owner-fenced active attempt.
        """
        attempt_identifier = _QueueSupport._bounded_identity(
            attempt_id, "attempt_id", 36
        )
        worker = _QueueSupport._bounded_identity(worker_id, "worker_id", 128)
        if not isinstance(outcome, TaskOutcome):
            raise TypeError("outcome must be a TaskOutcome")
        completed_at = self._time()
        timestamp = _QueueSupport._timestamp(completed_at)
        error_json = (
            _QueueSupport._mapping_json_text(outcome.error, "error", MAX_ERROR_BYTES)
            if outcome.error is not None
            else None
        )
        result_json = (
            _QueueSupport._mapping_json_text(outcome.result, "result", MAX_ERROR_BYTES)
            if outcome.result is not None
            else None
        )

        def write(connection: Connection) -> None:
            attempt, task = _QueueSupport._attempt_and_task(
                connection, attempt_identifier
            )
            _QueueSupport._require_owner(attempt, task, worker, attempt_identifier)
            attempt_status = cast(str, attempt["status"])
            task_status = cast(str, task["status"])
            if attempt_status in _TERMINAL_STATUSES:
                if (
                    attempt_status == task_status == outcome.status.value
                    and attempt["error_json"] == task["error_json"] == error_json
                    and attempt["result_json"] == task["result_json"] == result_json
                ):
                    return
                _QueueSupport._raise_state_conflict(
                    cast(str, task["id"]), "finish", task_status
                )
            _QueueSupport._require_active_pair(attempt, task, "finish")
            registered_backtest_success = False
            if (
                task_status == TaskStatus.CANCEL_REQUESTED.value
                and outcome.status is TaskStatus.SUCCEEDED
                and task["task_type"] == "BACKTEST"
                and task["experiment_id"] is not None
            ):
                experiment_status = connection.scalar(
                    select(ExperimentORM.status).where(
                        ExperimentORM.id == cast(str, task["experiment_id"])
                    )
                )
                registered_backtest_success = experiment_status == "SUCCEEDED"
            if (
                task_status == TaskStatus.CANCEL_REQUESTED.value
                and outcome.status is not TaskStatus.CANCELLED
                and not registered_backtest_success
            ):
                _QueueSupport._raise_state_conflict(
                    cast(str, task["id"]), "finish", task_status
                )
            progress = _QueueSupport._parse_progress(
                cast(str, attempt["progress_json"])
            )
            progress_json = _QueueSupport._progress_json(progress)
            connection.execute(
                update(TaskAttemptORM)
                .where(TaskAttemptORM.id == attempt_identifier)
                .values(
                    status=outcome.status.value,
                    completed_at=timestamp,
                    progress_json=progress_json,
                    error_json=error_json,
                    result_json=result_json,
                )
            )
            connection.execute(
                update(TaskORM)
                .where(TaskORM.id == cast(str, task["id"]))
                .values(
                    status=outcome.status.value,
                    completed_at=timestamp,
                    updated_at=timestamp,
                    progress_json=progress_json,
                    error_json=error_json,
                    result_json=result_json,
                )
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=cast(str | None, task["experiment_id"]),
                task_id=cast(str, task["id"]),
                event_type="TASK_FINISHED",
                actor=worker,
                details={
                    "attempt_id": attempt_identifier,
                    "old_status": task_status,
                    "status": outcome.status.value,
                    "error": outcome.error,
                },
                created_at=completed_at,
            )

        self._immediate(write)

    def mark_orphans(self, now: datetime, stale_after: timedelta) -> int:
        """标记``orphans``。

        入参：
            now：当前 UTC 时间。
            stale_after：失联判定``after``。
        返回值：
            返回``orphans``（``int``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        Mark strictly stale active task/attempt pairs without re-enqueueing.
        """
        if not isinstance(stale_after, timedelta):
            raise TypeError("stale_after must be a timedelta")
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        orphaned_at = self._time(now)
        timestamp = _QueueSupport._timestamp(orphaned_at)
        error: dict[str, JsonValue] = {
            "code": "TASK_ORPHANED",
            "message": "task heartbeat exceeded the stale threshold",
            "stale_after_seconds": stale_after.total_seconds(),
        }
        error_json = _QueueSupport._mapping_json_text(error, "error", MAX_ERROR_BYTES)
        candidate_ids = self._orphan_candidate_ids(orphaned_at, stale_after)
        count = 0
        for task_id in candidate_ids:

            def write(connection: Connection, *, identifier: str = task_id) -> bool:
                task = (
                    connection.execute(
                        select(TaskORM.__table__).where(TaskORM.id == identifier)
                    )
                    .mappings()
                    .one_or_none()
                )
                if task is None or task["status"] not in _EXECUTION_STATUSES:
                    return False
                attempts = list(
                    connection.execute(
                        select(TaskAttemptORM.__table__).where(
                            TaskAttemptORM.task_id == identifier,
                            TaskAttemptORM.status.in_(_EXECUTION_STATUSES),
                        )
                    ).mappings()
                )
                if len(attempts) != 1:
                    _QueueSupport._raise_state_conflict(
                        identifier, "mark_orphans", cast(str, task["status"])
                    )
                attempt = attempts[0]
                _QueueSupport._require_active_pair(attempt, task, "mark_orphans")
                heartbeats = _QueueSupport._active_heartbeats(task, attempt)
                if not heartbeats:
                    _QueueSupport._raise_state_conflict(
                        identifier, "mark_orphans", cast(str, task["status"])
                    )
                if orphaned_at - max(heartbeats) <= stale_after:
                    return False
                connection.execute(
                    update(TaskAttemptORM)
                    .where(TaskAttemptORM.id == cast(str, attempt["id"]))
                    .values(
                        status=TaskStatus.ORPHANED.value,
                        completed_at=timestamp,
                        error_json=error_json,
                    )
                )
                connection.execute(
                    update(TaskORM)
                    .where(TaskORM.id == identifier)
                    .values(
                        status=TaskStatus.ORPHANED.value,
                        completed_at=timestamp,
                        updated_at=timestamp,
                        error_json=error_json,
                    )
                )
                _QueueSupport._converge_experiment_for_orphan(
                    connection,
                    task=task,
                    attempt=attempt,
                    error=error,
                    orphaned_at=orphaned_at,
                )
                _QueueSupport._add_audit(
                    connection,
                    experiment_id=cast(str | None, task["experiment_id"]),
                    task_id=identifier,
                    event_type="TASK_ORPHANED",
                    actor="system",
                    details={
                        "attempt_id": cast(str, attempt["id"]),
                        "old_status": cast(str, task["status"]),
                        "status": TaskStatus.ORPHANED.value,
                        "error": error,
                    },
                    created_at=orphaned_at,
                )
                return True

            count += int(self._immediate(write))
        return count

    def _orphan_candidate_ids(
        self, now: datetime, stale_after: timedelta
    ) -> tuple[str, ...]:
        """Read all active pairs without a write lock and return stale candidates."""
        with self._engine.connect() as connection:
            rows = list(
                connection.execute(
                    select(
                        TaskORM.id.label("task_id"),
                        TaskORM.status.label("task_status"),
                        TaskORM.heartbeat_at.label("task_heartbeat_at"),
                        TaskAttemptORM.id.label("attempt_id"),
                        TaskAttemptORM.status.label("attempt_status"),
                        TaskAttemptORM.heartbeat_at.label("attempt_heartbeat_at"),
                    )
                    .select_from(TaskORM)
                    .outerjoin(
                        TaskAttemptORM,
                        and_(
                            TaskAttemptORM.task_id == TaskORM.id,
                            TaskAttemptORM.status.in_(_EXECUTION_STATUSES),
                        ),
                    )
                    .where(TaskORM.status.in_(_EXECUTION_STATUSES))
                    .order_by(TaskORM.id, TaskAttemptORM.id)
                ).mappings()
            )

        grouped: dict[str, list[RowMapping]] = {}
        for row in rows:
            grouped.setdefault(cast(str, row["task_id"]), []).append(row)

        candidates: list[str] = []
        for task_id, task_rows in grouped.items():
            if len(task_rows) != 1:
                candidates.append(task_id)
                continue
            row = task_rows[0]
            if row["attempt_id"] is None or row["attempt_status"] != row["task_status"]:
                candidates.append(task_id)
                continue
            heartbeats = [
                _QueueSupport._parse_timestamp(cast(str, value))
                for value in (
                    row["task_heartbeat_at"],
                    row["attempt_heartbeat_at"],
                )
                if value is not None
            ]
            if not heartbeats or now - max(heartbeats) > stale_after:
                candidates.append(task_id)
        return tuple(candidates)

    def retry(
        self,
        task_id: str,
        actor: str,
        *,
        available_at: datetime | None = None,
        request_id: str | None = None,
    ) -> str:
        """重新提交可重试任务。

        入参：
            task_id：目标任务标识，类型为 ``str``。
            actor：操作主体。
            available_at：该条观测首次可供研究使用的带时区时间戳。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回重试（``str``）。
        异常：
            无。
        Explicitly reset one failed, cancelled, or orphaned task for a new claim.
        """
        identifier = _QueueSupport._bounded_identity(task_id, "task_id", 36)
        subject = _QueueSupport._bounded_identity(actor, "actor", 128)
        request = _QueueSupport._optional_identity(request_id, "request_id", 128)
        retried_at = self._time()
        available = self._time(available_at) if available_at is not None else retried_at
        timestamp = _QueueSupport._timestamp(retried_at)
        progress_json = _QueueSupport._progress_json(DEFAULT_PROGRESS)

        def write(connection: Connection) -> str:
            task = _QueueSupport._task(connection, identifier)
            status = cast(str, task["status"])
            if status not in _RETRYABLE_STATUSES:
                _QueueSupport._raise_state_conflict(identifier, "retry", status)
            active_attempt = connection.scalar(
                select(TaskAttemptORM.id).where(
                    TaskAttemptORM.task_id == identifier,
                    TaskAttemptORM.status.in_(_EXECUTION_STATUSES),
                )
            )
            if active_attempt is not None:
                _QueueSupport._raise_state_conflict(identifier, "retry", status)
            key = cast(str | None, task["idempotency_key"])
            if key is not None:
                collision = connection.scalar(
                    select(TaskORM.id).where(
                        TaskORM.id != identifier,
                        TaskORM.task_type == cast(str, task["task_type"]),
                        func.coalesce(TaskORM.experiment_id, "")
                        == (cast(str | None, task["experiment_id"]) or ""),
                        TaskORM.idempotency_key == key,
                        TaskORM.status.in_(_ACTIVE_STATUSES),
                    )
                )
                if collision is not None:
                    _QueueSupport._raise_idempotency_conflict(
                        collision,
                        cast(str, task["task_type"]),
                        cast(str | None, task["experiment_id"]),
                        key,
                    )
            connection.execute(
                update(TaskORM)
                .where(TaskORM.id == identifier)
                .values(
                    status=TaskStatus.QUEUED.value,
                    worker_id=None,
                    locked_at=None,
                    heartbeat_at=None,
                    completed_at=None,
                    error_json=None,
                    result_json=None,
                    progress_json=progress_json,
                    available_at=_QueueSupport._timestamp(available),
                    updated_at=timestamp,
                )
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=cast(str | None, task["experiment_id"]),
                task_id=identifier,
                event_type="TASK_RETRIED",
                actor=subject,
                details={
                    "request_id": request,
                    "old_status": status,
                    "status": TaskStatus.QUEUED.value,
                },
                created_at=retried_at,
            )
            return identifier

        return self._immediate(write)

    def clone_for_retry(
        self,
        task_id: str,
        actor: str,
        *,
        request_id: str | None = None,
    ) -> tuple[str | None, str]:
        """处理基础设施中的克隆``for``重试。

        入参：
            task_id：目标任务标识，类型为 ``str``。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
        返回值：
            返回``for``重试（``tuple[str | None, str]``）。
        异常：
            无。
        Create fresh queued identities from one immutable terminal task.
        """
        identifier = _QueueSupport._bounded_identity(task_id, "task_id", 36)
        subject = _QueueSupport._bounded_identity(actor, "actor", 128)
        request = _QueueSupport._optional_identity(request_id, "request_id", 128)
        cloned_at = self._time()
        timestamp = _QueueSupport._timestamp(cloned_at)
        progress_json = _QueueSupport._progress_json(DEFAULT_PROGRESS)

        def write(connection: Connection) -> tuple[str | None, str]:
            task = _QueueSupport._task(connection, identifier)
            status = cast(str, task["status"])
            if status not in _TERMINAL_STATUSES:
                _QueueSupport._raise_state_conflict(
                    identifier, "clone_for_retry", status
                )
            active_attempt = connection.scalar(
                select(TaskAttemptORM.id).where(
                    TaskAttemptORM.task_id == identifier,
                    TaskAttemptORM.status.in_(_EXECUTION_STATUSES),
                )
            )
            if active_attempt is not None:
                _QueueSupport._raise_state_conflict(
                    identifier, "clone_for_retry", status
                )
            original_experiment_id = cast(str | None, task["experiment_id"])
            existing_clone = _QueueSupport._existing_retry_clone(
                connection,
                identifier,
                original_experiment_id,
            )
            if existing_clone is not None:
                return existing_clone
            if original_experiment_id is None:
                new_task_id = str(uuid4())
                original_payload = _QueueSupport._parse_json_object(
                    cast(str, task["payload_json"]), "payload"
                )
                connection.execute(
                    insert(TaskORM).values(
                        id=new_task_id,
                        experiment_id=None,
                        task_type=task["task_type"],
                        payload_json=_QueueSupport._mapping_json_text(
                            original_payload,
                            "payload",
                            MAX_PAYLOAD_BYTES,
                        ),
                        status=TaskStatus.QUEUED.value,
                        priority=task["priority"],
                        progress_json=progress_json,
                        created_at=timestamp,
                        available_at=timestamp,
                        updated_at=timestamp,
                        heartbeat_at=None,
                        completed_at=None,
                        idempotency_key=task["idempotency_key"],
                        worker_id=None,
                        locked_at=None,
                        error_json=None,
                        result_json=None,
                    )
                )
                _QueueSupport._add_audit(
                    connection,
                    experiment_id=None,
                    task_id=new_task_id,
                    event_type="TASK_ENQUEUED",
                    actor=subject,
                    details={
                        "cloned_from_task_id": identifier,
                        "idempotency_key": cast(str | None, task["idempotency_key"]),
                        "request_id": request,
                        "status": TaskStatus.QUEUED.value,
                    },
                    created_at=cloned_at,
                )
                _QueueSupport._add_audit(
                    connection,
                    experiment_id=None,
                    task_id=identifier,
                    event_type="TASK_CLONED_FOR_RETRY",
                    actor=subject,
                    details={
                        "new_experiment_id": None,
                        "new_task_id": new_task_id,
                        "old_status": status,
                        "request_id": request,
                    },
                    created_at=cloned_at,
                )
                return None, new_task_id
            experiment = (
                connection.execute(
                    select(ExperimentORM.__table__).where(
                        ExperimentORM.id == original_experiment_id
                    )
                )
                .mappings()
                .one_or_none()
            )
            if experiment is None:
                _QueueSupport._raise_not_found(
                    "TASK_EXPERIMENT_NOT_FOUND",
                    "task experiment does not exist",
                    {"experiment_id": original_experiment_id, "task_id": identifier},
                )
            experiment_status = cast(str, experiment["status"])
            if experiment_status not in {
                ExperimentStatus.SUCCEEDED.value,
                ExperimentStatus.FAILED.value,
                ExperimentStatus.CANCELLED.value,
            }:
                _QueueSupport._raise_conflict(
                    "EXPERIMENT_RETRY_STATE_CONFLICT",
                    "experiment is not terminal for retry cloning",
                    {
                        "experiment_id": original_experiment_id,
                        "status": experiment_status,
                        "task_id": identifier,
                    },
                )
            if cast(str, task["task_type"]) != "BACKTEST":
                _QueueSupport._raise_conflict(
                    "TASK_RETRY_TYPE_CONFLICT",
                    "experiment retry requires a BACKTEST task",
                    {"task_id": identifier},
                )
            original_payload = _QueueSupport._parse_json_object(
                cast(str, task["payload_json"]), "payload"
            )
            if original_payload != {
                "experiment_id": original_experiment_id,
                "config_hash": cast(str, experiment["config_hash"]),
            }:
                _QueueSupport._raise_conflict(
                    "TASK_RETRY_PAYLOAD_CONFLICT",
                    "original backtest payload does not match its experiment",
                    {"task_id": identifier},
                )
            new_experiment_id = str(uuid4())
            new_task_id = str(uuid4())
            new_payload_json = _QueueSupport._mapping_json_text(
                {
                    "experiment_id": new_experiment_id,
                    "config_hash": cast(str, experiment["config_hash"]),
                },
                "payload",
                MAX_PAYLOAD_BYTES,
            )
            connection.execute(
                insert(ExperimentORM).values(
                    id=new_experiment_id,
                    strategy_id=experiment["strategy_id"],
                    config_json=experiment["config_json"],
                    config_hash=experiment["config_hash"],
                    data_hash=experiment["data_hash"],
                    source_tree_hash=experiment["source_tree_hash"],
                    git_commit_hash=experiment["git_commit_hash"],
                    lockfile_hash=experiment["lockfile_hash"],
                    rulebook_hash=experiment["rulebook_hash"],
                    fingerprint=experiment["fingerprint"],
                    status=ExperimentStatus.QUEUED.value,
                    research_mark=ResearchMark.UNREVIEWED.value,
                    created_at=timestamp,
                    queued_at=timestamp,
                    started_at=None,
                    completed_at=None,
                )
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=new_experiment_id,
                task_id=None,
                event_type="EXPERIMENT_CREATED",
                actor=subject,
                details={
                    "cloned_from_experiment_id": original_experiment_id,
                    "request_id": request,
                    "status": ExperimentStatus.CREATED.value,
                },
                created_at=cloned_at,
            )
            connection.execute(
                insert(TaskORM).values(
                    id=new_task_id,
                    experiment_id=new_experiment_id,
                    task_type="BACKTEST",
                    payload_json=new_payload_json,
                    status=TaskStatus.QUEUED.value,
                    priority=cast(int, task["priority"]),
                    progress_json=progress_json,
                    created_at=timestamp,
                    available_at=timestamp,
                    updated_at=timestamp,
                    heartbeat_at=None,
                    completed_at=None,
                    idempotency_key=_BACKTEST_IDEMPOTENCY_KEY,
                    worker_id=None,
                    locked_at=None,
                    error_json=None,
                    result_json=None,
                )
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=new_experiment_id,
                task_id=None,
                event_type="EXPERIMENT_STATE_TRANSITIONED",
                actor=subject,
                details={
                    "new_status": ExperimentStatus.QUEUED.value,
                    "old_status": ExperimentStatus.CREATED.value,
                    "request_id": request,
                },
                created_at=cloned_at,
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=new_experiment_id,
                task_id=new_task_id,
                event_type="TASK_ENQUEUED",
                actor=subject,
                details={
                    "idempotency_key": _BACKTEST_IDEMPOTENCY_KEY,
                    "request_id": request,
                    "status": TaskStatus.QUEUED.value,
                },
                created_at=cloned_at,
            )
            _QueueSupport._add_audit(
                connection,
                experiment_id=original_experiment_id,
                task_id=identifier,
                event_type="TASK_CLONED_FOR_RETRY",
                actor=subject,
                details={
                    "new_experiment_id": new_experiment_id,
                    "new_task_id": new_task_id,
                    "old_status": status,
                    "request_id": request,
                },
                created_at=cloned_at,
            )
            return new_experiment_id, new_task_id

        return self._immediate(write)

    def _time(self, supplied: datetime | None = None) -> datetime:
        return _QueueSupport._utc(
            supplied if supplied is not None else self._clock(), "timestamp"
        )

    def _immediate(self, write: Callable[[Connection], _T]) -> _T:
        """Run one short write with an explicit SQLite BEGIN IMMEDIATE."""
        for retry_no in range(len(self._lock_retry_delays) + 1):
            try:
                return self._immediate_once(write)
            except OperationalError as error:
                if not _QueueSupport._is_sqlite_lock_error(error):
                    raise
                if retry_no == len(self._lock_retry_delays):
                    _QueueSupport._raise_queue_busy(len(self._lock_retry_delays))
                self._sleeper(self._lock_retry_delays[retry_no])
        raise AssertionError("unreachable lock retry state")

    def _immediate_once(self, write: Callable[[Connection], _T]) -> _T:
        with self._engine.connect() as connection:
            try:
                connection.exec_driver_sql("BEGIN IMMEDIATE")
                result = write(connection)
                connection.commit()
                return result
            except BaseException:
                if connection.in_transaction():
                    connection.rollback()
                raise


class _QueueSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _task(connection: Connection, task_id: str) -> RowMapping:
        row = (
            connection.execute(select(TaskORM.__table__).where(TaskORM.id == task_id))
            .mappings()
            .one_or_none()
        )
        if row is None:
            _QueueSupport._raise_not_found(
                "TASK_NOT_FOUND",
                f"task does not exist: {task_id}",
                {"task_id": task_id},
            )
        return row

    @staticmethod
    def _converge_experiment_for_orphan(
        connection: Connection,
        *,
        task: RowMapping,
        attempt: RowMapping,
        error: Mapping[str, JsonValue],
        orphaned_at: datetime,
    ) -> None:
        """Converge a crashed canonical BACKTEST and its experiment atomically."""
        experiment_id = cast(str | None, task["experiment_id"])
        if cast(str, task["task_type"]) != "BACKTEST" or experiment_id is None:
            return
        experiment = (
            connection.execute(
                select(ExperimentORM.__table__).where(ExperimentORM.id == experiment_id)
            )
            .mappings()
            .one_or_none()
        )
        if experiment is None:
            return
        old_status = cast(str, experiment["status"])
        if old_status == ExperimentStatus.RUNNING.value:
            new_status = ExperimentStatus.FAILED.value
        elif old_status == ExperimentStatus.QUEUED.value:
            new_status = ExperimentStatus.CANCELLED.value
        else:
            return
        timestamp = _QueueSupport._timestamp(orphaned_at)
        connection.execute(
            update(ExperimentORM)
            .where(
                ExperimentORM.id == experiment_id,
                ExperimentORM.status == old_status,
            )
            .values(
                status=new_status,
                completed_at=timestamp,
            )
        )
        _QueueSupport._add_audit(
            connection,
            experiment_id=experiment_id,
            task_id=cast(str, task["id"]),
            event_type="EXPERIMENT_STATE_TRANSITIONED",
            actor="system",
            details={
                "old_status": old_status,
                "new_status": new_status,
                "reason": {
                    "code": cast(str, error["code"]),
                    "severity": Severity.FATAL.value,
                    "context": {
                        "attempt_id": cast(str, attempt["id"]),
                        "task_id": cast(str, task["id"]),
                    },
                    "retryable": False,
                },
            },
            created_at=orphaned_at,
        )

    @staticmethod
    def _existing_retry_clone(
        connection: Connection,
        task_id: str,
        original_experiment_id: str | None,
    ) -> tuple[str | None, str] | None:
        lineage = (
            connection.execute(
                select(AuditEventORM.details_json)
                .where(
                    AuditEventORM.task_id == task_id,
                    AuditEventORM.event_type == "TASK_CLONED_FOR_RETRY",
                )
                .order_by(AuditEventORM.id)
                .limit(2)
            )
            .scalars()
            .all()
        )
        if not lineage:
            return None
        if len(lineage) != 1:
            _QueueSupport._raise_conflict(
                "TASK_RETRY_LINEAGE_CONFLICT",
                "task has multiple retry clone lineage records",
                {"task_id": task_id},
            )
        details = _QueueSupport._parse_json_object(lineage[0], "retry clone lineage")
        new_task_id = details.get("new_task_id")
        new_experiment_id = details.get("new_experiment_id")
        if not isinstance(new_task_id, str) or (
            new_experiment_id is not None and not isinstance(new_experiment_id, str)
        ):
            _QueueSupport._raise_conflict(
                "TASK_RETRY_LINEAGE_CONFLICT",
                "task retry clone lineage is invalid",
                {"task_id": task_id},
            )
        if (original_experiment_id is None) != (new_experiment_id is None):
            _QueueSupport._raise_conflict(
                "TASK_RETRY_LINEAGE_CONFLICT",
                "task retry clone lineage changes experiment association",
                {"task_id": task_id},
            )
        clone = (
            connection.execute(
                select(TaskORM.id, TaskORM.experiment_id).where(
                    TaskORM.id == new_task_id
                )
            )
            .mappings()
            .one_or_none()
        )
        if clone is None or clone["experiment_id"] != new_experiment_id:
            _QueueSupport._raise_conflict(
                "TASK_RETRY_LINEAGE_CONFLICT",
                "task retry clone lineage target does not exist",
                {"task_id": task_id},
            )
        if new_experiment_id is not None:
            clone_experiment = connection.scalar(
                select(ExperimentORM.id).where(ExperimentORM.id == new_experiment_id)
            )
            if clone_experiment is None:
                _QueueSupport._raise_conflict(
                    "TASK_RETRY_LINEAGE_CONFLICT",
                    "task retry clone experiment does not exist",
                    {"task_id": task_id},
                )
        return new_experiment_id, new_task_id

    @staticmethod
    def _attempt_and_task(
        connection: Connection, attempt_id: str
    ) -> tuple[RowMapping, RowMapping]:
        attempt = (
            connection.execute(
                select(TaskAttemptORM.__table__).where(TaskAttemptORM.id == attempt_id)
            )
            .mappings()
            .one_or_none()
        )
        if attempt is None:
            _QueueSupport._raise_not_found(
                "TASK_ATTEMPT_NOT_FOUND",
                f"task attempt does not exist: {attempt_id}",
                {"attempt_id": attempt_id},
            )
        task = _QueueSupport._task(connection, cast(str, attempt["task_id"]))
        return attempt, task

    @staticmethod
    def _require_owner(
        attempt: RowMapping | Mapping[str, object],
        task: RowMapping | Mapping[str, object],
        worker_id: str,
        attempt_id: str,
    ) -> None:
        if attempt["worker_id"] != worker_id or task["worker_id"] != worker_id:
            _QueueSupport._raise_conflict(
                "TASK_OWNERSHIP_CONFLICT",
                "task attempt is owned by another worker",
                {
                    "attempt_id": attempt_id,
                    "task_id": cast(str, task["id"]),
                    "worker_id": worker_id,
                    "owner_id": cast(str | None, attempt["worker_id"]),
                },
            )

    @staticmethod
    def _require_active_pair(
        attempt: RowMapping | Mapping[str, object],
        task: RowMapping | Mapping[str, object],
        operation: str,
    ) -> None:
        attempt_status = cast(str, attempt["status"])
        task_status = cast(str, task["status"])
        if (
            attempt_status not in _EXECUTION_STATUSES
            or task_status not in _EXECUTION_STATUSES
            or attempt_status != task_status
        ):
            _QueueSupport._raise_state_conflict(
                cast(str, task["id"]), operation, task_status
            )

    @staticmethod
    def _active_heartbeats(task: RowMapping, attempt: RowMapping) -> list[datetime]:
        return [
            _QueueSupport._parse_timestamp(cast(str, value))
            for value in (task["heartbeat_at"], attempt["heartbeat_at"])
            if value is not None
        ]

    @staticmethod
    def _mapping_json_text(
        value: Mapping[str, JsonValue] | None,
        label: str,
        limit: int,
    ) -> str:
        if value is None or not isinstance(value, Mapping):
            raise TypeError(f"{label} must be a mapping")
        _QueueSupport._validate_json_keys(value, label)
        encoded = canonical_json_bytes(_QueueSupport._plain_json(value))
        if len(encoded) > limit:
            raise ValueError(f"{label} JSON exceeds {limit} bytes")
        parsed = json.loads(encoded)
        if not isinstance(parsed, dict):
            raise TypeError(f"{label} must be a JSON object")
        return encoded.decode("utf-8")

    @staticmethod
    def _plain_json(value: JsonValue) -> JsonValue:
        if isinstance(value, Mapping):
            return {key: _QueueSupport._plain_json(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_QueueSupport._plain_json(item) for item in value]
        return value

    @staticmethod
    def _validate_json_keys(value: JsonValue, label: str) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError(f"{label} JSON object keys must be strings")
                _QueueSupport._validate_json_keys(item, label)
        elif isinstance(value, list):
            for item in value:
                _QueueSupport._validate_json_keys(item, label)

    @staticmethod
    def _parse_json_object(value: str, label: str) -> dict[str, JsonValue]:
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError) as error:
            raise ValueError(f"persisted {label} must be valid JSON") from error
        if not isinstance(parsed, dict):
            raise TypeError(f"persisted {label} must be a JSON object")
        canonical_json_bytes(cast(JsonValue, parsed))
        return cast(dict[str, JsonValue], parsed)

    @staticmethod
    def _progress_json(progress: TaskProgress) -> str:
        return canonical_json_bytes(cast(JsonValue, progress.model_dump())).decode(
            "utf-8"
        )

    @staticmethod
    def _parse_progress(value: str) -> TaskProgress:
        parsed = _QueueSupport._parse_json_object(value, "progress")
        if not parsed:
            return DEFAULT_PROGRESS
        return TaskProgress.model_validate(parsed)

    @staticmethod
    def _add_audit(
        connection: Connection,
        *,
        experiment_id: str | None,
        task_id: str | None,
        event_type: str,
        actor: str,
        details: Mapping[str, JsonValue],
        created_at: datetime,
    ) -> None:
        details_json = _QueueSupport._mapping_json_text(
            details, "audit details", MAX_AUDIT_BYTES
        )
        connection.execute(
            insert(AuditEventORM).values(
                experiment_id=experiment_id,
                task_id=task_id,
                event_type=event_type,
                actor=actor,
                details_json=details_json,
                created_at=_QueueSupport._timestamp(created_at),
            )
        )

    @staticmethod
    def _task_record(row: RowMapping) -> TaskRecord:
        progress = cast(
            dict[str, JsonValue],
            _QueueSupport._parse_progress(cast(str, row["progress_json"])).model_dump(
                mode="json"
            ),
        )
        error = (
            _QueueSupport._parse_json_object(cast(str, row["error_json"]), "error")
            if row["error_json"] is not None
            else None
        )
        result = (
            _QueueSupport._parse_json_object(cast(str, row["result_json"]), "result")
            if row["result_json"] is not None
            else None
        )
        return TaskRecord(
            id=cast(str, row["id"]),
            experiment_id=cast(str | None, row["experiment_id"]),
            task_type=cast(str, row["task_type"]),
            payload=_QueueSupport._parse_json_object(
                cast(str, row["payload_json"]), "payload"
            ),
            status=TaskStatus(cast(str, row["status"])),
            priority=cast(int, row["priority"]),
            progress=progress,
            created_at=_QueueSupport._parse_timestamp(cast(str, row["created_at"])),
            available_at=_QueueSupport._parse_timestamp(cast(str, row["available_at"])),
            updated_at=_QueueSupport._parse_timestamp(cast(str, row["updated_at"])),
            heartbeat_at=(
                _QueueSupport._parse_timestamp(cast(str, row["heartbeat_at"]))
                if row["heartbeat_at"] is not None
                else None
            ),
            completed_at=(
                _QueueSupport._parse_timestamp(cast(str, row["completed_at"]))
                if row["completed_at"] is not None
                else None
            ),
            idempotency_key=cast(str | None, row["idempotency_key"]),
            worker_id=cast(str | None, row["worker_id"]),
            locked_at=(
                _QueueSupport._parse_timestamp(cast(str, row["locked_at"]))
                if row["locked_at"] is not None
                else None
            ),
            error=error,
            result=result,
        )

    @staticmethod
    def _task_attempt_record(row: RowMapping) -> TaskAttemptRecord:
        error = (
            _QueueSupport._parse_json_object(
                cast(str, row["error_json"]), "attempt error"
            )
            if row["error_json"] is not None
            else None
        )
        result = (
            _QueueSupport._parse_json_object(
                cast(str, row["result_json"]), "attempt result"
            )
            if row["result_json"] is not None
            else None
        )
        return TaskAttemptRecord(
            id=cast(str, row["id"]),
            task_id=cast(str, row["task_id"]),
            attempt_no=cast(int, row["attempt_no"]),
            status=TaskStatus(cast(str, row["status"])),
            worker_id=cast(str | None, row["worker_id"]),
            started_at=_QueueSupport._parse_timestamp(cast(str, row["started_at"])),
            heartbeat_at=(
                _QueueSupport._parse_timestamp(cast(str, row["heartbeat_at"]))
                if row["heartbeat_at"] is not None
                else None
            ),
            completed_at=(
                _QueueSupport._parse_timestamp(cast(str, row["completed_at"]))
                if row["completed_at"] is not None
                else None
            ),
            log_path=cast(str | None, row["log_path"]),
            progress=_QueueSupport._parse_progress(
                cast(str, row["progress_json"])
            ).model_dump(mode="json"),
            error=error,
            result=result,
        )

    @staticmethod
    def _bounded_identity(value: str, label: str, limit: int) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{label} must be a string")
        if not value.strip():
            raise ValueError(f"{label} must not be empty")
        if len(value) > limit:
            raise ValueError(f"{label} exceeds {limit} characters")
        return value

    @staticmethod
    def _optional_identity(value: str | None, label: str, limit: int) -> str | None:
        return (
            None
            if value is None
            else _QueueSupport._bounded_identity(value, label, limit)
        )

    @staticmethod
    def _sha256(value: str, label: str) -> str:
        normalized = _QueueSupport._bounded_identity(value, label, 64)
        if len(normalized) != 64 or any(
            character not in "0123456789abcdef" for character in normalized
        ):
            raise ValueError(f"{label} must be a lowercase SHA-256 hex digest")
        return normalized

    @staticmethod
    def _utc(value: datetime, label: str) -> datetime:
        if not isinstance(value, datetime):
            raise TypeError(f"{label} must be a datetime")
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{label} must be timezone-aware")
        return value.astimezone(UTC)

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return _QueueSupport._utc(value, "timestamp").isoformat()

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(value)
        except (TypeError, ValueError) as error:
            raise ValueError("persisted timestamp must be ISO-8601") from error
        return _QueueSupport._utc(parsed, "persisted timestamp")

    @staticmethod
    def _raise_not_found(
        code: str, message: str, context: Mapping[str, object]
    ) -> Never:
        raise TaskQueueNotFound(
            ErrorDetail(
                code=code,
                severity=Severity.SEVERE,
                message=message,
                context=context,
                remediation="use an identity returned by the durable task queue",
                retryable=False,
            )
        )

    @staticmethod
    def _raise_conflict(
        code: str, message: str, context: Mapping[str, object]
    ) -> Never:
        raise TaskQueueConflict(
            ErrorDetail(
                code=code,
                severity=Severity.SEVERE,
                message=message,
                context=context,
                remediation="reload durable task state before retrying the operation",
                retryable=False,
            )
        )

    @staticmethod
    def _raise_state_conflict(task_id: str, operation: str, status: str) -> Never:
        _QueueSupport._raise_conflict(
            "TASK_STATE_CONFLICT",
            f"task {task_id} cannot perform {operation} from {status}",
            {"task_id": task_id, "operation": operation, "status": status},
        )

    @staticmethod
    def _raise_idempotency_conflict(
        task_id: str,
        task_type: str,
        experiment_id: str | None,
        idempotency_key: str,
    ) -> Never:
        _QueueSupport._raise_conflict(
            "TASK_IDEMPOTENCY_CONFLICT",
            "active idempotency namespace has a different canonical payload",
            {
                "task_id": task_id,
                "task_type": task_type,
                "experiment_id": experiment_id,
                "idempotency_key": idempotency_key,
            },
        )

    @staticmethod
    def _is_sqlite_lock_error(error: OperationalError) -> bool:
        message = str(error.orig).lower()
        return any(
            marker in message
            for marker in (
                "database is locked",
                "database table is locked",
                "database is busy",
            )
        )

    @staticmethod
    def _raise_queue_busy(retries: int) -> Never:
        raise TaskQueueBusy(
            ErrorDetail(
                code="TASK_QUEUE_BUSY",
                severity=Severity.SEVERE,
                message="SQLite task queue write lock remained busy",
                context={"retries": retries},
                remediation="retry the queue operation after the current writer commits",
                retryable=True,
            )
        )
