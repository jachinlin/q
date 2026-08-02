"""Durable, transactionally fenced SQLite task queue operations."""

from __future__ import annotations

import json
import math
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Never, TypeVar, cast
from uuid import uuid4

from sqlalchemy import (
    Connection,
    Engine,
    RowMapping,
    and_,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.exc import OperationalError

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.domain.enums import Severity
from quant_core.errors import ErrorDetail, QuantError
from quant_core.persistence.orm import (
    AuditEventORM,
    ExperimentORM,
    TaskAttemptORM,
    TaskORM,
)
from quant_core.tasks.models import (
    ClaimedTask,
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


class TaskQueueError(QuantError):
    """Base class for machine-readable queue failures."""


class TaskQueueNotFound(TaskQueueError):
    """A requested task or attempt identity is absent."""


class TaskQueueConflict(TaskQueueError):
    """A queue state, ownership, or idempotency precondition failed."""


class TaskQueueBusy(TaskQueueError):
    """SQLite could not acquire its bounded queue write lock."""


TaskNotFound = TaskQueueNotFound
TaskConflict = TaskQueueConflict


class TaskQueue:
    """Persist queue state in short explicit SQLite write transactions."""

    def __init__(
        self,
        engine: Engine,
        *,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] | None = None,
        lock_retry_delays: tuple[float, ...] = (0.01, 0.02, 0.04),
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
        """Create queued work or return its active canonical idempotency match."""
        task_kind = _bounded_identity(task_type, "task_type", 64)
        subject = _bounded_identity(actor, "actor", 128)
        experiment = _optional_identity(experiment_id, "experiment_id", 36)
        key = _optional_identity(idempotency_key, "idempotency_key", 128)
        request = _optional_identity(request_id, "request_id", 128)
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise TypeError("priority must be an int and not bool")
        payload_json = _mapping_json_text(payload, "payload", MAX_PAYLOAD_BYTES)
        created_at = self._time()
        available = self._time(available_at) if available_at is not None else created_at
        progress_json = _progress_json(DEFAULT_PROGRESS)

        def write(connection: Connection) -> str:
            if experiment is not None:
                exists = connection.scalar(
                    select(ExperimentORM.id).where(ExperimentORM.id == experiment)
                )
                if exists is None:
                    _raise_not_found(
                        "TASK_EXPERIMENT_NOT_FOUND",
                        f"experiment does not exist: {experiment}",
                        {"experiment_id": experiment},
                    )
            if key is not None:
                existing = connection.execute(
                    select(TaskORM.__table__).where(
                        TaskORM.task_type == task_kind,
                        func.coalesce(TaskORM.experiment_id, "")
                        == (experiment or ""),
                        TaskORM.idempotency_key == key,
                        TaskORM.status.in_(_ACTIVE_STATUSES),
                    )
                ).mappings().one_or_none()
                if existing is not None:
                    if existing["payload_json"] != payload_json:
                        _raise_idempotency_conflict(
                            cast(str, existing["id"]), task_kind, experiment, key
                        )
                    _add_audit(
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
                    created_at=_timestamp(created_at),
                    available_at=_timestamp(available),
                    updated_at=_timestamp(created_at),
                    heartbeat_at=None,
                    completed_at=None,
                    idempotency_key=key,
                    worker_id=None,
                    locked_at=None,
                    error_json=None,
                )
            )
            _add_audit(
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
        """Atomically queue one immutable experiment or return its active task."""
        experiment = _bounded_identity(experiment_id, "experiment_id", 36)
        expected_hash = _sha256(config_hash, "config_hash")
        subject = _bounded_identity(actor, "actor", 128)
        request = _optional_identity(request_id, "request_id", 128)
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise TypeError("priority must be an int and not bool")
        payload: dict[str, JsonValue] = {
            "experiment_id": experiment,
            "config_hash": expected_hash,
        }
        payload_json = _mapping_json_text(payload, "payload", MAX_PAYLOAD_BYTES)
        submitted_at = self._time()
        timestamp = _timestamp(submitted_at)
        progress_json = _progress_json(DEFAULT_PROGRESS)

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
                _raise_not_found(
                    "TASK_EXPERIMENT_NOT_FOUND",
                    f"experiment does not exist: {experiment}",
                    {"experiment_id": experiment},
                )
            if record["config_hash"] != expected_hash:
                _raise_conflict(
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
                if (
                    existing["payload_json"] != payload_json
                    or record["status"] not in {"QUEUED", "RUNNING"}
                ):
                    _raise_idempotency_conflict(
                        cast(str, existing["id"]),
                        "BACKTEST",
                        experiment,
                        _BACKTEST_IDEMPOTENCY_KEY,
                    )
                _add_audit(
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
                _raise_conflict(
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
                _raise_conflict(
                    "EXPERIMENT_SUBMIT_CONFLICT",
                    "experiment changed state during submission",
                    {"experiment_id": experiment, "status": status},
                )
            _add_audit(
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
            _add_audit(
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

    def get(self, task_id: str) -> TaskRecord:
        """Read one immutable task record without claiming or mutating it."""
        identifier = _bounded_identity(task_id, "task_id", 36)
        with self._engine.connect() as connection:
            return _task_record(_task(connection, identifier))

    def claim(self, worker_id: str, now: datetime) -> ClaimedTask | None:
        """Atomically claim the first available task under SQLite BEGIN IMMEDIATE."""
        worker = _bounded_identity(worker_id, "worker_id", 128)
        claimed_at = self._time(now)
        timestamp = _timestamp(claimed_at)

        def write(connection: Connection) -> ClaimedTask | None:
            task = connection.execute(
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
            ).mappings().one_or_none()
            if task is None:
                return None

            task_id = cast(str, task["id"])
            attempt_no = int(
                connection.scalar(
                    select(func.max(TaskAttemptORM.attempt_no)).where(
                        TaskAttemptORM.task_id == task_id
                    )
                )
                or 0
            ) + 1
            attempt_id = str(uuid4())
            progress = _parse_progress(cast(str, task["progress_json"]))
            progress_json = _progress_json(progress)
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
                _raise_state_conflict(task_id, "claim", cast(str, task["status"]))
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
                )
            )
            _add_audit(
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
            payload = _parse_json_object(cast(str, task["payload_json"]), "payload")
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
        """Persist owner-fenced progress to both active runtime rows."""
        attempt_identifier = _bounded_identity(attempt_id, "attempt_id", 36)
        worker = _bounded_identity(worker_id, "worker_id", 128)
        if not isinstance(progress, TaskProgress):
            raise TypeError("progress must be a TaskProgress")
        heartbeat_at = self._time(now)
        timestamp = _timestamp(heartbeat_at)
        progress_json = _progress_json(progress)

        def write(connection: Connection) -> None:
            attempt, task = _attempt_and_task(connection, attempt_identifier)
            _require_owner(attempt, task, worker, attempt_identifier)
            _require_active_pair(attempt, task, "heartbeat")
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
        """Read cooperative-cancellation state under the current owner fence."""
        attempt_identifier = _bounded_identity(attempt_id, "attempt_id", 36)
        worker = _bounded_identity(worker_id, "worker_id", 128)
        with self._engine.connect() as connection:
            row = connection.execute(
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
            ).mappings().one_or_none()
            if row is None:
                _raise_not_found(
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
            _require_owner(attempt, task, worker, attempt_identifier)
            _require_active_pair(attempt, task, "is_cancel_requested")
            status = cast(str, task["status"])
            return status == TaskStatus.CANCEL_REQUESTED.value

    def request_cancel(self, task_id: str, actor: str) -> None:
        """Cancel queued work or request cooperative cancellation from its owner."""
        identifier = _bounded_identity(task_id, "task_id", 36)
        subject = _bounded_identity(actor, "actor", 128)
        cancelled_at = self._time()
        timestamp = _timestamp(cancelled_at)

        def write(connection: Connection) -> None:
            task = _task(connection, identifier)
            status = cast(str, task["status"])
            if status in (
                TaskStatus.CANCEL_REQUESTED.value,
                TaskStatus.CANCELLED.value,
            ):
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
                    _raise_state_conflict(identifier, "request_cancel", status)
                attempt = attempts[0]
                if task["worker_id"] is None or (
                    task["worker_id"] != attempt["worker_id"]
                ):
                    _raise_state_conflict(identifier, "request_cancel", status)
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
                _raise_state_conflict(identifier, "request_cancel", status)

            _add_audit(
                connection,
                experiment_id=cast(str | None, task["experiment_id"]),
                task_id=identifier,
                event_type=event_type,
                actor=subject,
                details={"old_status": status, "status": new_status},
                created_at=cancelled_at,
            )

        self._immediate(write)

    def finish(
        self, attempt_id: str, worker_id: str, outcome: TaskOutcome
    ) -> None:
        """Atomically finish one owner-fenced active attempt."""
        attempt_identifier = _bounded_identity(attempt_id, "attempt_id", 36)
        worker = _bounded_identity(worker_id, "worker_id", 128)
        if not isinstance(outcome, TaskOutcome):
            raise TypeError("outcome must be a TaskOutcome")
        completed_at = self._time()
        timestamp = _timestamp(completed_at)
        error_json = (
            _mapping_json_text(outcome.error, "error", MAX_ERROR_BYTES)
            if outcome.error is not None
            else None
        )

        def write(connection: Connection) -> None:
            attempt, task = _attempt_and_task(connection, attempt_identifier)
            _require_owner(attempt, task, worker, attempt_identifier)
            attempt_status = cast(str, attempt["status"])
            task_status = cast(str, task["status"])
            if attempt_status in _TERMINAL_STATUSES:
                if (
                    attempt_status == task_status == outcome.status.value
                    and attempt["error_json"] == task["error_json"] == error_json
                ):
                    return
                _raise_state_conflict(
                    cast(str, task["id"]), "finish", task_status
                )
            _require_active_pair(attempt, task, "finish")
            if (
                task_status == TaskStatus.CANCEL_REQUESTED.value
                and outcome.status is not TaskStatus.CANCELLED
            ):
                _raise_state_conflict(
                    cast(str, task["id"]), "finish", task_status
                )
            progress = _parse_progress(cast(str, attempt["progress_json"]))
            progress_json = _progress_json(progress)
            connection.execute(
                update(TaskAttemptORM)
                .where(TaskAttemptORM.id == attempt_identifier)
                .values(
                    status=outcome.status.value,
                    completed_at=timestamp,
                    progress_json=progress_json,
                    error_json=error_json,
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
                )
            )
            _add_audit(
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
        """Mark strictly stale active task/attempt pairs without re-enqueueing."""
        if not isinstance(stale_after, timedelta):
            raise TypeError("stale_after must be a timedelta")
        if stale_after <= timedelta(0):
            raise ValueError("stale_after must be positive")
        orphaned_at = self._time(now)
        timestamp = _timestamp(orphaned_at)
        error: dict[str, JsonValue] = {
            "code": "TASK_ORPHANED",
            "message": "task heartbeat exceeded the stale threshold",
            "stale_after_seconds": stale_after.total_seconds(),
        }
        error_json = _mapping_json_text(error, "error", MAX_ERROR_BYTES)
        candidate_ids = self._orphan_candidate_ids(orphaned_at, stale_after)
        count = 0
        for task_id in candidate_ids:

            def write(connection: Connection, *, identifier: str = task_id) -> bool:
                task = connection.execute(
                    select(TaskORM.__table__).where(TaskORM.id == identifier)
                ).mappings().one_or_none()
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
                    _raise_state_conflict(
                        identifier, "mark_orphans", cast(str, task["status"])
                    )
                attempt = attempts[0]
                _require_active_pair(attempt, task, "mark_orphans")
                heartbeats = _active_heartbeats(task, attempt)
                if not heartbeats:
                    _raise_state_conflict(
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
                _add_audit(
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
            if (
                row["attempt_id"] is None
                or row["attempt_status"] != row["task_status"]
            ):
                candidates.append(task_id)
                continue
            heartbeats = [
                _parse_timestamp(cast(str, value))
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
        """Explicitly reset one failed, cancelled, or orphaned task for a new claim."""
        identifier = _bounded_identity(task_id, "task_id", 36)
        subject = _bounded_identity(actor, "actor", 128)
        request = _optional_identity(request_id, "request_id", 128)
        retried_at = self._time()
        available = self._time(available_at) if available_at is not None else retried_at
        timestamp = _timestamp(retried_at)
        progress_json = _progress_json(DEFAULT_PROGRESS)

        def write(connection: Connection) -> str:
            task = _task(connection, identifier)
            status = cast(str, task["status"])
            if status not in _RETRYABLE_STATUSES:
                _raise_state_conflict(identifier, "retry", status)
            active_attempt = connection.scalar(
                select(TaskAttemptORM.id).where(
                    TaskAttemptORM.task_id == identifier,
                    TaskAttemptORM.status.in_(_EXECUTION_STATUSES),
                )
            )
            if active_attempt is not None:
                _raise_state_conflict(identifier, "retry", status)
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
                    _raise_idempotency_conflict(
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
                    progress_json=progress_json,
                    available_at=_timestamp(available),
                    updated_at=timestamp,
                )
            )
            _add_audit(
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

    def _time(self, supplied: datetime | None = None) -> datetime:
        return _utc(supplied if supplied is not None else self._clock(), "timestamp")

    def _immediate(self, write: Callable[[Connection], _T]) -> _T:
        """Run one short write with an explicit SQLite BEGIN IMMEDIATE."""
        for retry_no in range(len(self._lock_retry_delays) + 1):
            try:
                return self._immediate_once(write)
            except OperationalError as error:
                if not _is_sqlite_lock_error(error):
                    raise
                if retry_no == len(self._lock_retry_delays):
                    _raise_queue_busy(len(self._lock_retry_delays))
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


def _task(connection: Connection, task_id: str) -> RowMapping:
    row = connection.execute(
        select(TaskORM.__table__).where(TaskORM.id == task_id)
    ).mappings().one_or_none()
    if row is None:
        _raise_not_found(
            "TASK_NOT_FOUND",
            f"task does not exist: {task_id}",
            {"task_id": task_id},
        )
    return row


def _attempt_and_task(
    connection: Connection, attempt_id: str
) -> tuple[RowMapping, RowMapping]:
    attempt = connection.execute(
        select(TaskAttemptORM.__table__).where(TaskAttemptORM.id == attempt_id)
    ).mappings().one_or_none()
    if attempt is None:
        _raise_not_found(
            "TASK_ATTEMPT_NOT_FOUND",
            f"task attempt does not exist: {attempt_id}",
            {"attempt_id": attempt_id},
        )
    task = _task(connection, cast(str, attempt["task_id"]))
    return attempt, task


def _require_owner(
    attempt: RowMapping | Mapping[str, object],
    task: RowMapping | Mapping[str, object],
    worker_id: str,
    attempt_id: str,
) -> None:
    if attempt["worker_id"] != worker_id or task["worker_id"] != worker_id:
        _raise_conflict(
            "TASK_OWNERSHIP_CONFLICT",
            "task attempt is owned by another worker",
            {
                "attempt_id": attempt_id,
                "task_id": cast(str, task["id"]),
                "worker_id": worker_id,
                "owner_id": cast(str | None, attempt["worker_id"]),
            },
        )


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
        _raise_state_conflict(cast(str, task["id"]), operation, task_status)


def _active_heartbeats(
    task: RowMapping, attempt: RowMapping
) -> list[datetime]:
    return [
        _parse_timestamp(cast(str, value))
        for value in (task["heartbeat_at"], attempt["heartbeat_at"])
        if value is not None
    ]


def _mapping_json_text(
    value: Mapping[str, JsonValue] | None,
    label: str,
    limit: int,
) -> str:
    if value is None or not isinstance(value, Mapping):
        raise TypeError(f"{label} must be a mapping")
    _validate_json_keys(value, label)
    encoded = canonical_json_bytes(_plain_json(value))
    if len(encoded) > limit:
        raise ValueError(f"{label} JSON exceeds {limit} bytes")
    parsed = json.loads(encoded)
    if not isinstance(parsed, dict):
        raise TypeError(f"{label} must be a JSON object")
    return encoded.decode("utf-8")


def _plain_json(value: JsonValue) -> JsonValue:
    if isinstance(value, Mapping):
        return {key: _plain_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain_json(item) for item in value]
    return value


def _validate_json_keys(value: JsonValue, label: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{label} JSON object keys must be strings")
            _validate_json_keys(item, label)
    elif isinstance(value, list):
        for item in value:
            _validate_json_keys(item, label)


def _parse_json_object(value: str, label: str) -> dict[str, JsonValue]:
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError) as error:
        raise ValueError(f"persisted {label} must be valid JSON") from error
    if not isinstance(parsed, dict):
        raise TypeError(f"persisted {label} must be a JSON object")
    canonical_json_bytes(cast(JsonValue, parsed))
    return cast(dict[str, JsonValue], parsed)


def _progress_json(progress: TaskProgress) -> str:
    return canonical_json_bytes(cast(JsonValue, progress.model_dump())).decode("utf-8")


def _parse_progress(value: str) -> TaskProgress:
    parsed = _parse_json_object(value, "progress")
    if not parsed:
        return DEFAULT_PROGRESS
    return TaskProgress.model_validate(parsed)


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
    details_json = _mapping_json_text(details, "audit details", MAX_AUDIT_BYTES)
    connection.execute(
        insert(AuditEventORM).values(
            experiment_id=experiment_id,
            task_id=task_id,
            event_type=event_type,
            actor=actor,
            details_json=details_json,
            created_at=_timestamp(created_at),
        )
    )


def _task_record(row: RowMapping) -> TaskRecord:
    progress = cast(
        dict[str, JsonValue],
        _parse_progress(cast(str, row["progress_json"])).model_dump(mode="json"),
    )
    error = (
        _parse_json_object(cast(str, row["error_json"]), "error")
        if row["error_json"] is not None
        else None
    )
    return TaskRecord(
        id=cast(str, row["id"]),
        experiment_id=cast(str | None, row["experiment_id"]),
        task_type=cast(str, row["task_type"]),
        payload=_parse_json_object(cast(str, row["payload_json"]), "payload"),
        status=TaskStatus(cast(str, row["status"])),
        priority=cast(int, row["priority"]),
        progress=progress,
        created_at=_parse_timestamp(cast(str, row["created_at"])),
        available_at=_parse_timestamp(cast(str, row["available_at"])),
        updated_at=_parse_timestamp(cast(str, row["updated_at"])),
        heartbeat_at=(
            _parse_timestamp(cast(str, row["heartbeat_at"]))
            if row["heartbeat_at"] is not None
            else None
        ),
        completed_at=(
            _parse_timestamp(cast(str, row["completed_at"]))
            if row["completed_at"] is not None
            else None
        ),
        idempotency_key=cast(str | None, row["idempotency_key"]),
        worker_id=cast(str | None, row["worker_id"]),
        locked_at=(
            _parse_timestamp(cast(str, row["locked_at"]))
            if row["locked_at"] is not None
            else None
        ),
        error=error,
    )


def _bounded_identity(value: str, label: str, limit: int) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a string")
    if not value.strip():
        raise ValueError(f"{label} must not be empty")
    if len(value) > limit:
        raise ValueError(f"{label} exceeds {limit} characters")
    return value


def _optional_identity(
    value: str | None, label: str, limit: int
) -> str | None:
    return None if value is None else _bounded_identity(value, label, limit)


def _sha256(value: str, label: str) -> str:
    normalized = _bounded_identity(value, label, 64)
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{label} must be a lowercase SHA-256 hex digest")
    return normalized


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{label} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _utc(value, "timestamp").isoformat()


def _parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as error:
        raise ValueError("persisted timestamp must be ISO-8601") from error
    return _utc(parsed, "persisted timestamp")


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


def _raise_state_conflict(task_id: str, operation: str, status: str) -> Never:
    _raise_conflict(
        "TASK_STATE_CONFLICT",
        f"task {task_id} cannot perform {operation} from {status}",
        {"task_id": task_id, "operation": operation, "status": status},
    )


def _raise_idempotency_conflict(
    task_id: str,
    task_type: str,
    experiment_id: str | None,
    idempotency_key: str,
) -> Never:
    _raise_conflict(
        "TASK_IDEMPOTENCY_CONFLICT",
        "active idempotency namespace has a different canonical payload",
        {
            "task_id": task_id,
            "task_type": task_type,
            "experiment_id": experiment_id,
            "idempotency_key": idempotency_key,
        },
    )


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
