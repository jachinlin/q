"""Validated domain DTOs for durable background tasks and attempts."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from quant_core.data.contracts import JsonValue, canonical_json_bytes


class TaskStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    ORPHANED = "ORPHANED"


class _TaskModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class TaskSpec(_TaskModel):
    experiment_id: str | None = None
    task_type: str
    payload: dict[str, JsonValue]
    priority: int = 0
    created_at: datetime
    available_at: datetime

    @field_validator("created_at", "available_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime, info: object) -> datetime:
        return _utc(value, getattr(info, "field_name", "timestamp"))

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        canonical_json_bytes(value)
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> TaskSpec:
        if self.experiment_id is not None and not self.experiment_id:
            raise ValueError("experiment_id must not be empty")
        if not self.task_type:
            raise ValueError("task_type must not be empty")
        return self


class TaskRecord(_TaskModel):
    id: str
    experiment_id: str | None
    task_type: str
    payload: dict[str, JsonValue]
    status: TaskStatus
    priority: int
    progress: dict[str, JsonValue]
    created_at: datetime
    available_at: datetime
    updated_at: datetime
    heartbeat_at: datetime | None
    completed_at: datetime | None
    idempotency_key: str | None = None
    worker_id: str | None = None
    locked_at: datetime | None = None
    error: dict[str, JsonValue] | None = None


class TaskAttemptRecord(_TaskModel):
    id: str
    task_id: str
    attempt_no: int
    status: TaskStatus
    worker_id: str | None
    started_at: datetime
    heartbeat_at: datetime | None
    completed_at: datetime | None
    log_path: str | None
    progress: dict[str, JsonValue]
    error: dict[str, JsonValue] | None


class AuditEventSpec(_TaskModel):
    event_type: str
    details: dict[str, JsonValue]
    created_at: datetime
    experiment_id: str | None = None
    task_id: str | None = None
    actor: str | None = None

    @field_validator("details")
    @classmethod
    def validate_details(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        canonical_json_bytes(value)
        return value

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at")

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, value: str) -> str:
        if not value:
            raise ValueError("event_type must not be empty")
        return value


class AuditEventRecord(AuditEventSpec):
    id: int


class TaskProgress(_TaskModel):
    stage: str = Field(min_length=1, max_length=128)
    completed: int
    total: int
    message: str = Field(max_length=2048)

    @field_validator("stage")
    @classmethod
    def validate_stage(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("stage must not be empty")
        return value

    @model_validator(mode="after")
    def validate_bounds(self) -> TaskProgress:
        if self.total < 0:
            raise ValueError("total must be non-negative")
        if self.completed < 0 or self.completed > self.total:
            raise ValueError("completed must satisfy 0 <= completed <= total")
        return self


class TaskOutcome(_TaskModel):
    status: TaskStatus
    error: dict[str, JsonValue] | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> TaskOutcome:
        allowed = {
            TaskStatus.SUCCEEDED,
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }
        if self.status not in allowed:
            raise ValueError("outcome status must be SUCCEEDED, FAILED, or CANCELLED")
        if self.status is TaskStatus.FAILED:
            if self.error is None:
                raise ValueError("FAILED outcome requires an error")
            encoded = canonical_json_bytes(self.error)
            if len(encoded) > 65_536:
                raise ValueError("error JSON exceeds 65536 bytes")
        elif self.error is not None:
            raise ValueError("SUCCEEDED and CANCELLED outcomes must not include error")
        return self


class ClaimedTask(_TaskModel):
    id: str
    attempt_id: str
    attempt_no: int
    experiment_id: str | None
    task_type: str
    payload: dict[str, JsonValue]
    priority: int
    worker_id: str
    progress: TaskProgress
    claimed_at: datetime

    @field_validator("claimed_at")
    @classmethod
    def normalize_claimed_at(cls, value: datetime) -> datetime:
        return _utc(value, "claimed_at")

    @field_validator("payload")
    @classmethod
    def validate_claimed_payload(
        cls, value: dict[str, JsonValue]
    ) -> dict[str, JsonValue]:
        encoded = canonical_json_bytes(value)
        if len(encoded) > 1_048_576:
            raise ValueError("payload JSON exceeds 1048576 bytes")
        return value

    @model_validator(mode="after")
    def validate_claim_identity(self) -> ClaimedTask:
        if not self.id or not self.attempt_id or not self.task_type or not self.worker_id:
            raise ValueError("claim identity fields must not be empty")
        if self.attempt_no <= 0:
            raise ValueError("attempt_no must be positive")
        if self.experiment_id is not None and not self.experiment_id:
            raise ValueError("experiment_id must not be empty")
        return self


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)
