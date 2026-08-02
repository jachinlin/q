"""Validated domain DTOs for durable background tasks and attempts."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

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
    experiment_id: str
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
        if not self.experiment_id or not self.task_type:
            raise ValueError("experiment_id and task_type must not be empty")
        return self


class TaskRecord(_TaskModel):
    id: str
    experiment_id: str
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


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)
