"""Validated domain DTOs for persisted strategy experiments."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from quant_core.data.contracts import JsonValue, canonical_json_bytes

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class ExperimentStatus(StrEnum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ResearchMark(StrEnum):
    UNREVIEWED = "UNREVIEWED"
    BASELINE = "BASELINE"
    CANDIDATE = "CANDIDATE"
    DISCARDED = "DISCARDED"


class _ExperimentModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ExperimentSpec(_ExperimentModel):
    strategy_id: str
    strategy_version: str
    config: dict[str, JsonValue]
    config_hash: str
    snapshot_id: str | None = None
    snapshot_manifest_hash: str
    source_tree_hash: str | None = None
    git_commit_hash: str | None = None
    lockfile_hash: str
    rulebook_version: str
    fingerprint: str
    created_at: datetime

    @field_validator(
        "config_hash",
        "snapshot_manifest_hash",
        "source_tree_hash",
        "git_commit_hash",
        "lockfile_hash",
        "fingerprint",
    )
    @classmethod
    def validate_hash(cls, value: str | None, info: object) -> str | None:
        if value is not None and not _SHA256.fullmatch(value):
            field_name = getattr(info, "field_name", "hash")
            raise ValueError(f"{field_name} must be a lowercase SHA-256 hex digest")
        return value

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at")

    @field_validator("config")
    @classmethod
    def validate_config(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        canonical_json_bytes(value)
        return value

    @model_validator(mode="after")
    def validate_metadata(self) -> ExperimentSpec:
        for field, value in (
            ("strategy_id", self.strategy_id),
            ("strategy_version", self.strategy_version),
            ("rulebook_version", self.rulebook_version),
        ):
            if not value:
                raise ValueError(f"{field} must not be empty")
        if self.source_tree_hash is None and self.git_commit_hash is None:
            raise ValueError("source_tree_hash or git_commit_hash is required")
        canonical_hash = hashlib.sha256(canonical_json_bytes(self.config)).hexdigest()
        if self.config_hash != canonical_hash:
            raise ValueError("config_hash must match canonical config")
        return self


class ExperimentRecord(_ExperimentModel):
    id: str
    strategy_id: str
    strategy_version: str
    config: dict[str, JsonValue]
    config_hash: str
    snapshot_id: str | None
    snapshot_manifest_hash: str
    source_tree_hash: str | None
    git_commit_hash: str | None
    lockfile_hash: str
    rulebook_version: str
    fingerprint: str
    status: ExperimentStatus
    research_mark: ResearchMark
    created_at: datetime
    queued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None


class ExperimentTag(_ExperimentModel):
    experiment_id: str
    tag: str


class ExperimentMetric(_ExperimentModel):
    experiment_id: str
    name: str
    value: float
    unit: str | None = None
    created_at: datetime


class ExperimentArtifact(_ExperimentModel):
    experiment_id: str
    name: str
    artifact_type: str
    path: str
    content_hash: str
    metadata: dict[str, JsonValue]
    created_at: datetime

    @field_validator("content_hash")
    @classmethod
    def validate_content_hash(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("content_hash must be a lowercase SHA-256 hex digest")
        return value

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        canonical_json_bytes(value)
        return value

    @field_validator("created_at")
    @classmethod
    def normalize_created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at")


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)
