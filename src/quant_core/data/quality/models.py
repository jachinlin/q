"""Immutable input and result models for quality evaluation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType

from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.domain.enums import DatasetKind, Severity
from quant_core.domain.identifiers import DatasetVersionId


@dataclass(frozen=True, slots=True)
class QualityIssue:
    """One actionable quality-rule violation."""

    rule_id: str
    severity: Severity
    dataset: DatasetKind
    scope: Mapping[str, JsonValue]
    actual: JsonValue
    threshold: JsonValue
    message: str
    remediation: str

    def __post_init__(self) -> None:
        if not self.rule_id or not self.message or not self.remediation:
            raise ValueError("quality issue text fields must not be empty")
        normalized_scope = dict(self.scope)
        canonical_json_bytes(normalized_scope)
        canonical_json_bytes(self.actual)
        canonical_json_bytes(self.threshold)
        object.__setattr__(self, "scope", MappingProxyType(normalized_scope))


@dataclass(frozen=True, slots=True)
class QualityRunSpec:
    """A quality run and the exact immutable versions it evaluated."""

    dataset_versions: Mapping[str, DatasetVersionId]
    started_at: datetime
    completed_at: datetime | None
    issues: tuple[QualityIssue, ...]

    def __post_init__(self) -> None:
        if not self.dataset_versions:
            raise ValueError("quality run must bind at least one dataset version")
        started = _utc(self.started_at, "started_at")
        completed = (
            _utc(self.completed_at, "completed_at")
            if self.completed_at is not None
            else None
        )
        if completed is not None and completed < started:
            raise ValueError("completed_at must not precede started_at")
        object.__setattr__(self, "started_at", started)
        object.__setattr__(self, "completed_at", completed)


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)
