"""Immutable input and result models for quality evaluation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType

from quant_core.data.contracts import JsonScalar, JsonValue, canonical_json_bytes
from quant_core.domain.enums import DatasetKind, Severity
from quant_core.domain.identifiers import DatasetVersionId

type FrozenJsonValue = (
    JsonScalar | tuple["FrozenJsonValue", ...] | Mapping[str, "FrozenJsonValue"]
)
type QualityJsonValue = (
    JsonScalar
    | list["QualityJsonValue"]
    | tuple["QualityJsonValue", ...]
    | Mapping[str, "QualityJsonValue"]
)


@dataclass(frozen=True, slots=True)
class QualityIssue:
    """One actionable quality-rule violation."""

    rule_id: str
    severity: Severity
    dataset: DatasetKind
    scope: Mapping[str, QualityJsonValue]
    actual: QualityJsonValue
    threshold: QualityJsonValue
    message: str
    remediation: str

    def __post_init__(self) -> None:
        if not self.rule_id or not self.message or not self.remediation:
            raise ValueError("quality issue text fields must not be empty")
        frozen_scope = freeze_json(dict(self.scope))
        frozen_actual = freeze_json(self.actual)
        frozen_threshold = freeze_json(self.threshold)
        canonical_json_bytes(thaw_json(frozen_scope))
        canonical_json_bytes(thaw_json(frozen_actual))
        canonical_json_bytes(thaw_json(frozen_threshold))
        if not isinstance(frozen_scope, Mapping):
            raise TypeError("quality issue scope must be a mapping")
        object.__setattr__(self, "scope", frozen_scope)
        object.__setattr__(self, "actual", frozen_actual)
        object.__setattr__(self, "threshold", frozen_threshold)


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
        object.__setattr__(
            self, "dataset_versions", MappingProxyType(dict(self.dataset_versions))
        )
        object.__setattr__(self, "issues", tuple(self.issues))


def freeze_json(value: object) -> FrozenJsonValue:
    """Recursively copy JSON-like input into immutable containers."""
    if isinstance(value, Mapping):
        return MappingProxyType(
            {str(key): freeze_json(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(freeze_json(item) for item in value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def thaw_json(value: object) -> JsonValue:
    """Return ordinary JSON containers for canonical persistence."""
    if isinstance(value, Mapping):
        return {str(key): thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [thaw_json(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)
