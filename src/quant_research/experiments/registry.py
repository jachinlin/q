"""提供实验与实验注册相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import hashlib
import json
import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from math import isfinite
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from sqlalchemy import Engine, delete, func, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

from quant_research.backtest.artifacts import (
    ExperimentArtifactEntry,
    ExperimentArtifactPublication,
    validate_experiment_artifacts,
)
from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.experiments.fingerprint import (
    ExperimentFingerprintInput,
    compute_fingerprint,
)
from quant_research.experiments.models import (
    ExperimentSpec,
    ExperimentStatus,
    ResearchMark,
)
from quant_research.infrastructure.persistence.orm import (
    AuditEventORM,
    DataCatalogStateORM,
    ExperimentArtifactORM,
    ExperimentMetricORM,
    ExperimentORM,
    ExperimentTagORM,
)

ALLOWED_TRANSITIONS: frozenset[tuple[ExperimentStatus, ExperimentStatus]] = frozenset(
    {
        (ExperimentStatus.CREATED, ExperimentStatus.QUEUED),
        (ExperimentStatus.QUEUED, ExperimentStatus.RUNNING),
        (ExperimentStatus.QUEUED, ExperimentStatus.CANCELLED),
        (ExperimentStatus.RUNNING, ExperimentStatus.SUCCEEDED),
        (ExperimentStatus.RUNNING, ExperimentStatus.FAILED),
        (ExperimentStatus.RUNNING, ExperimentStatus.CANCELLED),
    }
)

_REASON_CONTEXT_ALLOWLIST = frozenset(
    {
        "actual",
        "attempt",
        "connection",
        "dataset",
        "error_code",
        "expected",
        "experiment_id",
        "host",
        "operation",
        "provider",
        "stage",
        "status",
        "target",
    }
)
_REASON_SECRET_KEY_MARKERS = (
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "credential",
    "password",
    "secret",
    "token",
)
_REASON_SECRET_CONTAINER_KEYS = frozenset({"env", "environ", "environment"})


class InvalidTransition(QuantError):
    """表示 ``InvalidTransition`` 对应的领域异常。

    入参：
        返回完成字段规范化和不变量校验的对象。
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    The requested lifecycle edge is absent from the authoritative matrix.
    """

    def __init__(self, expected: ExperimentStatus, target: ExperimentStatus) -> None:
        super().__init__(
            ErrorDetail(
                code="EXPERIMENT_INVALID_TRANSITION",
                severity=Severity.SEVERE,
                message=f"experiment cannot transition from {expected} to {target}",
                context={"expected": expected.value, "target": target.value},
                remediation="request one of the documented lifecycle transitions",
                retryable=False,
            )
        )


class StateConflict(QuantError):
    """表示 ``StateConflict`` 对应的领域异常。

    入参：
        experiment_id：目标实验标识，类型为 ``str``。
        返回完成字段规范化和不变量校验的对象。
        返回完成字段规范化和不变量校验的对象。
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    A database compare-and-swap observed a stale experiment state.
    """

    def __init__(
        self,
        experiment_id: str,
        expected: ExperimentStatus,
        target: ExperimentStatus,
        actual: ExperimentStatus | None,
    ) -> None:
        super().__init__(
            ErrorDetail(
                code="EXPERIMENT_STATE_CONFLICT",
                severity=Severity.SEVERE,
                message=f"experiment {experiment_id} state changed before transition",
                context={
                    "experiment_id": experiment_id,
                    "expected": expected.value,
                    "target": target.value,
                    "actual": actual.value if actual is not None else None,
                },
                remediation="reload the experiment before requesting another transition",
                retryable=False,
            )
        )


class ExperimentNotFound(QuantError):
    """表示 ``ExperimentNotFound`` 对应的领域异常。

    入参：
        experiment_id：目标实验标识，类型为 ``str``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    An experiment identity does not exist in the registry.
    """

    def __init__(self, experiment_id: str) -> None:
        super().__init__(
            ErrorDetail(
                code="EXPERIMENT_NOT_FOUND",
                severity=Severity.SEVERE,
                message=f"experiment does not exist: {experiment_id}",
                context={"experiment_id": experiment_id},
                remediation="use an experiment ID returned by the registry",
                retryable=False,
            )
        )


class ExperimentDeletionConflict(QuantError):
    """表示活动实验因 Worker 竞态风险而不能删除。

    入参：
        experiment_id：目标实验标识。
        status：阻止删除的当前活动状态。
    返回值：
        构造并返回结构化删除冲突异常。
    异常：
        无主动抛出的异常。
    """

    def __init__(self, experiment_id: str, status: ExperimentStatus) -> None:
        """创建可重试的删除冲突。

        入参：
            experiment_id：目标实验标识。
            status：阻止删除的当前活动状态。
        返回值：
            无；构造异常对象。
        异常：
            无主动抛出的异常。
        """
        super().__init__(
            ErrorDetail(
                code="EXPERIMENT_DELETE_CONFLICT",
                severity=Severity.SEVERE,
                message=f"active experiment cannot be deleted: {experiment_id}",
                context={"experiment_id": experiment_id, "status": status.value},
                remediation="wait for the experiment to finish or cancel its task first",
                retryable=True,
            )
        )


class ExperimentDeletionFailure(QuantError):
    """表示受控实验文件无法安全暂存、恢复或清理。

    入参：
        experiment_id：目标实验标识。
        operation：失败的受控清理阶段。
    返回值：
        构造并返回结构化文件清理异常。
    异常：
        无主动抛出的异常。
    """

    def __init__(self, experiment_id: str, operation: str) -> None:
        """创建不泄露本地路径的实验删除文件错误。

        入参：
            experiment_id：目标实验标识。
            operation：失败的受控清理阶段。
        返回值：
            无；构造异常对象。
        异常：
            无主动抛出的异常。
        """
        super().__init__(
            ErrorDetail(
                code="EXPERIMENT_DELETE_STORAGE_FAILED",
                severity=Severity.SEVERE,
                message=f"experiment resources could not be safely deleted: {experiment_id}",
                context={"experiment_id": experiment_id, "operation": operation},
                remediation="close processes using experiment files and retry the deletion",
                retryable=True,
            )
        )


class DuplicateResearchWarning(UserWarning):
    """表示实验流程中调用方需要识别的``duplicate``研究``warning``。

    入参：
        fingerprint：由策略、数据、源码、依赖锁和交易规则共同形成的研究指纹。
        返回完成字段规范化和不变量校验的对象。
        experiment_id：目标实验标识，类型为 ``str``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    A new experiment intentionally repeats a prior research fingerprint.
    """

    def __init__(
        self, fingerprint: str, existing_count: int, experiment_id: str
    ) -> None:
        self.fingerprint = fingerprint
        self.existing_count = existing_count
        self.experiment_id = experiment_id
        super().__init__(
            f"fingerprint {fingerprint} already has {existing_count} experiment(s); "
            f"created {experiment_id}"
        )


@dataclass(frozen=True, slots=True)
class TransitionTimestamps:
    """记录一次合法实验状态转换后应持久化的生命周期时间戳。

    入参：
        queued_at：记录入队时间``at``的带时区 UTC 时间戳。
        started_at：执行实际开始的 UTC 时间戳。
        completed_at：执行进入终态的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    queued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None


def validate_transition(
    expected: ExperimentStatus,
    target: ExperimentStatus,
    reason: ErrorDetail | None = None,
) -> None:
    """校验``transition``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        expected：``expected``。
        target：目标组合。
        reason：原因。
    返回值：
        无。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``InvalidTransition``、``TypeError``、``ValueError``。
    Validate one lifecycle edge independently of persistence.
    """
    if reason is not None and not isinstance(reason, ErrorDetail):
        raise TypeError("reason must be an ErrorDetail")
    if (expected, target) not in ALLOWED_TRANSITIONS:
        raise InvalidTransition(expected, target)
    if reason is not None and target not in {
        ExperimentStatus.FAILED,
        ExperimentStatus.CANCELLED,
    }:
        raise ValueError("reason is allowed only for FAILED or CANCELLED")


def transition_timestamps(
    expected: ExperimentStatus,
    target: ExperimentStatus,
    *,
    queued_at: datetime | None,
    started_at: datetime | None,
    completed_at: datetime | None,
    now: datetime,
) -> TransitionTimestamps:
    """校验并推进``timestamps``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        expected：``expected``。
        target：目标组合。
        queued_at：记录入队时间``at``的带时区 UTC 时间戳。
        started_at：执行实际开始的 UTC 时间戳。
        completed_at：执行进入终态的 UTC 时间戳。
        now：当前 UTC 时间。
    返回值：
        返回``timestamps``（``TransitionTimestamps``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Derive immutable UTC lifecycle timestamps for one validated edge.
    """
    validate_transition(expected, target)
    normalized_now = _RegistrySupport._utc(now, "now")
    normalized_queued = _RegistrySupport._optional_utc(queued_at, "queued_at")
    normalized_started = _RegistrySupport._optional_utc(started_at, "started_at")
    normalized_completed = _RegistrySupport._optional_utc(completed_at, "completed_at")
    if target is ExperimentStatus.QUEUED:
        normalized_queued = normalized_queued or normalized_now
    elif target is ExperimentStatus.RUNNING:
        if normalized_queued is None:
            raise ValueError("queued_at is required before RUNNING")
        normalized_started = normalized_started or normalized_now
    else:
        if expected is not ExperimentStatus.QUEUED and normalized_started is None:
            raise ValueError("started_at is required before a terminal state")
        normalized_completed = normalized_completed or normalized_now
    return TransitionTimestamps(
        normalized_queued,
        normalized_started,
        normalized_completed,
    )


class _RegistrySupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _optional_utc(value: datetime | None, field: str) -> datetime | None:
        return _RegistrySupport._utc(value, field) if value is not None else None

    @staticmethod
    def _utc(value: datetime, field: str) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field} must be timezone-aware")
        return value.astimezone(UTC)

    @staticmethod
    def _validate_publication(
        supplied: ExperimentArtifactPublication,
        validated: ExperimentArtifactPublication,
    ) -> None:
        if supplied.artifact_dir.resolve() != validated.artifact_dir.resolve():
            raise ValueError("publication artifact directory changed")
        if supplied.manifest_path.resolve() != validated.manifest_path.resolve():
            raise ValueError("publication manifest path changed")
        if supplied.entries != validated.entries:
            raise ValueError("publication entries do not match revalidation")
        if canonical_json_bytes(
            cast(JsonValue, supplied.manifest)
        ) != canonical_json_bytes(cast(JsonValue, validated.manifest)):
            raise ValueError("publication manifest does not match revalidation")

    @staticmethod
    def _validate_success_identity(
        experiment_id: str,
        experiment: _ImmutableExperiment,
        publication: ExperimentArtifactPublication,
    ) -> None:
        manifest = publication.manifest
        if manifest.get("experiment_id") != experiment_id:
            raise ValueError("artifact manifest experiment ID does not match registry")
        if publication.artifact_dir.name != f"experiment_id={experiment_id}":
            raise ValueError(
                "artifact publication directory identity does not match registry"
            )
        if manifest.get("strategy_id") != experiment.strategy_id:
            raise ValueError(
                "artifact manifest strategy identity does not match registry"
            )
        if (
            experiment.catalog_data_hash is None
            or experiment.data_hash != experiment.catalog_data_hash
        ):
            raise ValueError(
                "experiment data hash does not match current canonical catalog"
            )
        if manifest.get("data_hash") != experiment.data_hash:
            raise ValueError("artifact manifest data hash does not match registry")
        if manifest.get("rulebook_hash") != experiment.rulebook_hash:
            raise ValueError(
                "artifact manifest RuleBook identity does not match registry"
            )
        environment = _RegistrySupport._read_json_object(
            publication.artifact_dir / "environment.json", "environment.json"
        )
        source_hash = environment.get("source_hash")
        if not isinstance(source_hash, str):
            raise TypeError("artifact source hash identity must be a string")
        if environment.get("lockfile_hash") != experiment.lockfile_hash:
            raise ValueError("artifact lockfile identity does not match registry")
        if experiment.source_tree_hash is not None and (
            environment.get("source_tree_hash") != experiment.source_tree_hash
        ):
            raise ValueError("artifact source tree identity does not match registry")
        if experiment.git_commit_hash is not None and (
            environment.get("git_commit") != experiment.git_commit_hash
        ):
            raise ValueError("artifact Git identity does not match registry")
        expected_fingerprint = compute_fingerprint(
            ExperimentFingerprintInput(
                strategy_id=experiment.strategy_id,
                resolved_config=experiment.config,
                data_hash=experiment.data_hash,
                source_hash=source_hash,
                lockfile_hash=experiment.lockfile_hash,
                rulebook_hash=experiment.rulebook_hash,
            )
        )
        if experiment.fingerprint != expected_fingerprint:
            raise ValueError("experiment fingerprint does not match validated identity")

    @staticmethod
    def _validated_metrics(
        artifact_dir: Path, metrics: Mapping[str, float]
    ) -> tuple[tuple[str, float], ...]:
        if not isinstance(metrics, Mapping):
            raise TypeError("metrics must be a mapping")
        payload = _RegistrySupport._read_json_object(
            artifact_dir / "metrics.json", "metrics.json"
        )
        normalized: list[tuple[str, float]] = []
        for name, value in metrics.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError("metric names must be nonempty strings")
            if name != name.strip() or len(name) > 128:
                raise ValueError(
                    "metric names must be trimmed and at most 128 characters"
                )
            if type(value) is not float:
                raise TypeError("metric values must be float and not bool")
            if not isfinite(value):
                raise ValueError("metric values must be finite float scalars")
            bundle_value = payload.get(name)
            if (
                isinstance(bundle_value, bool)
                or not isinstance(bundle_value, (int, float))
                or not isfinite(bundle_value)
                or value != bundle_value
            ):
                raise ValueError(f"metric {name} does not exactly match metrics.json")
            normalized.append((name, value))
        return tuple(sorted(normalized))

    @staticmethod
    def _artifact_rows(
        experiment_id: str,
        publication: ExperimentArtifactPublication,
        created_at: datetime,
    ) -> list[ExperimentArtifactORM]:
        rows = [
            _RegistrySupport._artifact_row(
                experiment_id,
                name,
                publication.artifact_dir / entry.path,
                entry.sha256,
                entry,
                created_at,
            )
            for name, entry in sorted(publication.entries.items())
        ]
        manifest_bytes = publication.manifest_path.read_bytes()
        if manifest_bytes != canonical_json_bytes(
            cast(JsonValue, publication.manifest)
        ):
            raise ValueError("validated manifest marker changed before registration")
        rows.append(
            ExperimentArtifactORM(
                experiment_id=experiment_id,
                name="manifest.json",
                artifact_type="manifest",
                path=str(publication.manifest_path.resolve()),
                content_hash=hashlib.sha256(manifest_bytes).hexdigest(),
                metadata_json=canonical_json_bytes(
                    {
                        "schema": "quant.experiment.manifest.v1",
                        "size_bytes": len(manifest_bytes),
                    }
                ).decode("utf-8"),
                created_at=_RegistrySupport._timestamp(created_at),
            )
        )
        return rows

    @staticmethod
    def _artifact_row(
        experiment_id: str,
        name: str,
        path: Path,
        content_hash: str,
        entry: ExperimentArtifactEntry,
        created_at: datetime,
    ) -> ExperimentArtifactORM:
        metadata: dict[str, JsonValue] = {"size_bytes": entry.size_bytes}
        if entry.schema is not None:
            metadata["schema"] = entry.schema
        if entry.row_count is not None:
            metadata["row_count"] = entry.row_count
        return ExperimentArtifactORM(
            experiment_id=experiment_id,
            name=name,
            artifact_type=Path(name).suffix.removeprefix(".") or "file",
            path=str(path.resolve()),
            content_hash=content_hash,
            metadata_json=canonical_json_bytes(metadata).decode("utf-8"),
            created_at=_RegistrySupport._timestamp(created_at),
        )

    @staticmethod
    def _latest_note(session: Session, experiment_id: str) -> str | None:
        raw = session.scalar(
            select(AuditEventORM.details_json)
            .where(
                AuditEventORM.experiment_id == experiment_id,
                AuditEventORM.event_type == "EXPERIMENT_RESEARCH_UPDATED",
            )
            .order_by(AuditEventORM.created_at.desc(), AuditEventORM.id.desc())
            .limit(1)
        )
        if raw is None:
            return None
        parsed = json.loads(raw)
        value = parsed.get("new_value", {}).get("note")
        return value if isinstance(value, str) else None

    @staticmethod
    def _reason_payload(reason: ErrorDetail) -> dict[str, JsonValue]:
        return {
            "code": _RegistrySupport._bounded_text(reason.code, "reason code", 128),
            "severity": reason.severity.value,
            "context": _RegistrySupport._safe_mapping(reason.context),
            "retryable": reason.retryable,
        }

    @staticmethod
    def _safe_mapping(value: Mapping[str, object]) -> dict[str, JsonValue]:
        return _RegistrySupport._safe_mapping_at_depth(value, depth=0)

    @staticmethod
    def _safe_mapping_at_depth(
        value: Mapping[str, object], *, depth: int
    ) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {}
        for index, (key, item) in enumerate(sorted(value.items())):
            if index >= 50:
                break
            name = _RegistrySupport._bounded_text(key, "reason context key", 128)
            normalized_name = name.casefold().replace("-", "_")
            if normalized_name in _REASON_SECRET_CONTAINER_KEYS or any(
                marker in normalized_name for marker in _REASON_SECRET_KEY_MARKERS
            ):
                result[name] = "[REDACTED]"
            elif normalized_name in _REASON_CONTEXT_ALLOWLIST:
                result[name] = _RegistrySupport._safe_value(item, depth=depth)
        return result

    @staticmethod
    def _safe_value(value: object, *, depth: int) -> JsonValue:
        if depth >= 4:
            return "[TRUNCATED]"
        if value is None or isinstance(value, (str, bool, int)):
            return value if not isinstance(value, str) else value[:2048]
        if isinstance(value, float):
            return value if isfinite(value) else "[NONFINITE]"
        if isinstance(value, Mapping):
            return _RegistrySupport._safe_mapping_at_depth(
                {str(key): item for key, item in list(value.items())[:50]},
                depth=depth + 1,
            )
        if isinstance(value, (list, tuple)):
            return [
                _RegistrySupport._safe_value(item, depth=depth + 1)
                for item in value[:50]
            ]
        return f"[UNSERIALIZABLE:{type(value).__name__}]"

    @staticmethod
    def _details(
        subject: str,
        action: str,
        experiment_id: str,
        old_value: Mapping[str, JsonValue],
        new_value: Mapping[str, JsonValue],
        request_id: str,
        **extra: JsonValue,
    ) -> dict[str, JsonValue]:
        return {
            "subject": subject,
            "action": action,
            "object": {"type": "experiment", "id": experiment_id},
            "old_value": dict(old_value),
            "new_value": dict(new_value),
            "request_id": request_id,
            **extra,
        }

    @staticmethod
    def _add_audit(
        session: Session,
        experiment_id: str | None,
        event_type: str,
        actor: str,
        details: Mapping[str, JsonValue],
        created_at: datetime,
    ) -> None:
        session.add(
            AuditEventORM(
                experiment_id=experiment_id,
                task_id=None,
                event_type=event_type,
                actor=actor,
                details_json=canonical_json_bytes(details).decode("utf-8"),
                created_at=_RegistrySupport._timestamp(created_at),
            )
        )

    @staticmethod
    def _read_json_object(path: Path, label: str) -> dict[str, Any]:
        try:
            value = json.loads(path.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"{label} must be valid UTF-8 JSON") from error
        if not isinstance(value, dict):
            raise TypeError(f"{label} must be a JSON object")
        return cast(dict[str, Any], value)

    @staticmethod
    def _tags(values: Sequence[str]) -> tuple[str, ...]:
        if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
            raise TypeError("tags must be a sequence of strings")
        normalized: set[str] = set()
        for value in values:
            if not isinstance(value, str):
                raise TypeError("tags must contain strings")
            tag = value.strip()
            if not tag:
                raise ValueError("tag must not be empty")
            try:
                encoded = tag.encode("utf-8")
            except UnicodeEncodeError as error:
                raise ValueError("tag must be valid UTF-8") from error
            if len(tag) > 64 or len(encoded) > 256:
                raise ValueError("tag is too long")
            normalized.add(tag)
        return tuple(sorted(normalized))

    @staticmethod
    def _note(value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("note must be text")
        try:
            encoded = value.encode("utf-8")
        except UnicodeEncodeError as error:
            raise ValueError("note must be valid UTF-8") from error
        if len(encoded) > 16_384:
            raise ValueError("note exceeds the 16384-byte limit")
        return value

    @staticmethod
    def _subject(value: str) -> str:
        return _RegistrySupport._bounded_text(value, "actor", 128)

    @staticmethod
    def _request_id(value: str | None) -> str:
        return (
            str(uuid4())
            if value is None
            else _RegistrySupport._bounded_text(value, "request_id", 256)
        )

    @staticmethod
    def _experiment_id(value: str) -> str:
        return _RegistrySupport._bounded_text(value, "experiment_id", 128)

    @staticmethod
    def _bounded_text(value: str, field: str, limit: int) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{field} must be a string")
        if not value.strip():
            raise ValueError(f"{field} must not be empty")
        if len(value) > limit:
            raise ValueError(f"{field} is too long")
        return value

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return _RegistrySupport._utc(value, "timestamp").isoformat()

    @staticmethod
    def _optional_timestamp(value: datetime | None) -> str | None:
        return _RegistrySupport._timestamp(value) if value is not None else None

    @staticmethod
    def _parse_optional_timestamp(value: str | None) -> datetime | None:
        if value is None:
            return None
        parsed = datetime.fromisoformat(value)
        return _RegistrySupport._utc(parsed, "persisted timestamp")


@dataclass(frozen=True, slots=True)
class _ImmutableExperiment:
    id: str
    config: dict[str, JsonValue]
    strategy_id: str
    data_hash: str
    catalog_data_hash: str | None
    source_tree_hash: str | None
    git_commit_hash: str | None
    lockfile_hash: str
    rulebook_hash: str
    fingerprint: str
    status: ExperimentStatus
    queued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None


class ExperimentRegistry:
    """登记并按稳定身份查询实验定义。

    入参：
        engine：引擎。
        clock：用于产生可复现 UTC 时间戳的可注入时钟。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ExperimentNotFound``、``StateConflict``、``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Own short experiment mutations without exposing ORM instances.
    """

    def __init__(
        self,
        engine: Engine,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._engine = engine
        self._clock = clock or (lambda: datetime.now(UTC))

    def create(
        self,
        config: ExperimentSpec,
        fingerprint: str,
        *,
        actor: str = "system",
        request_id: str | None = None,
        now: datetime | None = None,
    ) -> str:
        """创建并返回约定对象。

        入参：
            config：调用所用的配置对象，类型为 ``ExperimentSpec``。
            fingerprint：由策略、数据、源码、依赖锁和交易规则共同形成的研究指纹。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
            now：当前 UTC 时间。
        返回值：
            返回创建实验后的``create``（``str``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        Create a fresh experiment and atomically audit duplicate evidence.
        """
        if not isinstance(config, ExperimentSpec):
            raise TypeError("config must be an ExperimentSpec")
        if fingerprint != config.fingerprint:
            raise ValueError("fingerprint must equal config.fingerprint")
        config_bytes = canonical_json_bytes(config.config)
        if hashlib.sha256(config_bytes).hexdigest() != config.config_hash:
            raise ValueError("config_hash must match canonical config")
        identifier = str(uuid4())
        audit_time = self._time(now)
        subject = _RegistrySupport._subject(actor)
        request = _RegistrySupport._request_id(request_id)
        with Session(self._engine) as session, session.begin():
            session.add(
                ExperimentORM(
                    id=identifier,
                    strategy_id=config.strategy_id,
                    config_json=config_bytes.decode("utf-8"),
                    config_hash=config.config_hash,
                    data_hash=config.data_hash,
                    source_tree_hash=config.source_tree_hash,
                    git_commit_hash=config.git_commit_hash,
                    lockfile_hash=config.lockfile_hash,
                    rulebook_hash=config.rulebook_hash,
                    fingerprint=fingerprint,
                    status=ExperimentStatus.CREATED.value,
                    research_mark=ResearchMark.UNREVIEWED.value,
                    created_at=_RegistrySupport._timestamp(config.created_at),
                    queued_at=None,
                    started_at=None,
                    completed_at=None,
                )
            )
            # The insert reserves SQLite's writer before any fingerprint read.
            # Concurrent creators then serialize instead of upgrading deferred
            # read transactions, so the following count is linearizable.
            session.flush()
            duplicate_count = int(
                session.scalar(
                    select(func.count())
                    .select_from(ExperimentORM)
                    .where(
                        ExperimentORM.fingerprint == fingerprint,
                        ExperimentORM.id != identifier,
                    )
                )
                or 0
            )
            _RegistrySupport._add_audit(
                session,
                identifier,
                "EXPERIMENT_CREATED",
                subject,
                _RegistrySupport._details(
                    subject,
                    "create",
                    identifier,
                    {},
                    {
                        "fingerprint": fingerprint,
                        "status": ExperimentStatus.CREATED.value,
                    },
                    request,
                    duplicate_count=duplicate_count,
                ),
                audit_time,
            )
        if duplicate_count:
            warnings.warn(
                DuplicateResearchWarning(fingerprint, duplicate_count, identifier),
                stacklevel=2,
            )
        return identifier

    def transition(
        self,
        experiment_id: str,
        expected: ExperimentStatus,
        target: ExperimentStatus,
        reason: ErrorDetail | None = None,
        *,
        actor: str = "system",
        request_id: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """校验并推进实验。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
            expected：``expected``。
            target：目标组合。
            reason：原因。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
            now：当前 UTC 时间。
        返回值：
            无。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``ExperimentNotFound``、``StateConflict``、``ValueError``。
        Apply one lifecycle edge with a database compare-and-swap.
        """
        _RegistrySupport._experiment_id(experiment_id)
        validate_transition(expected, target, reason)
        if target is ExperimentStatus.SUCCEEDED:
            raise ValueError("SUCCEEDED must be registered with register_success")
        transition_time = self._time(now)
        subject = _RegistrySupport._subject(actor)
        request = _RegistrySupport._request_id(request_id)
        reason_payload = (
            _RegistrySupport._reason_payload(reason) if reason is not None else None
        )
        cas_succeeded = False
        with Session(self._engine) as session, session.begin():
            timestamp = _RegistrySupport._timestamp(transition_time)
            values: dict[str, object] = {"status": target.value}
            prerequisites = []
            if target is ExperimentStatus.QUEUED:
                values["queued_at"] = func.coalesce(ExperimentORM.queued_at, timestamp)
            elif target is ExperimentStatus.RUNNING:
                prerequisites.append(ExperimentORM.queued_at.is_not(None))
                values["started_at"] = func.coalesce(
                    ExperimentORM.started_at, timestamp
                )
            else:
                if expected is not ExperimentStatus.QUEUED:
                    prerequisites.append(ExperimentORM.started_at.is_not(None))
                values["completed_at"] = func.coalesce(
                    ExperimentORM.completed_at, timestamp
                )
            statement = (
                update(ExperimentORM)
                .where(
                    ExperimentORM.id == experiment_id,
                    ExperimentORM.status == expected.value,
                    *prerequisites,
                )
                .values(**values)
                .returning(
                    ExperimentORM.queued_at,
                    ExperimentORM.started_at,
                    ExperimentORM.completed_at,
                )
                .execution_options(synchronize_session=False)
            )
            updated = session.execute(statement).one_or_none()
            cas_succeeded = updated is not None
            if updated is not None:
                new_value: dict[str, JsonValue] = {
                    "status": target.value,
                    "queued_at": updated.queued_at,
                    "started_at": updated.started_at,
                    "completed_at": updated.completed_at,
                }
                details = _RegistrySupport._details(
                    subject,
                    "transition",
                    experiment_id,
                    {"status": expected.value},
                    new_value,
                    request,
                )
                if reason_payload is not None:
                    details["reason"] = reason_payload
                _RegistrySupport._add_audit(
                    session,
                    experiment_id,
                    "EXPERIMENT_STATE_TRANSITIONED",
                    subject,
                    details,
                    transition_time,
                )
        if not cas_succeeded:
            actual = self._actual_status(experiment_id)
            self._record_conflict(
                experiment_id,
                expected,
                target,
                actual,
                subject,
                request,
                transition_time,
                reason_payload,
            )
            if actual is None:
                raise ExperimentNotFound(experiment_id)
            raise StateConflict(experiment_id, expected, target, actual)

    def register_success(
        self,
        experiment_id: str,
        manifest: ExperimentArtifactPublication,
        metrics: Mapping[str, float],
        *,
        actor: str = "system",
        request_id: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """登记``success``。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
            manifest：已发布产物文件、哈希、字节数、Schema 和输入身份的清单。
            metrics：参与本次处理的指标集合；调用方不得依赖未声明的顺序。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
            now：当前 UTC 时间。
        返回值：
            无。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``ExperimentNotFound``、``StateConflict``、``TypeError``。
        Atomically register a deeply revalidated RUNNING experiment result.
        """
        _RegistrySupport._experiment_id(experiment_id)
        if not isinstance(manifest, ExperimentArtifactPublication):
            raise TypeError("manifest must be an ExperimentArtifactPublication")
        immutable = self._load_immutable(experiment_id)
        completed_at = self._time(now)
        subject = _RegistrySupport._subject(actor)
        request = _RegistrySupport._request_id(request_id)
        if immutable.status is not ExperimentStatus.RUNNING:
            self._record_conflict(
                experiment_id,
                ExperimentStatus.RUNNING,
                ExperimentStatus.SUCCEEDED,
                immutable.status,
                subject,
                request,
                completed_at,
                None,
            )
            raise StateConflict(
                experiment_id,
                ExperimentStatus.RUNNING,
                ExperimentStatus.SUCCEEDED,
                immutable.status,
            )

        # This trust-boundary work intentionally finishes before the write transaction.
        validated = validate_experiment_artifacts(
            manifest.artifact_dir, resolved_config=immutable.config
        )
        _RegistrySupport._validate_publication(manifest, validated)
        _RegistrySupport._validate_success_identity(experiment_id, immutable, validated)
        normalized_metrics = _RegistrySupport._validated_metrics(
            validated.artifact_dir, metrics
        )
        artifacts = _RegistrySupport._artifact_rows(
            experiment_id, validated, completed_at
        )
        times = transition_timestamps(
            ExperimentStatus.RUNNING,
            ExperimentStatus.SUCCEEDED,
            queued_at=immutable.queued_at,
            started_at=immutable.started_at,
            completed_at=immutable.completed_at,
            now=completed_at,
        )

        cas_succeeded = False
        with Session(self._engine) as session, session.begin():
            result = cast(
                CursorResult[Any],
                session.execute(
                    update(ExperimentORM)
                    .where(
                        ExperimentORM.id == experiment_id,
                        ExperimentORM.status == ExperimentStatus.RUNNING.value,
                    )
                    .values(
                        status=ExperimentStatus.SUCCEEDED.value,
                        completed_at=_RegistrySupport._optional_timestamp(
                            times.completed_at
                        ),
                    )
                    .execution_options(synchronize_session=False)
                ),
            )
            cas_succeeded = result.rowcount == 1
            if cas_succeeded:
                session.add_all(
                    [
                        ExperimentMetricORM(
                            experiment_id=experiment_id,
                            name=name,
                            value=value,
                            unit=None,
                            created_at=_RegistrySupport._timestamp(completed_at),
                        )
                        for name, value in normalized_metrics
                    ]
                )
                session.add_all(artifacts)
                _RegistrySupport._add_audit(
                    session,
                    experiment_id,
                    "EXPERIMENT_SUCCEEDED",
                    subject,
                    _RegistrySupport._details(
                        subject,
                        "register_success",
                        experiment_id,
                        {"status": ExperimentStatus.RUNNING.value},
                        {
                            "status": ExperimentStatus.SUCCEEDED.value,
                            "completed_at": _RegistrySupport._timestamp(completed_at),
                            "metric_count": len(normalized_metrics),
                            "artifact_count": len(artifacts),
                        },
                        request,
                    ),
                    completed_at,
                )
        if not cas_succeeded:
            actual = self._actual_status(experiment_id)
            self._record_conflict(
                experiment_id,
                ExperimentStatus.RUNNING,
                ExperimentStatus.SUCCEEDED,
                actual,
                subject,
                request,
                completed_at,
                None,
            )
            if actual is None:
                raise ExperimentNotFound(experiment_id)
            raise StateConflict(
                experiment_id,
                ExperimentStatus.RUNNING,
                ExperimentStatus.SUCCEEDED,
                actual,
            )

    def update_research(
        self,
        experiment_id: str,
        mark: ResearchMark,
        tags: Sequence[str],
        note: str,
        actor: str,
        *,
        request_id: str | None = None,
        now: datetime | None = None,
    ) -> None:
        """更新处理状态或进度。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
            mark：需要写入实验记录的研究标记。
            tags：参与本次处理的标签集合；调用方不得依赖未声明的顺序。
            note：不参与研究身份计算的可选人工备注。
            actor：操作主体。
            request_id：用于关联一次跨边界调用及其日志的请求标识。
            now：当前 UTC 时间。
        返回值：
            无。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``ExperimentNotFound``、``TypeError``。
        Update only mutable research annotations and audit the note fact.
        """
        _RegistrySupport._experiment_id(experiment_id)
        if not isinstance(mark, ResearchMark):
            raise TypeError("mark must be a ResearchMark")
        normalized_tags = _RegistrySupport._tags(tags)
        normalized_note = _RegistrySupport._note(note)
        subject = _RegistrySupport._subject(actor)
        request = _RegistrySupport._request_id(request_id)
        updated_at = self._time(now)
        with Session(self._engine) as session, session.begin():
            row = session.get(ExperimentORM, experiment_id)
            if row is None:
                raise ExperimentNotFound(experiment_id)
            old_tags = tuple(
                session.scalars(
                    select(ExperimentTagORM.tag)
                    .where(ExperimentTagORM.experiment_id == experiment_id)
                    .order_by(ExperimentTagORM.tag)
                )
            )
            old_note = _RegistrySupport._latest_note(session, experiment_id)
            old_mark = row.research_mark
            row.research_mark = mark.value
            session.execute(
                delete(ExperimentTagORM).where(
                    ExperimentTagORM.experiment_id == experiment_id
                )
            )
            session.add_all(
                [
                    ExperimentTagORM(experiment_id=experiment_id, tag=tag)
                    for tag in normalized_tags
                ]
            )
            _RegistrySupport._add_audit(
                session,
                experiment_id,
                "EXPERIMENT_RESEARCH_UPDATED",
                subject,
                _RegistrySupport._details(
                    subject,
                    "update_research",
                    experiment_id,
                    {
                        "research_mark": old_mark,
                        "tags": list(old_tags),
                        "note": old_note,
                    },
                    {
                        "research_mark": mark.value,
                        "tags": list(normalized_tags),
                        "note": normalized_note,
                    },
                    request,
                ),
                updated_at,
            )

    def _time(self, supplied: datetime | None) -> datetime:
        return _RegistrySupport._utc(
            supplied if supplied is not None else self._clock(), "now"
        )

    def _load_immutable(self, experiment_id: str) -> _ImmutableExperiment:
        with Session(self._engine) as session:
            row = session.get(ExperimentORM, experiment_id)
            if row is None:
                raise ExperimentNotFound(experiment_id)
            state = session.get(DataCatalogStateORM, 1)
            catalog_data_hash = state.catalog_hash if state is not None else None
            config = json.loads(row.config_json)
            canonical_json_bytes(cast(JsonValue, config))
            return _ImmutableExperiment(
                id=row.id,
                config=cast(dict[str, JsonValue], config),
                strategy_id=row.strategy_id,
                data_hash=row.data_hash,
                catalog_data_hash=catalog_data_hash,
                source_tree_hash=row.source_tree_hash,
                git_commit_hash=row.git_commit_hash,
                lockfile_hash=row.lockfile_hash,
                rulebook_hash=row.rulebook_hash,
                fingerprint=row.fingerprint,
                status=ExperimentStatus(row.status),
                queued_at=_RegistrySupport._parse_optional_timestamp(row.queued_at),
                started_at=_RegistrySupport._parse_optional_timestamp(row.started_at),
                completed_at=_RegistrySupport._parse_optional_timestamp(
                    row.completed_at
                ),
            )

    def _actual_status(self, experiment_id: str) -> ExperimentStatus | None:
        with Session(self._engine) as session:
            value = session.scalar(
                select(ExperimentORM.status).where(ExperimentORM.id == experiment_id)
            )
        return ExperimentStatus(value) if value is not None else None

    def _record_conflict(
        self,
        experiment_id: str,
        expected: ExperimentStatus,
        target: ExperimentStatus,
        actual: ExperimentStatus | None,
        subject: str,
        request_id: str,
        created_at: datetime,
        reason: dict[str, JsonValue] | None,
    ) -> None:
        details = _RegistrySupport._details(
            subject,
            "state_conflict" if actual is not None else "state_not_found",
            experiment_id,
            {"status": actual.value if actual is not None else None},
            {"expected": expected.value, "target": target.value},
            request_id,
        )
        if reason is not None:
            details["reason"] = reason
        with Session(self._engine) as session, session.begin():
            persisted_id = (
                experiment_id
                if session.get(ExperimentORM, experiment_id) is not None
                else None
            )
            _RegistrySupport._add_audit(
                session,
                persisted_id,
                "EXPERIMENT_STATE_CONFLICT",
                subject,
                details,
                created_at,
            )
