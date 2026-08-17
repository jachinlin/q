"""提供实验与实验查询相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Never, cast

from sqlalchemy import Engine, func, select
from sqlalchemy.orm import Session

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.experiments.models import (
    ExperimentArtifact,
    ExperimentMetric,
    ExperimentRecord,
    ExperimentStatus,
    ResearchMark,
)
from quant_research.experiments.registry import ExperimentNotFound
from quant_research.infrastructure.persistence.orm import (
    AuditEventORM,
    ExperimentArtifactORM,
    ExperimentMetricORM,
    ExperimentORM,
    ExperimentTagORM,
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class ExperimentAuditEvent:
    """记录实验生命周期中的操作主体、事件类型、时间和安全详情。

    入参：
        event_type：事件类型。
        actor：操作主体。
        details：参与本次处理的``details``；调用方不得依赖未声明的顺序。
        created_at：记录创建时的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    event_type: str
    actor: str | None
    details: Mapping[str, JsonValue]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ExperimentDetail:
    """组合实验记录、标签、指标、产物与审计事件形成只读详情。

    入参：
        record：记录。
        metrics：参与本次处理的指标集合；调用方不得依赖未声明的顺序。
        artifacts：参与本次处理的产物集合；调用方不得依赖未声明的顺序。
        tags：参与本次处理的标签集合；调用方不得依赖未声明的顺序。
        note：不参与研究身份计算的可选人工备注。
        audit：参与本次处理的审计事件；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    record: ExperimentRecord
    metrics: tuple[ExperimentMetric, ...]
    artifacts: tuple[ExperimentArtifact, ...]
    tags: tuple[str, ...]
    note: str | None
    audit: tuple[ExperimentAuditEvent, ...]


@dataclass(frozen=True, slots=True)
class ExperimentSummaryMetric:
    """提供控制面展示所需的有界实验指标字段。

    入参：
        name：供用户识别研究、任务或数据对象的非空名称。
        value：待校验或转换的值，类型为 ``float``。
        unit：计量单位。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Safe metric fields required by control-plane inspection.
    """

    name: str
    value: float
    unit: str | None


@dataclass(frozen=True, slots=True)
class ExperimentSummaryArtifact:
    """提供控制面展示所需的有界实验产物字段。

    入参：
        name：供用户识别研究、任务或数据对象的非空名称。
        artifact_type：产物类型。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        metadata：参与本次处理的元数据；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Safe artifact fields required by control-plane inspection.
    """

    name: str
    artifact_type: str
    content_hash: str
    metadata: Mapping[str, JsonValue]


@dataclass(frozen=True, slots=True)
class ExperimentSummary:
    """表示实验流程中的实验摘要及其业务不变量。

    入参：
        id：用于持久化关联和日志追踪的标识。
        status：当前记录所处的受控生命周期状态。
        strategy_id：用于持久化关联和日志追踪的策略标识。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        config_hash：确定性序列化后的实验或策略配置身份。
        fingerprint：由策略、数据、源码、依赖锁和交易规则共同形成的研究指纹。
        metrics：参与本次处理的指标集合；调用方不得依赖未声明的顺序。
        artifacts：参与本次处理的产物集合；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Explicitly bounded experiment fields for the CLI control plane.
    """

    id: str
    status: ExperimentStatus
    strategy_id: str
    data_hash: str
    config_hash: str
    fingerprint: str
    metrics: tuple[ExperimentSummaryMetric, ...]
    artifacts: tuple[ExperimentSummaryArtifact, ...]


class ExperimentQuery:
    """校验分页与筛选条件并执行稳定的只读实验查询。

    入参：
        engine：引擎。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ExperimentNotFound``、``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Validate and execute stable read-only experiment queries.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def list(
        self,
        *,
        statuses: ExperimentStatus | Sequence[ExperimentStatus] | None = None,
        strategy_id: str | None = None,
        research_mark: ResearchMark | None = None,
        tags: Sequence[str] = (),
        created_from: datetime | None = None,
        created_to: datetime | None = None,
        fingerprint: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[ExperimentRecord, ...]:
        """列出符合条件的记录。

        入参：
            statuses：状态集合。
            strategy_id：用于持久化关联和日志追踪的策略标识。
            research_mark：用户对实验标记的基线、候选或废弃研究结论。
            tags：参与本次处理的标签集合；调用方不得依赖未声明的顺序。
            created_from：创建时间来源。
            created_to：创建时间目标。
            fingerprint：由策略、数据、源码、依赖锁和交易规则共同形成的研究指纹。
            limit：单次查询允许返回的最大记录数。
            offset：分页查询跳过的记录数。
        返回值：
            返回按确定性顺序列出实验后的``list``（``tuple[ExperimentRecord, ...]``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        List experiments in stable newest-first order with validated filters.
        """
        normalized_statuses = _QuerySupport._statuses(statuses)
        normalized_tags = _QuerySupport._filter_tags(tags)
        normalized_from = _QuerySupport._optional_utc(created_from, "created_from")
        normalized_to = _QuerySupport._optional_utc(created_to, "created_to")
        if (
            normalized_from is not None
            and normalized_to is not None
            and normalized_from > normalized_to
        ):
            raise ValueError("created_from must not follow created_to")
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("limit must be an integer from 1 through 500")
        if type(offset) is not int or offset < 0:
            raise ValueError("offset must be a nonnegative integer")
        normalized_strategy = _QuerySupport._optional_text(
            strategy_id, "strategy_id", 128
        )
        if research_mark is not None and not isinstance(research_mark, ResearchMark):
            raise TypeError("research_mark must be a ResearchMark")
        if fingerprint is not None:
            _QuerySupport._fingerprint(fingerprint)

        statement = select(ExperimentORM)
        if normalized_statuses is not None:
            statement = statement.where(
                ExperimentORM.status.in_(
                    [status.value for status in normalized_statuses]
                )
            )
        if normalized_strategy is not None:
            statement = statement.where(
                ExperimentORM.strategy_id == normalized_strategy
            )
        if research_mark is not None:
            statement = statement.where(
                ExperimentORM.research_mark == research_mark.value
            )
        if normalized_tags:
            tagged = (
                select(ExperimentTagORM.experiment_id)
                .where(ExperimentTagORM.tag.in_(normalized_tags))
                .group_by(ExperimentTagORM.experiment_id)
                .having(func.count(ExperimentTagORM.tag) == len(normalized_tags))
            )
            statement = statement.where(ExperimentORM.id.in_(tagged))
        if normalized_from is not None:
            statement = statement.where(
                ExperimentORM.created_at >= normalized_from.isoformat()
            )
        if normalized_to is not None:
            statement = statement.where(
                ExperimentORM.created_at <= normalized_to.isoformat()
            )
        if fingerprint is not None:
            statement = statement.where(ExperimentORM.fingerprint == fingerprint)
        statement = (
            statement.order_by(ExperimentORM.created_at.desc(), ExperimentORM.id.desc())
            .limit(limit)
            .offset(offset)
        )
        with Session(self._engine) as session:
            return tuple(
                _QuerySupport._record(row) for row in session.scalars(statement)
            )

    def find_duplicates(
        self,
        fingerprint: str,
        *,
        limit: int = 500,
        offset: int = 0,
    ) -> tuple[ExperimentRecord, ...]:
        """查询``duplicates``。

        入参：
            fingerprint：由策略、数据、源码、依赖锁和交易规则共同形成的研究指纹。
            limit：单次查询允许返回的最大记录数。
            offset：分页查询跳过的记录数。
        返回值：
            返回查询``duplicates``后的``duplicates``（``tuple[ExperimentRecord, ...]``）。
        异常：
            无。
        Return repeated-research candidates without collapsing identities.
        """
        _QuerySupport._fingerprint(fingerprint)
        return self.list(fingerprint=fingerprint, limit=limit, offset=offset)

    def get(self, experiment_id: str) -> ExperimentDetail:
        """读取并返回约定对象。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
        返回值：
            返回读取实验后的``get``（``ExperimentDetail``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``ExperimentNotFound``。
        Assemble one experiment, its result indexes, annotations, and timeline.
        """
        identifier = _QuerySupport._text(experiment_id, "experiment_id", 128)
        with Session(self._engine) as session:
            row = session.get(ExperimentORM, identifier)
            if row is None:
                raise ExperimentNotFound(identifier)
            record = _QuerySupport._record(row)
            metrics = tuple(
                _QuerySupport._metric(item)
                for item in session.scalars(
                    select(ExperimentMetricORM)
                    .where(ExperimentMetricORM.experiment_id == identifier)
                    .order_by(ExperimentMetricORM.name)
                )
            )
            artifacts = tuple(
                _QuerySupport._artifact(item)
                for item in session.scalars(
                    select(ExperimentArtifactORM)
                    .where(ExperimentArtifactORM.experiment_id == identifier)
                    .order_by(ExperimentArtifactORM.name)
                )
            )
            tags = tuple(
                session.scalars(
                    select(ExperimentTagORM.tag)
                    .where(ExperimentTagORM.experiment_id == identifier)
                    .order_by(ExperimentTagORM.tag)
                )
            )
            audit = tuple(
                _QuerySupport._audit(item)
                for item in session.scalars(
                    select(AuditEventORM)
                    .where(AuditEventORM.experiment_id == identifier)
                    .order_by(AuditEventORM.created_at, AuditEventORM.id)
                )
            )
        note = _QuerySupport._note_from_timeline(audit)
        return ExperimentDetail(record, metrics, artifacts, tags, note, audit)

    def inspection_summary(
        self,
        experiment_id: str,
        *,
        metric_limit: int = 100,
        artifact_limit: int = 100,
    ) -> ExperimentSummary:
        """读取有界实验检查摘要摘要。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
            metric_limit：指标数量上限。
            artifact_limit：产物数量上限。
        返回值：
            返回摘要（``ExperimentSummary``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``ExperimentNotFound``。
        Read only explicitly safe fields with bounded result indexes.
        """
        identifier = _QuerySupport._text(experiment_id, "experiment_id", 128)
        metrics_limit = _QuerySupport._inspection_limit(metric_limit, "metric_limit")
        artifacts_limit = _QuerySupport._inspection_limit(
            artifact_limit, "artifact_limit"
        )
        with Session(self._engine) as session:
            record = (
                session.execute(
                    select(
                        ExperimentORM.id,
                        ExperimentORM.status,
                        ExperimentORM.strategy_id,
                        ExperimentORM.data_hash,
                        ExperimentORM.config_hash,
                        ExperimentORM.fingerprint,
                    ).where(ExperimentORM.id == identifier)
                )
                .mappings()
                .one_or_none()
            )
            if record is None:
                raise ExperimentNotFound(identifier)
            metric_rows = (
                session.execute(
                    select(
                        ExperimentMetricORM.name,
                        ExperimentMetricORM.value,
                        ExperimentMetricORM.unit,
                    )
                    .where(ExperimentMetricORM.experiment_id == identifier)
                    .order_by(ExperimentMetricORM.name)
                    .limit(metrics_limit + 1)
                )
                .mappings()
                .all()
            )
            if len(metric_rows) > metrics_limit:
                _QuerySupport._raise_inspection_limit("metrics", metrics_limit)
            artifact_rows = (
                session.execute(
                    select(
                        ExperimentArtifactORM.name,
                        ExperimentArtifactORM.artifact_type,
                        ExperimentArtifactORM.content_hash,
                        ExperimentArtifactORM.metadata_json,
                    )
                    .where(ExperimentArtifactORM.experiment_id == identifier)
                    .order_by(ExperimentArtifactORM.name)
                    .limit(artifacts_limit + 1)
                )
                .mappings()
                .all()
            )
            if len(artifact_rows) > artifacts_limit:
                _QuerySupport._raise_inspection_limit("artifacts", artifacts_limit)
        return ExperimentSummary(
            id=cast(str, record["id"]),
            status=ExperimentStatus(cast(str, record["status"])),
            strategy_id=cast(str, record["strategy_id"]),
            data_hash=cast(str, record["data_hash"]),
            config_hash=cast(str, record["config_hash"]),
            fingerprint=cast(str, record["fingerprint"]),
            metrics=tuple(
                ExperimentSummaryMetric(
                    name=cast(str, row["name"]),
                    value=cast(float, row["value"]),
                    unit=cast(str | None, row["unit"]),
                )
                for row in metric_rows
            ),
            artifacts=tuple(
                ExperimentSummaryArtifact(
                    name=cast(str, row["name"]),
                    artifact_type=cast(str, row["artifact_type"]),
                    content_hash=cast(str, row["content_hash"]),
                    metadata=_QuerySupport._summary_metadata(
                        cast(str, row["metadata_json"])
                    ),
                )
                for row in artifact_rows
            ),
        )

    get_detail = get


ExperimentQueryService = ExperimentQuery


class _QuerySupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _record(row: ExperimentORM) -> ExperimentRecord:
        config = json.loads(row.config_json)
        canonical_json_bytes(cast(JsonValue, config))
        return ExperimentRecord(
            id=row.id,
            strategy_id=row.strategy_id,
            config=cast(dict[str, JsonValue], config),
            config_hash=row.config_hash,
            data_hash=row.data_hash,
            source_tree_hash=row.source_tree_hash,
            git_commit_hash=row.git_commit_hash,
            lockfile_hash=row.lockfile_hash,
            rulebook_hash=row.rulebook_hash,
            fingerprint=row.fingerprint,
            status=ExperimentStatus(row.status),
            research_mark=ResearchMark(row.research_mark),
            created_at=_QuerySupport._parse_timestamp(row.created_at),
            queued_at=_QuerySupport._parse_optional_timestamp(row.queued_at),
            started_at=_QuerySupport._parse_optional_timestamp(row.started_at),
            completed_at=_QuerySupport._parse_optional_timestamp(row.completed_at),
        )

    @staticmethod
    def _metric(row: ExperimentMetricORM) -> ExperimentMetric:
        return ExperimentMetric(
            experiment_id=row.experiment_id,
            name=row.name,
            value=row.value,
            unit=row.unit,
            created_at=_QuerySupport._parse_timestamp(row.created_at),
        )

    @staticmethod
    def _artifact(row: ExperimentArtifactORM) -> ExperimentArtifact:
        metadata = json.loads(row.metadata_json)
        canonical_json_bytes(cast(JsonValue, metadata))
        return ExperimentArtifact(
            experiment_id=row.experiment_id,
            name=row.name,
            artifact_type=row.artifact_type,
            path=row.path,
            content_hash=row.content_hash,
            metadata=cast(dict[str, JsonValue], metadata),
            created_at=_QuerySupport._parse_timestamp(row.created_at),
        )

    @staticmethod
    def _summary_metadata(value: str) -> dict[str, JsonValue]:
        metadata = json.loads(value)
        canonical_json_bytes(cast(JsonValue, metadata))
        if not isinstance(metadata, dict):
            raise TypeError("persisted artifact metadata must be a JSON object")
        return cast(dict[str, JsonValue], metadata)

    @staticmethod
    def _inspection_limit(value: int, field: str) -> int:
        if type(value) is not int or not 1 <= value <= 500:
            raise ValueError(f"{field} must be an integer from 1 through 500")
        return value

    @staticmethod
    def _raise_inspection_limit(collection: str, limit: int) -> Never:
        raise QuantError(
            ErrorDetail(
                code="EXPERIMENT_INSPECTION_LIMIT_EXCEEDED",
                severity=Severity.SEVERE,
                message="experiment inspection result exceeds its safe limit",
                context={"collection": collection, "limit": limit},
                remediation="reduce registered result indexes before inspection",
                retryable=False,
            )
        )

    @staticmethod
    def _audit(row: AuditEventORM) -> ExperimentAuditEvent:
        details = json.loads(row.details_json)
        canonical_json_bytes(cast(JsonValue, details))
        if not isinstance(details, dict):
            raise TypeError("persisted audit details must be a JSON object")
        return ExperimentAuditEvent(
            event_type=row.event_type,
            actor=row.actor,
            details=cast(dict[str, JsonValue], details),
            created_at=_QuerySupport._parse_timestamp(row.created_at),
        )

    @staticmethod
    def _note_from_timeline(audit: tuple[ExperimentAuditEvent, ...]) -> str | None:
        for entry in reversed(audit):
            if entry.event_type != "EXPERIMENT_RESEARCH_UPDATED":
                continue
            new_value = entry.details.get("new_value")
            if isinstance(new_value, Mapping):
                note = new_value.get("note")
                if isinstance(note, str):
                    return note
        return None

    @staticmethod
    def _statuses(
        value: ExperimentStatus | Sequence[ExperimentStatus] | None,
    ) -> tuple[ExperimentStatus, ...] | None:
        if value is None:
            return None
        if isinstance(value, ExperimentStatus):
            return (value,)
        if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
            raise TypeError("statuses must be an ExperimentStatus or sequence")
        if not value:
            raise ValueError("statuses must not be empty")
        if any(not isinstance(status, ExperimentStatus) for status in value):
            raise TypeError("statuses must contain ExperimentStatus values")
        return tuple(dict.fromkeys(cast(Sequence[ExperimentStatus], value)))

    @staticmethod
    def _filter_tags(values: Sequence[str]) -> tuple[str, ...]:
        if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
            raise TypeError("tags must be a sequence of strings")
        normalized: set[str] = set()
        for value in values:
            tag = _QuerySupport._text(value, "tag", 64).strip()
            if not tag:
                raise ValueError("tag must not be empty")
            normalized.add(tag)
        return tuple(sorted(normalized))

    @staticmethod
    def _fingerprint(value: str) -> None:
        if not isinstance(value, str) or not _SHA256.fullmatch(value):
            raise ValueError("fingerprint must be a lowercase SHA-256 hex digest")

    @staticmethod
    def _optional_text(value: str | None, field: str, limit: int) -> str | None:
        return _QuerySupport._text(value, field, limit) if value is not None else None

    @staticmethod
    def _text(value: str, field: str, limit: int) -> str:
        if not isinstance(value, str):
            raise TypeError(f"{field} must be a string")
        if not value.strip():
            raise ValueError(f"{field} must not be empty")
        if value != value.strip():
            raise ValueError(f"{field} must be trimmed")
        if len(value) > limit:
            raise ValueError(f"{field} is too long")
        return value

    @staticmethod
    def _optional_utc(value: datetime | None, field: str) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{field} must be timezone-aware")
        return value.astimezone(UTC)

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("persisted timestamp must be timezone-aware")
        return parsed.astimezone(UTC)

    @staticmethod
    def _parse_optional_timestamp(value: str | None) -> datetime | None:
        return _QuerySupport._parse_timestamp(value) if value is not None else None
