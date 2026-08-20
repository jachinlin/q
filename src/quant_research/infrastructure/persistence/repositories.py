"""提供事务化持久化操作，并返回面向领域层的不可变数据对象。"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Never, cast
from uuid import uuid4

from sqlalchemy import Engine, delete, func, select
from sqlalchemy.orm import Session

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.data.quality.models import (
    QualityEvidenceSource,
    QualityIssue,
    QualityRuleResult,
    QualityRuleStatus,
    QualityRunSpec,
    thaw_json,
)
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.domain.identifiers import QualityRunId
from quant_research.experiments.models import (
    ExperimentRecord,
    ExperimentSpec,
    ExperimentStatus,
    ResearchMark,
)
from quant_research.infrastructure.persistence.orm import (
    CanonicalDatasetORM,
    CanonicalPartitionORM,
    DataCatalogStateORM,
    DataInitializationStateORM,
    DatasetOperationalStateORM,
    ExperimentORM,
    QualityIssueORM,
    QualityRuleResultORM,
    QualityRunDatasetORM,
    QualityRunORM,
    RawObjectORM,
    RawRequestORM,
    TaskORM,
)
from quant_research.tasks.models import TaskRecord, TaskSpec, TaskStatus

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class RawPartitionSpec:
    """注册一个 Raw 对象及其请求当前头所需的输入。

    入参：
        source：数据来源。
        endpoint：供应商端点。
        request：参与本次处理的请求；调用方不得依赖未声明的顺序。
        request_hash：规范供应商请求的确定性身份，用于幂等定位 Raw 响应。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        data_path：经可信根边界校验后使用的数据路径。
        manifest_path：记录文件身份、Schema、行数和输入身份的清单路径。
        schema_fingerprint：字段名称和类型形成的确定性 Schema 身份。
        row_count：产物或分区中经验证的数据行数。
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    source: str
    endpoint: str
    request: Mapping[str, JsonValue]
    request_hash: str
    content_hash: str
    data_path: Path
    manifest_path: Path
    schema_fingerprint: str
    row_count: int
    retrieved_at: datetime

    def __post_init__(self) -> None:
        if not self.source or not self.endpoint:
            raise ValueError("raw source and endpoint must not be empty")
        _RepositoriesSupport._validate_hash(self.request_hash, "request_hash")
        _RepositoriesSupport._validate_hash(self.content_hash, "content_hash")
        _RepositoriesSupport._validate_hash(
            self.schema_fingerprint, "schema_fingerprint"
        )
        if hashlib.sha256(canonical_json_bytes(self.request)).hexdigest() != (
            self.request_hash
        ):
            raise ValueError("request_hash must match the canonical raw request")
        if self.row_count < 0:
            raise ValueError("raw row_count must be non-negative")
        object.__setattr__(self, "request", MappingProxyType(dict(self.request)))
        object.__setattr__(self, "data_path", self.data_path.resolve())
        object.__setattr__(self, "manifest_path", self.manifest_path.resolve())
        object.__setattr__(
            self, "retrieved_at", _RepositoriesSupport._utc_datetime(self.retrieved_at)
        )


@dataclass(frozen=True, slots=True)
class RawPartitionRecord:
    """从元数据仓库读取的不可变 Raw 对象记录。

    入参：
        source：数据来源。
        endpoint：供应商端点。
        request：参与本次处理的请求；调用方不得依赖未声明的顺序。
        request_hash：规范供应商请求的确定性身份，用于幂等定位 Raw 响应。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        data_path：经可信根边界校验后使用的数据路径。
        manifest_path：记录文件身份、Schema、行数和输入身份的清单路径。
        schema_fingerprint：字段名称和类型形成的确定性 Schema 身份。
        row_count：产物或分区中经验证的数据行数。
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    source: str
    endpoint: str
    request: Mapping[str, JsonValue]
    request_hash: str
    content_hash: str
    data_path: Path
    manifest_path: Path
    schema_fingerprint: str
    row_count: int
    retrieved_at: datetime


@dataclass(frozen=True, slots=True)
class CanonicalPartitionSpec:
    """发布一个 Canonical 分区所需的输入。

    入参：
        partition_key：分区``key``。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        path：待处理的文件系统路径，类型为 ``Path``。
        schema_fingerprint：字段名称和类型形成的确定性 Schema 身份。
        input_hash：决定 Canonical 分区是否需要重建的 Raw 输入身份。
        row_count：产物或分区中经验证的数据行数。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    partition_key: str
    content_hash: str
    path: Path
    schema_fingerprint: str
    input_hash: str
    row_count: int

    def __post_init__(self) -> None:
        if not self.partition_key:
            raise ValueError("partition_key must not be empty")
        _RepositoriesSupport._validate_hash(self.content_hash, "content_hash")
        _RepositoriesSupport._validate_hash(
            self.schema_fingerprint, "schema_fingerprint"
        )
        _RepositoriesSupport._validate_hash(self.input_hash, "input_hash")
        if self.row_count < 0:
            raise ValueError("row_count must be non-negative")
        object.__setattr__(self, "path", self.path.resolve())


@dataclass(frozen=True, slots=True)
class CanonicalPartitionRecord:
    """当前 Canonical 数据集中的不可变分区记录。

    入参：
        partition_key：分区``key``。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        path：待处理的文件系统路径，类型为 ``Path``。
        schema_fingerprint：字段名称和类型形成的确定性 Schema 身份。
        input_hash：决定 Canonical 分区是否需要重建的 Raw 输入身份。
        row_count：产物或分区中经验证的数据行数。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    partition_key: str
    content_hash: str
    path: Path
    schema_fingerprint: str
    input_hash: str
    row_count: int


@dataclass(frozen=True, slots=True)
class RawHeadIdentity:
    """参与 Curate 一致性校验的单个 Raw 当前头身份。

    入参：
        source：数据来源。
        endpoint：供应商端点。
        request_hash：规范供应商请求的确定性身份，用于幂等定位 Raw 响应。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        schema_fingerprint：字段名称和类型形成的确定性 Schema 身份。
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    source: str
    endpoint: str
    request_hash: str
    content_hash: str
    schema_fingerprint: str
    retrieved_at: datetime

    @classmethod
    def from_record(cls, record: RawPartitionRecord) -> RawHeadIdentity:
        """从 Raw 分区记录提取当前头身份。

        入参：
            record：记录。
        返回值：
            返回记录（``RawHeadIdentity``）。
        异常：
            无。
        """
        return cls(
            source=record.source,
            endpoint=record.endpoint,
            request_hash=record.request_hash,
            content_hash=record.content_hash,
            schema_fingerprint=record.schema_fingerprint,
            retrieved_at=record.retrieved_at,
        )


@dataclass(frozen=True, slots=True)
class RawHeadSnapshot:
    """Curate 规划时选中 Raw 当前头的有序快照。

    入参：
        source：数据来源。
        endpoints：参与本次处理的``endpoints``；调用方不得依赖未声明的顺序。
        heads：参与本次处理的``heads``；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    source: str
    endpoints: tuple[str, ...]
    heads: tuple[RawHeadIdentity, ...]

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("raw head snapshot source must not be empty")
        if not self.endpoints or len(self.endpoints) != len(set(self.endpoints)):
            raise ValueError("raw head snapshot endpoints must be unique and non-empty")
        if tuple(sorted(self.endpoints)) != self.endpoints:
            raise ValueError("raw head snapshot endpoints must be sorted")
        expected = tuple(
            sorted(self.heads, key=_RepositoriesSupport._raw_head_identity_key)
        )
        if expected != self.heads:
            raise ValueError("raw head snapshot heads must be sorted")
        if any(head.source != self.source for head in self.heads):
            raise ValueError("raw head snapshot contains a different source")
        if any(head.endpoint not in self.endpoints for head in self.heads):
            raise ValueError("raw head snapshot contains an unselected endpoint")


@dataclass(frozen=True, slots=True)
class CanonicalDatasetSpec:
    """原子发布一个完整 Canonical 数据集所需的输入。

    入参：
        dataset：目标数据集，类型为 ``DatasetKind``。
        source：数据来源。
        partitions：参与本次处理的分区集合；调用方不得依赖未声明的顺序。
        start_date：查询或运行覆盖区间的首日（含）。
        end_date：查询或运行覆盖区间的末日（含）。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    dataset: DatasetKind
    source: str
    partitions: tuple[CanonicalPartitionSpec, ...]
    start_date: date | None
    end_date: date | None

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("source must not be empty")
        if not self.partitions:
            raise ValueError("canonical dataset must contain at least one partition")
        keys = [item.partition_key for item in self.partitions]
        if len(keys) != len(set(keys)):
            raise ValueError("canonical partition keys must be unique")
        paths = [item.path for item in self.partitions]
        if len(paths) != len(set(paths)):
            raise ValueError("canonical partition paths must be unique")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must not follow end_date")


@dataclass(frozen=True, slots=True)
class CanonicalDatasetRecord:
    """元数据仓库中当前生效的 Canonical 数据集记录。

    入参：
        dataset：目标数据集，类型为 ``DatasetKind``。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        source：数据来源。
        partitions：参与本次处理的分区集合；调用方不得依赖未声明的顺序。
        start_date：查询或运行覆盖区间的首日（含）。
        end_date：查询或运行覆盖区间的末日（含）。
        updated_at：记录最近持久化变更的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    dataset: DatasetKind
    content_hash: str
    source: str
    partitions: tuple[CanonicalPartitionRecord, ...]
    start_date: date | None
    end_date: date | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class CanonicalPublishResult:
    """Canonical 数据集原子发布结果。

    入参：
        record：记录。
        changed：控制是否启用``changed``规则的布尔开关。
        orphan_paths：参与本次处理的失联任务``paths``；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    record: CanonicalDatasetRecord
    changed: bool
    orphan_paths: tuple[Path, ...]


@dataclass(frozen=True, slots=True)
class DatasetOperationalStateRecord:
    """描述单个数据集最近成功完成的流水线阶段。

    入参：由字段声明给出。返回值：构造不可变状态。异常：非法类型按 Python 契约传播。
    """

    dataset: DatasetKind
    last_localized_at: datetime | None
    localized_through: date | None
    last_curated_at: datetime | None
    last_validated_at: datetime | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class DataCatalogState:
    """全局 Canonical 目录及其读取门禁状态。

    入参：
        catalog_hash：提交时捕获并在运行阶段防漂移校验的 Canonical 数据目录身份。
        validated_catalog_hash：参与幂等、漂移或完整性校验的``validated``数据目录哈希；使用 SHA-256 十六进制文本。
        quality_run_id：用于持久化关联和日志追踪的质量校验运行标识。
        updated_at：记录最近持久化变更的 UTC 时间戳。
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    catalog_hash: str
    validated_catalog_hash: str | None
    quality_run_id: QualityRunId | None
    updated_at: datetime
    validated_at: datetime | None

    @property
    def is_validated(self) -> bool:
        """判断当前目录是否仍由一次全局质量运行有效覆盖。

        入参：
            无。
        返回值：
            返回是否``validated``。
        异常：
            无。
        """
        return (
            self.quality_run_id is not None
            and self.validated_catalog_hash == self.catalog_hash
        )


@dataclass(frozen=True, slots=True)
class DataInitializationStateRecord:
    """记录首次初始化的冻结年数、日期窗口和完成证据。

    入参：初始化状态、年数、闭区间、时间戳及可选完成身份。
    返回值：跨流水线与仓储边界传递的不可变状态。
    异常：字段来自受约束的持久化记录，构造时不主动抛出异常。
    """

    status: str
    years: int
    start_date: date
    end_date: date
    started_at: datetime
    completed_at: datetime | None
    catalog_hash: str | None
    quality_run_id: QualityRunId | None


@dataclass(frozen=True, slots=True)
class QualityRunRecord:
    """一次数据质量运行的不可变持久化记录。

    入参：
        id：用于持久化关联和日志追踪的标识。
        scope：范围。
        input_hash：决定 Canonical 分区是否需要重建的 Raw 输入身份。
        status：当前记录所处的受控生命周期状态。
        dataset_hashes：参与本次处理的数据集``hashes``；调用方不得依赖未声明的顺序。
        started_at：执行实际开始的 UTC 时间戳。
        completed_at：执行进入终态的 UTC 时间戳。
        issues：参与本次处理的质量问题；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    id: QualityRunId
    scope: str
    input_hash: str
    status: str
    dataset_hashes: Mapping[str, str]
    started_at: datetime
    completed_at: datetime | None
    issues: tuple[QualityIssue, ...]
    rule_results: tuple[QualityRuleResult, ...]
    results_complete: bool


class MetadataRepository:
    """使用 SQLAlchemy 事务持久化数据目录、质量、实验和任务元数据。

    入参：
        engine：引擎。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``KeyError``、``QuantError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    def __init__(self, engine: Engine) -> None:
        """创建元数据仓库。

        参数:
            engine: 已完成 Schema 初始化的 SQLAlchemy 数据库引擎。

        返回:
            ``None``。
        """
        self._engine = engine

    def register_raw_partition(self, spec: RawPartitionSpec) -> RawPartitionRecord:
        """幂等注册 Raw 对象，并将对应请求的当前头切换到该内容。

        入参：
            spec：不可变规格。
        返回值：
            返回``raw``分区（``RawPartitionRecord``）。
        异常：
            无。
        """
        request_json = canonical_json_bytes(dict(spec.request)).decode("utf-8")
        now = datetime.now(UTC)
        identity = (spec.source, spec.endpoint, spec.request_hash)
        object_identity = (*identity, spec.content_hash)
        with Session(self._engine) as session, session.begin():
            request_row = session.get(RawRequestORM, identity)
            if request_row is None:
                request_row = RawRequestORM(
                    source=spec.source,
                    endpoint=spec.endpoint,
                    request_hash=spec.request_hash,
                    request_json=request_json,
                    current_content_hash=spec.content_hash,
                    updated_at=_RepositoriesSupport._timestamp(now),
                )
                session.add(request_row)
            elif request_row.request_json != request_json:
                _RepositoriesSupport._raise_repository_conflict(
                    "raw request hash collision"
                )
            object_row = session.get(RawObjectORM, object_identity)
            if object_row is None:
                object_row = RawObjectORM(
                    source=spec.source,
                    endpoint=spec.endpoint,
                    request_hash=spec.request_hash,
                    content_hash=spec.content_hash,
                    data_path=spec.data_path.as_posix(),
                    manifest_path=spec.manifest_path.as_posix(),
                    schema_fingerprint=spec.schema_fingerprint,
                    row_count=spec.row_count,
                    retrieved_at=_RepositoriesSupport._timestamp(spec.retrieved_at),
                    created_at=_RepositoriesSupport._timestamp(now),
                )
                session.add(object_row)
            elif not _RepositoriesSupport._raw_object_matches(object_row, spec):
                _RepositoriesSupport._raise_repository_conflict(
                    "raw object identity collision"
                )
            request_row.current_content_hash = spec.content_hash
            request_row.updated_at = _RepositoriesSupport._timestamp(now)
            session.flush()
            return self._raw_partition_record(request_row, object_row)

    def find_raw_partition(
        self, source: str, endpoint: str, request_hash: str
    ) -> RawPartitionRecord | None:
        """按请求身份查找其当前 Raw 对象。

        入参：
            source：数据来源。
            endpoint：供应商端点。
            request_hash：规范供应商请求的确定性身份，用于幂等定位 Raw 响应。
        返回值：
            返回查询``raw``分区后的``raw``分区（``RawPartitionRecord | None``）。
        异常：
            供应商会话、请求或响应校验失败时传播对应适配器异常。
        """
        with Session(self._engine) as session:
            request = session.get(RawRequestORM, (source, endpoint, request_hash))
            if request is None:
                return None
            obj = session.get(
                RawObjectORM,
                (source, endpoint, request_hash, request.current_content_hash),
            )
            if obj is None:
                _RepositoriesSupport._raise_repository_conflict(
                    "raw request head has no raw object"
                )
            return self._raw_partition_record(request, obj)

    def list_raw_partitions(
        self, *, source: str | None = None, endpoint: str | None = None
    ) -> tuple[RawPartitionRecord, ...]:
        """列出符合条件的所有请求当前 Raw 头。

        入参：
            source：数据来源。
            endpoint：供应商端点。
        返回值：
            返回按确定性顺序列出``raw``分区集合后的``raw``分区集合（``tuple[RawPartitionRecord, ...]``）。
        异常：
            供应商会话、请求或响应校验失败时传播对应适配器异常。
        """
        with Session(self._engine) as session:
            query = select(RawRequestORM).order_by(
                RawRequestORM.source,
                RawRequestORM.endpoint,
                RawRequestORM.request_hash,
            )
            if source is not None:
                query = query.where(RawRequestORM.source == source)
            if endpoint is not None:
                query = query.where(RawRequestORM.endpoint == endpoint)
            records: list[RawPartitionRecord] = []
            for request in session.scalars(query):
                obj = session.get(
                    RawObjectORM,
                    (
                        request.source,
                        request.endpoint,
                        request.request_hash,
                        request.current_content_hash,
                    ),
                )
                if obj is None:
                    _RepositoriesSupport._raise_repository_conflict(
                        "raw request head has no raw object"
                    )
                records.append(self._raw_partition_record(request, obj))
            return tuple(records)

    def list_raw_objects(
        self, *, source: str | None = None, endpoint: str | None = None
    ) -> tuple[RawPartitionRecord, ...]:
        """列出符合条件的全部不可变 Raw 对象，包括已被替换的旧头。

        入参：
            source：可选的数据供应商筛选条件；空值表示不限供应商。
            endpoint：可选的供应商端点筛选条件；空值表示不限端点。
        返回值：
            返回按供应商、端点、请求哈希、获取时间和内容哈希排序的 Raw 对象记录。
        异常：
            SQLAlchemyError：SQLite 查询失败时传播。
        """
        with Session(self._engine) as session:
            query = (
                select(RawRequestORM, RawObjectORM)
                .join(
                    RawObjectORM,
                    (RawObjectORM.source == RawRequestORM.source)
                    & (RawObjectORM.endpoint == RawRequestORM.endpoint)
                    & (RawObjectORM.request_hash == RawRequestORM.request_hash),
                )
                .order_by(
                    RawRequestORM.source,
                    RawRequestORM.endpoint,
                    RawRequestORM.request_hash,
                    RawObjectORM.retrieved_at,
                    RawObjectORM.content_hash,
                )
            )
            if source is not None:
                query = query.where(RawRequestORM.source == source)
            if endpoint is not None:
                query = query.where(RawRequestORM.endpoint == endpoint)
            return tuple(
                self._raw_partition_record(request, obj)
                for request, obj in session.execute(query)
            )

    def count_raw_requests(self) -> int:
        """统计具有当前头的 Raw 请求数量。

        入参：
            无。
        返回值：
            返回 ``raw_request`` 表中具有当前 Raw 头指针的请求总数。
        异常：
            无。
        """
        with Session(self._engine) as session:
            return int(
                session.scalar(select(func.count()).select_from(RawRequestORM)) or 0
            )

    def replace_canonical_dataset(
        self,
        spec: CanonicalDatasetSpec,
        *,
        updated_at: datetime,
        expected_raw_heads: RawHeadSnapshot | None = None,
    ) -> CanonicalPublishResult:
        """在单个事务中原子替换一个 Canonical 数据集的全部当前分区。

        入参：
            spec：不可变规格。
            updated_at：记录最近持久化变更的 UTC 时间戳。
            返回Canonical数据集（``CanonicalPublishResult``）。
        返回值：
            返回Canonical数据集（``CanonicalPublishResult``）。
        异常：
            无。
        """
        updated_at = _RepositoriesSupport._utc_datetime(updated_at)
        content_hash = canonical_dataset_hash(spec)
        with Session(self._engine) as session, session.begin():
            if expected_raw_heads is not None:
                self._verify_raw_head_snapshot(session, expected_raw_heads)
            existing = session.get(CanonicalDatasetORM, spec.dataset.value)
            if existing is not None and existing.content_hash == content_hash:
                current = {
                    row.partition_key: row
                    for row in session.scalars(
                        select(CanonicalPartitionORM).where(
                            CanonicalPartitionORM.dataset == spec.dataset.value
                        )
                    )
                }
                if set(current) != {item.partition_key for item in spec.partitions}:
                    _RepositoriesSupport._raise_repository_conflict(
                        "canonical dataset hash matches a different partition set"
                    )
                for partition in spec.partitions:
                    row = current[partition.partition_key]
                    if (
                        row.content_hash != partition.content_hash
                        or row.schema_fingerprint != partition.schema_fingerprint
                        or row.row_count != partition.row_count
                    ):
                        _RepositoriesSupport._raise_repository_conflict(
                            "canonical dataset hash matches different partition metadata"
                        )
                    row.input_hash = partition.input_hash
                session.flush()
                return CanonicalPublishResult(
                    self._canonical_record(session, spec.dataset), False, ()
                )
            old_paths = tuple(
                Path(row.path)
                for row in session.scalars(
                    select(CanonicalPartitionORM).where(
                        CanonicalPartitionORM.dataset == spec.dataset.value
                    )
                )
            )
            if existing is None:
                existing = CanonicalDatasetORM(
                    dataset=spec.dataset.value,
                    content_hash=content_hash,
                    source=spec.source,
                    start_date=_RepositoriesSupport._date_text(spec.start_date),
                    end_date=_RepositoriesSupport._date_text(spec.end_date),
                    updated_at=_RepositoriesSupport._timestamp(updated_at),
                )
                session.add(existing)
                session.flush()
            else:
                existing.content_hash = content_hash
                existing.source = spec.source
                existing.start_date = _RepositoriesSupport._date_text(spec.start_date)
                existing.end_date = _RepositoriesSupport._date_text(spec.end_date)
                existing.updated_at = _RepositoriesSupport._timestamp(updated_at)
                session.execute(
                    delete(CanonicalPartitionORM).where(
                        CanonicalPartitionORM.dataset == spec.dataset.value
                    )
                )
            for ordinal, partition in enumerate(
                sorted(spec.partitions, key=lambda item: item.partition_key)
            ):
                session.add(
                    CanonicalPartitionORM(
                        dataset=spec.dataset.value,
                        partition_key=partition.partition_key,
                        ordinal=ordinal,
                        content_hash=partition.content_hash,
                        path=partition.path.as_posix(),
                        schema_fingerprint=partition.schema_fingerprint,
                        input_hash=partition.input_hash,
                        row_count=partition.row_count,
                    )
                )
            session.flush()
            catalog_hash = self._catalog_hash(session)
            state = self._state_row(session, updated_at)
            state.catalog_hash = catalog_hash
            state.validated_catalog_hash = None
            state.quality_run_id = None
            state.updated_at = _RepositoriesSupport._timestamp(updated_at)
            state.validated_at = None
            current_paths = set(session.scalars(select(CanonicalPartitionORM.path)))
            orphan_paths = tuple(
                sorted(
                    (
                        path
                        for path in old_paths
                        if path.as_posix() not in current_paths
                    ),
                    key=lambda path: path.as_posix(),
                )
            )
            return CanonicalPublishResult(
                self._canonical_record(session, spec.dataset), True, orphan_paths
            )

    @staticmethod
    def _verify_raw_head_snapshot(session: Session, expected: RawHeadSnapshot) -> None:
        rows = session.execute(
            select(RawRequestORM, RawObjectORM)
            .join(
                RawObjectORM,
                (
                    (RawObjectORM.source == RawRequestORM.source)
                    & (RawObjectORM.endpoint == RawRequestORM.endpoint)
                    & (RawObjectORM.request_hash == RawRequestORM.request_hash)
                    & (RawObjectORM.content_hash == RawRequestORM.current_content_hash)
                ),
            )
            .where(
                RawRequestORM.source == expected.source,
                RawRequestORM.endpoint.in_(expected.endpoints),
            )
            .order_by(
                RawRequestORM.source,
                RawRequestORM.endpoint,
                RawRequestORM.request_hash,
            )
        ).all()
        actual = tuple(
            RawHeadIdentity(
                source=request.source,
                endpoint=request.endpoint,
                request_hash=request.request_hash,
                content_hash=obj.content_hash,
                schema_fingerprint=obj.schema_fingerprint,
                retrieved_at=_RepositoriesSupport._parse_timestamp(obj.retrieved_at),
            )
            for request, obj in rows
        )
        if actual != expected.heads:
            raise QuantError(
                ErrorDetail(
                    code="DATA_CURATE_INPUT_CHANGED",
                    severity=Severity.SEVERE,
                    message="Raw current heads changed while Curate was running",
                    context={
                        "source": expected.source,
                        "endpoints": list(expected.endpoints),
                        "expected_head_count": len(expected.heads),
                        "actual_head_count": len(actual),
                    },
                    remediation="retry Curate against the new Raw current heads",
                    retryable=True,
                )
            )

    def get_canonical_dataset(self, dataset: DatasetKind) -> CanonicalDatasetRecord:
        """取得指定数据集的当前 Canonical 记录。

        入参：
            dataset：目标数据集，类型为 ``DatasetKind``。
        返回值：
            返回读取Canonical数据集后的Canonical数据集（``CanonicalDatasetRecord``）。
        异常：
            无。
        """
        with Session(self._engine) as session:
            return self._canonical_record(session, dataset)

    def find_canonical_dataset(
        self, dataset: DatasetKind
    ) -> CanonicalDatasetRecord | None:
        """查找指定数据集的当前 Canonical 记录。

        入参：
            dataset：目标数据集，类型为 ``DatasetKind``。
        返回值：
            返回查询Canonical数据集后的Canonical数据集（``CanonicalDatasetRecord | None``）。
        异常：
            无。
        """
        with Session(self._engine) as session:
            if session.get(CanonicalDatasetORM, dataset.value) is None:
                return None
            return self._canonical_record(session, dataset)

    def list_canonical_datasets(self) -> tuple[CanonicalDatasetRecord, ...]:
        """列出当前目录中的全部 Canonical 数据集。

        入参：
            无。
        返回值：
            返回按确定性顺序列出Canonical``datasets``后的Canonical``datasets``（``tuple[CanonicalDatasetRecord, ...]``）。
        异常：
            无。
        """
        with Session(self._engine) as session:
            datasets = tuple(
                DatasetKind(value)
                for value in session.scalars(
                    select(CanonicalDatasetORM.dataset).order_by(
                        CanonicalDatasetORM.dataset
                    )
                )
            )
            return tuple(self._canonical_record(session, item) for item in datasets)

    def record_dataset_stage(
        self,
        dataset: DatasetKind,
        stage: str,
        *,
        completed_at: datetime,
        localized_through: date | None = None,
    ) -> DatasetOperationalStateRecord:
        """记录数据集成功完成的阶段并返回最新运营状态。

        入参：
            dataset：目标 Canonical 数据集。
            stage：``LOCALIZE``、``CURATE`` 或 ``VALIDATE``。
            completed_at：阶段成功完成时间。
            localized_through：Localize 已检查到的业务日期。
        返回值：
            返回更新后的不可变运营状态。
        异常：
            ValueError：阶段或时间不符合契约时抛出。
        """
        if stage not in {"LOCALIZE", "CURATE", "VALIDATE"}:
            raise ValueError(f"unsupported dataset stage: {stage}")
        completed = _RepositoriesSupport._utc_datetime(completed_at)
        timestamp = _RepositoriesSupport._timestamp(completed)
        with Session(self._engine) as session, session.begin():
            row = session.get(DatasetOperationalStateORM, dataset.value)
            if row is None:
                row = DatasetOperationalStateORM(
                    dataset=dataset.value,
                    last_localized_at=None,
                    localized_through=None,
                    last_curated_at=None,
                    last_validated_at=None,
                    updated_at=timestamp,
                )
                session.add(row)
            if stage == "LOCALIZE":
                row.last_localized_at = timestamp
                row.localized_through = (
                    localized_through.isoformat()
                    if localized_through is not None
                    else row.localized_through
                )
            elif stage == "CURATE":
                row.last_curated_at = timestamp
            else:
                row.last_validated_at = timestamp
            row.updated_at = timestamp
            session.flush()
            return self._dataset_operational_record(row)

    def list_dataset_operational_states(
        self,
    ) -> tuple[DatasetOperationalStateRecord, ...]:
        """列出全部数据集运营状态。

        入参：
            无。
        返回值：
            按数据集名称排序的不可变状态集合。
        异常：
            无主动抛出的异常；数据库异常按原契约传播。
        """
        with Session(self._engine) as session:
            return tuple(
                self._dataset_operational_record(row)
                for row in session.scalars(
                    select(DatasetOperationalStateORM).order_by(
                        DatasetOperationalStateORM.dataset
                    )
                )
            )

    def catalog_state(self) -> DataCatalogState:
        """读取全局 Canonical 目录及质量门禁状态。

        入参：
            无。
        返回值：
            返回``state``（``DataCatalogState``）。
        异常：
            无。
        """
        now = datetime.now(UTC)
        with Session(self._engine) as session, session.begin():
            return self._state_record(self._state_row(session, now))

    def find_data_initialization(self) -> DataInitializationStateRecord | None:
        """读取首次初始化状态。

        入参：无。返回值：冻结状态；尚未启动时返回空值。
        异常：SQLite 读取异常保持原语义。
        """

        with Session(self._engine) as session:
            row = session.get(DataInitializationStateORM, 1)
            return None if row is None else self._initialization_record(row)

    def begin_data_initialization(
        self,
        *,
        years: int,
        start_date: date,
        end_date: date,
        started_at: datetime,
    ) -> DataInitializationStateRecord:
        """原子登记或恢复一份冻结的首次初始化。

        入参：正整数年数、基础闭区间和首次开始时间。
        返回值：新建或既有的不可变初始化状态。
        异常：再次登记不同年数或窗口时抛出 ``ValueError``。
        """

        with Session(self._engine) as session, session.begin():
            row = session.get(DataInitializationStateORM, 1)
            if row is None:
                row = DataInitializationStateORM(
                    id=1,
                    status="IN_PROGRESS",
                    years=years,
                    start_date=start_date.isoformat(),
                    end_date=end_date.isoformat(),
                    started_at=_RepositoriesSupport._timestamp(started_at),
                    completed_at=None,
                    catalog_hash=None,
                    quality_run_id=None,
                )
                session.add(row)
                session.flush()
            elif (
                row.years != years
                or row.start_date != start_date.isoformat()
                or row.end_date != end_date.isoformat()
            ):
                raise ValueError("data initialization window is already frozen")
            return self._initialization_record(row)

    def complete_data_initialization(
        self,
        *,
        catalog_hash: str,
        quality_run_id: QualityRunId,
        completed_at: datetime,
    ) -> DataInitializationStateRecord:
        """在全目录校验成功后原子标记首次初始化完成。

        入参：最终目录哈希、质量运行标识和完成时间。
        返回值：包含完成证据的不可变初始化状态。
        异常：初始化尚未开始时抛出 ``RuntimeError``。
        """

        with Session(self._engine) as session, session.begin():
            row = session.get(DataInitializationStateORM, 1)
            if row is None:
                raise RuntimeError("data initialization has not started")
            row.status = "COMPLETED"
            row.completed_at = _RepositoriesSupport._timestamp(completed_at)
            row.catalog_hash = catalog_hash
            row.quality_run_id = str(quality_run_id)
            session.flush()
            return self._initialization_record(row)

    def require_validated_catalog(self) -> DataCatalogState:
        """取得当前目录状态，并要求全局质量门禁处于开放状态。

        入参：
            无。
        返回值：
            返回``validated``数据目录（``DataCatalogState``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``QuantError``。
        """
        state = self.catalog_state()
        if not state.is_validated:
            raise QuantError(
                ErrorDetail(
                    code="DATA_NOT_VALIDATED",
                    severity=Severity.FATAL,
                    message="current canonical data has not passed validate-all",
                    context={"catalog_hash": state.catalog_hash},
                    remediation="run quant data validate-all before reading research data",
                    retryable=False,
                )
            )
        return state

    def register_quality_run(self, spec: QualityRunSpec) -> QualityRunRecord:
        """持久化一次质量运行、其数据集身份和全部质量问题。

        入参：
            spec：不可变规格。
        返回值：
            返回质量校验运行（``QualityRunRecord``）。
        异常：
            无。
        """
        identifier = QualityRunId.new()
        blocking = any(
            issue.severity in {Severity.SEVERE, Severity.FATAL} for issue in spec.issues
        )
        status = "FAILED" if blocking else "PASSED"
        with Session(self._engine) as session, session.begin():
            session.add(
                QualityRunORM(
                    id=str(identifier),
                    scope=spec.scope,
                    input_hash=spec.input_hash,
                    status=status,
                    results_complete=spec.results_complete,
                    started_at=_RepositoriesSupport._timestamp(spec.started_at),
                    completed_at=(
                        _RepositoriesSupport._timestamp(spec.completed_at)
                        if spec.completed_at is not None
                        else None
                    ),
                    created_at=_RepositoriesSupport._timestamp(datetime.now(UTC)),
                )
            )
            # Relationships are intentionally not exposed by these mappings, so
            # flush the parent before inserting rows that reference it.
            session.flush()
            for dataset, content_hash in sorted(spec.dataset_hashes.items()):
                DatasetKind(dataset)
                session.add(
                    QualityRunDatasetORM(
                        quality_run_id=str(identifier),
                        dataset=dataset,
                        content_hash=content_hash,
                    )
                )
            for issue in spec.issues:
                session.add(
                    QualityIssueORM(
                        quality_run_id=str(identifier),
                        rule_id=issue.rule_id,
                        severity=issue.severity.value,
                        dataset=issue.dataset.value,
                        scope_json=_RepositoriesSupport._json_text(issue.scope),
                        actual_json=_RepositoriesSupport._json_text(issue.actual),
                        threshold_json=_RepositoriesSupport._json_text(issue.threshold),
                        message=issue.message,
                        remediation=issue.remediation,
                    )
                )
            for result in spec.rule_results:
                session.add(
                    QualityRuleResultORM(
                        quality_run_id=str(identifier),
                        rule_id=result.rule_id,
                        dataset=result.dataset.value,
                        status=result.status.value,
                        severity=result.severity.value,
                        title=result.title,
                        description=result.description,
                        pass_criterion=result.pass_criterion,
                        scope_json=_RepositoriesSupport._json_text(result.scope),
                        actual_json=_RepositoriesSupport._json_text(result.actual),
                        threshold_json=_RepositoriesSupport._json_text(
                            result.threshold
                        ),
                        skip_reason=result.skip_reason,
                        evidence=result.evidence.value,
                    )
                )
            session.flush()
            return self._quality_record(session, str(identifier))

    def mark_catalog_validated(
        self, quality_run_id: QualityRunId, *, validated_at: datetime
    ) -> DataCatalogState:
        """用成功的全局质量运行原子开放当前目录读取门禁。

        入参：
            quality_run_id：用于持久化关联和日志追踪的质量校验运行标识。
            返回数据目录``validated``（``DataCatalogState``）。
        返回值：
            返回数据目录``validated``（``DataCatalogState``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``QuantError``、``ValueError``。
        """
        validated_at = _RepositoriesSupport._utc_datetime(validated_at)
        with Session(self._engine) as session, session.begin():
            run = self._quality_record(session, str(quality_run_id))
            state = self._state_row(session, validated_at)
            if run.scope != "ALL" or run.status != "PASSED":
                raise ValueError(
                    "only a passed validate-all run can open the data gate"
                )
            if run.input_hash != state.catalog_hash:
                raise QuantError(
                    ErrorDetail(
                        code="DATA_VALIDATE_INPUT_CHANGED",
                        severity=Severity.FATAL,
                        message="canonical data changed while validate-all was running",
                        context={
                            "validated_hash": run.input_hash,
                            "current_hash": state.catalog_hash,
                        },
                        remediation="run validate-all again against the current data",
                        retryable=True,
                    )
                )
            state.validated_catalog_hash = state.catalog_hash
            state.quality_run_id = str(quality_run_id)
            state.validated_at = _RepositoriesSupport._timestamp(validated_at)
            state.updated_at = _RepositoriesSupport._timestamp(validated_at)
            return self._state_record(state)

    def get_quality_run(self, identifier: QualityRunId) -> QualityRunRecord:
        """按 ID 取得完整质量运行记录。

        入参：
            identifier：``identifier``。
        返回值：
            返回读取质量校验运行后的质量校验运行（``QualityRunRecord``）。
        异常：
            无。
        """
        with Session(self._engine) as session:
            return self._quality_record(session, str(identifier))

    def latest_quality_run(self) -> QualityRunRecord | None:
        """取得最近创建的一次质量运行。

        入参：
            无。
        返回值：
            返回质量校验运行（``QualityRunRecord | None``）。
        异常：
            无。
        """
        with Session(self._engine) as session:
            identifier = session.scalar(
                select(QualityRunORM.id).order_by(
                    QualityRunORM.created_at.desc(), QualityRunORM.id.desc()
                )
            )
            return (
                None
                if identifier is None
                else self._quality_record(session, identifier)
            )

    def list_quality_runs(self) -> tuple[QualityRunRecord, ...]:
        """按创建时间倒序列出全部质量运行。

        入参：
            无。
        返回值：
            完整质量运行记录集合。
        异常：
            无主动抛出的异常；数据库异常按原契约传播。
        """
        with Session(self._engine) as session:
            identifiers = tuple(
                session.scalars(
                    select(QualityRunORM.id).order_by(
                        QualityRunORM.created_at.desc(), QualityRunORM.id.desc()
                    )
                )
            )
            return tuple(self._quality_record(session, item) for item in identifiers)

    def register_experiment(self, spec: ExperimentSpec) -> ExperimentRecord:
        """注册一个初始状态为 ``CREATED`` 的研究实验。

        入参：
            spec：不可变规格。
        返回值：
            返回实验（``ExperimentRecord``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        config_bytes = canonical_json_bytes(spec.config)
        if hashlib.sha256(config_bytes).hexdigest() != spec.config_hash:
            raise ValueError("config_hash must match canonical config")
        identifier = str(uuid4())
        with Session(self._engine) as session, session.begin():
            row = ExperimentORM(
                id=identifier,
                strategy_id=spec.strategy_id,
                config_json=config_bytes.decode("utf-8"),
                config_hash=spec.config_hash,
                data_hash=spec.data_hash,
                source_tree_hash=spec.source_tree_hash,
                git_commit_hash=spec.git_commit_hash,
                lockfile_hash=spec.lockfile_hash,
                rulebook_hash=spec.rulebook_hash,
                fingerprint=spec.fingerprint,
                status=ExperimentStatus.CREATED.value,
                research_mark=ResearchMark.UNREVIEWED.value,
                created_at=_RepositoriesSupport._timestamp(spec.created_at),
                queued_at=None,
                started_at=None,
                completed_at=None,
            )
            session.add(row)
            session.flush()
            return self._experiment_record(row)

    def count_experiments_by_fingerprint(self, fingerprint: str) -> int:
        """统计具有指定研究指纹的实验数量。

        入参：
            fingerprint：由策略、数据、源码、依赖锁和交易规则共同形成的研究指纹。
        返回值：
            返回与该研究指纹完全相同的已登记实验数量。
        异常：
            无。
        """
        _RepositoriesSupport._validate_hash(fingerprint, "fingerprint")
        with Session(self._engine) as session:
            return int(
                session.scalar(
                    select(func.count())
                    .select_from(ExperimentORM)
                    .where(ExperimentORM.fingerprint == fingerprint)
                )
                or 0
            )

    def create_task(self, spec: TaskSpec) -> TaskRecord:
        """为已存在的实验创建一个初始状态为 ``QUEUED`` 的任务。

        入参：
            spec：不可变规格。
        返回值：
            返回创建任务后的任务（``TaskRecord``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``KeyError``。
        """
        identifier = str(uuid4())
        with Session(self._engine) as session, session.begin():
            if session.get(ExperimentORM, spec.experiment_id) is None:
                raise KeyError(f"experiment does not exist: {spec.experiment_id}")
            row = TaskORM(
                id=identifier,
                experiment_id=spec.experiment_id,
                task_type=spec.task_type,
                payload_json=_RepositoriesSupport._json_text(spec.payload),
                status=TaskStatus.QUEUED.value,
                priority=spec.priority,
                progress_json="{}",
                created_at=_RepositoriesSupport._timestamp(spec.created_at),
                available_at=_RepositoriesSupport._timestamp(spec.available_at),
                updated_at=_RepositoriesSupport._timestamp(spec.created_at),
                heartbeat_at=None,
                completed_at=None,
                result_json=None,
            )
            session.add(row)
            session.flush()
            return self._task_record(row)

    @staticmethod
    def _raw_partition_record(
        request: RawRequestORM, obj: RawObjectORM
    ) -> RawPartitionRecord:
        value = json.loads(request.request_json)
        if not isinstance(value, dict):
            _RepositoriesSupport._raise_repository_conflict(
                "raw request JSON is not an object"
            )
        return RawPartitionRecord(
            source=request.source,
            endpoint=request.endpoint,
            request=MappingProxyType(value),
            request_hash=request.request_hash,
            content_hash=obj.content_hash,
            data_path=Path(obj.data_path),
            manifest_path=Path(obj.manifest_path),
            schema_fingerprint=obj.schema_fingerprint,
            row_count=obj.row_count,
            retrieved_at=_RepositoriesSupport._parse_timestamp(obj.retrieved_at),
        )

    def _canonical_record(
        self, session: Session, dataset: DatasetKind
    ) -> CanonicalDatasetRecord:
        row = session.get(CanonicalDatasetORM, dataset.value)
        if row is None:
            raise KeyError(f"canonical dataset does not exist: {dataset.value}")
        partitions = tuple(
            CanonicalPartitionRecord(
                partition_key=item.partition_key,
                content_hash=item.content_hash,
                path=Path(item.path),
                schema_fingerprint=item.schema_fingerprint,
                input_hash=item.input_hash,
                row_count=item.row_count,
            )
            for item in session.scalars(
                select(CanonicalPartitionORM)
                .where(CanonicalPartitionORM.dataset == dataset.value)
                .order_by(CanonicalPartitionORM.ordinal)
            )
        )
        if not partitions:
            _RepositoriesSupport._raise_repository_conflict(
                "canonical dataset has no partitions"
            )
        return CanonicalDatasetRecord(
            dataset=dataset,
            content_hash=row.content_hash,
            source=row.source,
            partitions=partitions,
            start_date=_RepositoriesSupport._parse_date(row.start_date),
            end_date=_RepositoriesSupport._parse_date(row.end_date),
            updated_at=_RepositoriesSupport._parse_timestamp(row.updated_at),
        )

    @staticmethod
    def _catalog_hash(session: Session) -> str:
        payload: JsonValue = [
            {"dataset": dataset, "content_hash": content_hash}
            for dataset, content_hash in session.execute(
                select(
                    CanonicalDatasetORM.dataset, CanonicalDatasetORM.content_hash
                ).order_by(CanonicalDatasetORM.dataset)
            )
        ]
        return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()

    def _state_row(self, session: Session, now: datetime) -> DataCatalogStateORM:
        state = session.get(DataCatalogStateORM, 1)
        if state is None:
            state = DataCatalogStateORM(
                id=1,
                catalog_hash=self._catalog_hash(session),
                validated_catalog_hash=None,
                quality_run_id=None,
                updated_at=_RepositoriesSupport._timestamp(now),
                validated_at=None,
            )
            session.add(state)
            session.flush()
        return state

    @staticmethod
    def _state_record(row: DataCatalogStateORM) -> DataCatalogState:
        return DataCatalogState(
            catalog_hash=row.catalog_hash,
            validated_catalog_hash=row.validated_catalog_hash,
            quality_run_id=(
                QualityRunId.parse(row.quality_run_id)
                if row.quality_run_id is not None
                else None
            ),
            updated_at=_RepositoriesSupport._parse_timestamp(row.updated_at),
            validated_at=(
                _RepositoriesSupport._parse_timestamp(row.validated_at)
                if row.validated_at is not None
                else None
            ),
        )

    @staticmethod
    def _initialization_record(
        row: DataInitializationStateORM,
    ) -> DataInitializationStateRecord:
        return DataInitializationStateRecord(
            status=row.status,
            years=row.years,
            start_date=date.fromisoformat(row.start_date),
            end_date=date.fromisoformat(row.end_date),
            started_at=_RepositoriesSupport._parse_timestamp(row.started_at),
            completed_at=_RepositoriesSupport._parse_optional_timestamp(
                row.completed_at
            ),
            catalog_hash=row.catalog_hash,
            quality_run_id=(
                None
                if row.quality_run_id is None
                else QualityRunId.parse(row.quality_run_id)
            ),
        )

    @staticmethod
    def _dataset_operational_record(
        row: DatasetOperationalStateORM,
    ) -> DatasetOperationalStateRecord:
        return DatasetOperationalStateRecord(
            dataset=DatasetKind(row.dataset),
            last_localized_at=_RepositoriesSupport._parse_optional_timestamp(
                row.last_localized_at
            ),
            localized_through=_RepositoriesSupport._parse_date(row.localized_through),
            last_curated_at=_RepositoriesSupport._parse_optional_timestamp(
                row.last_curated_at
            ),
            last_validated_at=_RepositoriesSupport._parse_optional_timestamp(
                row.last_validated_at
            ),
            updated_at=_RepositoriesSupport._parse_timestamp(row.updated_at),
        )

    def _quality_record(self, session: Session, identifier: str) -> QualityRunRecord:
        row = session.get(QualityRunORM, identifier)
        if row is None:
            raise KeyError(f"quality run does not exist: {identifier}")
        datasets = MappingProxyType(
            {
                item.dataset: item.content_hash
                for item in session.scalars(
                    select(QualityRunDatasetORM)
                    .where(QualityRunDatasetORM.quality_run_id == identifier)
                    .order_by(QualityRunDatasetORM.dataset)
                )
            }
        )
        issues = tuple(
            QualityIssue(
                rule_id=item.rule_id,
                severity=Severity(item.severity),
                dataset=DatasetKind(item.dataset),
                scope=json.loads(item.scope_json),
                actual=json.loads(item.actual_json),
                threshold=json.loads(item.threshold_json),
                message=item.message,
                remediation=item.remediation,
            )
            for item in session.scalars(
                select(QualityIssueORM)
                .where(QualityIssueORM.quality_run_id == identifier)
                .order_by(QualityIssueORM.id)
            )
        )
        rule_results = tuple(
            QualityRuleResult(
                rule_id=item.rule_id,
                dataset=DatasetKind(item.dataset),
                status=QualityRuleStatus(item.status),
                severity=Severity(item.severity),
                title=item.title,
                description=item.description,
                pass_criterion=item.pass_criterion,
                scope=json.loads(item.scope_json),
                actual=json.loads(item.actual_json),
                threshold=json.loads(item.threshold_json),
                skip_reason=item.skip_reason,
                evidence=QualityEvidenceSource(item.evidence),
            )
            for item in session.scalars(
                select(QualityRuleResultORM)
                .where(QualityRuleResultORM.quality_run_id == identifier)
                .order_by(QualityRuleResultORM.id)
            )
        )
        return QualityRunRecord(
            id=QualityRunId.parse(row.id),
            scope=row.scope,
            input_hash=row.input_hash,
            status=row.status,
            dataset_hashes=datasets,
            started_at=_RepositoriesSupport._parse_timestamp(row.started_at),
            completed_at=(
                _RepositoriesSupport._parse_timestamp(row.completed_at)
                if row.completed_at is not None
                else None
            ),
            issues=issues,
            rule_results=rule_results,
            results_complete=row.results_complete,
        )

    @staticmethod
    def _experiment_record(row: ExperimentORM) -> ExperimentRecord:
        return ExperimentRecord(
            id=row.id,
            strategy_id=row.strategy_id,
            config=json.loads(row.config_json),
            config_hash=row.config_hash,
            data_hash=row.data_hash,
            source_tree_hash=row.source_tree_hash,
            git_commit_hash=row.git_commit_hash,
            lockfile_hash=row.lockfile_hash,
            rulebook_hash=row.rulebook_hash,
            fingerprint=row.fingerprint,
            status=ExperimentStatus(row.status),
            research_mark=ResearchMark(row.research_mark),
            created_at=_RepositoriesSupport._parse_timestamp(row.created_at),
            queued_at=_RepositoriesSupport._parse_optional_timestamp(row.queued_at),
            started_at=_RepositoriesSupport._parse_optional_timestamp(row.started_at),
            completed_at=_RepositoriesSupport._parse_optional_timestamp(
                row.completed_at
            ),
        )

    @staticmethod
    def _task_record(row: TaskORM) -> TaskRecord:
        return TaskRecord(
            id=row.id,
            experiment_id=row.experiment_id,
            task_type=row.task_type,
            payload=json.loads(row.payload_json),
            status=TaskStatus(row.status),
            priority=row.priority,
            progress=json.loads(row.progress_json),
            created_at=_RepositoriesSupport._parse_timestamp(row.created_at),
            available_at=_RepositoriesSupport._parse_timestamp(row.available_at),
            updated_at=_RepositoriesSupport._parse_timestamp(row.updated_at),
            heartbeat_at=_RepositoriesSupport._parse_optional_timestamp(
                row.heartbeat_at
            ),
            completed_at=_RepositoriesSupport._parse_optional_timestamp(
                row.completed_at
            ),
            result=(
                json.loads(row.result_json) if row.result_json is not None else None
            ),
        )


def canonical_dataset_hash(spec: CanonicalDatasetSpec) -> str:
    """计算 Canonical 数据集的稳定内容身份；该函数作为稳定公开 API保留在模块级。

    入参：
        spec：不可变规格。
    返回值：
        返回数据集哈希（``str``）。
    异常：
        无。
    """
    payload = cast(
        JsonValue,
        {
            "dataset": spec.dataset.value,
            "source": spec.source,
            "start_date": _RepositoriesSupport._date_text(spec.start_date),
            "end_date": _RepositoriesSupport._date_text(spec.end_date),
            "partitions": [
                {
                    "partition_key": item.partition_key,
                    "content_hash": item.content_hash,
                    "schema_fingerprint": item.schema_fingerprint,
                    "row_count": item.row_count,
                }
                for item in sorted(
                    spec.partitions, key=lambda value: value.partition_key
                )
            ],
        },
    )
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class _RepositoriesSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _validate_hash(value: str, field: str) -> None:
        if _SHA256.fullmatch(value) is None:
            raise ValueError(f"{field} must be a SHA-256 digest")

    @staticmethod
    def _raw_head_identity_key(
        value: RawHeadIdentity,
    ) -> tuple[str, str, str]:
        return value.source, value.endpoint, value.request_hash

    @staticmethod
    def _timestamp(value: datetime) -> str:
        return (
            _RepositoriesSupport._utc_datetime(value).isoformat().replace("+00:00", "Z")
        )

    @staticmethod
    def _utc_datetime(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime must be timezone-aware")
        return value.astimezone(UTC)

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        return datetime.fromisoformat(value).astimezone(UTC)

    @staticmethod
    def _parse_optional_timestamp(value: str | None) -> datetime | None:
        return None if value is None else _RepositoriesSupport._parse_timestamp(value)

    @staticmethod
    def _date_text(value: date | None) -> str | None:
        return None if value is None else value.isoformat()

    @staticmethod
    def _parse_date(value: str | None) -> date | None:
        return None if value is None else date.fromisoformat(value)

    @staticmethod
    def _json_text(value: object) -> str:
        return canonical_json_bytes(thaw_json(value)).decode("utf-8")

    @staticmethod
    def _raw_object_matches(row: RawObjectORM, spec: RawPartitionSpec) -> bool:
        return (
            row.data_path == spec.data_path.as_posix()
            and row.manifest_path == spec.manifest_path.as_posix()
            and row.schema_fingerprint == spec.schema_fingerprint
            and row.row_count == spec.row_count
            and row.retrieved_at == _RepositoriesSupport._timestamp(spec.retrieved_at)
        )

    @staticmethod
    def _raise_repository_conflict(message: str) -> Never:
        raise QuantError(
            ErrorDetail(
                code="DATA_REPOSITORY_CONFLICT",
                severity=Severity.FATAL,
                message=message,
                context={},
                remediation="rebuild the incompatible data root",
                retryable=False,
            )
        )
