"""提供持久化与orm相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """表示基础设施流程中的``base``及其业务不变量。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Declarative base kept private to the persistence implementation.
    """


class RawRequestORM(Base):
    """将``raw``请求``orm``记录映射到 SQLite 持久化表。

    入参：
        source：数据来源。
        endpoint：供应商端点。
        request_hash：规范供应商请求的确定性身份，用于幂等定位 Raw 响应。
        request_json：请求JSON。
        current_content_hash：参与幂等、漂移或完整性校验的当前值内容哈希；使用 SHA-256 十六进制文本。
        updated_at：记录最近持久化变更的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "raw_request"
    __table_args__ = (
        Index("ix_raw_request_endpoint_updated", "source", "endpoint", "updated_at"),
    )

    source: Mapped[str] = mapped_column(String(64), primary_key=True)
    endpoint: Mapped[str] = mapped_column(String(128), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    request_json: Mapped[str] = mapped_column(String, nullable=False)
    current_content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    updated_at: Mapped[str] = mapped_column(String(32), nullable=False)


class RawObjectORM(Base):
    """返回完成字段规范化和不变量校验的对象。

    入参：
        返回完成字段规范化和不变量校验的对象。
        返回完成字段规范化和不变量校验的对象。
        request_hash：规范供应商请求的确定性身份，用于幂等定位 Raw 响应。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        data_path：经可信根边界校验后使用的数据路径。
        manifest_path：记录文件身份、Schema、行数和输入身份的清单路径。
        schema_fingerprint：字段名称和类型形成的确定性 Schema 身份。
        row_count：产物或分区中经验证的数据行数。
        返回完成字段规范化和不变量校验的对象。
        created_at：记录创建时的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "raw_object"
    __table_args__ = (
        ForeignKeyConstraint(
            ("source", "endpoint", "request_hash"),
            (
                "raw_request.source",
                "raw_request.endpoint",
                "raw_request.request_hash",
            ),
            ondelete="RESTRICT",
        ),
        CheckConstraint("row_count >= 0", name="ck_raw_object_row_count"),
    )

    source: Mapped[str] = mapped_column(String(64), primary_key=True)
    endpoint: Mapped[str] = mapped_column(String(128), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    content_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    data_path: Mapped[str] = mapped_column(String, nullable=False)
    manifest_path: Mapped[str] = mapped_column(String, nullable=False)
    schema_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False)
    retrieved_at: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class CanonicalDatasetORM(Base):
    """将Canonical数据集``orm``记录映射到 SQLite 持久化表。

    入参：
        dataset：目标数据集，类型为 ``Mapped[str]``。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        source：数据来源。
        start_date：查询或运行覆盖区间的首日（含）。
        end_date：查询或运行覆盖区间的末日（含）。
        updated_at：记录最近持久化变更的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "canonical_dataset"

    dataset: Mapped[str] = mapped_column(String(64), primary_key=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    start_date: Mapped[str | None] = mapped_column(String(10))
    end_date: Mapped[str | None] = mapped_column(String(10))
    updated_at: Mapped[str] = mapped_column(String(32), nullable=False)


class CanonicalPartitionORM(Base):
    """将Canonical分区``orm``记录映射到 SQLite 持久化表。

    入参：
        dataset：目标数据集，类型为 ``Mapped[str]``。
        partition_key：分区``key``。
        ordinal：``ordinal``。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        path：待处理的文件系统路径，类型为 ``Mapped[str]``。
        schema_fingerprint：字段名称和类型形成的确定性 Schema 身份。
        input_hash：决定 Canonical 分区是否需要重建的 Raw 输入身份。
        row_count：产物或分区中经验证的数据行数。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "canonical_partition"

    dataset: Mapped[str] = mapped_column(
        ForeignKey("canonical_dataset.dataset", ondelete="CASCADE"), primary_key=True
    )
    partition_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    path: Mapped[str] = mapped_column(String, nullable=False)
    schema_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False)


class DatasetOperationalStateORM(Base):
    """持久化数据集最近成功的阶段和业务水位。

    入参：由 ORM 字段声明给出。返回值：构造持久化实体。异常：数据库约束异常按原契约传播。
    """

    __tablename__ = "dataset_operational_state"

    dataset: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_localized_at: Mapped[str | None] = mapped_column(String(32))
    localized_through: Mapped[str | None] = mapped_column(String(10))
    last_curated_at: Mapped[str | None] = mapped_column(String(32))
    last_validated_at: Mapped[str | None] = mapped_column(String(32))
    updated_at: Mapped[str] = mapped_column(String(32), nullable=False)


class QualityRunORM(Base):
    """将质量校验运行``orm``记录映射到 SQLite 持久化表。

    入参：
        id：用于持久化关联和日志追踪的标识。
        scope：范围。
        input_hash：决定 Canonical 分区是否需要重建的 Raw 输入身份。
        status：当前记录所处的受控生命周期状态。
        started_at：执行实际开始的 UTC 时间戳。
        completed_at：执行进入终态的 UTC 时间戳。
        created_at：记录创建时的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "quality_run"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    scope: Mapped[str] = mapped_column(String(16), nullable=False)
    input_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    results_complete: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("0")
    )
    started_at: Mapped[str] = mapped_column(String(32), nullable=False)
    completed_at: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class QualityRunDatasetORM(Base):
    """将质量校验运行数据集``orm``记录映射到 SQLite 持久化表。

    入参：
        quality_run_id：用于持久化关联和日志追踪的质量校验运行标识。
        dataset：目标数据集，类型为 ``Mapped[str]``。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "quality_run_dataset"

    quality_run_id: Mapped[str] = mapped_column(
        ForeignKey("quality_run.id", ondelete="CASCADE"), primary_key=True
    )
    dataset: Mapped[str] = mapped_column(String(64), primary_key=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)


class QualityIssueORM(Base):
    """将质量校验问题记录映射到 SQLite 持久化表。

    入参：
        id：用于持久化关联和日志追踪的标识。
        quality_run_id：用于持久化关联和日志追踪的质量校验运行标识。
        rule_id：用于持久化关联和日志追踪的规则标识。
        severity：质量问题或应用错误的严重程度。
        dataset：目标数据集，类型为 ``Mapped[str]``。
        scope_json：范围JSON。
        actual_json：实际值JSON。
        threshold_json：判定阈值JSON。
        message：面向用户且已脱敏的错误或状态说明。
        remediation：调用者可执行的修复建议。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "quality_issue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    quality_run_id: Mapped[str] = mapped_column(
        ForeignKey("quality_run.id", ondelete="CASCADE"), nullable=False
    )
    rule_id: Mapped[str] = mapped_column(String(128), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    dataset: Mapped[str] = mapped_column(String(64), nullable=False)
    scope_json: Mapped[str] = mapped_column(String, nullable=False)
    actual_json: Mapped[str] = mapped_column(String, nullable=False)
    threshold_json: Mapped[str] = mapped_column(String, nullable=False)
    message: Mapped[str] = mapped_column(String, nullable=False)
    remediation: Mapped[str] = mapped_column(String, nullable=False)


class QualityRuleResultORM(Base):
    """持久化一次质量规则与数据集组合的完整执行证据。

    入参：由 ORM 字段声明给出。返回值：构造持久化实体。异常：数据库约束异常按原契约传播。
    """

    __tablename__ = "quality_rule_result"
    __table_args__ = (
        UniqueConstraint(
            "quality_run_id", "dataset", "rule_id", name="uq_quality_rule_result_key"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    quality_run_id: Mapped[str] = mapped_column(
        ForeignKey("quality_run.id", ondelete="CASCADE"), nullable=False
    )
    rule_id: Mapped[str] = mapped_column(String(128), nullable=False)
    dataset: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False)
    pass_criterion: Mapped[str] = mapped_column(String, nullable=False)
    scope_json: Mapped[str] = mapped_column(String, nullable=False)
    actual_json: Mapped[str] = mapped_column(String, nullable=False)
    threshold_json: Mapped[str] = mapped_column(String, nullable=False)
    skip_reason: Mapped[str | None] = mapped_column(String)
    evidence: Mapped[str] = mapped_column(String(32), nullable=False)


class DataCatalogStateORM(Base):
    """将数据数据目录``state``记录映射到 SQLite 持久化表。

    入参：
        id：用于持久化关联和日志追踪的标识。
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

    __tablename__ = "data_catalog_state"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_data_catalog_state_singleton"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    catalog_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    validated_catalog_hash: Mapped[str | None] = mapped_column(String(64))
    quality_run_id: Mapped[str | None] = mapped_column(
        ForeignKey("quality_run.id", ondelete="SET NULL")
    )
    updated_at: Mapped[str] = mapped_column(String(32), nullable=False)
    validated_at: Mapped[str | None] = mapped_column(String(32))


class ExperimentORM(Base):
    """将实验``orm``记录映射到 SQLite 持久化表。

    入参：
        id：用于持久化关联和日志追踪的标识。
        strategy_id：用于持久化关联和日志追踪的策略标识。
        config_json：配置JSON。
        config_hash：确定性序列化后的实验或策略配置身份。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        source_tree_hash：参与幂等、漂移或完整性校验的数据来源``tree``哈希；使用 SHA-256 十六进制文本。
        返回完成字段规范化和不变量校验的对象。
        lockfile_hash：参与幂等、漂移或完整性校验的依赖锁文件哈希；使用 SHA-256 十六进制文本。
        rulebook_hash：唯一 A 股交易规则文件的内容身份。
        fingerprint：由策略、数据、源码、依赖锁和交易规则共同形成的研究指纹。
        status：当前记录所处的受控生命周期状态。
        research_mark：用户对实验标记的基线、候选或废弃研究结论。
        created_at：记录创建时的 UTC 时间戳。
        queued_at：记录入队时间``at``的带时区 UTC 时间戳。
        started_at：执行实际开始的 UTC 时间戳。
        completed_at：执行进入终态的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "experiment"
    __table_args__ = (
        CheckConstraint(
            "status IN ('CREATED', 'QUEUED', 'RUNNING', 'SUCCEEDED', "
            "'FAILED', 'CANCELLED')",
            name="ck_experiment_status",
        ),
        CheckConstraint(
            "research_mark IN ('UNREVIEWED', 'BASELINE', 'CANDIDATE', 'DISCARDED')",
            name="ck_experiment_research_mark",
        ),
        CheckConstraint(
            "source_tree_hash IS NOT NULL OR git_commit_hash IS NOT NULL",
            name="ck_experiment_source_identity",
        ),
        Index("ix_experiment_fingerprint", "fingerprint"),
        Index("ix_experiment_strategy_created", "strategy_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False)
    config_json: Mapped[str] = mapped_column(String, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    data_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_tree_hash: Mapped[str | None] = mapped_column(String(64))
    git_commit_hash: Mapped[str | None] = mapped_column(String(64))
    lockfile_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    rulebook_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    research_mark: Mapped[str] = mapped_column(String(16), nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
    queued_at: Mapped[str | None] = mapped_column(String(32))
    started_at: Mapped[str | None] = mapped_column(String(32))
    completed_at: Mapped[str | None] = mapped_column(String(32))


class ExperimentTagORM(Base):
    """将实验标签``orm``记录映射到 SQLite 持久化表。

    入参：
        experiment_id：目标实验标识，类型为 ``Mapped[str]``。
        tag：标签。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "experiment_tag"

    experiment_id: Mapped[str] = mapped_column(
        ForeignKey("experiment.id", ondelete="CASCADE"), primary_key=True
    )
    tag: Mapped[str] = mapped_column(String(64), primary_key=True)


class ExperimentMetricORM(Base):
    """将实验指标``orm``记录映射到 SQLite 持久化表。

    入参：
        id：用于持久化关联和日志追踪的标识。
        experiment_id：目标实验标识，类型为 ``Mapped[str]``。
        name：供用户识别研究、任务或数据对象的非空名称。
        value：待校验或转换的值，类型为 ``Mapped[float]``。
        unit：计量单位。
        created_at：记录创建时的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "experiment_metric"
    __table_args__ = (
        UniqueConstraint("experiment_id", "name", name="uq_experiment_metric_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[str] = mapped_column(
        ForeignKey("experiment.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class ExperimentArtifactORM(Base):
    """将实验产物``orm``记录映射到 SQLite 持久化表。

    入参：
        id：用于持久化关联和日志追踪的标识。
        experiment_id：目标实验标识，类型为 ``Mapped[str]``。
        name：供用户识别研究、任务或数据对象的非空名称。
        artifact_type：产物类型。
        path：待处理的文件系统路径，类型为 ``Mapped[str]``。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        metadata_json：元数据JSON。
        created_at：记录创建时的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "experiment_artifact"
    __table_args__ = (
        UniqueConstraint("experiment_id", "name", name="uq_experiment_artifact_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[str] = mapped_column(
        ForeignKey("experiment.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(64), nullable=False)
    path: Mapped[str] = mapped_column(String, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    metadata_json: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class TaskORM(Base):
    """将任务``orm``记录映射到 SQLite 持久化表。

    入参：
        id：用于持久化关联和日志追踪的标识。
        experiment_id：目标实验标识，类型为 ``Mapped[str | None]``。
        task_type：任务类型。
        payload_json：任务载荷JSON。
        status：当前记录所处的受控生命周期状态。
        priority：任务在同一可运行集合中的调度优先级。
        progress_json：执行进度JSON。
        created_at：记录创建时的 UTC 时间戳。
        available_at：该条观测首次可供研究使用的带时区时间戳。
        updated_at：记录最近持久化变更的 UTC 时间戳。
        heartbeat_at：Worker 最近证明仍持有任务的 UTC 时间戳。
        completed_at：执行进入终态的 UTC 时间戳。
        idempotency_key：幂等键``key``。
        worker_id：当前 Worker 实例的稳定所有者标识。
        返回完成字段规范化和不变量校验的对象。
        error_json：错误JSON。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "task"
    __table_args__ = (
        CheckConstraint(
            "status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', "
            "'CANCEL_REQUESTED', 'CANCELLED', 'ORPHANED')",
            name="ck_task_status",
        ),
        Index("ix_task_experiment", "experiment_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    experiment_id: Mapped[str | None] = mapped_column(
        ForeignKey("experiment.id", ondelete="CASCADE")
    )
    task_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[str] = mapped_column(String, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    progress_json: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
    available_at: Mapped[str] = mapped_column(String(32), nullable=False)
    updated_at: Mapped[str] = mapped_column(String(32), nullable=False)
    heartbeat_at: Mapped[str | None] = mapped_column(String(32))
    completed_at: Mapped[str | None] = mapped_column(String(32))
    idempotency_key: Mapped[str | None] = mapped_column(String(128))
    worker_id: Mapped[str | None] = mapped_column(String(128))
    locked_at: Mapped[str | None] = mapped_column(String(32))
    error_json: Mapped[str | None] = mapped_column(Text)
    result_json: Mapped[str | None] = mapped_column(Text)


class FactorStudyORM(Base):
    """将因子因子研究``orm``记录映射到 SQLite 持久化表。

    入参：
        id：用于持久化关联和日志追踪的标识。
        name：供用户识别研究、任务或数据对象的非空名称。
        config_json：配置JSON。
        config_hash：确定性序列化后的实验或策略配置身份。
        created_at：记录创建时的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "factor_study"
    __table_args__ = (Index("ix_factor_study_created", "created_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    config_json: Mapped[str] = mapped_column(String, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class FactorRunORM(Base):
    """将因子运行``orm``记录映射到 SQLite 持久化表。

    入参：
        id：用于持久化关联和日志追踪的标识。
        study_id：因子研究定义的 UUID 标识。
        task_id：目标任务标识，类型为 ``Mapped[str | None]``。
        config_json：配置JSON。
        config_hash：确定性序列化后的实验或策略配置身份。
        catalog_hash：提交时捕获并在运行阶段防漂移校验的 Canonical 数据目录身份。
        source_hash：参与计算的实现源码身份。
        status：当前记录所处的受控生命周期状态。
        manifest_path：记录文件身份、Schema、行数和输入身份的清单路径。
        manifest_hash：参与幂等、漂移或完整性校验的产物清单哈希；使用 SHA-256 十六进制文本。
        error_json：错误JSON。
        created_at：记录创建时的 UTC 时间戳。
        started_at：执行实际开始的 UTC 时间戳。
        completed_at：执行进入终态的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "factor_run"
    __table_args__ = (
        CheckConstraint(
            "status IN ('CREATED', 'QUEUED', 'RUNNING', 'SUCCEEDED', "
            "'FAILED', 'CANCELLED')",
            name="ck_factor_run_status",
        ),
        Index("ix_factor_run_study_created", "study_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    study_id: Mapped[str] = mapped_column(
        ForeignKey("factor_study.id", ondelete="CASCADE"), nullable=False
    )
    task_id: Mapped[str | None] = mapped_column(
        ForeignKey("task.id", ondelete="SET NULL"), unique=True
    )
    config_json: Mapped[str] = mapped_column(String, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    catalog_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    manifest_path: Mapped[str | None] = mapped_column(String)
    manifest_hash: Mapped[str | None] = mapped_column(String(64))
    error_json: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[str | None] = mapped_column(String(32))
    completed_at: Mapped[str | None] = mapped_column(String(32))


Index(
    "ix_task_queue",
    TaskORM.status,
    TaskORM.available_at,
    TaskORM.priority.desc(),
    TaskORM.created_at,
    TaskORM.id,
)
Index(
    "uq_task_active_idempotency",
    TaskORM.task_type,
    text("COALESCE(experiment_id, '')"),
    TaskORM.idempotency_key,
    unique=True,
    sqlite_where=text(
        "idempotency_key IS NOT NULL "
        "AND status IN ('QUEUED', 'RUNNING', 'CANCEL_REQUESTED')"
    ),
)


class TaskAttemptORM(Base):
    """将任务执行尝试``orm``记录映射到 SQLite 持久化表。

    入参：
        id：用于持久化关联和日志追踪的标识。
        task_id：目标任务标识，类型为 ``Mapped[str]``。
        attempt_no：执行尝试从一开始计数的尝试序号。
        status：当前记录所处的受控生命周期状态。
        worker_id：当前 Worker 实例的稳定所有者标识。
        started_at：执行实际开始的 UTC 时间戳。
        heartbeat_at：Worker 最近证明仍持有任务的 UTC 时间戳。
        completed_at：执行进入终态的 UTC 时间戳。
        log_path：经可信根边界校验后使用的日志路径。
        progress_json：执行进度JSON。
        error_json：错误JSON。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "task_attempt"
    __table_args__ = (
        CheckConstraint(
            "status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', "
            "'CANCEL_REQUESTED', 'CANCELLED', 'ORPHANED')",
            name="ck_task_attempt_status",
        ),
        CheckConstraint("attempt_no > 0", name="ck_task_attempt_positive"),
        UniqueConstraint("task_id", "attempt_no", name="uq_task_attempt_no"),
        Index("ix_task_attempt_task", "task_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_id: Mapped[str] = mapped_column(
        ForeignKey("task.id", ondelete="CASCADE"), nullable=False
    )
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(128))
    started_at: Mapped[str] = mapped_column(String(32), nullable=False)
    heartbeat_at: Mapped[str | None] = mapped_column(String(32))
    completed_at: Mapped[str | None] = mapped_column(String(32))
    log_path: Mapped[str | None] = mapped_column(String)
    progress_json: Mapped[str] = mapped_column(String, nullable=False)
    error_json: Mapped[str | None] = mapped_column(String)
    result_json: Mapped[str | None] = mapped_column(Text)


class AuditEventORM(Base):
    """将审计事件事件``orm``记录映射到 SQLite 持久化表。

    入参：
        id：用于持久化关联和日志追踪的标识。
        experiment_id：目标实验标识，类型为 ``Mapped[str | None]``。
        task_id：目标任务标识，类型为 ``Mapped[str | None]``。
        event_type：事件类型。
        actor：操作主体。
        details_json：详情JSON。
        created_at：记录创建时的 UTC 时间戳。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    __tablename__ = "audit_event"
    __table_args__ = (
        Index("ix_audit_event_experiment_created", "experiment_id", "created_at"),
        Index("ix_audit_event_task_created", "task_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[str | None] = mapped_column(
        ForeignKey("experiment.id", ondelete="SET NULL")
    )
    task_id: Mapped[str | None] = mapped_column(
        ForeignKey("task.id", ondelete="SET NULL")
    )
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    actor: Mapped[str | None] = mapped_column(String(128))
    details_json: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
