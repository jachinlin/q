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
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """


class RawRequestORM(Base):
    """将``raw``请求``orm``记录映射到 SQLite 持久化表。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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


class DataInitializationStateORM(Base):
    """映射首次数据初始化的冻结窗口和完成状态。

    入参：由 SQLAlchemy 在持久化时提供字段值。返回值：可由 Session 管理的 ORM 行。
    异常：数据库约束拒绝非法状态、年数或单例标识。
    """

    __tablename__ = "data_initialization_state"
    __table_args__ = (
        CheckConstraint("id = 1", name="ck_data_initialization_state_singleton"),
        CheckConstraint(
            "status IN ('IN_PROGRESS', 'COMPLETED')",
            name="ck_data_initialization_state_status",
        ),
        CheckConstraint("years > 0", name="ck_data_initialization_state_years"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    years: Mapped[int] = mapped_column(Integer, nullable=False)
    start_date: Mapped[str] = mapped_column(String(10), nullable=False)
    end_date: Mapped[str] = mapped_column(String(10), nullable=False)
    started_at: Mapped[str] = mapped_column(String(32), nullable=False)
    completed_at: Mapped[str | None] = mapped_column(String(32))
    catalog_hash: Mapped[str | None] = mapped_column(String(64))
    quality_run_id: Mapped[str | None] = mapped_column(String(36))


class ExperimentORM(Base):
    """持久化不可变实验定义和精确 baseline Run 指针。

    入参：
        字段保存实验标识、名称、类型、冻结定义、标签关系和基线指针。
    返回值：
        SQLAlchemy 查询或构造时返回实验 ORM 记录。
    异常：
        数据库约束拒绝非法实验类型、重复主键或缺失必填字段。
    """

    __tablename__ = "experiment"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('STRATEGY_BACKTEST', 'FACTOR_STUDY')", name="ck_experiment_kind"
        ),
        Index("ix_experiment_created", "created_at", "id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    definition_json: Mapped[str] = mapped_column(Text, nullable=False)
    definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    baseline_run_id: Mapped[str | None] = mapped_column(String(36))
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class ExperimentTagORM(Base):
    """将实验标签``orm``记录映射到 SQLite 持久化表。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    __tablename__ = "experiment_tag"

    experiment_id: Mapped[str] = mapped_column(
        ForeignKey("experiment.id", ondelete="CASCADE"), primary_key=True
    )
    tag: Mapped[str] = mapped_column(String(64), primary_key=True)


class RunORM(Base):
    """持久化冻结 Run 配置、数据身份、状态和发布结果。

    入参：
        字段保存 Run 身份、所属实验、任务、配置、目录哈希和生命周期状态。
    返回值：
        SQLAlchemy 查询或构造时返回 Run ORM 记录。
    异常：
        数据库约束拒绝非法状态、非法研究标记或不存在的实验外键。
    """

    __tablename__ = "run"
    __table_args__ = (
        CheckConstraint(
            "status IN ('CREATED', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name="ck_run_status",
        ),
        CheckConstraint(
            "research_mark IN ('UNREVIEWED', 'BASELINE', 'CANDIDATE', 'DISCARDED')",
            name="ck_run_research_mark",
        ),
        Index("ix_run_experiment_created", "experiment_id", "created_at", "id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    experiment_id: Mapped[str] = mapped_column(
        ForeignKey("experiment.id", ondelete="CASCADE"), nullable=False
    )
    task_id: Mapped[str | None] = mapped_column(String(36), unique=True)
    config_json: Mapped[str] = mapped_column(Text, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    catalog_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    research_mark: Mapped[str] = mapped_column(String(16), nullable=False)
    uses_test_region: Mapped[bool] = mapped_column(Boolean, nullable=False)
    artifact_dir: Mapped[str | None] = mapped_column(String)
    manifest_hash: Mapped[str | None] = mapped_column(String(64))
    error_json: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[str | None] = mapped_column(String(32))
    completed_at: Mapped[str | None] = mapped_column(String(32))


class RunTagORM(Base):
    """持久化 Run 用户标签。

    入参：
        run_id：Run 标识；tag：用户标签文本。
    返回值：
        SQLAlchemy 查询或构造时返回 Run 标签关联记录。
    异常：
        数据库约束拒绝重复标签或不存在的 Run 外键。
    """

    __tablename__ = "run_tag"

    run_id: Mapped[str] = mapped_column(
        ForeignKey("run.id", ondelete="CASCADE"), primary_key=True
    )
    tag: Mapped[str] = mapped_column(String(64), primary_key=True)


class RunMetricORM(Base):
    """持久化 Run 的命名标量指标及显著性字段。

    入参：
        字段保存指标名称、数值、单位、原始 p-value 和校正后 p-value。
    返回值：
        SQLAlchemy 查询或构造时返回 Run 指标记录。
    异常：
        数据库约束拒绝同一 Run 下的重复指标名或不存在的 Run 外键。
    """

    __tablename__ = "run_metric"
    __table_args__ = (UniqueConstraint("run_id", "name", name="uq_run_metric_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        ForeignKey("run.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str | None] = mapped_column(String(32))
    p_value: Mapped[float | None] = mapped_column(Float)
    adjusted_p_value: Mapped[float | None] = mapped_column(Float)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class RunArtifactORM(Base):
    """持久化可信 Manifest 中登记的 Run 产物。

    入参：
        字段保存产物类型、相对路径、内容哈希、大小、行数和 Schema。
    返回值：
        SQLAlchemy 查询或构造时返回 Run 产物记录。
    异常：
        数据库约束拒绝同一 Run 下的重复产物类型或不存在的 Run 外键。
    """

    __tablename__ = "run_artifact"
    __table_args__ = (
        UniqueConstraint("run_id", "artifact_type", name="uq_run_artifact_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        ForeignKey("run.id", ondelete="CASCADE"), nullable=False
    )
    artifact_type: Mapped[str] = mapped_column(String(64), nullable=False)
    relative_path: Mapped[str] = mapped_column(String, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    byte_count: Mapped[int] = mapped_column(Integer, nullable=False)
    row_count: Mapped[int | None] = mapped_column(Integer)
    schema_json: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class TaskORM(Base):
    """将任务``orm``记录映射到 SQLite 持久化表。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    __tablename__ = "task"
    __table_args__ = (
        CheckConstraint(
            "status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', "
            "'CANCEL_REQUESTED', 'CANCELLED', 'ORPHANED')",
            name="ck_task_status",
        ),
        Index("ix_task_subject", "subject_kind", "subject_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    subject_kind: Mapped[str | None] = mapped_column(String(32))
    subject_id: Mapped[str | None] = mapped_column(String(64))
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
    text("COALESCE(subject_kind, '')"),
    text("COALESCE(subject_id, '')"),
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
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
    """持久化关联 Run、任务或其他通用主体的审计事件。

    入参：
        字段保存 Run、通用主体、任务、事件类型、操作者和结构化详情。
    返回值：
        SQLAlchemy 查询或构造时返回审计事件记录。
    异常：
        数据库约束拒绝不存在的任务外键或缺失事件类型。
    """

    __tablename__ = "audit_event"
    __table_args__ = (Index("ix_audit_event_task_created", "task_id", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str | None] = mapped_column(String(36))
    subject_kind: Mapped[str | None] = mapped_column(String(32))
    subject_id: Mapped[str | None] = mapped_column(String(64))
    task_id: Mapped[str | None] = mapped_column(
        ForeignKey("task.id", ondelete="SET NULL")
    )
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    actor: Mapped[str | None] = mapped_column(String(128))
    details_json: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
