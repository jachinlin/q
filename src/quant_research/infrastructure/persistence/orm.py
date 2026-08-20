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
    """将实验``orm``记录映射到 SQLite 持久化表。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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


class ExperimentMetricORM(Base):
    """将实验指标``orm``记录映射到 SQLite 持久化表。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
    experiment_id: Mapped[str | None] = mapped_column(String(36))
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


class FactorStudyORM(Base):
    """将因子因子研究``orm``记录映射到 SQLite 持久化表。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
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
    """将审计事件事件``orm``记录映射到 SQLite 持久化表。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    __tablename__ = "audit_event"
    __table_args__ = (
        Index("ix_audit_event_task_created", "task_id", "created_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    experiment_id: Mapped[str | None] = mapped_column(String(36))
    task_id: Mapped[str | None] = mapped_column(
        ForeignKey("task.id", ondelete="SET NULL")
    )
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    actor: Mapped[str | None] = mapped_column(String(128))
    details_json: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class ResearchFamilyORM(Base):
    """持久化不可变研究族定义和可审计研究备注。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    __tablename__ = "research_family"
    __table_args__ = (
        CheckConstraint(
            "research_mode IN ('SIGNAL_STUDY', 'PORTFOLIO_STUDY', 'BACKTEST_EXPERIMENT')",
            name="ck_research_family_mode",
        ),
        CheckConstraint(
            "mark IN ('UNREVIEWED', 'BASELINE', 'CANDIDATE', 'DISCARDED')",
            name="ck_research_family_mark",
        ),
        Index("ix_research_family_created", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    hypothesis: Mapped[str] = mapped_column(Text, nullable=False)
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False)
    research_mode: Mapped[str] = mapped_column(String(32), nullable=False)
    config_json: Mapped[str] = mapped_column(Text, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    mark: Mapped[str] = mapped_column(String(16), nullable=False)
    note: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
    archived_at: Mapped[str | None] = mapped_column(String(32))


class ResearchFamilyExecutionORM(Base):
    """持久化研究族绑定的数据、源码、依赖和规则身份。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    __tablename__ = "research_family_execution"
    __table_args__ = (
        CheckConstraint(
            "status IN ('CREATED', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name="ck_research_execution_status",
        ),
        Index("ix_research_execution_family_created", "family_id", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    family_id: Mapped[str] = mapped_column(
        ForeignKey("research_family.id", ondelete="CASCADE"), nullable=False
    )
    catalog_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    lockfile_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    rulebook_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    environment_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    selected_variant_id: Mapped[str | None] = mapped_column(String(36))
    selection_reason: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[str | None] = mapped_column(String(32))
    completed_at: Mapped[str | None] = mapped_column(String(32))
    error_json: Mapped[str | None] = mapped_column(Text)


class ResearchVariantORM(Base):
    """持久化一次执行内确定性展开的候选。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    __tablename__ = "research_variant"
    __table_args__ = (
        UniqueConstraint("execution_id", "ordinal", name="uq_research_variant_ordinal"),
        UniqueConstraint("execution_id", "composition_hash", name="uq_research_variant_hash"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    execution_id: Mapped[str] = mapped_column(
        ForeignKey("research_family_execution.id", ondelete="CASCADE"), nullable=False
    )
    ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    composition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    parameters_json: Mapped[str] = mapped_column(Text, nullable=False)
    config_json: Mapped[str] = mapped_column(Text, nullable=False)
    rejection_reasons_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class ResearchRunORM(Base):
    """持久化候选在开发区间或锁定测试区间上的运行。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    __tablename__ = "research_run"
    __table_args__ = (
        UniqueConstraint("execution_id", "variant_id", "phase", name="uq_research_run_phase"),
        CheckConstraint("phase IN ('TRAIN_VALIDATION', 'TEST')", name="ck_research_run_phase"),
        CheckConstraint(
            "status IN ('CREATED', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name="ck_research_run_status",
        ),
        CheckConstraint(
            "stage IN ('VALIDATE', 'UNIVERSE', 'RESEARCH_COMPUTE', 'SIMULATE', 'ANALYTICS', 'ARTIFACT_VERIFY', 'REGISTER')",
            name="ck_research_run_stage",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    execution_id: Mapped[str] = mapped_column(
        ForeignKey("research_family_execution.id", ondelete="CASCADE"), nullable=False
    )
    variant_id: Mapped[str] = mapped_column(
        ForeignKey("research_variant.id", ondelete="CASCADE"), nullable=False
    )
    phase: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    stage_status_json: Mapped[str] = mapped_column(Text, nullable=False)
    manifest_path: Mapped[str | None] = mapped_column(String)
    manifest_hash: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[str | None] = mapped_column(String(32))
    completed_at: Mapped[str | None] = mapped_column(String(32))
    error_json: Mapped[str | None] = mapped_column(Text)


class ResearchMetricORM(Base):
    """持久化运行按 TRAIN、VALIDATION、TEST 分区的指标。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    __tablename__ = "research_metric"
    __table_args__ = (
        UniqueConstraint("run_id", "split", "category", "name", name="uq_research_metric_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        ForeignKey("research_run.id", ondelete="CASCADE"), nullable=False
    )
    split: Mapped[str] = mapped_column(String(16), nullable=False)
    category: Mapped[str] = mapped_column(String(32), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)
    unit: Mapped[str | None] = mapped_column(String(32))
    p_value: Mapped[float | None] = mapped_column(Float)
    adjusted_p_value: Mapped[float | None] = mapped_column(Float)


class ResearchArtifactORM(Base):
    """持久化可信 Manifest 中登记的研究产物。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    __tablename__ = "research_artifact"
    __table_args__ = (
        UniqueConstraint("execution_id", "relative_path", name="uq_research_artifact_path"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    execution_id: Mapped[str] = mapped_column(
        ForeignKey("research_family_execution.id", ondelete="CASCADE"), nullable=False
    )
    run_id: Mapped[str | None] = mapped_column(
        ForeignKey("research_run.id", ondelete="CASCADE")
    )
    relative_path: Mapped[str] = mapped_column(String, nullable=False)
    artifact_type: Mapped[str] = mapped_column(String(64), nullable=False)
    producer_component_id: Mapped[str] = mapped_column(String(128), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    byte_count: Mapped[int] = mapped_column(Integer, nullable=False)
    row_count: Mapped[int | None] = mapped_column(Integer)
    metadata_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[str] = mapped_column(String(32), nullable=False)


class ResearchTagORM(Base):
    """持久化研究族用户标签。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    __tablename__ = "research_tag"

    family_id: Mapped[str] = mapped_column(
        ForeignKey("research_family.id", ondelete="CASCADE"), primary_key=True
    )
    tag: Mapped[str] = mapped_column(String(64), primary_key=True)
