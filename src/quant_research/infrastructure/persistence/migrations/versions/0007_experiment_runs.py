"""硬切到统一 Experiment → Run 研究主脊。

Revision ID: experiment_runs
Revises: data_initialization
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    inspect,
)

revision = "experiment_runs"
down_revision = "data_initialization"
branch_labels = None
depends_on = None

_metadata = MetaData()
_experiment = Table(
    "experiment",
    _metadata,
    Column("id", String(36), primary_key=True),
    Column("name", String(128), nullable=False),
    Column("description", Text, nullable=False),
    Column("definition_json", Text, nullable=False),
    Column("definition_hash", String(64), nullable=False),
    Column("baseline_run_id", String(36)),
    Column("created_at", String(32), nullable=False),
    Index("ix_experiment_created", "created_at", "id"),
)
_experiment_tag = Table(
    "experiment_tag",
    _metadata,
    Column(
        "experiment_id",
        String(36),
        ForeignKey("experiment.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("tag", String(64), primary_key=True),
)
_run = Table(
    "run",
    _metadata,
    Column("id", String(36), primary_key=True),
    Column(
        "experiment_id",
        String(36),
        ForeignKey("experiment.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("task_id", String(36), unique=True),
    Column("config_json", Text, nullable=False),
    Column("config_hash", String(64), nullable=False),
    Column("catalog_hash", String(64), nullable=False),
    Column("status", String(16), nullable=False),
    Column("stage", String(32), nullable=False),
    Column("research_mark", String(16), nullable=False),
    Column("uses_test_region", Boolean, nullable=False),
    Column("artifact_dir", String),
    Column("manifest_hash", String(64)),
    Column("error_json", Text),
    Column("created_at", String(32), nullable=False),
    Column("started_at", String(32)),
    Column("completed_at", String(32)),
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
_run_tag = Table(
    "run_tag",
    _metadata,
    Column(
        "run_id",
        String(36),
        ForeignKey("run.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("tag", String(64), primary_key=True),
)
_run_metric = Table(
    "run_metric",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(36), ForeignKey("run.id", ondelete="CASCADE")),
    Column("name", String(128), nullable=False),
    Column("value", Float, nullable=False),
    Column("unit", String(32)),
    Column("p_value", Float),
    Column("adjusted_p_value", Float),
    Column("created_at", String(32), nullable=False),
    UniqueConstraint("run_id", "name", name="uq_run_metric_name"),
)
_run_artifact = Table(
    "run_artifact",
    _metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", String(36), ForeignKey("run.id", ondelete="CASCADE")),
    Column("artifact_type", String(64), nullable=False),
    Column("relative_path", String, nullable=False),
    Column("content_hash", String(64), nullable=False),
    Column("byte_count", Integer, nullable=False),
    Column("row_count", Integer),
    Column("schema_json", Text),
    Column("created_at", String(32), nullable=False),
    UniqueConstraint("run_id", "artifact_type", name="uq_run_artifact_type"),
)
_NEW_TABLES = (
    _experiment,
    _experiment_tag,
    _run,
    _run_tag,
    _run_metric,
    _run_artifact,
)
_REMOVED_TABLES = (
    "research_tag",
    "research_artifact",
    "research_metric",
    "research_run",
    "research_variant",
    "research_family_execution",
    "research_family",
    "factor_run",
    "factor_study",
    "experiment_artifact",
    "experiment_metric",
)


def upgrade() -> None:
    """由 Alembic 框架升级到统一 Experiment → Run 数据模型。

    入参：
        无；数据库连接由 Alembic 迁移上下文提供。
    返回值：
        完成迁移后返回 None。
    异常：
        SQL 执行、约束变更或表创建失败时传播数据库异常。
    """
    bind = op.get_bind()
    inspector = inspect(bind)
    if inspector.has_table("task"):
        bind.exec_driver_sql(
            "DELETE FROM task WHERE task_type LIKE 'RESEARCH_%' "
            "OR task_type IN ('BACKTEST', 'FACTOR_ANALYSIS') "
            "OR subject_kind IN ('RESEARCH_FAMILY', 'RESEARCH_EXECUTION', "
            "'RESEARCH_RUN', 'FACTOR_RUN', 'EXPERIMENT')"
        )
        columns = {item["name"] for item in inspector.get_columns("task")}
        if "experiment_id" in columns:
            with op.batch_alter_table("task") as batch:
                batch.drop_column("experiment_id")
    for table_name in _REMOVED_TABLES:
        if inspect(bind).has_table(table_name):
            op.drop_table(table_name)
    for table in _NEW_TABLES:
        table.create(bind=bind, checkfirst=True)
    if inspect(bind).has_table("audit_event"):
        columns = {item["name"] for item in inspect(bind).get_columns("audit_event")}
        with op.batch_alter_table("audit_event") as batch:
            if "run_id" not in columns:
                batch.add_column(Column("run_id", String(36), nullable=True))
            if "subject_kind" not in columns:
                batch.add_column(Column("subject_kind", String(32), nullable=True))
            if "subject_id" not in columns:
                batch.add_column(Column("subject_id", String(64), nullable=True))
            if "experiment_id" in columns:
                batch.drop_column("experiment_id")


def downgrade() -> None:
    """由 Alembic 框架删除统一实验表并恢复旧任务占位列。

    入参：
        无；数据库连接由 Alembic 迁移上下文提供。
    返回值：
        完成降级后返回 None。
    异常：
        SQL 执行、约束变更或表删除失败时传播数据库异常。
    """
    bind = op.get_bind()
    for table in reversed(_NEW_TABLES):
        table.drop(bind=bind, checkfirst=True)
    if inspect(bind).has_table("task"):
        columns = {item["name"] for item in inspect(bind).get_columns("task")}
        if "experiment_id" not in columns:
            op.add_column("task", Column("experiment_id", String(36), nullable=True))
