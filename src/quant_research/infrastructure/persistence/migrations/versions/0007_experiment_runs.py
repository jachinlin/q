"""硬切到统一 Experiment → Run 研究主脊。

Revision ID: experiment_runs
Revises: data_initialization
"""

from __future__ import annotations

from typing import cast

from alembic import op
from sqlalchemy import Column, String, Table, inspect

from quant_research.infrastructure.persistence.orm import (
    ExperimentORM,
    ExperimentTagORM,
    RunArtifactORM,
    RunMetricORM,
    RunORM,
    RunTagORM,
)

revision = "experiment_runs"
down_revision = "data_initialization"
branch_labels = None
depends_on = None

_NEW_TABLES = (
    ExperimentORM.__table__,
    ExperimentTagORM.__table__,
    RunORM.__table__,
    RunTagORM.__table__,
    RunMetricORM.__table__,
    RunArtifactORM.__table__,
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
        cast(Table, table).create(bind=bind, checkfirst=True)
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
        cast(Table, table).drop(bind=bind, checkfirst=True)
    if inspect(bind).has_table("task"):
        columns = {item["name"] for item in inspect(bind).get_columns("task")}
        if "experiment_id" not in columns:
            op.add_column("task", Column("experiment_id", String(36), nullable=True))
