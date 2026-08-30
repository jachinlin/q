"""硬切到单一 StrategyStudy 主脊。

Revision ID: strategy_studies
Revises: independent_factor_studies
"""

from __future__ import annotations

from typing import cast

from alembic import op
from sqlalchemy import Column, String, Table, inspect

from quant_research.infrastructure.persistence.orm import (
    StrategyStudyArtifactORM,
    StrategyStudyMetricORM,
    StrategyStudyORM,
    StrategyStudyTagORM,
)

revision = "strategy_studies"
down_revision = "independent_factor_studies"
branch_labels = None
depends_on = None

_NEW_TABLES = (
    StrategyStudyORM.__table__,
    StrategyStudyTagORM.__table__,
    StrategyStudyMetricORM.__table__,
    StrategyStudyArtifactORM.__table__,
)
_REMOVED_TABLES = (
    "run_artifact",
    "run_metric",
    "run_tag",
    "run",
    "experiment_tag",
    "experiment",
)


def upgrade() -> None:
    """执行升级；该模块级函数由 Alembic 框架调用。

    入参：无，连接由迁移上下文提供。返回值：无。异常：DDL 或数据清理失败时传播。
    """

    bind = op.get_bind()
    inspector = inspect(bind)
    if inspector.has_table("audit_event"):
        bind.exec_driver_sql(
            "DELETE FROM audit_event WHERE subject_kind = 'EXPERIMENT_RUN'"
        )
    if inspector.has_table("task"):
        bind.exec_driver_sql(
            "DELETE FROM task WHERE subject_kind = 'EXPERIMENT_RUN' "
            "OR task_type = 'EXPERIMENT_RUN'"
        )
    for table_name in _REMOVED_TABLES:
        if inspect(bind).has_table(table_name):
            op.drop_table(table_name)
    for table in _NEW_TABLES:
        cast(Table, table).create(bind=bind, checkfirst=True)
    if inspect(bind).has_table("audit_event"):
        columns = {item["name"] for item in inspect(bind).get_columns("audit_event")}
        if "run_id" in columns:
            with op.batch_alter_table("audit_event") as batch:
                batch.drop_column("run_id")


def downgrade() -> None:
    """执行降级；该模块级函数由 Alembic 框架调用。

    入参：无，连接由迁移上下文提供。返回值：无。异常：DDL 失败时传播。
    """

    bind = op.get_bind()
    for table in reversed(_NEW_TABLES):
        cast(Table, table).drop(bind=bind, checkfirst=True)
    if inspect(bind).has_table("audit_event"):
        columns = {item["name"] for item in inspect(bind).get_columns("audit_event")}
        if "run_id" not in columns:
            op.add_column("audit_event", Column("run_id", String(36)))
