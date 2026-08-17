"""增加质量规则全量执行结果。

Revision ID: quality_rule_results
Revises: data_center_operations
"""

from __future__ import annotations

from typing import cast

from alembic import op
from sqlalchemy import Boolean, Column, Table, inspect, text

from quant_research.infrastructure.persistence.orm import QualityRuleResultORM

revision = "quality_rule_results"
down_revision = "data_center_operations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """增加完整规则结果表并把既有运行标记为证据不完整；该函数是 Alembic 框架入口。

    入参：无。返回值：无。异常：迁移失败时由 Alembic 或数据库异常描述原因。
    """
    bind = op.get_bind()
    columns = {item["name"] for item in inspect(bind).get_columns("quality_run")}
    if "results_complete" not in columns:
        op.add_column(
            "quality_run",
            Column(
                "results_complete",
                Boolean(),
                nullable=False,
                server_default=text("0"),
            ),
        )
    cast(Table, QualityRuleResultORM.__table__).create(bind=bind, checkfirst=True)


def downgrade() -> None:
    """删除质量规则全量结果结构；该函数是 Alembic 框架入口。

    入参：无。返回值：无。异常：迁移失败时由 Alembic 或数据库异常描述原因。
    """
    cast(Table, QualityRuleResultORM.__table__).drop(
        bind=op.get_bind(), checkfirst=True
    )
    op.drop_column("quality_run", "results_complete")
