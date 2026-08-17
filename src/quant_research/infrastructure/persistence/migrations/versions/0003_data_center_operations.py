"""增加数据中心运营状态与任务结果字段。

Revision ID: data_center_operations
Revises: factor_studies
"""

from __future__ import annotations

from typing import cast

from alembic import op
from sqlalchemy import Column, Table, Text, inspect

from quant_research.infrastructure.persistence.orm import DatasetOperationalStateORM

revision = "data_center_operations"
down_revision = "factor_studies"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建运营状态和任务结果列；该模块级函数是 Alembic 框架入口。

    入参：无。返回值：无。异常：迁移失败时由 Alembic 或数据库异常描述原因。
    """

    bind = op.get_bind()
    cast(Table, DatasetOperationalStateORM.__table__).create(bind=bind, checkfirst=True)
    inspector = inspect(bind)
    task_columns = {item["name"] for item in inspector.get_columns("task")}
    if "result_json" not in task_columns:
        op.add_column("task", Column("result_json", Text(), nullable=True))
    attempt_columns = {item["name"] for item in inspector.get_columns("task_attempt")}
    if "result_json" not in attempt_columns:
        op.add_column("task_attempt", Column("result_json", Text(), nullable=True))


def downgrade() -> None:
    """删除数据中心新增结构；该模块级函数是 Alembic 框架入口。

    入参：无。返回值：无。异常：迁移失败时由 Alembic 或数据库异常描述原因。
    """

    op.drop_column("task_attempt", "result_json")
    op.drop_column("task", "result_json")
    cast(Table, DatasetOperationalStateORM.__table__).drop(
        bind=op.get_bind(), checkfirst=True
    )
