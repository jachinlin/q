"""增加首次数据初始化的冻结状态。

Revision ID: data_initialization
Revises: research_platform
"""

from __future__ import annotations

from typing import cast

from alembic import op
from sqlalchemy import Table

from quant_research.infrastructure.persistence.orm import DataInitializationStateORM

revision = "data_initialization"
down_revision = "research_platform"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建首次初始化状态表。

    该函数作为 Alembic 模块级框架入口保留。

    入参：无。返回值：无。
    异常：建表失败时传播 Alembic 或数据库异常；既有数据表保持不变。
    """

    cast(Table, DataInitializationStateORM.__table__).create(
        bind=op.get_bind(), checkfirst=True
    )


def downgrade() -> None:
    """删除首次初始化状态表。

    该函数作为 Alembic 模块级框架入口保留。

    入参：无。返回值：无。
    异常：删表失败时传播 Alembic 或数据库异常；Raw 和 Canonical 产物不受影响。
    """

    cast(Table, DataInitializationStateORM.__table__).drop(
        bind=op.get_bind(), checkfirst=True
    )
