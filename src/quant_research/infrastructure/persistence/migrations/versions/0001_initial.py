"""提供versions与0001_initial相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from alembic import op

from quant_research.infrastructure.persistence.orm import Base

revision = "initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """处理基础设施中的``upgrade``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        无。
    返回值：
        无。
    异常：
        无。
    """
    Base.metadata.create_all(bind=op.get_bind())


def downgrade() -> None:
    """处理基础设施中的``downgrade``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        无。
    返回值：
        无。
    异常：
        无。
    """
    Base.metadata.drop_all(bind=op.get_bind())
