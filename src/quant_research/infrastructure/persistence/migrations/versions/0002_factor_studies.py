"""增加独立因子研究及其不可变运行表。

Revision ID: factor_studies
Revises: initial_schema
"""

from __future__ import annotations

revision = "factor_studies"
down_revision = "initial_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """创建因子研究和运行表；已有最终表时保持幂等；该函数作为框架约定入口保留在模块级。

    入参：
        无。
    返回值：
        无。
    异常：
        无。
    """
    # 该历史 revision 只保留 Alembic 链身份；最终架构不创建独立因子研究表。


def downgrade() -> None:
    """按外键依赖顺序删除因子运行和研究表；该函数作为框架约定入口保留在模块级。

    入参：
        无。
    返回值：
        无。
    异常：
        无。
    """
    # 硬切后没有可恢复的独立因子研究元数据。
