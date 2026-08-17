"""增加独立因子研究及其不可变运行表。

Revision ID: factor_studies
Revises: initial_schema
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import Table

from quant_research.infrastructure.persistence.orm import FactorRunORM, FactorStudyORM

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
    study = FactorStudyORM.__table__
    run = FactorRunORM.__table__
    assert isinstance(study, Table)
    assert isinstance(run, Table)
    study.create(bind=op.get_bind(), checkfirst=True)
    run.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    """按外键依赖顺序删除因子运行和研究表；该函数作为框架约定入口保留在模块级。

    入参：
        无。
    返回值：
        无。
    异常：
        无。
    """
    run = FactorRunORM.__table__
    study = FactorStudyORM.__table__
    assert isinstance(run, Table)
    assert isinstance(study, Table)
    run.drop(bind=op.get_bind(), checkfirst=True)
    study.drop(bind=op.get_bind(), checkfirst=True)
