"""增加目标研究族、候选、运行、指标和产物结构。

Revision ID: research_platform
Revises: quality_rule_results
"""

from __future__ import annotations

from typing import cast

from alembic import op
from sqlalchemy import Column, MetaData, String, Table, inspect
from sqlalchemy.engine import Connection

from quant_research.infrastructure.persistence.orm import (
    AuditEventORM,
    ExperimentArtifactORM,
    ExperimentMetricORM,
    ExperimentORM,
    ExperimentTagORM,
    FactorRunORM,
    FactorStudyORM,
    ResearchArtifactORM,
    ResearchFamilyExecutionORM,
    ResearchFamilyORM,
    ResearchMetricORM,
    ResearchRunORM,
    ResearchTagORM,
    ResearchVariantORM,
    TaskAttemptORM,
    TaskORM,
)

revision = "research_platform"
down_revision = "quality_rule_results"
branch_labels = None
depends_on = None

_TABLES = (
    ResearchFamilyORM.__table__,
    ResearchFamilyExecutionORM.__table__,
    ResearchVariantORM.__table__,
    ResearchRunORM.__table__,
    ResearchMetricORM.__table__,
    ResearchArtifactORM.__table__,
    ResearchTagORM.__table__,
)
_LEGACY_TABLES = (
    FactorRunORM.__table__,
    FactorStudyORM.__table__,
    ExperimentArtifactORM.__table__,
    ExperimentMetricORM.__table__,
    ExperimentTagORM.__table__,
    ExperimentORM.__table__,
)


def upgrade() -> None:
    """按外键依赖顺序创建目标研究平台表。

该函数作为模块级确定性辅助或框架入口保留。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """
    bind = op.get_bind()
    for table in _TABLES:
        cast(Table, table).create(bind=bind, checkfirst=True)
    inspector = inspect(bind)
    if not inspector.has_table("task"):
        return
    task_columns = {item["name"] for item in inspector.get_columns("task")}
    if "subject_kind" not in task_columns:
        op.add_column("task", Column("subject_kind", String(32), nullable=True))
    if "subject_id" not in task_columns:
        op.add_column("task", Column("subject_id", String(64), nullable=True))
    _MigrationSupport.remove_legacy_research_foreign_keys(bind)
    for table in _LEGACY_TABLES:
        cast(Table, table).drop(bind=bind, checkfirst=True)


def downgrade() -> None:
    """按外键逆序删除目标研究平台表。

该函数作为模块级确定性辅助或框架入口保留。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """
    bind = op.get_bind()
    for table in reversed(_TABLES):
        cast(Table, table).drop(bind=bind, checkfirst=True)
    for table in reversed(_LEGACY_TABLES):
        cast(Table, table).create(bind=bind, checkfirst=True)
    inspector = inspect(bind)
    if not inspector.has_table("task"):
        return
    task_columns = {item["name"] for item in inspector.get_columns("task")}
    if "subject_id" in task_columns:
        op.drop_column("task", "subject_id")
    if "subject_kind" in task_columns:
        op.drop_column("task", "subject_kind")


class _MigrationSupport:
    """在保留数据任务、attempt 和审计的前提下解除旧研究外键。"""

    @staticmethod
    def remove_legacy_research_foreign_keys(bind: Connection) -> None:
        """重建通用任务表，使删除旧实验表不会破坏数据任务。"""
        inspector = inspect(bind)
        task_fks = inspector.get_foreign_keys("task")
        audit_fks = inspector.get_foreign_keys("audit_event")
        if not any(item.get("referred_table") == "experiment" for item in (*task_fks, *audit_fks)):
            return
        connection = op.get_bind()
        connection.exec_driver_sql(
            "CREATE TABLE _research_task_attempt_backup AS SELECT * FROM task_attempt"
        )
        connection.exec_driver_sql(
            "CREATE TABLE _research_audit_event_backup AS SELECT * FROM audit_event"
        )
        cast(Table, AuditEventORM.__table__).drop(bind=connection, checkfirst=True)
        cast(Table, TaskAttemptORM.__table__).drop(bind=connection, checkfirst=True)
        metadata = MetaData()
        temporary = cast(Table, TaskORM.__table__).to_metadata(
            metadata, name="_research_task"
        )
        for index in tuple(temporary.indexes):
            temporary.indexes.remove(index)
        temporary.create(bind=connection)
        columns = [item.name for item in cast(Table, TaskORM.__table__).columns]
        column_sql = ", ".join(f'"{item}"' for item in columns)
        connection.exec_driver_sql(
            f"INSERT INTO _research_task ({column_sql}) SELECT {column_sql} FROM task"
        )
        connection.exec_driver_sql("DROP TABLE task")
        connection.exec_driver_sql("ALTER TABLE _research_task RENAME TO task")
        cast(Table, TaskAttemptORM.__table__).create(bind=connection, checkfirst=True)
        cast(Table, AuditEventORM.__table__).create(bind=connection, checkfirst=True)
        attempt_columns = [
            item.name for item in cast(Table, TaskAttemptORM.__table__).columns
        ]
        attempt_sql = ", ".join(f'"{item}"' for item in attempt_columns)
        connection.exec_driver_sql(
            f"INSERT INTO task_attempt ({attempt_sql}) "
            f"SELECT {attempt_sql} FROM _research_task_attempt_backup"
        )
        audit_columns = [
            item.name for item in cast(Table, AuditEventORM.__table__).columns
        ]
        audit_sql = ", ".join(f'"{item}"' for item in audit_columns)
        connection.exec_driver_sql(
            f"INSERT INTO audit_event ({audit_sql}) "
            f"SELECT {audit_sql} FROM _research_audit_event_backup"
        )
        connection.exec_driver_sql("DROP TABLE _research_task_attempt_backup")
        connection.exec_driver_sql("DROP TABLE _research_audit_event_backup")
        for index in cast(Table, TaskORM.__table__).indexes:
            index.create(bind=connection, checkfirst=True)
