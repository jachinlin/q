"""创建独立扁平 FactorStudy 主脊。

Revision ID: independent_factor_studies
Revises: experiment_runs
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, cast

from alembic import op
from sqlalchemy import Column, String, Table, inspect, text
from sqlalchemy.engine import Connection

from quant_research.data.contracts import canonical_json_bytes
from quant_research.infrastructure.persistence.orm import (
    FactorStudyArtifactORM,
    FactorStudyDecisionORM,
    FactorStudyMetricORM,
    FactorStudyORM,
    FactorStudyTagORM,
)

revision = "independent_factor_studies"
down_revision = "experiment_runs"
branch_labels = None
depends_on = None

_TABLES = (
    FactorStudyORM.__table__,
    FactorStudyTagORM.__table__,
    FactorStudyMetricORM.__table__,
    FactorStudyArtifactORM.__table__,
    FactorStudyDecisionORM.__table__,
)


def upgrade() -> None:
    """执行 Alembic 升级；该函数作为框架入口保留在模块级。

    入参：无。返回值：无。异常：DDL 或数据转换失败时由数据库抛出。
    """
    bind = op.get_bind()
    inspector = inspect(bind)
    if inspector.has_table("experiment"):
        columns = {item["name"] for item in inspector.get_columns("experiment")}
        if "kind" in columns:
            if inspector.has_table("task") and inspector.has_table("run"):
                bind.exec_driver_sql(
                    "DELETE FROM task WHERE subject_kind = 'EXPERIMENT_RUN' "
                    "AND subject_id IN (SELECT run.id FROM run JOIN experiment "
                    "ON run.experiment_id = experiment.id "
                    "WHERE experiment.kind = 'FACTOR_STUDY')"
                )
            bind.exec_driver_sql(
                "DELETE FROM experiment WHERE kind = 'FACTOR_STUDY'"
            )
            _strip_strategy_kind(bind)
            dependent_rows = _backup_experiment_dependents(bind)
            checks = {
                item["name"]
                for item in inspector.get_check_constraints("experiment")
            }
            with op.batch_alter_table("experiment") as batch:
                if "ck_experiment_kind" in checks:
                    batch.drop_constraint("ck_experiment_kind", type_="check")
                batch.drop_column("kind")
            _restore_experiment_dependents(bind, dependent_rows)
    for table in _TABLES:
        cast(Table, table).create(bind=bind, checkfirst=True)


def downgrade() -> None:
    """执行 Alembic 降级；该函数作为框架入口保留在模块级。

    入参：无。返回值：无。异常：DDL 或数据恢复失败时由数据库抛出。
    """
    bind = op.get_bind()
    for table in reversed(_TABLES):
        cast(Table, table).drop(bind=bind, checkfirst=True)
    columns = {item["name"] for item in inspect(bind).get_columns("experiment")}
    if "kind" not in columns:
        with op.batch_alter_table("experiment") as batch:
            batch.add_column(
                Column(
                    "kind",
                    String(32),
                    nullable=False,
                    server_default="STRATEGY_BACKTEST",
                )
            )
            batch.create_check_constraint(
                "ck_experiment_kind",
                "kind IN ('STRATEGY_BACKTEST', 'FACTOR_STUDY')",
            )
        _restore_strategy_kind(bind)


def _strip_strategy_kind(bind: Connection) -> None:
    rows = bind.exec_driver_sql(
        "SELECT id, definition_json FROM experiment ORDER BY id"
    ).fetchall()
    for experiment_id, definition_json in rows:
        definition = json.loads(definition_json)
        definition.pop("kind", None)
        initial = definition.get("initial_run")
        if isinstance(initial, dict):
            initial.pop("kind", None)
        encoded = canonical_json_bytes(definition).decode("utf-8")
        bind.exec_driver_sql(
            "UPDATE experiment SET definition_json = ?, definition_hash = ? "
            "WHERE id = ?",
            (
                encoded,
                hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
                experiment_id,
            ),
        )
    run_rows = bind.exec_driver_sql(
        "SELECT id, config_json FROM run ORDER BY id"
    ).fetchall()
    for run_id, config_json in run_rows:
        config = json.loads(config_json)
        config.pop("kind", None)
        encoded = canonical_json_bytes(config).decode("utf-8")
        bind.exec_driver_sql(
            "UPDATE run SET config_json = ?, config_hash = ? WHERE id = ?",
            (encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest(), run_id),
        )


def _restore_strategy_kind(bind: Connection) -> None:
    rows = bind.exec_driver_sql(
        "SELECT id, definition_json FROM experiment ORDER BY id"
    ).fetchall()
    for experiment_id, definition_json in rows:
        definition = json.loads(definition_json)
        definition["kind"] = "STRATEGY_BACKTEST"
        initial = definition.get("initial_run")
        if isinstance(initial, dict):
            initial["kind"] = "STRATEGY_BACKTEST"
        encoded = canonical_json_bytes(definition).decode("utf-8")
        bind.exec_driver_sql(
            "UPDATE experiment SET definition_json = ?, definition_hash = ? "
            "WHERE id = ?",
            (
                encoded,
                hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
                experiment_id,
            ),
        )
    run_rows = bind.exec_driver_sql(
        "SELECT id, config_json FROM run ORDER BY id"
    ).fetchall()
    for run_id, config_json in run_rows:
        config = json.loads(config_json)
        config["kind"] = "STRATEGY_BACKTEST"
        encoded = canonical_json_bytes(config).decode("utf-8")
        bind.exec_driver_sql(
            "UPDATE run SET config_json = ?, config_hash = ? WHERE id = ?",
            (encoded, hashlib.sha256(encoded.encode("utf-8")).hexdigest(), run_id),
        )


def _backup_experiment_dependents(
    bind: Connection,
) -> dict[str, list[dict[str, Any]]]:
    inspector = inspect(bind)
    result: dict[str, list[dict[str, Any]]] = {}
    for table_name in (
        "experiment_tag",
        "run",
        "run_tag",
        "run_metric",
        "run_artifact",
    ):
        if inspector.has_table(table_name):
            result[table_name] = [
                dict(row)
                for row in bind.exec_driver_sql(
                    f'SELECT * FROM "{table_name}" ORDER BY rowid'
                ).mappings()
            ]
    return result


def _restore_experiment_dependents(
    bind: Connection, rows_by_table: dict[str, list[dict[str, Any]]]
) -> None:
    for table_name, rows in rows_by_table.items():
        for row in rows:
            columns = tuple(row)
            names = ", ".join(f'"{name}"' for name in columns)
            values = ", ".join(f":{name}" for name in columns)
            bind.execute(
                text(
                    f'INSERT OR IGNORE INTO "{table_name}" ({names}) VALUES ({values})'
                ),
                row,
            )
