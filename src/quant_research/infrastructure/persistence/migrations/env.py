"""提供migrations与env相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import os
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

from quant_research.infrastructure.persistence.orm import Base

config = context.config
target_metadata = Base.metadata


class _EnvSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _configured_url() -> str:
        explicit = config.get_main_option("sqlalchemy.url")
        state_db = os.environ.get("QUANT_STATE_DB")
        if state_db:
            return f"sqlite+pysqlite:///{Path(state_db).resolve().as_posix()}"
        return explicit or "sqlite://"


def run_migrations_offline() -> None:
    """执行完整处理流程；该函数作为框架约定入口保留在模块级。

    入参：
        无。
    返回值：
        无。
    异常：
        无。
    """
    context.configure(
        url=_EnvSupport._configured_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """执行完整处理流程；该函数作为框架约定入口保留在模块级。

    入参：
        无。
    返回值：
        无。
    异常：
        无。
    """
    supplied = config.attributes.get("connection")
    if supplied is not None:
        with supplied.connect() as connection:
            context.configure(connection=connection, target_metadata=target_metadata)
            with context.begin_transaction():
                context.run_migrations()
        return
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = _EnvSupport._configured_url()
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
