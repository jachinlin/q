"""Alembic environment for the local SQLite control database."""

from __future__ import annotations

import os
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

from quant_core.persistence.orm import Base

config = context.config
target_metadata = Base.metadata


def _configured_url() -> str:
    explicit = config.get_main_option("sqlalchemy.url")
    state_db = os.environ.get("QUANT_STATE_DB")
    if state_db:
        return f"sqlite+pysqlite:///{Path(state_db).resolve().as_posix()}"
    return explicit or "sqlite://"


def run_migrations_offline() -> None:
    context.configure(
        url=_configured_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    supplied = config.attributes.get("connection")
    if supplied is not None:
        with supplied.connect() as connection:
            context.configure(connection=connection, target_metadata=target_metadata)
            with context.begin_transaction():
                context.run_migrations()
        return
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = _configured_url()
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
