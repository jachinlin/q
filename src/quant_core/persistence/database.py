"""SQLite engine construction and Alembic migration helpers."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.engine import URL
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.pool import ConnectionPoolEntry


def create_sqlite_engine(database_path: Path) -> Engine:
    """Create a SQLite engine with the control-plane safety pragmas enabled."""
    resolved = database_path.resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(
        URL.create("sqlite+pysqlite", database=str(resolved)),
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _configure_sqlite(
        dbapi_connection: DBAPIConnection, _: ConnectionPoolEntry
    ) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=5000")
        finally:
            cursor.close()

    return engine


def upgrade_database(database_path: Path) -> None:
    """Upgrade one explicit SQLite database to the current catalog schema."""
    engine = create_sqlite_engine(database_path)
    config = Config()
    migrations = Path(__file__).resolve().parent / "migrations"
    config.set_main_option("script_location", str(migrations))
    config.attributes["connection"] = engine
    command.upgrade(config, "head")
    engine.dispose()
