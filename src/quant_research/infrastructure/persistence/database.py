"""提供持久化与数据库相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.util.exc import CommandError
from sqlalchemy import Engine, create_engine, event, text
from sqlalchemy.engine import URL
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.pool import ConnectionPoolEntry

from quant_research.domain.enums import Severity
from quant_research.domain.errors import ErrorDetail, QuantError


def create_sqlite_engine(database_path: Path) -> Engine:
    """创建并返回约定对象；该函数作为稳定公开 API保留在模块级。

    入参：
        database_path：经可信根边界校验后使用的数据库路径。
    返回值：
        返回创建``sqlite``引擎后的``sqlite``引擎（``Engine``）。
    异常：
        无。
    Create a SQLite engine with the control-plane safety pragmas enabled.
    """
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
    """处理基础设施中的``upgrade``数据库；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        database_path：经可信根边界校验后使用的数据库路径。
    返回值：
        无。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``QuantError``。
    Upgrade one explicit SQLite database to the current catalog schema.
    """
    engine = create_sqlite_engine(database_path)
    config = Config()
    migrations = Path(__file__).resolve().parent / "migrations"
    config.set_main_option("script_location", str(migrations))
    config.attributes["connection"] = engine
    try:
        command.upgrade(config, "head")
    except CommandError as error:
        revision = _DatabaseSupport._current_revision(engine)
        raise QuantError(
            ErrorDetail(
                code="DATA_STATE_INCOMPATIBLE",
                severity=Severity.FATAL,
                message="the SQLite state database is not compatible with this project",
                context={"revision": revision},
                remediation=(
                    "move or remove the state database and run bootstrap; preserve "
                    "raw files separately if they must be re-indexed"
                ),
                retryable=False,
            )
        ) from error
    finally:
        engine.dispose()


class _DatabaseSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _current_revision(engine: Engine) -> str | None:
        try:
            with engine.connect() as connection:
                value = connection.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one_or_none()
        except Exception:  # noqa: BLE001 - diagnostic must not mask migration failure.
            return None
        return value if isinstance(value, str) else None
