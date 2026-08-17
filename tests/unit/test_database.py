"""SQLite initialization errors remain actionable at the CLI boundary."""

from pathlib import Path

import pytest
from sqlalchemy import text

from quant_research.domain.errors import QuantError
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)


def test_unknown_legacy_revision_is_reported_as_incompatible_state(
    tmp_path: Path,
) -> None:
    database = tmp_path / "legacy.db"
    engine = create_sqlite_engine(database)
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE alembic_version (version_num VARCHAR(64))")
        )
        connection.execute(
            text("INSERT INTO alembic_version VALUES ('0007_quality_rule_set_version')")
        )
    engine.dispose()

    with pytest.raises(QuantError) as caught:
        upgrade_database(database)

    assert caught.value.detail.code == "DATA_STATE_INCOMPATIBLE"
    assert caught.value.detail.context == {"revision": "0007_quality_rule_set_version"}
