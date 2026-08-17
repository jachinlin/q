"""实验详情关联最新后台任务的测试。"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

from quant_research.config import Settings
from quant_research.dashboard.market_review import MarketReviewService
from quant_research.dashboard.views import DashboardViewService
from quant_research.data.contracts import canonical_json_bytes
from quant_research.data.repository import ResearchDataRepository
from quant_research.experiments.models import ExperimentSpec
from quant_research.infrastructure.baostock.routing import BAOSTOCK_ROUTES
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue


def test_experiment_detail_returns_latest_bound_task(tmp_path: Path) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    config = {"strategy_id": "etf_rotation"}
    config_hash = hashlib.sha256(canonical_json_bytes(config)).hexdigest()
    spec = ExperimentSpec(
        strategy_id="etf_rotation",
        config=config,
        config_hash=config_hash,
        data_hash="a" * 64,
        source_tree_hash="b" * 64,
        git_commit_hash=None,
        lockfile_hash="c" * 64,
        rulebook_hash="d" * 64,
        fingerprint="e" * 64,
        created_at=datetime(2026, 8, 15, tzinfo=UTC),
    )
    experiment_id, task_id = TaskQueue(engine).create_experiment_and_submit(spec)
    service = DashboardViewService(
        engine,
        Settings(
            timezone=ZoneInfo("Asia/Shanghai"),
            data_root=tmp_path,
        ),
        cast(ResearchDataRepository, object()),
        cast(MarketReviewService, object()),
        BAOSTOCK_ROUTES,
    )

    detail = service.experiment_detail(experiment_id)

    assert detail["latest_task"] == {"id": task_id, "status": "QUEUED"}
    engine.dispose()
