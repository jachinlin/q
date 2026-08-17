from __future__ import annotations

from datetime import date

from quant_research.factor_studies.models import FactorRunStatus, FactorStudyConfig
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.factor_studies import (
    FactorStudyRepository,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue


def test_study_and_run_are_independent_from_experiments(tmp_path) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    try:
        repository = FactorStudyRepository(engine)
        study_id = repository.create_study(
            "价值研究",
            FactorStudyConfig(
                factor_refs=("earnings_yield_ttm",),
                start_date=date(2024, 1, 2),
                end_date=date(2024, 12, 31),
            ),
        )
        run_id = repository.create_run(study_id, "a" * 64, "b" * 64)
        task_id = TaskQueue(engine).enqueue(
            "FACTOR_ANALYSIS",
            {
                "run_id": run_id,
                "config_hash": repository.get_run(run_id)["config_hash"],
            },
            0,
        )
        repository.bind_task(run_id, task_id)
        run = repository.get_run(run_id)
    finally:
        engine.dispose()

    assert run["study_id"] == study_id
    assert run["task_id"] == task_id
    assert run["status"] == FactorRunStatus.QUEUED.value
    assert run["config"]["ic_rolling_window"] == 20
    assert run["config"]["ic_rolling_min_valid"] == 10
    assert run["config"]["ic_quantile_probabilities"] == [0.05, 0.25, 0.5, 0.75, 0.95]
