"""验证研究族持久化、确定性展开和 TEST 锁定。"""

from datetime import UTC, datetime
from pathlib import Path

from quant_research.experiments.research import ResearchPhase
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.research_registry import ResearchRegistry
from quant_research.research_protocols import ResearchConfigResolver


def _config() -> str:
    return """name: test family
hypothesis: trend persists
research_mode: SIGNAL_STUDY
strategy_id: dual_ma_trend
benchmark: 510300.SH
initial_cash_fen: 1000000
research_protocol:
  train: {start: 2019-01-01, end: 2020-12-31}
  validation: {start: 2021-01-01, end: 2022-12-31}
  test: {start: 2023-01-01, end: 2024-12-31}
  parameter_search_space:
    signal.short_window_sessions: [10, 20]
  selection:
    primary_metric: sharpe
    direction: MAXIMIZE
    constraints: []
    tie_breakers: []
    multiple_testing_method: NONE
  random_seed: 0
universe: {component: fixed_instruments, instruments: [510300.SH]}
features: {component: moving_average}
signal: {component: dual_ma_directional, short_window_sessions: 20}
decision_schedule: {component: every_session}
risk: {estimator: asset_volatility_and_liquidity}
pretrade_cost: {component: liquidity_impact_surface}
portfolio: {constructor: directional_exposure_mapper}
rebalance: {policy: signal_state_change}
execution: {simulator: a_share_daily}
analytics: {analyzers: [time_series_signal]}
"""


def test_registry_creates_variants_and_one_locked_test_run(tmp_path: Path) -> None:
    database = tmp_path / "state.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    registry = ResearchRegistry(engine, clock=lambda: now)
    resolved = ResearchConfigResolver().resolve_yaml(_config())
    family, execution = registry.create_family(
        resolved,
        catalog_hash="a" * 64,
        source_hash="b" * 64,
        lockfile_hash="c" * 64,
        rulebook_hash="d" * 64,
        environment_hash="e" * 64,
    )
    runs = registry.expand(execution.id, resolved.variants)
    assert family.strategy_id == "dual_ma_trend"
    assert len(runs) == 2
    assert {item.phase for item in runs} == {ResearchPhase.TRAIN_VALIDATION}
    selected = registry.list_variants(execution.id)[0]
    test_run = registry.create_test_run(execution.id, selected.id, "validation sharpe")
    assert test_run.phase is ResearchPhase.TEST
    assert registry.get_execution(execution.id).selected_variant_id == selected.id
    assert len(registry.list_runs(execution.id)) == 3
    engine.dispose()
