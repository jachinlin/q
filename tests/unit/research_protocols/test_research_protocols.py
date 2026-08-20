"""验证研究协议展开、测试集隔离和候选选型。"""

from quant_research.research_protocols import (
    CandidateEvaluation,
    ResearchConfigResolver,
    ResearchSelector,
)


def _yaml() -> str:
    return """name: 双均线研究
hypothesis: 短均线上穿长均线后存在趋势延续
research_mode: BACKTEST_EXPERIMENT
strategy_id: dual_ma_trend
benchmark: 510300.SH
initial_cash_fen: 100000000
research_protocol:
  train: {start: 2018-01-02, end: 2020-12-31}
  validation: {start: 2021-01-04, end: 2022-12-30}
  test: {start: 2023-01-03, end: 2024-12-31}
  parameter_search_space:
    signal.short_window_sessions: [10, 20]
    signal.long_window_sessions: [60, 120]
  selection:
    primary_metric: calmar
    direction: MAXIMIZE
    constraints:
      - {metric: max_drawdown, operator: GTE, threshold: -0.30}
    tie_breakers: [sharpe]
    multiple_testing_method: HOLM_BONFERRONI
    adjusted_alpha: 0.05
  random_seed: 7
universe: {component: fixed_instruments, instruments: [510300.SH]}
features: {component: moving_average, windows: [20, 120]}
signal: {component: dual_ma_directional, kind: DIRECTIONAL, short_window_sessions: 20, long_window_sessions: 120}
decision_schedule: {component: every_session}
risk: {estimator: asset_volatility_and_liquidity}
pretrade_cost: {component: liquidity_impact_surface}
portfolio: {constructor: directional_exposure_mapper}
rebalance: {policy: signal_state_change}
execution: {simulator: a_share_daily}
analytics: {analyzers: [time_series_signal, execution, performance]}
"""


def test_grid_expansion_is_deterministic_and_excludes_protocol() -> None:
    resolved = ResearchConfigResolver().resolve_yaml(_yaml())
    assert len(resolved.variants) == 4
    assert tuple(item.variant_id for item in resolved.variants) == tuple(
        sorted(item.variant_id for item in resolved.variants)
    )
    assert {
        (
            item.parameters["signal.short_window_sessions"],
            item.parameters["signal.long_window_sessions"],
        )
        for item in resolved.variants
    } == {(10, 60), (10, 120), (20, 60), (20, 120)}
    assert all(
        item.config["research_protocol"] == resolved.normalized["research_protocol"]
        for item in resolved.variants
    )


def test_selection_uses_validation_metrics_and_stable_rules() -> None:
    policy = ResearchConfigResolver().resolve_yaml(_yaml()).config.research_protocol.selection
    selection = ResearchSelector().select(
        (
            CandidateEvaluation("a", {"calmar": 1.2, "max_drawdown": -0.2, "sharpe": 1.0}, 0.001),
            CandidateEvaluation("b", {"calmar": 1.4, "max_drawdown": -0.4, "sharpe": 2.0}, 0.001),
            CandidateEvaluation("c", {"calmar": 1.3, "max_drawdown": -0.2, "sharpe": 1.1}, 0.002),
        ),
        policy,
    )
    assert selection.selected_variant_id == "c"
    assert selection.rejected["b"] == ("CONSTRAINT_GTE:max_drawdown",)


def test_test_period_cannot_be_a_search_target() -> None:
    text = _yaml().replace(
        "signal.short_window_sessions: [10, 20]",
        "research_protocol.test.start: [2023-01-03]",
    )
    try:
        ResearchConfigResolver().resolve_yaml(text)
    except ValueError as error:
        assert "cannot modify research_protocol" in str(error)
    else:
        raise AssertionError("research protocol search mutation must fail")
