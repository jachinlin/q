"""验证纯策略 Experiment/Run 严格配置和 TEST 隔离语义。"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest

from quant_research.application.experiments import ExperimentService
from quant_research.domain.enums import MultipleTestingMethod
from quant_research.experiments.config import ExperimentConfigParser
from quant_research.experiments.statistics import MultipleTestingCorrector
from quant_research.strategies.registry import StrategyRegistry


@dataclass(frozen=True)
class _CatalogIdentity:
    """提供实验服务测试使用的固定目录身份。"""

    catalog_hash: str


class _ValidatedCatalog:
    """记录实验提交是否通过显式目录门禁读取身份。"""

    def __init__(self, catalog_hash: str) -> None:
        self.catalog_hash = catalog_hash
        self.calls = 0

    def require_validated_catalog(self) -> _CatalogIdentity:
        """返回固定目录身份并记录门禁调用次数。"""
        self.calls += 1
        return _CatalogIdentity(self.catalog_hash)


def experiment_yaml(end_date: str = "2022-12-31") -> str:
    """返回可直接提交的双均线实验 YAML。"""
    return "\n".join(
        (
            "name: dual ma",
            "description: trend",
            "tags: [trend]",
            "sample_windows:",
            "  train: {start: 2018-01-01, end: 2020-12-31}",
            "  validation: {start: 2021-01-01, end: 2022-12-31}",
            "  test: {start: 2023-01-01, end: 2024-12-31}",
            "governance: {test_budget: 1, correction: BONFERRONI}",
            "initial_run:",
            "  start_date: 2018-01-01",
            f"  end_date: {end_date}",
            "  strategy:",
            "    strategy_id: dual_ma_trend",
            "    parameters: {instrument_id: 510300.SH, short_window: 20, long_window: 60}",
            "  benchmark: 000300.SH",
            "  initial_cash_fen: 1000000",
            "  execution: {reference_price: OPEN, slippage_bps: 0.0, max_volume_participation: 0.1, limit_order_policy: REJECT}",
        )
    )


def test_strict_config_is_deterministic_and_marks_test_use() -> None:
    parser = ExperimentConfigParser()
    resolved = parser.parse_experiment(experiment_yaml("2023-01-02"))
    repeated = parser.parse_experiment(experiment_yaml("2023-01-02"))
    assert resolved.config_hash == repeated.config_hash
    assert resolved.definition.uses_test_region(resolved.definition.initial_run)


def test_unknown_fields_are_rejected() -> None:
    parser = ExperimentConfigParser()
    invalid = experiment_yaml().replace("  benchmark:", "  unknown: true\n  benchmark:")
    try:
        parser.parse_experiment(invalid)
    except ValueError as error:
        assert "unknown" in str(error)
    else:
        raise AssertionError("unknown field was accepted")


def test_all_checked_in_examples_are_strict_and_strategy_buildable() -> None:
    parser = ExperimentConfigParser()
    strategies = StrategyRegistry.builtins(
        commission_bps=3.0, commission_minimum_fen=500
    )
    root = Path("configs/experiments/examples")
    paths = tuple(sorted(root.glob("*.yaml")))
    assert [path.name for path in paths] == [
        "dual_ma_trend.yaml",
        "etf_rotation.yaml",
        "multifactor.yaml",
    ]
    for path in paths:
        definition = parser.parse_experiment_file(path).definition
        run = definition.initial_run
        strategies.validate(run.strategy.strategy_id, run.strategy.parameters)


def test_legacy_factor_study_experiment_is_rejected() -> None:
    parser = ExperimentConfigParser()
    legacy = """name: old factor study
kind: FACTOR_STUDY
tags: [factor]
sample_windows:
  train: {start: 2018-01-01, end: 2020-12-31}
  validation: {start: 2021-01-01, end: 2021-12-31}
  test: {start: 2022-01-01, end: 2022-12-31}
governance: {test_budget: 1, correction: BH_FDR}
initial_run:
  kind: FACTOR_STUDY
  start_date: 2018-01-01
  end_date: 2022-12-31
  factor_study: {factor_ids: [value], horizons: [5]}
"""
    with pytest.raises(ValueError, match="kind|strategy"):
        parser.parse_experiment(legacy)


def test_multiple_testing_corrections_use_literal_oracles() -> None:
    values = (0.01, 0.04, 0.03)
    assert MultipleTestingCorrector.adjust(
        MultipleTestingMethod.BONFERRONI, values
    ) == (0.03, 0.12, 0.09)
    assert MultipleTestingCorrector.adjust(MultipleTestingMethod.BH_FDR, values) == (
        0.03,
        0.04,
        0.04,
    )


def test_submit_reads_catalog_hash_through_validated_catalog_gate() -> None:
    catalog = _ValidatedCatalog("a" * 64)
    service = ExperimentService(
        cast(Any, object()),
        catalog,
        StrategyRegistry.builtins(
            commission_bps=3.0, commission_minimum_fen=500
        ),
    )

    assert service._catalog_hash() == "a" * 64
    assert catalog.calls == 1
