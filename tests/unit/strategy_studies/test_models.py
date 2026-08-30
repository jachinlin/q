"""验证单一策略研究配置与应用服务。"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest

from quant_research.application.strategy_studies import StrategyStudyService
from quant_research.strategies.registry import StrategyRegistry
from quant_research.strategy_studies.config import StrategyStudyConfigParser


def strategy_study_yaml(end_date: str = "2024-12-31") -> str:
    """返回可直接提交的双均线策略研究 YAML。"""
    return "\n".join(
        (
            "name: dual ma",
            "description: trend",
            "tags: [trend]",
            "start_date: 2018-01-01",
            f"end_date: {end_date}",
            "strategy:",
            "  strategy_id: dual_ma_trend",
            "  parameters: {instrument_id: 510300.SH, short_window: 20, long_window: 60}",
            "benchmark: 000300.SH",
            "initial_cash_fen: 1000000",
            "execution: {reference_price: OPEN, slippage_bps: 0.0, max_volume_participation: 0.1, limit_order_policy: REJECT}",
        )
    )


def test_strict_config_is_deterministic_and_has_one_date_range() -> None:
    """相同定义必须产生相同身份且不接受样本治理字段。"""
    parser = StrategyStudyConfigParser()
    first = parser.parse(strategy_study_yaml())
    second = parser.parse(strategy_study_yaml())
    assert first.config_hash == second.config_hash
    assert first.definition.start_date.isoformat() == "2018-01-01"
    assert first.definition.end_date.isoformat() == "2024-12-31"
    with pytest.raises(ValueError, match="sample_windows"):
        parser.parse(strategy_study_yaml() + "\nsample_windows: {}")


@pytest.mark.parametrize(
    "replacement, message",
    [
        ("tags: [trend, trend]", "tags"),
        ("start_date: yesterday", "start_date"),
        ("unknown: true", "unknown"),
    ],
)
def test_invalid_tags_dates_and_unknown_fields_are_rejected(
    replacement: str, message: str
) -> None:
    """标签、日期和未知字段必须由严格 Schema 拒绝。"""
    text = strategy_study_yaml()
    if replacement.startswith("tags:"):
        text = text.replace("tags: [trend]", replacement)
    elif replacement.startswith("start_date:"):
        text = text.replace("start_date: 2018-01-01", replacement)
    else:
        text += f"\n{replacement}"
    with pytest.raises(ValueError, match=message):
        StrategyStudyConfigParser().parse(text)


def test_checked_in_examples_are_strict_and_strategy_buildable() -> None:
    """仓库示例必须可由真实策略目录构造。"""
    parser = StrategyStudyConfigParser()
    strategies = StrategyRegistry.builtins(
        commission_bps=3.0, commission_minimum_fen=500
    )
    paths = tuple(sorted(Path("configs/strategy_studies/examples").glob("*.yaml")))
    assert [path.name for path in paths] == [
        "dual_ma_trend.yaml",
        "etf_rotation.yaml",
        "multifactor.yaml",
    ]
    for path in paths:
        definition = parser.parse_file(path).definition
        strategies.validate(
            definition.strategy.strategy_id, definition.strategy.parameters
        )


@dataclass(frozen=True)
class _CatalogIdentity:
    catalog_hash: str


class _Catalog:
    def require_validated_catalog(self) -> _CatalogIdentity:
        return _CatalogIdentity("a" * 64)


class _Registry:
    def __init__(self) -> None:
        self.created = False

    def create(self, *args: object, **kwargs: object) -> tuple[str, str]:
        del args, kwargs
        self.created = True
        return "study-1", "task-1"

    def get(self, study_id: str) -> Any:
        assert study_id == "study-1"
        return study_id


def test_submit_validates_strategy_and_captures_catalog_once() -> None:
    """提交必须先通过真实策略参数校验再创建唯一研究任务。"""
    registry = _Registry()
    service = StrategyStudyService(
        cast(Any, registry),
        _Catalog(),
        StrategyRegistry.builtins(
            commission_bps=3.0, commission_minimum_fen=500
        ),
    )
    assert service.submit(strategy_study_yaml()) == "study-1"
    assert registry.created
