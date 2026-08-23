"""延迟暴露策略插件、PIT 决策视图和内置策略。"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "AccountView": "base",
    "ConstraintSet": "components",
    "ComponentRef": "components",
    "CrossSectionalPortfolioAssembler": "cross_sectional",
    "DecisionContext": "base",
    "DecisionData": "base",
    "DualMAConfig": "dual_ma",
    "DualMATrendStrategy": "dual_ma",
    "EtfRotationConfig": "etf_rotation",
    "EtfRotationStrategy": "etf_rotation",
    "MultifactorConfig": "multifactor",
    "MultifactorStrategy": "multifactor",
    "OrderIntent": "base",
    "OrderSide": "base",
    "Strategy": "base",
    "StrategyComponentCatalog": "components",
    "StrategyPipelineConfig": "components",
    "StrategyRegistry": "registry",
    "StrategySpec": "base",
    "TargetWeights": "base",
    "TrendState": "dual_ma",
    "WeightTargetStrategy": "base",
    "ScoredInstrument": "cross_sectional",
}

__all__ = [
    "AccountView",
    "ComponentRef",
    "ConstraintSet",
    "CrossSectionalPortfolioAssembler",
    "DecisionContext",
    "DecisionData",
    "DualMAConfig",
    "DualMATrendStrategy",
    "EtfRotationConfig",
    "EtfRotationStrategy",
    "MultifactorConfig",
    "MultifactorStrategy",
    "OrderIntent",
    "OrderSide",
    "ScoredInstrument",
    "Strategy",
    "StrategyComponentCatalog",
    "StrategyPipelineConfig",
    "StrategyRegistry",
    "StrategySpec",
    "TargetWeights",
    "TrendState",
    "WeightTargetStrategy",
]


def __getattr__(name: str) -> Any:
    """按需加载策略符号，避免策略与组合模块形成导入环；这是包模块级框架入口。

    入参：公开导出符号名。返回值：对应策略模块中的对象。异常：符号未登记时抛出 ``AttributeError``。
    """
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(name)
    return getattr(import_module(f"quant_research.strategies.{module_name}"), name)
