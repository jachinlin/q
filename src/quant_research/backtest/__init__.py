"""延迟暴露待迁移到 ``execution`` 的旧回测内核，避免包导入环。"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "AShareRuleBook": "rulebook",
    "AccountExecutionView": "accounting",
    "AccountSnapshot": "accounting",
    "AccountView": "models",
    "ArtifactEntry": "artifacts",
    "BacktestArtifactRecovery": "artifacts",
    "BacktestArtifactWriter": "artifacts",
    "BacktestCancelled": "engine",
    "BacktestEngine": "engine",
    "BacktestRequest": "engine",
    "BacktestResult": "engine",
    "BoundMarketSlice": "engine",
    "ExecutionBatch": "models",
    "ExecutionConfig": "models",
    "ExecutionModel": "execution",
    "ExecutionPrice": "models",
    "ExecutionReason": "models",
    "FeeBreakdown": "rulebook",
    "FillResult": "models",
    "InstrumentTradingProfile": "rulebook",
    "LedgerEvent": "accounting",
    "LedgerEventType": "accounting",
    "ManifestContext": "artifacts",
    "MarketRuleBook": "rulebook",
    "MarketSlice": "models",
    "PortfolioAccount": "accounting",
    "PositionSnapshot": "accounting",
    "PriceBand": "rulebook",
    "RejectResult": "models",
    "SecurityStatus": "rulebook",
    "Side": "rulebook",
    "SimulatedFill": "rulebook",
    "StrategyRef": "engine",
    "TradingCalendar": "calendar",
    "WriterState": "artifacts",
    "validate_backtest_artifacts": "artifacts",
}

__all__ = [
    "AShareRuleBook",
    "AccountExecutionView",
    "AccountSnapshot",
    "AccountView",
    "ArtifactEntry",
    "BacktestArtifactRecovery",
    "BacktestArtifactWriter",
    "BacktestCancelled",
    "BacktestEngine",
    "BacktestRequest",
    "BacktestResult",
    "BoundMarketSlice",
    "ExecutionBatch",
    "ExecutionConfig",
    "ExecutionModel",
    "ExecutionPrice",
    "ExecutionReason",
    "FeeBreakdown",
    "FillResult",
    "InstrumentTradingProfile",
    "LedgerEvent",
    "LedgerEventType",
    "ManifestContext",
    "MarketRuleBook",
    "MarketSlice",
    "PortfolioAccount",
    "PositionSnapshot",
    "PriceBand",
    "RejectResult",
    "SecurityStatus",
    "Side",
    "SimulatedFill",
    "StrategyRef",
    "TradingCalendar",
    "WriterState",
    "validate_backtest_artifacts",
]


def __getattr__(name: str) -> Any:
    """按需加载旧内核符号；目标业务代码不得依赖本入口。

该函数作为模块级确定性辅助或框架入口保留。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(name)
    return getattr(import_module(f"quant_research.backtest.{module_name}"), name)
