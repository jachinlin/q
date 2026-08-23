"""延迟暴露订单驱动回测、撮合、账户和规则契约。"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS = {
    "AShareRuleBook": "rulebook",
    "AccountExecutionView": "accounting",
    "AccountSnapshot": "accounting",
    "AccountView": "models",
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
    "MarketRuleBook": "rulebook",
    "MarketSlice": "models",
    "PortfolioAccount": "accounting",
    "PositionSnapshot": "accounting",
    "PriceBand": "rulebook",
    "RejectResult": "models",
    "SecurityStatus": "rulebook",
    "Side": "rulebook",
    "SimulatedFill": "rulebook",
    "TradingCalendar": "calendar",
}

__all__ = [
    "AShareRuleBook",
    "AccountExecutionView",
    "AccountSnapshot",
    "AccountView",
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
    "MarketRuleBook",
    "MarketSlice",
    "PortfolioAccount",
    "PositionSnapshot",
    "PriceBand",
    "RejectResult",
    "SecurityStatus",
    "Side",
    "SimulatedFill",
    "TradingCalendar",
]


def __getattr__(name: str) -> Any:
    """按需加载回测符号，避免账户与撮合模块形成导入环。

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
