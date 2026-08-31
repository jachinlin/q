"""暴露双均线趋势策略的配置、状态和实现。"""

from quant_research.strategies.dual_ma_trend.strategy import (
    DualMAConfig,
    DualMATrendStrategy,
    TrendState,
)

__all__ = ["DualMAConfig", "DualMATrendStrategy", "TrendState"]
