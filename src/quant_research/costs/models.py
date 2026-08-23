"""分离事前成本估计与账户实际费用核算。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import isfinite, sqrt


@dataclass(frozen=True, slots=True)
class CostEstimate:
    """表示一个证券方向上的事前成本曲面参数。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    instrument_id: str
    fixed_cost: float
    linear_coefficient: float
    impact_coefficient: float
    capacity_limit: float


@dataclass(frozen=True, slots=True)
class PreTradeCostSlice:
    """表示一个决策日的事前成本参数。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    signal_date: date
    estimates: tuple[CostEstimate, ...]

    def estimate(self, instrument_id: str, weight_change: float) -> float:
        """估计给定组合权重变化的费用比例。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        item = next(
            (value for value in self.estimates if value.instrument_id == instrument_id),
            None,
        )
        if item is None or weight_change == 0.0:
            return 0.0
        size = abs(weight_change)
        return max(item.fixed_cost, item.linear_coefficient * size) + (
            item.impact_coefficient * sqrt(size)
        )


class LiquidityImpactCostModel:
    """根据 ADV 和参与率平方根假设生成事前成本曲面。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self, *, fixed_bps: float, impact_bps: float, max_participation: float
    ) -> None:
        if any(not isfinite(value) or value < 0.0 for value in (fixed_bps, impact_bps)):
            raise ValueError("cost bps must be finite and non-negative")
        if not 0.0 < max_participation <= 1.0:
            raise ValueError("max_participation must be in (0, 1]")
        self._fixed = fixed_bps / 10_000.0
        self._impact = impact_bps / 10_000.0
        self._participation = max_participation

    def build(
        self, signal_date: date, liquidity: dict[str, float], portfolio_value: float
    ) -> PreTradeCostSlice:
        """按证券 ADV 生成权重容量和冲击参数。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        estimates = tuple(
            CostEstimate(
                instrument_id,
                self._fixed,
                0.0,
                self._impact,
                max(0.0, amount * self._participation / portfolio_value)
                if portfolio_value > 0.0
                else 0.0,
            )
            for instrument_id, amount in sorted(liquidity.items())
        )
        return PreTradeCostSlice(signal_date, estimates)


@dataclass(frozen=True, slots=True)
class RealizedCost:
    """记录一笔实际成交产生的规则费用和滑点。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    commission_fen: int
    tax_fen: int
    slippage_fen: int

    @property
    def total_fen(self) -> int:
        """返回账户实际扣减总额。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return self.commission_fen + self.tax_fen + self.slippage_fen


class RealizedCostModel:
    """根据实际成交金额核算佣金、卖出税费和滑点。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self, *, commission_bps: float, sell_tax_bps: float, slippage_bps: float
    ) -> None:
        self._commission = commission_bps / 10_000.0
        self._tax = sell_tax_bps / 10_000.0
        self._slippage = slippage_bps / 10_000.0

    def calculate(self, notional_fen: int, *, is_sell: bool) -> RealizedCost:
        """只依据实际成交名义金额返回整数分费用。

        入参：
            参数和字段含义由公开签名及类型声明给出。
        返回值：
            返回该操作构造、计算或查询得到的领域结果。
        异常：
            输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        amount = abs(notional_fen)
        return RealizedCost(
            round(amount * self._commission),
            round(amount * self._tax) if is_sell else 0,
            round(amount * self._slippage),
        )
