"""提供策略与etf_rotation相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from math import isfinite
from types import MappingProxyType
from typing import cast

from quant_research.backtest.engine import StrategyRef
from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import canonical_factor_ref, is_available_on_signal_day
from quant_research.portfolio.constructor import TargetPortfolio, TargetPosition
from quant_research.strategies.base import (
    PortfolioState,
    RebalanceFrequency,
    StrategyContext,
    ValidationIssue,
    is_rebalance_boundary,
    validated_factor_values,
)

_RETURN_REFS = (
    "return_20d",
    "return_60d",
    "return_120d",
)
_DEFAULT_TREND = "trend_120d"
_DEFAULT_VOLATILITY = "volatility_60d"
_EPSILON = 1e-10


@dataclass(frozen=True, slots=True)
class EtfRotationConfig:
    """定义策略信号流程使用的不可变配置及取值约束。

    入参：
        返回完成字段规范化和不变量校验的对象。
        return_factor_weights：参与本次处理的收益因子``weights``；调用方不得依赖未声明的顺序。
        trend_factor_ref：``trend``因子引用。
        volatility_factor_ref：波动率因子引用。
        volatility_penalty：波动率``penalty``。
        top_n：入选数量倍数。
        frequency：调仓频率。
        missing_signal_policy：缺失项信号日期``policy``。
        weighting：加权方式。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    etf_pool: tuple[InstrumentId, ...]
    return_factor_weights: Mapping[str, float]
    trend_factor_ref: str = _DEFAULT_TREND
    volatility_factor_ref: str = _DEFAULT_VOLATILITY
    volatility_penalty: float = 0.0
    top_n: int = 1
    frequency: RebalanceFrequency = RebalanceFrequency.MONTHLY
    missing_signal_policy: str = "EXCLUDE"
    weighting: str = "EQUAL"

    def __post_init__(self) -> None:
        if not isinstance(self.etf_pool, tuple) or not self.etf_pool:
            raise ValueError("etf_pool must be a nonempty tuple")
        if any(not isinstance(item, InstrumentId) for item in self.etf_pool):
            raise TypeError("etf_pool must contain InstrumentId")
        canonical = tuple(item.canonical() for item in self.etf_pool)
        if canonical != tuple(sorted(canonical)) or len(set(canonical)) != len(
            canonical
        ):
            raise ValueError("etf_pool must be unique and canonical-ID sorted")
        if not isinstance(self.return_factor_weights, Mapping):
            raise TypeError("return_factor_weights must be a mapping")
        weights = {
            canonical_factor_ref(key): value
            for key, value in self.return_factor_weights.items()
        }
        if set(weights) != set(_RETURN_REFS):
            raise ValueError(
                "return_factor_weights must contain the three fixed return refs"
            )
        if any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not isfinite(value)
            or value < 0
            for value in weights.values()
        ):
            raise ValueError("return factor weights must be finite and nonnegative")
        if abs(sum(weights.values()) - 1.0) > _EPSILON:
            raise ValueError("return factor weights must sum to one")
        if canonical_factor_ref(self.trend_factor_ref) != _DEFAULT_TREND:
            raise ValueError("trend_factor_ref is not supported")
        if canonical_factor_ref(self.volatility_factor_ref) != _DEFAULT_VOLATILITY:
            raise ValueError("volatility_factor_ref is not supported")
        if (
            not isinstance(self.volatility_penalty, (int, float))
            or isinstance(self.volatility_penalty, bool)
            or not isfinite(self.volatility_penalty)
            or self.volatility_penalty < 0
        ):
            raise ValueError("volatility_penalty must be finite and nonnegative")
        if type(self.top_n) is not int or not 0 < self.top_n <= len(self.etf_pool):
            raise ValueError("top_n must be positive and no greater than etf_pool")
        if self.frequency is not RebalanceFrequency.MONTHLY:
            raise ValueError("ETF rotation frequency must be MONTHLY")
        if self.missing_signal_policy != "EXCLUDE":
            raise ValueError("missing_signal_policy must be EXCLUDE")
        if self.weighting != "EQUAL":
            raise ValueError("weighting must be EQUAL")
        object.__setattr__(
            self,
            "return_factor_weights",
            MappingProxyType(dict(sorted(weights.items()))),
        )

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> EtfRotationConfig:
        """从输入解析配置映射。

        入参：
            mapping：参与本次处理的配置映射；调用方不得依赖未声明的顺序。
        返回值：
            返回配置映射（``EtfRotationConfig``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        if not isinstance(mapping, Mapping):
            raise TypeError("ETF config must be a mapping")
        allowed = {
            "etf_pool",
            "return_factor_weights",
            "trend_factor_ref",
            "volatility_factor_ref",
            "volatility_penalty",
            "top_n",
            "frequency",
            "missing_signal_policy",
            "weighting",
        }
        unknown = set(mapping) - allowed
        if unknown:
            raise ValueError(f"unknown ETF config key: {min(unknown)}")
        required = {"etf_pool", "return_factor_weights", "volatility_penalty", "top_n"}
        missing = required - set(mapping)
        if missing:
            raise ValueError(f"missing ETF config key: {min(missing)}")
        pool = mapping["etf_pool"]
        if not isinstance(pool, list):
            raise TypeError("etf_pool must be a list")
        raw_weights = mapping["return_factor_weights"]
        if not isinstance(raw_weights, Mapping):
            raise TypeError("return_factor_weights must be a mapping")
        frequency = mapping.get("frequency", RebalanceFrequency.MONTHLY)
        return cls(
            tuple(InstrumentId.parse(cast(str, item)) for item in pool),
            cast(Mapping[str, float], raw_weights),
            cast(str, mapping.get("trend_factor_ref", _DEFAULT_TREND)),
            cast(str, mapping.get("volatility_factor_ref", _DEFAULT_VOLATILITY)),
            cast(float, mapping["volatility_penalty"]),
            cast(int, mapping["top_n"]),
            RebalanceFrequency(cast(str, frequency)),
            cast(str, mapping.get("missing_signal_policy", "EXCLUDE")),
            cast(str, mapping.get("weighting", "EQUAL")),
        )


class EtfRotationStrategy:
    """按趋势、动量与波动率在 ETF 候选池中生成轮动目标组合。

    入参：
        config：调用所用的配置对象，类型为 ``EtfRotationConfig``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    """

    strategy_id = "etf_rotation"

    def __init__(self, config: EtfRotationConfig) -> None:
        if not isinstance(config, EtfRotationConfig):
            raise TypeError("config must be an EtfRotationConfig")
        self.config = config

    @property
    def ref(self) -> StrategyRef:
        """处理策略信号中的``ref``。

        入参：
            无。
        返回值：
            返回``ref``（``StrategyRef``）。
        异常：
            无。
        """
        return StrategyRef(self.strategy_id)

    def validate(self, ctx: StrategyContext) -> list[ValidationIssue]:
        """校验策略信号。

        入参：
            ctx：本次计算的上下文，类型为 ``StrategyContext``。
        返回值：
            返回校验策略信号后的``validate``（``list[ValidationIssue]``）。
        异常：
            无。
        """
        if not isinstance(ctx, StrategyContext):
            return [ValidationIssue("INVALID_CONTEXT", "strategy context is invalid")]
        return []

    def should_rebalance(self, ctx: StrategyContext, rebalance_date: date) -> bool:
        """判断是否需要调仓。

        入参：
            ctx：本次计算的上下文，类型为 ``StrategyContext``。
            rebalance_date：限定本次业务操作覆盖范围的调仓日期（含边界）。
        返回值：
            返回是否是否需要调仓。
        异常：
            无。
        """
        return rebalance_date == ctx.signal_date and is_rebalance_boundary(
            ctx, self.config.frequency
        )

    def generate_targets(
        self, ctx: StrategyContext, rebalance_date: date, current: PortfolioState
    ) -> TargetPortfolio:
        """生成``targets``。

        入参：
            ctx：本次计算的上下文，类型为 ``StrategyContext``。
            rebalance_date：限定本次业务操作覆盖范围的调仓日期（含边界）。
            current：当前值。
        返回值：
            返回生成``targets``后的``targets``（``TargetPortfolio``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        if rebalance_date != ctx.signal_date:
            raise ValueError("rebalance_date must equal signal_date")
        if not isinstance(current, PortfolioState):
            raise TypeError("current must be a PortfolioState")
        if current.trade_date != rebalance_date:
            raise ValueError("current portfolio state must match rebalance_date")
        factor_refs = (
            *_RETURN_REFS,
            self.config.trend_factor_ref,
            self.config.volatility_factor_ref,
        )
        frame = validated_factor_values(
            ctx.data.factor_values(ctx.signal_date, self.config.etf_pool, factor_refs),
            signal_date=ctx.signal_date,
            instruments=self.config.etf_pool,
            factor_refs=factor_refs,
        )
        selected: list[tuple[InstrumentId, float]] = []
        rows = {
            (cast(str, row["instrument_id"]), cast(str, row["factor_ref"])): row
            for row in frame.iter_rows(named=True)
        }
        for instrument in self.config.etf_pool:
            values: dict[str, float] = {}
            complete = True
            for factor_ref in factor_refs:
                row = rows.get((instrument.canonical(), factor_ref))
                if (
                    row is None
                    or row["is_valid"] is not True
                    or not is_available_on_signal_day(
                        row["available_at"], ctx.signal_date
                    )
                ):
                    complete = False
                    break
                value = row["value"]
                if not isinstance(value, float) or not isfinite(value):
                    complete = False
                    break
                values[factor_ref] = value
            if not complete or values[self.config.trend_factor_ref] <= 0.0:
                continue
            score = (
                sum(
                    self.config.return_factor_weights[ref] * values[ref]
                    for ref in _RETURN_REFS
                )
                - self.config.volatility_penalty
                * values[self.config.volatility_factor_ref]
            )
            selected.append((instrument, score))
        selected.sort(key=lambda item: (-item[1], item[0].canonical()))
        winners = selected[: self.config.top_n]
        if not winners:
            return TargetPortfolio(ctx.signal_date, ctx.execute_date, (), 1.0)
        weight = 1.0 / len(winners)
        positions = tuple(
            TargetPosition(item, weight, score, "ETF_ROTATION_SELECTED")
            for item, score in winners
        )
        return TargetPortfolio(ctx.signal_date, ctx.execute_date, positions, 0.0)
