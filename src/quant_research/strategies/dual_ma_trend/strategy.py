"""实现前复权双均线趋势策略。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from math import isfinite

import polars as pl

from quant_research.data.contracts import JsonValue
from quant_research.domain.enums import DatasetKind
from quant_research.domain.identifiers import InstrumentId
from quant_research.strategies.base import (
    DecisionContext,
    StrategySpec,
    TargetWeights,
    WeightTargetStrategy,
)


class TrendState(StrEnum):
    """表示双均线在有效信号日结束后的目标状态。

    入参：``LONG`` 或 ``FLAT`` 字符串。返回值：趋势状态成员。异常：未知值抛出 ``ValueError``。
    """

    LONG = "LONG"
    FLAT = "FLAT"


@dataclass(frozen=True, slots=True)
class DualMAConfig:
    """定义双均线窗口、目标仓位和目标差额容差。

    入参：证券、短长窗口、多头与空仓权重、续单容差。返回值：冻结配置。异常：窗口关系或 P3 权重非法时抛出错误。
    """

    instrument_id: InstrumentId
    short_window: int = 20
    long_window: int = 60
    long_weight: float = 1.0
    flat_weight: float = 0.0
    target_tolerance: float = 0.001

    def __post_init__(self) -> None:
        if not isinstance(self.instrument_id, InstrumentId):
            raise TypeError("instrument_id must be an InstrumentId")
        if type(self.short_window) is not int or type(self.long_window) is not int:
            raise TypeError("MA windows must be integers")
        if self.short_window <= 0 or self.long_window <= self.short_window:
            raise ValueError("MA windows must satisfy 0 < short_window < long_window")
        for value, name in (
            (self.long_weight, "long_weight"),
            (self.flat_weight, "flat_weight"),
            (self.target_tolerance, "target_tolerance"),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not isfinite(float(value))
            ):
                raise TypeError(f"{name} must be finite")
        if not 0 < self.long_weight <= 1:
            raise ValueError("long_weight must be in (0, 1]")
        if self.flat_weight != 0:
            raise ValueError("P3 flat_weight must be zero")
        if not 0 <= self.target_tolerance <= 0.1:
            raise ValueError("target_tolerance must be in [0, 0.1]")

    @classmethod
    def from_mapping(cls, value: Mapping[str, JsonValue]) -> DualMAConfig:
        """从严格参数映射创建配置，未知字段立即失败。

        入参：YAML 解析后的参数映射。返回值：``DualMAConfig``。异常：缺少证券、字段未知或类型非法时抛出错误。
        """
        allowed = {
            "instrument_id",
            "short_window",
            "long_window",
            "long_weight",
            "flat_weight",
            "target_tolerance",
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unknown dual_ma_trend parameter: {min(unknown)}")
        if "instrument_id" not in value:
            raise ValueError("instrument_id is required")
        raw_id = value["instrument_id"]
        if not isinstance(raw_id, str):
            raise TypeError("instrument_id must be a string")
        return cls(
            instrument_id=InstrumentId.parse(raw_id),
            short_window=_DualMAValues.integer(
                value.get("short_window", 20), "short_window"
            ),
            long_window=_DualMAValues.integer(
                value.get("long_window", 60), "long_window"
            ),
            long_weight=_DualMAValues.number(
                value.get("long_weight", 1.0), "long_weight"
            ),
            flat_weight=_DualMAValues.number(
                value.get("flat_weight", 0.0), "flat_weight"
            ),
            target_tolerance=_DualMAValues.number(
                value.get("target_tolerance", 0.001), "target_tolerance"
            ),
        )


class DualMATrendStrategy(WeightTargetStrategy):
    """以连续有效前复权收盘价计算双均线并持续追踪目标差额。

    入参：双均线配置和再平衡规划器。返回值：每日订单与信号审计表。异常：依赖类型、行情窗口或订单转换非法时抛出错误。
    """

    def __init__(self, config: DualMAConfig, planner: object) -> None:
        from quant_research.portfolio.rebalance import RebalancePlanner

        if not isinstance(config, DualMAConfig):
            raise TypeError("config must be DualMAConfig")
        if not isinstance(planner, RebalancePlanner):
            raise TypeError("planner must be RebalancePlanner")
        super().__init__(planner, target_tolerance=config.target_tolerance)
        self.config = config
        self._state: TrendState | None = None
        self._signals: list[dict[str, object]] = []

    @property
    def spec(self) -> StrategySpec:
        """返回策略身份、频率、数据依赖和冻结参数。

        入参：无。返回值：``dual_ma_trend`` 策略规格。异常：无。
        """
        return StrategySpec(
            strategy_id="dual_ma_trend",
            frequency="DAILY",
            data_dependencies=(
                DatasetKind.FUND_DAILY_BAR,
                DatasetKind.FUND_ADJUSTMENT_FACTOR,
            ),
            factor_dependencies=(),
            parameters={
                "instrument_id": self.config.instrument_id.canonical(),
                "short_window": self.config.short_window,
                "long_window": self.config.long_window,
                "long_weight": self.config.long_weight,
                "flat_weight": self.config.flat_weight,
                "target_tolerance": self.config.target_tolerance,
            },
        )

    def target_weights(self, ctx: DecisionContext) -> TargetWeights | None:
        """在有效窗口产生 LONG/FLAT 状态变化；无效日不推进状态。

        入参：绑定当日的决策上下文。返回值：状态变化时的新权重，否则为 ``None``。异常：数据端口读取失败时保留原异常。
        """
        frame = (
            ctx.data.adjusted_bars(
                (self.config.instrument_id,), self.config.long_window
            )
            .collect()
            .sort("trade_date")
        )
        valid = self._continuous_prices(frame, ctx)
        if valid is None:
            self._signals.append(
                self._signal_row(ctx, None, None, None, False, "INVALID_WINDOW")
            )
            return None
        prices = valid
        short_ma = sum(prices[-self.config.short_window :]) / self.config.short_window
        long_ma = sum(prices) / self.config.long_window
        state = TrendState.LONG if short_ma > long_ma else TrendState.FLAT
        changed = state is not self._state
        previous = self._state
        self._state = state
        self._signals.append(
            self._signal_row(ctx, short_ma, long_ma, state, changed, None)
        )
        if not changed or (previous is None and state is TrendState.FLAT):
            return None
        weight = (
            self.config.long_weight
            if state is TrendState.LONG
            else self.config.flat_weight
        )
        return TargetWeights(
            ctx.signal_date, ctx.execute_date, {self.config.instrument_id: weight}
        )

    def signal_frame(self) -> pl.DataFrame:
        """返回按决策日稳定排序的双均线信号审计表。

        入参：无。返回值：含均线、状态、变化标记和无效原因的表。异常：无。
        """
        return (
            pl.DataFrame(self._signals).sort("signal_date")
            if self._signals
            else pl.DataFrame(
                schema={
                    "signal_date": pl.Date,
                    "instrument_id": pl.String,
                    "short_ma": pl.Float64,
                    "long_ma": pl.Float64,
                    "state": pl.String,
                    "state_changed": pl.Boolean,
                    "invalid_reason": pl.String,
                }
            )
        )

    def _continuous_prices(
        self, frame: pl.DataFrame, ctx: DecisionContext
    ) -> list[float] | None:
        if len(frame) != self.config.long_window or "trade_date" not in frame.columns:
            return None
        if frame["trade_date"][-1] != ctx.signal_date:
            return None
        price_column = (
            "adjusted_close" if "adjusted_close" in frame.columns else "close"
        )
        if price_column not in frame.columns:
            return None
        prices: list[float] = []
        for raw in frame[price_column].to_list():
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                return None
            value = float(raw)
            if not isfinite(value) or value <= 0:
                return None
            prices.append(value)
        return prices

    def _signal_row(
        self,
        ctx: DecisionContext,
        short_ma: float | None,
        long_ma: float | None,
        state: TrendState | None,
        changed: bool,
        invalid_reason: str | None,
    ) -> dict[str, object]:
        return {
            "signal_date": ctx.signal_date,
            "instrument_id": self.config.instrument_id.canonical(),
            "short_ma": short_ma,
            "long_ma": long_ma,
            "state": state.value if state is not None else None,
            "state_changed": changed,
            "invalid_reason": invalid_reason,
        }


class _DualMAValues:
    @staticmethod
    def integer(value: JsonValue, name: str) -> int:
        if type(value) is not int:
            raise TypeError(f"{name} must be an integer")
        return value

    @staticmethod
    def number(value: JsonValue, name: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{name} must be numeric")
        return float(value)


__all__ = ["DualMAConfig", "DualMATrendStrategy", "TrendState"]
