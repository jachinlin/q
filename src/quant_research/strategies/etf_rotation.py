"""实现由五模块流水线装配的固定 ETF 池轮动预设。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from math import isfinite, sqrt
from typing import cast

import polars as pl

from quant_research.data.contracts import JsonValue
from quant_research.domain.enums import DatasetKind
from quant_research.domain.identifiers import InstrumentId
from quant_research.strategies.base import (
    DecisionContext,
    RebalancePlanner,
    StrategySpec,
    TargetWeights,
    WeightTargetStrategy,
)
from quant_research.strategies.components import StrategyPipelineConfig
from quant_research.strategies.cross_sectional import (
    CrossSectionalPortfolioAssembler,
    ScoredInstrument,
)


@dataclass(frozen=True, slots=True)
class EtfRotationConfig:
    """定义 ETF 动量、趋势、波动率 Alpha 及其余四模块装配。

    入参：通过组件目录校验的不可变流水线配置。
    返回值：ETF 轮动预设配置。
    异常：频率、ETF 池或 Alpha 参数非法时抛出类型或值错误。
    """

    pipeline: StrategyPipelineConfig

    def __post_init__(self) -> None:
        if self.pipeline.frequency != "MONTHLY":
            raise ValueError("etf_rotation frequency must be MONTHLY")
        params = self.pipeline.alpha.params
        allowed = (
            {"etf_pool", "lookback", "direction"}
            if self.pipeline.alpha.model_id == "single_factor"
            else {
                "etf_pool",
                "momentum_windows",
                "momentum_weights",
                "trend_window",
                "trend_weight",
                "volatility_window",
                "volatility_weight",
            }
        )
        unknown = set(params) - allowed
        if unknown:
            raise ValueError(f"unknown ETF alpha parameter: {min(unknown)}")
        _ = self.etf_pool
        _ = self.lookback
        if self.pipeline.alpha.model_id == "multi_factor_composite":
            windows = self.momentum_windows
            weights = self.momentum_weights
            if len(windows) != len(weights):
                raise ValueError("momentum_windows and momentum_weights must align")
            for field in ("trend_weight", "volatility_weight"):
                self._number(params.get(field, 1.0), field)
        else:
            self._number(params.get("direction", 1.0), "direction")

    @classmethod
    def from_mapping(cls, value: Mapping[str, JsonValue]) -> EtfRotationConfig:
        """从策略参数解析并校验 ETF 五模块配置。

        入参：严格 YAML 中 ``strategy.parameters`` 的映射。
        返回值：规范化 ETF 轮动配置。
        异常：未知字段、组件冲突或 Alpha 参数非法时抛出错误。
        """
        return cls(StrategyPipelineConfig.from_parameters(value))

    @property
    def etf_pool(self) -> tuple[InstrumentId, ...]:
        """返回规范化、去重并稳定排序的固定 ETF 池。

        入参：无。
        返回值：证券标识元组。
        异常：池为空、重复或字段类型非法时抛出类型或值错误。
        """
        raw = self.pipeline.alpha.params.get("etf_pool")
        if (
            not isinstance(raw, list)
            or not raw
            or any(not isinstance(item, str) for item in raw)
        ):
            raise TypeError("etf_pool must be a nonempty string list")
        names = cast(list[str], raw)
        parsed = tuple(
            sorted(
                (InstrumentId.parse(item) for item in names),
                key=InstrumentId.canonical,
            )
        )
        if len(set(parsed)) != len(parsed):
            raise ValueError("etf_pool must not contain duplicates")
        return parsed

    @property
    def momentum_windows(self) -> tuple[int, ...]:
        """返回复合 Alpha 的稳定动量窗口。

        入参：无。
        返回值：严格递增的正整数窗口元组。
        异常：字段不是整数列表、重复或乱序时抛出值错误。
        """
        raw = self.pipeline.alpha.params.get("momentum_windows", [20, 60, 120])
        return self._windows(raw, "momentum_windows")

    @property
    def momentum_weights(self) -> tuple[float, ...]:
        """返回与动量窗口逐一对应的有限权重。

        入参：无。
        返回值：浮点权重元组。
        异常：字段不是数值列表或包含非有限值时抛出错误。
        """
        raw = self.pipeline.alpha.params.get("momentum_weights", [1.0, 1.0, 1.0])
        if not isinstance(raw, list) or not raw:
            raise TypeError("momentum_weights must be a nonempty numeric list")
        return tuple(self._number(item, "momentum weight") for item in raw)

    @property
    def lookback(self) -> int:
        """返回计算所需的最长连续有效价格窗口。

        入参：无。
        返回值：至少为二的交易日窗口。
        异常：Alpha 参数中的窗口非法时抛出值错误。
        """
        params = self.pipeline.alpha.params
        if self.pipeline.alpha.model_id == "single_factor":
            return self._positive_integer(params.get("lookback", 60), "lookback")
        trend = self._positive_integer(params.get("trend_window", 60), "trend_window")
        volatility = self._positive_integer(
            params.get("volatility_window", 20), "volatility_window"
        )
        return max(*self.momentum_windows, trend, volatility + 1)

    @staticmethod
    def _windows(value: JsonValue, field: str) -> tuple[int, ...]:
        if (
            not isinstance(value, list)
            or not value
            or any(type(item) is not int for item in value)
        ):
            raise TypeError(f"{field} must be a nonempty integer list")
        result = tuple(cast(list[int], value))
        if tuple(sorted(set(result))) != result or result[0] < 2:
            raise ValueError(f"{field} must be unique, ascending, and at least two")
        return result

    @staticmethod
    def _positive_integer(value: JsonValue, field: str) -> int:
        if type(value) is not int or value < 2:
            raise ValueError(f"{field} must be an integer of at least two")
        return value

    @staticmethod
    def _number(value: JsonValue, field: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{field} must be numeric")
        result = float(value)
        if not isfinite(result):
            raise ValueError(f"{field} must be finite")
        return result


class EtfRotationStrategy(WeightTargetStrategy):
    """在月边界运行 ETF Alpha→Risk→Cost→Construction→Constraint 流水线。

    入参：ETF 轮动配置和目标权重转订单规划器。
    返回值：月边界整数订单及动量、趋势、波动率评分审计。
    异常：PIT 数据不足或组合约束失败时传播对应错误。
    """

    def __init__(
        self,
        config: EtfRotationConfig,
        planner: RebalancePlanner,
        *,
        commission_bps: float,
        commission_minimum_fen: int,
    ) -> None:
        super().__init__(planner, target_tolerance=config.pipeline.target_tolerance)
        self.config = config
        self._assembler = CrossSectionalPortfolioAssembler(
            config.pipeline,
            commission_bps=commission_bps,
            commission_minimum_fen=commission_minimum_fen,
        )
        self._signals: list[dict[str, object]] = []

    @property
    def spec(self) -> StrategySpec:
        """返回 ETF 策略依赖和完整五模块参数。

        入参：无。
        返回值：``etf_rotation`` 策略规格。
        异常：无。
        """
        return StrategySpec(
            "etf_rotation",
            self.config.pipeline.frequency,
            (DatasetKind.FUND_DAILY_BAR, DatasetKind.FUND_ADJUSTMENT_FACTOR),
            (),
            {"pipeline": self.config.pipeline.as_json()},
        )

    def target_weights(self, ctx: DecisionContext) -> TargetWeights | None:
        """在跨月边界计算固定池评分并构建目标组合。

        入参：绑定信号日、下一执行日和账户的决策上下文。
        返回值：新目标权重；非月边界或没有有效评分时为空。
        异常：PIT 行情或约束不满足时传播对应错误。
        """
        if (ctx.signal_date.year, ctx.signal_date.month) == (
            ctx.execute_date.year,
            ctx.execute_date.month,
        ):
            return None
        frame = (
            ctx.data.adjusted_bars(self.config.etf_pool, self.config.lookback)
            .collect()
            .sort("instrument_id", "trade_date")
        )
        price_column = (
            "adjusted_close" if "adjusted_close" in frame.columns else "close"
        )
        scored = tuple(
            self._score(instrument, frame, price_column)
            for instrument in self.config.etf_pool
        )
        self._record(ctx, scored)
        return self._assembler.construct(ctx, scored)

    def signal_frame(self) -> pl.DataFrame:
        """返回按信号日和 ETF 稳定排序的评分审计表。

        入参：无。
        返回值：包含 score、有效性和原因码的 Polars DataFrame。
        异常：无。
        """
        if not self._signals:
            return pl.DataFrame(
                schema={
                    "signal_date": pl.Date,
                    "instrument_id": pl.String,
                    "state": pl.String,
                    "score": pl.Float64,
                    "state_changed": pl.Boolean,
                    "invalid_reason": pl.String,
                }
            )
        return pl.DataFrame(self._signals).sort("signal_date", "instrument_id")

    def _score(
        self, instrument: InstrumentId, frame: pl.DataFrame, price_column: str
    ) -> ScoredInstrument:
        prices = cast(
            list[float],
            frame.filter(pl.col("instrument_id") == instrument.canonical())[
                price_column
            ]
            .drop_nulls()
            .to_list(),
        )
        if len(prices) < self.config.lookback or any(
            not isfinite(float(value)) or float(value) <= 0 for value in prices
        ):
            return ScoredInstrument(instrument, None, "INSUFFICIENT_CONTIGUOUS_WINDOW")
        params = self.config.pipeline.alpha.params
        if self.config.pipeline.alpha.model_id == "single_factor":
            lookback = cast(int, params.get("lookback", 60))
            direction = self.config._number(params.get("direction", 1.0), "direction")
            score = direction * (prices[-1] / prices[-lookback] - 1.0)
            return ScoredInstrument(instrument, score)
        score = 0.0
        for window, weight in zip(
            self.config.momentum_windows,
            self.config.momentum_weights,
            strict=True,
        ):
            score += weight * (prices[-1] / prices[-window] - 1.0)
        trend_window = cast(int, params.get("trend_window", 60))
        trend = prices[-1] / (sum(prices[-trend_window:]) / trend_window) - 1.0
        volatility_window = cast(int, params.get("volatility_window", 20))
        returns = [
            prices[index] / prices[index - 1] - 1.0
            for index in range(len(prices) - volatility_window, len(prices))
        ]
        mean_return = sum(returns) / len(returns)
        volatility = sqrt(
            sum((item - mean_return) ** 2 for item in returns)
            / max(1, len(returns) - 1)
        )
        score += (
            self.config._number(params.get("trend_weight", 1.0), "trend_weight") * trend
        )
        score -= (
            self.config._number(
                params.get("volatility_weight", 1.0), "volatility_weight"
            )
            * volatility
        )
        return ScoredInstrument(instrument, score)

    def _record(self, ctx: DecisionContext, scored: Sequence[ScoredInstrument]) -> None:
        for item in scored:
            self._signals.append(
                {
                    "signal_date": ctx.signal_date,
                    "instrument_id": item.instrument_id.canonical(),
                    "state": "ALLOCATION" if item.score is not None else "INVALID",
                    "score": item.score,
                    "state_changed": False,
                    "invalid_reason": item.invalid_reason,
                }
            )


__all__ = ["EtfRotationConfig", "EtfRotationStrategy"]
