"""实现由五模块流水线装配的股票截面策略预设。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from math import isfinite
from typing import cast

import polars as pl

from quant_research.data.contracts import JsonValue
from quant_research.domain.enums import DatasetKind
from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.transforms import winsorize_mad, zscore
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
class MultifactorConfig:
    """定义股票多因子策略的完整五模块流水线。

    入参：通过组件目录校验的不可变流水线配置。
    返回值：股票多因子预设配置。
    异常：频率或 Alpha 参数不适用于股票截面时抛出值错误。
    """

    pipeline: StrategyPipelineConfig

    def __post_init__(self) -> None:
        if self.pipeline.frequency != "WEEKLY":
            raise ValueError("stock_multifactor frequency must be WEEKLY")
        params = self.pipeline.alpha.params
        if self.pipeline.alpha.model_id == "single_factor":
            unknown = set(params) - {"factor_id", "direction"}
            factor_id = params.get("factor_id")
            direction = params.get("direction", 1.0)
            if unknown:
                raise ValueError(f"unknown single_factor parameter: {min(unknown)}")
            if not isinstance(factor_id, str) or not factor_id:
                raise TypeError("single_factor factor_id must be a nonempty string")
            self._finite(direction, "single_factor direction")
            return
        unknown = set(params) - {"factor_weights", "min_valid_factors"}
        if unknown:
            raise ValueError(
                f"unknown multi_factor_composite parameter: {min(unknown)}"
            )
        weights = params.get("factor_weights")
        if not isinstance(weights, Mapping) or not weights:
            raise TypeError("factor_weights must be a nonempty mapping")
        for factor_id, weight in weights.items():
            if not isinstance(factor_id, str) or not factor_id:
                raise TypeError("factor IDs must be nonempty strings")
            self._finite(weight, f"factor weight {factor_id}")
        minimum = params.get("min_valid_factors", len(weights))
        if type(minimum) is not int or not 1 <= minimum <= len(weights):
            raise ValueError("min_valid_factors must be within factor count")

    @classmethod
    def from_mapping(cls, value: Mapping[str, JsonValue]) -> MultifactorConfig:
        """从策略参数解析并校验股票五模块配置。

        入参：严格 YAML 中 ``strategy.parameters`` 的映射。
        返回值：规范化多因子配置。
        异常：未知字段、组件冲突或 Alpha 参数非法时抛出错误。
        """
        return cls(StrategyPipelineConfig.from_parameters(value))

    @property
    def factor_weights(self) -> dict[str, float]:
        """返回 Alpha 模块展开后的因子方向和权重。

        入参：无。
        返回值：按因子 ID 排序的浮点权重映射。
        异常：配置构造已完成校验，本属性不抛出主动异常。
        """
        params = self.pipeline.alpha.params
        if self.pipeline.alpha.model_id == "single_factor":
            return {
                cast(str, params["factor_id"]): self._finite(
                    params.get("direction", 1.0), "single_factor direction"
                )
            }
        weights = cast(Mapping[str, JsonValue], params["factor_weights"])
        return {
            key: self._finite(weights[key], f"factor weight {key}")
            for key in sorted(weights)
        }

    @property
    def min_valid_factors(self) -> int:
        """返回复合 Alpha 要求的最少有效因子数。

        入参：无。
        返回值：正整数有效因子门槛；单因子模型固定为一。
        异常：无。
        """
        if self.pipeline.alpha.model_id == "single_factor":
            return 1
        return cast(
            int,
            self.pipeline.alpha.params.get(
                "min_valid_factors", len(self.factor_weights)
            ),
        )

    @staticmethod
    def _finite(value: object, field: str) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{field} must be numeric")
        number = float(value)
        if not isfinite(number):
            raise ValueError(f"{field} must be finite")
        return number


class MultifactorStrategy(WeightTargetStrategy):
    """按周运行 Alpha→Risk→Cost→Construction→Constraint 流水线。

    入参：股票多因子配置和目标权重转订单规划器。
    返回值：周边界整数订单及可审计横截面信号。
    异常：PIT 数据、因子变换或组合约束失败时传播对应错误。
    """

    def __init__(
        self,
        config: MultifactorConfig,
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
        """返回股票策略依赖和完整五模块参数。

        入参：无。
        返回值：``stock_multifactor`` 策略规格。
        异常：无。
        """
        return StrategySpec(
            "stock_multifactor",
            self.config.pipeline.frequency,
            (
                DatasetKind.DAILY_BAR,
                DatasetKind.SECURITY_STATUS,
                DatasetKind.DAILY_BASIC,
                DatasetKind.INDUSTRY_CLASSIFICATION,
            ),
            tuple(self.config.factor_weights),
            {"pipeline": self.config.pipeline.as_json()},
        )

    def target_weights(self, ctx: DecisionContext) -> TargetWeights | None:
        """仅在周边界计算完整截面评分并构建目标组合。

        入参：绑定信号日、下一执行日和账户的决策上下文。
        返回值：新目标权重；非周边界或没有有效候选时为空。
        异常：PIT 数据读取、因子变换或组合约束失败时传播对应错误。
        """
        if ctx.signal_date.isocalendar()[:2] == ctx.execute_date.isocalendar()[:2]:
            return None
        universe = ctx.data.stock_universe().collect().sort("instrument_id")
        if "eligible" in universe.columns:
            universe = universe.filter(pl.col("eligible"))
        identifiers = tuple(
            InstrumentId.parse(value) for value in universe["instrument_id"].to_list()
        )
        if not identifiers:
            return None
        scored = self._scores(ctx, identifiers)
        self._record(ctx, scored)
        return self._assembler.construct(ctx, scored)

    def signal_frame(self) -> pl.DataFrame:
        """返回按信号日和证券稳定排序的评分审计表。

        入参：无。
        返回值：包含 score、有效性和原因码的 Polars DataFrame。
        异常：无。
        """
        schema = {
            "signal_date": pl.Date,
            "instrument_id": pl.String,
            "state": pl.String,
            "score": pl.Float64,
            "state_changed": pl.Boolean,
            "invalid_reason": pl.String,
        }
        if not self._signals:
            return pl.DataFrame(schema=schema)
        return pl.DataFrame(self._signals, schema=schema, strict=False).sort(
            "signal_date", "instrument_id"
        )

    def _scores(
        self, ctx: DecisionContext, identifiers: tuple[InstrumentId, ...]
    ) -> tuple[ScoredInstrument, ...]:
        weights = self.config.factor_weights
        raw = ctx.data.factor_values(tuple(weights), identifiers).collect()
        factor_column = "factor_id" if "factor_id" in raw.columns else "factor_ref"
        reason_column = (
            "invalid_reason" if "invalid_reason" in raw.columns else "reason_code"
        )
        frame = raw.select(
            "trade_date",
            "instrument_id",
            pl.col(factor_column).alias("factor_id"),
            pl.col("value").cast(pl.Float64),
            pl.col("is_valid").cast(pl.Boolean),
            (
                pl.col(reason_column).cast(pl.String)
                if reason_column in raw.columns
                else pl.lit(None, dtype=pl.String)
            ).alias("invalid_reason"),
        )
        transformed = zscore(
            winsorize_mad(frame, "value", ["trade_date", "factor_id"]),
            "value",
            ["trade_date", "factor_id"],
        ).with_columns(
            pl.col("factor_id")
            .replace(weights, default=0.0)
            .cast(pl.Float64)
            .alias("_weight")
        )
        aggregated = (
            transformed.group_by("instrument_id")
            .agg(
                (pl.col("value") * pl.col("_weight"))
                .filter(pl.col("is_valid"))
                .sum()
                .alias("score"),
                pl.col("is_valid").sum().alias("valid_count"),
            )
            .sort("instrument_id")
        )
        values = {str(row["instrument_id"]): row for row in aggregated.to_dicts()}
        result: list[ScoredInstrument] = []
        for identifier in identifiers:
            row = values.get(identifier.canonical())
            if (
                row is None
                or int(row["valid_count"] or 0) < self.config.min_valid_factors
            ):
                result.append(
                    ScoredInstrument(identifier, None, "INSUFFICIENT_VALID_FACTORS")
                )
            else:
                result.append(ScoredInstrument(identifier, float(row["score"])))
        return tuple(result)

    def _record(
        self, ctx: DecisionContext, scored: tuple[ScoredInstrument, ...]
    ) -> None:
        for item in scored:
            self._signals.append(
                {
                    "signal_date": ctx.signal_date,
                    "instrument_id": item.instrument_id.canonical(),
                    "state": "SCORE" if item.score is not None else "INVALID",
                    "score": item.score,
                    "state_changed": False,
                    "invalid_reason": item.invalid_reason,
                }
            )


__all__ = ["MultifactorConfig", "MultifactorStrategy"]
