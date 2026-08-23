"""实现截面五模块流水线共享的组合构建与审计逻辑。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, time
from math import isfinite
from typing import cast

import polars as pl

from quant_research.costs.models import CostEstimate, PreTradeCostSlice
from quant_research.data.contracts import JsonValue
from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio.research import AlphaRiskCostOptimizer
from quant_research.risk.models import StatisticalRiskEstimator
from quant_research.signals.models import CrossSectionalScoreRow
from quant_research.strategies.base import DecisionContext, TargetWeights
from quant_research.strategies.components import StrategyPipelineConfig


@dataclass(frozen=True, slots=True)
class ScoredInstrument:
    """保存单个截面候选的分数和无效原因。

    入参：证券标识、可选分数和稳定无效原因码。
    返回值：不可变候选记录。
    异常：有效性与分数不一致或分数非有限时抛出值错误。
    """

    instrument_id: InstrumentId
    score: float | None
    invalid_reason: str | None = None

    def __post_init__(self) -> None:
        if self.score is None and not self.invalid_reason:
            raise ValueError("invalid score requires an invalid_reason")
        if self.score is not None and (
            not isfinite(self.score) or self.invalid_reason is not None
        ):
            raise ValueError("valid score must be finite and have no invalid_reason")


class CrossSectionalPortfolioAssembler:
    """消费 Risk、Cost、Construction 与 Constraint 配置生成目标权重。

    入参：经过组件目录校验的流水线配置。
    返回值：Top-N 等权或 Alpha-Risk-Cost 优化目标及审计分解。
    异常：数据不足、约束不可行或组件参数非法时抛出领域错误。
    """

    def __init__(
        self,
        pipeline: StrategyPipelineConfig,
        *,
        commission_bps: float,
        commission_minimum_fen: int,
    ) -> None:
        if not isfinite(commission_bps) or commission_bps < 0:
            raise ValueError("commission_bps must be finite and nonnegative")
        if type(commission_minimum_fen) is not int or commission_minimum_fen < 0:
            raise ValueError("commission_minimum_fen must be nonnegative")
        self._pipeline = pipeline
        self._constraints = pipeline.constraint_set
        self._commission_bps = commission_bps
        self._commission_minimum_fen = commission_minimum_fen
        self._last_objective: dict[str, float] | None = None

    @property
    def objective(self) -> Mapping[str, float] | None:
        """返回最近决策日的优化目标分解。

        入参：无。
        返回值：Alpha、风险、成本和总目标的只读副本；尚未优化时为空。
        异常：无。
        """
        return None if self._last_objective is None else dict(self._last_objective)

    def construct(
        self,
        ctx: DecisionContext,
        scores: tuple[ScoredInstrument, ...],
    ) -> TargetWeights | None:
        """按配置构建并二次验证多头目标。

        入参：绑定单一信号日的上下文和按证券排序的完整评分切片。
        返回值：满足约束的目标权重；无有效候选时返回 ``None``。
        异常：风险、成本或组合约束不满足时抛出值错误。
        """
        valid = tuple(item for item in scores if item.score is not None)
        if not valid:
            return None
        identifiers = tuple(item.instrument_id for item in valid)
        liquidity = self._liquidity(ctx, identifiers)
        eligible = tuple(
            item
            for item in valid
            if liquidity.get(item.instrument_id, 0.0)
            >= self._constraints.min_adv_amount
        )
        if len(eligible) < self._constraints.min_positions:
            raise ValueError("min_positions constraint is not satisfied")
        if self._pipeline.construction.model_id == "top_n_equal_weight":
            weights = self._top_n(ctx, eligible, liquidity)
            self._last_objective = None
        else:
            weights = self._mean_variance(ctx, eligible, liquidity)
        weights = self._apply_industry_cap(ctx, weights)
        weights = self._apply_turnover(ctx, weights)
        self._validate(weights)
        return TargetWeights(ctx.signal_date, ctx.execute_date, weights)

    def _top_n(
        self,
        ctx: DecisionContext,
        scores: tuple[ScoredInstrument, ...],
        liquidity: Mapping[InstrumentId, float],
    ) -> dict[InstrumentId, float]:
        del ctx, liquidity
        raw_top_n = self._pipeline.construction.params.get(
            "top_n", self._constraints.max_positions
        )
        if type(raw_top_n) is not int or raw_top_n <= 0:
            raise ValueError("top_n must be a positive integer")
        count = min(raw_top_n, self._constraints.max_positions)
        selected = sorted(
            scores,
            key=lambda item: (-cast(float, item.score), item.instrument_id.canonical()),
        )[:count]
        if len(selected) < self._constraints.min_positions:
            raise ValueError("min_positions constraint is not satisfied")
        base = min(
            self._constraints.max_position_weight,
            self._constraints.long_exposure / len(selected),
        )
        return {item.instrument_id: base for item in selected}

    def _mean_variance(
        self,
        ctx: DecisionContext,
        scores: tuple[ScoredInstrument, ...],
        liquidity: Mapping[InstrumentId, float],
    ) -> dict[InstrumentId, float]:
        instruments = tuple(item.instrument_id for item in scores)
        risk_params = self._pipeline.risk.params
        raw_lookback = risk_params.get("lookback", 60)
        if type(raw_lookback) is not int or raw_lookback < 2:
            raise ValueError("risk lookback must be an integer of at least two")
        shrinkage = 0.0
        if self._pipeline.risk.model_id == "shrinkage":
            raw_shrinkage = risk_params.get("shrinkage", 0.2)
            shrinkage = self._number(raw_shrinkage, "shrinkage", minimum=0.0)
            if shrinkage > 1.0:
                raise ValueError("shrinkage must be at most one")
        returns = ctx.data.log_returns(instruments, raw_lookback).collect()
        return_column = (
            "forward_log_return"
            if "forward_log_return" in returns.columns
            else "log_return"
        )
        observations = returns.select(
            "trade_date",
            "instrument_id",
            pl.col(return_column).alias("log_return"),
        ).with_columns(
            pl.col("instrument_id")
            .replace(
                {item.canonical(): amount for item, amount in liquidity.items()},
                default=0.0,
            )
            .cast(pl.Float64)
            .alias("amount")
        )
        artifact = StatisticalRiskEstimator(
            lookback=raw_lookback, shrinkage=shrinkage
        ).estimate(observations, (ctx.signal_date,))
        if not artifact.slices:
            raise ValueError("PIPELINE_MODEL_UNAVAILABLE: risk observations are empty")
        cost = self._cost_slice(ctx, liquidity)
        params = self._pipeline.construction.params
        optimizer = AlphaRiskCostOptimizer(
            min_positions=self._constraints.min_positions,
            max_positions=self._constraints.max_positions,
            max_position_weight=self._constraints.max_position_weight,
            max_turnover=self._constraints.max_turnover,
            risk_aversion=self._number(
                params.get("risk_aversion", 1.0), "risk_aversion", minimum=0.0
            ),
            cost_aversion=self._number(
                params.get("cost_aversion", 1.0), "cost_aversion", minimum=0.0
            ),
            iterations=self._integer(params.get("iterations", 200), "iterations"),
            learning_rate=self._number(
                params.get("learning_rate", 0.05),
                "learning_rate",
                minimum=0.0,
                exclusive=True,
            ),
        )
        now = datetime.combine(ctx.signal_date, time.max, tzinfo=UTC)
        signal_rows = tuple(
            CrossSectionalScoreRow(
                ctx.signal_date,
                item.instrument_id.canonical(),
                "cross_sectional_alpha",
                item.score,
                1.0,
                now,
                True,
                None,
            )
            for item in scores
        )
        current = self._current_weights(ctx, instruments)
        result = optimizer.construct(
            signal_date=ctx.signal_date,
            execute_date=ctx.execute_date,
            signals=signal_rows,
            risk=artifact.slices[0],
            costs=cost,
            current_weights={key.canonical(): value for key, value in current.items()},
        )
        self._last_objective = {
            "alpha": result.objective.alpha,
            "risk_penalty": result.objective.risk_penalty,
            "cost_penalty": result.objective.cost_penalty,
            "objective": result.objective.objective,
        }
        scale = min(
            1.0,
            self._constraints.long_exposure
            / max(
                sum(item.target_weight for item in result.target.positions),
                self._constraints.long_exposure,
            ),
        )
        return {
            item.instrument_id: item.target_weight * scale
            for item in result.target.positions
        }

    def _cost_slice(
        self, ctx: DecisionContext, liquidity: Mapping[InstrumentId, float]
    ) -> PreTradeCostSlice:
        params = self._pipeline.cost.params
        impact = self._number(params.get("impact_bps", 0.0), "impact_bps", minimum=0.0)
        participation = self._number(
            params.get("max_participation", 0.1),
            "max_participation",
            minimum=0.0,
            exclusive=True,
        )
        if participation > 1.0:
            raise ValueError("max_participation must be at most one")
        portfolio_value = ctx.account.equity_fen / 100.0
        minimum_ratio = (
            self._commission_minimum_fen / ctx.account.equity_fen
            if ctx.account.equity_fen > 0
            else 0.0
        )
        estimates: list[CostEstimate] = []
        for instrument, amount in sorted(
            liquidity.items(), key=lambda item: item[0].canonical()
        ):
            capacity = (
                max(0.0, amount * participation / portfolio_value)
                if portfolio_value > 0
                else 0.0
            )
            if self._pipeline.cost.model_id == "sqrt_impact":
                linear, square_root = (
                    self._commission_bps / 10_000.0,
                    impact / 10_000.0,
                )
            elif self._pipeline.cost.model_id == "linear_impact":
                linear, square_root = (
                    (self._commission_bps + impact) / 10_000.0,
                    0.0,
                )
            else:
                linear, square_root = self._commission_bps / 10_000.0, 0.0
            estimates.append(
                CostEstimate(
                    instrument.canonical(),
                    minimum_ratio,
                    linear,
                    square_root,
                    capacity,
                )
            )
        return PreTradeCostSlice(ctx.signal_date, tuple(estimates))

    def _liquidity(
        self, ctx: DecisionContext, instruments: tuple[InstrumentId, ...]
    ) -> dict[InstrumentId, float]:
        bars = ctx.data.bars(instruments, 20).collect()
        if "amount" not in bars.columns:
            return {item: 0.0 for item in instruments}
        rows = bars.group_by("instrument_id").agg(pl.col("amount").mean()).to_dicts()
        return {
            InstrumentId.parse(str(row["instrument_id"])): float(row["amount"] or 0.0)
            for row in rows
        }

    def _current_weights(
        self, ctx: DecisionContext, instruments: tuple[InstrumentId, ...]
    ) -> dict[InstrumentId, float]:
        if ctx.account.equity_fen <= 0:
            return {}
        frame = (
            ctx.data.bars(instruments, 1).collect().sort("trade_date", "instrument_id")
        )
        latest = frame.group_by("instrument_id").agg(pl.col("close").last())
        prices = {
            InstrumentId.parse(identifier): float(close)
            for identifier, close in latest.select("instrument_id", "close").iter_rows()
            if close is not None
        }
        return {
            instrument: quantity * prices[instrument] * 100 / ctx.account.equity_fen
            for instrument, quantity in ctx.account.positions.items()
            if quantity > 0 and instrument in prices
        }

    def _apply_turnover(
        self,
        ctx: DecisionContext,
        target: Mapping[InstrumentId, float],
    ) -> dict[InstrumentId, float]:
        instruments = tuple(
            sorted(set(target) | set(ctx.account.positions), key=InstrumentId.canonical)
        )
        current = self._current_weights(ctx, instruments)
        if not current:
            return dict(target)
        keys = set(current) | set(target)
        turnover = (
            sum(abs(target.get(key, 0.0) - current.get(key, 0.0)) for key in keys) / 2.0
        )
        if turnover <= self._constraints.max_turnover or turnover == 0:
            return dict(target)
        return self._turnover_limited_target(current, target)

    def _turnover_limited_target(
        self,
        current: Mapping[InstrumentId, float],
        target: Mapping[InstrumentId, float],
    ) -> dict[InstrumentId, float]:
        """在持仓数上限内按目标顺序执行换手预算允许的离散替换。"""
        epsilon = 1e-12
        active_current = {
            key: value for key, value in current.items() if value > epsilon
        }
        if len(active_current) > self._constraints.max_positions:
            raise ValueError(
                "max_turnover and max_positions constraints are jointly infeasible"
            )
        active_target = {
            key: value for key, value in target.items() if value > epsilon
        }
        target_order = tuple(
            key for key, value in target.items() if value > epsilon
        )
        result = {
            key: min(value, self._constraints.max_position_weight)
            for key, value in active_current.items()
        }
        if self._turnover(current, result) > self._constraints.max_turnover + epsilon:
            raise ValueError(
                "max_turnover and max_position_weight constraints are jointly infeasible"
            )
        support = set(result)
        exits = sorted(
            support - set(active_target),
            key=lambda key: (active_current[key], key.canonical()),
        )
        for entrant in target_order:
            if entrant in support:
                continue
            exit_key = exits[0] if len(support) >= self._constraints.max_positions else None
            if exit_key is None and len(support) >= self._constraints.max_positions:
                break
            trial = dict(result)
            if exit_key is not None:
                trial.pop(exit_key)
            trial[entrant] = active_target[entrant]
            if self._turnover(current, trial) > self._constraints.max_turnover + epsilon:
                continue
            result = trial
            support = set(result)
            if exit_key is not None:
                exits.pop(0)

        desired = {
            key: active_target.get(key, result[key])
            for key in sorted(result, key=InstrumentId.canonical)
        }
        if self._turnover(current, desired) <= self._constraints.max_turnover + epsilon:
            return desired
        return self._interpolate_supported_target(current, result, desired)

    def _interpolate_supported_target(
        self,
        current: Mapping[InstrumentId, float],
        start: Mapping[InstrumentId, float],
        desired: Mapping[InstrumentId, float],
    ) -> dict[InstrumentId, float]:
        """只在固定持仓集合内逼近目标，避免重新引入已退出证券。"""
        lower, upper = 0.0, 1.0
        keys = tuple(sorted(start, key=InstrumentId.canonical))
        for _ in range(60):
            fraction = (lower + upper) / 2.0
            candidate = {
                key: start[key] + fraction * (desired[key] - start[key])
                for key in keys
            }
            if self._turnover(current, candidate) <= self._constraints.max_turnover:
                lower = fraction
            else:
                upper = fraction
        return {
            key: max(0.0, start[key] + lower * (desired[key] - start[key]))
            for key in keys
        }

    @staticmethod
    def _turnover(
        current: Mapping[InstrumentId, float],
        target: Mapping[InstrumentId, float],
    ) -> float:
        """按策略既有单边口径计算两个资产权重集合的换手率。"""
        keys = set(current) | set(target)
        return sum(
            abs(target.get(key, 0.0) - current.get(key, 0.0)) for key in keys
        ) / 2.0

    def _apply_industry_cap(
        self, ctx: DecisionContext, target: Mapping[InstrumentId, float]
    ) -> dict[InstrumentId, float]:
        if self._constraints.max_industry_weight >= 1.0 or not target:
            return dict(target)
        industries = ctx.data.industry(tuple(target)).collect()
        industry_column = (
            "industry_code" if "industry_code" in industries.columns else "industry_id"
        )
        if industry_column not in industries.columns:
            raise ValueError("industry constraint requires PIT industry_code")
        mapping = {
            InstrumentId.parse(str(identifier)): str(industry)
            for identifier, industry in industries.select(
                "instrument_id", industry_column
            ).iter_rows()
            if industry is not None
        }
        totals: dict[str, float] = {}
        for instrument, weight in target.items():
            industry = mapping.get(instrument)
            if industry is None:
                continue
            totals[industry] = totals.get(industry, 0.0) + weight
        scales = {
            industry: min(1.0, self._constraints.max_industry_weight / total)
            for industry, total in totals.items()
            if total > 0
        }
        return {
            instrument: weight * scales.get(mapping.get(instrument, ""), 1.0)
            for instrument, weight in target.items()
        }

    def _validate(self, target: Mapping[InstrumentId, float]) -> None:
        active = [value for value in target.values() if value > 1e-12]
        if (
            not self._constraints.min_positions
            <= len(active)
            <= self._constraints.max_positions
        ):
            raise ValueError("position count constraint is violated")
        if any(
            not isfinite(value)
            or value < 0
            or value > self._constraints.max_position_weight + 1e-10
            for value in active
        ):
            raise ValueError("position weight constraint is violated")
        if sum(active) > self._constraints.long_exposure + 1e-10:
            raise ValueError("long exposure constraint is violated")

    @staticmethod
    def _number(
        value: JsonValue, field: str, *, minimum: float, exclusive: bool = False
    ) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"{field} must be numeric")
        result = float(value)
        if (
            not isfinite(result)
            or result < minimum
            or (exclusive and result == minimum)
        ):
            qualifier = "greater than" if exclusive else "at least"
            raise ValueError(f"{field} must be finite and {qualifier} {minimum}")
        return result

    @staticmethod
    def _integer(value: JsonValue, field: str) -> int:
        if type(value) is not int or value <= 0:
            raise ValueError(f"{field} must be a positive integer")
        return value


__all__ = ["CrossSectionalPortfolioAssembler", "ScoredInstrument"]
