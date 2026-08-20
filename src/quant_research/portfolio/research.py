"""实现类型化信号到目标组合的三个参考构建器。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from math import isfinite
from typing import cast

import numpy as np
from numpy.typing import NDArray

from quant_research.costs.models import PreTradeCostSlice
from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio.constructor import TargetPortfolio, TargetPosition
from quant_research.risk.models import RiskSlice
from quant_research.signals.models import (
    AllocationSignalRow,
    CrossSectionalScoreRow,
    Direction,
    DirectionalSignalRow,
)


@dataclass(frozen=True, slots=True)
class ObjectiveBreakdown:
    """披露优化目标中的 Alpha、风险和事前成本项。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    alpha: float
    risk_penalty: float
    cost_penalty: float
    objective: float


@dataclass(frozen=True, slots=True)
class ConstructedPortfolio:
    """返回目标组合及其可审计目标函数分解。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    target: TargetPortfolio
    objective: ObjectiveBreakdown


class AlphaRiskCostOptimizer:
    """确定性求解多头 Alpha-Risk-Cost 组合。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(
        self,
        *,
        min_positions: int,
        max_positions: int,
        max_position_weight: float,
        max_turnover: float,
        risk_aversion: float,
        cost_aversion: float,
        iterations: int = 200,
        learning_rate: float = 0.05,
    ) -> None:
        if not 0 < min_positions <= max_positions:
            raise ValueError("position limits are invalid")
        if not 0.0 < max_position_weight <= 1.0 or not 0.0 <= max_turnover <= 1.0:
            raise ValueError("weight or turnover limits are invalid")
        if any(not isfinite(value) or value < 0.0 for value in (risk_aversion, cost_aversion)):
            raise ValueError("optimizer penalties must be finite and non-negative")
        if iterations <= 0 or learning_rate <= 0.0:
            raise ValueError("optimizer iteration controls are invalid")
        self._min = min_positions
        self._max = max_positions
        self._max_weight = max_position_weight
        self._max_turnover = max_turnover
        self._risk = risk_aversion
        self._cost = cost_aversion
        self._iterations = iterations
        self._learning_rate = learning_rate

    def construct(
        self,
        *,
        signal_date: date,
        execute_date: date,
        signals: tuple[CrossSectionalScoreRow, ...],
        risk: RiskSlice,
        costs: PreTradeCostSlice,
        current_weights: dict[str, float],
    ) -> ConstructedPortfolio:
        """稳定预选证券后，在上限单纯形和换手预算内优化权重。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if execute_date <= signal_date:
            raise ValueError("execute_date must follow signal_date")
        candidates = [item for item in signals if item.is_valid and item.score is not None]
        candidates.sort(
            key=lambda item: (-cast(float, item.score), item.instrument_id)
        )
        candidates = candidates[: self._max]
        if len(candidates) < self._min:
            raise ValueError("insufficient valid signals for min_positions")
        instruments = tuple(item.instrument_id for item in candidates)
        scores: NDArray[np.float64] = np.asarray(
            [cast(float, item.score) for item in candidates], dtype=np.float64
        )
        scores = scores - scores.mean()
        risk_index = {instrument: index for index, instrument in enumerate(risk.instruments)}
        if any(item not in risk_index for item in instruments):
            raise ValueError("risk covariance does not cover selected signals")
        indices = [risk_index[item] for item in instruments]
        covariance: NDArray[np.float64] = np.asarray(
            risk.covariance, dtype=np.float64
        )[np.ix_(indices, indices)]
        current: NDArray[np.float64] = np.asarray(
            [max(0.0, current_weights.get(item, 0.0)) for item in instruments],
            dtype=np.float64,
        )
        weights = self._project(np.maximum(scores, 0.0) + 1e-6, instruments, costs)
        for _ in range(self._iterations):
            gradient = scores - 2.0 * self._risk * covariance.dot(weights)
            gradient -= self._cost * np.sign(weights - current)
            next_weights = self._project(weights + self._learning_rate * gradient, instruments, costs)
            if np.max(np.abs(next_weights - weights)) < 1e-10:
                weights = next_weights
                break
            weights = next_weights
        turnover = float(np.abs(weights - current).sum() / 2.0)
        if turnover > self._max_turnover and turnover > 0.0:
            fraction = self._max_turnover / turnover
            weights = current + fraction * (weights - current)
            weights = self._project(weights, instruments, costs)
        weights = self._project(np.maximum(weights, 1e-8), instruments, costs)
        alpha = float(scores.dot(weights))
        risk_penalty = float(self._risk * weights.dot(covariance).dot(weights))
        cost_penalty = float(
            self._cost
            * sum(costs.estimate(instrument, float(weight - current[index])) for index, (instrument, weight) in enumerate(zip(instruments, weights, strict=True)))
        )
        positions = tuple(
            TargetPosition(InstrumentId.parse(instrument), float(weight), cast(float, candidates[index].score), "ALPHA_RISK_COST_SELECTED")
            for index, (instrument, weight) in enumerate(zip(instruments, weights, strict=True))
            if weight > 1e-12
        )
        cash = max(0.0, 1.0 - sum(item.target_weight for item in positions))
        return ConstructedPortfolio(
            TargetPortfolio(signal_date, execute_date, positions, cash),
            ObjectiveBreakdown(alpha, risk_penalty, cost_penalty, alpha - risk_penalty - cost_penalty),
        )

    def _project(
        self,
        values: NDArray[np.float64],
        instruments: tuple[str, ...],
        costs: PreTradeCostSlice,
    ) -> NDArray[np.float64]:
        upper: NDArray[np.float64] = np.asarray(
            [
                min(
                    self._max_weight,
                    next((item.capacity_limit for item in costs.estimates if item.instrument_id == instrument), self._max_weight),
                )
                for instrument in instruments
            ],
            dtype=np.float64,
        )
        projected = np.clip(values, 0.0, upper)
        for _ in range(100):
            total = float(projected.sum())
            if total <= 1.0 + 1e-12:
                break
            active = projected > 0.0
            projected[active] -= (total - 1.0) / int(active.sum())
            projected = np.clip(projected, 0.0, upper)
        return np.asarray(projected, dtype=np.float64)


class DirectionalExposureMapper:
    """把 LONG/FLAT 方向信号显式映射为风险资产和现金权重。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(self, *, long_weight: float, flat_weight: float = 0.0) -> None:
        if not 0.0 <= flat_weight <= long_weight <= 1.0:
            raise ValueError("directional weights are invalid")
        self._long = long_weight
        self._flat = flat_weight

    def construct(self, signal: DirectionalSignalRow, execute_date: date) -> TargetPortfolio:
        """拒绝 SHORT，并返回单资产目标组合。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if not signal.is_valid or signal.direction is None:
            raise ValueError("directional signal is invalid")
        if signal.direction is Direction.SHORT:
            raise ValueError("SHORT is incompatible with long-only execution")
        weight = self._long if signal.direction is Direction.LONG else self._flat
        positions = () if weight == 0.0 else (TargetPosition(InstrumentId.parse(signal.instrument_id), weight, signal.strength, "DIRECTIONAL_EXPOSURE"),)
        return TargetPortfolio(
            signal.signal_date, execute_date, positions, round(1.0 - weight, 12)
        )


class AllocationProjector:
    """归一化 Allocation 信号并应用单证券权重上限。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(self, *, max_position_weight: float = 1.0) -> None:
        if not 0.0 < max_position_weight <= 1.0:
            raise ValueError("max_position_weight must be in (0, 1]")
        self._max_weight = max_position_weight

    def construct(self, signals: tuple[AllocationSignalRow, ...], execute_date: date) -> TargetPortfolio:
        """把非负目标暴露投影到多头权重单纯形。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        valid = [item for item in signals if item.is_valid and item.desired_exposure is not None and item.desired_exposure > 0.0]
        if not signals:
            raise ValueError("allocation signals must not be empty")
        signal_date = signals[0].signal_date
        if any(item.signal_date != signal_date for item in signals):
            raise ValueError("allocation signal slice must use one decision date")
        exposures = [(item, cast(float, item.desired_exposure)) for item in valid]
        total = float(sum(value for _, value in exposures))
        raw = [
            (item, min(self._max_weight, value / total))
            for item, value in exposures
        ] if total > 0.0 else []
        positions = tuple(TargetPosition(InstrumentId.parse(item.instrument_id), weight, cast(float, item.desired_exposure), "ALLOCATION_PROJECTED") for item, weight in sorted(raw, key=lambda pair: pair[0].instrument_id))
        cash = round(
            max(0.0, 1.0 - sum(item.target_weight for item in positions)), 12
        )
        return TargetPortfolio(signal_date, execute_date, positions, cash)
