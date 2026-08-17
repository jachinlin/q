"""提供策略与multifactor相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date
from math import isfinite
from types import MappingProxyType
from typing import cast

import polars as pl

from quant_research.backtest.engine import StrategyRef
from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import canonical_factor_ref, is_available_on_signal_day
from quant_research.factors.transforms import winsorize_mad, zscore
from quant_research.portfolio.constraints import PortfolioConstraints
from quant_research.portfolio.constructor import TargetPortfolio
from quant_research.strategies.base import (
    PortfolioState,
    RebalanceFrequency,
    StrategyContext,
    ValidationIssue,
    is_rebalance_boundary,
    validated_factor_values,
    validated_stock_universe,
)

_FACTOR_DEFINITIONS = {
    "earnings_yield_ttm": ("VALUE", 1),
    "book_to_price_mrq": ("VALUE", 1),
    "roe_pit": ("QUALITY", 1),
    "momentum_120_20": ("MOMENTUM", 1),
    "volatility_60d": ("RISK", -1),
    "downside_volatility_60d": ("RISK", -1),
    "max_drawdown_120d": ("RISK", -1),
}
_CATEGORY_WEIGHTS = {"VALUE": 0.25, "QUALITY": 0.25, "MOMENTUM": 0.30, "RISK": 0.20}
_EPSILON = 1e-10


@dataclass(frozen=True, slots=True)
class MultifactorDecision:
    """记录单只股票的多因子综合得分或被排除的证据。

    入参：
        instrument_id：目标证券标识，类型为 ``InstrumentId``。
        score：综合得分。
        reason_code：说明成交、拒绝或排除原因的稳定机器码。
        factor_reasons：参与本次处理的因子``reasons``；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Immutable per-instrument score or exclusion evidence.
    """

    instrument_id: InstrumentId
    score: float | None
    reason_code: str
    factor_reasons: Mapping[str, str]

    def __post_init__(self) -> None:
        if not isinstance(self.instrument_id, InstrumentId):
            raise TypeError("instrument_id must be an InstrumentId")
        if self.score is not None and (
            not isinstance(self.score, float) or not isfinite(self.score)
        ):
            raise ValueError("score must be finite or None")
        if not isinstance(self.reason_code, str) or not self.reason_code.strip():
            raise ValueError("reason_code must be a nonempty string")
        if not isinstance(self.factor_reasons, Mapping) or any(
            not isinstance(reference, str)
            or not isinstance(reason, str)
            or not reason.strip()
            for reference, reason in self.factor_reasons.items()
        ):
            raise ValueError("factor_reasons must map factor refs to nonempty reasons")
        object.__setattr__(
            self,
            "factor_reasons",
            MappingProxyType(dict(sorted(self.factor_reasons.items()))),
        )


class _MultifactorSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _default_factor_definitions() -> Mapping[str, tuple[str, int]]:
        return MappingProxyType(dict(_FACTOR_DEFINITIONS))

    @staticmethod
    def _default_category_weights() -> Mapping[str, float]:
        return MappingProxyType(dict(_CATEGORY_WEIGHTS))

    @staticmethod
    def _constraints_from_mapping(
        mapping: Mapping[str, object],
    ) -> PortfolioConstraints:
        names = {
            "max_position_weight",
            "min_positions",
            "max_positions",
            "min_adv_amount",
            "max_turnover",
        }
        unknown = set(mapping) - names
        if unknown:
            raise ValueError(f"unknown constraint key: {min(unknown)}")
        missing = names - set(mapping)
        if missing:
            raise ValueError(f"missing constraint key: {min(missing)}")
        return PortfolioConstraints(
            max_position_weight=cast(float, mapping["max_position_weight"]),
            min_positions=cast(int, mapping["min_positions"]),
            max_positions=cast(int, mapping["max_positions"]),
            min_adv_amount=cast(float, mapping["min_adv_amount"]),
            max_turnover=cast(float, mapping["max_turnover"]),
        )


@dataclass(frozen=True, slots=True)
class MultifactorConfig:
    """定义策略信号流程使用的不可变配置及取值约束。

    入参：
        constraints：组合约束。
        factor_definitions：参与本次处理的因子``definitions``；调用方不得依赖未声明的顺序。
        category_weights：参与本次处理的因子类别``weights``；调用方不得依赖未声明的顺序。
        min_valid_factors：判定输入或结果有效所需达到的下限``valid``因子集合。
        mad_multiplier：MAD倍数。
        frequency：调仓频率。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """

    constraints: PortfolioConstraints
    factor_definitions: Mapping[str, tuple[str, int]] = field(
        default_factory=_MultifactorSupport._default_factor_definitions
    )
    category_weights: Mapping[str, float] = field(
        default_factory=_MultifactorSupport._default_category_weights
    )
    min_valid_factors: int = 5
    mad_multiplier: float = 3.0
    frequency: RebalanceFrequency = RebalanceFrequency.WEEKLY

    def __post_init__(self) -> None:
        if not isinstance(self.constraints, PortfolioConstraints):
            raise TypeError("constraints must be PortfolioConstraints")
        if not isinstance(self.factor_definitions, Mapping):
            raise TypeError("factor_definitions must be a mapping")
        definitions: dict[str, tuple[str, int]] = {}
        for reference, definition in self.factor_definitions.items():
            factor_ref = canonical_factor_ref(reference)
            if not isinstance(definition, tuple) or len(definition) != 2:
                raise ValueError("factor definitions must be (category, direction)")
            category, direction = definition
            if (
                category not in _CATEGORY_WEIGHTS
                or type(direction) is not int
                or direction not in {-1, 1}
            ):
                raise ValueError(
                    "factor definitions have invalid category or direction"
                )
            definitions[factor_ref] = (category, direction)
        if definitions != _FACTOR_DEFINITIONS:
            raise ValueError("factor_definitions must be the fixed seven alpha refs")
        if not isinstance(self.category_weights, Mapping):
            raise TypeError("category_weights must be a mapping")
        weights = dict(self.category_weights)
        if set(weights) != set(_CATEGORY_WEIGHTS):
            raise ValueError("category_weights must include each alpha category")
        if any(
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not isfinite(value)
            or value < 0
            for value in weights.values()
        ):
            raise ValueError("category weights must be finite and nonnegative")
        if abs(sum(weights.values()) - 1.0) > _EPSILON:
            raise ValueError("category weights must sum to one")
        if type(self.min_valid_factors) is not int or self.min_valid_factors != 5:
            raise ValueError("min_valid_factors must be the value 5")
        if (
            not isinstance(self.mad_multiplier, (int, float))
            or isinstance(self.mad_multiplier, bool)
            or not isfinite(self.mad_multiplier)
            or self.mad_multiplier <= 0
        ):
            raise ValueError("mad_multiplier must be finite and positive")
        if self.frequency is not RebalanceFrequency.WEEKLY:
            raise ValueError("multifactor frequency must be WEEKLY")
        object.__setattr__(
            self,
            "factor_definitions",
            MappingProxyType(dict(sorted(definitions.items()))),
        )
        object.__setattr__(
            self, "category_weights", MappingProxyType(dict(sorted(weights.items())))
        )

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, object]) -> MultifactorConfig:
        """从输入解析配置映射。

        入参：
            mapping：参与本次处理的配置映射；调用方不得依赖未声明的顺序。
        返回值：
            返回配置映射（``MultifactorConfig``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        if not isinstance(mapping, Mapping):
            raise TypeError("multifactor config must be a mapping")
        allowed = {
            "constraints",
            "factor_definitions",
            "category_weights",
            "min_valid_factors",
            "mad_multiplier",
            "frequency",
        }
        unknown = set(mapping) - allowed
        if unknown:
            raise ValueError(f"unknown multifactor config key: {min(unknown)}")
        if "constraints" not in mapping:
            raise ValueError("missing multifactor config key: constraints")
        raw_constraints = mapping["constraints"]
        if not isinstance(raw_constraints, Mapping):
            raise TypeError("constraints must be a mapping")
        constraints = _MultifactorSupport._constraints_from_mapping(raw_constraints)
        raw_definitions = mapping.get("factor_definitions", _FACTOR_DEFINITIONS)
        if not isinstance(raw_definitions, Mapping):
            raise TypeError("factor_definitions must be a mapping")
        definitions: dict[str, tuple[str, int]] = {}
        for reference, value in raw_definitions.items():
            if not isinstance(reference, str):
                raise TypeError("factor definition reference must be a string")
            if not isinstance(value, Mapping):
                raise TypeError("factor definition must be a mapping")
            if set(value) != {"category", "direction"}:
                raise ValueError(
                    "factor definition keys must be category and direction"
                )
            category, direction = value["category"], value["direction"]
            if not isinstance(category, str):
                raise TypeError("factor definition category must be a string")
            if type(direction) is not int:
                raise TypeError("factor definition direction must be an integer")
            definitions[reference] = (category, direction)
        raw_weights = mapping.get("category_weights", _CATEGORY_WEIGHTS)
        if not isinstance(raw_weights, Mapping):
            raise TypeError("category_weights must be a mapping")
        return cls(
            constraints,
            definitions,
            cast(Mapping[str, float], raw_weights),
            cast(int, mapping.get("min_valid_factors", 5)),
            cast(float, mapping.get("mad_multiplier", 3.0)),
            RebalanceFrequency(cast(str, mapping.get("frequency", "WEEKLY"))),
        )


class MultifactorStrategy:
    """表示策略信号流程中的``multifactor``策略及其业务不变量。

    入参：
        config：调用所用的配置对象，类型为 ``MultifactorConfig``。
        audit_sink：由组合根注入、用于隔离外部副作用的审计事件``sink``端口。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    """

    strategy_id = "stock_multifactor"

    def __init__(
        self,
        config: MultifactorConfig,
        *,
        audit_sink: Callable[[tuple[MultifactorDecision, ...]], None] | None = None,
    ) -> None:
        if not isinstance(config, MultifactorConfig):
            raise TypeError("config must be a MultifactorConfig")
        if audit_sink is not None and not callable(audit_sink):
            raise TypeError("audit_sink must be callable or None")
        self.config = config
        self._audit_sink = audit_sink

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
        universe = validated_stock_universe(
            ctx.data.stock_universe(ctx.signal_date),
            signal_date=ctx.signal_date,
        ).filter(pl.col("eligible"))
        eligible_ids = tuple(
            InstrumentId.parse(value) for value in universe["instrument_id"].to_list()
        )
        factors = validated_factor_values(
            ctx.data.factor_values(
                ctx.signal_date,
                eligible_ids,
                tuple(_FACTOR_DEFINITIONS),
            ),
            signal_date=ctx.signal_date,
            instruments=eligible_ids,
            factor_refs=tuple(_FACTOR_DEFINITIONS),
        )
        scores, decisions = self._scores(universe, factors, ctx.signal_date)
        if self._audit_sink is not None:
            self._audit_sink(decisions)
        current_weights = {
            item.instrument_id.canonical(): item.current_weight
            for item in current.positions
        }
        candidates: list[dict[str, object]] = []
        for row in universe.iter_rows(named=True):
            instrument_id = cast(str, row["instrument_id"])
            candidates.append(
                {
                    "instrument_id": instrument_id,
                    "score": scores.get(instrument_id),
                    "adv_amount": row["adv_amount"],
                    "current_weight": current_weights.pop(instrument_id, 0.0),
                }
            )
        for instrument_id, weight in current_weights.items():
            candidates.append(
                {
                    "instrument_id": instrument_id,
                    "score": None,
                    "adv_amount": 0.0,
                    "current_weight": weight,
                }
            )
        candidates.sort(key=lambda row: cast(str, row["instrument_id"]))
        frame = pl.DataFrame(
            candidates,
            schema={
                "instrument_id": pl.String,
                "score": pl.Float64,
                "adv_amount": pl.Float64,
                "current_weight": pl.Float64,
            },
        )
        return ctx.portfolio_constructor.construct(
            frame, self.config.constraints, ctx.signal_date, ctx.execute_date
        )

    def _scores(
        self, universe: pl.DataFrame, factors: pl.DataFrame, signal_date: date
    ) -> tuple[dict[str, float], tuple[MultifactorDecision, ...]]:
        base_rows = list(universe.iter_rows(named=True))
        transformed: dict[str, dict[str, float]] = {
            cast(str, row["instrument_id"]): {} for row in base_rows
        }
        audit_reasons: dict[str, dict[str, str]] = {
            cast(str, row["instrument_id"]): {} for row in base_rows
        }
        source = {
            (cast(str, row["instrument_id"]), cast(str, row["factor_ref"])): row
            for row in factors.iter_rows(named=True)
        }
        for factor_ref in _FACTOR_DEFINITIONS:
            _, direction = self.config.factor_definitions[factor_ref]
            rows: list[dict[str, object]] = []
            for item in base_rows:
                identifier = cast(str, item["instrument_id"])
                observed = source.get((identifier, factor_ref))
                source_reason: str | None = None
                if observed is None:
                    source_reason = "MISSING_FACTOR_ROW"
                elif observed["is_valid"] is not True:
                    raw_reason = (
                        observed.get("invalid_reason")
                        if "invalid_reason" in factors.columns
                        else None
                    )
                    source_reason = (
                        raw_reason
                        if isinstance(raw_reason, str) and raw_reason.strip()
                        else "INPUT_INVALID"
                    )
                elif not is_available_on_signal_day(
                    observed["available_at"], signal_date
                ):
                    source_reason = "FUTURE_AVAILABLE_AT"
                valid = observed is not None and source_reason is None
                value = observed["value"] if observed is not None and valid else None
                rows.append(
                    {
                        "trade_date": signal_date,
                        "instrument_id": identifier,
                        "value": value,
                        "is_valid": valid,
                        "invalid_reason": source_reason,
                    }
                )
            frame = pl.DataFrame(
                rows,
                schema={
                    "trade_date": pl.Date,
                    "instrument_id": pl.String,
                    "value": pl.Float64,
                    "is_valid": pl.Boolean,
                    "invalid_reason": pl.String,
                },
            )
            result = zscore(
                winsorize_mad(
                    frame, "value", ("trade_date",), self.config.mad_multiplier
                ),
                "value",
                ("trade_date",),
            )
            for row in result.iter_rows(named=True):
                value = row["value"]
                if (
                    row["is_valid"] is True
                    and isinstance(value, float)
                    and isfinite(value)
                ):
                    transformed[cast(str, row["instrument_id"])][factor_ref] = (
                        direction * value
                    )
                else:
                    reason = row["invalid_reason"]
                    if isinstance(reason, str) and reason.strip():
                        audit_reasons[cast(str, row["instrument_id"])][factor_ref] = (
                            reason
                        )
        result_scores: dict[str, float] = {}
        decisions: list[MultifactorDecision] = []
        for identifier, values in transformed.items():
            if len(values) < self.config.min_valid_factors:
                decisions.append(
                    MultifactorDecision(
                        InstrumentId.parse(identifier),
                        None,
                        "INSUFFICIENT_FACTOR_COVERAGE",
                        audit_reasons[identifier],
                    )
                )
                continue
            category_scores: dict[str, list[float]] = {
                category: [] for category in _CATEGORY_WEIGHTS
            }
            for factor_ref in sorted(values):
                value = values[factor_ref]
                category_scores[self.config.factor_definitions[factor_ref][0]].append(
                    value
                )
            if any(not values for values in category_scores.values()):
                decisions.append(
                    MultifactorDecision(
                        InstrumentId.parse(identifier),
                        None,
                        "INSUFFICIENT_FACTOR_COVERAGE",
                        audit_reasons[identifier],
                    )
                )
                continue
            score = sum(
                self.config.category_weights[category] * (sum(items) / len(items))
                for category, items in category_scores.items()
            )
            result_scores[identifier] = score
            decisions.append(
                MultifactorDecision(
                    InstrumentId.parse(identifier),
                    score,
                    "MULTIFACTOR_SELECTED",
                    audit_reasons[identifier],
                )
            )
        return result_scores, tuple(decisions)
