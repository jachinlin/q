"""Weekly stock cross-sectional multifactor strategy using shared transforms."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from math import isfinite
from types import MappingProxyType
from typing import cast

import polars as pl

from quant_core.backtest.engine import StrategyRef
from quant_core.domain.identifiers import InstrumentId
from quant_core.factors.base import canonical_factor_ref, is_available_on_signal_day
from quant_core.factors.transforms import neutralize_wls, winsorize_mad, zscore
from quant_core.portfolio.constraints import PortfolioConstraints
from quant_core.portfolio.constructor import TargetPortfolio
from quant_core.strategies.base import (
    PortfolioState,
    RebalanceFrequency,
    StrategyContext,
    ValidationIssue,
    is_rebalance_boundary,
    validated_factor_values,
    validated_stock_universe,
)

_FACTOR_DEFINITIONS = {
    "earnings_yield_ttm_v1@1.0.0": ("VALUE", 1),
    "book_to_price_mrq_v1@1.0.0": ("VALUE", 1),
    "roe_avg_pit_v1@1.0.0": ("QUALITY", 1),
    "cfo_to_np_pit_v1@1.0.0": ("QUALITY", 1),
    "momentum_120_20_v1@1.0.0": ("MOMENTUM", 1),
    "volatility_60d_v1@1.0.0": ("RISK", -1),
    "downside_volatility_60d_v1@1.0.0": ("RISK", -1),
    "max_drawdown_120d_v1@1.0.0": ("RISK", -1),
}
_CATEGORY_WEIGHTS = {"VALUE": 0.25, "QUALITY": 0.25, "MOMENTUM": 0.30, "RISK": 0.20}
_EPSILON = 1e-10


def _default_factor_definitions() -> Mapping[str, tuple[str, int]]:
    return MappingProxyType(dict(_FACTOR_DEFINITIONS))


def _default_category_weights() -> Mapping[str, float]:
    return MappingProxyType(dict(_CATEGORY_WEIGHTS))


@dataclass(frozen=True, slots=True)
class MultifactorConfig:
    constraints: PortfolioConstraints
    factor_definitions: Mapping[str, tuple[str, int]] = field(
        default_factory=_default_factor_definitions
    )
    category_weights: Mapping[str, float] = field(
        default_factory=_default_category_weights
    )
    min_valid_factors: int = 6
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
            raise ValueError("factor_definitions must be the fixed eight alpha refs")
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
        if type(self.min_valid_factors) is not int or self.min_valid_factors != 6:
            raise ValueError("min_valid_factors must be the MVP value 6")
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
        constraints = _constraints_from_mapping(raw_constraints)
        raw_definitions = mapping.get("factor_definitions", _FACTOR_DEFINITIONS)
        if not isinstance(raw_definitions, Mapping):
            raise TypeError("factor_definitions must be a mapping")
        definitions: dict[str, tuple[str, int]] = {}
        for reference, value in raw_definitions.items():
            if not isinstance(value, Mapping):
                raise TypeError("factor definition must be a mapping")
            definitions[cast(str, reference)] = (
                cast(str, value.get("category")),
                cast(int, value.get("direction")),
            )
        raw_weights = mapping.get("category_weights", _CATEGORY_WEIGHTS)
        if not isinstance(raw_weights, Mapping):
            raise TypeError("category_weights must be a mapping")
        return cls(
            constraints,
            definitions,
            cast(Mapping[str, float], raw_weights),
            cast(int, mapping.get("min_valid_factors", 6)),
            cast(float, mapping.get("mad_multiplier", 3.0)),
            RebalanceFrequency(cast(str, mapping.get("frequency", "WEEKLY"))),
        )


def _constraints_from_mapping(mapping: Mapping[str, object]) -> PortfolioConstraints:
    names = {
        "max_position_weight",
        "max_industry_weight",
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
        max_industry_weight=cast(float, mapping["max_industry_weight"]),
        min_positions=cast(int, mapping["min_positions"]),
        max_positions=cast(int, mapping["max_positions"]),
        min_adv_amount=cast(float, mapping["min_adv_amount"]),
        max_turnover=cast(float, mapping["max_turnover"]),
    )


class MultifactorStrategy:
    strategy_id = "stock_multifactor"
    version = "1.0.0"

    def __init__(self, config: MultifactorConfig) -> None:
        if not isinstance(config, MultifactorConfig):
            raise TypeError("config must be a MultifactorConfig")
        self.config = config

    @property
    def ref(self) -> StrategyRef:
        return StrategyRef(self.strategy_id, self.version)

    def validate(self, ctx: StrategyContext) -> list[ValidationIssue]:
        if not isinstance(ctx, StrategyContext):
            return [ValidationIssue("INVALID_CONTEXT", "strategy context is invalid")]
        return []

    def should_rebalance(self, ctx: StrategyContext, rebalance_date: date) -> bool:
        return rebalance_date == ctx.signal_date and is_rebalance_boundary(
            ctx, self.config.frequency
        )

    def generate_targets(
        self, ctx: StrategyContext, rebalance_date: date, current: PortfolioState
    ) -> TargetPortfolio:
        if rebalance_date != ctx.signal_date:
            raise ValueError("rebalance_date must equal signal_date")
        if not isinstance(current, PortfolioState):
            raise TypeError("current must be a PortfolioState")
        universe = validated_stock_universe(
            ctx.data.stock_universe(ctx.snapshot_id, ctx.signal_date),
            signal_date=ctx.signal_date,
        ).filter(pl.col("eligible"))
        eligible_ids = tuple(
            InstrumentId.parse(value) for value in universe["instrument_id"].to_list()
        )
        factors = validated_factor_values(
            ctx.data.factor_values(
                ctx.snapshot_id,
                ctx.signal_date,
                eligible_ids,
                tuple(_FACTOR_DEFINITIONS),
            ),
            signal_date=ctx.signal_date,
            instruments=eligible_ids,
            factor_refs=tuple(_FACTOR_DEFINITIONS),
        )
        scores = self._scores(universe, factors, ctx.signal_date)
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
                    "industry": row["industry"],
                    "adv_amount": row["adv_amount"],
                    "current_weight": current_weights.pop(instrument_id, 0.0),
                }
            )
        for instrument_id, weight in current_weights.items():
            candidates.append(
                {
                    "instrument_id": instrument_id,
                    "score": None,
                    "industry": None,
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
                "industry": pl.String,
                "adv_amount": pl.Float64,
                "current_weight": pl.Float64,
            },
        )
        return ctx.portfolio_constructor.construct(
            frame, self.config.constraints, ctx.signal_date, ctx.execute_date
        )

    def _scores(
        self, universe: pl.DataFrame, factors: pl.DataFrame, signal_date: date
    ) -> dict[str, float]:
        base_rows = list(universe.iter_rows(named=True))
        transformed: dict[str, dict[str, float]] = {
            cast(str, row["instrument_id"]): {} for row in base_rows
        }
        source = {
            (cast(str, row["instrument_id"]), cast(str, row["factor_ref"])): row
            for row in factors.iter_rows(named=True)
        }
        for factor_ref, (_, direction) in self.config.factor_definitions.items():
            rows: list[dict[str, object]] = []
            for item in base_rows:
                identifier = cast(str, item["instrument_id"])
                observed = source.get((identifier, factor_ref))
                valid = (
                    observed is not None
                    and observed["is_valid"] is True
                    and is_available_on_signal_day(
                        observed["available_at"], signal_date
                    )
                )
                value = observed["value"] if observed is not None and valid else None
                rows.append(
                    {
                        "trade_date": signal_date,
                        "instrument_id": identifier,
                        "industry": item["industry"],
                        "log_market_cap": item["log_market_cap"],
                        "value": value,
                        "is_valid": valid,
                        "invalid_reason": None,
                    }
                )
            frame = pl.DataFrame(
                rows,
                schema={
                    "trade_date": pl.Date,
                    "instrument_id": pl.String,
                    "industry": pl.String,
                    "log_market_cap": pl.Float64,
                    "value": pl.Float64,
                    "is_valid": pl.Boolean,
                    "invalid_reason": pl.String,
                },
            )
            result = zscore(
                neutralize_wls(
                    winsorize_mad(
                        frame, "value", ("trade_date",), self.config.mad_multiplier
                    ),
                    "value",
                    "industry",
                    "log_market_cap",
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
        result_scores: dict[str, float] = {}
        for identifier, values in transformed.items():
            if len(values) < self.config.min_valid_factors:
                continue
            category_scores: dict[str, list[float]] = {
                category: [] for category in _CATEGORY_WEIGHTS
            }
            for factor_ref, value in values.items():
                category_scores[self.config.factor_definitions[factor_ref][0]].append(
                    value
                )
            if any(not values for values in category_scores.values()):
                continue
            result_scores[identifier] = sum(
                self.config.category_weights[category] * (sum(items) / len(items))
                for category, items in category_scores.items()
            )
        return result_scores
