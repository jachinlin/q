"""定义策略模板和目标架构组件注册表。"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from math import isfinite
from typing import cast

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.domain.components import (
    ComponentDescriptor,
    CompositionValidator,
    SignalKind,
)
from quant_research.research_protocols.models import ResearchFamilyConfig


@dataclass(frozen=True, slots=True)
class StrategyTemplate:
    """声明一个参考策略使用的信号类型和组件槽位。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    strategy_id: str
    label: str
    signal_kind: SignalKind
    components: tuple[str, ...]


class ComponentRegistry:
    """提供组合根可装配且 Dashboard 可发现的组件目录。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def __init__(self) -> None:
        self._components = {item.component_id: item for item in _descriptors()}
        self._templates = {item.strategy_id: item for item in _templates()}

    def descriptors(self) -> tuple[ComponentDescriptor, ...]:
        """按组件 ID 返回稳定目录。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return tuple(self._components[key] for key in sorted(self._components))

    def templates(self) -> tuple[StrategyTemplate, ...]:
        """按策略 ID 返回三个参考模板。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return tuple(self._templates[key] for key in sorted(self._templates))

    def descriptor(self, component_id: str) -> ComponentDescriptor:
        """读取单个组件，不存在时明确失败。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        try:
            return self._components[component_id]
        except KeyError as error:
            raise KeyError(f"component not found: {component_id}") from error

    def template(self, strategy_id: str) -> StrategyTemplate:
        """读取单个策略模板。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        try:
            return self._templates[strategy_id]
        except KeyError as error:
            raise KeyError(f"strategy template not found: {strategy_id}") from error

    def as_json(self) -> dict[str, JsonValue]:
        """返回可直接提供给 CLI 和 Dashboard 的目录 DTO。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return {
            "components": [
                {
                    **asdict(item),
                    "supported_signal_kinds": [value.value for value in item.supported_signal_kinds],
                }
                for item in self.descriptors()
            ],
            "templates": [
                {
                    "strategy_id": item.strategy_id,
                    "label": item.label,
                    "signal_kind": item.signal_kind.value,
                    "components": list(item.components),
                }
                for item in self.templates()
            ],
        }

    def validate(self, config: ResearchFamilyConfig) -> StrategyTemplate:
        """校验配置槽位、能力闭包、信号类型和决策/调仓语义。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        template = self.template(config.strategy_id)
        configured_blocks = (
            (config.universe, "component"),
            (config.features, "component"),
            (config.signal, "component"),
            (config.decision_schedule, "component"),
            (config.risk, "estimator"),
            (config.pretrade_cost, "component"),
            (config.portfolio, "constructor"),
            (config.rebalance, "policy"),
            (config.execution, "simulator"),
        )
        for block, discriminator in configured_blocks:
            component_id = self._field(block, discriminator, discriminator)
            descriptor = self.descriptor(component_id)
            _SchemaValidator.validate(block, descriptor.config_schema, component_id)
        _SchemaValidator.validate_analytics(config.analytics)
        slots = (
            self._field(config.universe, "component", "universe"),
            self._field(config.features, "component", "features"),
            self._field(config.signal, "component", "signal"),
            self._field(config.risk, "estimator", "risk"),
            self._field(config.pretrade_cost, "component", "pretrade_cost"),
            self._field(config.portfolio, "constructor", "portfolio"),
            "rebalance_planner",
            self._field(config.execution, "simulator", "execution"),
        )
        if slots != template.components:
            raise ValueError(
                "strategy component slots do not match template: "
                + ", ".join(slots)
            )
        declared_kind = self._field(config.signal, "kind", "signal")
        if declared_kind != template.signal_kind.value:
            raise ValueError(
                f"signal kind {declared_kind} does not match {template.signal_kind.value}"
            )
        CompositionValidator().validate(
            tuple(self.descriptor(item) for item in slots),
            signal_kind=template.signal_kind,
        )
        schedule = self._field(
            config.decision_schedule, "component", "decision_schedule"
        )
        policy = self._field(config.rebalance, "policy", "rebalance")
        expected = {
            "stock_multifactor": ("period_boundary", "scheduled_with_drift_threshold"),
            "dual_ma_trend": ("every_session", "signal_state_change"),
            "etf_rotation": ("period_boundary", "scheduled_with_drift_threshold"),
        }[config.strategy_id]
        if (schedule, policy) != expected:
            raise ValueError(
                "decision schedule and rebalance policy do not match strategy semantics"
            )
        self._validate_cross_fields(config)
        return template

    @staticmethod
    def _validate_cross_fields(config: ResearchFamilyConfig) -> None:
        """校验无法由单对象 JSON Schema 表达的字段关系。"""
        if config.strategy_id == "dual_ma_trend":
            short = cast(int, config.signal["short_window_sessions"])
            long = cast(int, config.signal["long_window_sessions"])
            if short >= long:
                raise ValueError("short_window_sessions must be less than long_window_sessions")
        if config.strategy_id == "stock_multifactor":
            minimum = cast(int, config.portfolio["min_positions"])
            maximum = cast(int, config.portfolio["max_positions"])
            if minimum > maximum:
                raise ValueError("min_positions must not exceed max_positions")

    @staticmethod
    def _field(block: dict[str, JsonValue], field: str, slot: str) -> str:
        value = block.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(f"{slot} requires string field {field}")
        return value


def _descriptor(
    component_id: str,
    *,
    inputs: tuple[str, ...],
    outputs: tuple[str, ...],
    datasets: tuple[str, ...],
    kinds: tuple[SignalKind, ...] = (),
    discriminator: str = "component",
    properties: Mapping[str, JsonValue] | None = None,
    required: tuple[str, ...] = (),
) -> ComponentDescriptor:
    schema_properties: dict[str, JsonValue] = {
        discriminator: {"const": component_id, "type": "string"}
    }
    schema_properties.update(properties or {})
    schema: dict[str, JsonValue] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "properties": schema_properties,
        "required": [discriminator, *required],
    }
    identity: dict[str, JsonValue] = {
        "component_id": component_id,
        "inputs": list(inputs),
        "outputs": list(outputs),
        "datasets": list(datasets),
        "kinds": [item.value for item in kinds],
        "schema": schema,
    }
    digest = hashlib.sha256(canonical_json_bytes(identity)).hexdigest()
    return ComponentDescriptor(
        component_id=component_id,
        component_hash=digest,
        input_capabilities=tuple(sorted(inputs)),
        output_capabilities=tuple(sorted(outputs)),
        required_datasets=tuple(sorted(datasets)),
        required_fields=(),
        supported_signal_kinds=kinds,
        supports_batch=True,
        determinism_contract="canonical-inputs/stable-sort/no-cross-run-cache",
        config_schema=schema,
    )


def _descriptors() -> tuple[ComponentDescriptor, ...]:
    all_kinds = tuple(SignalKind)
    return (
        _descriptor("cn_stock_standard", inputs=(), outputs=("UNIVERSE",), datasets=("daily_bar", "instrument", "security_status"), properties={"exclude_st": _boolean(), "exclude_suspended": _boolean(), "min_listing_days": _integer(0), "allowed_boards": _array(_enum("MAIN", "CHINEXT", "STAR"), 1), "min_avg_amount_20d": _number(0.0)}, required=("exclude_st", "exclude_suspended", "min_listing_days")),
        _descriptor("fixed_instruments", inputs=(), outputs=("UNIVERSE",), datasets=("instrument", "security_status"), properties={"instruments": _array({"type": "string", "pattern": r"^\d{6}\.(SH|SZ)$"}, 1)}, required=("instruments",)),
        _descriptor("stock_research_features", inputs=("UNIVERSE",), outputs=("FEATURES",), datasets=("daily_bar", "daily_basic", "financial_observation", "industry_classification"), properties={"components": _array({"type": "string"}, 1)}, required=("components",)),
        _descriptor("price_trend_features", inputs=("UNIVERSE",), outputs=("FEATURES",), datasets=("daily_bar",), properties={"windows": _array(_integer(1), 1), "return_windows": _array(_integer(1), 1), "trend_window": _integer(1), "volatility_window": _integer(2), "adv_window": _integer(1)}, required=("volatility_window",)),
        _descriptor("cross_sectional_multifactor", inputs=("FEATURES",), outputs=("SIGNAL",), datasets=(), kinds=(SignalKind.CROSS_SECTIONAL_SCORE,), properties={"kind": {"const": "CROSS_SECTIONAL_SCORE", "type": "string"}, "factor_weights": _number_map()}, required=("kind", "factor_weights")),
        _descriptor("dual_ma_directional", inputs=("FEATURES",), outputs=("SIGNAL",), datasets=(), kinds=(SignalKind.DIRECTIONAL,), properties={"kind": {"const": "DIRECTIONAL", "type": "string"}, "short_window_sessions": _integer(1), "long_window_sessions": _integer(2)}, required=("kind", "short_window_sessions", "long_window_sessions")),
        _descriptor("etf_rotation_allocation", inputs=("FEATURES",), outputs=("SIGNAL",), datasets=(), kinds=(SignalKind.ALLOCATION,), properties={"kind": {"const": "ALLOCATION", "type": "string"}, "return_weights": _number_map(), "trend_window_sessions": _integer(1), "volatility_window_sessions": _integer(2), "volatility_penalty": _number(0.0), "top_n": _integer(1)}, required=("kind", "return_weights", "trend_window_sessions", "volatility_window_sessions", "volatility_penalty", "top_n")),
        _descriptor("period_boundary", inputs=(), outputs=("DECISION_SCHEDULE",), datasets=("trade_calendar",), properties={"frequency": _enum("WEEKLY", "MONTHLY")}, required=("frequency",)),
        _descriptor("every_session", inputs=(), outputs=("DECISION_SCHEDULE",), datasets=("trade_calendar",), properties={"frequency": {"const": "DAILY", "type": "string"}}, required=("frequency",)),
        _descriptor("fundamental_statistical", inputs=("FEATURES",), outputs=("RISK",), datasets=("daily_bar", "industry_classification"), kinds=all_kinds, discriminator="estimator", properties={"lookback_sessions": _integer(2), "covariance_shrinkage": _number(0.0, 1.0)}, required=("lookback_sessions", "covariance_shrinkage")),
        _descriptor("asset_volatility_and_liquidity", inputs=("FEATURES",), outputs=("RISK",), datasets=("daily_bar",), kinds=all_kinds, discriminator="estimator", properties={"lookback_sessions": _integer(2)}, required=("lookback_sessions",)),
        _descriptor("liquidity_impact_surface", inputs=("FEATURES",), outputs=("PRETRADE_COST",), datasets=("daily_bar",), kinds=all_kinds, properties={"fixed_bps": _number(0.0), "impact_bps": _number(0.0), "max_volume_participation": _number(0.0, 1.0, exclusive_minimum=True)}, required=("fixed_bps", "impact_bps", "max_volume_participation")),
        _descriptor("alpha_risk_cost_optimizer", inputs=("PRETRADE_COST", "RISK", "SIGNAL", "UNIVERSE"), outputs=("TARGET_PORTFOLIO",), datasets=(), kinds=(SignalKind.CROSS_SECTIONAL_SCORE,), discriminator="constructor", properties={"min_positions": _integer(1), "max_positions": _integer(1), "max_position_weight": _number(0.0, 1.0, exclusive_minimum=True), "max_turnover": _number(0.0, 2.0), "risk_aversion": _number(0.0), "cost_aversion": _number(0.0)}, required=("min_positions", "max_positions", "max_position_weight", "max_turnover", "risk_aversion", "cost_aversion")),
        _descriptor("directional_exposure_mapper", inputs=("PRETRADE_COST", "RISK", "SIGNAL", "UNIVERSE"), outputs=("TARGET_PORTFOLIO",), datasets=(), kinds=(SignalKind.DIRECTIONAL,), discriminator="constructor", properties={"long_weight": _number(0.0, 1.0), "flat_weight": {"const": 0.0, "type": "number"}}, required=("long_weight", "flat_weight")),
        _descriptor("allocation_projector", inputs=("PRETRADE_COST", "RISK", "SIGNAL", "UNIVERSE"), outputs=("TARGET_PORTFOLIO",), datasets=(), kinds=(SignalKind.ALLOCATION,), discriminator="constructor", properties={"max_position_weight": _number(0.0, 1.0, exclusive_minimum=True)}, required=("max_position_weight",)),
        _descriptor("scheduled_with_drift_threshold", inputs=("TARGET_PORTFOLIO",), outputs=("REBALANCE_POLICY",), datasets=(), kinds=all_kinds, discriminator="policy", properties={"min_weight_drift": _number(0.0, 1.0)}, required=("min_weight_drift",)),
        _descriptor("signal_state_change", inputs=("TARGET_PORTFOLIO",), outputs=("REBALANCE_POLICY",), datasets=(), kinds=all_kinds, discriminator="policy"),
        _descriptor("rebalance_planner", inputs=("TARGET_PORTFOLIO",), outputs=("REBALANCE_PLAN",), datasets=(), kinds=all_kinds),
        _descriptor("a_share_daily", inputs=("REBALANCE_PLAN",), outputs=("EXECUTION",), datasets=("daily_bar", "security_status"), kinds=all_kinds, discriminator="simulator", properties={"reference_price": _enum("OPEN", "CLOSE"), "slippage_bps": _number(0.0), "max_volume_participation": _number(0.0, 1.0, exclusive_minimum=True)}, required=("reference_price", "slippage_bps", "max_volume_participation")),
    )


def _boolean() -> dict[str, JsonValue]:
    return {"type": "boolean"}


def _integer(minimum: int) -> dict[str, JsonValue]:
    return {"type": "integer", "minimum": minimum}


def _number(
    minimum: float,
    maximum: float | None = None,
    *,
    exclusive_minimum: bool = False,
) -> dict[str, JsonValue]:
    schema: dict[str, JsonValue] = {"type": "number"}
    schema["exclusiveMinimum" if exclusive_minimum else "minimum"] = minimum
    if maximum is not None:
        schema["maximum"] = maximum
    return schema


def _enum(*values: str) -> dict[str, JsonValue]:
    return {"type": "string", "enum": list(values)}


def _array(items: dict[str, JsonValue], minimum: int) -> dict[str, JsonValue]:
    return {"type": "array", "items": items, "minItems": minimum}


def _number_map() -> dict[str, JsonValue]:
    return {
        "type": "object",
        "additionalProperties": {"type": "number"},
        "minProperties": 1,
    }


class _SchemaValidator:
    """执行组件目录所发布 JSON Schema 的受控子集。"""

    @classmethod
    def validate(
        cls, value: JsonValue, schema: Mapping[str, JsonValue], path: str
    ) -> None:
        """验证对象、标量、数组、边界、枚举和附加字段。"""
        expected = schema.get("type")
        if expected == "object":
            if not isinstance(value, Mapping):
                raise ValueError(f"{path} must be an object")
            properties = schema.get("properties", {})
            if not isinstance(properties, Mapping):
                raise ValueError(f"invalid component schema: {path}")
            required = schema.get("required", [])
            if isinstance(required, Sequence):
                missing = [
                    key
                    for key in cast(Sequence[str], required)
                    if key not in value
                ]
                if missing:
                    raise ValueError(f"{path} missing required fields: {', '.join(missing)}")
            extras = sorted(set(value) - set(properties))
            additional = schema.get("additionalProperties", True)
            if extras and additional is False:
                raise ValueError(f"{path} has unknown fields: {', '.join(extras)}")
            for key, item in value.items():
                child = properties.get(key, additional)
                if isinstance(child, Mapping):
                    cls.validate(item, child, f"{path}.{key}")
            minimum = schema.get("minProperties")
            if isinstance(minimum, int) and len(value) < minimum:
                raise ValueError(f"{path} must contain at least {minimum} fields")
            return
        if expected == "array":
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise ValueError(f"{path} must be an array")
            minimum = schema.get("minItems")
            if isinstance(minimum, int) and len(value) < minimum:
                raise ValueError(f"{path} must contain at least {minimum} items")
            items = schema.get("items")
            if isinstance(items, Mapping):
                for index, item in enumerate(value):
                    cls.validate(item, items, f"{path}[{index}]")
        elif expected == "string" and not isinstance(value, str):
            raise ValueError(f"{path} must be a string")
        elif expected == "integer" and (type(value) is not int):
            raise ValueError(f"{path} must be an integer")
        elif expected == "number" and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(float(value))
        ):
            raise ValueError(f"{path} must be a finite number")
        elif expected == "boolean" and type(value) is not bool:
            raise ValueError(f"{path} must be a boolean")
        if "const" in schema and value != schema["const"]:
            raise ValueError(f"{path} must equal {schema['const']}")
        allowed = schema.get("enum")
        if isinstance(allowed, Sequence) and value not in allowed:
            raise ValueError(f"{path} must be one of {', '.join(map(str, allowed))}")
        if (
            isinstance(value, str)
            and isinstance(schema.get("pattern"), str)
            and re.fullmatch(str(schema["pattern"]), value) is None
        ):
            raise ValueError(f"{path} has invalid format")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            minimum_value = cast(int | float, schema.get("minimum"))
            exclusive_minimum = cast(int | float, schema.get("exclusiveMinimum"))
            maximum_value = cast(int | float, schema.get("maximum"))
            if "minimum" in schema and float(value) < float(minimum_value):
                raise ValueError(f"{path} is below minimum")
            if "exclusiveMinimum" in schema and float(value) <= float(exclusive_minimum):
                raise ValueError(f"{path} must exceed minimum")
            if "maximum" in schema and float(value) > float(maximum_value):
                raise ValueError(f"{path} exceeds maximum")

    @classmethod
    def validate_analytics(cls, value: Mapping[str, JsonValue]) -> None:
        """校验首版分析器配置，拒绝未知分析器和字段。"""
        if set(value) != {"analyzers"}:
            raise ValueError("analytics only accepts analyzers")
        analyzers = value["analyzers"]
        allowed = {
            "allocation_signal",
            "cross_sectional_signal",
            "execution",
            "performance",
            "portfolio_risk",
            "regime",
            "time_series_signal",
        }
        if not isinstance(analyzers, list) or not analyzers:
            raise ValueError("analytics.analyzers must be a non-empty list")
        if any(not isinstance(item, str) or item not in allowed for item in analyzers):
            raise ValueError("analytics.analyzers contains an unknown analyzer")


def _templates() -> tuple[StrategyTemplate, ...]:
    return (
        StrategyTemplate("stock_multifactor", "股票多因子", SignalKind.CROSS_SECTIONAL_SCORE, ("cn_stock_standard", "stock_research_features", "cross_sectional_multifactor", "fundamental_statistical", "liquidity_impact_surface", "alpha_risk_cost_optimizer", "rebalance_planner", "a_share_daily")),
        StrategyTemplate("dual_ma_trend", "双均线趋势", SignalKind.DIRECTIONAL, ("fixed_instruments", "price_trend_features", "dual_ma_directional", "asset_volatility_and_liquidity", "liquidity_impact_surface", "directional_exposure_mapper", "rebalance_planner", "a_share_daily")),
        StrategyTemplate("etf_rotation", "ETF 轮动", SignalKind.ALLOCATION, ("fixed_instruments", "price_trend_features", "etf_rotation_allocation", "asset_volatility_and_liquidity", "liquidity_impact_surface", "allocation_projector", "rebalance_planner", "a_share_daily")),
    )
