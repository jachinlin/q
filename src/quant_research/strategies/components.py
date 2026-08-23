"""定义截面策略五模块的严格配置、目录和装配前校验。"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from math import isfinite
from types import MappingProxyType

from quant_research.data.contracts import JsonValue


@dataclass(frozen=True, slots=True)
class ComponentRef:
    """引用一个已登记模型及其冻结 JSON 参数。

    入参：非空 ``model_id`` 和由该模型解释的参数映射。
    返回值：不可变、可进入策略配置哈希的组件引用。
    异常：标识为空或参数不是映射时抛出类型或值错误。
    """

    model_id: str
    params: Mapping[str, JsonValue]

    def __post_init__(self) -> None:
        if not isinstance(self.model_id, str) or not self.model_id.strip():
            raise ValueError("component model_id must be nonempty")
        if not isinstance(self.params, Mapping):
            raise TypeError("component params must be a mapping")
        object.__setattr__(
            self, "params", MappingProxyType(dict(sorted(self.params.items())))
        )

    @classmethod
    def from_mapping(cls, value: object) -> ComponentRef:
        """从严格 ``{model_id, params}`` 映射构造引用。

        入参：YAML 解析后的组件值。
        返回值：规范化组件引用，缺省 ``params`` 为空映射。
        异常：字段缺失、多余或类型非法时抛出类型或值错误。
        """
        if not isinstance(value, Mapping):
            raise TypeError("component reference must be a mapping")
        unknown = set(value) - {"model_id", "params"}
        if unknown:
            raise ValueError(f"unknown component field: {min(unknown)}")
        model_id = value.get("model_id")
        if not isinstance(model_id, str):
            raise TypeError("component model_id must be a string")
        params = value.get("params", {})
        if not isinstance(params, Mapping):
            raise TypeError("component params must be a mapping")
        return cls(model_id, dict(params))

    def as_json(self) -> dict[str, JsonValue]:
        """返回可确定性序列化的组件配置。

        入参：无。
        返回值：包含 ``model_id`` 和参数副本的 JSON 映射。
        异常：无。
        """
        return {"model_id": self.model_id, "params": dict(self.params)}


@dataclass(frozen=True, slots=True)
class ConstraintSet:
    """声明 P3 多头组合的持仓、换手、行业、ADV 和敞口约束。

    入参：持仓数量、单标的权重、换手、行业权重、最小 ADV 和最大多头敞口。
    返回值：冻结的声明式约束集合。
    异常：数量关系、非有限值或比例范围非法时抛出值错误。
    """

    min_positions: int = 1
    max_positions: int = 20
    max_position_weight: float = 0.1
    max_turnover: float = 1.0
    max_industry_weight: float = 1.0
    min_adv_amount: float = 0.0
    long_exposure: float = 1.0

    def __post_init__(self) -> None:
        if not 0 < self.min_positions <= self.max_positions:
            raise ValueError("position count constraint is invalid")
        proportions = (
            self.max_position_weight,
            self.max_turnover,
            self.max_industry_weight,
            self.long_exposure,
        )
        if any(not isfinite(value) or not 0 <= value <= 1 for value in proportions):
            raise ValueError("weight constraints must be finite values in [0, 1]")
        if self.max_position_weight <= 0 or self.long_exposure <= 0:
            raise ValueError("position cap and long exposure must be positive")
        if not isfinite(self.min_adv_amount) or self.min_adv_amount < 0:
            raise ValueError("minimum ADV must be finite and nonnegative")

    @classmethod
    def from_mapping(cls, value: Mapping[str, JsonValue]) -> ConstraintSet:
        """从严格约束参数映射构造 P3 约束。

        入参：``long_only`` 组件的参数映射。
        返回值：经过范围校验的约束集合。
        异常：未知字段、类型不符或约束不可行时抛出类型或值错误。
        """
        allowed = {
            "min_positions",
            "max_positions",
            "max_position_weight",
            "max_turnover",
            "max_industry_weight",
            "min_adv_amount",
            "long_exposure",
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unknown constraint parameter: {min(unknown)}")
        integers: dict[str, int] = {}
        for field in ("min_positions", "max_positions"):
            raw = value.get(field, 1 if field == "min_positions" else 20)
            if type(raw) is not int:
                raise TypeError(f"{field} must be an integer")
            integers[field] = raw
        numbers: dict[str, float] = {}
        defaults = {
            "max_position_weight": 0.1,
            "max_turnover": 1.0,
            "max_industry_weight": 1.0,
            "min_adv_amount": 0.0,
            "long_exposure": 1.0,
        }
        for field, default in defaults.items():
            raw = value.get(field, default)
            if isinstance(raw, bool) or not isinstance(raw, (int, float)):
                raise TypeError(f"{field} must be numeric")
            numbers[field] = float(raw)
        return cls(
            min_positions=integers["min_positions"],
            max_positions=integers["max_positions"],
            max_position_weight=numbers["max_position_weight"],
            max_turnover=numbers["max_turnover"],
            max_industry_weight=numbers["max_industry_weight"],
            min_adv_amount=numbers["min_adv_amount"],
            long_exposure=numbers["long_exposure"],
        )

    def as_json(self) -> dict[str, JsonValue]:
        """返回进入策略规格和配置哈希的完整约束值。

        入参：无。
        返回值：包含全部显式默认值的 JSON 映射。
        异常：无。
        """
        return {
            "min_positions": self.min_positions,
            "max_positions": self.max_positions,
            "max_position_weight": self.max_position_weight,
            "max_turnover": self.max_turnover,
            "max_industry_weight": self.max_industry_weight,
            "min_adv_amount": self.min_adv_amount,
            "long_exposure": self.long_exposure,
        }


@dataclass(frozen=True, slots=True)
class StrategyPipelineConfig:
    """保存 Alpha、Risk、Cost、Construction、Constraint 五模块装配。

    入参：五个组件引用、决策频率和目标差额续单容差。
    返回值：不可变截面策略流水线配置。
    异常：组件未知、参数非法或能力组合冲突时抛出错误。
    """

    alpha: ComponentRef
    risk: ComponentRef
    cost: ComponentRef
    construction: ComponentRef
    constraints: ComponentRef
    frequency: str
    target_tolerance: float = 0.001

    @classmethod
    def from_parameters(
        cls,
        value: Mapping[str, JsonValue],
        catalog: StrategyComponentCatalog | None = None,
    ) -> StrategyPipelineConfig:
        """解析策略参数中的唯一 ``pipeline`` 配置。

        入参：策略参数和可选组件目录；参数只允许一个 ``pipeline`` 根字段。
        返回值：规范化且完成能力校验的五模块配置。
        异常：字段缺失、多余、模型未知或能力冲突时抛出类型或值错误。
        """
        unknown = set(value) - {"pipeline"}
        if unknown:
            raise ValueError(f"unknown cross-sectional parameter: {min(unknown)}")
        raw = value.get("pipeline")
        if not isinstance(raw, Mapping):
            raise TypeError("pipeline must be a mapping")
        allowed = {
            "alpha",
            "risk",
            "cost",
            "construction",
            "constraints",
            "frequency",
            "target_tolerance",
        }
        extra = set(raw) - allowed
        if extra:
            raise ValueError(f"unknown pipeline field: {min(extra)}")
        required = {
            "alpha",
            "risk",
            "cost",
            "construction",
            "constraints",
            "frequency",
        }
        missing = sorted(required - set(raw))
        if missing:
            raise ValueError(f"pipeline field is required: {missing[0]}")
        frequency = raw["frequency"]
        if not isinstance(frequency, str) or not frequency:
            raise TypeError("pipeline frequency must be a nonempty string")
        tolerance = raw.get("target_tolerance", 0.001)
        if isinstance(tolerance, bool) or not isinstance(tolerance, (int, float)):
            raise TypeError("target_tolerance must be numeric")
        result = cls(
            ComponentRef.from_mapping(raw["alpha"]),
            ComponentRef.from_mapping(raw["risk"]),
            ComponentRef.from_mapping(raw["cost"]),
            ComponentRef.from_mapping(raw["construction"]),
            ComponentRef.from_mapping(raw["constraints"]),
            frequency,
            float(tolerance),
        )
        (catalog or StrategyComponentCatalog()).validate_pipeline(result)
        return result

    def __post_init__(self) -> None:
        if not isfinite(self.target_tolerance) or not 0 <= self.target_tolerance <= 0.1:
            raise ValueError("target_tolerance must be in [0, 0.1]")

    @property
    def constraint_set(self) -> ConstraintSet:
        """解析并返回声明式 ``long_only`` 约束。

        入参：无。
        返回值：冻结的约束集合。
        异常：约束组件不是 ``long_only`` 或参数非法时抛出值错误。
        """
        if self.constraints.model_id != "long_only":
            raise ValueError("P3 supports only long_only constraints")
        return ConstraintSet.from_mapping(self.constraints.params)

    def as_json(self) -> dict[str, JsonValue]:
        """返回包含显式默认值的稳定 JSON 配置。

        入参：无。
        返回值：可进入 ``StrategySpec`` 和配置哈希的流水线映射。
        异常：约束参数非法时传播约束校验错误。
        """
        constraints = ComponentRef("long_only", self.constraint_set.as_json())
        return {
            "alpha": self.alpha.as_json(),
            "risk": self.risk.as_json(),
            "cost": self.cost.as_json(),
            "construction": self.construction.as_json(),
            "constraints": constraints.as_json(),
            "frequency": self.frequency,
            "target_tolerance": self.target_tolerance,
        }


class StrategyComponentCatalog:
    """公开五模块目录、参数 Schema，并执行提交前能力装配校验。

    入参：构造无需参数。
    返回值：稳定组件目录、JSON Schema 或校验结果。
    异常：未知类别、模型、参数或不兼容装配时抛出值错误。
    """

    _components = MappingProxyType(
        {
            "alpha": ("single_factor", "multi_factor_composite"),
            "risk": ("none", "sample_cov", "shrinkage"),
            "cost": ("fixed_bps", "linear_impact", "sqrt_impact"),
            "construction": ("top_n_equal_weight", "mean_variance"),
            "constraint": ("long_only",),
        }
    )

    def list(self) -> dict[str, tuple[str, ...]]:
        """返回按类别和模型 ID 稳定排序的目录。

        入参：无。
        返回值：类别到模型 ID 元组的映射。
        异常：无。
        """
        return {
            key: tuple(sorted(values))
            for key, values in sorted(self._components.items())
        }

    def describe(self) -> dict[str, JsonValue]:
        """返回 Dashboard 编排器使用的组件 JSON Schema。

        入参：无。
        返回值：按类别和模型排序的参数 Schema 及能力约束。
        异常：无。
        """
        return {
            "components": {
                category: [
                    {
                        "model_id": model_id,
                        "params_schema": self._schema(category, model_id),
                    }
                    for model_id in sorted(model_ids)
                ]
                for category, model_ids in sorted(self._components.items())
            },
            "capability_rules": [
                {
                    "if": {"construction": "mean_variance"},
                    "requires": {"risk": ["sample_cov", "shrinkage"]},
                    "error": "PIPELINE_MODEL_UNAVAILABLE",
                }
            ],
        }

    def validate(self, category: str, component_id: str) -> None:
        """确认模型存在于指定类别。

        入参：组件类别和模型 ID。
        返回值：无。
        异常：类别或模型未登记时抛出 ``ValueError``。
        """
        if category not in self._components:
            raise ValueError(f"unknown component category: {category}")
        if component_id not in self._components[category]:
            raise ValueError(f"unknown {category} component: {component_id}")

    def validate_pipeline(self, pipeline: StrategyPipelineConfig) -> None:
        """校验五模块身份、通用参数和跨模块能力关系。

        入参：已解析的流水线配置。
        返回值：装配兼容时返回 None。
        异常：模型未知、参数多余或 MVO 使用退化风险时抛出值错误。
        """
        refs = {
            "alpha": pipeline.alpha,
            "risk": pipeline.risk,
            "cost": pipeline.cost,
            "construction": pipeline.construction,
            "constraint": pipeline.constraints,
        }
        for category, ref in refs.items():
            self.validate(category, ref.model_id)
            self._validate_params(category, ref)
        if (
            pipeline.construction.model_id == "mean_variance"
            and pipeline.risk.model_id == "none"
        ):
            raise ValueError(
                "PIPELINE_MODEL_UNAVAILABLE: mean_variance requires non-degenerate risk"
            )
        _ = pipeline.constraint_set

    @staticmethod
    def _validate_params(category: str, ref: ComponentRef) -> None:
        common: dict[tuple[str, str], set[str]] = {
            ("risk", "none"): set(),
            ("risk", "sample_cov"): {"lookback"},
            ("risk", "shrinkage"): {"lookback", "shrinkage"},
            ("cost", "fixed_bps"): set(),
            ("cost", "linear_impact"): {
                "impact_bps",
                "max_participation",
            },
            ("cost", "sqrt_impact"): {
                "impact_bps",
                "max_participation",
            },
            ("construction", "top_n_equal_weight"): {"top_n"},
            ("construction", "mean_variance"): {
                "risk_aversion",
                "cost_aversion",
                "iterations",
                "learning_rate",
            },
            ("constraint", "long_only"): {
                "min_positions",
                "max_positions",
                "max_position_weight",
                "max_turnover",
                "max_industry_weight",
                "min_adv_amount",
                "long_exposure",
            },
        }
        allowed = common.get((category, ref.model_id))
        if allowed is None:
            return
        unknown = set(ref.params) - allowed
        if unknown:
            raise ValueError(f"unknown {ref.model_id} parameter: {min(unknown)}")

    @staticmethod
    def _schema(category: str, model_id: str) -> dict[str, JsonValue]:
        properties: dict[str, JsonValue] = {}
        if category == "risk" and model_id != "none":
            properties["lookback"] = {"type": "integer", "minimum": 2}
        if model_id == "shrinkage":
            properties["shrinkage"] = {
                "type": "number",
                "minimum": 0,
                "maximum": 1,
            }
        if category == "cost" and model_id != "fixed_bps":
            properties["impact_bps"] = {"type": "number", "minimum": 0}
            properties["max_participation"] = {
                "type": "number",
                "exclusiveMinimum": 0,
                "maximum": 1,
            }
        if category == "construction" and model_id == "top_n_equal_weight":
            properties["top_n"] = {"type": "integer", "minimum": 1}
        if category == "construction" and model_id == "mean_variance":
            properties = {
                "risk_aversion": {"type": "number", "minimum": 0},
                "cost_aversion": {"type": "number", "minimum": 0},
                "iterations": {"type": "integer", "minimum": 1},
                "learning_rate": {"type": "number", "exclusiveMinimum": 0},
            }
        if category == "constraint":
            properties = {
                "min_positions": {"type": "integer", "minimum": 1},
                "max_positions": {"type": "integer", "minimum": 1},
                "max_position_weight": {
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "maximum": 1,
                },
                "max_turnover": {"type": "number", "minimum": 0, "maximum": 1},
                "max_industry_weight": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                },
                "min_adv_amount": {"type": "number", "minimum": 0},
                "long_exposure": {
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "maximum": 1,
                },
            }
        return {
            "type": "object",
            "additionalProperties": False,
            "properties": properties,
        }


__all__ = [
    "ComponentRef",
    "ConstraintSet",
    "StrategyComponentCatalog",
    "StrategyPipelineConfig",
]
