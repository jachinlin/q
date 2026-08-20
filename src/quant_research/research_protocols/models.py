"""定义不可变研究协议与严格策略组合配置。"""

from __future__ import annotations

from datetime import date
from enum import StrEnum
from math import isfinite
from typing import cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.domain.identifiers import InstrumentId


class ResearchMode(StrEnum):
    """定义研究链路执行深度。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    SIGNAL_STUDY = "SIGNAL_STUDY"
    PORTFOLIO_STUDY = "PORTFOLIO_STUDY"
    BACKTEST_EXPERIMENT = "BACKTEST_EXPERIMENT"


class MetricDirection(StrEnum):
    """定义主要选择指标的优化方向。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    MAXIMIZE = "MAXIMIZE"
    MINIMIZE = "MINIMIZE"


class MultipleTestingMethod(StrEnum):
    """定义候选显著性检验校正方法。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    NONE = "NONE"
    BENJAMINI_HOCHBERG = "BENJAMINI_HOCHBERG"
    HOLM_BONFERRONI = "HOLM_BONFERRONI"


class ConstraintOperator(StrEnum):
    """定义选型约束比较运算。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    LTE = "LTE"
    GTE = "GTE"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ResearchPeriod(_FrozenModel):
    """表示一个闭区间研究样本段。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    start: date
    end: date

    @model_validator(mode="after")
    def validate_order(self) -> ResearchPeriod:
        """校验区间端点顺序。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if self.start > self.end:
            raise ValueError("research period start must not exceed end")
        return self


class SelectionConstraint(_FrozenModel):
    """定义候选必须满足的次要指标阈值。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    metric: str = Field(min_length=1)
    operator: ConstraintOperator
    threshold: float

    @field_validator("operator", mode="before")
    @classmethod
    def parse_operator(cls, value: object) -> ConstraintOperator:
        """把 YAML 枚举文本转换为严格领域枚举。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return value if isinstance(value, ConstraintOperator) else ConstraintOperator(cast(str, value))

    @field_validator("threshold")
    @classmethod
    def validate_threshold(cls, value: float) -> float:
        """确认约束阈值有限。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if not isfinite(value):
            raise ValueError("selection threshold must be finite")
        return value


class SelectionPolicy(_FrozenModel):
    """定义只读取验证集指标的确定性选型规则。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    primary_metric: str = Field(min_length=1)
    direction: MetricDirection
    constraints: tuple[SelectionConstraint, ...] = ()
    tie_breakers: tuple[str, ...] = ()
    multiple_testing_method: MultipleTestingMethod = MultipleTestingMethod.NONE
    adjusted_alpha: float | None = None

    @field_validator("direction", mode="before")
    @classmethod
    def parse_direction(cls, value: object) -> MetricDirection:
        """把 YAML 枚举文本转换为严格领域枚举。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return value if isinstance(value, MetricDirection) else MetricDirection(cast(str, value))

    @field_validator("multiple_testing_method", mode="before")
    @classmethod
    def parse_testing_method(cls, value: object) -> MultipleTestingMethod:
        """把 YAML 枚举文本转换为严格领域枚举。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if isinstance(value, MultipleTestingMethod):
            return value
        return MultipleTestingMethod(cast(str, value))

    @field_validator("constraints", "tie_breakers", mode="before")
    @classmethod
    def parse_tuple_fields(cls, value: object) -> object:
        """把 YAML 序列规范化为不可变元组。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="after")
    def validate_testing(self) -> SelectionPolicy:
        """校验显著性校正参数组合。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if self.multiple_testing_method is MultipleTestingMethod.NONE:
            if self.adjusted_alpha is not None:
                raise ValueError("adjusted_alpha requires a multiple testing method")
        elif self.adjusted_alpha is None or not 0.0 < self.adjusted_alpha < 1.0:
            raise ValueError("adjusted_alpha must be between zero and one")
        if len(set(self.tie_breakers)) != len(self.tie_breakers):
            raise ValueError("tie_breakers must be unique")
        return self


class ResearchProtocol(_FrozenModel):
    """固定训练、验证、测试边界和候选搜索治理规则。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    train: ResearchPeriod
    validation: ResearchPeriod
    test: ResearchPeriod
    parameter_search_space: dict[str, tuple[JsonValue, ...]]
    selection: SelectionPolicy
    random_seed: int = 0

    @field_validator("parameter_search_space", mode="before")
    @classmethod
    def validate_search_space(
        cls, value: object
    ) -> dict[str, tuple[JsonValue, ...]]:
        """校验有限、确定性的字段路径搜索空间。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if not isinstance(value, dict):
            raise TypeError("parameter_search_space must be a mapping")
        if not value:
            raise ValueError("parameter_search_space must not be empty")
        normalized: dict[str, tuple[JsonValue, ...]] = {}
        if any(not isinstance(path, str) for path in value):
            raise TypeError("search paths must be strings")
        for path in sorted(value):
            if not path or path.startswith(".") or path.endswith("."):
                raise ValueError("search path must be a dotted field path")
            if any(not part or not part.replace("_", "a").isalnum() for part in path.split(".")):
                raise ValueError(f"invalid search path: {path}")
            if path == "research_protocol" or path.startswith("research_protocol."):
                raise ValueError("parameter search cannot modify research_protocol")
            raw_candidates = value[path]
            if not isinstance(raw_candidates, (list, tuple)) or not raw_candidates:
                raise ValueError(f"search candidates must not be empty: {path}")
            candidates = tuple(raw_candidates)
            for candidate in candidates:
                canonical_json_bytes(candidate)
            normalized[path] = tuple(candidates)
        return normalized

    @model_validator(mode="after")
    def validate_periods(self) -> ResearchProtocol:
        """确保三个样本段严格有序且互不重叠。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if not self.train.end < self.validation.start:
            raise ValueError("TRAIN must end before VALIDATION starts")
        if not self.validation.end < self.test.start:
            raise ValueError("VALIDATION must end before TEST starts")
        return self


class ResearchFamilyConfig(_FrozenModel):
    """定义一个不可变研究族及其完整组件组合。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    name: str = Field(min_length=1, max_length=128)
    hypothesis: str = Field(min_length=1, max_length=4000)
    research_mode: ResearchMode
    strategy_id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    benchmark: str
    initial_cash_fen: int = Field(gt=0)
    research_protocol: ResearchProtocol
    universe: dict[str, JsonValue]
    features: dict[str, JsonValue]
    signal: dict[str, JsonValue]
    decision_schedule: dict[str, JsonValue]
    risk: dict[str, JsonValue]
    pretrade_cost: dict[str, JsonValue]
    portfolio: dict[str, JsonValue]
    rebalance: dict[str, JsonValue]
    execution: dict[str, JsonValue]
    analytics: dict[str, JsonValue]

    @field_validator("research_mode", mode="before")
    @classmethod
    def parse_research_mode(cls, value: object) -> ResearchMode:
        """把 YAML 枚举文本转换为严格研究模式。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return value if isinstance(value, ResearchMode) else ResearchMode(cast(str, value))

    @field_validator("benchmark")
    @classmethod
    def validate_benchmark(cls, value: str) -> str:
        """校验证券标识并返回规范文本。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        return InstrumentId.parse(value).canonical()

    @field_validator(
        "universe",
        "features",
        "signal",
        "decision_schedule",
        "risk",
        "pretrade_cost",
        "portfolio",
        "rebalance",
        "execution",
        "analytics",
    )
    @classmethod
    def validate_component_block(
        cls, value: dict[str, JsonValue]
    ) -> dict[str, JsonValue]:
        """确认组件配置是非空确定性 JSON。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if not value:
            raise ValueError("component configuration must not be empty")
        canonical_json_bytes(value)
        return value
