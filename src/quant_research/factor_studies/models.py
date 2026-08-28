"""定义独立因子研究、执行状态、产物和人工结论模型。"""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from quant_research.data.contracts import JsonValue
from quant_research.domain.enums import MultipleTestingMethod


class FactorStudyStatus(StrEnum):
    """表示研究生命周期状态。入参：枚举值。返回值：状态成员。异常：值未知时抛出值错误。"""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class FactorStudyStage(StrEnum):
    """表示固定执行阶段。入参：枚举值。返回值：阶段成员。异常：值未知时抛出值错误。"""

    VALIDATE = "VALIDATE"
    PREPARE_INPUTS = "PREPARE_INPUTS"
    ANALYZE_FACTORS = "ANALYZE_FACTORS"
    PUBLISH = "PUBLISH"


class FactorDecisionMark(StrEnum):
    """表示人工结论。入参：枚举值。返回值：结论成员。异常：值未知时抛出值错误。"""

    UNREVIEWED = "UNREVIEWED"
    CANDIDATE = "CANDIDATE"
    DISCARDED = "DISCARDED"


class IndustryUnclassifiedPolicy(StrEnum):
    """定义未分类策略。入参：枚举值。返回值：策略成员。异常：值未知时抛出值错误。"""

    EXCLUDE = "EXCLUDE"
    UNCLASSIFIED = "UNCLASSIFIED"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @staticmethod
    def parse_date(value: object) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                parsed = date.fromisoformat(value)
            except ValueError as error:
                raise ValueError("date must use YYYY-MM-DD") from error
            if parsed.isoformat() == value:
                return parsed
        raise ValueError("date must use YYYY-MM-DD")


class FactorStudyUniverse(_FrozenModel):
    """定义 PIT 股票池。入参：股票池名称。返回值：冻结股票池。异常：名称非法时校验失败。"""

    name: Literal["CN_STOCK_STANDARD"]


class FactorIndustrySettings(_FrozenModel):
    """定义 PIT 行业口径。入参：分类法和缺失策略。返回值：冻结设置。异常：字段非法时校验失败。"""

    taxonomy: Literal["SW2021"]
    unclassified_policy: IndustryUnclassifiedPolicy


class FactorMarketCapSettings(_FrozenModel):
    """定义市值中性化暴露。入参：固定对数总市值口径。返回值：冻结设置。异常：口径非法时校验失败。"""

    exposure: Literal["LOG_TOTAL_MARKET_VALUE"]


class FactorStudyDefinition(_FrozenModel):
    """定义一次不可变、提交后仅执行一次的因子研究。

    入参：研究身份、日期、校正方法、因子、股票池和分析参数。
    返回值：冻结定义。异常：字段、日期或确定性顺序非法时抛出校验错误。
    """

    name: str = Field(min_length=1, max_length=128)
    description: str = Field(default="", max_length=4000)
    tags: tuple[str, ...] = ()
    start_date: date
    end_date: date
    correction: MultipleTestingMethod
    factor_ids: tuple[str, ...] = Field(min_length=1)
    universe: FactorStudyUniverse
    horizons: tuple[int, ...] = Field(min_length=1)
    quantiles: int = Field(default=5, ge=2)
    industry: FactorIndustrySettings | None = None
    market_cap: FactorMarketCapSettings | None = None
    cost_bps_scenarios: tuple[int, ...] = (5, 10, 20)

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def _date(cls, value: object) -> date:
        return _FrozenModel.parse_date(value)

    @model_validator(mode="after")
    def _validate_definition(self) -> FactorStudyDefinition:
        if self.start_date > self.end_date:
            raise ValueError("start_date must not follow end_date")
        if tuple(sorted(set(self.tags))) != self.tags:
            raise ValueError("tags must be unique and sorted")
        if len(set(self.factor_ids)) != len(self.factor_ids) or any(
            not value for value in self.factor_ids
        ):
            raise ValueError("factor_ids must be unique and nonempty")
        if tuple(sorted(set(self.horizons))) != self.horizons or any(
            type(value) is not int or value <= 0 for value in self.horizons
        ):
            raise ValueError(
                "horizons must be unique positive integers in ascending order"
            )
        if (
            not self.cost_bps_scenarios
            or tuple(sorted(set(self.cost_bps_scenarios)))
            != self.cost_bps_scenarios
            or any(
                type(value) is not int or value < 0
                for value in self.cost_bps_scenarios
            )
        ):
            raise ValueError(
                "cost_bps_scenarios must be unique nonnegative integers in ascending order"
            )
        return self


class FactorStudyMetricRecord(_FrozenModel):
    """表示标量指标。入参：数值、单位和显著性。返回值：冻结指标。异常：字段非法时校验失败。"""

    name: str
    value: float
    unit: str | None
    p_value: float | None
    adjusted_p_value: float | None


class FactorStudyArtifactRecord(_FrozenModel):
    """表示可信产物。入参：路径、哈希和表证据。返回值：冻结产物。异常：字段非法时校验失败。"""

    artifact_type: str
    relative_path: str
    content_hash: str
    byte_count: int
    row_count: int | None
    artifact_schema: dict[str, str] | None = Field(alias="schema")


class FactorStudyDecisionKey(_FrozenModel):
    """标识决策矩阵行。入参：四维键。返回值：冻结决策键。异常：维度非法时校验失败。"""

    signal_variant: str = Field(min_length=1)
    label_kind: str = Field(min_length=1)
    factor_ref: str = Field(min_length=1)
    horizon: int = Field(gt=0)


class FactorStudyDecisionRecord(FactorStudyDecisionKey):
    """保存人工结论。入参：决策键和审计内容。返回值：冻结记录。异常：字段非法时校验失败。"""

    mark: FactorDecisionMark
    note: str = Field(default="", max_length=4000)
    actor: str
    updated_at: datetime


class FactorStudyRecord(_FrozenModel):
    """表示研究聚合。入参：定义、状态和发布证据。返回值：冻结快照。异常：字段非法时校验失败。"""

    id: str
    definition: FactorStudyDefinition
    config_hash: str
    catalog_hash: str
    status: FactorStudyStatus
    stage: FactorStudyStage
    task_id: str
    artifact_dir: str | None
    manifest_hash: str | None
    error: dict[str, JsonValue] | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    metrics: tuple[FactorStudyMetricRecord, ...] = ()
    artifacts: tuple[FactorStudyArtifactRecord, ...] = ()
    decisions: tuple[FactorStudyDecisionRecord, ...] = ()


FACTOR_STUDY_STAGES = (
    FactorStudyStage.VALIDATE,
    FactorStudyStage.PREPARE_INPUTS,
    FactorStudyStage.ANALYZE_FACTORS,
    FactorStudyStage.PUBLISH,
)


__all__ = [
    "FACTOR_STUDY_STAGES",
    "FactorDecisionMark",
    "FactorIndustrySettings",
    "FactorMarketCapSettings",
    "FactorStudyArtifactRecord",
    "FactorStudyDecisionKey",
    "FactorStudyDecisionRecord",
    "FactorStudyDefinition",
    "FactorStudyMetricRecord",
    "FactorStudyRecord",
    "FactorStudyStage",
    "FactorStudyStatus",
    "FactorStudyUniverse",
    "IndustryUnclassifiedPolicy",
]
