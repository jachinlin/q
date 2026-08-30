"""定义一次提交、一次执行的独立策略研究模型。"""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from quant_research.data.contracts import JsonValue
from quant_research.domain.identifiers import IndexId


class StrategyStudyStatus(StrEnum):
    """表示策略研究生命周期状态。入参：枚举值。返回值：状态成员。异常：非法值抛出值错误。"""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class StrategyStudyStage(StrEnum):
    """表示策略研究固定执行阶段。入参：阶段值。返回值：阶段成员。异常：非法值抛出值错误。"""

    VALIDATE = "VALIDATE"
    BACKTEST = "BACKTEST"
    ANALYTICS = "ANALYTICS"
    PUBLISH = "PUBLISH"


class _FrozenModel(BaseModel):
    """为策略研究跨边界模型提供严格、冻结的公共配置。"""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_by_alias=True,
        validate_by_name=True,
        serialize_by_alias=True,
    )

    @staticmethod
    def parse_date(value: object) -> date:
        """解析严格 ``YYYY-MM-DD`` 日期。"""

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


class StrategyConfig(_FrozenModel):
    """保存策略标识及严格参数。入参：策略 ID 与参数。返回值：冻结配置。异常：字段非法时校验失败。"""

    strategy_id: str = Field(min_length=1)
    parameters: dict[str, JsonValue]


class ExecutionSettings(_FrozenModel):
    """定义撮合价格、滑点和容量上限。入参：执行字段。返回值：冻结设置。异常：取值越界时校验失败。"""

    reference_price: str = "OPEN"
    slippage_bps: float = Field(default=0.0, ge=0)
    max_volume_participation: float = Field(default=0.1, gt=0, le=1)
    limit_order_policy: str = "REJECT"

    @model_validator(mode="after")
    def _supported_values(self) -> ExecutionSettings:
        if self.reference_price not in {"OPEN", "CLOSE"}:
            raise ValueError("reference_price must be OPEN or CLOSE")
        if self.limit_order_policy not in {"REJECT", "PARTIAL"}:
            raise ValueError("limit_order_policy must be REJECT or PARTIAL")
        return self


class StrategyStudyDefinition(_FrozenModel):
    """定义一次执行的策略研究。入参：研究配置字段。返回值：冻结定义。异常：日期、标签或标识非法时校验失败。"""

    name: str = Field(min_length=1, max_length=128)
    description: str = Field(default="", max_length=4000)
    tags: tuple[str, ...] = ()
    start_date: date
    end_date: date
    strategy: StrategyConfig
    benchmark: IndexId
    initial_cash_fen: int = Field(gt=0)
    execution: ExecutionSettings

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def _date(cls, value: object) -> date:
        return _FrozenModel.parse_date(value)

    @field_validator("benchmark", mode="before")
    @classmethod
    def _benchmark(cls, value: object) -> IndexId:
        if isinstance(value, IndexId):
            return value
        if isinstance(value, str):
            return IndexId.parse(value)
        raise TypeError("benchmark must be an IndexId or canonical index code")

    @field_serializer("benchmark")
    def _serialize_benchmark(self, value: IndexId) -> str:
        return value.canonical()

    @model_validator(mode="after")
    def _validate_definition(self) -> StrategyStudyDefinition:
        if self.start_date > self.end_date:
            raise ValueError("start_date must not follow end_date")
        if tuple(sorted(set(self.tags))) != self.tags:
            raise ValueError("tags must be unique and sorted")
        return self


class StrategyStudyMetricRecord(_FrozenModel):
    """表示登记的标量指标。入参：名称、值和单位。返回值：冻结指标。异常：字段非法时校验失败。"""

    name: str
    value: float
    unit: str | None


class StrategyStudyArtifactRecord(_FrozenModel):
    """表示可信 Manifest 产物。入参：路径和完整性字段。返回值：冻结产物记录。异常：字段非法时校验失败。"""

    artifact_type: str
    relative_path: str
    content_hash: str
    byte_count: int
    row_count: int | None
    artifact_schema: dict[str, str] | None = Field(alias="schema")


class StrategyStudyRecord(_FrozenModel):
    """汇总研究完整快照。入参：身份、生命周期和输出字段。返回值：冻结快照。异常：字段非法时校验失败。"""

    id: str
    definition: StrategyStudyDefinition
    config_hash: str
    catalog_hash: str
    status: StrategyStudyStatus
    stage: StrategyStudyStage
    task_id: str
    artifact_dir: str | None
    manifest_hash: str | None
    error: dict[str, JsonValue] | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    metrics: tuple[StrategyStudyMetricRecord, ...] = ()
    artifacts: tuple[StrategyStudyArtifactRecord, ...] = ()


STRATEGY_STUDY_STAGES = (
    StrategyStudyStage.VALIDATE,
    StrategyStudyStage.BACKTEST,
    StrategyStudyStage.ANALYTICS,
    StrategyStudyStage.PUBLISH,
)


__all__ = [
    "STRATEGY_STUDY_STAGES",
    "ExecutionSettings",
    "StrategyConfig",
    "StrategyStudyArtifactRecord",
    "StrategyStudyDefinition",
    "StrategyStudyMetricRecord",
    "StrategyStudyRecord",
    "StrategyStudyStage",
    "StrategyStudyStatus",
]
