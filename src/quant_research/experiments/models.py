"""定义纯策略实验、运行、研究治理和持久化记录。"""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_serializer,
    field_validator,
    model_validator,
)

from quant_research.data.contracts import JsonValue
from quant_research.domain.enums import MultipleTestingMethod
from quant_research.domain.identifiers import IndexId


class RunStatus(StrEnum):
    """表示一次 Run 的受控生命周期。

    入参：Run 状态字符串。返回值：对应枚举成员。异常：未知值抛出 ``ValueError``。
    """

    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ResearchMark(StrEnum):
    """表示用户对 Run 的研究结论标记。

    入参：研究标记字符串。返回值：对应枚举成员。异常：未知值抛出 ``ValueError``。
    """

    UNREVIEWED = "UNREVIEWED"
    BASELINE = "BASELINE"
    CANDIDATE = "CANDIDATE"
    DISCARDED = "DISCARDED"


class RunStage(StrEnum):
    """列出策略实验编排器可能执行的阶段。

    入参：阶段字符串。返回值：对应枚举成员。异常：未知值抛出 ``ValueError``。
    """

    VALIDATE = "VALIDATE"
    PREPARE_INPUTS = "PREPARE_INPUTS"
    STRATEGY_RUN = "STRATEGY_RUN"
    ANALYTICS = "ANALYTICS"
    PERSIST = "PERSIST"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        validate_by_alias=True,
        validate_by_name=True,
        serialize_by_alias=True,
    )

    @staticmethod
    def _parse_date(value: object) -> date:
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


class DateWindow(_FrozenModel):
    """表示一个首尾均包含的非空样本窗口。

    入参：ISO 日期格式的起止日。返回值：冻结窗口。异常：日期非法或倒序时抛出值错误。
    """

    start: date
    end: date

    @field_validator("start", "end", mode="before")
    @classmethod
    def _date(cls, value: object) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                parsed = date.fromisoformat(value)
            except ValueError as error:
                raise ValueError("date must use YYYY-MM-DD") from error
            if parsed.isoformat() != value:
                raise ValueError("date must use YYYY-MM-DD")
            return parsed
        raise TypeError("date must use YYYY-MM-DD")

    @model_validator(mode="after")
    def _ordered(self) -> DateWindow:
        if self.start > self.end:
            raise ValueError("window start must not follow end")
        return self

    def overlaps(self, start: date, end: date) -> bool:
        """判断给定闭区间是否与本窗口相交。

        入参：待判断闭区间的起止日。返回值：相交时为真。异常：无。
        """
        return start <= self.end and end >= self.start


class SampleWindows(_FrozenModel):
    """固定 TRAIN、VALIDATION、TEST 三段互不重叠且有序的协议。

    入参：三个日期窗口。返回值：冻结样本协议。异常：窗口重叠或乱序时抛出值错误。
    """

    train: DateWindow
    validation: DateWindow
    test: DateWindow

    @model_validator(mode="after")
    def _ordered(self) -> SampleWindows:
        if not (
            self.train.end < self.validation.start
            and self.validation.end < self.test.start
        ):
            raise ValueError("sample windows must be ordered and non-overlapping")
        return self

    @property
    def start(self) -> date:
        """返回研究协议首日。

        入参：无。返回值：TRAIN 首日。异常：无。
        """
        return self.train.start

    @property
    def end(self) -> date:
        """返回研究协议末日。

        入参：无。返回值：TEST 末日。异常：无。
        """
        return self.test.end


class GovernanceConfig(_FrozenModel):
    """定义 TEST 使用预算和多重检验校正方法。

    入参：非负测试预算和校正方法。返回值：冻结治理配置。异常：预算非法时抛出校验错误。
    """

    test_budget: int = Field(ge=0)
    correction: MultipleTestingMethod


class StrategyConfig(_FrozenModel):
    """保存策略标识及由对应策略工厂严格解释的参数。

    入参：非空策略 ID 和参数映射。返回值：冻结策略配置。异常：字段缺失或额外时抛出校验错误。
    """

    strategy_id: str = Field(min_length=1)
    parameters: dict[str, JsonValue]


class ExecutionSettings(_FrozenModel):
    """定义未复权行情上的撮合价格、滑点和容量上限。

    入参：参考价、滑点、成交量参与率和涨跌停策略。返回值：冻结撮合配置。异常：数值越界时抛出校验错误。
    """

    reference_price: Literal["OPEN", "CLOSE"] = "OPEN"
    slippage_bps: float = Field(default=0.0, ge=0)
    max_volume_participation: float = Field(default=0.1, gt=0, le=1)
    limit_order_policy: Literal["REJECT", "PARTIAL"] = "REJECT"


class StrategyBacktestRunConfig(_FrozenModel):
    """定义策略回测 Run 的冻结业务输入。

    入参：日期、策略、基准、初始资金和撮合配置。返回值：冻结回测 Run。异常：日期或字段非法时抛出校验错误。
    """

    start_date: date
    end_date: date
    strategy: StrategyConfig
    benchmark: IndexId
    initial_cash_fen: int = Field(gt=0)
    execution: ExecutionSettings

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def _date(cls, value: object) -> date:
        return _FrozenModel._parse_date(value)

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
    def _range(self) -> StrategyBacktestRunConfig:
        if self.start_date > self.end_date:
            raise ValueError("run start_date must not follow end_date")
        return self


class ExperimentDefinition(_FrozenModel):
    """定义一个不可变研究问题及提交时立即运行的首个配置。

    入参：名称、描述、标签、样本协议、治理和首个策略 Run。返回值：冻结实验定义。异常：标签或区间不一致时抛出值错误。
    """

    name: str = Field(min_length=1, max_length=128)
    description: str = Field(default="", max_length=4000)
    tags: tuple[str, ...] = ()
    sample_windows: SampleWindows
    governance: GovernanceConfig
    initial_run: StrategyBacktestRunConfig

    @model_validator(mode="after")
    def _consistent(self) -> ExperimentDefinition:
        if (
            len(set(self.tags)) != len(self.tags)
            or tuple(sorted(self.tags)) != self.tags
        ):
            raise ValueError("tags must be unique and sorted")
        self.validate_run(self.initial_run)
        return self

    def validate_run(self, config: StrategyBacktestRunConfig) -> None:
        """校验派生策略 Run 与实验协议总区间一致。

        入参：待追加的策略 Run 配置。返回值：无。异常：日期越界时抛出 ``ValueError``。
        """
        if (
            config.start_date < self.sample_windows.start
            or config.end_date > self.sample_windows.end
        ):
            raise ValueError("run dates must stay inside the experiment protocol")

    def uses_test_region(self, config: StrategyBacktestRunConfig) -> bool:
        """返回 Run 是否触及锁定 TEST 区间。

        入参：已归属本实验的 Run 配置。返回值：相交时为真。异常：Run 不符合实验协议时抛出值错误。
        """
        self.validate_run(config)
        return self.sample_windows.test.overlaps(config.start_date, config.end_date)


class ExperimentRecord(_FrozenModel):
    """表示持久化后的实验定义摘要。

    入参：实验 ID、定义、baseline Run 和创建时间。返回值：冻结实验记录。异常：字段类型非法时抛出校验错误。
    """

    id: str
    definition: ExperimentDefinition
    baseline_run_id: str | None
    created_at: datetime


class RunMetricRecord(_FrozenModel):
    """表示一个已登记的 Run 标量指标和显著性结果。

    入参：指标名、数值、单位、原始和校正后 p-value。返回值：冻结指标记录。异常：字段类型非法时抛出校验错误。
    """

    name: str
    value: float
    unit: str | None
    p_value: float | None
    adjusted_p_value: float | None


class RunArtifactRecord(_FrozenModel):
    """表示可信 Manifest 中登记的一个 Run 产物。

    入参：产物类型、相对路径、内容哈希、字节数、行数和 Schema。返回值：冻结产物记录。异常：字段类型非法时抛出校验错误。
    """

    artifact_type: str
    relative_path: str
    content_hash: str
    byte_count: int
    row_count: int | None
    artifact_schema: dict[str, str] | None = Field(alias="schema")


class RunRecord(_FrozenModel):
    """表示一次不可变配置执行及其生命周期和审计字段。

    入参：Run 身份、配置、状态、产物与时间字段。返回值：冻结 Run 快照。异常：字段不符合严格模型时抛出校验错误。
    """

    id: str
    experiment_id: str
    config: StrategyBacktestRunConfig
    config_hash: str
    catalog_hash: str
    status: RunStatus
    stage: RunStage
    research_mark: ResearchMark
    uses_test_region: bool
    task_id: str | None
    artifact_dir: str | None
    manifest_hash: str | None
    error: dict[str, object] | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    tags: tuple[str, ...] = ()
    metrics: tuple[RunMetricRecord, ...] = ()
    artifacts: tuple[RunArtifactRecord, ...] = ()


class ExperimentAggregate(_FrozenModel):
    """汇总实验定义、全部 Run 和稳定标签。

    入参：实验记录、Run 元组和标签元组。返回值：冻结聚合快照。异常：字段类型非法时抛出校验错误。
    """

    experiment: ExperimentRecord
    runs: tuple[RunRecord, ...]
    tags: tuple[str, ...]


STRATEGY_STAGES = (
    RunStage.VALIDATE,
    RunStage.PREPARE_INPUTS,
    RunStage.STRATEGY_RUN,
    RunStage.ANALYTICS,
    RunStage.PERSIST,
)
__all__ = [
    "STRATEGY_STAGES",
    "ExperimentAggregate",
    "ExperimentDefinition",
    "ExperimentRecord",
    "GovernanceConfig",
    "MultipleTestingMethod",
    "ResearchMark",
    "RunArtifactRecord",
    "RunMetricRecord",
    "RunRecord",
    "RunStage",
    "RunStatus",
    "SampleWindows",
    "StrategyBacktestRunConfig",
]
