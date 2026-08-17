"""定义独立因子研究的严格不可变契约。"""

from __future__ import annotations

from datetime import date
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from quant_research.factors.base import canonical_factor_ref

STOCK_FACTOR_REFS = (
    "book_to_price_mrq",
    "downside_volatility_60d",
    "earnings_yield_ttm",
    "max_drawdown_120d",
    "momentum_120_20",
    "roe_pit",
    "volatility_60d",
)
HORIZONS = (1, 5, 20)
QUANTILES = 5
MIN_CROSS_SECTION = 30
IC_ROLLING_WINDOW = 20
IC_ROLLING_MIN_VALID = 10
IC_QUANTILE_PROBABILITIES = (0.05, 0.25, 0.5, 0.75, 0.95)
INDUSTRY_TAXONOMY = "证监会行业分类"
INDUSTRY_UNCLASSIFIED_POLICIES = ("EXCLUDE", "UNCLASSIFIED")
DIRECTION_ADJUSTED = "DIRECTION_ADJUSTED"
INDUSTRY_NEUTRALIZED = "INDUSTRY_NEUTRALIZED"
SIGNAL_VARIANTS = (DIRECTION_ADJUSTED, INDUSTRY_NEUTRALIZED)


class FactorStudyIndustryConfig(BaseModel):
    """定义因子研究显式使用的 PIT 行业分类语义。

    入参：
        taxonomy：Canonical 行业分类体系。
        unclassified_policy：未分类和无历史状态样本的处理策略。
    返回值：
        构造并返回严格不可变的行业研究配置。
    异常：
        taxonomy 或未分类策略不受支持时抛出 ``ValueError``。
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    taxonomy: str = INDUSTRY_TAXONOMY
    unclassified_policy: str = "EXCLUDE"

    @model_validator(mode="after")
    def validate_contract(self) -> FactorStudyIndustryConfig:
        """确认当前唯一分类体系和未分类策略。

        入参：
            无。
        返回值：
            返回校验通过的行业研究配置。
        异常：
            分类体系或未分类策略不受支持时抛出 ``ValueError``。
        """
        if self.taxonomy != INDUSTRY_TAXONOMY:
            raise ValueError("unsupported factor-study industry taxonomy")
        if self.unclassified_policy not in INDUSTRY_UNCLASSIFIED_POLICIES:
            raise ValueError("unsupported factor-study unclassified policy")
        return self


class FactorRunStatus(StrEnum):
    """表示不可变因子运行的生命周期状态。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class FactorStudyConfig(BaseModel):
    """校验因子研究固定 MVP 参数并提供确定性序列化。

    入参：
        factor_refs：按规范 ``factor_id`` 指定的因子引用集合。
        start_date：查询或运行覆盖区间的首日（含）。
        end_date：查询或运行覆盖区间的末日（含）。
        horizons：参与本次处理的收益期限集合；调用方不得依赖未声明的顺序。
        quantiles：分位组数。
        返回完成字段规范化和不变量校验的对象。
        ic_rolling_window：IC滚动窗口。
        ic_rolling_min_valid：IC滚动下限有效样本。
        ic_quantile_probabilities：参与本次处理的``ic``分位组``probabilities``；调用方不得依赖未声明的顺序。
        universe：股票池。
        return_definition：收益``definition``。
        direction_adjusted：控制是否启用因子方向复权规则的布尔开关。
        industry：可选的 PIT 行业中性化研究配置。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    factor_refs: tuple[str, ...] = Field(min_length=1, max_length=7)
    start_date: date
    end_date: date
    horizons: tuple[int, ...] = HORIZONS
    quantiles: int = QUANTILES
    min_cross_section: int = MIN_CROSS_SECTION
    ic_rolling_window: int = IC_ROLLING_WINDOW
    ic_rolling_min_valid: int = IC_ROLLING_MIN_VALID
    ic_quantile_probabilities: tuple[float, ...] = IC_QUANTILE_PROBABILITIES
    universe: str = "CN_STOCK_STANDARD"
    return_definition: str = "T1_OPEN_TO_TH_CLOSE"
    direction_adjusted: bool = True
    industry: FactorStudyIndustryConfig | None = None

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def parse_date(cls, value: object) -> object:
        """将 Canonical JSON 日期文本解析为日期对象。

        入参：
            value：待校验或转换的值，类型为 ``object``。
        返回值：
            返回解析并校验日期后的日期（``object``）。
        异常：
            无。
        """
        return date.fromisoformat(value) if isinstance(value, str) else value

    @field_validator(
        "factor_refs", "horizons", "ic_quantile_probabilities", mode="before"
    )
    @classmethod
    def parse_tuples(cls, value: object) -> object:
        """接受 JSON 数组并保留不可变元组字段。

        入参：
            value：待校验或转换的值，类型为 ``object``。
        返回值：
            返回解析并校验``tuples``后的``tuples``（``object``）。
        异常：
            无。
        """
        return tuple(value) if isinstance(value, list) else value

    @field_validator("factor_refs")
    @classmethod
    def validate_factors(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """规范化因子 ID，并拒绝重复或不支持的股票因子。

        入参：
            value：待校验或转换的值，类型为 ``tuple[str, ...]``。
        返回值：
            返回校验因子集合后的因子集合（``tuple[str, ...]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        refs = tuple(sorted(canonical_factor_ref(item) for item in value))
        if len(set(refs)) != len(refs):
            raise ValueError("factor_refs must be unique")
        if not set(refs).issubset(STOCK_FACTOR_REFS):
            raise ValueError("factor_refs contains an unsupported stock factor")
        return refs

    @model_validator(mode="after")
    def validate_fixed_contract(self) -> FactorStudyConfig:
        """确认日期范围和 MVP 固定分析契约。

        入参：
            无。
        返回值：
            返回校验固定契约后的固定契约（``FactorStudyConfig``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if self.start_date > self.end_date:
            raise ValueError("start_date must not follow end_date")
        if self.horizons != HORIZONS or self.quantiles != QUANTILES:
            raise ValueError("MVP horizons and quantiles are fixed")
        if self.min_cross_section != MIN_CROSS_SECTION:
            raise ValueError("MVP minimum cross-section is fixed")
        if (
            self.ic_rolling_window != IC_ROLLING_WINDOW
            or self.ic_rolling_min_valid != IC_ROLLING_MIN_VALID
            or self.ic_quantile_probabilities != IC_QUANTILE_PROBABILITIES
        ):
            raise ValueError("MVP IC diagnostics contract is fixed")
        if (
            self.universe != "CN_STOCK_STANDARD"
            or self.return_definition != "T1_OPEN_TO_TH_CLOSE"
            or not self.direction_adjusted
        ):
            raise ValueError("MVP study contract is fixed")
        return self
