"""提供研究界面与领域模型相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from quant_research.domain.enums import DatasetKind
from quant_research.experiments.models import ResearchMark
from quant_research.factor_studies.models import (
    FactorStudyConfig,
    FactorStudyIndustryConfig,
)


class DashboardModel(BaseModel):
    """返回完成字段规范化和不变量校验的对象。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class NotebookStatusResponse(DashboardModel):
    """返回 Dashboard 内嵌 Notebook 的本机就绪状态。

    入参：
        status：JupyterLab 当前是否已经可以接受请求。
    返回值：
        构造并返回不可变的 Notebook 状态响应。
    异常：
        状态不属于约定枚举时由 Pydantic 抛出校验异常。
    """

    status: Literal["READY", "UNAVAILABLE"]


class MarketReviewDates(DashboardModel):
    """描述市场全景可选交易日。

    入参：目录身份、验证时间、最新交易日与升序交易日集合。
    返回值：构造并返回不可变的日期目录 DTO。
    异常：字段类型或必填约束不合法时由 Pydantic 抛出校验异常。
    """

    catalog_hash: str
    validated_at: datetime
    latest_trade_date: date
    dates: tuple[date, ...]


class MarketReviewDataQuality(DashboardModel):
    """描述市场全景横截面的数据质量与股票口径。

    入参：股票数量、行情覆盖数量、停牌和风险警示统计。
    返回值：构造并返回不可变的数据质量 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    expected_count: int
    priced_count: int
    suspended_count: int
    st_count: int
    missing_bar_count: int
    coverage_rate: float | None


class MarketReviewSeriesPoint(DashboardModel):
    """描述市场全景时间序列中的一个日期和值。

    入参：交易日、主值与可选辅助值。
    返回值：构造并返回不可变的序列点 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    trade_date: date
    value: float
    auxiliary: float | None = None


class MarketReviewIndex(DashboardModel):
    """描述一个核心指数的当日状态和近期走势。

    入参：指数身份、收益、振幅及归一化收盘序列。
    返回值：构造并返回不可变的指数 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    index_id: str
    name: str
    daily_return: float | None
    amplitude: float | None
    return_5d: float | None
    return_20d: float | None
    series: tuple[MarketReviewSeriesPoint, ...]


class MarketReviewLiquidity(DashboardModel):
    """描述全市场成交额及其近期参照。

    入参：当日成交额、比较值、均值、分位与历史序列。
    返回值：构造并返回不可变的成交 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    amount: float
    change_vs_previous: float | None
    average_5d: float | None
    average_20d: float | None
    percentile_20d: float | None
    series: tuple[MarketReviewSeriesPoint, ...]


class MarketReviewBucket(DashboardModel):
    """描述收益分布的一个固定区间。

    入参：区间标签与股票数量。
    返回值：构造并返回不可变的分桶 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    label: str
    count: int


class MarketReviewBreadth(DashboardModel):
    """描述当日市场广度与横截面收益分布。

    入参：涨跌家数、集中趋势、分位数及固定分桶。
    返回值：构造并返回不可变的广度 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    up_count: int
    down_count: int
    flat_count: int
    advance_rate: float | None
    net_advance_count: int
    equal_weight_return: float | None
    median_return: float | None
    p10_return: float | None
    p25_return: float | None
    p75_return: float | None
    p90_return: float | None
    buckets: tuple[MarketReviewBucket, ...]


class MarketReviewLimitEvent(DashboardModel):
    """描述一个按规则识别的涨跌停事件。

    入参：证券身份、名称、板块、风险状态、收益、成交额和事件类型。
    返回值：构造并返回不可变的涨跌停事件 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    instrument_id: str
    name: str
    board: str
    is_st: bool
    pct_change: float
    amount: float | None
    event: Literal["LIMIT_UP", "LIMIT_DOWN", "BROKEN_LIMIT_UP", "ONE_PRICE_LIMIT_UP"]


class MarketReviewSentiment(DashboardModel):
    """描述涨跌停情绪及规则覆盖情况。

    入参：各事件数量、规则覆盖数量、覆盖率与事件明细。
    返回值：构造并返回不可变的情绪 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    limit_up_count: int
    limit_down_count: int
    broken_limit_up_count: int
    one_price_limit_up_count: int
    eligible_count: int
    unresolved_count: int
    coverage_rate: float | None
    events: tuple[MarketReviewLimitEvent, ...]
    note: str


class MarketReviewIndustry(DashboardModel):
    """描述一个行业的行情结构。

    入参：行业身份、收益、上涨率、成交占比、覆盖数量与涨跌停数量。
    返回值：构造并返回不可变的行业 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    industry_code: str
    industry_name: str
    equal_weight_return: float | None
    advance_rate: float | None
    amount_share: float | None
    instrument_count: int
    priced_count: int
    limit_up_count: int
    limit_down_count: int


class MarketReviewIndustries(DashboardModel):
    """描述行业板块是否可用及其聚合结果。

    入参：可用状态、分类体系、覆盖率、原因和行业集合。
    返回值：构造并返回不可变的行业板块 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    available: bool
    taxonomy: str | None
    coverage_rate: float | None
    unavailable_reason: str | None
    items: tuple[MarketReviewIndustry, ...]


class MarketReviewValuationMetric(DashboardModel):
    """描述一个估值字段的有效横截面分布。

    入参：指标标识、中位数、四分位数及样本数量。
    返回值：构造并返回不可变的估值指标 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    metric: Literal["pe_ttm", "pb_mrq", "ps_ttm"]
    median: float | None
    p25: float | None
    p75: float | None
    valid_count: int


class MarketReviewValuation(DashboardModel):
    """描述市场估值与换手率快照。

    入参：三个估值分布及换手率中位数和样本数。
    返回值：构造并返回不可变的估值 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    metrics: tuple[MarketReviewValuationMetric, ...]
    turnover_median: float | None
    turnover_valid_count: int


class MarketReviewResponse(DashboardModel):
    """描述一个交易日完整、可追溯的市场全景。

    入参：数据身份、股票口径及各市场全景模块。
    返回值：构造并返回不可变的市场全景响应 DTO。
    异常：字段类型不合法时由 Pydantic 抛出校验异常。
    """

    trade_date: date
    catalog_hash: str
    validated_at: datetime
    exclude_st: bool
    data_quality: MarketReviewDataQuality
    indexes: tuple[MarketReviewIndex, ...]
    liquidity: MarketReviewLiquidity
    breadth: MarketReviewBreadth
    sentiment: MarketReviewSentiment
    industries: MarketReviewIndustries
    valuation: MarketReviewValuation


class DataUpdatePlanRequest(DashboardModel):
    """封装数据更新计划预览所需的可选显式日期区间。

    入参：
        start：处理区间的开始日期，类型为 ``date | None``。
        end：处理区间的结束日期，类型为 ``date | None``。
        datasets：可选的非空、无重复数据集集合；省略表示全部。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    start: date | None = None
    end: date | None = None
    datasets: tuple[DatasetKind, ...] | None = None

    @field_validator("start", "end", mode="before")
    @classmethod
    def parse_date(cls, value: object) -> object:
        """解析并校验输入值。

        入参：
            value：待校验或转换的值，类型为 ``object``。
        返回值：
            返回解析并校验日期后的日期（``object``）。
        异常：
            无。
        """
        if isinstance(value, str):
            return date.fromisoformat(value)
        return value

    @field_validator("datasets", mode="before")
    @classmethod
    def parse_datasets(cls, value: object) -> object:
        """把 JSON 数据集名称数组显式解析为领域枚举元组。

        入参：待解析的请求字段。返回值：枚举元组、空值或交由 Pydantic 继续校验的原值。
        异常：数据集数组为空、重复或包含非法成员时抛出 ``ValueError``。
        """
        if value is None:
            return None
        if not isinstance(value, (list, tuple)):
            return value
        datasets = tuple(
            DatasetKind(item) if isinstance(item, str) else item for item in value
        )
        if any(not isinstance(item, DatasetKind) for item in datasets):
            raise ValueError("datasets must contain dataset names")
        if not datasets:
            raise ValueError("datasets must not be empty")
        if len(set(datasets)) != len(datasets):
            raise ValueError("datasets must be unique")
        return tuple(sorted(datasets, key=lambda item: item.value))

    @model_validator(mode="after")
    def validate_window(self) -> DataUpdatePlanRequest:
        """校验窗口。

        入参：
            无。
        返回值：
            返回校验窗口后的窗口（``DataUpdatePlanRequest``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if (self.start is None) != (self.end is None):
            raise ValueError("start and end must be supplied together")
        if self.start is not None and self.end is not None and self.start > self.end:
            raise ValueError("start must not follow end")
        return self


class DataUpdateRequest(DataUpdatePlanRequest):
    """提交一份已由用户确认且未过期的数据更新计划。

    入参：可选完整日期区间和 64 位小写十六进制计划 hash。
    返回值：构造冻结请求。异常：日期或 hash 不满足约束时抛出校验异常。
    """

    plan_hash: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")


class QualityRunRequest(DashboardModel):
    """创建一次全目录或单数据集后台质量运行。

    入参：
        dataset：省略时执行全目录 ``validate-all``；提供时仅诊断该数据集。
    返回值：
        构造冻结且严格校验的数据质量运行请求。
    异常：
        ValueError：数据集名称不属于当前领域枚举时抛出。
    """

    dataset: DatasetKind | None = None

    @field_validator("dataset", mode="before")
    @classmethod
    def parse_dataset(cls, value: object) -> object:
        """把可选 JSON 数据集名称解析为领域枚举。

        入参：
            value：待解析的数据集名称、领域枚举或空值。
        返回值：
            返回解析后的 ``DatasetKind``、空值或交由 Pydantic 校验的原值。
        异常：
            ``ValueError``：字符串不是已注册的数据集名称时抛出。
        """
        if value is None or isinstance(value, DatasetKind):
            return value
        if isinstance(value, str):
            return DatasetKind(value)
        return value


class FreshnessResponse(DashboardModel):
    """表示单个数据集的新鲜度判断及其水位证据。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    status: Literal["CURRENT", "STALE", "MISSING", "UNKNOWN"]
    actual_watermark: str | None
    expected_watermark: str | None
    lag_days: int | None
    evaluated_at: str
    reason: str


class OperationalStateResponse(DashboardModel):
    """表示数据集最近成功执行各流水线阶段的运营状态。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    last_localized_at: str | None
    localized_through: str | None
    last_curated_at: str | None
    last_validated_at: str | None


class DatasetResponse(DashboardModel):
    """返回不含物理路径的数据资产摘要。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    dataset: str
    source: str | None
    start_date: str | None
    end_date: str | None
    partition_count: int
    row_count: int
    content_hash: str | None
    updated_at: str | None
    partitioning: str
    cadence: str
    fetch_granularity: str
    reuse: str
    overlap_days: int
    freshness: FreshnessResponse
    operational: OperationalStateResponse
    quality_issue_count: int
    blocking_issue_count: int


class DatasetListResponse(DashboardModel):
    """返回数据目录中所有资产摘要。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    items: tuple[DatasetResponse, ...]


class DatasetContractResponse(DashboardModel):
    """返回数据集目录、Schema 与供应端点契约。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    partitioning: str
    fetch_granularity: str
    cadence: str
    reuse: str
    overlap_days: int
    primary_key: tuple[str, ...]
    sort_key: tuple[str, ...]
    pit_fields: tuple[str, ...]
    schema_fields: tuple[dict[str, str], ...] = Field(alias="schema")
    sources: tuple[dict[str, object], ...]


class DatasetDetailResponse(DatasetResponse):
    """返回单个数据集完整的可公开审计详情。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    contract: DatasetContractResponse
    partitions: tuple[dict[str, object], ...]


class QualityRunResponse(DashboardModel):
    """返回一次质量运行的摘要证据。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    run_id: str
    scope: str
    input_hash: str
    status: str
    started_at: str
    completed_at: str | None
    issue_count: int
    blocking_issue_count: int


class QualityRunListResponse(DashboardModel):
    """返回经过筛选和分页的质量运行历史。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    items: tuple[QualityRunResponse, ...]
    page: int
    page_size: int
    total: int


class QualityRunDetailResponse(QualityRunResponse):
    """返回质量运行的完整问题字段与数据集哈希。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    dataset_hashes: dict[str, str]
    results_complete: bool
    result_counts: QualityRuleResultCountsResponse
    rule_results: tuple[QualityRuleResultResponse, ...]
    issues: tuple[QualityIssueResponse, ...]


class QualityIssueResponse(DashboardModel):
    """返回一项完整质量问题及修复证据。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    rule_id: str
    severity: str
    dataset: str
    scope: object
    actual: object
    threshold: object
    message: str
    remediation: str


class QualityRuleResultCountsResponse(DashboardModel):
    """返回四种质量规则结果状态的精确计数。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    PASS: int
    FAIL: int
    SKIPPED: int
    UNKNOWN: int


class QualityRuleResultResponse(DashboardModel):
    """返回规则与数据集组合的说明、状态和判断证据。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    rule_id: str
    dataset: str
    status: Literal["PASS", "FAIL", "SKIPPED", "UNKNOWN"]
    severity: str
    title: str
    description: str
    pass_criterion: str
    scope: object
    actual: object
    threshold: object
    skip_reason: str | None
    evidence: Literal["RUN_SNAPSHOT", "LEGACY_ISSUE", "MISSING"]
    issues: tuple[QualityIssueResponse, ...]


class TaskSummaryResponse(DashboardModel):
    """返回数据中心所需的活动或最近成功任务摘要。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    id: str
    experiment_id: str | None
    factor_run_id: str | None
    task_type: str
    status: str
    priority: int
    progress: dict[str, object]
    created_at: str
    started_at: str | None
    updated_at: str
    heartbeat_at: str | None
    completed_at: str | None
    worker_id: str | None
    error: dict[str, object] | None
    result: dict[str, object] | None


class GateResponse(DashboardModel):
    """返回研究读取门及其绑定证据。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    status: Literal["READY", "BLOCKED"]
    reason: str
    catalog_hash: str
    validated_catalog_hash: str | None
    quality_run_id: str | None
    updated_at: str
    validated_at: str | None


class FreshnessSummaryResponse(DashboardModel):
    """返回数据目录整体新鲜度汇总。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    status: Literal["CURRENT", "STALE", "MISSING", "UNKNOWN"]
    counts: dict[str, int]
    evaluated_at: str | None
    latest_complete_session: str | None


class WorkerHeartbeatResponse(DashboardModel):
    """返回最近一次本地 Worker 心跳。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    worker_id: str | None
    task_id: str
    task_status: str
    heartbeat_at: str | None


class DataSummaryResponse(DashboardModel):
    """返回数据中心顶部运营状态与研究门证据。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    gate: GateResponse
    freshness: FreshnessSummaryResponse
    gate_quality_run: QualityRunResponse | None
    latest_quality_run: QualityRunResponse | None
    active_update: TaskSummaryResponse | None
    last_successful_update: TaskSummaryResponse | None
    worker: WorkerHeartbeatResponse | None
    active_research_task_count: int


class TaskStatusCountsResponse(DashboardModel):
    """返回所有持久任务状态的全局计数。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    QUEUED: int
    RUNNING: int
    SUCCEEDED: int
    FAILED: int
    CANCEL_REQUESTED: int
    CANCELLED: int
    ORPHANED: int


class ExperimentStatusCountsResponse(DashboardModel):
    """返回所有实验状态的全局计数。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    CREATED: int
    QUEUED: int
    RUNNING: int
    SUCCEEDED: int
    FAILED: int
    CANCELLED: int


class ExperimentSummaryResponse(DashboardModel):
    """返回总览与登记册共用的实验摘要。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    id: str
    strategy_id: str
    status: str
    research_mark: str
    data_hash: str
    config_hash: str
    fingerprint: str
    created_at: str
    started_at: str | None
    completed_at: str | None
    tags: tuple[str, ...]
    metrics: dict[str, float | None]


class BenchmarkSummaryResponse(DashboardModel):
    """返回单个内置基准策略最近成功实验。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    strategy_id: Literal["etf_rotation", "stock_multifactor"]
    experiment: ExperimentSummaryResponse | None


class OverviewTasksResponse(DashboardModel):
    """返回研究工作台所需的全局任务状态与活动任务。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    status_counts: TaskStatusCountsResponse
    active: tuple[TaskSummaryResponse, ...]


class OverviewExperimentsResponse(DashboardModel):
    """返回研究工作台所需的实验状态、近期记录与基准结果。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    status_counts: ExperimentStatusCountsResponse
    recent: tuple[ExperimentSummaryResponse, ...]
    benchmarks: tuple[BenchmarkSummaryResponse, ...]


class OverviewResponse(DashboardModel):
    """返回研究工作台的就绪状态、运行证据和研究摘要。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    gate: GateResponse
    freshness: FreshnessSummaryResponse
    latest_trade_date: str | None
    dataset_count: int
    gate_quality_run: QualityRunResponse | None
    latest_quality_run: QualityRunResponse | None
    worker: WorkerHeartbeatResponse | None
    last_successful_update: TaskSummaryResponse | None
    tasks: OverviewTasksResponse
    experiments: OverviewExperimentsResponse


class ResearchUpdateRequest(DashboardModel):
    """定义一次研究工作台操作在进入用例边界前必须校验的输入。

    入参：
        mark：需要写入实验记录的研究标记。
        tags：参与本次处理的标签集合；调用方不得依赖未声明的顺序。
        note：不参与研究身份计算的可选人工备注。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    mark: ResearchMark
    tags: tuple[str, ...] = ()
    note: str = Field(max_length=16_384)


class ExperimentCloneRequest(DashboardModel):
    """定义一次研究工作台操作在进入用例边界前必须校验的输入。

    入参：
        submit：控制是否启用``submit``规则的布尔开关。
        priority：任务在同一可运行集合中的调度优先级。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    submit: bool = True
    priority: int = Field(default=0, ge=-100, le=100)


class ExperimentSubmitRequest(DashboardModel):
    """校验 Dashboard 从 YAML 文本提交新实验的请求。

    入参：
        config_yaml：用户提交的实验 YAML 原文；仅从受信配置根或内存文本解析。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    config_yaml: str = Field(min_length=1, max_length=1_048_576)

    @field_validator("config_yaml")
    @classmethod
    def validate_encoded_size(cls, value: str) -> str:
        """限制 UTF-8 实际负载不超过 1 MiB。

        入参：
            value：值。
        返回值：
            返回校验``encoded``字节数后的``encoded``字节数（``str``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if len(value.encode("utf-8")) > 1_048_576:
            raise ValueError("config_yaml exceeds the 1 MiB UTF-8 limit")
        return value


class RetryRequest(DashboardModel):
    """定义一次研究工作台操作在进入用例边界前必须校验的输入。

    入参：
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    confirm_orphaned: bool = False


class CompareRequest(DashboardModel):
    """定义一次研究工作台操作在进入用例边界前必须校验的输入。

    入参：
        experiment_ids：参与本次处理的实验``ids``；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    experiment_ids: tuple[str, ...] = Field(min_length=2, max_length=5)


class PanelAvailability(DashboardModel):
    """说明 Dashboard 面板是否可用以及不可用时的安全原因。

    入参：
        status：当前记录所处的受控生命周期状态。
        reason：原因。
        data：待处理的数据，类型为 ``list[dict[str, object]]``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    status: Literal["AVAILABLE", "UNAVAILABLE"]
    reason: str | None = None
    data: list[dict[str, object]] = Field(default_factory=list)


class FactorStudyCreateRequest(DashboardModel):
    """校验 Dashboard 创建独立因子研究的 JSON 请求。

    入参：
        name：供用户识别研究、任务或数据对象的非空名称。
        factor_refs：按规范 ``factor_id`` 指定的因子引用集合。
        start_date：查询或运行覆盖区间的首日（含）。
        end_date：查询或运行覆盖区间的末日（含）。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    name: str = Field(min_length=1, max_length=128)
    factor_refs: tuple[str, ...] = Field(min_length=1, max_length=7)
    start_date: date
    end_date: date
    industry: FactorStudyIndustryConfig | None = None

    @field_validator("factor_refs", mode="before")
    @classmethod
    def parse_factor_refs(cls, value: object) -> object:
        """将 JSON 因子数组转换为不可变元组。

        入参：
            value：待校验或转换的值，类型为 ``object``。
        返回值：
            返回解析并校验因子``refs``后的因子``refs``（``object``）。
        异常：
            无。
        """
        return tuple(value) if isinstance(value, list) else value

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def parse_dates(cls, value: object) -> object:
        """将 ISO 日期文本转换为日期对象。

        入参：
            value：待校验或转换的值，类型为 ``object``。
        返回值：
            返回解析并校验``dates``后的``dates``（``object``）。
        异常：
            无。
        """
        return date.fromisoformat(value) if isinstance(value, str) else value

    def config(self) -> FactorStudyConfig:
        """返回经过固定 MVP 契约校验的研究配置。

        入参：
            无。
        返回值：
            返回配置（``FactorStudyConfig``）。
        异常：
            无。
        """
        return FactorStudyConfig(
            factor_refs=self.factor_refs,
            start_date=self.start_date,
            end_date=self.end_date,
            industry=self.industry,
        )
