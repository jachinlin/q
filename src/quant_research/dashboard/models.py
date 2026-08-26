"""提供研究界面与领域模型相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from quant_research.domain.enums import DatasetKind


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

    metric: Literal["pe_ttm", "pb", "ps_ttm"]
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


class DataBootstrapRequest(DashboardModel):
    """提交首次 Canonical 基线初始化所需的历史覆盖年数。

    入参：
        years：从当前初始化窗口向前覆盖的正整数年数。
    返回值：
        构造冻结且严格校验的初始化请求。
    异常：
        年数小于一时由 Pydantic 抛出校验异常。
    """

    years: int = Field(ge=1)


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
    """表示单个数据集的新鲜度判断及其水位或披露触发证据。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    status: Literal["CURRENT", "STALE", "MISSING", "UNKNOWN"]
    actual_watermark: str | None
    expected_watermark: str | None
    lag_days: int | None
    evaluated_at: str
    reason: str
    trigger_date: str | None
    update_required: bool | None


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
    subject_kind: str | None
    subject_id: str | None
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


class DataInitializationResponse(DashboardModel):
    """返回首次 Canonical 基线初始化状态和冻结窗口。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    status: Literal["NOT_STARTED", "IN_PROGRESS", "COMPLETED"]
    years: int | None
    start_date: str | None
    end_date: str | None
    started_at: str | None
    completed_at: str | None


class DataSummaryResponse(DashboardModel):
    """返回数据中心顶部运营状态与研究门证据。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    initialization: DataInitializationResponse
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


class OverviewTasksResponse(DashboardModel):
    """返回研究工作台所需的全局任务状态与活动任务。

    入参：由字段声明给出。返回值：构造冻结响应对象。异常：字段非法时抛出校验异常。
    """

    status_counts: TaskStatusCountsResponse
    active: tuple[TaskSummaryResponse, ...]


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


class DataSourceTokenStatusResponse(DashboardModel):
    """返回数据源 Token 的非敏感配置状态。

    入参：是否已配置、解析来源和可选文件更新时间。
    返回值：构造不包含 Token 明文或派生片段的冻结响应。
    异常：来源或字段类型非法时由 Pydantic 抛出校验异常。
    """

    configured: bool
    source: Literal["DATA_ROOT_ENV", "PROCESS_ENVIRONMENT", "NONE"]
    updated_at: str | None


class DataSourceRateLimitStatusResponse(DashboardModel):
    """返回数据源每分钟请求上限及其配置来源。

    入参：严格限流整数、解析来源和可选文件更新时间。
    返回值：构造不含敏感信息的冻结响应。
    异常：来源或字段范围非法时由 Pydantic 抛出校验异常。
    """

    requests_per_minute: int = Field(ge=1, le=10_000)
    source: Literal["DATA_ROOT_ENV", "PROCESS_ENVIRONMENT", "DEFAULT"]
    updated_at: str | None


class DataSourceProxyStatusResponse(DashboardModel):
    """返回 Tushare 代理 URL 及其配置来源。

    入参：可选代理 URL、解析来源和可选文件更新时间。
    返回值：构造可供设置页展示的冻结响应。
    异常：字段类型或来源非法时由 Pydantic 抛出校验异常。
    """

    url: str | None
    source: Literal["DATA_ROOT_ENV", "PROCESS_ENVIRONMENT", "NONE"]
    updated_at: str | None


class DataSourceConcurrencyStatusResponse(DashboardModel):
    """返回 LOCALIZE 最大并发请求数及其配置来源。

    入参：并发数、解析来源和可选文件更新时间。
    返回值：构造可安全展示的冻结响应。
    异常：字段类型、范围或来源非法时由 Pydantic 抛出校验异常。
    """

    max_concurrent_requests: int = Field(ge=1, le=32)
    source: Literal["DATA_ROOT_ENV", "PROCESS_ENVIRONMENT", "DEFAULT"]
    updated_at: str | None


class DashboardSettingsResponse(DashboardModel):
    """返回 Dashboard 支持的通用设置安全投影。

    入参：受控设置文件路径、数据源 Token、请求限流和代理状态。
    返回值：构造冻结响应，任何字段均不得包含 Token 内容。
    异常：字段缺失或类型非法时由 Pydantic 抛出校验异常。
    """

    settings_path: str
    data_source_token: DataSourceTokenStatusResponse
    data_source_rate_limit: DataSourceRateLimitStatusResponse
    data_source_proxy: DataSourceProxyStatusResponse
    data_source_concurrency: DataSourceConcurrencyStatusResponse


class DataSourceTokenChangeRequest(DashboardModel):
    """定义数据源 Token 的类型化设置或清除操作。

    入参：操作名称和仅 ``SET`` 允许携带的明文值。
    返回值：构造经过组合校验的冻结修改请求。
    异常：操作和值组合不一致时抛出值错误。
    """

    operation: Literal["SET", "CLEAR"]
    value: str | None = Field(default=None, max_length=512)

    @model_validator(mode="after")
    def validate_operation(self) -> DataSourceTokenChangeRequest:
        """校验操作和值必须形成唯一有效组合。

        入参：无。
        返回值：校验后的当前请求对象。
        异常：``SET`` 缺值或 ``CLEAR`` 携带值时抛出值错误。
        """
        if self.operation == "SET" and self.value is None:
            raise ValueError("SET data source token operation requires value")
        if self.operation == "CLEAR" and self.value is not None:
            raise ValueError("CLEAR data source token operation must omit value")
        return self


class DataSourceRateLimitChangeRequest(DashboardModel):
    """定义数据源限流的设置或清除操作。

    入参：操作名称和仅 ``SET`` 允许携带的每分钟请求数。
    返回值：构造经过组合校验的冻结修改请求。
    异常：操作和值组合不一致或数值越界时抛出值错误。
    """

    operation: Literal["SET", "CLEAR"]
    requests_per_minute: int | None = Field(default=None, ge=1, le=10_000)

    @model_validator(mode="after")
    def validate_operation(self) -> DataSourceRateLimitChangeRequest:
        """校验 SET 必须带值且 CLEAR 必须省略值。

        入参：无。
        返回值：组合合法的当前请求模型。
        异常：操作和值不形成唯一合法组合时抛出值错误。
        """
        if self.operation == "SET" and self.requests_per_minute is None:
            raise ValueError("SET data source rate limit requires requests_per_minute")
        if self.operation == "CLEAR" and self.requests_per_minute is not None:
            raise ValueError("CLEAR data source rate limit must omit requests_per_minute")
        return self


class DataSourceProxyChangeRequest(DashboardModel):
    """定义 Tushare 代理 URL 的设置或清除操作。

    入参：操作名称和仅 ``SET`` 允许携带的 URL。
    返回值：构造经过组合校验的冻结修改请求。
    异常：操作和值组合不一致或 URL 过长时抛出值错误。
    """

    operation: Literal["SET", "CLEAR"]
    url: str | None = Field(default=None, max_length=2048)

    @model_validator(mode="after")
    def validate_operation(self) -> DataSourceProxyChangeRequest:
        """校验 SET 必须带 URL 且 CLEAR 必须省略 URL。

        入参：无。
        返回值：组合合法的当前请求模型。
        异常：操作和值不形成唯一合法组合时抛出值错误。
        """
        if self.operation == "SET" and self.url is None:
            raise ValueError("SET data source proxy requires a URL")
        if self.operation == "CLEAR" and self.url is not None:
            raise ValueError("CLEAR data source proxy must omit URL")
        return self


class DataSourceConcurrencyChangeRequest(DashboardModel):
    """定义 LOCALIZE 最大并发请求数的设置或清除操作。

    入参：操作名称和仅 ``SET`` 允许携带的并发数。
    返回值：构造经过组合校验的冻结修改请求。
    异常：操作和值组合不一致或范围非法时抛出值错误。
    """

    operation: Literal["SET", "CLEAR"]
    max_concurrent_requests: int | None = Field(default=None, ge=1, le=32)

    @model_validator(mode="after")
    def validate_operation(self) -> DataSourceConcurrencyChangeRequest:
        """校验 SET 必须带值且 CLEAR 必须省略值。

        入参：无。
        返回值：组合合法的当前请求模型。
        异常：操作和值不形成唯一合法组合时抛出值错误。
        """
        if self.operation == "SET" and self.max_concurrent_requests is None:
            raise ValueError(
                "SET data source concurrency requires max_concurrent_requests"
            )
        if self.operation == "CLEAR" and self.max_concurrent_requests is not None:
            raise ValueError(
                "CLEAR data source concurrency must omit max_concurrent_requests"
            )
        return self


class DashboardSettingsPatchRequest(DashboardModel):
    """定义可逐字段扩展的通用 Dashboard 设置修改请求。

    入参：可选的数据源 Token、请求限流与代理类型化修改。
    返回值：构造至少包含一个明确修改字段的冻结请求。
    异常：请求未包含任何修改时抛出值错误。
    """

    data_source_token: DataSourceTokenChangeRequest | None = None
    data_source_rate_limit: DataSourceRateLimitChangeRequest | None = None
    data_source_proxy: DataSourceProxyChangeRequest | None = None
    data_source_concurrency: DataSourceConcurrencyChangeRequest | None = None

    @model_validator(mode="after")
    def require_change(self) -> DashboardSettingsPatchRequest:
        """拒绝不会产生任何设置修改的空 PATCH。

        入参：无。
        返回值：包含至少一个修改的当前请求对象。
        异常：所有设置字段均省略时抛出值错误。
        """
        if (
            self.data_source_token is None
            and self.data_source_rate_limit is None
            and self.data_source_proxy is None
            and self.data_source_concurrency is None
        ):
            raise ValueError("settings patch must contain at least one change")
        return self


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
