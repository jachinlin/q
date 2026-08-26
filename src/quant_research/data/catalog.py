"""定义 Canonical 数据集的唯一可执行目录及增量语义。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS, CanonicalSchema
from quant_research.domain.enums import DatasetKind


class Partitioning(StrEnum):
    """枚举 Canonical 数据集的物理分区方式。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``Partitioning`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    YEAR = "year"
    REPORT_YEAR = "report_year"
    ALL = "all"


class FetchGranularity(StrEnum):
    """枚举供应商请求的最小抓取粒度。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``FetchGranularity`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    MARKET_TRADE_DATE = "market_trade_date"
    DATE_RANGE = "date_range"
    MARKET_SNAPSHOT = "market_snapshot"
    REPORT_PERIOD = "report_period"
    INDUSTRY_L1 = "industry_l1"
    INDEX_RANGE_EXCEPTION = "index_range_exception"


class FetchPlan(StrEnum):
    """枚举 LOCALIZE 阶段的数据集抓取编排策略。

    入参：按枚举值构造。返回值：返回枚举成员。异常：非法值抛出 ``ValueError``。
    """

    MARKET_SNAPSHOT = "market_snapshot"
    TRADE_CALENDAR_RANGE = "trade_calendar_range"
    MARKET_TRADE_DATE = "market_trade_date"
    INDEX_RANGE_EXCEPTION = "index_range_exception"
    REPORT_PERIOD = "report_period"
    INDUSTRY_L1 = "industry_l1"


class UpdateCadence(StrEnum):
    """枚举数据集的计划更新频率。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``UpdateCadence`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    DAILY = "daily"
    WEEKLY = "weekly"
    QUARTERLY_DISCLOSURE = "quarterly_disclosure"


class FreshnessBasis(StrEnum):
    """枚举数据集新鲜度使用的业务判定基础。

    入参：按枚举值构造。返回值：返回枚举成员。异常：非法值抛出 ValueError。
    """

    TRADING_SESSION = "trading_session"
    CALENDAR_HORIZON = "calendar_horizon"
    SUCCESSFUL_REFRESH = "successful_refresh"
    DISCLOSURE_DEADLINE = "disclosure_deadline"


@dataclass(frozen=True, slots=True)
class FreshnessPolicy:
    """声明数据集新鲜度的确定性判定策略。

    入参：
        basis：判定所依据的业务语义。
        watermark_field：用于内容水位的 Canonical 日期列；刷新型策略为 ``None``。
        tolerance_days：允许的自然日延迟或要求的未来日历覆盖天数。
    返回值：
        构造并返回不可变策略。
    异常：
        ValueError：字段组合不满足策略契约时抛出。
    """

    basis: FreshnessBasis
    watermark_field: str | None
    tolerance_days: int

    def __post_init__(self) -> None:
        if self.tolerance_days < 0:
            raise ValueError("freshness tolerance_days must not be negative")
        if self.basis in {
            FreshnessBasis.SUCCESSFUL_REFRESH,
            FreshnessBasis.DISCLOSURE_DEADLINE,
        }:
            if self.watermark_field is not None:
                raise ValueError(
                    "refresh and disclosure freshness must not declare a watermark field"
                )
        elif not self.watermark_field:
            raise ValueError("watermark freshness requires a field")


class ReuseSemantics(StrEnum):
    """枚举 Curate 阶段允许采用的增量复用语义。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``ReuseSemantics`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    APPEND_ONLY = "append_only"
    APPEND_WITH_TAIL_REVISION = "append_with_tail_revision"
    APPEND_WITH_RESTATEMENT = "append_with_restatement"
    FULL_REFRESH = "full_refresh"


@dataclass(frozen=True, slots=True)
class EndpointMapping:
    """描述一个供应商端点向 Canonical 数据集贡献的字段。

    入参：
        endpoint：供应商原生端点名称。
        field_map：供应商原生字段名到 Canonical 字段名的映射。
    返回值：
        构造并返回 ``EndpointMapping`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    endpoint: str
    field_map: Mapping[str, str]
    def __post_init__(self) -> None:
        if not self.endpoint:
            raise ValueError("endpoint must not be empty")
        object.__setattr__(self, "field_map", MappingProxyType(dict(self.field_map)))


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """描述一个 Canonical 数据集的完整运行契约。

    入参：
        kind：Canonical 数据集枚举值。
        schema：数据集必须满足的列、类型、主键和排序键契约。
        partitioning：Canonical 文件采用的物理分区方式。
        pit_fields：研究读取和质量校验依赖的 PIT 审计字段。
        fetch_granularity：供应商请求能够独立执行和复用的最小粒度。
        fetch_plan：LOCALIZE 阶段用于委派抓取编排的策略标识。
        cadence：计划器期望的数据集刷新频率。
        reuse：CURATE 判断历史数据追加、尾部修订或全量刷新的语义。
        overlap_days：增量抓取时向当前水位之前回看的自然日数。
        source_endpoints：按供应商标识组织的可用端点及字段映射。
        freshness：数据集的新鲜度判定策略。
    返回值：
        构造并返回 ``DatasetSpec`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    kind: DatasetKind
    schema: CanonicalSchema
    partitioning: Partitioning
    pit_fields: tuple[str, ...]
    fetch_granularity: FetchGranularity
    fetch_plan: FetchPlan
    cadence: UpdateCadence
    reuse: ReuseSemantics
    overlap_days: int
    source_endpoints: Mapping[str, tuple[EndpointMapping, ...]]
    freshness: FreshnessPolicy

    def __post_init__(self) -> None:
        if self.overlap_days < 0:
            raise ValueError("overlap_days must not be negative")
        columns = set(self.schema.columns.names())
        for field in (
            *self.schema.primary_key,
            *self.schema.sort_key,
            *self.pit_fields,
        ):
            if field not in columns:
                raise ValueError(
                    f"dataset {self.kind.value} references unknown {field}"
                )
        if (
            self.freshness.watermark_field is not None
            and self.freshness.watermark_field not in columns
        ):
            raise ValueError(
                f"dataset {self.kind.value} freshness references unknown "
                f"{self.freshness.watermark_field}"
            )
        object.__setattr__(
            self,
            "source_endpoints",
            MappingProxyType(
                {
                    source: tuple(endpoints)
                    for source, endpoints in self.source_endpoints.items()
                }
            ),
        )


class DatasetCatalog(Mapping[DatasetKind, DatasetSpec]):
    """保存并校验覆盖全部 Canonical 数据集的不可变目录。

    入参：
        specs：覆盖全部 Canonical 数据集的契约集合。
    返回值：
        构造并返回 ``DatasetCatalog`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def __init__(self, specs: tuple[DatasetSpec, ...]) -> None:
        values = {spec.kind: spec for spec in specs}
        if len(values) != len(specs):
            raise ValueError("duplicate dataset specification")
        expected = set(DatasetKind)
        if set(values) != expected:
            missing = sorted(item.value for item in expected - set(values))
            extra = sorted(item.value for item in set(values) - expected)
            raise ValueError(
                f"dataset catalog mismatch: missing={missing}, extra={extra}"
            )
        self._specs = MappingProxyType(values)

    def __getitem__(self, key: DatasetKind) -> DatasetSpec:
        """按键读取集合元素。

        入参：
            key：``key``。
        返回值：
            返回``getitem``（``DatasetSpec``）。
        异常：
            无。
        """
        return self._specs[key]

    def __iter__(self) -> Iterator[DatasetKind]:
        """返回集合迭代器。

        入参：
            无。
        返回值：
            返回``iter``（``Iterator[DatasetKind]``）。
        异常：
            无。
        """
        return iter(self._specs)

    def __len__(self) -> int:
        """返回集合元素数量。

        入参：
            无。
        返回值：
            返回``len``（``int``）。
        异常：
            无。
        """
        return len(self._specs)

    def parse(self, value: str) -> DatasetSpec:
        """将数据集字符串解析为目录中的数据集契约。

        入参：
            value：待处理或解析的输入值。
        返回值：
            返回解析并校验Canonical 数据后的``parse``（``DatasetSpec``）。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        try:
            return self[DatasetKind(value)]
        except ValueError as error:
            raise ValueError(f"unsupported dataset: {value}") from error


_AUDIT = ("source", "available_at", "availability_source", "pit_usable", "ingested_at")


class _DatasetSpecFactory:
    """集中构造 Tushare 一端点一数据集目录。"""

    @staticmethod
    def spec(
        kind: DatasetKind,
        endpoint: str,
        partitioning: Partitioning,
        granularity: FetchGranularity,
        plan: FetchPlan,
        cadence: UpdateCadence,
        reuse: ReuseSemantics,
        overlap_days: int,
        freshness: FreshnessPolicy,
        fields: Mapping[str, str],
        *,
        extra_pit_fields: tuple[str, ...] = (),
    ) -> DatasetSpec:
        return DatasetSpec(
            kind,
            CANONICAL_SCHEMAS[kind],
            partitioning,
            _AUDIT + extra_pit_fields,
            granularity,
            plan,
            cadence,
            reuse,
            overlap_days,
            {"tushare": (EndpointMapping(endpoint, fields),)},
            freshness,
        )


_BAR_FIELDS = {
    "ts_code": "instrument_id", "trade_date": "trade_date", "open": "open",
    "high": "high", "low": "low", "close": "close", "pre_close": "preclose",
    "change": "change", "pct_chg": "pct_change", "vol": "volume",
    "amount": "amount",
}


DATASET_CATALOG = DatasetCatalog(
    (
        _DatasetSpecFactory.spec(
            DatasetKind.STOCK_MASTER, "stock_basic", Partitioning.ALL,
            FetchGranularity.MARKET_SNAPSHOT, FetchPlan.MARKET_SNAPSHOT,
            UpdateCadence.DAILY, ReuseSemantics.FULL_REFRESH, 0,
            FreshnessPolicy(FreshnessBasis.SUCCESSFUL_REFRESH, None, 0),
            {"ts_code": "instrument_id"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.FUND_MASTER, "fund_basic", Partitioning.ALL,
            FetchGranularity.MARKET_SNAPSHOT, FetchPlan.MARKET_SNAPSHOT,
            UpdateCadence.DAILY, ReuseSemantics.FULL_REFRESH, 0,
            FreshnessPolicy(FreshnessBasis.SUCCESSFUL_REFRESH, None, 0),
            {"ts_code": "instrument_id"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.INDEX_MASTER, "index_basic", Partitioning.ALL,
            FetchGranularity.MARKET_SNAPSHOT, FetchPlan.MARKET_SNAPSHOT,
            UpdateCadence.DAILY, ReuseSemantics.FULL_REFRESH, 0,
            FreshnessPolicy(FreshnessBasis.SUCCESSFUL_REFRESH, None, 0),
            {"ts_code": "index_id"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.TRADE_CALENDAR, "trade_cal", Partitioning.ALL,
            FetchGranularity.DATE_RANGE, FetchPlan.TRADE_CALENDAR_RANGE,
            UpdateCadence.DAILY, ReuseSemantics.APPEND_WITH_TAIL_REVISION, 30,
            FreshnessPolicy(FreshnessBasis.CALENDAR_HORIZON, "trade_date", 30),
            {"cal_date": "trade_date", "is_open": "is_trading_day",
             "pretrade_date": "previous_trade_date"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.STOCK_DAILY_BAR, "daily", Partitioning.YEAR,
            FetchGranularity.MARKET_TRADE_DATE, FetchPlan.MARKET_TRADE_DATE,
            UpdateCadence.DAILY, ReuseSemantics.APPEND_WITH_TAIL_REVISION, 5,
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
            {**_BAR_FIELDS, "ah_vol": "after_hours_volume",
             "ah_amount": "after_hours_amount"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.STOCK_ADJUSTMENT_FACTOR, "adj_factor", Partitioning.YEAR,
            FetchGranularity.MARKET_TRADE_DATE, FetchPlan.MARKET_TRADE_DATE,
            UpdateCadence.DAILY, ReuseSemantics.APPEND_WITH_TAIL_REVISION, 5,
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
            {"ts_code": "instrument_id", "trade_date": "trade_date",
             "adj_factor": "adjustment_factor"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.FUND_DAILY_BAR, "fund_daily", Partitioning.YEAR,
            FetchGranularity.MARKET_TRADE_DATE, FetchPlan.MARKET_TRADE_DATE,
            UpdateCadence.DAILY, ReuseSemantics.APPEND_WITH_TAIL_REVISION, 5,
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
            _BAR_FIELDS,
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.FUND_ADJUSTMENT_FACTOR, "fund_adj", Partitioning.YEAR,
            FetchGranularity.MARKET_TRADE_DATE, FetchPlan.MARKET_TRADE_DATE,
            UpdateCadence.DAILY, ReuseSemantics.APPEND_WITH_TAIL_REVISION, 5,
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
            {"ts_code": "instrument_id", "trade_date": "trade_date",
             "adj_factor": "adjustment_factor"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.INDEX_DAILY_BAR, "index_daily", Partitioning.YEAR,
            FetchGranularity.INDEX_RANGE_EXCEPTION,
            FetchPlan.INDEX_RANGE_EXCEPTION, UpdateCadence.DAILY,
            ReuseSemantics.APPEND_WITH_TAIL_REVISION, 5,
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
            {**_BAR_FIELDS, "ts_code": "index_id"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.STOCK_DAILY_BASIC, "daily_basic", Partitioning.YEAR,
            FetchGranularity.MARKET_TRADE_DATE, FetchPlan.MARKET_TRADE_DATE,
            UpdateCadence.DAILY, ReuseSemantics.APPEND_WITH_TAIL_REVISION, 5,
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
            {"ts_code": "instrument_id", "trade_date": "trade_date"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.STOCK_SUSPENSION, "suspend_d", Partitioning.YEAR,
            FetchGranularity.MARKET_TRADE_DATE, FetchPlan.MARKET_TRADE_DATE,
            UpdateCadence.DAILY, ReuseSemantics.APPEND_WITH_TAIL_REVISION, 5,
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
            {"ts_code": "instrument_id", "trade_date": "trade_date"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.STOCK_RISK_WARNING, "stock_st", Partitioning.YEAR,
            FetchGranularity.MARKET_TRADE_DATE, FetchPlan.MARKET_TRADE_DATE,
            UpdateCadence.DAILY, ReuseSemantics.APPEND_WITH_TAIL_REVISION, 5,
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
            {"ts_code": "instrument_id", "trade_date": "trade_date",
             "type": "risk_type", "type_name": "risk_type_name"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.STOCK_FINANCIAL_INDICATOR, "fina_indicator_vip",
            Partitioning.REPORT_YEAR, FetchGranularity.REPORT_PERIOD,
            FetchPlan.REPORT_PERIOD, UpdateCadence.QUARTERLY_DISCLOSURE,
            ReuseSemantics.APPEND_WITH_RESTATEMENT, 0,
            FreshnessPolicy(FreshnessBasis.DISCLOSURE_DEADLINE, None, 0),
            {"ts_code": "instrument_id", "ann_date": "announcement_date",
             "end_date": "report_period"},
            extra_pit_fields=("announcement_date",),
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.INDUSTRY_CATALOG, "index_classify", Partitioning.ALL,
            FetchGranularity.MARKET_SNAPSHOT, FetchPlan.MARKET_SNAPSHOT,
            UpdateCadence.WEEKLY, ReuseSemantics.FULL_REFRESH, 0,
            FreshnessPolicy(FreshnessBasis.SUCCESSFUL_REFRESH, None, 7),
            {"index_code": "industry_index_id", "src": "taxonomy"},
        ),
        _DatasetSpecFactory.spec(
            DatasetKind.INDUSTRY_MEMBERSHIP, "index_member_all", Partitioning.ALL,
            FetchGranularity.INDUSTRY_L1, FetchPlan.INDUSTRY_L1,
            UpdateCadence.WEEKLY, ReuseSemantics.FULL_REFRESH, 0,
            FreshnessPolicy(FreshnessBasis.SUCCESSFUL_REFRESH, None, 7),
            {"ts_code": "instrument_id", "name": "instrument_name",
             "l1_code": "level1_code", "l1_name": "level1_name",
             "l2_code": "level2_code", "l2_name": "level2_name",
             "l3_code": "level3_code", "l3_name": "level3_name"},
        ),
    )
)
