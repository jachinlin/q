"""定义 Canonical 数据集的唯一可执行目录及增量语义。"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

from quant_research.data.schemas import CANONICAL_SCHEMAS, CanonicalSchema
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

    TRADING_DAY = "trading_day"
    DATE_RANGE = "date_range"
    FULL_SNAPSHOT = "full_snapshot"
    FINANCIAL_CELL = "instrument_year_quarter_endpoint"
    INSTRUMENT = "instrument"
    INDEX_RANGE = "index_range"


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


class FreshnessBasis(StrEnum):
    """枚举数据集新鲜度使用的业务判定基础。

    入参：按枚举值构造。返回值：返回枚举成员。异常：非法值抛出 ValueError。
    """

    TRADING_SESSION = "trading_session"
    CALENDAR_HORIZON = "calendar_horizon"
    SUCCESSFUL_REFRESH = "successful_refresh"


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
        if self.basis is FreshnessBasis.SUCCESSFUL_REFRESH:
            if self.watermark_field is not None:
                raise ValueError("refresh freshness must not declare a watermark field")
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
        field_map：构造对象所需的同名字段，约束见类型标注。
        fan_out：构造对象所需的同名字段，约束见类型标注。
    返回值：
        构造并返回 ``EndpointMapping`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    endpoint: str
    field_map: Mapping[str, str]
    fan_out: tuple[DatasetKind, ...] = ()

    def __post_init__(self) -> None:
        if not self.endpoint:
            raise ValueError("endpoint must not be empty")
        object.__setattr__(self, "field_map", MappingProxyType(dict(self.field_map)))


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """描述一个 Canonical 数据集的完整运行契约。

    入参：
        kind：Canonical 数据集枚举值。
        schema：构造对象所需的同名字段，约束见类型标注。
        partitioning：构造对象所需的同名字段，约束见类型标注。
        pit_fields：构造对象所需的同名字段，约束见类型标注。
        fetch_granularity：构造对象所需的同名字段，约束见类型标注。
        cadence：构造对象所需的同名字段，约束见类型标注。
        reuse：构造对象所需的同名字段，约束见类型标注。
        overlap_days：构造对象所需的同名字段，约束见类型标注。
        source_endpoints：构造对象所需的同名字段，约束见类型标注。
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


_AUDIT = (
    "source",
    "available_at",
    "availability_source",
    "pit_usable",
    "ingested_at",
)
_DAILY_ENDPOINT = "query_daily_history_k_AStock"
_ETF_ENDPOINT = "query_etf_history_k_data_plus"
_DAILY_FAN_OUT = (
    DatasetKind.DAILY_BAR,
    DatasetKind.DAILY_BASIC,
    DatasetKind.SECURITY_STATUS,
)
_ETF_FAN_OUT = (
    DatasetKind.DAILY_BAR,
    DatasetKind.SECURITY_STATUS,
)


class _DatasetSpecFactory:
    """集中构造目录常量使用的端点映射。"""

    @staticmethod
    def endpoint(
        name: str,
        fields: Mapping[str, str],
        *,
        fan_out: tuple[DatasetKind, ...] = (),
    ) -> EndpointMapping:
        return EndpointMapping(name, fields, fan_out)


DATASET_CATALOG = DatasetCatalog(
    (
        DatasetSpec(
            DatasetKind.DAILY_BAR,
            CANONICAL_SCHEMAS[DatasetKind.DAILY_BAR],
            Partitioning.YEAR,
            _AUDIT,
            FetchGranularity.TRADING_DAY,
            UpdateCadence.DAILY,
            ReuseSemantics.APPEND_ONLY,
            5,
            {
                "baostock": (
                    _DatasetSpecFactory.endpoint(
                        _DAILY_ENDPOINT,
                        {
                            "code": "instrument_id",
                            "date": "trade_date",
                            "open": "open",
                            "high": "high",
                            "low": "low",
                            "close": "close",
                            "preclose": "preclose",
                            "volume": "volume",
                            "amount": "amount",
                            "adjustflag": "adjustment_flag",
                            "pctChg": "pct_change",
                        },
                        fan_out=_DAILY_FAN_OUT,
                    ),
                    _DatasetSpecFactory.endpoint(
                        _ETF_ENDPOINT,
                        {
                            "code": "instrument_id",
                            "date": "trade_date",
                            "open": "open",
                            "high": "high",
                            "low": "low",
                            "close": "close",
                            "preclose": "preclose",
                            "volume": "volume",
                            "amount": "amount",
                            "adjustflag": "adjustment_flag",
                            "pctChg": "pct_change",
                        },
                        fan_out=_ETF_FAN_OUT,
                    ),
                )
            },
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
        ),
        DatasetSpec(
            DatasetKind.DAILY_BASIC,
            CANONICAL_SCHEMAS[DatasetKind.DAILY_BASIC],
            Partitioning.YEAR,
            _AUDIT,
            FetchGranularity.TRADING_DAY,
            UpdateCadence.DAILY,
            ReuseSemantics.APPEND_ONLY,
            5,
            {
                "baostock": (
                    _DatasetSpecFactory.endpoint(
                        _DAILY_ENDPOINT,
                        {
                            "code": "instrument_id",
                            "date": "trade_date",
                            "peTTM": "pe_ttm",
                            "pbMRQ": "pb_mrq",
                            "psTTM": "ps_ttm",
                            "turn": "turnover",
                        },
                        fan_out=_DAILY_FAN_OUT,
                    ),
                )
            },
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
        ),
        DatasetSpec(
            DatasetKind.SECURITY_STATUS,
            CANONICAL_SCHEMAS[DatasetKind.SECURITY_STATUS],
            Partitioning.YEAR,
            _AUDIT,
            FetchGranularity.TRADING_DAY,
            UpdateCadence.DAILY,
            ReuseSemantics.APPEND_ONLY,
            5,
            {
                "baostock": (
                    _DatasetSpecFactory.endpoint(
                        _DAILY_ENDPOINT,
                        {
                            "code": "instrument_id",
                            "date": "trade_date",
                            "tradestatus": "is_suspended",
                            "isST": "is_st",
                        },
                        fan_out=_DAILY_FAN_OUT,
                    ),
                    _DatasetSpecFactory.endpoint(
                        _ETF_ENDPOINT,
                        {
                            "code": "instrument_id",
                            "date": "trade_date",
                            "tradestatus": "is_suspended",
                            "isST": "is_st",
                        },
                        fan_out=_ETF_FAN_OUT,
                    ),
                )
            },
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
        ),
        DatasetSpec(
            DatasetKind.TRADE_CALENDAR,
            CANONICAL_SCHEMAS[DatasetKind.TRADE_CALENDAR],
            Partitioning.ALL,
            _AUDIT,
            FetchGranularity.DATE_RANGE,
            UpdateCadence.DAILY,
            ReuseSemantics.APPEND_WITH_TAIL_REVISION,
            30,
            {
                "baostock": (
                    _DatasetSpecFactory.endpoint(
                        "query_trade_dates",
                        {
                            "calendar_date": "trade_date",
                            "is_trading_day": "is_trading_day",
                        },
                    ),
                )
            },
            FreshnessPolicy(FreshnessBasis.CALENDAR_HORIZON, "trade_date", 30),
        ),
        DatasetSpec(
            DatasetKind.INSTRUMENT,
            CANONICAL_SCHEMAS[DatasetKind.INSTRUMENT],
            Partitioning.ALL,
            _AUDIT,
            FetchGranularity.FULL_SNAPSHOT,
            UpdateCadence.DAILY,
            ReuseSemantics.FULL_REFRESH,
            0,
            {
                "baostock": (
                    _DatasetSpecFactory.endpoint(
                        "query_stock_basic",
                        {
                            "code": "instrument_id",
                            "code_name": "name",
                            "ipoDate": "list_date",
                            "outDate": "delist_date",
                            "type": "instrument_type",
                            "status": "listing_status",
                        },
                    ),
                )
            },
            FreshnessPolicy(FreshnessBasis.SUCCESSFUL_REFRESH, None, 0),
        ),
        DatasetSpec(
            DatasetKind.FINANCIAL_OBSERVATION,
            CANONICAL_SCHEMAS[DatasetKind.FINANCIAL_OBSERVATION],
            Partitioning.REPORT_YEAR,
            _AUDIT + ("announced_at",),
            FetchGranularity.FINANCIAL_CELL,
            UpdateCadence.WEEKLY,
            ReuseSemantics.APPEND_WITH_RESTATEMENT,
            370,
            {"baostock": (_DatasetSpecFactory.endpoint("query_dupont_data", {}),)},
            FreshnessPolicy(FreshnessBasis.SUCCESSFUL_REFRESH, None, 7),
        ),
        DatasetSpec(
            DatasetKind.INDUSTRY_CLASSIFICATION,
            CANONICAL_SCHEMAS[DatasetKind.INDUSTRY_CLASSIFICATION],
            Partitioning.YEAR,
            _AUDIT,
            FetchGranularity.TRADING_DAY,
            UpdateCadence.DAILY,
            ReuseSemantics.APPEND_WITH_RESTATEMENT,
            5,
            {
                "baostock": (
                    _DatasetSpecFactory.endpoint(
                        "query_stock_industry",
                        {
                            "as_of_date": "as_of_date",
                            "updateDate": "supplier_update_date",
                            "code": "instrument_id",
                            "industry": "industry_name",
                        },
                    ),
                )
            },
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "as_of_date", 0),
        ),
        DatasetSpec(
            DatasetKind.INDEX_BAR,
            CANONICAL_SCHEMAS[DatasetKind.INDEX_BAR],
            Partitioning.YEAR,
            _AUDIT,
            FetchGranularity.INDEX_RANGE,
            UpdateCadence.DAILY,
            ReuseSemantics.APPEND_ONLY,
            5,
            {
                "baostock": (
                    _DatasetSpecFactory.endpoint(
                        "query_history_k_data_plus",
                        {
                            "code": "index_id",
                            "date": "trade_date",
                            "open": "open",
                            "high": "high",
                            "low": "low",
                            "close": "close",
                            "preclose": "preclose",
                            "volume": "volume",
                            "amount": "amount",
                            "pctChg": "pct_change",
                        },
                    ),
                )
            },
            FreshnessPolicy(FreshnessBasis.TRADING_SESSION, "trade_date", 0),
        ),
    )
)
