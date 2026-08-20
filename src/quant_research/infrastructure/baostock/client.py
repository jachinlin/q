"""封装 BaoStock SDK 边界与可复现的 Raw 数据采集客户端。"""

from __future__ import annotations

import hashlib
import importlib
import logging
import math
import re
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as clock_time
from typing import Protocol, cast
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_research.data.contracts import (
    JsonValue,
    ProviderCapabilities,
    PublishedPartition,
    RawBatch,
)
from quant_research.data.sources.financials import (
    financial_report_period_end,
    financial_request_is_eligible,
)
from quant_research.domain.enums import DatasetKind, Exchange, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.domain.identifiers import InstrumentId

BAOSTOCK_RESEARCH_CAPABILITIES = ProviderCapabilities(
    daily_bars=True,
    trade_calendar=True,
    instruments=True,
    security_status=True,
    financials_with_announcement_date=True,
    adjustment_factors=False,
)
BAOSTOCK_CAPABILITIES = frozenset(
    {
        DatasetKind.DAILY_BAR,
        DatasetKind.DAILY_BASIC,
        DatasetKind.SECURITY_STATUS,
        DatasetKind.TRADE_CALENDAR,
        DatasetKind.INSTRUMENT,
        DatasetKind.FINANCIAL_OBSERVATION,
        DatasetKind.INDUSTRY_CLASSIFICATION,
        DatasetKind.INDEX_BAR,
    }
)

DAILY_BAR_FIELDS = (
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "adjustflag",
    "turn",
    "tradestatus",
    "pctChg",
    "peTTM",
    "pbMRQ",
    "psTTM",
    "pcfNcfTTM",
    "isST",
)
INDEX_BAR_FIELDS = (
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "pctChg",
)
INSTRUMENT_FIELDS = ("code", "code_name", "ipoDate", "outDate", "type", "status")
TRADE_CALENDAR_FIELDS = ("calendar_date", "is_trading_day")
DUPONT_FIELDS = (
    "code",
    "pubDate",
    "statDate",
    "dupontROE",
    "dupontAssetStoEquity",
    "dupontAssetTurn",
    "dupontPnitoni",
    "dupontNitogr",
    "dupontTaxBurden",
    "dupontIntburden",
    "dupontEbittogr",
)
FINANCIAL_FIELDS_BY_ENDPOINT: Mapping[str, tuple[str, ...]] = {
    "query_dupont_data": DUPONT_FIELDS,
}
INDUSTRY_FIELDS = (
    "code",
    "code_name",
    "industry",
    "industryClassification",
    "updateDate",
    "as_of_date",
)
_DAILY_BAR_FIELD_ARGUMENT = ",".join(DAILY_BAR_FIELDS)
_INDEX_BAR_FIELD_ARGUMENT = ",".join(INDEX_BAR_FIELDS)
_BAOSTOCK_CODE = re.compile(r"(sh|sz)\.([0-9]{6})\Z")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_BENCHMARK_CODE = "sh.000300"
_BENCHMARK_ROW = ("sh.000300", "沪深300", "", "", "2", "1")
_INDEX_NAMES = {
    "sz.399317": "国证A指",
    "sh.000300": "沪深300",
    "sh.000905": "中证500",
    "sh.000852": "中证1000",
    "sh.000016": "上证50",
}
_FINANCIAL_APIS = tuple(FINANCIAL_FIELDS_BY_ENDPOINT)
_INDUSTRY_REQUIRED_FIELDS = ("code", "industry")


class BaoStockResponse(Protocol):
    """约束 BaoStock SDK 响应共享的状态字段。

    入参：
        error_code：BaoStock 返回的状态码；``0`` 表示调用成功。
        error_msg：供应商对当前状态码给出的诊断文本。
    返回值：
        构造并返回 ``BaoStockResponse`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    error_code: str
    error_msg: str


class BaoStockCursor(BaoStockResponse, Protocol):
    """约束 BaoStock 历史查询返回的可迭代游标。

    入参：
        fields：需要读取、映射或计算的字段集合。
    返回值：
        构造并返回 ``BaoStockCursor`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    fields: Sequence[str]

    def next(self) -> bool:
        """推进 BaoStock 游标并处理 SDK 内部分页。

        入参：
            无。
        返回值：
            返回是否处理基础设施中的``next``。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def get_row_data(self) -> Sequence[str]:
        """返回游标当前行的供应商原生字符串字段。

        入参：
            无。
        返回值：
            返回读取数据行数据后的数据行数据（``Sequence[str]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """


class BaoStockGateway(Protocol):
    """约束可注入的 BaoStock SDK 最小能力集合。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``BaoStockGateway`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def login(self) -> BaoStockResponse:
        """建立供应商会话；重复调用保持幂等。

        入参：
            无。
        返回值：
            返回``login``（``BaoStockResponse``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def logout(self) -> BaoStockResponse:
        """关闭底层 BaoStock SDK 会话。

        入参：
            无。
        返回值：
            返回``logout``（``BaoStockResponse``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def query_daily_history_k_AStock(self, date: str = "") -> BaoStockCursor:
        """调用 SDK 查询单日全市场 A 股行情。

        入参：
            date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回日频历史``k``A 股（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> BaoStockCursor:
        """调用 SDK 查询单个证券或指数的日期区间行情。

        入参：
            code：调用接口所需的同名参数，具体约束见类型标注。
            fields：需要读取、映射或计算的字段集合。
            start_date：调用接口所需的同名参数，具体约束见类型标注。
            end_date：调用接口所需的同名参数，具体约束见类型标注。
            frequency：调用接口所需的同名参数，具体约束见类型标注。
            adjustflag：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回历史``k``数据``plus``（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def query_stock_basic(self, *, code: str, code_name: str) -> BaoStockCursor:
        """调用 SDK 查询历史证券主数据。

        入参：
            code：调用接口所需的同名参数，具体约束见类型标注。
            code_name：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回股票``basic``（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def query_trade_dates(self, *, start_date: str, end_date: str) -> BaoStockCursor:
        """调用 SDK 查询交易日历。

        入参：
            start_date：调用接口所需的同名参数，具体约束见类型标注。
            end_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回交易``dates``（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def query_dupont_data(
        self, *, code: str, year: int, quarter: int
    ) -> BaoStockCursor:
        """调用 SDK 查询指定证券和报告期的杜邦指标。

        入参：
            code：调用接口所需的同名参数，具体约束见类型标注。
            year：调用接口所需的同名参数，具体约束见类型标注。
            quarter：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回``dupont``数据（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """

    def query_stock_industry(self, *, code: str, date: str) -> BaoStockCursor:
        """调用 SDK 查询单个证券或全市场行业分类。

        入参：
            code：调用接口所需的同名参数，具体约束见类型标注。
            date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回股票行业分类（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """


class _BaoStockSdk(Protocol):
    """Structural type for the imported external SDK module."""

    def login(self) -> BaoStockResponse:
        """Open an SDK session."""

    def logout(self) -> BaoStockResponse:
        """Close an SDK session."""

    def query_daily_history_k_AStock(self, date: str = "") -> BaoStockCursor:
        """Query all A-share daily bars for one provider date."""

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> BaoStockCursor:
        """Query one provider code over one closed date interval."""

    def query_stock_basic(self, *, code: str, code_name: str) -> BaoStockCursor:
        """Query security master data."""

    def query_trade_dates(self, *, start_date: str, end_date: str) -> BaoStockCursor:
        """Query exchange open dates."""

    def query_dupont_data(
        self, *, code: str, year: int, quarter: int
    ) -> BaoStockCursor:
        """Query one security's DuPont metrics for one report year and quarter."""

    def query_stock_industry(self, *, code: str, date: str) -> BaoStockCursor:
        """Query industry classifications for one security or the whole market."""


class BaoStockSdkGateway:
    """隔离真实 BaoStock SDK 导入并转发供应商调用。

    入参：
        sdk：实现 BaoStock SDK 调用面的可选模块或测试替身；为空时延迟导入真实 SDK。
    返回值：
        构造并返回 ``BaoStockSdkGateway`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def __init__(self, *, sdk: _BaoStockSdk | None = None) -> None:
        self._sdk = sdk or cast(_BaoStockSdk, importlib.import_module("baostock"))

    def login(self) -> BaoStockResponse:
        """建立供应商会话；重复调用保持幂等。

        入参：
            无。
        返回值：
            返回``login``（``BaoStockResponse``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return self._sdk.login()

    def logout(self) -> BaoStockResponse:
        """关闭底层 BaoStock SDK 会话。

        入参：
            无。
        返回值：
            返回``logout``（``BaoStockResponse``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return self._sdk.logout()

    def query_daily_history_k_AStock(self, date: str = "") -> BaoStockCursor:
        """调用 SDK 查询单日全市场 A 股行情。

        入参：
            date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回日频历史``k``A 股（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return self._sdk.query_daily_history_k_AStock(date)

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> BaoStockCursor:
        """调用 SDK 查询单个证券或指数的日期区间行情。

        入参：
            code：调用接口所需的同名参数，具体约束见类型标注。
            fields：需要读取、映射或计算的字段集合。
            start_date：调用接口所需的同名参数，具体约束见类型标注。
            end_date：调用接口所需的同名参数，具体约束见类型标注。
            frequency：调用接口所需的同名参数，具体约束见类型标注。
            adjustflag：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回历史``k``数据``plus``（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return self._sdk.query_history_k_data_plus(
            code,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag=adjustflag,
        )

    def query_stock_basic(self, *, code: str, code_name: str) -> BaoStockCursor:
        """调用 SDK 查询历史证券主数据。

        入参：
            code：调用接口所需的同名参数，具体约束见类型标注。
            code_name：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回股票``basic``（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return self._sdk.query_stock_basic(code=code, code_name=code_name)

    def query_trade_dates(self, *, start_date: str, end_date: str) -> BaoStockCursor:
        """调用 SDK 查询交易日历。

        入参：
            start_date：调用接口所需的同名参数，具体约束见类型标注。
            end_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回交易``dates``（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return self._sdk.query_trade_dates(start_date=start_date, end_date=end_date)

    def query_dupont_data(
        self, *, code: str, year: int, quarter: int
    ) -> BaoStockCursor:
        """调用 SDK 查询指定证券和报告期的杜邦指标。

        入参：
            code：调用接口所需的同名参数，具体约束见类型标注。
            year：调用接口所需的同名参数，具体约束见类型标注。
            quarter：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回``dupont``数据（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return self._sdk.query_dupont_data(code=code, year=year, quarter=quarter)

    def query_stock_industry(self, *, code: str, date: str) -> BaoStockCursor:
        """调用 SDK 查询单个证券或全市场行业分类。

        入参：
            code：调用接口所需的同名参数，具体约束见类型标注。
            date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回股票行业分类（``BaoStockCursor``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return self._sdk.query_stock_industry(code=code, date=date)


@dataclass(frozen=True, slots=True)
class InstrumentListing:
    """描述一个 Canonical 证券在交易所的完整上市区间。

    入参：
        instrument_id：供应商无关的证券标识。
        list_date：证券首次上市交易日期。
        delist_date：证券退市日期；仍在可用历史目录中时可以为空。
        provider_type：BaoStock 使用的证券类型代码，用于选择历史采集范围。
    返回值：
        构造并返回 ``InstrumentListing`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    instrument_id: InstrumentId
    list_date: date
    delist_date: date | None
    provider_type: str = "1"


class InstrumentCatalog(Protocol):
    """约束独立于当前上市状态的历史证券目录。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``InstrumentCatalog`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def list_instruments(self) -> Sequence[InstrumentListing]:
        """返回包含已退市证券的历史上市记录。

        入参：
            无。
        返回值：
            返回按确定性顺序列出证券集合后的证券集合（``Sequence[InstrumentListing]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """


@dataclass(frozen=True, slots=True)
class BaoStockHistoricalCatalog:
    """从不可变 Raw 行重建可复用的历史证券目录。

    入参：
        listings：从证券主数据 Raw 证据恢复并按证券标识排序的完整上市区间。
    返回值：
        构造并返回 ``BaoStockHistoricalCatalog`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    listings: tuple[InstrumentListing, ...]

    @classmethod
    def from_raw_rows(
        cls, rows: Sequence[dict[str, JsonValue]]
    ) -> BaoStockHistoricalCatalog:
        """从供应商原生证券行构造历史目录。

        入参：
            rows：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回``raw``数据行集合（``BaoStockHistoricalCatalog``）。
        异常：
            TypeError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        listings: list[InstrumentListing] = []
        for row in rows:
            code = row.get("code")
            list_text = row.get("ipoDate")
            delist_text = row.get("outDate")
            provider_type = row.get("type")
            if not isinstance(code, str) or not isinstance(list_text, str):
                raise TypeError("instrument raw rows require string code and ipoDate")
            if not isinstance(delist_text, str):
                raise TypeError("instrument raw outDate must be a string")
            if not isinstance(provider_type, str):
                raise TypeError("instrument raw type must be a string")
            listings.append(
                InstrumentListing(
                    instrument_id=from_baostock_code(code),
                    list_date=date.fromisoformat(list_text),
                    delist_date=(
                        date.fromisoformat(delist_text) if delist_text else None
                    ),
                    provider_type=provider_type,
                )
            )
        return cls(
            tuple(sorted(listings, key=lambda item: item.instrument_id.canonical()))
        )

    def list_instruments(self) -> Sequence[InstrumentListing]:
        """返回包含已退市证券的历史上市记录。

        入参：
            无。
        返回值：
            返回按确定性顺序列出证券集合后的证券集合（``Sequence[InstrumentListing]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return self.listings


@dataclass(frozen=True, slots=True)
class BaoStockConfig:
    """保存显式的 BaoStock 采集上限与重试策略。

    入参：
        max_instruments_per_batch：一次批量计划允许包含的最大证券数量。
        max_days_per_batch：一个区间行情请求允许覆盖的最大自然日数。
        max_attempts：单次供应商请求包含首次调用在内的最大尝试次数。
        retry_backoff_seconds：各次重试前依次采用的等待秒数。
        retryable_error_codes：允许按退避策略重试的 BaoStock 状态码集合。
        index_codes：需要采集区间行情的指数供应商代码白名单。
        etf_codes：需要补充采集日线的 ETF 供应商代码白名单。
    返回值：
        构造并返回 ``BaoStockConfig`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    max_instruments_per_batch: int
    max_days_per_batch: int
    max_attempts: int
    retry_backoff_seconds: tuple[float, ...]
    retryable_error_codes: frozenset[str]
    index_codes: tuple[str, ...] = (
        "sz.399317",
        "sh.000300",
        "sh.000905",
        "sh.000852",
        "sh.000016",
    )
    etf_codes: tuple[str, ...] = (
        "sh.510050",
        "sh.510300",
        "sh.588000",
        "sh.513100",
    )

    def __post_init__(self) -> None:
        limits = (
            self.max_instruments_per_batch,
            self.max_days_per_batch,
            self.max_attempts,
        )
        if any(type(limit) is not int or limit <= 0 for limit in limits):
            raise ValueError("batch limits and max_attempts must be positive integers")
        if len(self.retry_backoff_seconds) != self.max_attempts - 1:
            raise ValueError("retry_backoff_seconds must contain one delay per retry")
        if any(
            not math.isfinite(delay) or delay < 0
            for delay in self.retry_backoff_seconds
        ):
            raise ValueError("retry backoff seconds must be finite and non-negative")
        if not self.index_codes or any(
            _BAOSTOCK_CODE.fullmatch(code) is None for code in self.index_codes
        ):
            raise ValueError("index_codes must contain BaoStock index identifiers")
        if (
            not self.etf_codes
            or len(set(self.etf_codes)) != len(self.etf_codes)
            or any(_BAOSTOCK_CODE.fullmatch(code) is None for code in self.etf_codes)
        ):
            raise ValueError("etf_codes must contain unique BaoStock ETF identifiers")


def to_baostock_code(instrument_id: InstrumentId) -> str:
    """将供应商无关证券标识转换为 BaoStock 代码；该边界转换是稳定公开 API，因此保留为模块级入口。

    入参：
        instrument_id：供应商无关的证券标识。
    返回值：
        返回``baostock``代码（``str``）。
    异常：
        实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
    """
    prefix = {Exchange.SSE: "sh", Exchange.SZSE: "sz"}[instrument_id.exchange]
    return f"{prefix}.{instrument_id.symbol}"


def from_baostock_code(value: str) -> InstrumentId:
    """严格解析 BaoStock 代码为供应商无关证券标识；该边界转换是稳定公开 API，因此保留为模块级入口。

    入参：
        value：待处理或解析的输入值。
    返回值：
        返回``baostock``代码（``InstrumentId``）。
    异常：
        ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
    """
    match = _BAOSTOCK_CODE.fullmatch(value)
    if match is None:
        raise ValueError(
            "BaoStock code must be sh. or sz. followed by six ASCII digits"
        )
    prefix, symbol = match.groups()
    exchange = Exchange.SSE if prefix == "sh" else Exchange.SZSE
    instrument_id = InstrumentId(exchange=exchange, symbol=symbol)
    if to_baostock_code(instrument_id) != value:
        raise ValueError("BaoStock code did not pass canonical round-trip validation")
    return instrument_id


class _BaoStockSourceSupport:
    """集中解析 BaoStock 请求字段、原始行字段与 UTC 时钟。"""

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(UTC)

    @staticmethod
    def required_request_text(request: Mapping[str, JsonValue], key: str) -> str:
        value = request.get(key)
        if not isinstance(value, str) or not value:
            raise ValueError(f"request field {key} must be a nonempty string")
        return value

    @staticmethod
    def required_request_int(request: Mapping[str, JsonValue], key: str) -> int:
        value = request.get(key)
        if type(value) is not int:
            raise ValueError(f"request field {key} must be an integer")
        return value

    @staticmethod
    def row_text(row: Mapping[str, JsonValue], field: str) -> str:
        value = row.get(field)
        if value is None:
            return ""
        if not isinstance(value, str):
            raise TypeError(f"raw field {field} must be a provider-native string")
        return value


class BaoStockClient:
    """将 BaoStock 响应采集为确定性的供应商原生 Raw 批次。

    入参：
        gateway：隔离 BaoStock SDK 调用和响应类型的供应商网关。
        catalog：独立于当前上市状态的历史证券目录；首次建库时可以为空。
        config：批次上限、重试策略及指数和 ETF 白名单。
        clock：为 Raw 批次生成带时区抓取时间的可注入时钟。
        sleep：执行请求退避等待的可注入函数，测试可替换为无等待实现。
        logger：记录请求、供应商响应和重试证据的结构化日志器。
    返回值：
        构造并返回 ``BaoStockClient`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    capabilities = BAOSTOCK_CAPABILITIES

    def __init__(
        self,
        gateway: BaoStockGateway,
        catalog: InstrumentCatalog | None,
        config: BaoStockConfig,
        *,
        clock: Callable[[], datetime] = _BaoStockSourceSupport.utc_now,
        sleep: Callable[[float], None] = time.sleep,
        logger: logging.Logger | None = None,
    ) -> None:
        self._gateway = gateway
        self._catalog = catalog
        self._config = config
        self._clock = clock
        self._sleep = sleep
        self._logger = logger or logging.getLogger(__name__)
        self._logged_in = False

    @property
    def provider(self) -> str:
        """返回当前数据源的稳定供应商标识。

        入参：
            无。
        返回值：
            返回数据供应商（``str``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return "baostock"

    def login(self) -> None:
        """建立供应商会话；重复调用保持幂等。

        入参：
            无。
        返回值：
            无。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        if self._logged_in:
            return

        def perform_login() -> None:
            response = self._gateway.login()
            self._raise_provider_error(response, operation="login")

        self._retry("login", perform_login)
        self._logged_in = True

    def close(self) -> None:
        """关闭供应商会话；未登录时不执行额外操作。

        入参：
            无。
        返回值：
            无。
        异常：
            _transport_error：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        if not self._logged_in:
            return

        def perform_logout() -> None:
            response = self._gateway.logout()
            self._raise_provider_error(response, operation="logout")

        try:
            # Logout is cleanup, not data acquisition. Retrying a dead socket
            # only delays shutdown and repeats SDK diagnostics without making
            # any published data safer.
            try:
                perform_logout()
            except (TimeoutError, ConnectionError, OSError) as error:
                raise self._transport_error("logout", error) from error
        finally:
            # A broken remote connection cannot be made usable by retaining a
            # local "logged in" flag.  Callers may report the cleanup failure,
            # but a second close must remain a no-op.
            self._logged_in = False

    def daily_bars_request(self, trade_date: date) -> Mapping[str, JsonValue]:
        """构造单个开市日的全市场日行情请求。

        入参：
            trade_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回行情请求（``Mapping[str, JsonValue]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return {
            "api": "query_daily_history_k_AStock",
            "scope": "ALL",
            "date": trade_date.isoformat(),
            "frequency": "d",
        }

    def trade_calendar_request(self, start: date, end: date) -> Mapping[str, JsonValue]:
        """构造指定闭区间的规范化交易日历请求。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回交易日历请求（``Mapping[str, JsonValue]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return {"start_date": start.isoformat(), "end_date": end.isoformat()}

    def fetch_daily_bars(
        self,
        start: date,
        end: date | None = None,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> Iterable[RawBatch]:
        """获取指定证券或交易日范围的日行情 Raw 批次。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
            instruments：需要读取或采集的证券标识集合。
        返回值：
            返回从供应商获取日频行情后的日频行情（``Iterable[RawBatch]``）。
        异常：
            ValueError、_state_error：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        if not self._logged_in:
            raise self._state_error("fetch_daily_bars")
        if end is None:
            if instruments is not None:
                raise ValueError("single-date fetch does not support selections")
            trading_day = start
            self._require_completed_session(trading_day)
            rows = self._fetch_all_market_rows(trading_day)
            yield RawBatch(
                source=self.provider,
                endpoint="query_daily_history_k_AStock",
                request=self.daily_bars_request(trading_day),
                retrieved_at=self._clock(),
                schema=DAILY_BAR_FIELDS,
                rows=tuple(rows),
            )
            return
        if start > end:
            raise ValueError("start must not follow end")
        if instruments is None or len(instruments) == 0:
            if instruments is not None:
                self._logger.info(
                    "empty instrument selection resolved as all-market daily route",
                    extra={
                        "event": "empty_instruments_resolved_as_all",
                        "scope": "ALL",
                    },
                )
            _, catalog_instruments = self._resolve_instruments(start, end, instruments)
            _, open_dates = self._load_trade_calendar(start, end)
            yield from self._fetch_all_market_daily_bars(
                open_dates, catalog_instruments
            )
            return
        yield from self._fetch_selected_daily_bars(start, end, instruments)

    def _fetch_selected_daily_bars(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId],
    ) -> Iterable[RawBatch]:
        """Yield the existing instrument-range batches for explicit selections."""

        _, open_dates = self._load_trade_calendar(start, end)
        for trading_day in open_dates:
            self._require_completed_session(trading_day)
        scope, resolved = self._resolve_instruments(start, end, instruments)
        canonical_ids = tuple(item.canonical() for item in resolved)
        resolved_hash = hashlib.sha256("\n".join(canonical_ids).encode()).hexdigest()
        instrument_chunks = tuple(
            self._chunks(resolved, self._config.max_instruments_per_batch)
        )
        date_chunks = tuple(self._date_chunks(start, end))
        instrument_chunk_count = len(instrument_chunks)
        date_chunk_count = len(date_chunks)
        batch_count = instrument_chunk_count * date_chunk_count

        for instrument_index, instrument_chunk in enumerate(instrument_chunks, start=1):
            chunk_ids: list[JsonValue] = [item.canonical() for item in instrument_chunk]
            for date_index, (chunk_start, chunk_end) in enumerate(date_chunks, start=1):
                rows: list[dict[str, JsonValue]] = []
                for instrument_id in instrument_chunk:
                    rows.extend(
                        self._fetch_instrument_rows(
                            instrument_id,
                            start=chunk_start,
                            end=chunk_end,
                        )
                    )
                batch_index = (instrument_index - 1) * date_chunk_count + date_index
                request: dict[str, JsonValue] = {
                    "scope": scope,
                    "resolved_instrument_count": len(resolved),
                    "resolved_instruments_sha256": resolved_hash,
                    "instrument_chunk_index": instrument_index,
                    "instrument_chunk_count": instrument_chunk_count,
                    "date_chunk_index": date_index,
                    "date_chunk_count": date_chunk_count,
                    "batch_index": batch_index,
                    "batch_count": batch_count,
                    "start_date": chunk_start.isoformat(),
                    "end_date": chunk_end.isoformat(),
                    "instruments": chunk_ids,
                    "frequency": "d",
                    "adjustflag": "3",
                }
                yield RawBatch(
                    source="baostock",
                    endpoint="query_daily_history_k_AStock",
                    request=request,
                    retrieved_at=self._clock(),
                    schema=DAILY_BAR_FIELDS,
                    rows=tuple(rows),
                )

    def _fetch_all_market_daily_bars(
        self,
        open_dates: Sequence[date],
        catalog_instruments: Sequence[InstrumentId],
    ) -> Iterable[RawBatch]:
        """Yield one validated all-market response for each exchange-open date."""
        catalog_ids = sorted(item.canonical() for item in catalog_instruments)
        catalog_hash = hashlib.sha256("\n".join(catalog_ids).encode()).hexdigest()
        for index, trading_day in enumerate(open_dates, start=1):
            self._require_completed_session(trading_day)
            rows = self._fetch_all_market_rows(trading_day)
            codes = sorted(cast(str, row["code"]) for row in rows)
            request: dict[str, JsonValue] = {
                "api": "query_daily_history_k_AStock",
                "scope": "ALL",
                "date": trading_day.isoformat(),
                "frequency": "d",
                "catalog_instrument_count": len(catalog_ids),
                "catalog_instruments_sha256": catalog_hash,
                "response_instrument_count": len(codes),
                "response_instruments_sha256": hashlib.sha256(
                    "\n".join(codes).encode()
                ).hexdigest(),
            }
            self._logger.info(
                "BaoStock all-market daily date completed",
                extra={
                    "event": "baostock_all_market_daily_progress",
                    "date": trading_day.isoformat(),
                    "completed_dates": index,
                    "total_dates": len(open_dates),
                    "response_rows": len(rows),
                },
            )
            yield RawBatch(
                source=self.provider,
                endpoint="query_daily_history_k_AStock",
                request=request,
                retrieved_at=self._clock(),
                schema=DAILY_BAR_FIELDS,
                rows=tuple(rows),
            )

    def _require_completed_session(self, trading_day: date) -> None:
        now = self._clock().astimezone(_SHANGHAI)
        close_at = datetime.combine(trading_day, clock_time(15, 0), _SHANGHAI)
        if now < close_at:
            raise ValueError("daily market session is not complete")

    def fetch_instruments(self) -> Iterable[RawBatch]:
        """获取完整的供应商原生证券目录。

        入参：
            无。
        返回值：
            返回从供应商获取证券集合后的证券集合（``Iterable[RawBatch]``）。
        异常：
            _state_error：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        if not self._logged_in:
            raise self._state_error("fetch_instruments")
        rows = self._read_cursor(
            "query_stock_basic",
            lambda: self._gateway.query_stock_basic(code="", code_name=""),
            INSTRUMENT_FIELDS,
        )
        self._catalog = BaoStockHistoricalCatalog.from_raw_rows(rows)
        # The all-market A-share route excludes indexes, so the configured
        # benchmark indexes are declared explicitly only when BaoStock did not
        # already include them in query_stock_basic.
        provider_codes = {str(row["code"]) for row in rows}
        index_rows = tuple(
            dict(
                zip(
                    INSTRUMENT_FIELDS,
                    (code, _INDEX_NAMES.get(code, code), "", "", "2", "1"),
                    strict=True,
                )
            )
            for code in self._config.index_codes
            if code not in provider_codes
        )
        yield RawBatch(
            source=self.provider,
            endpoint="query_stock_basic",
            request={"code": "", "code_name": "", "scope": "ALL_HISTORICAL"},
            retrieved_at=self._clock(),
            schema=INSTRUMENT_FIELDS,
            rows=(*rows, *index_rows),
        )

    def benchmark_bars_request(self, trade_date: date) -> Mapping[str, JsonValue]:
        """构造单个开市日的基准指数行情请求。

        入参：
            trade_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回行情请求（``Mapping[str, JsonValue]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return {
            "api": "query_history_k_data_plus",
            "scope": "BENCHMARK",
            "instrument": _BENCHMARK_CODE,
            "date": trade_date.isoformat(),
            "frequency": "d",
            "adjustflag": "3",
        }

    def index_bar_requests(
        self, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """构造配置中各基准指数的区间请求。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回行情``requests``（``tuple[Mapping[str, JsonValue], ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        return tuple(
            {
                "endpoint": "query_history_k_data_plus",
                "index_id": code,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "frequency": "d",
            }
            for code in self._config.index_codes
        )

    def etf_bar_requests(
        self, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """构造 ETF 白名单中各证券的区间日线请求。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            每只 ETF 按供应商允许跨度切分后的稳定排序规范请求。
        异常：
            ``ValueError``：日期范围非法。
        """
        if start > end:
            raise ValueError("start must not follow end")
        return tuple(
            {
                "endpoint": "query_etf_history_k_data_plus",
                "etf_id": code,
                "start_date": chunk_start.isoformat(),
                "end_date": chunk_end.isoformat(),
                "frequency": "d",
                "adjustflag": "3",
            }
            for code in self._config.etf_codes
            for chunk_start, chunk_end in self._date_chunks(start, end)
        )

    def fetch_etf_bars(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取一个白名单 ETF 的闭区间未复权 Raw 行情。

        入参：
            request：由 ``etf_bar_requests`` 构造的规范请求。
        返回值：
            一个 ETF Raw 批次。
        异常：
            ``QuantError``、``ValueError``：会话、请求或供应商响应不合法。
        """
        if not self._logged_in:
            raise self._state_error("fetch_etf_bars")
        code = _BaoStockSourceSupport.required_request_text(request, "etf_id")
        start = date.fromisoformat(
            _BaoStockSourceSupport.required_request_text(request, "start_date")
        )
        end = date.fromisoformat(
            _BaoStockSourceSupport.required_request_text(request, "end_date")
        )
        if code not in self._config.etf_codes:
            raise ValueError("ETF request is outside the configured whitelist")
        rows = self._fetch_instrument_rows(
            from_baostock_code(code), start=start, end=end
        )
        yield RawBatch(
            source=self.provider,
            endpoint="query_etf_history_k_data_plus",
            request=request,
            retrieved_at=self._clock(),
            schema=DAILY_BAR_FIELDS,
            rows=tuple(rows),
        )

    def fetch_index_bars(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取一个指数闭区间的未复权 Raw 行情。

        入参：
            request：包含完整业务字段的规范化供应商请求。
        返回值：
            返回从供应商获取索引行情后的索引行情（``Iterable[RawBatch]``）。
        异常：
            _state_error：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        if not self._logged_in:
            raise self._state_error("fetch_index_bars")
        code = _BaoStockSourceSupport.required_request_text(request, "index_id")
        start = _BaoStockSourceSupport.required_request_text(request, "start_date")
        end = _BaoStockSourceSupport.required_request_text(request, "end_date")
        rows = self._read_cursor(
            "query_history_k_data_plus",
            lambda: self._gateway.query_history_k_data_plus(
                code,
                _INDEX_BAR_FIELD_ARGUMENT,
                start_date=start,
                end_date=end,
                frequency="d",
                adjustflag="3",
            ),
            INDEX_BAR_FIELDS,
        )
        yield RawBatch(
            source=self.provider,
            endpoint="query_history_k_data_plus",
            request=request,
            retrieved_at=self._clock(),
            schema=INDEX_BAR_FIELDS,
            rows=tuple(rows),
        )

    def fetch_benchmark_bars(self, trade_date: date) -> Iterable[RawBatch]:
        """获取单个开市日的基准指数 Raw 批次。

        入参：
            trade_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回从供应商获取基准行情后的基准行情（``Iterable[RawBatch]``）。
        异常：
            _state_error：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        if not self._logged_in:
            raise self._state_error("fetch_benchmark_bars")
        self._require_completed_session(trade_date)
        rows = self._fetch_benchmark_rows(trade_date)
        yield RawBatch(
            source=self.provider,
            endpoint="query_history_k_data_plus",
            request=self.benchmark_bars_request(trade_date),
            retrieved_at=self._clock(),
            schema=INDEX_BAR_FIELDS,
            rows=tuple(rows),
        )

    def financial_requests(
        self, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """为报告期末闭区间构造已越过披露截止日的财务请求单元。

        入参：
            start：最早报告期末日。
            end：最晚报告期末日。
        返回值：
            返回``requests``（``tuple[Mapping[str, JsonValue], ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        if start > end:
            raise ValueError("financial report-period start must not follow end")
        cutoff = self._clock().astimezone(_SHANGHAI).date()
        requests: list[Mapping[str, JsonValue]] = []
        for listing in self._financial_listings():
            for year in range(start.year, end.year + 1):
                for quarter in range(1, 5):
                    period_end = financial_report_period_end(year, quarter)
                    if not start <= period_end <= end:
                        continue
                    if not financial_request_is_eligible(
                        year,
                        quarter,
                        cutoff=cutoff,
                    ):
                        continue
                    if listing.list_date > period_end:
                        continue
                    if (
                        listing.delist_date is not None
                        and listing.delist_date < period_end
                    ):
                        continue
                    for endpoint in _FINANCIAL_APIS:
                        requests.append(
                            {
                                "endpoint": endpoint,
                                "instrument_id": listing.instrument_id.canonical(),
                                "report_year": year,
                                "report_quarter": quarter,
                            }
                        )
        return tuple(requests)

    def fetch_financials(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取一个报告单元的供应商原生财务批次。

        入参：
            request：包含完整业务字段的规范化供应商请求。
        返回值：
            返回从供应商获取``financials``后的``financials``（``Iterable[RawBatch]``）。
        异常：
            ValueError、_state_error：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        if not self._logged_in:
            raise self._state_error("fetch_financials")
        year, quarter = self._financial_cell(request)
        instrument_id = InstrumentId.parse(
            _BaoStockSourceSupport.required_request_text(request, "instrument_id")
        )
        endpoint = _BaoStockSourceSupport.required_request_text(request, "endpoint")
        fields = FINANCIAL_FIELDS_BY_ENDPOINT.get(endpoint)
        if fields is None:
            raise ValueError(f"unsupported financial endpoint: {endpoint}")
        code = to_baostock_code(instrument_id)

        def perform_query() -> list[dict[str, JsonValue]]:
            queries: Mapping[
                str,
                Callable[..., BaoStockCursor],
            ] = {
                "query_dupont_data": self._gateway.query_dupont_data,
            }
            cursor = queries[endpoint](code=code, year=year, quarter=quarter)
            return self._consume_cursor(endpoint, cursor, fields)

        rows = self._retry(endpoint, perform_query)
        yield RawBatch(
            source=self.provider,
            endpoint=endpoint,
            request=request,
            retrieved_at=self._clock(),
            schema=fields,
            rows=tuple(rows),
        )

    def industry_requests(
        self, trading_days: Sequence[date]
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """为已完整结束的交易日构造全市场行业分类请求。

        入参：
            trading_days：按升序提供的已完整结束交易日集合。
        返回值：
            返回``requests``（``tuple[Mapping[str, JsonValue], ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ordered = tuple(sorted(set(trading_days)))
        if tuple(trading_days) != ordered:
            raise ValueError("industry trading_days must be unique and sorted")
        return tuple(
            {
                "api": "query_stock_industry",
                "scope": "ALL",
                "date": as_of.isoformat(),
                "as_of": as_of.isoformat(),
            }
            for as_of in ordered
        )

    def fetch_industry(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取指定时点的全市场行业分类批次。

        入参：
            request：包含完整业务字段的规范化供应商请求。
        返回值：
            返回从供应商获取行业分类后的行业分类（``Iterable[RawBatch]``）。
        异常：
            _state_error：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        if not self._logged_in:
            raise self._state_error("fetch_industry")
        as_of_text = _BaoStockSourceSupport.required_request_text(request, "as_of")
        operation = "query_stock_industry"

        def perform_query() -> list[dict[str, JsonValue]]:
            rows, _ = self._consume_adaptive(
                operation,
                self._gateway.query_stock_industry(code="", date=as_of_text),
                _INDUSTRY_REQUIRED_FIELDS,
            )
            return [
                {
                    "code": _BaoStockSourceSupport.row_text(row, "code"),
                    "code_name": _BaoStockSourceSupport.row_text(row, "code_name"),
                    "industry": _BaoStockSourceSupport.row_text(row, "industry"),
                    "industryClassification": _BaoStockSourceSupport.row_text(
                        row, "industryClassification"
                    ),
                    "updateDate": _BaoStockSourceSupport.row_text(row, "updateDate"),
                    "as_of_date": as_of_text,
                }
                for row in rows
            ]

        rows = self._retry(operation, perform_query)
        yield RawBatch(
            source=self.provider,
            endpoint="query_stock_industry",
            request=request,
            retrieved_at=self._clock(),
            schema=INDUSTRY_FIELDS,
            rows=tuple(rows),
        )

    def fetch_trade_calendar(self, start: date, end: date) -> Iterable[RawBatch]:
        """获取指定闭区间的供应商原生交易日历批次。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回从供应商获取交易交易日历后的交易交易日历（``Iterable[RawBatch]``）。
        异常：
            _state_error：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        if not self._logged_in:
            raise self._state_error("fetch_trade_calendar")
        batch, _ = self._load_trade_calendar(start, end)
        yield batch

    def calendar_trading_days(
        self, calendar_partition: PublishedPartition, start: date, end: date
    ) -> tuple[date, ...]:
        """从交易日历分区提取指定闭区间内的开市日期。

        入参：
            calendar_partition：调用接口所需的同名参数，具体约束见类型标注。
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回交易``days``（``tuple[date, ...]``）。
        异常：
            TypeError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        table = pq.read_table(calendar_partition.data_path)
        days: set[date] = set()
        for row in table.to_pylist():
            if row.get("is_trading_day") != "1":
                continue
            value = row.get("calendar_date")
            if not isinstance(value, str):
                raise TypeError("calendar_date must be a provider string")
            parsed = date.fromisoformat(value)
            if start <= parsed <= end:
                days.add(parsed)
        return tuple(sorted(days))

    def _resolve_instruments(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None,
    ) -> tuple[str, tuple[InstrumentId, ...]]:
        if instruments is None or len(instruments) == 0:
            if self._catalog is None:
                tuple(self.fetch_instruments())
            assert self._catalog is not None
            candidates = (
                listing.instrument_id
                for listing in self._catalog.list_instruments()
                if listing.provider_type == "1"
                and listing.list_date <= end
                and (listing.delist_date is None or listing.delist_date >= start)
            )
            return "ALL", self._sorted_unique(candidates)
        return "SELECTED", self._sorted_unique(instruments)

    def _load_trade_calendar(
        self, start: date, end: date
    ) -> tuple[RawBatch, tuple[date, ...]]:
        rows = self._read_cursor(
            "query_trade_dates",
            lambda: self._gateway.query_trade_dates(
                start_date=start.isoformat(), end_date=end.isoformat()
            ),
            TRADE_CALENDAR_FIELDS,
        )
        open_dates = tuple(
            date.fromisoformat(str(row["calendar_date"]))
            for row in rows
            if row["is_trading_day"] == "1"
        )
        batch = RawBatch(
            source=self.provider,
            endpoint="query_trade_dates",
            request={"start_date": start.isoformat(), "end_date": end.isoformat()},
            retrieved_at=self._clock(),
            schema=TRADE_CALENDAR_FIELDS,
            rows=tuple(rows),
        )
        return batch, open_dates

    @staticmethod
    def _sorted_unique(
        instruments: Iterable[InstrumentId],
    ) -> tuple[InstrumentId, ...]:
        return tuple(sorted(set(instruments), key=InstrumentId.canonical))

    @staticmethod
    def _chunks[T](values: Sequence[T], limit: int) -> Iterable[tuple[T, ...]]:
        for offset in range(0, len(values), limit):
            yield tuple(values[offset : offset + limit])

    def _date_chunks(self, start: date, end: date) -> Iterable[tuple[date, date]]:
        chunk_start = start
        span = timedelta(days=self._config.max_days_per_batch - 1)
        while chunk_start <= end:
            chunk_end = min(chunk_start + span, end)
            yield chunk_start, chunk_end
            chunk_start = chunk_end + timedelta(days=1)

    @staticmethod
    def _financial_cell(request: Mapping[str, JsonValue]) -> tuple[int, int]:
        return (
            _BaoStockSourceSupport.required_request_int(request, "report_year"),
            _BaoStockSourceSupport.required_request_int(request, "report_quarter"),
        )

    def _financial_instruments(self) -> tuple[InstrumentId, ...]:
        """Resolve the full stock-only catalog once, independent of any window."""
        return tuple(listing.instrument_id for listing in self._financial_listings())

    def _financial_listings(self) -> tuple[InstrumentListing, ...]:
        """Resolve deterministic stock listings with their complete lifecycle."""
        if self._catalog is None:
            tuple(self.fetch_instruments())
        assert self._catalog is not None
        candidates = (
            listing
            for listing in self._catalog.list_instruments()
            if listing.provider_type == "1"
        )
        by_instrument = {listing.instrument_id: listing for listing in candidates}
        return tuple(
            sorted(
                by_instrument.values(),
                key=lambda listing: listing.instrument_id.canonical(),
            )
        )

    def _fetch_instrument_rows(
        self,
        instrument_id: InstrumentId,
        *,
        start: date,
        end: date,
    ) -> list[dict[str, JsonValue]]:
        operation = "query_history_k_data_plus"
        expected_code = to_baostock_code(instrument_id)

        def perform_query() -> list[dict[str, JsonValue]]:
            rows = self._consume_cursor(
                operation,
                self._gateway.query_history_k_data_plus(
                    expected_code,
                    _DAILY_BAR_FIELD_ARGUMENT,
                    start_date=start.isoformat(),
                    end_date=end.isoformat(),
                    frequency="d",
                    adjustflag="3",
                ),
                DAILY_BAR_FIELDS,
            )
            self._validate_daily_bar_rows(
                operation,
                rows,
                expected_code=expected_code,
                expected_dates=(start, end),
            )
            return rows

        return self._retry(operation, perform_query)

    def _fetch_all_market_rows(self, trading_day: date) -> list[dict[str, JsonValue]]:
        operation = "query_daily_history_k_AStock"

        def perform_query() -> list[dict[str, JsonValue]]:
            rows = self._consume_cursor(
                operation,
                self._gateway.query_daily_history_k_AStock(trading_day.isoformat()),
                DAILY_BAR_FIELDS,
            )
            if not rows:
                raise self._empty_open_day_error(trading_day)
            self._validate_daily_bar_rows(
                operation,
                rows,
                expected_dates=trading_day,
            )
            return rows

        return self._retry(operation, perform_query)

    def _fetch_benchmark_rows(self, trading_day: date) -> list[dict[str, JsonValue]]:
        """Fetch one unadjusted benchmark-index bar for one exchange-open date."""
        operation = "query_history_k_data_plus"

        def perform_query() -> list[dict[str, JsonValue]]:
            rows = self._consume_cursor(
                operation,
                self._gateway.query_history_k_data_plus(
                    _BENCHMARK_CODE,
                    _DAILY_BAR_FIELD_ARGUMENT,
                    start_date=trading_day.isoformat(),
                    end_date=trading_day.isoformat(),
                    frequency="d",
                    adjustflag="3",
                ),
                DAILY_BAR_FIELDS,
            )
            if not rows:
                raise self._empty_benchmark_day_error(trading_day)
            self._validate_daily_bar_rows(
                operation,
                rows,
                expected_code=_BENCHMARK_CODE,
                expected_dates=trading_day,
            )
            return rows

        return self._retry(operation, perform_query)

    def _read_cursor(
        self,
        operation: str,
        query: Callable[[], BaoStockCursor],
        fields: tuple[str, ...],
    ) -> list[dict[str, JsonValue]]:
        def perform_query() -> list[dict[str, JsonValue]]:
            return self._consume_cursor(operation, query(), fields)

        return self._retry(operation, perform_query)

    def _consume_cursor(
        self,
        operation: str,
        cursor: BaoStockCursor,
        fields: tuple[str, ...],
    ) -> list[dict[str, JsonValue]]:
        self._raise_provider_error(cursor, operation=operation)
        if tuple(cursor.fields) != fields:
            raise self._schema_error(
                operation,
                f"cursor fields do not match the fixed {operation} schema",
                expected=list(fields),
                actual=list(cursor.fields),
            )
        rows: list[dict[str, JsonValue]] = []
        while True:
            has_row = cursor.next()
            self._raise_provider_error(cursor, operation=operation)
            if not has_row:
                return rows
            values = tuple(cursor.get_row_data())
            if len(values) != len(fields):
                raise self._schema_error(
                    operation,
                    f"cursor row length does not match the fixed {operation} schema",
                    expected=len(fields),
                    actual=len(values),
                )
            for field, value in zip(fields, values, strict=True):
                if not isinstance(value, str):
                    raise self._schema_error(
                        operation,
                        f"cursor field {field} must be a provider-native string",
                        expected="str",
                        actual=type(value).__name__,
                    )
            rows.append(dict(zip(fields, values, strict=True)))

    def _consume_adaptive(
        self,
        operation: str,
        cursor: BaoStockCursor,
        required_fields: tuple[str, ...],
    ) -> tuple[list[dict[str, JsonValue]], tuple[str, ...]]:
        """Consume a cursor whose full field set is server-side and unverified.

        Only the required subset is validated by name; any extra server fields
        ride along in the raw rows so future adapters can extract them without
        a re-fetch.  All values are validated as provider-native strings.
        """
        self._raise_provider_error(cursor, operation=operation)
        fields = tuple(cursor.fields)
        missing = [name for name in required_fields if name not in fields]
        if missing:
            raise self._schema_error(
                operation,
                "cursor fields are missing required columns",
                expected=list(required_fields),
                actual=list(fields),
            )
        rows: list[dict[str, JsonValue]] = []
        while True:
            has_row = cursor.next()
            self._raise_provider_error(cursor, operation=operation)
            if not has_row:
                return rows, fields
            values = tuple(cursor.get_row_data())
            if len(values) != len(fields):
                raise self._schema_error(
                    operation,
                    "cursor row length does not match the server field set",
                    expected=len(fields),
                    actual=len(values),
                )
            for field, value in zip(fields, values, strict=True):
                if not isinstance(value, str):
                    raise self._schema_error(
                        operation,
                        f"cursor field {field} must be a provider-native string",
                        expected="str",
                        actual=type(value).__name__,
                    )
            rows.append(dict(zip(fields, values, strict=True)))

    def _validate_daily_bar_rows(
        self,
        operation: str,
        rows: Sequence[dict[str, JsonValue]],
        *,
        expected_dates: date | tuple[date, date],
        expected_code: str | None = None,
    ) -> None:
        seen_keys: set[tuple[str, str]] = set()
        for row in rows:
            raw_date = cast(str, row["date"])
            raw_code = cast(str, row["code"])
            try:
                parsed_code = from_baostock_code(raw_code)
            except ValueError as error:
                raise self._schema_error(
                    operation,
                    "daily bar code is not a canonical BaoStock security code",
                    expected="sh. or sz. followed by six ASCII digits",
                    actual=raw_code,
                ) from error
            if (
                expected_code is not None
                and to_baostock_code(parsed_code) != expected_code
            ):
                raise self._schema_error(
                    operation,
                    "selected daily bar code does not match the queried security",
                    expected=expected_code,
                    actual=raw_code,
                )

            try:
                parsed_date = date.fromisoformat(raw_date)
            except ValueError as error:
                raise self._schema_error(
                    operation,
                    "daily bar date is not a valid ISO calendar date",
                    expected="YYYY-MM-DD",
                    actual=raw_date,
                ) from error
            if parsed_date.isoformat() != raw_date:
                raise self._schema_error(
                    operation,
                    "daily bar date is not in canonical ISO format",
                    expected=parsed_date.isoformat(),
                    actual=raw_date,
                )
            if isinstance(expected_dates, date):
                if parsed_date != expected_dates:
                    raise self._schema_error(
                        operation,
                        "all-market daily bar date does not match the requested date",
                        expected=expected_dates.isoformat(),
                        actual=raw_date,
                    )
            else:
                start, end = expected_dates
                if not start <= parsed_date <= end:
                    raise self._schema_error(
                        operation,
                        "selected daily bar date falls outside the requested chunk",
                        expected=[start.isoformat(), end.isoformat()],
                        actual=raw_date,
                    )

            if row["adjustflag"] != "3":
                raise self._schema_error(
                    operation,
                    "daily bar response must use adjustflag 3",
                    expected="3",
                    actual=row["adjustflag"],
                )

            primary_key = (raw_date, raw_code)
            if primary_key in seen_keys:
                raise self._schema_error(
                    operation,
                    "daily bar response contains a duplicate date and code",
                    expected="unique (date, code)",
                    actual=list(primary_key),
                )
            seen_keys.add(primary_key)

    def _retry[T](self, operation: str, function: Callable[[], T]) -> T:
        for attempt in range(self._config.max_attempts):
            should_reconnect = False
            try:
                return function()
            except QuantError as error:
                if (
                    not error.detail.retryable
                    or attempt == self._config.max_attempts - 1
                ):
                    raise
                should_reconnect = operation not in {"login", "logout"}
            except (TimeoutError, ConnectionError, OSError) as error:
                if attempt == self._config.max_attempts - 1:
                    raise self._transport_error(operation, error) from error
                should_reconnect = operation not in {"login", "logout"}
            if should_reconnect:
                self._logged_in = False
                self.login()
            self._sleep(self._config.retry_backoff_seconds[attempt])
        raise AssertionError("validated retry configuration made loop unreachable")

    def _raise_provider_error(
        self,
        response: BaoStockResponse,
        *,
        operation: str,
    ) -> None:
        if response.error_code == "0":
            return
        retryable = response.error_code in self._config.retryable_error_codes
        remediation = (
            "stop retrying and contact the BaoStock administrator to remove the "
            "account or IP from the blacklist"
            if response.error_code == "10001011"
            else "retry if configured or inspect the BaoStock provider response"
        )
        raise QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_BAOSTOCK",
                severity=Severity.SEVERE,
                message=f"BaoStock {operation} failed: {response.error_msg}",
                context={
                    "operation": operation,
                    "provider_error_code": response.error_code,
                    "provider_error_message": response.error_msg,
                },
                remediation=remediation,
                retryable=retryable,
            )
        )

    @staticmethod
    def _state_error(operation: str) -> QuantError:
        return QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_STATE",
                severity=Severity.SEVERE,
                message="BaoStock client must be logged in before fetching data",
                context={"operation": operation, "provider": "baostock"},
                remediation="call login() before fetching provider data",
                retryable=False,
            )
        )

    @staticmethod
    def _schema_error(
        operation: str, message: str, *, expected: object, actual: object
    ) -> QuantError:
        return QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_BAOSTOCK_SCHEMA",
                severity=Severity.SEVERE,
                message=message,
                context={
                    "operation": operation,
                    "expected": expected,
                    "actual": actual,
                },
                remediation="inspect the provider schema before accepting raw data",
                retryable=False,
            )
        )

    @staticmethod
    def _empty_open_day_error(trading_day: date) -> QuantError:
        return QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_BAOSTOCK_EMPTY_OPEN_DAY",
                severity=Severity.FATAL,
                message="BaoStock returned no A-share daily bars for an open trading day",
                context={
                    "operation": "query_daily_history_k_AStock",
                    "date": trading_day.isoformat(),
                },
                remediation="retry the date or inspect BaoStock completeness",
                retryable=True,
            )
        )

    @staticmethod
    def _empty_benchmark_day_error(trading_day: date) -> QuantError:
        return QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_BAOSTOCK_EMPTY_BENCHMARK_DAY",
                severity=Severity.FATAL,
                message="BaoStock returned no benchmark bars for an open trading day",
                context={
                    "operation": "query_history_k_data_plus",
                    "instrument": _BENCHMARK_CODE,
                    "date": trading_day.isoformat(),
                },
                remediation="retry the date or inspect BaoStock benchmark completeness",
                retryable=True,
            )
        )

    @staticmethod
    def _transport_error(operation: str, error: Exception) -> QuantError:
        return QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_BAOSTOCK",
                severity=Severity.SEVERE,
                message=f"BaoStock {operation} transport failed: {error}",
                context={
                    "operation": operation,
                    "transport_error_type": type(error).__name__,
                    "transport_error_message": str(error),
                },
                remediation="retry after checking provider connectivity",
                retryable=True,
            )
        )


class BaoStockCalendarPolicy:
    """基于供应商交易日历证据解析准确的采集窗口。

    入参：
        client：用于读取 BaoStock 交易日历证据的数据源客户端。
        clock：提供当前带时区时间、支持确定性测试的可注入时钟。
        completion_hour：上海时区中当日行情被视为完整可采集的小时边界。
    返回值：
        构造并返回 ``BaoStockCalendarPolicy`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def __init__(
        self,
        client: BaoStockClient,
        *,
        clock: Callable[[], datetime] = _BaoStockSourceSupport.utc_now,
        completion_hour: int = 18,
    ) -> None:
        self._client = client
        self._clock = clock
        self._completion_hour = completion_hour
        self._timezone = ZoneInfo("Asia/Shanghai")

    def bootstrap_window(self, years: int) -> tuple[date, date]:
        """计算首次构建所需的交易日历窗口。

        入参：
            years：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回窗口（``tuple[date, date]``）。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        if years <= 0:
            raise ValueError("years must be positive")
        end = self.latest_complete_day()
        try:
            target = end.replace(year=end.year - years)
        except ValueError:
            target = end.replace(year=end.year - years, day=28)
        candidates = self._open_dates(target, target + timedelta(days=31))
        if not candidates:
            raise ValueError("provider calendar has no bootstrap start day")
        return candidates[0], end

    def latest_complete_day(self) -> date:
        """返回当前时钟下最近一个已完整结束的交易日。

        入参：
            无。
        返回值：
            返回上海时区当前自然日之前最近一个已经完整结束的交易日。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        local_now = self._clock().astimezone(self._timezone)
        candidate = local_now.date()
        if local_now.hour < self._completion_hour:
            candidate -= timedelta(days=1)
        candidates = self._open_dates(candidate - timedelta(days=31), candidate)
        if not candidates:
            raise ValueError("provider calendar has no latest complete trading day")
        return candidates[-1]

    def explicit_window(self, start: date, end: date) -> tuple[date, date]:
        """校验并返回用户明确指定的日期闭区间。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回窗口（``tuple[date, date]``）。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        if start > end:
            raise ValueError("start must not follow end")
        candidates = self._open_dates(start, end)
        if not candidates:
            raise ValueError("requested range contains no trading day")
        return candidates[0], candidates[-1]

    def update_window(self, watermark: date, overlap_days: int) -> tuple[date, date]:
        """根据当前数据状态计算日常增量窗口。

        入参：
            watermark：调用接口所需的同名参数，具体约束见类型标注。
            overlap_days：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回窗口（``tuple[date, date]``）。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        if overlap_days < 0:
            raise ValueError("overlap_days must be non-negative")
        end = self.latest_complete_day()
        candidates = self._open_dates(watermark - timedelta(days=45), end)
        at_or_before = [item for item in candidates if item <= watermark]
        if not at_or_before:
            raise ValueError("provider calendar does not cover the update watermark")
        index = max(0, len(at_or_before) - overlap_days - 1)
        return at_or_before[index], end

    def _open_dates(self, start: date, end: date) -> list[date]:
        self._client.login()
        try:
            batches = tuple(self._client.fetch_trade_calendar(start, end))
        finally:
            self._client.close()
        dates: list[date] = []
        for batch in batches:
            for row in batch.rows:
                if row.get("is_trading_day") == "1":
                    value = row.get("calendar_date")
                    if not isinstance(value, str):
                        raise ValueError("calendar_date must be a provider string")
                    dates.append(date.fromisoformat(value))
        return sorted(set(dates))
