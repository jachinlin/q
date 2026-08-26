"""封装 Tushare SDK 与全市场请求生成策略。"""

from __future__ import annotations

import importlib
import math
import secrets
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Protocol, cast
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.contracts import JsonValue, PublishedPartition, RawBatch
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MARKET_DATE_ENDPOINTS = frozenset(
    {"daily_vip", "adj_factor", "fund_daily", "fund_adj", "daily_basic", "suspend_d", "stock_st"}
)
_INDEX_MARKETS = ("MSCI", "CSI", "SSE", "SZSE", "CICC", "SW", "OTH")
_STOCK_STATUSES = ("L", "D", "P", "G")
_SW2021_L1_CODES = (
    "801010", "801030", "801040", "801050", "801080", "801110", "801120",
    "801130", "801140", "801150", "801160", "801170", "801180", "801200",
    "801210", "801230", "801710", "801720", "801730", "801740", "801750",
    "801760", "801770", "801780", "801790", "801880", "801890", "801950",
    "801960", "801970", "801980",
)
_ROW_LIMITS: Mapping[str, int] = {
    "stock_basic": 6000,
    "daily_vip": 10000,
    "daily_basic": 6000,
    "fund_daily": 2000,
    "fund_adj": 2000,
    "index_member_all": 2000,
}

_FIELDS: Mapping[str, tuple[str, ...]] = {
    "stock_basic": (
        "ts_code", "symbol", "name", "area", "industry", "fullname", "enname",
        "cnspell", "market", "exchange", "curr_type", "list_status", "list_date",
        "delist_date", "is_hs", "act_name", "act_ent_type",
    ),
    "fund_basic": (
        "ts_code", "name", "management", "custodian", "fund_type", "found_date",
        "due_date", "list_date", "issue_date", "delist_date", "issue_amount",
        "m_fee", "c_fee", "duration_year", "p_value", "min_amount", "exp_return",
        "benchmark", "status", "invest_type", "type", "trustee", "purc_startdate",
        "redm_startdate", "market",
    ),
    "index_basic": (
        "ts_code", "name", "fullname", "market", "publisher", "index_type",
        "category", "base_date", "base_point", "list_date", "weight_rule", "desc",
        "exp_date",
    ),
    "trade_cal": ("exchange", "cal_date", "is_open", "pretrade_date"),
    "daily_vip": (
        "ts_code", "trade_date", "open", "high", "low", "close", "pre_close",
        "change", "pct_chg", "vol", "amount", "ah_vol", "ah_amount",
    ),
    "adj_factor": ("ts_code", "trade_date", "adj_factor"),
    "fund_daily": (
        "ts_code", "trade_date", "open", "high", "low", "close", "pre_close",
        "change", "pct_chg", "vol", "amount",
    ),
    "fund_adj": ("ts_code", "trade_date", "adj_factor"),
    "index_daily": (
        "ts_code", "trade_date", "close", "open", "high", "low", "pre_close",
        "change", "pct_chg", "vol", "amount",
    ),
    "daily_basic": (
        "ts_code", "trade_date", "close", "turnover_rate", "turnover_rate_f",
        "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio",
        "dv_ttm", "total_share", "float_share", "free_share", "total_mv",
        "circ_mv", "limit_status",
    ),
    "suspend_d": ("ts_code", "trade_date", "suspend_timing", "suspend_type"),
    "stock_st": ("ts_code", "name", "trade_date", "type", "type_name"),
    "index_classify": (
        "index_code", "industry_name", "level", "industry_code", "is_pub",
        "parent_code", "src",
    ),
    "index_member_all": (
        "l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name",
        "ts_code", "name", "in_date", "out_date", "is_new",
    ),
}

_FINANCIAL_RENAMES = {
    "instrument_id": "ts_code",
    "announcement_date": "ann_date",
    "report_period": "end_date",
    "revision": "update_flag",
}
_FINANCIAL_FIELDS = tuple(
    _FINANCIAL_RENAMES.get(name, name)
    for name in CANONICAL_SCHEMAS[
        DatasetKind.STOCK_FINANCIAL_INDICATOR
    ].columns.names()
    if name
    not in {
        "source", "available_at", "availability_source", "pit_usable", "ingested_at",
        "revision",
    }
)
_FIELDS = {**_FIELDS, "fina_indicator_vip": _FINANCIAL_FIELDS}


class TushareFrame(Protocol):
    """约束 SDK 响应。入参：由 SDK 实现。返回值：列与记录。异常：实现按原类型传播。"""

    @property
    def columns(self) -> Sequence[object]:
        """返回列。入参：无。返回值：列序列。异常：实现按原类型传播。"""
        ...

    def to_dict(self, orient: str) -> list[dict[str, object]]:
        """转为记录。入参：方向。返回值：记录列表。异常：方向非法时抛出。"""
        ...


class TushareApi(Protocol):
    """约束 Pro 对象。入参：无。返回值：动态端点。异常：调用异常按原类型传播。"""


class TushareGateway(Protocol):
    """约束 SDK 边界。入参：令牌与请求。返回值：响应。异常：SDK 异常按原类型传播。"""

    def connect(self, token: str) -> TushareApi:
        """连接 Pro。入参：令牌。返回值：API。异常：认证失败时传播。"""
        ...

    def call(
        self,
        api: TushareApi,
        endpoint: str,
        params: Mapping[str, JsonValue],
        fields: tuple[str, ...],
    ) -> TushareFrame:
        """调用端点。入参：API、端点、参数和字段。返回值：响应帧。异常：调用失败时传播。"""
        ...


class TushareSdkGateway:
    """隔离 SDK。入参：连接或调用参数。返回值：SDK 结果。异常：SDK 异常按原类型传播。"""

    def connect(self, token: str) -> TushareApi:
        """连接 Pro。入参：令牌。返回值：API。异常：导入或认证失败时传播。"""
        module = importlib.import_module("tushare")
        factory = module.pro_api
        return cast(TushareApi, factory(token))

    def call(
        self,
        api: TushareApi,
        endpoint: str,
        params: Mapping[str, JsonValue],
        fields: tuple[str, ...],
    ) -> TushareFrame:
        """调用端点。入参：API、端点、参数和字段。返回值：响应帧。异常：调用失败时传播。"""
        method = getattr(api, endpoint)
        kwargs = {key: value for key, value in params.items() if key != "endpoint"}
        kwargs["fields"] = ",".join(fields)
        return cast(TushareFrame, method(**kwargs))


@dataclass(frozen=True, slots=True)
class TushareConfig:
    """定义连接配置。入参：令牌、基准与重试。返回值：配置。异常：配置非法时抛出。"""

    token: str = field(repr=False)
    benchmark_indexes: tuple[str, ...]
    max_attempts: int = 5
    retry_backoff_seconds: tuple[float, ...] = (1.0, 2.0, 4.0, 8.0)
    token_provider: Callable[[], str | None] | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not self.token.strip() and self.token_provider is None:
            raise ValueError("QUANT_TUSHARE_TOKEN must not be empty")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if len(self.retry_backoff_seconds) != self.max_attempts - 1:
            raise ValueError("retry_backoff_seconds must cover every retry")
        if not self.benchmark_indexes or len(set(self.benchmark_indexes)) != len(
            self.benchmark_indexes
        ):
            raise ValueError("benchmark_indexes must be nonempty and unique")

    def resolved_token(self) -> str:
        """在连接边界解析最新数据源 Token。

        入参：无。
        返回值：动态 Provider 优先、静态配置回退的非空 Token。
        异常：Provider 类型错误时抛出类型错误；未配置时抛出受控 ``QuantError``。
        """
        candidate = self.token_provider() if self.token_provider is not None else self.token
        if candidate is not None and not isinstance(candidate, str):
            raise TypeError("data source token provider must return a string or None")
        if candidate is None or not candidate.strip():
            raise QuantError(
                ErrorDetail(
                    code="TUSHARE_TOKEN_MISSING",
                    severity=Severity.SEVERE,
                    message="data source token is not configured",
                    context={},
                    remediation="open Dashboard settings and configure the data source token",
                    retryable=False,
                )
            )
        return candidate


class TushareClient:
    """采集全市场数据。入参：网关、配置与时钟。返回值：客户端。异常：依赖非法时抛出。"""

    provider = "tushare"

    def __init__(
        self,
        gateway: TushareGateway,
        config: TushareConfig,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._gateway = gateway
        self._config = config
        self._clock = clock
        self._sleeper = sleeper
        self._api: TushareApi | None = None
        self._connected_token: str | None = None

    def login(self) -> None:
        """登录。入参：无。返回值：无。异常：认证失败时传播。"""
        token = self._config.resolved_token()
        if self._api is None or self._connected_token is None or not secrets.compare_digest(
            self._connected_token, token
        ):
            try:
                self._api = self._gateway.connect(token)
            except Exception as error:
                raise QuantError(
                    ErrorDetail(
                        code="TUSHARE_AUTH_FAILED",
                        severity=Severity.SEVERE,
                        message="Tushare authentication failed",
                        context={"error_type": type(error).__name__},
                        remediation="verify the data source token in Dashboard settings",
                        retryable=False,
                    )
                ) from error
            self._connected_token = token

    def close(self) -> None:
        """关闭。入参：无。返回值：无。异常：无。"""
        self._api = None
        self._connected_token = None

    def requests(
        self, endpoint: str, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """构造请求。入参：端点和日期范围。返回值：请求单元。异常：端点或日期非法时抛出。"""
        if start > end:
            raise ValueError("request start must not follow end")
        if endpoint not in _FIELDS:
            raise ValueError(f"unsupported Tushare endpoint: {endpoint}")
        fields = ",".join(_FIELDS[endpoint])
        base: dict[str, JsonValue] = {"endpoint": endpoint, "fields": fields}
        if endpoint == "stock_basic":
            return tuple({**base, "list_status": status} for status in _STOCK_STATUSES)
        if endpoint == "fund_basic":
            return ({**base, "market": "E"},)
        if endpoint == "index_basic":
            return tuple({**base, "market": market} for market in _INDEX_MARKETS)
        if endpoint == "trade_cal":
            return (
                {
                    **base,
                    "exchange": "SSE",
                    "start_date": start.strftime("%Y%m%d"),
                    "end_date": end.strftime("%Y%m%d"),
                },
            )
        if endpoint in _MARKET_DATE_ENDPOINTS:
            if start != end:
                raise ValueError(f"{endpoint} requires one market trade date")
            request = {**base, "trade_date": start.strftime("%Y%m%d")}
            if endpoint == "suspend_d":
                return tuple(
                    {**request, "suspend_type": kind} for kind in ("S", "R")
                )
            return (request,)
        if endpoint == "index_daily":
            return tuple(
                {
                    **base,
                    "ts_code": code,
                    "start_date": start.strftime("%Y%m%d"),
                    "end_date": end.strftime("%Y%m%d"),
                }
                for code in self._config.benchmark_indexes
            )
        if endpoint == "fina_indicator_vip":
            return tuple(
                {**base, "period": period.strftime("%Y%m%d")}
                for period in self._report_periods(start, end)
            )
        if endpoint == "index_classify":
            return ({**base, "level": "L1", "src": "SW2021"},)
        if endpoint == "index_member_all":
            return tuple(
                {**base, "l1_code": code}
                for code in _SW2021_L1_CODES
            )
        raise AssertionError("endpoint request policy is incomplete")

    def fetch(
        self, endpoint: str, request: Mapping[str, JsonValue]
    ) -> Iterable[RawBatch]:
        """执行请求。入参：端点和请求。返回值：Raw 批次。异常：逐证券或响应非法时抛出。"""
        if request.get("endpoint") != endpoint:
            raise ValueError("request endpoint does not match fetch endpoint")
        if endpoint != "index_daily" and "ts_code" in request:
            raise ValueError("only index_daily may contain ts_code")
        if self._api is None:
            raise RuntimeError("Tushare client is not logged in")
        fields = _FIELDS[endpoint]
        params = {
            key: value
            for key, value in request.items()
            if key not in {"endpoint", "fields"}
        }
        frame = self._call_with_retry(endpoint, params, fields)
        observed_columns = tuple(str(item) for item in frame.columns)
        if observed_columns != fields:
            raise ValueError(
                f"Tushare {endpoint} schema drift: expected={fields}, "
                f"observed={observed_columns}"
            )
        records = frame.to_dict("records")
        limit = _ROW_LIMITS.get(endpoint)
        if limit is not None and len(records) >= limit:
            raise ValueError(f"Tushare {endpoint} response may be truncated at {limit}")
        rows = tuple(
            {field: self._raw_value(record.get(field)) for field in fields}
            for record in records
        )
        return (
            RawBatch(
                source=self.provider,
                endpoint=endpoint,
                request=request,
                retrieved_at=self._clock(),
                schema=fields,
                rows=rows,
            ),
        )

    def calendar_trading_days(
        self, calendar_partition: PublishedPartition, start: date, end: date
    ) -> tuple[date, ...]:
        """读取交易日。入参：日历分区和日期范围。返回值：开市日。异常：Schema 非法时抛出。"""
        table = pq.read_table(calendar_partition.data_path)
        fields = set(table.column_names)
        if not {"cal_date", "is_open"}.issubset(fields):
            raise ValueError("trade_cal Raw schema is incomplete")
        dates = table.column("cal_date").to_pylist()
        flags = table.column("is_open").to_pylist()
        parsed = sorted(
            date.fromisoformat(f"{value[0:4]}-{value[4:6]}-{value[6:8]}")
            for value, flag in zip(dates, flags, strict=True)
            if value is not None and str(flag) == "1"
        )
        return tuple(item for item in parsed if start <= item <= end)

    def _call_with_retry(
        self,
        endpoint: str,
        params: Mapping[str, JsonValue],
        fields: tuple[str, ...],
    ) -> TushareFrame:
        assert self._api is not None
        for attempt in range(self._config.max_attempts):
            try:
                return self._gateway.call(self._api, endpoint, params, fields)
            except (ConnectionError, OSError, TimeoutError):
                if attempt + 1 == self._config.max_attempts:
                    raise
                self._sleeper(self._config.retry_backoff_seconds[attempt])
        raise AssertionError("retry loop did not return or raise")

    @staticmethod
    def _raw_value(value: object) -> JsonValue:
        if value is None:
            return None
        if isinstance(value, float) and math.isnan(value):
            return None
        if isinstance(value, (date, datetime)):
            return value.strftime("%Y%m%d")
        return str(value)

    @staticmethod
    def _report_periods(start: date, end: date) -> tuple[date, ...]:
        periods: list[date] = []
        for year in range(start.year, end.year + 1):
            for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
                candidate = date(year, month, day)
                if start <= candidate <= end:
                    periods.append(candidate)
        return tuple(periods)


class TushareCalendarPolicy:
    """解析更新边界。入参：客户端和时钟。返回值：日历策略。异常：日历缺失时抛出。"""

    def __init__(
        self,
        client: TushareClient,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._client = client
        self._clock = clock

    def bootstrap_window(self, years: int) -> tuple[date, date]:
        """生成初始窗口。入参：年数。返回值：日期范围。异常：年数非法时抛出。"""
        if years < 1:
            raise ValueError("bootstrap years must be positive")
        end = self.latest_complete_day()
        try:
            start = end.replace(year=end.year - years)
        except ValueError:
            start = end.replace(year=end.year - years, day=28)
        return start, end

    def latest_complete_day(self) -> date:
        """返回完整日。入参：无。返回值：日期。异常：日历为空时抛出。"""
        now = self._clock().astimezone(_SHANGHAI)
        candidate = now.date() if now.hour >= 18 else now.date() - timedelta(days=1)
        start = candidate - timedelta(days=20)
        self._client.login()
        requests = self._client.requests("trade_cal", start, candidate)
        batches = tuple(self._client.fetch("trade_cal", requests[0]))
        rows = batches[0].rows if batches else ()
        days = sorted(
            date.fromisoformat(f"{text[0:4]}-{text[4:6]}-{text[6:8]}")
            for row in rows
            if row.get("is_open") == "1"
            and (text := str(row.get("cal_date") or ""))
        )
        if not days:
            raise ValueError("Tushare trade calendar returned no complete session")
        return days[-1]

    def explicit_window(self, start: date, end: date) -> tuple[date, date]:
        """验证窗口。入参：开始与结束日。返回值：日期范围。异常：范围非法时抛出。"""
        if start > end:
            raise ValueError("explicit start must not follow end")
        return start, end
