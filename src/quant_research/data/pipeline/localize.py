"""按数据目录声明的抓取计划执行 LOCALIZE 供应商请求编排。"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from functools import partial
from types import MappingProxyType
from typing import Never, Protocol

from quant_research.data.catalog import FetchPlan
from quant_research.data.contracts import (
    JsonValue,
    PublishedPartition,
    RawBatch,
)
from quant_research.data.sources.contracts import PipelineSource

type RawFetch = Callable[[], Iterable[RawBatch]]
type RequestUnit = tuple[str, Mapping[str, JsonValue]]
type PendingRequestUnit = tuple[str, Mapping[str, JsonValue], bool]
type PublishBatch = Callable[[RawBatch], PublishedPartition]
type PublishOrReuse = Callable[
    [str, Mapping[str, JsonValue], RawFetch, bool], PublishedPartition | None
]
type FilterCompleted = Callable[
    [Sequence[RequestUnit], str], tuple[PendingRequestUnit, ...]
]


@dataclass(frozen=True, slots=True)
class LocalizePlanContext:
    """向抓取计划提供单次 LOCALIZE 运行所需的受控能力。

    入参：目标日期窗口、供应商、目录端点和发布/检查点回调。
    返回值：构造不可变计划上下文。异常：端点未在目录声明时抛出 ``ValueError``；
    其余异常由供应商或回调原样传播。
    """

    source: PipelineSource
    start: date
    end: date
    endpoints: tuple[str, ...]
    publish_batch: PublishBatch
    publish_or_reuse: PublishOrReuse
    filter_completed: FilterCompleted
    ensure_active: Callable[[], None]
    raise_contract: Callable[[str], Never]

    def require_endpoint(self, endpoint: str) -> str:
        """确认抓取端点已由目标数据集目录声明。

        入参：供应商端点。返回值：原端点字符串。异常：未声明时抛出 ``ValueError``。
        """
        if endpoint not in self.endpoints:
            raise ValueError(f"fetch plan endpoint is not declared: {endpoint}")
        return endpoint

    def require_partition(
        self, partition: PublishedPartition | None, message: str
    ) -> PublishedPartition:
        """确认依赖请求发布或复用了可见 Raw 分区。

        入参：候选分区与错误消息。返回值：非空分区。异常：空结果转换为数据源契约错误。
        """
        if partition is None:
            self.raise_contract(message)
        return partition


class _LocalizePlan(Protocol):
    def execute(self, context: LocalizePlanContext) -> None: ...


class _InstrumentSnapshotPlan:
    @staticmethod
    def execute(context: LocalizePlanContext) -> None:
        for batch in context.source.fetch_instruments():
            context.ensure_active()
            context.require_endpoint(batch.endpoint)
            context.publish_batch(batch)


class _TradeCalendarRangePlan:
    _ENDPOINT = "query_trade_dates"

    @classmethod
    def execute(cls, context: LocalizePlanContext) -> None:
        endpoint = context.require_endpoint(cls._ENDPOINT)
        request = context.source.trade_calendar_request(context.start, context.end)
        context.publish_or_reuse(
            endpoint,
            request,
            lambda: context.source.fetch_trade_calendar(context.start, context.end),
            False,
        )


class _IndexRangePlan:
    _ENDPOINT = "query_history_k_data_plus"

    @classmethod
    def execute(cls, context: LocalizePlanContext) -> None:
        endpoint = context.require_endpoint(cls._ENDPOINT)
        units = tuple(
            (endpoint, request)
            for request in context.source.index_bar_requests(context.start, context.end)
        )
        for current_endpoint, request, force_fetch in context.filter_completed(
            units, FetchPlan.INDEX_RANGE.value
        ):
            context.publish_or_reuse(
                current_endpoint,
                request,
                partial(context.source.fetch_index_bars, request),
                force_fetch,
            )


class _DailyMarketPlan:
    _CALENDAR_ENDPOINT = "query_trade_dates"
    _DAILY_ENDPOINT = "query_daily_history_k_AStock"
    _ETF_ENDPOINT = "query_etf_history_k_data_plus"

    @classmethod
    def execute(cls, context: LocalizePlanContext) -> None:
        daily_endpoint = context.require_endpoint(cls._DAILY_ENDPOINT)
        calendar_request = context.source.trade_calendar_request(
            context.start, context.end
        )
        calendar_partition = context.require_partition(
            context.publish_or_reuse(
                cls._CALENDAR_ENDPOINT,
                calendar_request,
                lambda: context.source.fetch_trade_calendar(
                    context.start, context.end
                ),
                False,
            ),
            "trade calendar returned no batch",
        )
        days = context.source.calendar_trading_days(
            calendar_partition, context.start, context.end
        )
        units = tuple(
            (daily_endpoint, context.source.daily_bars_request(day)) for day in days
        )
        for endpoint, request, force_fetch in context.filter_completed(
            units, FetchPlan.DAILY_MARKET.value
        ):
            day = date.fromisoformat(str(request["date"]))
            context.publish_or_reuse(
                endpoint,
                request,
                partial(context.source.fetch_daily_bars, day),
                force_fetch,
            )
        if cls._ETF_ENDPOINT not in context.endpoints:
            return
        etf_units = tuple(
            (cls._ETF_ENDPOINT, request)
            for request in context.source.etf_bar_requests(context.start, context.end)
        )
        for endpoint, request, force_fetch in context.filter_completed(
            etf_units, "etf_range"
        ):
            context.publish_or_reuse(
                endpoint,
                request,
                partial(context.source.fetch_etf_bars, request),
                force_fetch,
            )


class _FinancialCellPlan:
    @staticmethod
    def execute(context: LocalizePlanContext) -> None:
        units = tuple(
            (context.require_endpoint(str(request["endpoint"])), request)
            for request in context.source.financial_requests(context.start, context.end)
        )
        for endpoint, request, force_fetch in context.filter_completed(
            units, FetchPlan.FINANCIAL_CELL.value
        ):
            context.publish_or_reuse(
                endpoint,
                request,
                partial(context.source.fetch_financials, request),
                force_fetch,
            )


class _IndustryAsOfPlan:
    _CALENDAR_ENDPOINT = "query_trade_dates"
    _ENDPOINT = "query_stock_industry"

    @classmethod
    def execute(cls, context: LocalizePlanContext) -> None:
        endpoint = context.require_endpoint(cls._ENDPOINT)
        calendar_request = context.source.trade_calendar_request(
            context.start, context.end
        )
        calendar_partition = context.require_partition(
            context.publish_or_reuse(
                cls._CALENDAR_ENDPOINT,
                calendar_request,
                lambda: context.source.fetch_trade_calendar(
                    context.start, context.end
                ),
                False,
            ),
            "trade calendar returned no batch",
        )
        days = context.source.calendar_trading_days(
            calendar_partition, context.start, context.end
        )
        units = tuple(
            (endpoint, request) for request in context.source.industry_requests(days)
        )
        for current_endpoint, request, force_fetch in context.filter_completed(
            units, FetchPlan.INDUSTRY_AS_OF.value
        ):
            context.publish_or_reuse(
                current_endpoint,
                request,
                partial(context.source.fetch_industry, request),
                force_fetch,
            )


class LocalizePlanExecutor:
    """按目录中的 ``FetchPlan`` 委派无数据集分支的抓取编排。

    入参：抓取计划和单次运行上下文。返回值：无。异常：计划未注册时抛出
    ``ValueError``，供应商和发布异常原样传播。
    """

    _PLANS: Mapping[FetchPlan, _LocalizePlan] = MappingProxyType(
        {
            FetchPlan.INSTRUMENT_SNAPSHOT: _InstrumentSnapshotPlan(),
            FetchPlan.TRADE_CALENDAR_RANGE: _TradeCalendarRangePlan(),
            FetchPlan.DAILY_MARKET: _DailyMarketPlan(),
            FetchPlan.INDEX_RANGE: _IndexRangePlan(),
            FetchPlan.FINANCIAL_CELL: _FinancialCellPlan(),
            FetchPlan.INDUSTRY_AS_OF: _IndustryAsOfPlan(),
        }
    )

    @classmethod
    def execute(cls, plan: FetchPlan, context: LocalizePlanContext) -> None:
        """执行已注册的抓取计划。

        入参：目录计划标识与运行上下文。返回值：无。异常：计划未注册或执行失败时传播。
        """
        try:
            strategy = cls._PLANS[plan]
        except KeyError as error:
            raise ValueError(f"unregistered localize fetch plan: {plan}") from error
        strategy.execute(context)

    @classmethod
    def supported_plans(cls) -> frozenset[FetchPlan]:
        """返回全部已注册抓取计划。

        入参：无。返回值：不可变计划集合。异常：无主动抛出的异常。
        """
        return frozenset(cls._PLANS)
