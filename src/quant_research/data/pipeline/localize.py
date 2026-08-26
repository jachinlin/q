"""按数据目录声明执行 Tushare 全市场 LOCALIZE 请求。"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from functools import partial
from types import MappingProxyType
from typing import Never, Protocol

from quant_research.data.catalog import FetchPlan
from quant_research.data.contracts import JsonValue, PublishedPartition, RawBatch
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
type ExecutePending = Callable[[Sequence[PendingRequestUnit]], None]


@dataclass(frozen=True, slots=True)
class LocalizePlanContext:
    """提供运行上下文。入参：数据源、窗口与回调。返回值：上下文。异常：字段非法时抛出。"""

    source: PipelineSource
    start: date
    end: date
    endpoints: tuple[str, ...]
    publish_batch: PublishBatch
    publish_or_reuse: PublishOrReuse
    filter_completed: FilterCompleted
    execute_pending: ExecutePending
    ensure_active: Callable[[], None]
    raise_contract: Callable[[str], Never]

    def require_endpoint(self, endpoint: str) -> str:
        """确认端点。入参：端点。返回值：端点。异常：未声明时抛出。"""
        if endpoint not in self.endpoints:
            raise ValueError(f"fetch plan endpoint is not declared: {endpoint}")
        return endpoint

    def require_partition(
        self, partition: PublishedPartition | None, message: str
    ) -> PublishedPartition:
        """确认分区。入参：分区和消息。返回值：分区。异常：分区为空时抛出。"""
        if partition is None:
            self.raise_contract(message)
        return partition


class _LocalizePlan(Protocol):
    def execute(self, context: LocalizePlanContext) -> None: ...


class _RequestPlanSupport:
    """集中执行确定性请求单元和断点检查。"""

    @staticmethod
    def execute_units(
        context: LocalizePlanContext,
        units: Sequence[RequestUnit],
        checkpoint: str,
    ) -> None:
        context.execute_pending(context.filter_completed(units, checkpoint))


class _MarketSnapshotPlan:
    @staticmethod
    def execute(context: LocalizePlanContext) -> None:
        units = tuple(
            (endpoint, request)
            for endpoint in context.endpoints
            for request in context.source.requests(endpoint, context.start, context.end)
        )
        _RequestPlanSupport.execute_units(
            context, units, FetchPlan.MARKET_SNAPSHOT.value
        )


class _TradeCalendarRangePlan:
    @staticmethod
    def execute(context: LocalizePlanContext) -> None:
        units = tuple(
            (endpoint, request)
            for endpoint in context.endpoints
            for request in context.source.requests(endpoint, context.start, context.end)
        )
        _RequestPlanSupport.execute_units(
            context, units, FetchPlan.TRADE_CALENDAR_RANGE.value
        )


class _MarketTradeDatePlan:
    _CALENDAR_ENDPOINT = "trade_cal"

    @classmethod
    def execute(cls, context: LocalizePlanContext) -> None:
        calendar_requests = context.source.requests(
            cls._CALENDAR_ENDPOINT, context.start, context.end
        )
        if len(calendar_requests) != 1:
            context.raise_contract("trade calendar requires exactly one request")
        request = calendar_requests[0]
        calendar_partition = context.require_partition(
            context.publish_or_reuse(
                cls._CALENDAR_ENDPOINT,
                request,
                partial(context.source.fetch, cls._CALENDAR_ENDPOINT, request),
                False,
            ),
            "trade calendar returned no batch",
        )
        days = context.source.calendar_trading_days(
            calendar_partition, context.start, context.end
        )
        units = tuple(
            (endpoint, request_unit)
            for endpoint in context.endpoints
            for day in days
            for request_unit in context.source.requests(endpoint, day, day)
        )
        _RequestPlanSupport.execute_units(
            context, units, FetchPlan.MARKET_TRADE_DATE.value
        )


class _EndpointRangePlan:
    def __init__(self, checkpoint: str) -> None:
        self._checkpoint = checkpoint

    def execute(self, context: LocalizePlanContext) -> None:
        units = tuple(
            (endpoint, request)
            for endpoint in context.endpoints
            for request in context.source.requests(endpoint, context.start, context.end)
        )
        _RequestPlanSupport.execute_units(context, units, self._checkpoint)


class LocalizePlanExecutor:
    """编排抓取。入参：计划和上下文。返回值：执行器。异常：计划未知时抛出。"""

    _PLANS: Mapping[FetchPlan, _LocalizePlan] = MappingProxyType(
        {
            FetchPlan.MARKET_SNAPSHOT: _MarketSnapshotPlan(),
            FetchPlan.TRADE_CALENDAR_RANGE: _TradeCalendarRangePlan(),
            FetchPlan.MARKET_TRADE_DATE: _MarketTradeDatePlan(),
            FetchPlan.INDEX_RANGE_EXCEPTION: _EndpointRangePlan(
                FetchPlan.INDEX_RANGE_EXCEPTION.value
            ),
            FetchPlan.REPORT_PERIOD: _EndpointRangePlan(
                FetchPlan.REPORT_PERIOD.value
            ),
            FetchPlan.INDUSTRY_L1: _EndpointRangePlan(FetchPlan.INDUSTRY_L1.value),
        }
    )

    @classmethod
    def execute(cls, plan: FetchPlan, context: LocalizePlanContext) -> None:
        """执行计划。入参：计划和上下文。返回值：无。异常：计划未知时抛出。"""
        try:
            strategy = cls._PLANS[plan]
        except KeyError as error:
            raise ValueError(f"unregistered localize fetch plan: {plan}") from error
        strategy.execute(context)

    @classmethod
    def supported_plans(cls) -> frozenset[FetchPlan]:
        """返回计划。入参：无。返回值：计划集合。异常：无。"""
        return frozenset(cls._PLANS)
