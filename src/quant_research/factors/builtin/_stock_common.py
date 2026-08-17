"""提供内置实现与_stock_common相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date, datetime
from math import isfinite
from typing import Protocol

import polars as pl

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import (
    FACTOR_OUTPUT_SCHEMA,
    FactorSpec,
    is_available_on_signal_day,
)


class BarRepository(Protocol):
    """定义 ``BarRepository`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        构造并返回 ``BarRepository`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取 PIT 行情因子计算。

        入参：
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
        返回值：
            返回行情（``pl.LazyFrame``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def daily_basics(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """处理因子计算中的日频``basics``。

        入参：
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
        返回值：
            返回``basics``（``pl.LazyFrame``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...


class TradeCalendarProvider(Protocol):
    """定义 ``TradeCalendarProvider`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        构造并返回 ``TradeCalendarProvider`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        """处理因子计算中的交易交易日历。

        入参：
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
        返回值：
            返回交易日历（``pl.LazyFrame``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...


def output_frame(
    spec: FactorSpec, rows: Iterable[tuple[date, str, float | None, datetime | None]]
) -> pl.LazyFrame:
    """处理因子计算中的输出数据表；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        spec：不可变规格。
        rows：数据行集合。
    返回值：
        返回``frame``（``pl.LazyFrame``）。
    异常：
        无。
    """
    materialized = []
    for day, instrument, value, available_at in rows:
        available_on_day = is_available_on_signal_day(available_at, day)
        valid = (
            value is not None
            and isfinite(value)
            and _StockCommonSupport._known_availability(available_at)
            and available_on_day
        )
        materialized.append(
            {
                "trade_date": day,
                "instrument_id": instrument,
                "factor_id": spec.factor_id,
                "value": value if valid else None,
                "available_at": available_at
                if _StockCommonSupport._known_availability(available_at)
                else None,
                "is_valid": valid,
            }
        )
    return (
        pl.DataFrame(materialized, schema=FACTOR_OUTPUT_SCHEMA)
        .sort("trade_date", "instrument_id")
        .lazy()
    )


def trading_signal_dates(
    provider: TradeCalendarProvider, start: date, end: date
) -> tuple[date, ...]:
    """处理因子计算中的``trading``信号日期``dates``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        provider：数据供应商。
        start：处理区间的开始日期，类型为 ``date``。
        end：处理区间的结束日期，类型为 ``date``。
    返回值：
        返回信号日期``dates``（``tuple[date, ...]``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """
    frame = provider.trade_calendar(start, end).collect()
    required = {"trade_date", "is_trading_day"}
    if not required.issubset(frame.columns):
        raise ValueError("trade calendar missing required columns")
    if frame["trade_date"].is_duplicated().any():
        raise ValueError("duplicate trade calendar date")
    calendar_dates = frame["trade_date"].to_list()
    if any(type(day) is not date or day < start or day > end for day in calendar_dates):
        raise ValueError("trade calendar date is outside requested range")
    days = frame.filter(pl.col("is_trading_day"))["trade_date"].to_list()
    return tuple(sorted(days))


class _StockCommonSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _known_availability(value: datetime | None) -> bool:
        return (
            isinstance(value, datetime)
            and value.tzinfo is not None
            and value.utcoffset() is not None
        )


def canonical_scope(instruments: Sequence[InstrumentId]) -> tuple[InstrumentId, ...]:
    """输出规范形式的``scope``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
    返回值：
        返回``scope``（``tuple[InstrumentId, ...]``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    """
    scope = tuple(instruments)
    if any(not isinstance(item, InstrumentId) for item in scope):
        raise TypeError("instruments must contain InstrumentId values")
    identities = [item.canonical() for item in scope]
    if len(set(identities)) != len(identities):
        raise ValueError("instrument scope contains duplicates")
    return scope
