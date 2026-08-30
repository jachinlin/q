"""验证策略研究 Worker 的 Canonical 行情适配语义。"""

from datetime import date
from typing import Any, cast

import polars as pl

from quant_research.bootstrap.worker import CanonicalStrategyStudyData
from quant_research.domain.enums import DatasetKind
from quant_research.domain.identifiers import InstrumentId
from quant_research.strategies.base import StrategySpec


class _SuspendedRepository:
    """返回一条 Canonical 全空停牌占位行情。"""

    def stock_bars(self, *_args: object) -> pl.LazyFrame:
        """返回退市日全空行情及其规范类型。"""
        return pl.DataFrame(
            {
                "instrument_id": ["300114.SZ"],
                "trade_date": [date(2025, 2, 17)],
                "open": [None],
                "high": [None],
                "low": [None],
                "close": [None],
                "preclose": [None],
                "volume": [None],
            },
            schema={
                "instrument_id": pl.String,
                "trade_date": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "preclose": pl.Float64,
                "volume": pl.Int64,
            },
        ).lazy()

    def stock_suspensions(self, *_args: object) -> pl.LazyFrame:
        """返回与占位行情同日的权威停牌事件。"""
        return pl.DataFrame(
            {
                "instrument_id": ["300114.SZ"],
                "trade_date": [date(2025, 2, 17)],
            },
            schema={
                "instrument_id": pl.String,
                "trade_date": pl.Date,
            },
        ).lazy()

    def stock_risk_warnings(self, *_args: object) -> pl.LazyFrame:
        """返回固定空风险警示事件。"""
        return pl.DataFrame(
            schema={"instrument_id": pl.String, "trade_date": pl.Date}
        ).lazy()


class _InstrumentCatalogRepository:
    """提供同时含股票和基金的最小证券目录。"""

    def stocks(self) -> pl.LazyFrame:
        """返回一只与基金策略无关的股票。"""
        return pl.DataFrame(
            {
                "instrument_id": ["920167.BJ"],
                "list_date": [date(2020, 7, 27)],
                "delist_date": [None],
                "board": ["BSE"],
            },
            schema={
                "instrument_id": pl.String,
                "list_date": pl.Date,
                "delist_date": pl.Date,
                "board": pl.String,
            },
        ).lazy()

    def funds(self) -> pl.LazyFrame:
        """返回目标基金和一只无关基金。"""
        return pl.DataFrame(
            {
                "instrument_id": ["510050.SH", "510300.SH"],
                "list_date": [date(2005, 2, 23), date(2012, 5, 28)],
                "delist_date": [None, None],
            },
            schema={
                "instrument_id": pl.String,
                "list_date": pl.Date,
                "delist_date": pl.Date,
            },
        ).lazy()


class _DualMAStrategy:
    """暴露双均线策略所需的稳定市场范围。"""

    @property
    def spec(self) -> StrategySpec:
        """返回只依赖目标基金行情的策略规格。"""
        return StrategySpec(
            "dual_ma_trend",
            "DAILY",
            (
                DatasetKind.FUND_DAILY_BAR,
                DatasetKind.FUND_ADJUSTMENT_FACTOR,
            ),
            (),
            {"instrument_id": "510300.SH"},
        )


class _PrelistingRepository:
    """返回一条早于主数据上市日的历史股票行情。"""

    def stock_bars(self, *_args: object) -> pl.LazyFrame:
        """返回带空前收盘价的上市前行情。"""
        return pl.DataFrame(
            {
                "instrument_id": ["920167.BJ"],
                "trade_date": [date(2018, 1, 5)],
                "open": [3.7],
                "high": [3.7],
                "low": [3.7],
                "close": [3.7],
                "preclose": [None],
                "volume": [1000],
            },
            schema={
                "instrument_id": pl.String,
                "trade_date": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "preclose": pl.Float64,
                "volume": pl.Int64,
            },
        ).lazy()

    def stock_suspensions(self, *_args: object) -> pl.LazyFrame:
        """返回空停牌事件。"""
        return pl.DataFrame(
            schema={"instrument_id": pl.String, "trade_date": pl.Date}
        ).lazy()

    def stock_risk_warnings(self, *_args: object) -> pl.LazyFrame:
        """返回空风险警示事件。"""
        return pl.DataFrame(
            schema={"instrument_id": pl.String, "trade_date": pl.Date}
        ).lazy()


def test_market_slice_preserves_null_volume_for_suspended_placeholder() -> None:
    """适配器不得把全空停牌行的成交量改写为零。"""
    source = object.__new__(CanonicalStrategyStudyData)
    source._repository = cast(Any, _SuspendedRepository())
    source._metadata = {
        "300114.SZ": {
            "instrument_id": "300114.SZ",
            "instrument_type": "STOCK",
            "board": "CHINEXT",
        }
    }
    source._stock_ids = (InstrumentId.parse("300114.SZ"),)
    source._fund_ids = ()

    market = source.market_slice(date(2025, 2, 17)).market

    assert market.bars.item(0, "is_suspended") is True
    assert market.bars.item(0, "volume") is None
    assert market.bars.select(
        "open", "high", "low", "close", "preclose", "volume"
    ).null_count().row(0) == (1, 1, 1, 1, 1, 1)


def test_strategy_scope_excludes_unrelated_security_types_and_funds() -> None:
    """双均线数据源只应加载策略声明的目标基金。"""
    source = CanonicalStrategyStudyData(
        cast(Any, _InstrumentCatalogRepository()),
        "catalog-hash",
        strategy=cast(Any, _DualMAStrategy()),
    )

    assert source._stock_ids == ()
    assert tuple(item.canonical() for item in source._fund_ids) == ("510300.SH",)


def test_market_slice_ignores_bars_before_registered_listing_date() -> None:
    """当前证券身份生效前的历史行情不得进入回测市场切片。"""
    source = object.__new__(CanonicalStrategyStudyData)
    source._repository = cast(Any, _PrelistingRepository())
    source._metadata = {
        "920167.BJ": {
            "instrument_id": "920167.BJ",
            "instrument_type": "STOCK",
            "board": "BSE",
            "list_date": date(2020, 7, 27),
        }
    }
    source._stock_ids = (InstrumentId.parse("920167.BJ"),)
    source._fund_ids = ()

    market = source.market_slice(date(2018, 1, 5)).market

    assert market.bars.is_empty()
