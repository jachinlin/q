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

    def fund_bars(self, *_args: object) -> pl.LazyFrame:
        """返回目标基金在故障日期的有效行情。"""
        return pl.DataFrame(
            {
                "instrument_id": ["510300.SH"],
                "trade_date": [date(2018, 1, 5)],
                "open": [4.179],
                "high": [4.197],
                "low": [4.165],
                "close": [4.187],
                "preclose": [4.171],
                "volume": [127_131_673],
            }
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


class _ScopedCalendarRepository:
    """记录日历和按需行情查询范围。"""

    def __init__(self) -> None:
        self.sessions = (
            date(2024, 1, 8),
            date(2024, 1, 9),
            date(2024, 1, 10),
            date(2024, 1, 11),
            date(2024, 1, 12),
            date(2024, 1, 15),
        )
        self.calendar_calls = 0
        self.stock_scopes: list[tuple[str, ...]] = []
        self.fund_scopes: list[tuple[str, ...]] = []
        self.event_scopes: list[tuple[str, ...]] = []

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        """返回指定范围内的固定交易日并记录读取次数。"""
        self.calendar_calls += 1
        values = [item for item in self.sessions if start <= item <= end]
        return pl.DataFrame(
            {"trade_date": values, "is_trading_day": [True] * len(values)},
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()

    def stock_bars(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """返回请求股票的固定有效行情并记录范围。"""
        assert start == end
        scope = tuple(item.canonical() for item in instruments)
        self.stock_scopes.append(scope)
        return self._bars(scope, start)

    def fund_bars(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """返回请求基金的固定有效行情并记录范围。"""
        assert start == end
        scope = tuple(item.canonical() for item in instruments)
        self.fund_scopes.append(scope)
        return self._bars(scope, start)

    def stock_suspensions(
        self,
        start: date,
        end: date,
        instruments: tuple[InstrumentId, ...],
    ) -> pl.LazyFrame:
        """返回空停牌事件并记录范围。"""
        del start, end
        self.event_scopes.append(tuple(item.canonical() for item in instruments))
        return pl.DataFrame(
            schema={"instrument_id": pl.String, "trade_date": pl.Date}
        ).lazy()

    def stock_risk_warnings(
        self,
        start: date,
        end: date,
        instruments: tuple[InstrumentId, ...],
    ) -> pl.LazyFrame:
        """始终返回风险警示以验证上市前五日优先级。"""
        del end
        scope = tuple(item.canonical() for item in instruments)
        self.event_scopes.append(scope)
        warned = list(scope)
        return pl.DataFrame(
            {"instrument_id": warned, "trade_date": [start] * len(warned)},
            schema={"instrument_id": pl.String, "trade_date": pl.Date},
        ).lazy()

    @staticmethod
    def _bars(scope: tuple[str, ...], trade_date: date) -> pl.LazyFrame:
        return pl.DataFrame(
            {
                "instrument_id": list(scope),
                "trade_date": [trade_date] * len(scope),
                "open": [10.0] * len(scope),
                "high": [10.5] * len(scope),
                "low": [9.5] * len(scope),
                "close": [10.0] * len(scope),
                "preclose": [10.0] * len(scope),
                "volume": [1000] * len(scope),
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


def _scoped_source(
    repository: _ScopedCalendarRepository,
) -> CanonicalStrategyStudyData:
    source = object.__new__(CanonicalStrategyStudyData)
    source._repository = cast(Any, repository)
    source._metadata = {
        "600000.SH": {
            "instrument_id": "600000.SH",
            "instrument_type": "STOCK",
            "board": "MAIN",
            "list_date": date(2024, 1, 6),
            "delist_date": None,
        },
        "510300.SH": {
            "instrument_id": "510300.SH",
            "instrument_type": "ETF",
            "board": "MAIN",
            "list_date": date(2012, 5, 28),
            "delist_date": None,
        },
    }
    source._stock_ids = (InstrumentId.parse("600000.SH"),)
    source._fund_ids = (InstrumentId.parse("510300.SH"),)
    source._instruments = pl.DataFrame(tuple(source._metadata.values())).sort(
        "instrument_id"
    )
    source._no_limit_through = None
    return source


def test_stock_universe_builds_reason_codes_with_vectorized_event_joins() -> None:
    """股票池应通过事件连接生成稳定风险原因与可用标记。"""
    source = _scoped_source(_ScopedCalendarRepository())

    universe = source.universe(date(2024, 1, 15))

    assert universe.rows(named=True) == [
        {
            "instrument_id": "600000.SH",
            "as_of": date(2024, 1, 15),
            "eligible": False,
            "reason_codes": ["RISK_WARNING"],
        }
    ]


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

    market = source.market_slice(
        date(2025, 2, 17), (InstrumentId.parse("300114.SZ"),)
    ).market

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

    market = source.market_slice(
        date(2018, 1, 5), (InstrumentId.parse("510300.SH"),)
    ).market

    assert market.bars.item(0, "instrument_type") == "ETF"


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

    market = source.market_slice(
        date(2018, 1, 5), (InstrumentId.parse("920167.BJ"),)
    ).market

    assert market.bars.is_empty()


def test_market_slice_ignores_bars_after_registered_delisting_date() -> None:
    """证券身份退市后即使仍有历史行情也不得进入市场切片。"""
    source = object.__new__(CanonicalStrategyStudyData)
    source._repository = cast(Any, _PrelistingRepository())
    source._metadata = {
        "920167.BJ": {
            "instrument_id": "920167.BJ",
            "instrument_type": "STOCK",
            "board": "BSE",
            "list_date": date(2010, 1, 4),
            "delist_date": date(2017, 12, 29),
        }
    }
    source._stock_ids = (InstrumentId.parse("920167.BJ"),)
    source._fund_ids = ()
    source._no_limit_through = {"920167.BJ": date(2010, 1, 8)}

    market = source.market_slice(
        date(2018, 1, 5), (InstrumentId.parse("920167.BJ"),)
    ).market

    assert market.bars.is_empty()


def test_listing_limit_boundary_is_prepared_once_and_uses_literal_sessions() -> None:
    """非交易日上市后第 1 至 5 个交易日无涨跌停，第 6 日恢复 ST。"""
    repository = _ScopedCalendarRepository()
    source = _scoped_source(repository)
    stock = InstrumentId.parse("600000.SH")

    source.calendar(date(2024, 1, 8), date(2024, 1, 15), include_next_session=False)
    statuses = [
        source.market_slice(trade_date, (stock,)).market.bars.item(
            0, "security_status"
        )
        for trade_date in (
            date(2024, 1, 8),
            date(2024, 1, 12),
            date(2024, 1, 15),
        )
    ]

    assert statuses == ["NO_LIMIT", "NO_LIMIT", "ST"]
    assert repository.calendar_calls == 1
    assert repository.stock_scopes == [("600000.SH",)] * 3
    assert repository.event_scopes == [("600000.SH",)] * 6


def test_market_slice_reads_only_requested_known_stock_and_fund() -> None:
    """撮合行情只查询去重后的已登记证券，未知证券保留为缺失行情。"""
    repository = _ScopedCalendarRepository()
    source = _scoped_source(repository)
    stock = InstrumentId.parse("600000.SH")
    fund = InstrumentId.parse("510300.SH")
    unknown = InstrumentId.parse("000001.SZ")
    source.calendar(date(2024, 1, 8), date(2024, 1, 15), include_next_session=False)

    market = source.market_slice(
        date(2024, 1, 8), (fund, stock, unknown, stock)
    ).market

    assert market.bars["instrument_id"].to_list() == ["510300.SH", "600000.SH"]
    assert repository.stock_scopes == [("600000.SH",)]
    assert repository.fund_scopes == [("510300.SH",)]
    assert repository.event_scopes == [("600000.SH",), ("600000.SH",)]


def test_empty_market_scope_does_not_read_market_datasets() -> None:
    """空仓且无委托时返回类型完整的空行情且不触发市场数据查询。"""
    repository = _ScopedCalendarRepository()
    source = _scoped_source(repository)

    market = source.market_slice(date(2024, 1, 8), ()).market

    assert market.bars.is_empty()
    assert market.bars.schema == _market_schema()
    assert repository.stock_scopes == []
    assert repository.fund_scopes == []
    assert repository.event_scopes == []


def _market_schema() -> pl.Schema:
    return pl.Schema(
        {
            "instrument_id": pl.String,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "preclose": pl.Float64,
            "volume": pl.Int64,
            "is_suspended": pl.Boolean,
            "security_status": pl.String,
            "instrument_type": pl.String,
            "board": pl.String,
        }
    )
