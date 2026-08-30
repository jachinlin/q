"""验证策略研究 Worker 的 Canonical 行情适配语义。"""

from datetime import date
from typing import Any, cast

import polars as pl

from quant_research.bootstrap.worker import CanonicalStrategyStudyData
from quant_research.domain.identifiers import InstrumentId


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
