"""验证 Experiment Worker 的 Canonical 行情适配语义。"""

from datetime import date
from typing import Any, cast

import polars as pl

from quant_research.bootstrap.worker import CanonicalRunData


class _SuspendedRepository:
    """返回一条 Canonical 全空停牌占位行情。"""

    def bars(self, *_args: object) -> pl.LazyFrame:
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

    def security_status(self, *_args: object) -> pl.LazyFrame:
        """返回与占位行情同日的权威停牌状态。"""
        return pl.DataFrame(
            {
                "instrument_id": ["300114.SZ"],
                "is_suspended": [True],
                "is_st": [False],
            },
            schema={
                "instrument_id": pl.String,
                "is_suspended": pl.Boolean,
                "is_st": pl.Boolean,
            },
        ).lazy()


def test_market_slice_preserves_null_volume_for_suspended_placeholder() -> None:
    """适配器不得把全空停牌行的成交量改写为零。"""
    source = object.__new__(CanonicalRunData)
    source._repository = cast(Any, _SuspendedRepository())
    source._metadata = {
        "300114.SZ": {
            "instrument_id": "300114.SZ",
            "instrument_type": "STOCK",
            "board": "CHINEXT",
        }
    }

    market = source.market_slice(date(2025, 2, 17)).market

    assert market.bars.item(0, "is_suspended") is True
    assert market.bars.item(0, "volume") is None
    assert market.bars.select(
        "open", "high", "low", "close", "preclose", "volume"
    ).null_count().row(0) == (1, 1, 1, 1, 1, 1)
