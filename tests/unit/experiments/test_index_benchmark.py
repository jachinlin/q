"""验证回测市场切片从独立指数数据集读取指数基准。"""

from __future__ import annotations

from datetime import date
from typing import cast

import polars as pl

from quant_research.data.contracts import ProviderCapabilities
from quant_research.data.repository import ResearchDataRepository
from quant_research.domain.identifiers import InstrumentId
from quant_research.experiments.adapters import CanonicalBacktestMarketData

_TRADE_DATE = date(2025, 6, 3)


class _IndexBenchmarkRepository:
    """为市场切片测试提供分离的股票与指数行情。"""

    def __init__(self) -> None:
        self.index_requests: list[tuple[str, date, date]] = []

    def instruments(self) -> pl.LazyFrame:
        return pl.DataFrame(
            {
                "instrument_id": ["000300.SH", "600000.SH"],
                "instrument_type": ["INDEX", "STOCK"],
                "board": ["MAIN", "MAIN"],
            }
        ).lazy()

    def bars(
        self,
        instruments: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        del instruments, start, end
        return pl.DataFrame(
            {
                "instrument_id": ["600000.SH"],
                "trade_date": [_TRADE_DATE],
                "open": [10.0],
                "high": [10.5],
                "low": [9.8],
                "close": [10.2],
                "preclose": [9.9],
                "volume": [None],
            }
        ).lazy()

    def index_bars(
        self,
        indexes: tuple[InstrumentId, ...],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        self.index_requests.append((indexes[0].canonical(), start, end))
        return pl.DataFrame(
            {
                "index_id": ["000300.SH"],
                "trade_date": [_TRADE_DATE],
                "open": [4100.0],
                "high": [4120.0],
                "low": [4090.0],
                "close": [4110.0],
                "preclose": [4080.0],
                "volume": [1_000_000],
            }
        ).lazy()

    def security_status(
        self,
        as_of: date,
        instruments: tuple[InstrumentId, ...] | None = None,
    ) -> pl.LazyFrame:
        del as_of, instruments
        return pl.DataFrame(
            {
                "instrument_id": ["600000.SH"],
                "trade_date": [_TRADE_DATE],
                "is_listed": [True],
                "is_suspended": [True],
                "is_st": [False],
            }
        ).lazy()


def test_market_slice_merges_index_benchmark_without_security_status() -> None:
    """指数基准应从 ``index_bar`` 合并且不要求证券状态记录。"""
    repository = _IndexBenchmarkRepository()
    market_data = CanonicalBacktestMarketData(
        repository=cast(ResearchDataRepository, repository),
        benchmark=InstrumentId.parse("000300.SH"),
        capabilities=ProviderCapabilities.complete(),
        provider="baostock",
    )

    bars = market_data.market_slice(_TRADE_DATE).market.bars.sort("instrument_id")

    assert bars.select(
        "instrument_id",
        "close",
        "is_suspended",
        "security_status",
        "instrument_type",
        "board",
    ).rows() == [
        ("000300.SH", 4110.0, False, "NORMAL", "INDEX", "MAIN"),
        ("600000.SH", 10.2, True, "NORMAL", "STOCK", "MAIN"),
    ]
    assert bars.filter(pl.col("instrument_id") == "600000.SH")["volume"].item() == 0
    assert repository.index_requests == [("000300.SH", _TRADE_DATE, _TRADE_DATE)]
