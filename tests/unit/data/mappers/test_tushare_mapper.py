from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from quant_research.data.contracts import RawBatch
from quant_research.data.storage.partitions import RawPartitionStore
from quant_research.domain.enums import DatasetKind
from quant_research.infrastructure.tushare.mapper import TushareMapper


def test_daily_maps_preclose_percent_and_units(tmp_path: Path) -> None:
    request = {
        "endpoint": "daily",
        "trade_date": "20260825",
        "fields": (
            "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,"
            "vol,amount,ah_vol,ah_amount"
        ),
    }
    raw = RawPartitionStore(tmp_path).publish(
        RawBatch(
            source="tushare",
            endpoint="daily",
            request=request,
            retrieved_at=datetime(2026, 8, 26, tzinfo=UTC),
            schema=tuple(str(request["fields"]).split(",")),
            rows=(
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260825",
                    "open": "10",
                    "high": "10.5",
                    "low": "9.9",
                    "close": "10.25",
                    "pre_close": "10",
                    "change": "0.25",
                    "pct_chg": "2.5",
                    "vol": "123",
                    "amount": "456",
                    "ah_vol": "2",
                    "ah_amount": "3",
                },
            ),
        )
    )
    batch = TushareMapper().normalize(raw)[0]
    assert batch.dataset is DatasetKind.STOCK_DAILY_BAR
    row = batch.frame.row(0, named=True)
    assert row["instrument_id"] == "600000.SH"
    assert row["preclose"] == pytest.approx(10.0)
    assert row["pct_change"] == pytest.approx(0.025)
    assert row["volume"] == 12_300
    assert row["amount"] == pytest.approx(456_000.0)
    assert row["source"] == "tushare"


def test_stock_basic_adds_bse_board(tmp_path: Path) -> None:
    fields = (
        "ts_code",
        "symbol",
        "name",
        "area",
        "industry",
        "fullname",
        "enname",
        "cnspell",
        "market",
        "exchange",
        "curr_type",
        "list_status",
        "list_date",
        "delist_date",
        "is_hs",
        "act_name",
        "act_ent_type",
    )
    raw = RawPartitionStore(tmp_path).publish(
        RawBatch(
            source="tushare",
            endpoint="stock_basic",
            request={"endpoint": "stock_basic", "list_status": "L"},
            retrieved_at=datetime(2026, 8, 26, tzinfo=UTC),
            schema=fields,
            rows=(
                dict.fromkeys(fields, None)
                | {
                    "ts_code": "920001.BJ",
                    "symbol": "920001",
                    "name": "测试",
                    "market": "北交所",
                    "exchange": "BSE",
                    "list_status": "L",
                    "list_date": "20260801",
                },
            ),
        )
    )
    row = TushareMapper().normalize(raw)[0].frame.row(0, named=True)
    assert row["instrument_id"] == "920001.BJ"
    assert row["board"] == "BSE"


def test_daily_basic_keeps_proxy_omitted_limit_status_nullable(
    tmp_path: Path,
) -> None:
    fields = (
        "ts_code",
        "trade_date",
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
    )
    row = dict.fromkeys(fields, "1") | {
        "ts_code": "600000.SH",
        "trade_date": "20260825",
    }
    raw = RawPartitionStore(tmp_path).publish(
        RawBatch(
            source="tushare",
            endpoint="daily_basic",
            request={
                "endpoint": "daily_basic",
                "trade_date": "20260825",
                "fields": ",".join(fields),
            },
            retrieved_at=datetime(2026, 8, 26, tzinfo=UTC),
            schema=fields,
            rows=(row,),
        )
    )

    canonical = TushareMapper().normalize(raw)[0].frame.row(0, named=True)

    assert canonical["instrument_id"] == "600000.SH"
    assert canonical["limit_status"] is None
