from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.contracts import CanonicalBatch, PublishedPartition, RawBatch
from quant_research.data.storage.partitions import RawPartitionStore
from quant_research.domain.enums import DatasetKind
from quant_research.infrastructure.tushare.client import _FIELDS
from quant_research.infrastructure.tushare.mapper import TushareMapper


def _normalize_raw(raw: PublishedPartition) -> CanonicalBatch:
    return TushareMapper().normalize(raw, pq.read_table(raw.data_path))[0]


def test_trade_calendar_range_maps_to_all_partition(tmp_path: Path) -> None:
    fields = _FIELDS["trade_cal"]
    raw = RawPartitionStore(tmp_path).publish(
        RawBatch(
            source="tushare",
            endpoint="trade_cal",
            request={
                "endpoint": "trade_cal",
                "exchange": "SSE",
                "start_date": "20060826",
                "end_date": "20260826",
                "fields": ",".join(fields),
            },
            retrieved_at=datetime(2026, 8, 26, tzinfo=UTC),
            schema=fields,
            rows=(
                {
                    "exchange": "SSE",
                    "cal_date": "20260826",
                    "is_open": "1",
                    "pretrade_date": "20260825",
                },
            ),
        )
    )

    batch = _normalize_raw(raw)

    assert batch.dataset is DatasetKind.TRADE_CALENDAR
    assert TushareMapper().candidate_partition_keys_many(batch.dataset, (raw,)) == (
        ("all",),
    )


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
    batch = _normalize_raw(raw)
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
    row = _normalize_raw(raw).frame.row(0, named=True)
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

    canonical = _normalize_raw(raw).frame.row(0, named=True)

    assert canonical["instrument_id"] == "600000.SH"
    assert canonical["limit_status"] is None


@pytest.mark.parametrize(
    ("endpoint", "dataset", "value_field"),
    (
        ("income_vip", DatasetKind.STOCK_INCOME_STATEMENT, "total_revenue"),
        ("balancesheet_vip", DatasetKind.STOCK_BALANCE_SHEET, "total_assets"),
        ("cashflow_vip", DatasetKind.STOCK_CASH_FLOW_STATEMENT, "net_profit"),
    ),
)
def test_statement_maps_common_fields_and_actual_announcement_pit(
    tmp_path: Path,
    endpoint: str,
    dataset: DatasetKind,
    value_field: str,
) -> None:
    fields = _FIELDS[endpoint]
    row = dict.fromkeys(fields, None) | {
        "ts_code": "600000.SH",
        "ann_date": "20260428",
        "f_ann_date": "20260429",
        "end_date": "20260331",
        "report_type": "1",
        "comp_type": "2",
        "end_type": "1",
        value_field: "123.5",
        "update_flag": "1",
    }
    raw = RawPartitionStore(tmp_path).publish(
        RawBatch(
            source="tushare",
            endpoint=endpoint,
            request={
                "endpoint": endpoint,
                "period": "20260331",
                "report_type": "1",
                "fields": ",".join(fields),
            },
            retrieved_at=datetime(2026, 4, 30, tzinfo=UTC),
            schema=fields,
            rows=(row,),
        )
    )

    batch = _normalize_raw(raw)
    canonical = batch.frame.row(0, named=True)

    assert batch.dataset is dataset
    assert canonical["instrument_id"] == "600000.SH"
    assert canonical["report_period"] == date(2026, 3, 31)
    assert canonical["actual_announcement_date"] == date(2026, 4, 29)
    assert canonical["report_type"] == "1"
    assert canonical[value_field] == pytest.approx(123.5)
    assert canonical["available_at"] == datetime(2026, 4, 29, 10, tzinfo=UTC)
    assert canonical["revision"] == 0


@pytest.mark.parametrize(
    ("endpoint", "dataset", "amount_field"),
    (
        (
            "balancesheet_vip",
            DatasetKind.STOCK_BALANCE_SHEET,
            "payable_to_reinsurer",
        ),
        (
            "cashflow_vip",
            DatasetKind.STOCK_CASH_FLOW_STATEMENT,
            "c_paid_to_for_empl",
        ),
    ),
)
def test_statement_amount_names_containing_to_keep_supplier_units(
    tmp_path: Path,
    endpoint: str,
    dataset: DatasetKind,
    amount_field: str,
) -> None:
    fields = _FIELDS[endpoint]
    row = dict.fromkeys(fields, None) | {
        "ts_code": "600000.SH",
        "ann_date": "20260428",
        "f_ann_date": "20260429",
        "end_date": "20260331",
        "report_type": "1",
        "comp_type": "2",
        "end_type": "1",
        amount_field: "12345.67",
        "update_flag": "1",
    }
    raw = RawPartitionStore(tmp_path).publish(
        RawBatch(
            source="tushare",
            endpoint=endpoint,
            request={
                "endpoint": endpoint,
                "period": "20260331",
                "report_type": "1",
                "fields": ",".join(fields),
            },
            retrieved_at=datetime(2026, 4, 30, tzinfo=UTC),
            schema=fields,
            rows=(row,),
        )
    )

    canonical = _normalize_raw(raw)

    assert canonical.dataset is dataset
    assert canonical.frame.get_column(amount_field).item() == pytest.approx(12345.67)


def test_stock_dividend_maps_units_and_implementation_availability(
    tmp_path: Path,
) -> None:
    fields = _FIELDS["dividend"]
    row = dict.fromkeys(fields, None) | {
        "ts_code": "600000.SH",
        "end_date": "20251231",
        "ann_date": "20260320",
        "div_proc": "实施",
        "cash_div": "0.09",
        "cash_div_tax": "0.10",
        "record_date": "20260510",
        "ex_date": "20260511",
        "pay_date": "20260512",
        "imp_ann_date": "20260505",
        "base_date": "20251231",
        "base_share": "123.5",
    }
    raw = RawPartitionStore(tmp_path).publish(
        RawBatch(
            source="tushare",
            endpoint="dividend",
            request={
                "endpoint": "dividend",
                "imp_ann_date": "20260505",
                "fields": ",".join(fields),
            },
            retrieved_at=datetime(2026, 5, 6, tzinfo=UTC),
            schema=fields,
            rows=(row,),
        )
    )

    batch = _normalize_raw(raw)
    canonical = batch.frame.row(0, named=True)

    assert batch.dataset is DatasetKind.STOCK_DIVIDEND
    assert canonical["base_share_count"] == pytest.approx(1_235_000.0)
    assert canonical["cash_dividend_before_tax_per_share"] == pytest.approx(0.1)
    assert canonical["available_at"] == datetime(2026, 5, 5, 10, tzinfo=UTC)
    assert TushareMapper().candidate_partition_keys_many(batch.dataset, (raw,)) == (
        ("announcement_year=2026",),
    )


def test_fund_dividend_filters_off_exchange_funds_and_converts_units(
    tmp_path: Path,
) -> None:
    fields = _FIELDS["fund_div"]
    common = dict.fromkeys(fields, None) | {
        "ann_date": "20260401",
        "imp_anndate": "20260402",
        "base_date": "20260331",
        "div_proc": "实施",
        "ex_date": "20260405",
        "pay_date": "20260406",
        "div_cash": "0.02",
        "base_unit": "12.5",
        "ear_distr": "500",
        "ear_amount": "250",
        "base_year": "2026",
    }
    raw = RawPartitionStore(tmp_path).publish(
        RawBatch(
            source="tushare",
            endpoint="fund_div",
            request={
                "endpoint": "fund_div",
                "ann_date": "20260401",
                "fields": ",".join(fields),
            },
            retrieved_at=datetime(2026, 4, 3, tzinfo=UTC),
            schema=fields,
            rows=(
                common | {"ts_code": "510300.SH"},
                common | {"ts_code": "005068.OF"},
            ),
        )
    )

    batch = _normalize_raw(raw)

    assert batch.dataset is DatasetKind.FUND_DIVIDEND
    assert batch.frame.height == 1
    canonical = batch.frame.row(0, named=True)
    assert canonical["instrument_id"] == "510300.SH"
    assert canonical["base_unit_count"] == pytest.approx(125_000.0)
    assert canonical["distribution_amount"] == pytest.approx(250.0)


def test_fund_dividend_candidate_partition_uses_raw_announcement_year(
    tmp_path: Path,
) -> None:
    fields = _FIELDS["fund_div"]
    row = dict.fromkeys(fields, None) | {
        "ts_code": "510300.SH",
        "ann_date": "20201231",
        "ex_date": "20210104",
    }
    raw = RawPartitionStore(tmp_path).publish(
        RawBatch(
            source="tushare",
            endpoint="fund_div",
            request={
                "endpoint": "fund_div",
                "ex_date": "20210104",
                "fields": ",".join(fields),
            },
            retrieved_at=datetime(2021, 1, 5, tzinfo=UTC),
            schema=fields,
            rows=(row,),
        )
    )

    assert TushareMapper().candidate_partition_keys_many(
        DatasetKind.FUND_DIVIDEND,
        (raw,),
    ) == (("announcement_year=2020",),)


def test_revision_consolidation_deduplicates_supplier_content() -> None:
    schema = CANONICAL_SCHEMAS[DatasetKind.STOCK_INCOME_STATEMENT]
    base = {
        name: None for name in schema.columns.names()
    } | {
        "instrument_id": "600000.SH",
        "announcement_date": date(2026, 4, 28),
        "actual_announcement_date": date(2026, 4, 29),
        "report_period": date(2026, 3, 31),
        "report_type": "1",
        "company_type": "2",
        "report_period_type": "1",
        "total_revenue": 200.0,
        "update_flag": "1",
        "revision": 0,
        "source": "tushare",
        "available_at": datetime(2026, 4, 29, 10, tzinfo=UTC),
        "availability_source": "actual_announcement_date_eod",
        "pit_usable": True,
        "ingested_at": datetime(2026, 4, 30, tzinfo=UTC),
    }
    first = pl.DataFrame([base], schema=schema.columns, strict=False)
    duplicate = pl.DataFrame(
        [base | {"ingested_at": datetime(2026, 5, 1, tzinfo=UTC)}],
        schema=schema.columns,
        strict=False,
    )
    corrected = pl.DataFrame(
        [
            base
            | {
                "total_revenue": 100.0,
                "update_flag": "1",
                "ingested_at": datetime(2026, 5, 2, tzinfo=UTC),
            }
        ],
        schema=schema.columns,
        strict=False,
    )

    consolidated = TushareMapper().consolidate_partition(
        DatasetKind.STOCK_INCOME_STATEMENT,
        (first, duplicate, corrected),
    )

    assert consolidated.height == 2
    assert consolidated.get_column("revision").to_list() == [0, 1]
    assert consolidated.get_column("total_revenue").to_list() == [200.0, 100.0]
