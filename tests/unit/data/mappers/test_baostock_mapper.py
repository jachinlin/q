"""Focused tests for the current BaoStock raw-to-canonical boundary."""

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from quant_research.data.contracts import RawBatch
from quant_research.data.partitions import RawPartitionStore
from quant_research.domain.enums import DatasetKind
from quant_research.domain.errors import QuantError
from quant_research.infrastructure.baostock.client import (
    DAILY_BAR_FIELDS,
    INDUSTRY_FIELDS,
)
from quant_research.infrastructure.baostock.mapper import BaoStockMapper

RETRIEVED_AT = datetime(2026, 7, 31, 8, tzinfo=UTC)


def _publish(
    root: Path, endpoint: str, schema: tuple[str, ...], rows: tuple[dict[str, str], ...]
):
    return RawPartitionStore(root).publish(
        RawBatch(
            source="baostock",
            endpoint=endpoint,
            request={"endpoint": endpoint},
            retrieved_at=RETRIEVED_AT,
            schema=schema,
            rows=rows,
        )
    )


def test_instrument_mapper_emits_current_audit_schema(tmp_path: Path) -> None:
    partition = _publish(
        tmp_path,
        "query_stock_basic",
        ("code", "code_name", "ipoDate", "outDate", "type", "status"),
        (
            {
                "code": "sh.600000",
                "code_name": "浦发银行",
                "ipoDate": "1999-11-10",
                "outDate": "",
                "type": "1",
                "status": "1",
            },
        ),
    )

    batches = tuple(BaoStockMapper().normalize(partition))

    assert len(batches) == 1
    assert batches[0].dataset is DatasetKind.INSTRUMENT
    assert "source_version" not in batches[0].frame.columns
    assert batches[0].frame.select("instrument_id", "list_date").rows() == [
        ("600000.SH", date(1999, 11, 10))
    ]
    assert batches[0].frame["source"].to_list() == ["baostock"]
    assert batches[0].frame["ingested_at"].to_list() == [RETRIEVED_AT]


def test_daily_mapper_fans_out_to_three_canonical_datasets(tmp_path: Path) -> None:
    values = {
        "date": "2026-07-31",
        "code": "sh.600000",
        "open": "10.00",
        "high": "11.00",
        "low": "9.50",
        "close": "10.50",
        "preclose": "10.00",
        "volume": "100",
        "amount": "1050.00",
        "adjustflag": "3",
        "turn": "1.20",
        "tradestatus": "1",
        "pctChg": "5.00",
        "peTTM": "8.00",
        "pbMRQ": "1.00",
        "psTTM": "2.00",
        "pcfNcfTTM": "3.00",
        "isST": "0",
    }
    partition = _publish(
        tmp_path,
        "query_daily_history_k_AStock",
        DAILY_BAR_FIELDS,
        (values,),
    )

    outputs = {
        batch.dataset: batch.frame for batch in BaoStockMapper().normalize(partition)
    }

    assert set(outputs) == {
        DatasetKind.DAILY_BAR,
        DatasetKind.DAILY_BASIC,
        DatasetKind.SECURITY_STATUS,
    }
    assert outputs[DatasetKind.DAILY_BAR].schema["close"] == pl.Float64
    assert outputs[DatasetKind.DAILY_BAR]["available_at"].to_list() == [
        datetime(2026, 7, 31, 7, tzinfo=UTC)
    ]


def test_etf_raw_mapper_merges_daily_bars_and_shared_security_status(
    tmp_path: Path,
) -> None:
    values = {
        "date": "2026-07-31",
        "code": "sh.510300",
        "open": "4.00",
        "high": "4.10",
        "low": "3.95",
        "close": "4.05",
        "preclose": "4.00",
        "volume": "1000",
        "amount": "4050.00",
        "adjustflag": "3",
        "turn": "",
        "tradestatus": "1",
        "pctChg": "1.25",
        "peTTM": "",
        "pbMRQ": "",
        "psTTM": "",
        "pcfNcfTTM": "",
        "isST": "1",
    }
    partition = _publish(
        tmp_path,
        "query_etf_history_k_data_plus",
        DAILY_BAR_FIELDS,
        (values,),
    )

    outputs = {
        batch.dataset: batch.frame for batch in BaoStockMapper().normalize(partition)
    }

    assert set(outputs) == {DatasetKind.DAILY_BAR, DatasetKind.SECURITY_STATUS}
    assert outputs[DatasetKind.DAILY_BAR].select(
        "instrument_id", "trade_date", "close"
    ).rows() == [("510300.SH", date(2026, 7, 31), 4.05)]
    assert outputs[DatasetKind.SECURITY_STATUS].select(
        "instrument_id", "is_suspended", "is_st", "tradable_reason"
    ).rows() == [("510300.SH", False, False, "NORMAL")]


def test_mapper_rejects_raw_schema_drift(tmp_path: Path) -> None:
    partition = _publish(
        tmp_path,
        "query_trade_dates",
        ("calendar_date", "unexpected"),
        ({"calendar_date": "2026-07-31", "unexpected": "1"},),
    )

    with pytest.raises(QuantError) as caught:
        tuple(BaoStockMapper().normalize(partition))

    assert caught.value.detail.code == "DATA_SCHEMA_MISMATCH"
    assert caught.value.detail.context["endpoint"] == "query_trade_dates"


def test_industry_mapper_preserves_explicit_empty_classification_as_tombstone(
    tmp_path: Path,
) -> None:
    partition = _publish(
        tmp_path,
        "query_stock_industry",
        INDUSTRY_FIELDS,
        (
            {
                "code": "sh.600000",
                "code_name": "浦发银行",
                "industry": "货币金融服务",
                "industryClassification": "证监会行业分类",
                "updateDate": "2026-01-05",
                "as_of_date": "2026-12-31",
            },
            {
                "code": "sh.600001",
                "code_name": "邯郸钢铁",
                "industry": "",
                "industryClassification": "证监会行业分类",
                "updateDate": "2026-01-05",
                "as_of_date": "2026-12-31",
            },
        ),
    )

    (batch,) = tuple(BaoStockMapper().normalize(partition))

    assert batch.dataset is DatasetKind.INDUSTRY_CLASSIFICATION
    assert batch.frame.select(
        "as_of_date",
        "supplier_update_date",
        "instrument_id",
        "taxonomy",
        "industry_name",
        "is_classified",
        "availability_source",
    ).rows() == [
        (
            date(2026, 12, 31),
            date(2026, 1, 5),
            "600000.SH",
            "证监会行业分类",
            "货币金融服务",
            True,
            "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED",
        ),
        (
            date(2026, 12, 31),
            date(2026, 1, 5),
            "600001.SH",
            "证监会行业分类",
            None,
            False,
            "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED",
        ),
    ]


def test_industry_mapper_still_rejects_an_empty_taxonomy(tmp_path: Path) -> None:
    partition = _publish(
        tmp_path,
        "query_stock_industry",
        INDUSTRY_FIELDS,
        (
            {
                "code": "sh.600000",
                "code_name": "浦发银行",
                "industry": "货币金融服务",
                "industryClassification": "",
                "updateDate": "2026-01-05",
                "as_of_date": "2026-12-31",
            },
        ),
    )

    with pytest.raises(QuantError) as caught:
        tuple(BaoStockMapper().normalize(partition))

    assert caught.value.detail.code == "DATA_SCHEMA_MISMATCH"
    assert caught.value.detail.context["field"] == "industryClassification"
