"""Behavior tests for the pure BaoStock raw-to-canonical mapper."""

from __future__ import annotations

import importlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from quant_core.data.contracts import JsonValue, PublishedPartition, RawBatch
from quant_core.data.partitions import RawPartitionStore
from quant_core.domain.enums import DatasetKind
from quant_core.domain.identifiers import InstrumentId
from quant_core.errors import QuantError

RETRIEVED_AT = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
DAILY_FIELDS = (
    "date",
    "code",
    "open",
    "high",
    "low",
    "close",
    "preclose",
    "volume",
    "amount",
    "adjustflag",
    "turn",
    "tradestatus",
    "pctChg",
    "peTTM",
    "pbMRQ",
    "psTTM",
    "pcfNcfTTM",
    "isST",
)
INSTRUMENT_FIELDS = ("code", "code_name", "ipoDate", "outDate", "type", "status")
CALENDAR_FIELDS = ("calendar_date", "is_trading_day")
FINANCIAL_FIELDS = ("code", "pubDate", "statDate", "metric", "value")
ACTION_FIELDS = (
    "code",
    "dividPreNoticeDate",
    "dividAgmPumDate",
    "dividPlanAnnounceDate",
    "dividPlanDate",
    "dividRegistDate",
    "dividOperateDate",
    "dividPayDate",
    "dividStockMarketDate",
    "dividCashPsBeforeTax",
    "dividCashPsAfterTax",
    "dividStocksPs",
    "dividCashStock",
    "dividReserveToStockPs",
)


def _mapper_type() -> type[Any]:
    try:
        module = importlib.import_module("quant_core.data.mappers.baostock")
    except ModuleNotFoundError:
        pytest.fail("BaoStockMapper has not been implemented")
    return module.BaoStockMapper


def _publish(
    root: Path,
    dataset: str,
    schema: Sequence[str],
    rows: Sequence[Mapping[str, JsonValue]],
    *,
    run_id: str = "mapping-test",
    retrieved_at: datetime = RETRIEVED_AT,
) -> PublishedPartition:
    batch = RawBatch(
        provider="baostock",
        dataset=dataset,
        request={"fixture": dataset},
        retrieved_at=retrieved_at,
        schema=tuple(schema),
        rows=rows,
    )
    return RawPartitionStore(root).publish(batch, run_id=run_id)


def _normalize(partition: PublishedPartition) -> dict[DatasetKind, pl.DataFrame]:
    batches = tuple(_mapper_type()().normalize(partition))
    assert all(
        batch.source_content_hashes == (partition.content_hash,) for batch in batches
    )
    return {batch.dataset: batch.frame for batch in batches}


def _assert_audit_columns(frame: pl.DataFrame, partition: PublishedPartition) -> None:
    assert frame["source"].to_list() == ["baostock"] * frame.height
    assert frame["source_version"].to_list() == [partition.content_hash] * frame.height
    assert frame["ingested_at"].to_list() == [RETRIEVED_AT] * frame.height


def test_instruments_map_codes_types_boards_null_dates_and_sorting(
    tmp_path: Path,
) -> None:
    partition = _publish(
        tmp_path,
        "instruments",
        INSTRUMENT_FIELDS,
        [
            {
                "code": "sz.300001",
                "code_name": "特锐德",
                "ipoDate": "2009-10-30",
                "outDate": "",
                "type": "1",
                "status": "1",
            },
            {
                "code": "sh.688001",
                "code_name": "华兴源创",
                "ipoDate": "",
                "outDate": "2026-01-02",
                "type": "5",
                "status": "0",
            },
        ],
    )

    outputs = _normalize(partition)

    assert set(outputs) == {DatasetKind.INSTRUMENT}
    frame = outputs[DatasetKind.INSTRUMENT]
    assert frame.schema == pl.Schema(
        {
            "instrument_id": pl.String,
            "exchange": pl.String,
            "board": pl.String,
            "name": pl.String,
            "instrument_type": pl.String,
            "listing_status": pl.String,
            "list_date": pl.Date,
            "delist_date": pl.Date,
            "source": pl.String,
            "source_version": pl.String,
            "available_at": pl.Datetime("us", "UTC"),
            "availability_source": pl.String,
            "pit_usable": pl.Boolean,
            "ingested_at": pl.Datetime("us", "UTC"),
        }
    )
    assert frame.select(
        "instrument_id",
        "exchange",
        "board",
        "instrument_type",
        "listing_status",
        "list_date",
        "delist_date",
    ).rows() == [
        ("SSE:688001", "SSE", "STAR", "ETF", "DELISTED", None, date(2026, 1, 2)),
        ("SZSE:300001", "SZSE", "CHINEXT", "STOCK", "LISTED", date(2009, 10, 30), None),
    ]
    assert frame["available_at"].to_list() == [RETRIEVED_AT, RETRIEVED_AT]
    assert frame["availability_source"].to_list() == ["RAW_RETRIEVED_AT"] * 2
    assert frame["pit_usable"].to_list() == [True, True]
    _assert_audit_columns(frame, partition)


def test_mapped_instrument_ids_round_trip_through_the_domain_contract(
    tmp_path: Path,
) -> None:
    partition = _publish(
        tmp_path,
        "instruments",
        INSTRUMENT_FIELDS,
        [
            {
                "code": "sh.600000",
                "code_name": "浦发银行",
                "ipoDate": "1999-11-10",
                "outDate": "",
                "type": "1",
                "status": "1",
            },
            {
                "code": "sz.000001",
                "code_name": "平安银行",
                "ipoDate": "1991-04-03",
                "outDate": "",
                "type": "1",
                "status": "1",
            },
        ],
    )

    mapped_ids = _normalize(partition)[DatasetKind.INSTRUMENT]["instrument_id"]

    assert [
        InstrumentId.parse(value).canonical() for value in mapped_ids
    ] == mapped_ids.to_list()


def test_trade_calendar_maps_strict_flags_and_sorts(tmp_path: Path) -> None:
    partition = _publish(
        tmp_path,
        "trade_calendar",
        CALENDAR_FIELDS,
        [
            {"calendar_date": "2026-01-02", "is_trading_day": "1"},
            {"calendar_date": "2026-01-01", "is_trading_day": "0"},
        ],
    )

    frame = _normalize(partition)[DatasetKind.TRADE_CALENDAR]

    assert frame.select("trade_date", "is_trading_day").rows() == [
        (date(2026, 1, 1), False),
        (date(2026, 1, 2), True),
    ]
    assert frame.schema["is_trading_day"] == pl.Boolean
    _assert_audit_columns(frame, partition)


def _daily_row(
    trade_date: str,
    code: str,
    *,
    close: str = "10.50",
    optional: str = "1.25",
    trade_status: str = "1",
    is_st: str = "0",
) -> dict[str, str]:
    values = (
        trade_date,
        code,
        "10.00",
        "10.80",
        "9.90",
        close,
        "9.95",
        "123400",
        "1260000.25",
        "3",
        optional,
        trade_status,
        optional,
        optional,
        optional,
        optional,
        optional,
        is_st,
    )
    return dict(zip(DAILY_FIELDS, values, strict=True))


def test_daily_raw_yields_typed_bars_and_security_status(tmp_path: Path) -> None:
    partition = _publish(
        tmp_path,
        "daily_bars",
        DAILY_FIELDS,
        [
            _daily_row(
                "2026-01-02",
                "sz.300001",
                close="",
                optional="",
                trade_status="0",
                is_st="1",
            ),
            _daily_row("2026-01-01", "sh.600000"),
        ],
    )

    outputs = _normalize(partition)

    assert set(outputs) == {DatasetKind.DAILY_BAR, DatasetKind.SECURITY_STATUS}
    bars = outputs[DatasetKind.DAILY_BAR]
    assert bars.select("instrument_id", "trade_date", "close", "volume").rows() == [
        ("SSE:600000", date(2026, 1, 1), 10.5, 123400),
        ("SZSE:300001", date(2026, 1, 2), None, 123400),
    ]
    for column in (
        "open",
        "high",
        "low",
        "close",
        "preclose",
        "amount",
        "turnover",
        "pct_change",
        "pe_ttm",
        "pb_mrq",
        "ps_ttm",
        "pcf_ncf_ttm",
    ):
        assert bars.schema[column] == pl.Float64
    assert bars.schema["volume"] == pl.Int64
    assert bars["turnover"].to_list() == [1.25, None]
    statuses = outputs[DatasetKind.SECURITY_STATUS]
    assert statuses.select(
        "instrument_id",
        "is_listed",
        "is_suspended",
        "is_risk_warning",
        "board",
        "price_limit_rule_id",
        "tradable_reason",
    ).rows() == [
        ("SSE:600000", True, False, False, "MAIN", "UNRESOLVED", "NORMAL"),
        (
            "SZSE:300001",
            True,
            True,
            True,
            "CHINEXT",
            "UNRESOLVED",
            "SUSPENDED",
        ),
    ]
    _assert_audit_columns(bars, partition)
    _assert_audit_columns(statuses, partition)


def test_daily_bars_use_market_close_availability_for_historical_rows(
    tmp_path: Path,
) -> None:
    """Using retrieval time would make historical daily data invisible in its session."""
    retrieved_at = datetime(2026, 8, 1, 2, tzinfo=UTC)
    partition = _publish(
        tmp_path,
        "daily_bars",
        DAILY_FIELDS,
        [_daily_row("2024-04-26", "sh.600000")],
        retrieved_at=retrieved_at,
    )

    outputs = _normalize(partition)
    bars = outputs[DatasetKind.DAILY_BAR]
    statuses = outputs[DatasetKind.SECURITY_STATUS]
    expected_available = datetime(
        2024, 4, 26, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")
    ).astimezone(UTC)

    assert bars["available_at"].item() == expected_available
    assert statuses["available_at"].item() == expected_available
    assert bars["ingested_at"].item() == retrieved_at
    assert bars["availability_source"].item() == "MARKET_CLOSE_DERIVED"
    assert bars["pit_usable"].item() is True


def test_daily_bars_mark_incomplete_session_before_market_close(
    tmp_path: Path,
) -> None:
    """A pre-close daily row must not be usable even if malformed acquisition reaches mapping."""
    partition = _publish(
        tmp_path,
        "daily_bars",
        DAILY_FIELDS,
        [_daily_row("2024-04-26", "sh.600000")],
        retrieved_at=datetime(2024, 4, 26, 14, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    outputs = _normalize(partition)
    bars = outputs[DatasetKind.DAILY_BAR]
    statuses = outputs[DatasetKind.SECURITY_STATUS]

    assert bars["available_at"].item() is None
    assert statuses["available_at"].item() is None
    assert bars["pit_usable"].item() is False
    assert statuses["pit_usable"].item() is False
    assert bars["availability_source"].item() == "MARKET_SESSION_INCOMPLETE"
    assert statuses["availability_source"].item() == "MARKET_SESSION_INCOMPLETE"


def test_financials_preserve_unknown_announcement_and_assign_revisions(
    tmp_path: Path,
) -> None:
    partition = _publish(
        tmp_path,
        "financials",
        FINANCIAL_FIELDS,
        [
            {
                "code": "sh.600000",
                "pubDate": "2026-04-30",
                "statDate": "2026-03-31",
                "metric": "roe_avg",
                "value": "8.25",
            },
            {
                "code": "sh.600000",
                "pubDate": "",
                "statDate": "2026-03-31",
                "metric": "roe_avg",
                "value": "",
            },
        ],
    )

    frame = _normalize(partition)[DatasetKind.FINANCIAL_OBSERVATION]

    assert frame.select(
        "instrument_id",
        "report_period",
        "metric",
        "value",
        "revision",
        "announced_at",
        "available_at",
        "availability_source",
        "pit_usable",
    ).rows() == [
        (
            "SSE:600000",
            date(2026, 3, 31),
            "roe_avg",
            8.25,
            0,
            datetime(2026, 4, 30, 15, 59, 59, 999999, tzinfo=UTC),
            datetime(2026, 4, 30, 15, 59, 59, 999999, tzinfo=UTC),
            "INFERRED_PUBLICATION_DATE",
            True,
        ),
        (
            "SSE:600000",
            date(2026, 3, 31),
            "roe_avg",
            None,
            1,
            None,
            None,
            "UNKNOWN_ANNOUNCEMENT_DATE",
            False,
        ),
    ]
    assert frame.schema["value"] == pl.Float64
    assert frame.schema["revision"] == pl.Int64
    _assert_audit_columns(frame, partition)


def test_corporate_actions_use_only_plan_announcement_for_availability(
    tmp_path: Path,
) -> None:
    partition = _publish(
        tmp_path,
        "corporate_actions",
        ACTION_FIELDS,
        [
            {
                "code": "sh.600000",
                "dividPreNoticeDate": "2026-01-01",
                "dividAgmPumDate": "2026-02-01",
                "dividPlanAnnounceDate": "",
                "dividPlanDate": "2026-03-01",
                "dividRegistDate": "2026-06-01",
                "dividOperateDate": "2026-06-02",
                "dividPayDate": "2026-06-03",
                "dividStockMarketDate": "",
                "dividCashPsBeforeTax": "0.30",
                "dividCashPsAfterTax": "0.24",
                "dividStocksPs": "0.10",
                "dividCashStock": "",
                "dividReserveToStockPs": "0.05",
            },
            {
                "code": "sz.000001",
                "dividPreNoticeDate": "",
                "dividAgmPumDate": "",
                "dividPlanAnnounceDate": "2026-04-01",
                "dividPlanDate": "",
                "dividRegistDate": "2026-05-01",
                "dividOperateDate": "2026-05-02",
                "dividPayDate": "2026-05-03",
                "dividStockMarketDate": "",
                "dividCashPsBeforeTax": "",
                "dividCashPsAfterTax": "",
                "dividStocksPs": "",
                "dividCashStock": "",
                "dividReserveToStockPs": "",
            },
        ],
    )

    frame = _normalize(partition)[DatasetKind.CORPORATE_ACTION]

    assert frame.select(
        "instrument_id",
        "action_type",
        "record_date",
        "ex_date",
        "pay_date",
        "cash_per_share",
        "share_ratio",
        "rights_price",
        "available_at",
        "availability_source",
        "pit_usable",
    ).rows() == [
        (
            "SSE:600000",
            "DIVIDEND",
            date(2026, 6, 1),
            date(2026, 6, 2),
            date(2026, 6, 3),
            0.3,
            0.15,
            None,
            None,
            "UNKNOWN_ANNOUNCEMENT_DATE",
            False,
        ),
        (
            "SZSE:000001",
            "DIVIDEND",
            date(2026, 5, 1),
            date(2026, 5, 2),
            date(2026, 5, 3),
            None,
            None,
            None,
            datetime(2026, 4, 1, 15, 59, 59, 999999, tzinfo=UTC),
            "INFERRED_PUBLICATION_DATE",
            True,
        ),
    ]
    _assert_audit_columns(frame, partition)


def test_schema_drift_has_partition_and_field_diff_context(tmp_path: Path) -> None:
    partition = _publish(
        tmp_path,
        "trade_calendar",
        ("calendar_date", "unexpected"),
        [{"calendar_date": "2026-01-01", "unexpected": "1"}],
    )

    with pytest.raises(QuantError) as caught:
        _normalize(partition)

    assert caught.value.detail.code == "DATA_SCHEMA_MISMATCH"
    assert caught.value.detail.context == {
        "data_path": str(partition.data_path),
        "dataset": "trade_calendar",
        "expected_fields": ["calendar_date", "is_trading_day"],
        "actual_fields": ["calendar_date", "unexpected"],
        "missing_fields": ["is_trading_day"],
        "extra_fields": ["unexpected"],
    }


def test_non_object_manifest_is_a_structured_schema_mismatch(tmp_path: Path) -> None:
    partition = _publish(
        tmp_path,
        "trade_calendar",
        CALENDAR_FIELDS,
        [{"calendar_date": "2026-01-01", "is_trading_day": "1"}],
    )
    partition.manifest_path.write_text("[]", encoding="utf-8")

    with pytest.raises(QuantError) as caught:
        _normalize(partition)

    assert caught.value.detail.code == "DATA_SCHEMA_MISMATCH"
    assert caught.value.detail.context["data_path"] == str(partition.data_path)
    assert caught.value.detail.context["dataset"] == "trade_calendar"


@pytest.mark.parametrize(
    ("field", "mutation"),
    [
        ("request_hash", "remove"),
        ("request_hash", "replace"),
        ("retrieved_at", "remove"),
        ("retrieved_at", "replace"),
    ],
)
def test_manifest_requires_matching_request_identity_and_retrieval_time(
    tmp_path: Path,
    field: str,
    mutation: str,
) -> None:
    partition = _publish(
        tmp_path,
        "trade_calendar",
        CALENDAR_FIELDS,
        [{"calendar_date": "2026-01-01", "is_trading_day": "1"}],
    )
    manifest = json.loads(partition.manifest_path.read_text(encoding="utf-8"))
    if mutation == "remove":
        del manifest[field]
    else:
        manifest[field] = "tampered"
    partition.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(QuantError) as caught:
        _normalize(partition)

    assert caught.value.detail.code == "DATA_SCHEMA_MISMATCH"
    assert caught.value.detail.context["dataset"] == "trade_calendar"


def test_duplicate_canonical_primary_key_is_rejected(tmp_path: Path) -> None:
    row = _daily_row("2026-01-01", "sh.600000")
    partition = _publish(tmp_path, "daily_bars", DAILY_FIELDS, [row, row])

    with pytest.raises(QuantError) as caught:
        _normalize(partition)

    assert caught.value.detail.code == "DATA_SCHEMA_MISMATCH"
    assert caught.value.detail.context["dataset"] == "daily_bars"
    assert caught.value.detail.context["duplicate_primary_key"] == [
        "instrument_id",
        "trade_date",
    ]


@pytest.mark.parametrize(
    ("dataset", "schema", "row", "field", "invalid"),
    [
        (
            "trade_calendar",
            CALENDAR_FIELDS,
            {"calendar_date": "2026-01-01", "is_trading_day": "1"},
            "is_trading_day",
            "yes",
        ),
        (
            "daily_bars",
            DAILY_FIELDS,
            _daily_row("2026-01-01", "sh.600000"),
            "volume",
            "1.5",
        ),
        (
            "daily_bars",
            DAILY_FIELDS,
            _daily_row("2026-01-01", "sh.600000"),
            "tradestatus",
            "2",
        ),
        (
            "financials",
            FINANCIAL_FIELDS,
            {
                "code": "sh.600000",
                "pubDate": "2026-04-30",
                "statDate": "2026-03-31",
                "metric": "roe_avg",
                "value": "8.25",
            },
            "statDate",
            "2026-02-30",
        ),
        (
            "instruments",
            INSTRUMENT_FIELDS,
            {
                "code": "sh.600000",
                "code_name": "浦发银行",
                "ipoDate": "1999-11-10",
                "outDate": "",
                "type": "1",
                "status": "1",
            },
            "code",
            "bj.600000",
        ),
    ],
)
def test_invalid_nonempty_values_are_not_silently_coerced(
    tmp_path: Path,
    dataset: str,
    schema: tuple[str, ...],
    row: dict[str, str],
    field: str,
    invalid: str,
) -> None:
    invalid_row = dict(row)
    invalid_row[field] = invalid
    partition = _publish(tmp_path, dataset, schema, [invalid_row])

    with pytest.raises(QuantError) as caught:
        _normalize(partition)

    assert caught.value.detail.code == "DATA_SCHEMA_MISMATCH"
    assert caught.value.detail.context["dataset"] == dataset
    assert caught.value.detail.context["field"] == field
