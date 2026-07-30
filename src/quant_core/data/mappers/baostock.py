"""Pure mapping from published BaoStock raw partitions to canonical frames."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, Never
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.data.contracts import CanonicalBatch, PublishedPartition
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.data.sources.baostock import DAILY_BAR_FIELDS, from_baostock_code
from quant_core.domain.enums import DatasetKind, Exchange, Severity
from quant_core.errors import ErrorDetail, QuantError

INSTRUMENT_FIELDS = ("code", "code_name", "ipoDate", "outDate", "type", "status")
TRADE_CALENDAR_FIELDS = ("calendar_date", "is_trading_day")
FINANCIAL_FIELDS = ("code", "pubDate", "statDate", "metric", "value")
CORPORATE_ACTION_FIELDS = (
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

BAOSTOCK_RAW_SCHEMAS: dict[str, tuple[str, ...]] = {
    "instruments": INSTRUMENT_FIELDS,
    "trade_calendar": TRADE_CALENDAR_FIELDS,
    "daily_bars": DAILY_BAR_FIELDS,
    "financials": FINANCIAL_FIELDS,
    "corporate_actions": CORPORATE_ACTION_FIELDS,
}

_INTEGER = re.compile(r"[+-]?[0-9]+\Z")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_TYPE_NAMES = {
    "1": "STOCK",
    "2": "INDEX",
    "4": "CONVERTIBLE_BOND",
    "5": "ETF",
}

type RawRow = Mapping[str, Any]
type MapperResult = tuple[DatasetKind, list[dict[str, object | None]]]


class BaoStockMapper:
    """Normalize immutable BaoStock evidence without a provider session."""

    def normalize(self, raw_partition: PublishedPartition) -> Iterable[CanonicalBatch]:
        """Read one visible partition, validate it, and return canonical batches."""
        expected_fields = BAOSTOCK_RAW_SCHEMAS.get(raw_partition.dataset, ())
        if raw_partition.provider != "baostock" or not expected_fields:
            _raise_mismatch(
                raw_partition,
                expected_fields,
                (),
                "unsupported BaoStock raw partition",
            )
        table = _validated_table(raw_partition, expected_fields)
        rows = table.to_pylist()
        mapper = _MAPPERS[raw_partition.dataset]
        for dataset, records in mapper(raw_partition, rows):
            yield _canonical_batch(raw_partition, dataset, records)


def _validated_table(
    partition: PublishedPartition,
    expected_fields: tuple[str, ...],
) -> pa.Table:
    actual_fields: tuple[str, ...] = ()
    try:
        manifest = json.loads(partition.manifest_path.read_text(encoding="utf-8"))
        table = pq.read_table(partition.data_path)
        actual_fields = tuple(table.column_names)
    except (
        OSError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        pa.ArrowException,
    ) as error:
        _raise_mismatch(
            partition,
            expected_fields,
            actual_fields,
            "published raw partition is unreadable",
            cause=error,
        )
    if not isinstance(manifest, dict):
        _raise_mismatch(
            partition,
            expected_fields,
            actual_fields,
            "published raw manifest must be a JSON object",
        )

    integrity_matches = (
        manifest.get("provider") == partition.provider
        and manifest.get("dataset") == partition.dataset
        and manifest.get("content_hash") == partition.content_hash
        and manifest.get("schema_fingerprint") == partition.schema_fingerprint
        and manifest.get("row_count") == partition.row_count
        and _content_hash(table) == partition.content_hash
        and _schema_fingerprint(table.schema) == partition.schema_fingerprint
        and table.num_rows == partition.row_count
    )
    if not integrity_matches:
        _raise_mismatch(
            partition,
            expected_fields,
            actual_fields,
            "published raw partition failed manifest integrity validation",
        )
    if actual_fields != expected_fields:
        _raise_mismatch(
            partition,
            expected_fields,
            actual_fields,
            "BaoStock raw fields do not match the declared schema",
        )
    return table


def _content_hash(table: pa.Table) -> str:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def _schema_fingerprint(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _canonical_batch(
    partition: PublishedPartition,
    dataset: DatasetKind,
    records: list[dict[str, object | None]],
) -> CanonicalBatch:
    definition = CANONICAL_SCHEMAS[dataset]
    try:
        frame = pl.DataFrame(records, schema=definition.columns, strict=True)
    except (TypeError, ValueError, pl.exceptions.PolarsError) as error:
        _raise_mismatch(
            partition,
            BAOSTOCK_RAW_SCHEMAS[partition.dataset],
            BAOSTOCK_RAW_SCHEMAS[partition.dataset],
            f"canonical {dataset.value} frame does not match its declared schema",
            cause=error,
        )
    duplicate_count = (
        frame.group_by(list(definition.primary_key))
        .len()
        .filter(pl.col("len") > 1)
        .height
        if frame.height
        else 0
    )
    if duplicate_count:
        _raise_mismatch(
            partition,
            BAOSTOCK_RAW_SCHEMAS[partition.dataset],
            BAOSTOCK_RAW_SCHEMAS[partition.dataset],
            f"canonical {dataset.value} primary key is not unique",
            duplicate_primary_key=list(definition.primary_key),
        )
    return CanonicalBatch(
        dataset=dataset,
        frame=frame.sort(list(definition.sort_key), nulls_last=True),
        source_content_hashes=(partition.content_hash,),
    )


def _instrument_rows(
    partition: PublishedPartition, rows: Sequence[RawRow]
) -> tuple[MapperResult]:
    records: list[dict[str, object | None]] = []
    for row in rows:
        code, exchange, board = _identity(partition, row, "code")
        status = _flag(partition, row, "status")
        records.append(
            {
                "instrument_id": code,
                "exchange": exchange.value,
                "board": board,
                "name": _required_text(partition, row, "code_name"),
                "instrument_type": _TYPE_NAMES.get(
                    _required_text(partition, row, "type"), "UNKNOWN"
                ),
                "listing_status": "LISTED" if status else "DELISTED",
                "list_date": _optional_date(partition, row, "ipoDate"),
                "delist_date": _optional_date(partition, row, "outDate"),
                **_raw_availability(partition),
            }
        )
    return ((DatasetKind.INSTRUMENT, records),)


def _calendar_rows(
    partition: PublishedPartition, rows: Sequence[RawRow]
) -> tuple[MapperResult]:
    records = [
        {
            "trade_date": _required_date(partition, row, "calendar_date"),
            "is_trading_day": _flag(partition, row, "is_trading_day"),
            **_raw_availability(partition),
        }
        for row in rows
    ]
    return ((DatasetKind.TRADE_CALENDAR, records),)


def _daily_rows(
    partition: PublishedPartition, rows: Sequence[RawRow]
) -> tuple[MapperResult, MapperResult]:
    bars: list[dict[str, object | None]] = []
    statuses: list[dict[str, object | None]] = []
    for row in rows:
        code, _, board = _identity(partition, row, "code")
        trade_date = _required_date(partition, row, "date")
        trade_status = _flag(partition, row, "tradestatus")
        risk_warning = _flag(partition, row, "isST")
        audit = _raw_availability(partition)
        bars.append(
            {
                "instrument_id": code,
                "trade_date": trade_date,
                "open": _optional_float(partition, row, "open"),
                "high": _optional_float(partition, row, "high"),
                "low": _optional_float(partition, row, "low"),
                "close": _optional_float(partition, row, "close"),
                "preclose": _optional_float(partition, row, "preclose"),
                "volume": _optional_int(partition, row, "volume"),
                "amount": _optional_float(partition, row, "amount"),
                "adjustment_flag": _required_text(partition, row, "adjustflag"),
                "turnover": _optional_float(partition, row, "turn"),
                "pct_change": _optional_float(partition, row, "pctChg"),
                "pe_ttm": _optional_float(partition, row, "peTTM"),
                "pb_mrq": _optional_float(partition, row, "pbMRQ"),
                "ps_ttm": _optional_float(partition, row, "psTTM"),
                "pcf_ncf_ttm": _optional_float(partition, row, "pcfNcfTTM"),
                **audit,
            }
        )
        suspended = not trade_status
        reason = (
            "SUSPENDED" if suspended else "RISK_WARNING" if risk_warning else "NORMAL"
        )
        statuses.append(
            {
                "instrument_id": code,
                "trade_date": trade_date,
                "is_listed": True,
                "is_suspended": suspended,
                "is_risk_warning": risk_warning,
                "board": board,
                "price_limit_rule_id": "UNRESOLVED",
                "tradable_reason": reason,
                **audit,
            }
        )
    return (
        (DatasetKind.DAILY_BAR, bars),
        (DatasetKind.SECURITY_STATUS, statuses),
    )


def _financial_rows(
    partition: PublishedPartition, rows: Sequence[RawRow]
) -> tuple[MapperResult]:
    revisions: dict[tuple[str, date, str], int] = {}
    records: list[dict[str, object | None]] = []
    for row in rows:
        code, _, _ = _identity(partition, row, "code")
        report_period = _required_date(partition, row, "statDate")
        metric = _required_text(partition, row, "metric")
        key = (code, report_period, metric)
        revision = revisions.get(key, 0)
        revisions[key] = revision + 1
        announced_at = _announcement_time(partition, row, "pubDate")
        records.append(
            {
                "instrument_id": code,
                "report_period": report_period,
                "metric": metric,
                "value": _optional_float(partition, row, "value"),
                "revision": revision,
                "announced_at": announced_at,
                **_announcement_availability(partition, announced_at),
            }
        )
    return ((DatasetKind.FINANCIAL_OBSERVATION, records),)


def _action_rows(
    partition: PublishedPartition, rows: Sequence[RawRow]
) -> tuple[MapperResult]:
    records: list[dict[str, object | None]] = []
    for row in rows:
        code, _, _ = _identity(partition, row, "code")
        announced_at = _announcement_time(partition, row, "dividPlanAnnounceDate")
        stock_ratio = _optional_float(partition, row, "dividStocksPs")
        reserve_ratio = _optional_float(partition, row, "dividReserveToStockPs")
        share_ratio = (
            None
            if stock_ratio is None and reserve_ratio is None
            else float(
                Decimal(str(stock_ratio or 0.0)) + Decimal(str(reserve_ratio or 0.0))
            )
        )
        records.append(
            {
                "instrument_id": code,
                "action_type": "DIVIDEND",
                "record_date": _optional_date(partition, row, "dividRegistDate"),
                "ex_date": _optional_date(partition, row, "dividOperateDate"),
                "pay_date": _optional_date(partition, row, "dividPayDate"),
                "cash_per_share": _optional_float(
                    partition, row, "dividCashPsBeforeTax"
                ),
                "share_ratio": share_ratio,
                "rights_price": None,
                **_announcement_availability(partition, announced_at),
            }
        )
    return ((DatasetKind.CORPORATE_ACTION, records),)


_MAPPERS: dict[
    str,
    Callable[[PublishedPartition, Sequence[RawRow]], tuple[MapperResult, ...]],
] = {
    "instruments": _instrument_rows,
    "trade_calendar": _calendar_rows,
    "daily_bars": _daily_rows,
    "financials": _financial_rows,
    "corporate_actions": _action_rows,
}


def _identity(
    partition: PublishedPartition,
    row: RawRow,
    field: str,
) -> tuple[str, Exchange, str]:
    raw_code = _required_text(partition, row, field)
    try:
        instrument = from_baostock_code(raw_code)
    except ValueError as error:
        _raise_value(partition, field, raw_code, error)
    board = (
        "STAR"
        if instrument.exchange is Exchange.SSE and instrument.symbol.startswith("688")
        else "CHINEXT"
        if instrument.exchange is Exchange.SZSE
        and instrument.symbol.startswith(("300", "301"))
        else "MAIN"
    )
    return (
        f"{instrument.exchange.value}.{instrument.symbol}",
        instrument.exchange,
        board,
    )


def _raw_availability(partition: PublishedPartition) -> dict[str, object | None]:
    retrieved_at = partition.retrieved_at.astimezone(UTC)
    return {
        "source": partition.provider,
        "source_version": partition.content_hash,
        "available_at": retrieved_at,
        "availability_source": "RAW_RETRIEVED_AT",
        "pit_usable": True,
        "ingested_at": retrieved_at,
    }


def _announcement_availability(
    partition: PublishedPartition,
    announced_at: datetime | None,
) -> dict[str, object | None]:
    return {
        "source": partition.provider,
        "source_version": partition.content_hash,
        "available_at": announced_at,
        "availability_source": (
            "INFERRED_PUBLICATION_DATE"
            if announced_at is not None
            else "UNKNOWN_ANNOUNCEMENT_DATE"
        ),
        "pit_usable": announced_at is not None,
        "ingested_at": partition.retrieved_at.astimezone(UTC),
    }


def _required_text(partition: PublishedPartition, row: RawRow, field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        _raise_value(partition, field, value)
    return value


def _flag(partition: PublishedPartition, row: RawRow, field: str) -> bool:
    value = row.get(field)
    if not isinstance(value, str) or value not in ("0", "1"):
        _raise_value(partition, field, value)
    return value == "1"


def _required_date(partition: PublishedPartition, row: RawRow, field: str) -> date:
    value = _required_text(partition, row, field)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        _raise_value(partition, field, value, error)
    if parsed.isoformat() != value:
        _raise_value(partition, field, value)
    return parsed


def _optional_date(
    partition: PublishedPartition,
    row: RawRow,
    field: str,
) -> date | None:
    value = row.get(field)
    if value == "":
        return None
    return _required_date(partition, row, field)


def _announcement_time(
    partition: PublishedPartition,
    row: RawRow,
    field: str,
) -> datetime | None:
    published_on = _optional_date(partition, row, field)
    if published_on is None:
        return None
    local_end = datetime.combine(published_on, time.max, tzinfo=_SHANGHAI)
    return local_end.astimezone(UTC)


def _optional_float(
    partition: PublishedPartition,
    row: RawRow,
    field: str,
) -> float | None:
    value = row.get(field)
    if value == "":
        return None
    if not isinstance(value, str):
        _raise_value(partition, field, value)
    try:
        parsed = float(value)
    except ValueError as error:
        _raise_value(partition, field, value, error)
    if not math.isfinite(parsed):
        _raise_value(partition, field, value)
    return parsed


def _optional_int(
    partition: PublishedPartition,
    row: RawRow,
    field: str,
) -> int | None:
    value = row.get(field)
    if value == "":
        return None
    if not isinstance(value, str) or not _INTEGER.fullmatch(value):
        _raise_value(partition, field, value)
    return int(value)


def _raise_value(
    partition: PublishedPartition,
    field: str,
    value: object,
    cause: Exception | None = None,
) -> Never:
    _raise_mismatch(
        partition,
        BAOSTOCK_RAW_SCHEMAS[partition.dataset],
        BAOSTOCK_RAW_SCHEMAS[partition.dataset],
        f"BaoStock raw field {field!r} contains an invalid value",
        cause=cause,
        field=field,
        value=value,
    )


def _raise_mismatch(
    partition: PublishedPartition,
    expected_fields: Sequence[str],
    actual_fields: Sequence[str],
    message: str,
    *,
    cause: Exception | None = None,
    **extra_context: object,
) -> Never:
    expected = list(expected_fields)
    actual = list(actual_fields)
    detail = ErrorDetail(
        code="DATA_SCHEMA_MISMATCH",
        severity=Severity.SEVERE,
        message=message,
        context={
            "data_path": str(partition.data_path),
            "dataset": partition.dataset,
            "expected_fields": expected,
            "actual_fields": actual,
            "missing_fields": sorted(set(expected).difference(actual)),
            "extra_fields": sorted(set(actual).difference(expected)),
            **extra_context,
        },
        remediation="inspect the published raw partition and provider schema",
        retryable=False,
    )
    error = QuantError(detail)
    if cause is None:
        raise error
    raise error from cause
