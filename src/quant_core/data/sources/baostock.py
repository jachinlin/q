"""BaoStock SDK boundary and reproducible daily-bar acquisition client."""

from __future__ import annotations

import hashlib
import importlib
import logging
import math
import re
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Protocol, cast
from zoneinfo import ZoneInfo

from quant_core.data.contracts import JsonValue, RawBatch
from quant_core.domain.enums import Exchange, Severity
from quant_core.domain.identifiers import InstrumentId
from quant_core.errors import ErrorDetail, QuantError

BAOSTOCK_SOURCE_ADAPTER_VERSION = "baostock-source-adapter-v1"

DAILY_BAR_FIELDS = (
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
TRADE_CALENDAR_FIELDS = ("calendar_date", "is_trading_day")
_DAILY_BAR_FIELD_ARGUMENT = ",".join(DAILY_BAR_FIELDS)
_BAOSTOCK_CODE = re.compile(r"(sh|sz)\.([0-9]{6})\Z")


class BaoStockResponse(Protocol):
    """The status surface common to BaoStock SDK results."""

    error_code: str
    error_msg: str


class BaoStockCursor(BaoStockResponse, Protocol):
    """The iterable result surface returned by BaoStock history queries."""

    fields: Sequence[str]

    def next(self) -> bool:
        """Advance across rows, including SDK-managed result pages."""

    def get_row_data(self) -> Sequence[str]:
        """Return the provider-native strings for the current row."""


class BaoStockGateway(Protocol):
    """Injectable subset of the external BaoStock SDK."""

    def login(self) -> BaoStockResponse:
        """Open an SDK session."""

    def logout(self) -> BaoStockResponse:
        """Close an SDK session."""

    def query_daily_history_k_AStock(self, date: str = "") -> BaoStockCursor:
        """Query all A-share daily bars for one provider date."""

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> BaoStockCursor:
        """Query one provider code over one closed date interval."""

    def query_stock_basic(self, *, code: str, code_name: str) -> BaoStockCursor:
        """Query the provider-native historical security directory."""

    def query_trade_dates(self, *, start_date: str, end_date: str) -> BaoStockCursor:
        """Query the provider-native exchange calendar."""


class _BaoStockSdk(Protocol):
    """Structural type for the imported external SDK module."""

    def login(self) -> BaoStockResponse:
        """Open an SDK session."""

    def logout(self) -> BaoStockResponse:
        """Close an SDK session."""

    def query_daily_history_k_AStock(self, date: str = "") -> BaoStockCursor:
        """Query all A-share daily bars for one provider date."""

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> BaoStockCursor:
        """Query one provider code over one closed date interval."""

    def query_stock_basic(self, *, code: str, code_name: str) -> BaoStockCursor:
        """Query security master data."""

    def query_trade_dates(self, *, start_date: str, end_date: str) -> BaoStockCursor:
        """Query exchange open dates."""


class BaoStockSdkGateway:
    """Thin wrapper that confines the real SDK import to the provider boundary."""

    def __init__(self, *, sdk: _BaoStockSdk | None = None) -> None:
        self._sdk = sdk or cast(_BaoStockSdk, importlib.import_module("baostock"))

    def login(self) -> BaoStockResponse:
        return self._sdk.login()

    def logout(self) -> BaoStockResponse:
        return self._sdk.logout()

    def query_daily_history_k_AStock(self, date: str = "") -> BaoStockCursor:
        return self._sdk.query_daily_history_k_AStock(date)

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> BaoStockCursor:
        return self._sdk.query_history_k_data_plus(
            code,
            fields,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            adjustflag=adjustflag,
        )

    def query_stock_basic(self, *, code: str, code_name: str) -> BaoStockCursor:
        return self._sdk.query_stock_basic(code=code, code_name=code_name)

    def query_trade_dates(self, *, start_date: str, end_date: str) -> BaoStockCursor:
        return self._sdk.query_trade_dates(start_date=start_date, end_date=end_date)


@dataclass(frozen=True, slots=True)
class InstrumentListing:
    """One canonical instrument's complete exchange listing interval."""

    instrument_id: InstrumentId
    list_date: date
    delist_date: date | None
    provider_type: str = "1"


class InstrumentCatalog(Protocol):
    """Historical instrument directory independent of current listing state."""

    def list_instruments(self) -> Sequence[InstrumentListing]:
        """Return historical listings, including delisted instruments."""


@dataclass(frozen=True, slots=True)
class BaoStockHistoricalCatalog:
    """Reusable historical listing catalog reconstructed from immutable raw rows."""

    listings: tuple[InstrumentListing, ...]

    @classmethod
    def from_raw_rows(
        cls, rows: Sequence[dict[str, JsonValue]]
    ) -> BaoStockHistoricalCatalog:
        listings: list[InstrumentListing] = []
        for row in rows:
            code = row.get("code")
            list_text = row.get("ipoDate")
            delist_text = row.get("outDate")
            provider_type = row.get("type")
            if not isinstance(code, str) or not isinstance(list_text, str):
                raise TypeError("instrument raw rows require string code and ipoDate")
            if not isinstance(delist_text, str):
                raise TypeError("instrument raw outDate must be a string")
            if not isinstance(provider_type, str):
                raise TypeError("instrument raw type must be a string")
            listings.append(
                InstrumentListing(
                    instrument_id=from_baostock_code(code),
                    list_date=date.fromisoformat(list_text),
                    delist_date=(
                        date.fromisoformat(delist_text) if delist_text else None
                    ),
                    provider_type=provider_type,
                )
            )
        return cls(
            tuple(sorted(listings, key=lambda item: item.instrument_id.canonical()))
        )

    def list_instruments(self) -> Sequence[InstrumentListing]:
        return self.listings


@dataclass(frozen=True, slots=True)
class BaoStockConfig:
    """Explicit acquisition limits and retry policy."""

    max_instruments_per_batch: int
    max_days_per_batch: int
    max_attempts: int
    retry_backoff_seconds: tuple[float, ...]
    retryable_error_codes: frozenset[str]

    def __post_init__(self) -> None:
        limits = (
            self.max_instruments_per_batch,
            self.max_days_per_batch,
            self.max_attempts,
        )
        if any(type(limit) is not int or limit <= 0 for limit in limits):
            raise ValueError("batch limits and max_attempts must be positive integers")
        if len(self.retry_backoff_seconds) != self.max_attempts - 1:
            raise ValueError("retry_backoff_seconds must contain one delay per retry")
        if any(
            not math.isfinite(delay) or delay < 0
            for delay in self.retry_backoff_seconds
        ):
            raise ValueError("retry backoff seconds must be finite and non-negative")


def to_baostock_code(instrument_id: InstrumentId) -> str:
    """Convert a vendor-neutral identifier at the BaoStock boundary."""
    prefix = {Exchange.SSE: "sh", Exchange.SZSE: "sz"}[instrument_id.exchange]
    return f"{prefix}.{instrument_id.symbol}"


def from_baostock_code(value: str) -> InstrumentId:
    """Parse one strict BaoStock code into a vendor-neutral identifier."""
    match = _BAOSTOCK_CODE.fullmatch(value)
    if match is None:
        raise ValueError(
            "BaoStock code must be sh. or sz. followed by six ASCII digits"
        )
    prefix, symbol = match.groups()
    exchange = Exchange.SSE if prefix == "sh" else Exchange.SZSE
    instrument_id = InstrumentId(exchange=exchange, symbol=symbol)
    if to_baostock_code(instrument_id) != value:
        raise ValueError("BaoStock code did not pass canonical round-trip validation")
    return instrument_id


def _utc_now() -> datetime:
    return datetime.now(UTC)


class BaoStockClient:
    """Acquire BaoStock daily bars as deterministic provider-native Raw batches."""

    def __init__(
        self,
        gateway: BaoStockGateway,
        catalog: InstrumentCatalog | None,
        config: BaoStockConfig,
        *,
        clock: Callable[[], datetime] = _utc_now,
        sleep: Callable[[float], None] = time.sleep,
        logger: logging.Logger | None = None,
    ) -> None:
        self._gateway = gateway
        self._catalog = catalog
        self._config = config
        self._clock = clock
        self._sleep = sleep
        self._logger = logger or logging.getLogger(__name__)
        self._logged_in = False

    @property
    def provider(self) -> str:
        return "baostock"

    def login(self) -> None:
        """Establish the provider session once."""
        if self._logged_in:
            return

        def perform_login() -> None:
            response = self._gateway.login()
            self._raise_provider_error(response, operation="login")

        self._retry("login", perform_login)
        self._logged_in = True

    def close(self) -> None:
        """Close the provider session once."""
        if not self._logged_in:
            return

        def perform_logout() -> None:
            response = self._gateway.logout()
            self._raise_provider_error(response, operation="logout")

        self._retry("logout", perform_logout)
        self._logged_in = False

    def fetch_daily_bars(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> Iterable[RawBatch]:
        """Yield selected ranges or one all-market batch per open trading day."""
        if not self._logged_in:
            raise self._state_error("fetch_daily_bars")
        if start > end:
            raise ValueError("start must not follow end")
        if instruments is None or len(instruments) == 0:
            if instruments is not None:
                self._logger.info(
                    "empty instrument selection resolved as all-market daily route",
                    extra={"event": "empty_instruments_resolved_as_all", "scope": "ALL"},
                )
            _, catalog_instruments = self._resolve_instruments(start, end, instruments)
            _, open_dates = self._load_trade_calendar(start, end)
            yield from self._fetch_all_market_daily_bars(
                open_dates, catalog_instruments
            )
            return
        yield from self._fetch_selected_daily_bars(start, end, instruments)

    def _fetch_selected_daily_bars(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId],
    ) -> Iterable[RawBatch]:
        """Yield the existing instrument-range batches for explicit selections."""

        scope, resolved = self._resolve_instruments(start, end, instruments)
        canonical_ids = tuple(item.canonical() for item in resolved)
        resolved_hash = hashlib.sha256("\n".join(canonical_ids).encode()).hexdigest()
        instrument_chunks = tuple(
            self._chunks(resolved, self._config.max_instruments_per_batch)
        )
        date_chunks = tuple(self._date_chunks(start, end))
        instrument_chunk_count = len(instrument_chunks)
        date_chunk_count = len(date_chunks)
        batch_count = instrument_chunk_count * date_chunk_count

        for instrument_index, instrument_chunk in enumerate(instrument_chunks, start=1):
            chunk_ids: list[JsonValue] = [item.canonical() for item in instrument_chunk]
            for date_index, (chunk_start, chunk_end) in enumerate(date_chunks, start=1):
                rows: list[dict[str, JsonValue]] = []
                for instrument_id in instrument_chunk:
                    rows.extend(
                        self._fetch_instrument_rows(
                            instrument_id,
                            start=chunk_start,
                            end=chunk_end,
                        )
                    )
                batch_index = (instrument_index - 1) * date_chunk_count + date_index
                request: dict[str, JsonValue] = {
                    "scope": scope,
                    "resolved_instrument_count": len(resolved),
                    "resolved_instruments_sha256": resolved_hash,
                    "instrument_chunk_index": instrument_index,
                    "instrument_chunk_count": instrument_chunk_count,
                    "date_chunk_index": date_index,
                    "date_chunk_count": date_chunk_count,
                    "batch_index": batch_index,
                    "batch_count": batch_count,
                    "start_date": chunk_start.isoformat(),
                    "end_date": chunk_end.isoformat(),
                    "instruments": chunk_ids,
                    "frequency": "d",
                    "adjustflag": "3",
                }
                yield RawBatch(
                    provider="baostock",
                    dataset="daily_bars",
                    request=request,
                    retrieved_at=self._clock(),
                    schema=DAILY_BAR_FIELDS,
                    rows=tuple(rows),
                )

    def _fetch_all_market_daily_bars(
        self,
        open_dates: Sequence[date],
        catalog_instruments: Sequence[InstrumentId],
    ) -> Iterable[RawBatch]:
        """Yield one validated all-market response for each exchange-open date."""
        catalog_ids = sorted(item.canonical() for item in catalog_instruments)
        catalog_hash = hashlib.sha256("\n".join(catalog_ids).encode()).hexdigest()
        for index, trading_day in enumerate(open_dates, start=1):
            rows = self._fetch_all_market_rows(trading_day)
            codes = sorted(cast(str, row["code"]) for row in rows)
            request: dict[str, JsonValue] = {
                "api": "query_daily_history_k_AStock",
                "scope": "ALL",
                "date": trading_day.isoformat(),
                "frequency": "d",
                "catalog_instrument_count": len(catalog_ids),
                "catalog_instruments_sha256": catalog_hash,
                "response_instrument_count": len(codes),
                "response_instruments_sha256": hashlib.sha256(
                    "\n".join(codes).encode()
                ).hexdigest(),
            }
            self._logger.info(
                "BaoStock all-market daily date completed",
                extra={
                    "event": "baostock_all_market_daily_progress",
                    "date": trading_day.isoformat(),
                    "completed_dates": index,
                    "total_dates": len(open_dates),
                    "response_rows": len(rows),
                },
            )
            yield RawBatch(
                provider=self.provider,
                dataset="daily_bars",
                request=request,
                retrieved_at=self._clock(),
                schema=DAILY_BAR_FIELDS,
                rows=tuple(rows),
            )

    def fetch_instruments(self) -> Iterable[RawBatch]:
        """Yield the complete provider-native instrument directory once."""
        if not self._logged_in:
            raise self._state_error("fetch_instruments")
        rows = self._read_cursor(
            "query_stock_basic",
            lambda: self._gateway.query_stock_basic(code="", code_name=""),
            INSTRUMENT_FIELDS,
        )
        self._catalog = BaoStockHistoricalCatalog.from_raw_rows(rows)
        yield RawBatch(
            provider=self.provider,
            dataset="instruments",
            request={"code": "", "code_name": "", "scope": "ALL_HISTORICAL"},
            retrieved_at=self._clock(),
            schema=INSTRUMENT_FIELDS,
            rows=tuple(rows),
        )

    def fetch_trade_calendar(self, start: date, end: date) -> Iterable[RawBatch]:
        """Yield provider-native calendar rows for one closed date interval."""
        if not self._logged_in:
            raise self._state_error("fetch_trade_calendar")
        batch, _ = self._load_trade_calendar(start, end)
        yield batch

    def fetch_range(self, start: date, end: date) -> Iterable[RawBatch]:
        """Emit every production-supported Raw dataset in dependency order."""
        yield from self.fetch_instruments()
        calendar_batch, open_dates = self._load_trade_calendar(start, end)
        yield calendar_batch
        _, catalog_instruments = self._resolve_instruments(start, end, None)
        yield from self._fetch_all_market_daily_bars(open_dates, catalog_instruments)

    def _resolve_instruments(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None,
    ) -> tuple[str, tuple[InstrumentId, ...]]:
        if instruments is None or len(instruments) == 0:
            if self._catalog is None:
                tuple(self.fetch_instruments())
            assert self._catalog is not None
            candidates = (
                listing.instrument_id
                for listing in self._catalog.list_instruments()
                if listing.provider_type == "1"
                and listing.list_date <= end
                and (listing.delist_date is None or listing.delist_date >= start)
            )
            return "ALL", self._sorted_unique(candidates)
        return "SELECTED", self._sorted_unique(instruments)

    def _load_trade_calendar(
        self, start: date, end: date
    ) -> tuple[RawBatch, tuple[date, ...]]:
        rows = self._read_cursor(
            "query_trade_dates",
            lambda: self._gateway.query_trade_dates(
                start_date=start.isoformat(), end_date=end.isoformat()
            ),
            TRADE_CALENDAR_FIELDS,
        )
        open_dates = tuple(
            date.fromisoformat(str(row["calendar_date"]))
            for row in rows
            if row["is_trading_day"] == "1"
        )
        batch = RawBatch(
            provider=self.provider,
            dataset="trade_calendar",
            request={"start_date": start.isoformat(), "end_date": end.isoformat()},
            retrieved_at=self._clock(),
            schema=TRADE_CALENDAR_FIELDS,
            rows=tuple(rows),
        )
        return batch, open_dates

    @staticmethod
    def _sorted_unique(
        instruments: Iterable[InstrumentId],
    ) -> tuple[InstrumentId, ...]:
        return tuple(sorted(set(instruments), key=InstrumentId.canonical))

    @staticmethod
    def _chunks[T](values: Sequence[T], limit: int) -> Iterable[tuple[T, ...]]:
        for offset in range(0, len(values), limit):
            yield tuple(values[offset : offset + limit])

    def _date_chunks(self, start: date, end: date) -> Iterable[tuple[date, date]]:
        chunk_start = start
        span = timedelta(days=self._config.max_days_per_batch - 1)
        while chunk_start <= end:
            chunk_end = min(chunk_start + span, end)
            yield chunk_start, chunk_end
            chunk_start = chunk_end + timedelta(days=1)

    def _fetch_instrument_rows(
        self,
        instrument_id: InstrumentId,
        *,
        start: date,
        end: date,
    ) -> list[dict[str, JsonValue]]:
        return self._read_cursor(
            "query_history_k_data_plus",
            lambda: self._gateway.query_history_k_data_plus(
                to_baostock_code(instrument_id),
                _DAILY_BAR_FIELD_ARGUMENT,
                start_date=start.isoformat(),
                end_date=end.isoformat(),
                frequency="d",
                adjustflag="3",
            ),
            DAILY_BAR_FIELDS,
        )

    def _fetch_all_market_rows(self, trading_day: date) -> list[dict[str, JsonValue]]:
        operation = "query_daily_history_k_AStock"

        def perform_query() -> list[dict[str, JsonValue]]:
            rows = self._consume_cursor(
                operation,
                self._gateway.query_daily_history_k_AStock(trading_day.isoformat()),
                DAILY_BAR_FIELDS,
            )
            if not rows:
                raise self._empty_open_day_error(trading_day)
            invalid_adjustment = [
                row.get("adjustflag") for row in rows if row.get("adjustflag") != "3"
            ]
            if invalid_adjustment:
                raise self._schema_error(
                    operation,
                    "daily market response must use adjustflag 3",
                    expected="3",
                    actual=invalid_adjustment[0],
                )
            return rows

        return self._retry(operation, perform_query)

    def _read_cursor(
        self,
        operation: str,
        query: Callable[[], BaoStockCursor],
        fields: tuple[str, ...],
    ) -> list[dict[str, JsonValue]]:
        def perform_query() -> list[dict[str, JsonValue]]:
            return self._consume_cursor(operation, query(), fields)

        return self._retry(operation, perform_query)

    def _consume_cursor(
        self,
        operation: str,
        cursor: BaoStockCursor,
        fields: tuple[str, ...],
    ) -> list[dict[str, JsonValue]]:
        self._raise_provider_error(cursor, operation=operation)
        if tuple(cursor.fields) != fields:
            raise self._schema_error(
                operation,
                f"cursor fields do not match the fixed {operation} schema",
                expected=list(fields),
                actual=list(cursor.fields),
            )
        rows: list[dict[str, JsonValue]] = []
        while True:
            has_row = cursor.next()
            self._raise_provider_error(cursor, operation=operation)
            if not has_row:
                return rows
            values = tuple(cursor.get_row_data())
            if len(values) != len(fields):
                raise self._schema_error(
                    operation,
                    f"cursor row length does not match the fixed {operation} schema",
                    expected=len(fields),
                    actual=len(values),
                )
            for field, value in zip(fields, values, strict=True):
                if not isinstance(value, str):
                    raise self._schema_error(
                        operation,
                        f"cursor field {field} must be a provider-native string",
                        expected="str",
                        actual=type(value).__name__,
                    )
            rows.append(dict(zip(fields, values, strict=True)))

    def _retry[T](self, operation: str, function: Callable[[], T]) -> T:
        for attempt in range(self._config.max_attempts):
            try:
                return function()
            except QuantError as error:
                if (
                    not error.detail.retryable
                    or attempt == self._config.max_attempts - 1
                ):
                    raise
            except (TimeoutError, ConnectionError, OSError) as error:
                if attempt == self._config.max_attempts - 1:
                    raise self._transport_error(operation, error) from error
            self._sleep(self._config.retry_backoff_seconds[attempt])
        raise AssertionError("validated retry configuration made loop unreachable")

    def _raise_provider_error(
        self,
        response: BaoStockResponse,
        *,
        operation: str,
    ) -> None:
        if response.error_code == "0":
            return
        retryable = response.error_code in self._config.retryable_error_codes
        raise QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_BAOSTOCK",
                severity=Severity.SEVERE,
                message=f"BaoStock {operation} failed: {response.error_msg}",
                context={
                    "operation": operation,
                    "provider_error_code": response.error_code,
                    "provider_error_message": response.error_msg,
                },
                remediation="retry if configured or inspect the BaoStock provider response",
                retryable=retryable,
            )
        )

    @staticmethod
    def _state_error(operation: str) -> QuantError:
        return QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_STATE",
                severity=Severity.SEVERE,
                message="BaoStock client must be logged in before fetching data",
                context={"operation": operation, "provider": "baostock"},
                remediation="call login() before fetching provider data",
                retryable=False,
            )
        )

    @staticmethod
    def _schema_error(
        operation: str, message: str, *, expected: object, actual: object
    ) -> QuantError:
        return QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_BAOSTOCK_SCHEMA",
                severity=Severity.SEVERE,
                message=message,
                context={
                    "operation": operation,
                    "expected": expected,
                    "actual": actual,
                },
                remediation="inspect the provider schema before accepting raw data",
                retryable=False,
            )
        )

    @staticmethod
    def _empty_open_day_error(trading_day: date) -> QuantError:
        return QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_BAOSTOCK_EMPTY_OPEN_DAY",
                severity=Severity.FATAL,
                message="BaoStock returned no A-share daily bars for an open trading day",
                context={
                    "operation": "query_daily_history_k_AStock",
                    "date": trading_day.isoformat(),
                },
                remediation="retry the date or inspect BaoStock completeness",
                retryable=True,
            )
        )

    @staticmethod
    def _transport_error(operation: str, error: Exception) -> QuantError:
        return QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_BAOSTOCK",
                severity=Severity.SEVERE,
                message=f"BaoStock {operation} transport failed: {error}",
                context={
                    "operation": operation,
                    "transport_error_type": type(error).__name__,
                    "transport_error_message": str(error),
                },
                remediation="retry after checking provider connectivity",
                retryable=True,
            )
        )


class BaoStockCalendarPolicy:
    """Resolve accurate trading windows through provider calendar evidence."""

    def __init__(
        self,
        client: BaoStockClient,
        *,
        clock: Callable[[], datetime] = _utc_now,
        completion_hour: int = 18,
    ) -> None:
        self._client = client
        self._clock = clock
        self._completion_hour = completion_hour
        self._timezone = ZoneInfo("Asia/Shanghai")

    def bootstrap_window(self, years: int) -> tuple[date, date]:
        if years <= 0:
            raise ValueError("years must be positive")
        end = self.latest_complete_day()
        try:
            target = end.replace(year=end.year - years)
        except ValueError:
            target = end.replace(year=end.year - years, day=28)
        candidates = self._open_dates(target, target + timedelta(days=31))
        if not candidates:
            raise ValueError("provider calendar has no bootstrap start day")
        return candidates[0], end

    def latest_complete_day(self) -> date:
        local_now = self._clock().astimezone(self._timezone)
        candidate = local_now.date()
        if local_now.hour < self._completion_hour:
            candidate -= timedelta(days=1)
        candidates = self._open_dates(candidate - timedelta(days=31), candidate)
        if not candidates:
            raise ValueError("provider calendar has no latest complete trading day")
        return candidates[-1]

    def explicit_window(self, start: date, end: date) -> tuple[date, date]:
        if start > end:
            raise ValueError("start must not follow end")
        candidates = self._open_dates(start, end)
        if not candidates:
            raise ValueError("requested range contains no trading day")
        return candidates[0], candidates[-1]

    def update_window(self, watermark: date, overlap_days: int) -> tuple[date, date]:
        if overlap_days < 0:
            raise ValueError("overlap_days must be non-negative")
        end = self.latest_complete_day()
        candidates = self._open_dates(watermark - timedelta(days=45), end)
        at_or_before = [item for item in candidates if item <= watermark]
        if not at_or_before:
            raise ValueError("provider calendar does not cover the snapshot watermark")
        index = max(0, len(at_or_before) - overlap_days - 1)
        return at_or_before[index], end

    def _open_dates(self, start: date, end: date) -> list[date]:
        self._client.login()
        try:
            batches = tuple(self._client.fetch_trade_calendar(start, end))
        finally:
            self._client.close()
        dates: list[date] = []
        for batch in batches:
            for row in batch.rows:
                if row.get("is_trading_day") == "1":
                    value = row.get("calendar_date")
                    if not isinstance(value, str):
                        raise ValueError("calendar_date must be a provider string")
                    dates.append(date.fromisoformat(value))
        return sorted(set(dates))
