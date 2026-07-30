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

from quant_core.data.contracts import JsonValue, RawBatch
from quant_core.domain.enums import Exchange, Severity
from quant_core.domain.identifiers import InstrumentId
from quant_core.errors import ErrorDetail, QuantError

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


class _BaoStockSdk(Protocol):
    """Structural type for the imported external SDK module."""

    def login(self) -> BaoStockResponse:
        """Open an SDK session."""

    def logout(self) -> BaoStockResponse:
        """Close an SDK session."""

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


class BaoStockSdkGateway:
    """Thin wrapper that confines the real SDK import to the provider boundary."""

    def __init__(self, *, sdk: _BaoStockSdk | None = None) -> None:
        self._sdk = sdk or cast(_BaoStockSdk, importlib.import_module("baostock"))

    def login(self) -> BaoStockResponse:
        return self._sdk.login()

    def logout(self) -> BaoStockResponse:
        return self._sdk.logout()

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


@dataclass(frozen=True, slots=True)
class InstrumentListing:
    """One canonical instrument's complete exchange listing interval."""

    instrument_id: InstrumentId
    list_date: date
    delist_date: date | None


class InstrumentCatalog(Protocol):
    """Historical instrument directory independent of current listing state."""

    def list_instruments(self) -> Sequence[InstrumentListing]:
        """Return historical listings, including delisted instruments."""


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
        catalog: InstrumentCatalog,
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
        """Yield stable instrument-block by date-block provider-native batches."""
        if not self._logged_in:
            raise self._state_error("fetch_daily_bars")

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

    def _resolve_instruments(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None,
    ) -> tuple[str, tuple[InstrumentId, ...]]:
        if instruments is not None and len(instruments) == 0:
            self._logger.info(
                "empty instrument selection resolved as full historical market",
                extra={"event": "empty_instruments_resolved_as_all", "scope": "ALL"},
            )
        if instruments is None or len(instruments) == 0:
            candidates = (
                listing.instrument_id
                for listing in self._catalog.list_instruments()
                if listing.list_date <= end
                and (listing.delist_date is None or listing.delist_date >= start)
            )
            return "ALL", self._sorted_unique(candidates)
        return "SELECTED", self._sorted_unique(instruments)

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
        def perform_query() -> list[dict[str, JsonValue]]:
            cursor = self._gateway.query_history_k_data_plus(
                to_baostock_code(instrument_id),
                _DAILY_BAR_FIELD_ARGUMENT,
                start_date=start.isoformat(),
                end_date=end.isoformat(),
                frequency="d",
                adjustflag="3",
            )
            operation = "query_history_k_data_plus"
            self._raise_provider_error(cursor, operation=operation)
            if tuple(cursor.fields) != DAILY_BAR_FIELDS:
                raise self._schema_error(
                    "cursor fields do not match the fixed daily-bar schema",
                    expected=list(DAILY_BAR_FIELDS),
                    actual=list(cursor.fields),
                )
            rows: list[dict[str, JsonValue]] = []
            while True:
                has_row = cursor.next()
                self._raise_provider_error(cursor, operation=operation)
                if not has_row:
                    return rows
                values = tuple(cursor.get_row_data())
                if len(values) != len(DAILY_BAR_FIELDS):
                    raise self._schema_error(
                        "cursor row length does not match the fixed daily-bar schema",
                        expected=len(DAILY_BAR_FIELDS),
                        actual=len(values),
                    )
                rows.append(dict(zip(DAILY_BAR_FIELDS, values, strict=True)))

        return self._retry("query_history_k_data_plus", perform_query)

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
    def _schema_error(message: str, *, expected: object, actual: object) -> QuantError:
        return QuantError(
            ErrorDetail(
                code="DATA_PROVIDER_BAOSTOCK_SCHEMA",
                severity=Severity.SEVERE,
                message=message,
                context={
                    "operation": "query_history_k_data_plus",
                    "expected": expected,
                    "actual": actual,
                },
                remediation="inspect the provider schema before accepting raw data",
                retryable=False,
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
