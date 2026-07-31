"""Offline contract tests for the BaoStock source adapter."""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import ClassVar

import pytest

from quant_core.data.sources.baostock import (
    DAILY_BAR_FIELDS,
    BaoStockCalendarPolicy,
    BaoStockClient,
    BaoStockConfig,
    BaoStockHistoricalCatalog,
    BaoStockSdkGateway,
    InstrumentListing,
    from_baostock_code,
    to_baostock_code,
)
from quant_core.domain.enums import Exchange
from quant_core.domain.identifiers import InstrumentId
from quant_core.errors import QuantError

RAW_FIELDS = (
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
FIXED_TIME = datetime(2026, 7, 31, 9, 30, tzinfo=UTC)


@dataclass(slots=True)
class FakeResponse:
    """SDK-shaped status response."""

    error_code: str = "0"
    error_msg: str = "success"


class FakeCursor(FakeResponse):
    """SDK-shaped cursor that makes crossing fake result pages observable."""

    def __init__(
        self,
        pages: Sequence[Sequence[Sequence[str]]],
        *,
        fields: Sequence[str] = RAW_FIELDS,
        error_code: str = "0",
        error_msg: str = "success",
    ) -> None:
        super().__init__(error_code=error_code, error_msg=error_msg)
        self.fields = tuple(fields)
        self._pages = tuple(tuple(tuple(row) for row in page) for page in pages)
        self._page_index = 0
        self._row_index = 0
        self._current: tuple[str, ...] | None = None
        self.pages_entered = 0

    def next(self) -> bool:
        while self._page_index < len(self._pages):
            page = self._pages[self._page_index]
            if self._row_index == 0:
                self.pages_entered += 1
            if self._row_index < len(page):
                self._current = page[self._row_index]
                self._row_index += 1
                return True
            self._page_index += 1
            self._row_index = 0
        self._current = None
        return False

    def get_row_data(self) -> Sequence[str]:
        assert self._current is not None
        return self._current


type QueryOutcome = FakeCursor | Exception


class FakeGateway:
    """Records SDK boundary calls and returns deterministic offline data."""

    def __init__(self) -> None:
        self.login_calls = 0
        self.logout_calls = 0
        self.query_calls: list[dict[str, str]] = []
        self.daily_market_calls: list[str] = []
        self.login_outcomes: deque[FakeResponse | Exception] = deque()
        self.logout_outcomes: deque[FakeResponse | Exception] = deque()
        self.query_outcomes: dict[tuple[str, str, str], deque[QueryOutcome]] = {}
        self.daily_market_outcomes: dict[str, deque[QueryOutcome]] = {}
        self.stock_basic_cursor = FakeCursor(
            [
                [
                    ("sh.600000", "浦发银行", "1999-11-10", "", "1", "1"),
                    ("sz.000001", "平安银行", "1991-04-03", "", "1", "1"),
                ]
            ],
            fields=("code", "code_name", "ipoDate", "outDate", "type", "status"),
        )
        self.trade_dates_cursor = FakeCursor(
            [[("2026-01-02", "1"), ("2026-01-03", "0")]],
            fields=("calendar_date", "is_trading_day"),
        )

    def login(self) -> FakeResponse:
        self.login_calls += 1
        outcome = (
            self.login_outcomes.popleft() if self.login_outcomes else FakeResponse()
        )
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def logout(self) -> FakeResponse:
        self.logout_calls += 1
        outcome = (
            self.logout_outcomes.popleft() if self.logout_outcomes else FakeResponse()
        )
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> FakeCursor:
        self.query_calls.append(
            {
                "code": code,
                "fields": fields,
                "start_date": start_date,
                "end_date": end_date,
                "frequency": frequency,
                "adjustflag": adjustflag,
            }
        )
        key = (code, start_date, end_date)
        outcomes = self.query_outcomes.get(key)
        if outcomes:
            outcome = outcomes.popleft()
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return FakeCursor([[make_row(start_date, code)]])

    def query_daily_history_k_AStock(self, date: str = "") -> FakeCursor:
        self.daily_market_calls.append(date)
        outcomes = self.daily_market_outcomes.get(date)
        if outcomes:
            outcome = outcomes.popleft()
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return FakeCursor([[make_row(date, "sh.600000")]])

    def query_stock_basic(self, *, code: str, code_name: str) -> FakeCursor:
        assert code == ""
        assert code_name == ""
        return self.stock_basic_cursor

    def query_trade_dates(self, *, start_date: str, end_date: str) -> FakeCursor:
        assert start_date <= end_date
        return FakeCursor(
            self.trade_dates_cursor._pages,
            fields=self.trade_dates_cursor.fields,
            error_code=self.trade_dates_cursor.error_code,
            error_msg=self.trade_dates_cursor.error_msg,
        )

    def queue_query(
        self,
        code: str,
        start_date: str,
        end_date: str,
        *outcomes: QueryOutcome,
    ) -> None:
        self.query_outcomes[(code, start_date, end_date)] = deque(outcomes)


class FakeCatalog:
    """Historical listings supplied independently of the current market state."""

    def __init__(self, listings: Sequence[InstrumentListing] = ()) -> None:
        self._listings = tuple(listings)
        self.calls = 0

    def list_instruments(self) -> Sequence[InstrumentListing]:
        self.calls += 1
        return self._listings


def instrument(exchange: Exchange, symbol: str) -> InstrumentId:
    return InstrumentId(exchange=exchange, symbol=symbol)


def make_row(trade_date: str, code: str, *, close: str = "10.50") -> tuple[str, ...]:
    """Return one complete provider-native row with hand-checked string values."""
    return (
        trade_date,
        code,
        "10.00",
        "10.80",
        "9.90",
        close,
        "9.95",
        "123400",
        "1260000.00",
        "3",
        "0.42",
        "1",
        "5.53",
        "8.10",
        "1.20",
        "2.30",
        "4.50",
        "0",
    )


def config(
    *,
    max_instruments_per_batch: int = 10,
    max_days_per_batch: int = 10,
    max_attempts: int = 1,
    retry_backoff_seconds: tuple[float, ...] = (),
    retryable_error_codes: frozenset[str] = frozenset(),
) -> BaoStockConfig:
    return BaoStockConfig(
        max_instruments_per_batch=max_instruments_per_batch,
        max_days_per_batch=max_days_per_batch,
        max_attempts=max_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
        retryable_error_codes=retryable_error_codes,
    )


def make_client(
    gateway: FakeGateway,
    catalog: FakeCatalog | None = None,
    *,
    source_config: BaoStockConfig | None = None,
    sleep: object | None = None,
    logger: logging.Logger | None = None,
) -> BaoStockClient:
    sleep_function = sleep if callable(sleep) else lambda _: None
    return BaoStockClient(
        gateway,
        catalog or FakeCatalog(),
        source_config or config(),
        clock=lambda: FIXED_TIME,
        sleep=sleep_function,
        logger=logger,
    )


def test_client_emits_provider_native_instrument_and_calendar_raw() -> None:
    """Dropping either SDK query would leave the default pipeline incomplete."""
    gateway = FakeGateway()
    client = BaoStockClient(
        gateway,
        None,
        config(),
        clock=lambda: FIXED_TIME,
        sleep=lambda _: None,
    )
    client.login()

    instruments = tuple(client.fetch_instruments())
    calendar = tuple(client.fetch_trade_calendar(date(2026, 1, 2), date(2026, 1, 3)))

    assert instruments[0].dataset == "instruments"
    assert instruments[0].schema == (
        "code",
        "code_name",
        "ipoDate",
        "outDate",
        "type",
        "status",
    )
    assert instruments[0].rows[0]["code"] == "sh.600000"
    assert calendar[0].dataset == "trade_calendar"
    assert calendar[0].rows == (
        {"calendar_date": "2026-01-02", "is_trading_day": "1"},
        {"calendar_date": "2026-01-03", "is_trading_day": "0"},
    )


def test_historical_catalog_reuses_instrument_raw_without_second_sdk_query() -> None:
    """A second provider query would break reproducible full-market resolution."""
    rows = (
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
            "outDate": "2020-12-31",
            "type": "1",
            "status": "0",
        },
    )

    catalog = BaoStockHistoricalCatalog.from_raw_rows(rows)

    assert catalog.list_instruments() == (
        InstrumentListing(instrument(Exchange.SSE, "600000"), date(1999, 11, 10), None),
        InstrumentListing(
            instrument(Exchange.SZSE, "000001"),
            date(1991, 4, 3),
            date(2020, 12, 31),
        ),
    )


def test_historical_catalog_preserves_provider_type() -> None:
    """Defaulting every raw directory row to stock would include indexes in ALL."""
    catalog = BaoStockHistoricalCatalog.from_raw_rows(
        [
            {
                "code": "sh.000300",
                "code_name": "index",
                "ipoDate": "2005-04-08",
                "outDate": "",
                "type": "2",
                "status": "1",
            }
        ]
    )

    assert catalog.list_instruments()[0].provider_type == "2"


def test_calendar_policy_resolves_complete_bootstrap_explicit_and_overlap_windows() -> (
    None
):
    class CalendarGateway(FakeGateway):
        open_dates: ClassVar[set[date]] = {
            date(2006, 1, 5),
            date(2025, 12, 26),
            date(2025, 12, 29),
            date(2025, 12, 30),
            date(2025, 12, 31),
            date(2026, 1, 2),
            date(2026, 1, 5),
        }

        def query_trade_dates(self, *, start_date: str, end_date: str) -> FakeCursor:
            start = date.fromisoformat(start_date)
            end = date.fromisoformat(end_date)
            rows = [
                (item.isoformat(), "1")
                for item in sorted(self.open_dates)
                if start <= item <= end
            ]
            return FakeCursor([[*rows]], fields=("calendar_date", "is_trading_day"))

    gateway = CalendarGateway()
    client = BaoStockClient(
        gateway, None, config(), clock=lambda: FIXED_TIME, sleep=lambda _: None
    )
    policy = BaoStockCalendarPolicy(
        client, clock=lambda: datetime(2026, 1, 6, 2, tzinfo=UTC)
    )

    assert policy.bootstrap_window(20) == (date(2006, 1, 5), date(2026, 1, 5))
    assert policy.explicit_window(date(2026, 1, 3), date(2026, 1, 5)) == (
        date(2026, 1, 5),
        date(2026, 1, 5),
    )
    assert policy.update_window(date(2026, 1, 5), 5) == (
        date(2025, 12, 26),
        date(2026, 1, 5),
    )


@pytest.mark.parametrize(
    ("canonical", "vendor_code"),
    [
        (instrument(Exchange.SSE, "600000"), "sh.600000"),
        (instrument(Exchange.SZSE, "000001"), "sz.000001"),
    ],
)
def test_vendor_codes_round_trip_only_at_the_baostock_boundary(
    canonical: InstrumentId,
    vendor_code: str,
) -> None:
    assert to_baostock_code(canonical) == vendor_code
    assert from_baostock_code(vendor_code) == canonical


@pytest.mark.parametrize(
    "vendor_code",
    ["SH.600000", "sh:600000", "bj.600000", "sh.60000", "sh.６０００００"],
)
def test_vendor_code_parser_rejects_noncanonical_baostock_codes(
    vendor_code: str,
) -> None:
    with pytest.raises(ValueError, match="BaoStock code"):
        from_baostock_code(vendor_code)


@pytest.mark.parametrize(
    "overrides",
    [
        {"max_instruments_per_batch": 0},
        {"max_days_per_batch": 0},
        {"max_attempts": 0},
        {"max_attempts": 2, "retry_backoff_seconds": ()},
        {"max_attempts": 2, "retry_backoff_seconds": (-0.1,)},
        {"max_attempts": 2, "retry_backoff_seconds": (float("nan"),)},
        {"max_attempts": 2, "retry_backoff_seconds": (float("inf"),)},
        {"max_attempts": 2, "retry_backoff_seconds": (float("-inf"),)},
    ],
)
def test_config_rejects_invalid_explicit_batch_and_retry_limits(
    overrides: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "max_instruments_per_batch": 10,
        "max_days_per_batch": 10,
        "max_attempts": 1,
        "retry_backoff_seconds": (),
        "retryable_error_codes": frozenset(),
    }
    values.update(overrides)

    with pytest.raises(ValueError):
        BaoStockConfig(**values)  # type: ignore[arg-type]


def test_login_and_close_are_explicit_and_idempotent() -> None:
    gateway = FakeGateway()
    client = make_client(gateway)

    client.login()
    client.login()
    client.close()
    client.close()

    assert gateway.login_calls == 1
    assert gateway.logout_calls == 1


def test_fetch_rejects_use_before_login_with_structured_state_error() -> None:
    client = make_client(FakeGateway())

    with pytest.raises(QuantError) as error:
        tuple(
            client.fetch_daily_bars(
                date(2026, 1, 1),
                date(2026, 1, 1),
                [instrument(Exchange.SSE, "600000")],
            )
        )

    assert error.value.detail.retryable is False
    assert error.value.detail.context["operation"] == "fetch_daily_bars"


@pytest.mark.parametrize("selection", [None, []])
def test_all_market_selection_uses_daily_api_only(selection: object) -> None:
    """Routing all-market collection through range queries would be incomplete."""
    gateway = FakeGateway()
    client = make_client(gateway)
    client.login()

    batches = tuple(
        client.fetch_daily_bars(
            date(2026, 1, 2), date(2026, 1, 3), selection  # type: ignore[arg-type]
        )
    )

    assert gateway.daily_market_calls == ["2026-01-02"]
    assert gateway.query_calls == []
    assert [batch.request["api"] for batch in batches] == [
        "query_daily_history_k_AStock"
    ]


def test_selected_instruments_keep_range_api_only() -> None:
    """Routing selected instruments to the market endpoint loses caller selection."""
    gateway = FakeGateway()
    client = make_client(gateway)
    client.login()

    tuple(
        client.fetch_daily_bars(
            date(2026, 1, 2),
            date(2026, 1, 2),
            [instrument(Exchange.SSE, "600000")],
        )
    )

    assert gateway.daily_market_calls == []
    assert [call["code"] for call in gateway.query_calls] == ["sh.600000"]


def test_all_market_batch_records_response_scope_hash() -> None:
    """Dropping all-market response or catalog evidence makes a batch unverifiable."""
    gateway = FakeGateway()
    gateway.daily_market_outcomes["2026-01-02"] = deque(
        [
            FakeCursor(
                [
                    [
                        make_row("2026-01-02", "sz.000001"),
                        make_row("2026-01-02", "sh.600000"),
                    ]
                ]
            )
        ]
    )
    catalog = FakeCatalog(
        [
            InstrumentListing(
                instrument(Exchange.SZSE, "000001"), date(1991, 4, 3), None
            ),
        InstrumentListing(
            instrument(Exchange.SSE, "600000"), date(1999, 11, 10), None
        ),
        InstrumentListing(
            instrument(Exchange.SSE, "000300"), date(2005, 4, 8), None, "2"
        ),
        ]
    )
    client = make_client(gateway, catalog)
    client.login()

    batch = next(client.fetch_daily_bars(date(2026, 1, 2), date(2026, 1, 3), None))

    assert batch.request["scope"] == "ALL"
    assert batch.request["date"] == "2026-01-02"
    assert batch.request["response_instrument_count"] == 2
    assert len(str(batch.request["response_instruments_sha256"])) == 64
    assert batch.request["catalog_instrument_count"] == 2
    assert len(str(batch.request["catalog_instruments_sha256"])) == 64
    assert batch.schema == DAILY_BAR_FIELDS


@pytest.mark.parametrize(
    "cursor",
    [
        FakeCursor([[make_row("2026-01-02", "sh.600000")]], fields=RAW_FIELDS[:-1]),
        FakeCursor([[make_row("2026-01-02", "sh.600000")[:-1]]]),
    ],
)
def test_all_market_schema_drift_is_structured_and_nonretryable(
    cursor: FakeCursor,
) -> None:
    """Accepting changed daily-market shapes corrupts the raw schema contract."""
    gateway = FakeGateway()
    gateway.daily_market_outcomes["2026-01-02"] = deque([cursor])
    client = make_client(gateway)
    client.login()

    with pytest.raises(QuantError) as error:
        tuple(client.fetch_daily_bars(date(2026, 1, 2), date(2026, 1, 2), None))

    assert error.value.detail.code == "DATA_PROVIDER_BAOSTOCK_SCHEMA"
    assert error.value.detail.context["operation"] == "query_daily_history_k_AStock"
    assert error.value.detail.retryable is False


def test_all_market_rejects_non_post_adjusted_rows() -> None:
    """Accepting adjustment modes other than 3 violates the daily raw contract."""
    bad_row = list(make_row("2026-01-02", "sh.600000"))
    bad_row[9] = "2"
    gateway = FakeGateway()
    gateway.daily_market_outcomes["2026-01-02"] = deque([FakeCursor([[bad_row]])])
    client = make_client(gateway)
    client.login()

    with pytest.raises(QuantError) as error:
        tuple(client.fetch_daily_bars(date(2026, 1, 2), date(2026, 1, 2), None))

    assert error.value.detail.code == "DATA_PROVIDER_BAOSTOCK_SCHEMA"
    assert error.value.detail.context["operation"] == "query_daily_history_k_AStock"
    assert error.value.detail.context["expected"] == "3"
    assert error.value.detail.context["actual"] == "2"


def test_all_market_empty_open_day_retries_and_preserves_fatal_error() -> None:
    """Treating an empty open-day response as success silently loses the market."""
    gateway = FakeGateway()
    gateway.daily_market_outcomes["2026-01-02"] = deque([FakeCursor([]), FakeCursor([])])
    sleeps: list[float] = []
    client = make_client(
        gateway,
        source_config=config(max_attempts=2, retry_backoff_seconds=(0.25,)),
        sleep=sleeps.append,
    )
    client.login()

    with pytest.raises(QuantError) as error:
        tuple(client.fetch_daily_bars(date(2026, 1, 2), date(2026, 1, 2), None))

    assert gateway.daily_market_calls == ["2026-01-02", "2026-01-02"]
    assert sleeps == [0.25]
    assert error.value.detail.code == "DATA_PROVIDER_BAOSTOCK_EMPTY_OPEN_DAY"
    assert error.value.detail.severity.name == "FATAL"
    assert error.value.detail.retryable is True


def test_all_market_retries_the_same_date_after_transport_failure() -> None:
    """Retrying a different date after failure would leave the failed day uncovered."""
    gateway = FakeGateway()
    gateway.daily_market_outcomes["2026-01-02"] = deque(
        [TimeoutError("timed out"), FakeCursor([[make_row("2026-01-02", "sh.600000")]])]
    )
    sleeps: list[float] = []
    client = make_client(
        gateway,
        source_config=config(max_attempts=2, retry_backoff_seconds=(0.25,)),
        sleep=sleeps.append,
    )
    client.login()

    batches = tuple(client.fetch_daily_bars(date(2026, 1, 2), date(2026, 1, 2), None))

    assert len(batches) == 1
    assert gateway.daily_market_calls == ["2026-01-02", "2026-01-02"]
    assert sleeps == [0.25]


def test_none_and_empty_selection_resolve_the_same_historical_market_scope(
    caplog: pytest.LogCaptureFixture,
) -> None:
    gateway = FakeGateway()
    catalog = FakeCatalog(
        [
            InstrumentListing(
                instrument(Exchange.SSE, "600000"), date(1999, 11, 10), None
            ),
            InstrumentListing(
                instrument(Exchange.SZSE, "000001"),
                date(1991, 4, 3),
                date(2026, 1, 15),
            ),
            InstrumentListing(
                instrument(Exchange.SZSE, "300001"), date(2026, 2, 1), None
            ),
            InstrumentListing(
                instrument(Exchange.SSE, "600001"),
                date(2000, 1, 1),
                date(2025, 12, 31),
            ),
            InstrumentListing(
                instrument(Exchange.SSE, "600000"), date(1999, 11, 10), None
            ),
        ]
    )
    logger = logging.getLogger("tests.baostock.empty_scope")
    client = make_client(
        gateway,
        catalog,
        source_config=config(max_days_per_batch=31),
        logger=logger,
    )
    client.login()
    caplog.set_level(logging.INFO, logger=logger.name)

    from_none = tuple(
        client.fetch_daily_bars(date(2026, 1, 1), date(2026, 1, 31), None)
    )
    from_empty = tuple(client.fetch_daily_bars(date(2026, 1, 1), date(2026, 1, 31), []))

    assert from_empty == from_none
    assert len(from_none) == 1
    assert from_none[0].request["scope"] == "ALL"
    assert from_none[0].request["catalog_instrument_count"] == 2
    assert from_none[0].request["catalog_instruments_sha256"] == (
        "c16cda1e120ab0ef00a0df55e5ea93ffc7f80fbd6beb179f63e31ed7949397ee"
    )
    assert catalog.calls == 2
    assert any(
        record.levelno == logging.INFO
        and getattr(record, "event", None) == "empty_instruments_resolved_as_all"
        for record in caplog.records
    )


def test_all_market_catalog_evidence_is_stable_per_open_date(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Resolving catalog membership per day would make range evidence unstable."""

    class TwoDayCalendarGateway(FakeGateway):
        def query_trade_dates(self, *, start_date: str, end_date: str) -> FakeCursor:
            return FakeCursor(
                [[("2026-01-02", "1"), ("2026-01-03", "1")]],
                fields=("calendar_date", "is_trading_day"),
            )

    gateway = TwoDayCalendarGateway()
    catalog = FakeCatalog(
        [
            InstrumentListing(
                instrument(Exchange.SSE, "600000"), date(1999, 11, 10), None
            ),
            InstrumentListing(
                instrument(Exchange.SZSE, "000001"), date(1991, 4, 3), None
            ),
            InstrumentListing(
                instrument(Exchange.SSE, "000300"), date(2005, 4, 8), None, "2"
            ),
        ]
    )
    logger = logging.getLogger("tests.baostock.all_market_progress")
    client = make_client(gateway, catalog, logger=logger)
    client.login()
    caplog.set_level(logging.INFO, logger=logger.name)

    batches = tuple(client.fetch_daily_bars(date(2026, 1, 2), date(2026, 1, 3)))

    assert gateway.daily_market_calls == ["2026-01-02", "2026-01-03"]
    assert [batch.request["catalog_instrument_count"] for batch in batches] == [2, 2]
    assert [batch.request["catalog_instruments_sha256"] for batch in batches] == [
        "c16cda1e120ab0ef00a0df55e5ea93ffc7f80fbd6beb179f63e31ed7949397ee",
        "c16cda1e120ab0ef00a0df55e5ea93ffc7f80fbd6beb179f63e31ed7949397ee",
    ]
    progress = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "baostock_all_market_daily_progress"
    ]
    assert [record.date for record in progress] == [
        "2026-01-02",
        "2026-01-03",
    ]
    assert [record.completed_dates for record in progress] == [1, 2]
    assert [record.total_dates for record in progress] == [2, 2]
    assert [record.response_rows for record in progress] == [1, 1]


def test_daily_bars_rejects_reversed_date_range() -> None:
    """Silently yielding no batches for a reversed range hides caller mistakes."""
    client = make_client(FakeGateway())
    client.login()

    with pytest.raises(ValueError, match="start must not follow end"):
        tuple(client.fetch_daily_bars(date(2026, 1, 3), date(2026, 1, 2)))


def test_selected_scope_is_sorted_deduplicated_and_not_catalog_filtered() -> None:
    gateway = FakeGateway()
    catalog = FakeCatalog()
    client = make_client(gateway, catalog)
    client.login()

    batches = tuple(
        client.fetch_daily_bars(
            date(2026, 1, 1),
            date(2026, 1, 1),
            [
                instrument(Exchange.SZSE, "000001"),
                instrument(Exchange.SSE, "600000"),
                instrument(Exchange.SZSE, "000001"),
            ],
        )
    )

    assert batches[0].request["scope"] == "SELECTED"
    assert batches[0].request["instruments"] == ["SSE:600000", "SZSE:000001"]
    assert catalog.calls == 0


def test_instrument_outer_date_inner_chunking_has_stable_batch_metadata() -> None:
    gateway = FakeGateway()
    client = make_client(
        gateway,
        source_config=config(max_instruments_per_batch=2, max_days_per_batch=2),
    )
    client.login()

    batches = tuple(
        client.fetch_daily_bars(
            date(2026, 1, 1),
            date(2026, 1, 3),
            [
                instrument(Exchange.SZSE, "000001"),
                instrument(Exchange.SSE, "600001"),
                instrument(Exchange.SSE, "600000"),
            ],
        )
    )

    assert [
        (
            batch.request["batch_index"],
            batch.request["instrument_chunk_index"],
            batch.request["date_chunk_index"],
            batch.request["start_date"],
            batch.request["end_date"],
            batch.request["instruments"],
        )
        for batch in batches
    ] == [
        (1, 1, 1, "2026-01-01", "2026-01-02", ["SSE:600000", "SSE:600001"]),
        (2, 1, 2, "2026-01-03", "2026-01-03", ["SSE:600000", "SSE:600001"]),
        (3, 2, 1, "2026-01-01", "2026-01-02", ["SZSE:000001"]),
        (4, 2, 2, "2026-01-03", "2026-01-03", ["SZSE:000001"]),
    ]
    assert all(batch.request["batch_count"] == 4 for batch in batches)
    assert [
        (call["code"], call["start_date"], call["end_date"])
        for call in gateway.query_calls
    ] == [
        ("sh.600000", "2026-01-01", "2026-01-02"),
        ("sh.600001", "2026-01-01", "2026-01-02"),
        ("sh.600000", "2026-01-03", "2026-01-03"),
        ("sh.600001", "2026-01-03", "2026-01-03"),
        ("sz.000001", "2026-01-01", "2026-01-02"),
        ("sz.000001", "2026-01-03", "2026-01-03"),
    ]


def test_cursor_pages_are_fully_consumed_and_raw_strings_are_not_normalized() -> None:
    gateway = FakeGateway()
    cursor = FakeCursor(
        [
            [make_row("2026-01-01", "sh.600000", close="10.50")],
            [make_row("2026-01-02", "sh.600000", close="")],
        ]
    )
    gateway.queue_query("sh.600000", "2026-01-01", "2026-01-02", cursor)
    client = make_client(gateway)
    client.login()

    (batch,) = tuple(
        client.fetch_daily_bars(
            date(2026, 1, 1),
            date(2026, 1, 2),
            [instrument(Exchange.SSE, "600000")],
        )
    )

    assert batch.provider == "baostock"
    assert batch.dataset == "daily_bars"
    assert batch.schema == RAW_FIELDS
    assert batch.rows[0]["close"] == "10.50"
    assert batch.rows[1]["close"] == ""
    assert cursor.pages_entered == 2
    assert gateway.query_calls[0]["fields"] == ",".join(RAW_FIELDS)
    assert gateway.query_calls[0]["frequency"] == "d"
    assert gateway.query_calls[0]["adjustflag"] == "3"


@pytest.mark.parametrize(
    "cursor",
    [
        FakeCursor([[make_row("2026-01-01", "sh.600000")]], fields=RAW_FIELDS[:-1]),
        FakeCursor([[make_row("2026-01-01", "sh.600000")[:-1]]]),
    ],
)
def test_cursor_schema_drift_raises_a_structured_nonretryable_error(
    cursor: FakeCursor,
) -> None:
    gateway = FakeGateway()
    gateway.queue_query("sh.600000", "2026-01-01", "2026-01-01", cursor)
    client = make_client(gateway)
    client.login()

    with pytest.raises(QuantError) as error:
        tuple(
            client.fetch_daily_bars(
                date(2026, 1, 1),
                date(2026, 1, 1),
                [instrument(Exchange.SSE, "600000")],
            )
        )

    assert error.value.detail.retryable is False
    assert "schema" in error.value.detail.message.lower()


def test_retryable_provider_and_transport_failures_use_configured_backoff() -> None:
    gateway = FakeGateway()
    gateway.queue_query(
        "sh.600000",
        "2026-01-01",
        "2026-01-01",
        FakeCursor([], error_code="100", error_msg="busy"),
        TimeoutError("timed out"),
        FakeCursor([[make_row("2026-01-01", "sh.600000")]]),
    )
    sleeps: list[float] = []
    client = make_client(
        gateway,
        source_config=config(
            max_attempts=3,
            retry_backoff_seconds=(0.25, 0.5),
            retryable_error_codes=frozenset({"100"}),
        ),
        sleep=sleeps.append,
    )
    client.login()

    batches = tuple(
        client.fetch_daily_bars(
            date(2026, 1, 1),
            date(2026, 1, 1),
            [instrument(Exchange.SSE, "600000")],
        )
    )

    assert len(batches) == 1
    assert len(gateway.query_calls) == 3
    assert sleeps == [0.25, 0.5]


def test_nonretryable_provider_error_is_converted_without_sleep() -> None:
    gateway = FakeGateway()
    gateway.queue_query(
        "sh.600000",
        "2026-01-01",
        "2026-01-01",
        FakeCursor([], error_code="999", error_msg="bad request"),
    )
    sleeps: list[float] = []
    client = make_client(
        gateway,
        source_config=config(
            max_attempts=3,
            retry_backoff_seconds=(0.25, 0.5),
            retryable_error_codes=frozenset({"100"}),
        ),
        sleep=sleeps.append,
    )
    client.login()

    with pytest.raises(QuantError) as error:
        tuple(
            client.fetch_daily_bars(
                date(2026, 1, 1),
                date(2026, 1, 1),
                [instrument(Exchange.SSE, "600000")],
            )
        )

    assert error.value.detail.code == "DATA_PROVIDER_BAOSTOCK"
    assert error.value.detail.context == {
        "operation": "query_history_k_data_plus",
        "provider_error_code": "999",
        "provider_error_message": "bad request",
    }
    assert error.value.detail.retryable is False
    assert len(gateway.query_calls) == 1
    assert sleeps == []


def test_transport_failure_after_max_attempts_is_structured() -> None:
    gateway = FakeGateway()
    gateway.queue_query(
        "sh.600000",
        "2026-01-01",
        "2026-01-01",
        OSError("offline one"),
        ConnectionError("offline two"),
    )
    sleeps: list[float] = []
    client = make_client(
        gateway,
        source_config=config(
            max_attempts=2,
            retry_backoff_seconds=(0.25,),
        ),
        sleep=sleeps.append,
    )
    client.login()

    with pytest.raises(QuantError) as error:
        tuple(
            client.fetch_daily_bars(
                date(2026, 1, 1),
                date(2026, 1, 1),
                [instrument(Exchange.SSE, "600000")],
            )
        )

    assert error.value.detail.code == "DATA_PROVIDER_BAOSTOCK"
    assert error.value.detail.context["operation"] == "query_history_k_data_plus"
    assert error.value.detail.context["transport_error_type"] == "ConnectionError"
    assert error.value.detail.retryable is True
    assert len(gateway.query_calls) == 2
    assert sleeps == [0.25]


class DailyMarketSdk:
    def __init__(self) -> None:
        self.dates: list[str] = []

    def query_daily_history_k_AStock(self, date: str = "") -> FakeCursor:
        self.dates.append(date)
        return FakeCursor([[make_row(date, "sh.600000")]])


def test_sdk_gateway_forwards_daily_market_date_without_extra_parameters() -> None:
    """Adding SDK arguments would violate the provider's market-query contract."""
    sdk = DailyMarketSdk()
    gateway = BaoStockSdkGateway(sdk=sdk)  # type: ignore[arg-type]

    result = gateway.query_daily_history_k_AStock("2026-01-02")

    assert result.fields == DAILY_BAR_FIELDS
    assert sdk.dates == ["2026-01-02"]


def test_sdk_gateway_forwards_only_the_declared_baostock_surface() -> None:
    class FakeSdk:
        def __init__(self) -> None:
            self.query_arguments: tuple[object, ...] | None = None

        def login(self) -> FakeResponse:
            return FakeResponse()

        def logout(self) -> FakeResponse:
            return FakeResponse()

        def query_history_k_data_plus(
            self,
            code: str,
            fields: str,
            *,
            start_date: str,
            end_date: str,
            frequency: str,
            adjustflag: str,
        ) -> FakeCursor:
            self.query_arguments = (
                code,
                fields,
                start_date,
                end_date,
                frequency,
                adjustflag,
            )
            return FakeCursor([])

    sdk = FakeSdk()
    gateway = BaoStockSdkGateway(sdk=sdk)

    assert gateway.login().error_code == "0"
    gateway.query_history_k_data_plus(
        "sh.600000",
        ",".join(RAW_FIELDS),
        start_date="2026-01-01",
        end_date="2026-01-31",
        frequency="d",
        adjustflag="3",
    )
    assert gateway.logout().error_code == "0"
    assert sdk.query_arguments == (
        "sh.600000",
        ",".join(RAW_FIELDS),
        "2026-01-01",
        "2026-01-31",
        "d",
        "3",
    )
