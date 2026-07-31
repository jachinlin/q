"""Offline integration of BaoStock acquisition and immutable Raw publication."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from quant_core.data.partitions import RawPartitionStore
from quant_core.data.sources.baostock import (
    DAILY_BAR_FIELDS,
    INSTRUMENT_FIELDS,
    TRADE_CALENDAR_FIELDS,
    BaoStockClient,
    BaoStockConfig,
    InstrumentListing,
)
from quant_core.domain.enums import Exchange
from quant_core.domain.identifiers import InstrumentId


class OfflineCursor:
    """SDK cursor with deterministic rows, used without network access."""

    error_code = "0"
    error_msg = "success"
    def __init__(
        self, rows: Sequence[Sequence[str]], fields: Sequence[str] = DAILY_BAR_FIELDS
    ) -> None:
        self.fields = fields
        self._rows = tuple(rows)
        self._index = -1

    def next(self) -> bool:
        self._index += 1
        return self._index < len(self._rows)

    def get_row_data(self) -> Sequence[str]:
        return self._rows[self._index]


def daily_row(trade_date: str, code: str) -> tuple[str, ...]:
    return (
        trade_date,
        code,
        "10.00",
        "10.80",
        "9.90",
        "10.50",
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


class OfflineGateway:
    """Deterministic fake of only the external SDK boundary."""

    def __init__(self) -> None:
        self.logged_in = False
        self.trade_calendar_calls = 0
        self.daily_market_calls: list[str] = []
        self.selected_calls: list[str] = []

    def login(self) -> OfflineCursor:
        self.logged_in = True
        return OfflineCursor(())

    def logout(self) -> OfflineCursor:
        self.logged_in = False
        return OfflineCursor(())

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> OfflineCursor:
        assert self.logged_in
        assert fields == ",".join(DAILY_BAR_FIELDS)
        assert end_date >= start_date
        assert frequency == "d"
        assert adjustflag == "3"
        self.selected_calls.append(code)
        return OfflineCursor((daily_row(start_date, code),))

    def query_daily_history_k_AStock(self, date: str = "") -> OfflineCursor:
        assert self.logged_in
        self.daily_market_calls.append(date)
        return OfflineCursor((daily_row(date, "sh.600000"),))

    def query_stock_basic(self, *, code: str, code_name: str) -> OfflineCursor:
        assert self.logged_in
        assert code == ""
        assert code_name == ""
        return OfflineCursor(
            (
                ("sh.600000", "PF Bank", "1999-11-10", "", "1", "1"),
                ("sz.000001", "Ping An Bank", "1991-04-03", "2026-01-02", "1", "1"),
            ),
            INSTRUMENT_FIELDS,
        )

    def query_trade_dates(self, *, start_date: str, end_date: str) -> OfflineCursor:
        assert self.logged_in
        assert start_date <= end_date
        self.trade_calendar_calls += 1
        return OfflineCursor(
            (("2026-01-02", "1"), ("2026-01-03", "0")), TRADE_CALENDAR_FIELDS
        )


class HistoricalCatalog:
    """Historical scope containing an active and a delisted security."""

    def list_instruments(self) -> Sequence[InstrumentListing]:
        return (
            InstrumentListing(
                InstrumentId(Exchange.SSE, "600000"),
                date(1999, 11, 10),
                None,
            ),
            InstrumentListing(
                InstrumentId(Exchange.SZSE, "000001"),
                date(1991, 4, 3),
                date(2026, 1, 2),
            ),
            InstrumentListing(
                InstrumentId(Exchange.SZSE, "300001"),
                date(2026, 2, 1),
                None,
            ),
        )


def make_offline_client(gateway: OfflineGateway) -> BaoStockClient:
    return BaoStockClient(
        gateway,
        HistoricalCatalog(),
        BaoStockConfig(
            max_instruments_per_batch=1,
            max_days_per_batch=2,
            max_attempts=1,
            retry_backoff_seconds=(),
            retryable_error_codes=frozenset(),
        ),
        clock=lambda: datetime(2026, 7, 31, 9, 30, tzinfo=UTC),
    )


def test_fetch_range_reuses_calendar_and_uses_daily_market_api() -> None:
    """fetch_range must share its one calendar query with all-market bars."""
    gateway = OfflineGateway()
    client = make_offline_client(gateway)
    client.login()

    batches = tuple(client.fetch_range(date(2026, 1, 2), date(2026, 1, 3)))

    assert gateway.trade_calendar_calls == 1
    assert gateway.daily_market_calls == ["2026-01-02"]
    assert gateway.selected_calls == []
    assert [batch.dataset for batch in batches] == [
        "instruments",
        "trade_calendar",
        "daily_bars",
    ]


def test_offline_ingest_publishes_each_open_day_once_across_repeated_runs() -> None:
    """Same all-market requests reuse the one exchange-open Raw partition."""
    gateway = OfflineGateway()
    client = make_offline_client(gateway)
    client.login()

    with TemporaryDirectory(prefix="quant-baostock-integration-") as temporary:
        store = RawPartitionStore(Path(temporary))
        first = tuple(
            store.publish(batch, run_id="offline-run")
            for batch in client.fetch_daily_bars(
                date(2026, 1, 1), date(2026, 1, 3), instruments=None
            )
        )
        second = tuple(
            store.publish(batch, run_id="offline-run")
            for batch in client.fetch_daily_bars(
                date(2026, 1, 1), date(2026, 1, 3), instruments=[]
            )
        )

        assert second == first
        assert [partition.request["date"] for partition in first] == ["2026-01-02"]
        assert len(list(Path(temporary).rglob("*.parquet"))) == 1
        assert len(list(Path(temporary).rglob("*.manifest.json"))) == 1

    client.close()
    assert gateway.daily_market_calls == ["2026-01-02", "2026-01-02"]
    assert gateway.selected_calls == []
    assert gateway.logged_in is False


def test_offline_selected_ingest_keeps_the_range_api_and_publishes_raw() -> None:
    """An explicit security selection must retain the legacy range route."""
    gateway = OfflineGateway()
    client = make_offline_client(gateway)
    client.login()

    with TemporaryDirectory(prefix="quant-baostock-selected-integration-") as temporary:
        store = RawPartitionStore(Path(temporary))
        (partition,) = tuple(
            store.publish(batch, run_id="selected-run")
            for batch in client.fetch_daily_bars(
                date(2026, 1, 2),
                date(2026, 1, 2),
                [InstrumentId(Exchange.SSE, "600000")],
            )
        )

        assert partition.dataset == "daily_bars"
        assert partition.request["scope"] == "SELECTED"
        assert partition.row_count == 1

    assert gateway.selected_calls == ["sh.600000"]
    assert gateway.daily_market_calls == []
