"""Offline integration of BaoStock acquisition and immutable Raw publication."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from quant_core.data.partitions import RawPartitionStore
from quant_core.data.sources.baostock import (
    DAILY_BAR_FIELDS,
    BaoStockClient,
    BaoStockConfig,
    InstrumentListing,
)
from quant_core.domain.enums import Exchange
from quant_core.domain.identifiers import InstrumentId


class OfflineCursor:
    """One-row SDK cursor used without network access."""

    error_code = "0"
    error_msg = "success"
    fields = DAILY_BAR_FIELDS

    def __init__(self, trade_date: str, code: str) -> None:
        self._row = (
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
        self._available = True

    def next(self) -> bool:
        if not self._available:
            return False
        self._available = False
        return True

    def get_row_data(self) -> Sequence[str]:
        return self._row


class OfflineGateway:
    """Deterministic fake of only the external SDK boundary."""

    def __init__(self) -> None:
        self.logged_in = False
        self.query_count = 0

    def login(self) -> OfflineCursor:
        self.logged_in = True
        return OfflineCursor("", "")

    def logout(self) -> OfflineCursor:
        self.logged_in = False
        return OfflineCursor("", "")

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
        self.query_count += 1
        return OfflineCursor(start_date, code)


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


def test_offline_ingest_publishes_each_batch_once_across_repeated_runs() -> None:
    """Same run and requests reuse four immutable Raw partitions."""
    gateway = OfflineGateway()
    client = BaoStockClient(
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
        assert [partition.request["batch_index"] for partition in first] == [1, 2, 3, 4]
        assert len(list(Path(temporary).rglob("*.parquet"))) == 4
        assert len(list(Path(temporary).rglob("*.manifest.json"))) == 4

    client.close()
    assert gateway.query_count == 8
    assert gateway.logged_in is False
