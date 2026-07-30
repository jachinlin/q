"""Integration coverage for rebuilding canonical data from published raw only."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from quant_core.data.partitions import RawPartitionStore
from quant_core.data.sources.baostock import (
    DAILY_BAR_FIELDS,
    BaoStockClient,
    BaoStockConfig,
)
from quant_core.domain.enums import DatasetKind, Exchange
from quant_core.domain.identifiers import InstrumentId


class _Response:
    error_code = "0"
    error_msg = "success"


class _Cursor(_Response):
    fields = DAILY_BAR_FIELDS

    def __init__(self) -> None:
        self._pending = True

    def next(self) -> bool:
        pending, self._pending = self._pending, False
        return pending

    def get_row_data(self) -> Sequence[str]:
        return (
            "2026-01-02",
            "sh.600000",
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


class _Gateway:
    def __init__(self) -> None:
        self.disabled = False
        self.query_calls = 0

    def login(self) -> _Response:
        return _Response()

    def logout(self) -> _Response:
        return _Response()

    def query_history_k_data_plus(
        self,
        code: str,
        fields: str,
        *,
        start_date: str,
        end_date: str,
        frequency: str,
        adjustflag: str,
    ) -> _Cursor:
        if self.disabled:
            raise AssertionError(
                "gateway must not be used while rebuilding canonical data"
            )
        self.query_calls += 1
        return _Cursor()


class _Catalog:
    def list_instruments(self) -> tuple[()]:
        return ()


def test_published_raw_rebuilds_canonical_after_gateway_is_disabled(
    tmp_path: Path,
) -> None:
    gateway = _Gateway()
    client = BaoStockClient(
        gateway,
        _Catalog(),
        BaoStockConfig(
            max_instruments_per_batch=1,
            max_days_per_batch=1,
            max_attempts=1,
            retry_backoff_seconds=(),
            retryable_error_codes=frozenset(),
        ),
        clock=lambda: datetime(2026, 1, 2, 10, tzinfo=UTC),
    )
    client.login()
    (raw_batch,) = tuple(
        client.fetch_daily_bars(
            date(2026, 1, 2),
            date(2026, 1, 2),
            [InstrumentId(Exchange.SSE, "600000")],
        )
    )
    partition = RawPartitionStore(tmp_path).publish(raw_batch, run_id="offline")
    assert gateway.query_calls == 1
    gateway.disabled = True

    from quant_core.data.mappers.baostock import BaoStockMapper

    batches = tuple(BaoStockMapper().normalize(partition))

    assert [batch.dataset for batch in batches] == [
        DatasetKind.DAILY_BAR,
        DatasetKind.SECURITY_STATUS,
    ]
    assert batches[0].frame.select("instrument_id", "trade_date", "close").row(0) == (
        "SSE:600000",
        date(2026, 1, 2),
        10.5,
    )
    assert gateway.query_calls == 1
