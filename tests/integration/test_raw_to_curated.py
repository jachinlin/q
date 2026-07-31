"""Integration coverage for rebuilding canonical data from published raw only."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path

from quant_core.data.partitions import RawPartitionStore
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.data.sources.baostock import (
    DAILY_BAR_FIELDS,
    INSTRUMENT_FIELDS,
    TRADE_CALENDAR_FIELDS,
    BaoStockClient,
    BaoStockConfig,
)
from quant_core.domain.enums import DatasetKind


class _Response:
    error_code = "0"
    error_msg = "success"


class _Cursor(_Response):
    def __init__(
        self,
        rows: Sequence[Sequence[str]],
        fields: Sequence[str] = DAILY_BAR_FIELDS,
    ) -> None:
        self.fields = fields
        self._rows = tuple(rows)
        self._index = -1

    def next(self) -> bool:
        self._index += 1
        return self._index < len(self._rows)

    def get_row_data(self) -> Sequence[str]:
        return self._rows[self._index]


def _daily_row(code: str) -> tuple[str, ...]:
    return (
        "2026-01-02",
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
        return _Cursor((_daily_row(code),))

    def query_daily_history_k_AStock(self, date: str = "") -> _Cursor:
        if self.disabled:
            raise AssertionError(
                "gateway must not be used while rebuilding canonical data"
            )
        assert date == "2026-01-02"
        self.query_calls += 1
        return _Cursor((_daily_row("sh.600000"), _daily_row("sz.000001")))

    def query_stock_basic(self, *, code: str, code_name: str) -> _Cursor:
        assert code == ""
        assert code_name == ""
        return _Cursor(
            (
                ("sh.600000", "PF Bank", "1999-11-10", "", "1", "1"),
                ("sz.000001", "Ping An Bank", "1991-04-03", "", "1", "1"),
            ),
            INSTRUMENT_FIELDS,
        )

    def query_trade_dates(self, *, start_date: str, end_date: str) -> _Cursor:
        assert (start_date, end_date) == ("2026-01-02", "2026-01-02")
        return _Cursor((("2026-01-02", "1"),), TRADE_CALENDAR_FIELDS)


class _Catalog:
    def list_instruments(self) -> tuple[()]:
        return ()


def test_published_all_market_raw_rebuilds_canonical_after_gateway_is_disabled(
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
    raw_batches = tuple(client.fetch_range(date(2026, 1, 2), date(2026, 1, 2)))
    (raw_batch,) = (batch for batch in raw_batches if batch.dataset == "daily_bars")
    partition = RawPartitionStore(tmp_path).publish(raw_batch, run_id="offline")
    assert gateway.query_calls == 1
    gateway.disabled = True

    from quant_core.data.mappers.baostock import BaoStockMapper

    batches = tuple(BaoStockMapper().normalize(partition))

    assert [batch.dataset for batch in batches] == [
        DatasetKind.DAILY_BAR,
        DatasetKind.SECURITY_STATUS,
    ]
    daily, status = batches
    assert daily.frame.select("instrument_id").to_series().to_list() == [
        "SSE:600000",
        "SZSE:000001",
    ]
    assert daily.frame.select("trade_date").unique().item() == date(2026, 1, 2)
    assert status.frame.height == 2
    assert set(daily.frame.columns) == set(
        CANONICAL_SCHEMAS[DatasetKind.DAILY_BAR].columns
    )
    assert gateway.query_calls == 1
