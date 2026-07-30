"""Tests for vendor-neutral source data contracts."""

from datetime import UTC, datetime, timedelta, timezone

import pytest

from quant_core.data.contracts import RawBatch


def test_raw_batch_rejects_naive_retrieval_timestamp() -> None:
    """A batch without an offset cannot enter the reproducible raw boundary."""
    with pytest.raises(ValueError, match="timezone-aware"):
        RawBatch(
            provider="example",
            dataset="daily_bars",
            request={"symbol": "SSE:600000"},
            retrieved_at=datetime(2026, 7, 31, 9, 0, tzinfo=None),  # noqa: DTZ001
            schema=("symbol",),
            rows=({"symbol": "SSE:600000"},),
        )


def test_raw_batch_accepts_timezone_aware_retrieval_timestamp() -> None:
    """An offset-aware retrieval timestamp is retained unchanged."""
    retrieved_at = datetime(2026, 7, 31, 9, 0, tzinfo=UTC)

    batch = RawBatch(
        provider="example",
        dataset="daily_bars",
        request={"symbol": "SSE:600000"},
        retrieved_at=retrieved_at,
        schema=("symbol",),
        rows=({"symbol": "SSE:600000"},),
    )

    assert batch.retrieved_at == retrieved_at


def test_raw_batch_normalizes_aware_timestamp_to_utc() -> None:
    """Equivalent source offsets collapse to one UTC storage timestamp."""
    source_time = datetime(2026, 7, 31, 17, 0, tzinfo=timezone(timedelta(hours=8)))

    batch = RawBatch(
        provider="example",
        dataset="daily_bars",
        request={"symbol": "SSE:600000"},
        retrieved_at=source_time,
        schema=("symbol",),
        rows=({"symbol": "SSE:600000"},),
    )

    assert batch.retrieved_at == datetime(2026, 7, 31, 9, 0, tzinfo=UTC)
    assert batch.retrieved_at.tzinfo is UTC
