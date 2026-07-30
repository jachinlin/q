"""Tests for atomic raw Parquet partition publication."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from quant_core.data.contracts import RawBatch
from quant_core.data.partitions import RawPartitionStore
from quant_core.errors import QuantError


def make_batch(*, close: float = 10.25) -> RawBatch:
    """Build a hand-checked raw daily-bar batch for filesystem integration tests."""
    return RawBatch(
        provider="example",
        dataset="daily_bars",
        request={"end": "2026-07-31", "start": "2026-07-31"},
        retrieved_at=datetime(2026, 7, 31, 9, 30, tzinfo=UTC),
        schema=("symbol", "trade_date", "close"),
        rows=(
            {
                "symbol": "SSE:600000",
                "trade_date": "2026-07-31",
                "close": close,
            },
        ),
    )


def test_publish_uses_canonical_request_json_for_request_hash(tmp_path: Path) -> None:
    """Changing request key insertion order cannot change its partition identity."""
    store = RawPartitionStore(tmp_path)
    batch = make_batch()

    published = store.publish(batch, run_id="run-1")

    expected_json = b'{"end":"2026-07-31","start":"2026-07-31"}'
    assert published.request_hash == hashlib.sha256(expected_json).hexdigest()


def test_same_arrow_content_produces_same_content_hash(tmp_path: Path) -> None:
    """The canonical Arrow IPC stream, rather than a write's temporary name, is hashed."""
    store = RawPartitionStore(tmp_path)

    first = store.publish(make_batch(), run_id="run-1")
    second = store.publish(make_batch(), run_id="run-2")

    assert first.content_hash == second.content_hash
    assert first.schema_fingerprint == second.schema_fingerprint


def test_publish_leaves_no_final_files_when_batch_validation_fails(tmp_path: Path) -> None:
    """A row/schema mismatch never creates a partially visible partition."""
    store = RawPartitionStore(tmp_path)
    malformed = RawBatch(
        provider="example",
        dataset="daily_bars",
        request={"start": "2026-07-31"},
        retrieved_at=datetime(2026, 7, 31, 9, 30, tzinfo=UTC),
        schema=("symbol", "close"),
        rows=({"symbol": "SSE:600000"},),
    )

    with pytest.raises(ValueError, match="schema"):
        store.publish(malformed, run_id="run-1")

    assert list(tmp_path.rglob("*")) == []


def test_publish_creates_only_final_parquet_and_manifest_files(tmp_path: Path) -> None:
    """A visible partition has data plus its final manifest publication marker only."""
    store = RawPartitionStore(tmp_path)

    published = store.publish(make_batch(), run_id="run-1")

    partition_dir = published.data_path.parent
    assert sorted(path.name for path in partition_dir.iterdir()) == [
        f"{published.request_hash}.manifest.json",
        f"{published.request_hash}.parquet",
    ]
    manifest = json.loads(published.manifest_path.read_text(encoding="utf-8"))
    assert manifest == {
        "content_hash": published.content_hash,
        "dataset": "daily_bars",
        "provider": "example",
        "request_hash": published.request_hash,
        "retrieved_at": "2026-07-31T09:30:00+00:00",
        "row_count": 1,
        "schema_fingerprint": published.schema_fingerprint,
    }


def test_republishing_identical_partition_is_idempotent(tmp_path: Path) -> None:
    """A retried run returns the already-published immutable partition."""
    store = RawPartitionStore(tmp_path)
    batch = make_batch()

    first = store.publish(batch, run_id="run-1")
    second = store.publish(batch, run_id="run-1")

    assert second == first


def test_republishing_partition_with_different_content_is_a_structured_conflict(
    tmp_path: Path,
) -> None:
    """A request identity cannot silently overwrite already-published raw evidence."""
    store = RawPartitionStore(tmp_path)
    store.publish(make_batch(close=10.25), run_id="run-1")

    with pytest.raises(QuantError, match="already exists") as error:
        store.publish(make_batch(close=11.25), run_id="run-1")

    assert error.value.detail.code == "raw_partition_conflict"
