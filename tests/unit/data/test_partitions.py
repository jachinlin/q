"""Tests for atomic raw Parquet partition publication."""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from queue import Empty

import pytest

from quant_core.data.contracts import RawBatch
from quant_core.data.partitions import RawPartitionStore
from quant_core.errors import QuantError


def _hold_partition_lock(
    lock_path: str,
    acquired: multiprocessing.synchronize.Event,
    release: multiprocessing.synchronize.Event,
) -> None:
    """Hold the real cross-process partition lock until the parent releases it."""
    from quant_core.data import partitions

    with partitions._PartitionLock(Path(lock_path)):
        acquired.set()
        assert release.wait(timeout=10)


def _publish_after_signal(
    raw_root: str,
    started: multiprocessing.synchronize.Event,
    completed: multiprocessing.synchronize.Event,
    result: multiprocessing.queues.Queue[str],
) -> None:
    """Publish from a distinct process after exposing deterministic progress signals."""
    started.set()
    RawPartitionStore(Path(raw_root)).publish(make_batch(close=11.25), run_id="run-1")
    result.put("published")
    completed.set()


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

    assert list(tmp_path.rglob("*.*")) == []


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


def test_publish_normalizes_timestamp_in_partition_and_manifest(tmp_path: Path) -> None:
    """An offset timestamp is represented as UTC in all published metadata."""
    batch = make_batch()
    batch = RawBatch(
        provider=batch.provider,
        dataset=batch.dataset,
        request=batch.request,
        retrieved_at=datetime(2026, 7, 31, 17, 30, tzinfo=timezone(timedelta(hours=8))),
        schema=batch.schema,
        rows=batch.rows,
    )

    published = RawPartitionStore(tmp_path).publish(batch, run_id="run-1")

    assert published.retrieved_at == datetime(2026, 7, 31, 9, 30, tzinfo=UTC)
    assert json.loads(published.manifest_path.read_text(encoding="utf-8"))["retrieved_at"] == (
        "2026-07-31T09:30:00+00:00"
    )


def test_request_key_insertion_order_is_idempotent(tmp_path: Path) -> None:
    """Equivalent mappings with opposite insertion orders use one immutable partition."""
    original = make_batch()
    reordered = RawBatch(
        provider=original.provider,
        dataset=original.dataset,
        request={"start": "2026-07-31", "end": "2026-07-31"},
        retrieved_at=original.retrieved_at,
        schema=original.schema,
        rows=original.rows,
    )
    store = RawPartitionStore(tmp_path)

    first = store.publish(original, run_id="run-1")
    second = store.publish(reordered, run_id="run-1")

    assert second == first


@pytest.mark.parametrize("provider,dataset,run_id", [
    ("Example", "daily_bars", "run-1"),
    ("example", "daily/bars", "run-1"),
    ("example", "daily_bars", "../run-1"),
])
def test_publish_rejects_path_tokens_outside_the_partition_grammar(
    tmp_path: Path,
    provider: str,
    dataset: str,
    run_id: str,
) -> None:
    """Path components cannot escape or change the prescribed raw layout."""
    batch = RawBatch(
        provider=provider,
        dataset=dataset,
        request={"start": "2026-07-31"},
        retrieved_at=datetime(2026, 7, 31, 9, 30, tzinfo=UTC),
        schema=("symbol",),
        rows=({"symbol": "SSE:600000"},),
    )

    with pytest.raises(ValueError, match="unsupported|must not"):
        RawPartitionStore(tmp_path).publish(batch, run_id=run_id)

    assert list(tmp_path.rglob("*.*")) == []


def test_publish_removes_temporary_files_when_write_fails_after_creating_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed temporary Parquet write never leaves files or a publication marker."""
    from quant_core.data import partitions

    write_table = partitions.pq.write_table

    def write_then_fail(*args: object, **kwargs: object) -> None:
        write_table(*args, **kwargs)
        raise OSError("injected write failure")

    monkeypatch.setattr(partitions.pq, "write_table", write_then_fail)

    with pytest.raises(OSError, match="injected write failure"):
        RawPartitionStore(tmp_path).publish(make_batch(), run_id="run-1")

    assert list(tmp_path.rglob("*.*")) == []


def test_publish_removes_installed_data_when_manifest_install_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed manifest installation cannot expose or retain this call's data file."""
    replace = Path.replace

    def fail_manifest_replace(source: Path, target: Path) -> Path:
        if source.name.endswith(".manifest.tmp"):
            raise OSError("injected manifest install failure")
        return replace(source, target)

    monkeypatch.setattr(Path, "replace", fail_manifest_replace)

    with pytest.raises(OSError, match="injected manifest install failure"):
        RawPartitionStore(tmp_path).publish(make_batch(), run_id="run-1")

    assert list(tmp_path.rglob("*.*")) == []


def test_republishing_with_invalid_utf8_manifest_is_a_structured_conflict(
    tmp_path: Path,
) -> None:
    """A damaged manifest is reported as a raw conflict rather than leaking decoding."""
    store = RawPartitionStore(tmp_path)
    published = store.publish(make_batch(), run_id="run-1")
    published.manifest_path.write_bytes(b"\xff")

    with pytest.raises(QuantError) as error:
        store.publish(make_batch(), run_id="run-1")

    assert error.value.detail.code == "raw_partition_conflict"


def test_republishing_with_damaged_parquet_is_a_structured_conflict(tmp_path: Path) -> None:
    """A manifest cannot make corrupt Parquet appear idempotently published."""
    store = RawPartitionStore(tmp_path)
    published = store.publish(make_batch(), run_id="run-1")
    published.data_path.write_bytes(b"not parquet")

    with pytest.raises(QuantError) as error:
        store.publish(make_batch(), run_id="run-1")

    assert error.value.detail.code == "raw_partition_conflict"


def test_partition_lock_serializes_a_competing_publisher(tmp_path: Path) -> None:
    """A process cannot install a same-partition result while another owns its lock."""
    request_hash = hashlib.sha256(
        b'{"end":"2026-07-31","start":"2026-07-31"}'
    ).hexdigest()
    partition_dir = (
        tmp_path
        / "provider=example"
        / "dataset=daily_bars"
        / "ingest_date=2026-07-31"
        / "run_id=run-1"
    )
    partition_dir.mkdir(parents=True)
    lock_path = partition_dir / f".{request_hash}.lock"
    context = multiprocessing.get_context("spawn")
    acquired = context.Event()
    release = context.Event()
    started = context.Event()
    completed = context.Event()
    result = context.Queue()
    holder = context.Process(target=_hold_partition_lock, args=(str(lock_path), acquired, release))
    publisher = context.Process(
        target=_publish_after_signal,
        args=(str(tmp_path), started, completed, result),
    )
    holder.start()
    assert acquired.wait(timeout=10)
    publisher.start()
    assert started.wait(timeout=10)
    assert not completed.wait(timeout=0.2)

    release.set()
    assert completed.wait(timeout=10)
    holder.join(timeout=10)
    publisher.join(timeout=10)

    assert holder.exitcode == 0
    assert publisher.exitcode == 0
    assert result.get_nowait() == "published"
    with pytest.raises(Empty):
        result.get_nowait()


def test_partition_lock_reclaims_a_dead_owner_lock(tmp_path: Path) -> None:
    """A crashed owner's lock directory is recovered before publication starts."""
    from quant_core.data import partitions

    lock_path = tmp_path / ".partition.lock"
    lock_path.mkdir()
    (lock_path / "owner.json").write_text('{"pid":99999999}', encoding="utf-8")
    os.utime(lock_path, (0, 0))

    with partitions._PartitionLock(lock_path):
        assert (lock_path / "owner.json").exists()

    assert not lock_path.exists()
