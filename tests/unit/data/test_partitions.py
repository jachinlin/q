"""Tests for atomic raw Parquet partition publication."""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import subprocess
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from queue import Empty
from typing import cast

import pytest

from quant_research.data.contracts import PublishedPartition, RawBatch
from quant_research.data.partitions import RawPartitionStore
from quant_research.domain.errors import QuantError


def _create_directory_link(link: Path, target: Path) -> None:
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        if os.name != "nt":
            raise
        subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                f"New-Item -ItemType Junction -Path '{link}' -Target '{target}' | Out-Null",
            ],
            check=True,
            capture_output=True,
        )


def _hold_partition_lock(
    lock_path: str,
    acquired: multiprocessing.synchronize.Event,
    release: multiprocessing.synchronize.Event,
) -> None:
    """Hold the real cross-process partition lock until the parent releases it."""
    from quant_research.data import partitions

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
    RawPartitionStore(Path(raw_root)).publish(make_batch(close="11.25"))
    result.put("published")
    completed.set()


def _hold_acquisition_guard_until_crash(
    lock_path: str,
    acquired: multiprocessing.synchronize.Event,
    crash: multiprocessing.synchronize.Event,
) -> None:
    """Hold the OS guard, then exit without Python cleanup to model a crash."""
    from quant_research.data import partitions

    guard = partitions._AcquisitionGuard(
        Path(lock_path), deadline=time.monotonic() + 10, poll_seconds=0.01
    )
    guard.__enter__()
    acquired.set()
    assert crash.wait(timeout=10)
    os._exit(0)


def make_batch(
    *,
    close: str = "10.25",
    retrieved_at: datetime = datetime(2026, 7, 31, 9, 30, tzinfo=UTC),
) -> RawBatch:
    """Build a hand-checked raw daily-bar batch for filesystem integration tests."""
    return RawBatch(
        source="example",
        endpoint="daily_bars",
        request={"end": "2026-07-31", "start": "2026-07-31"},
        retrieved_at=retrieved_at,
        schema=("symbol", "trade_date", "close"),
        rows=(
            {
                "symbol": "600000.SH",
                "trade_date": "2026-07-31",
                "close": close,
            },
        ),
    )


def test_publish_rejects_real_directory_link_escape_without_touching_target(
    tmp_path: Path,
) -> None:
    """A provider directory junction must never redirect Raw writes outside root."""
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "victim.txt"
    victim.write_text("untouched", encoding="utf-8")
    _create_directory_link(raw_root / "source=example", outside)

    with pytest.raises((QuantError, ValueError)):
        RawPartitionStore(raw_root).publish(make_batch())

    assert victim.read_text(encoding="utf-8") == "untouched"
    assert sorted(path.name for path in outside.iterdir()) == ["victim.txt"]


def test_publish_uses_canonical_request_json_for_request_hash(tmp_path: Path) -> None:
    """Changing request key insertion order cannot change its partition identity."""
    store = RawPartitionStore(tmp_path)
    batch = make_batch()

    published = store.publish(batch)

    expected_json = b'{"end":"2026-07-31","start":"2026-07-31"}'
    assert published.request_hash == hashlib.sha256(expected_json).hexdigest()


def test_same_arrow_content_produces_same_content_hash(tmp_path: Path) -> None:
    """The canonical Arrow IPC stream, rather than a write's temporary name, is hashed."""
    store = RawPartitionStore(tmp_path)

    first = store.publish(make_batch())
    second = store.publish(make_batch())

    assert first.content_hash == second.content_hash
    assert first.schema_fingerprint == second.schema_fingerprint


def test_publish_leaves_no_final_files_when_batch_validation_fails(
    tmp_path: Path,
) -> None:
    """A row/schema mismatch never creates a partially visible partition."""
    store = RawPartitionStore(tmp_path)
    malformed = RawBatch(
        source="example",
        endpoint="daily_bars",
        request={"start": "2026-07-31"},
        retrieved_at=datetime(2026, 7, 31, 9, 30, tzinfo=UTC),
        schema=("symbol", "close"),
        rows=({"symbol": "600000.SH"},),
    )

    with pytest.raises(ValueError, match="schema"):
        store.publish(malformed)

    assert list(tmp_path.rglob("*.*")) == []


def test_publish_creates_only_final_parquet_and_manifest_files(tmp_path: Path) -> None:
    """A visible partition has data plus the request manifest publication marker."""
    store = RawPartitionStore(tmp_path)

    published = store.publish(make_batch())

    partition_dir = published.data_path.parent
    assert sorted(path.name for path in partition_dir.iterdir()) == [
        f"{published.content_hash}.parquet",
        "manifest.json",
    ]
    manifest = json.loads(published.manifest_path.read_text(encoding="utf-8"))
    assert manifest == {
        "current_content_hash": published.content_hash,
        "endpoint": "daily_bars",
        "files": [
            {
                "content_hash": published.content_hash,
                "ingest_date": "2026-07-31",
                "retrieved_at": "2026-07-31T09:30:00+00:00",
                "row_count": 1,
                "schema_fingerprint": published.schema_fingerprint,
            }
        ],
        "source": "example",
        "request": {"end": "2026-07-31", "start": "2026-07-31"},
        "request_hash": published.request_hash,
    }


def test_republishing_identical_partition_is_idempotent(tmp_path: Path) -> None:
    """A retried run returns the already-published immutable partition."""
    store = RawPartitionStore(tmp_path)
    batch = make_batch()

    first = store.publish(batch)
    second = store.publish(batch)

    assert second == first


def test_republishing_identical_request_reuses_manifest_retrieval_time(
    tmp_path: Path,
) -> None:
    """A retry after the clock advances must reuse the original Raw evidence."""
    store = RawPartitionStore(tmp_path)
    first = store.publish(make_batch())

    second = store.publish(
        make_batch(retrieved_at=datetime(2026, 8, 1, 9, 30, tzinfo=UTC))
    )

    assert second == first
    assert second.retrieved_at == datetime(2026, 7, 31, 9, 30, tzinfo=UTC)
    assert len(list(tmp_path.rglob("*.parquet"))) == 1
    assert len(list(tmp_path.rglob("manifest.json"))) == 1


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("source", "other"),
        ("endpoint", "other"),
        ("request_hash", "0" * 64),
        ("files", []),
        ("request", {}),
    ],
)
def test_republishing_with_later_timestamp_still_rejects_changed_manifest_metadata(
    tmp_path: Path,
    field: str,
    replacement: object,
) -> None:
    """Only the file history may grow when recovering an existing Raw partition."""
    store = RawPartitionStore(tmp_path)
    first = store.publish(make_batch())
    manifest = json.loads(first.manifest_path.read_text(encoding="utf-8"))
    manifest[field] = replacement
    first.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(QuantError) as error:
        store.publish(make_batch(retrieved_at=datetime(2026, 8, 1, 9, 30, tzinfo=UTC)))

    assert error.value.detail.code == "raw_partition_conflict"


def test_changed_content_for_same_request_appends_and_becomes_current(
    tmp_path: Path,
) -> None:
    """A data correction is appended as a new file and the manifest moves on."""
    store = RawPartitionStore(tmp_path)
    original = store.publish(make_batch(close="10.25"))
    corrected = store.publish(
        make_batch(
            close="11.25",
            retrieved_at=datetime(2026, 8, 1, 9, 30, tzinfo=UTC),
        )
    )

    assert corrected.content_hash != original.content_hash
    assert corrected.data_path.parent == original.data_path.parent
    assert len(list(tmp_path.rglob("*.parquet"))) == 2
    assert corrected.retrieved_at == datetime(2026, 8, 1, 9, 30, tzinfo=UTC)
    manifest = json.loads(corrected.manifest_path.read_text(encoding="utf-8"))
    assert manifest["current_content_hash"] == corrected.content_hash
    assert [entry["content_hash"] for entry in manifest["files"]] == [
        original.content_hash,
        corrected.content_hash,
    ]
    # the historical partition remains valid evidence
    RawPartitionStore.verify_partition(original)
    assert (
        store.find_by_request("example", "daily_bars", dict(original.request))
        == corrected
    )


def test_instruments_manifest_history_is_capped_at_twenty(tmp_path: Path) -> None:
    """Stable full requests bound their manifest file history; files are kept."""
    store = RawPartitionStore(tmp_path)
    published: list[object] = []
    for index in range(25):
        published.append(
            store.publish(
                RawBatch(
                    source="example",
                    endpoint="query_stock_basic",
                    request={"scope": "ALL"},
                    retrieved_at=datetime(2026, 7, 31, 9, 30, tzinfo=UTC),
                    schema=("code", "close"),
                    rows=({"code": "sh.600000", "close": f"{index}.00"},),
                )
            )
        )
    manifest = json.loads(
        cast(PublishedPartition, published[-1]).manifest_path.read_text(
            encoding="utf-8"
        )
    )
    assert len(manifest["files"]) == 20
    assert len(list(tmp_path.rglob("*.parquet"))) == 25


def test_find_by_request_returns_none_for_unknown_request(tmp_path: Path) -> None:
    """A request that was never fetched is a cache miss, not an error."""
    store = RawPartitionStore(tmp_path)
    store.publish(make_batch())

    assert (
        store.find_by_request("example", "daily_bars", {"start": "1999-01-01"}) is None
    )


def test_find_by_request_returns_current_partition_and_rejects_corruption(
    tmp_path: Path,
) -> None:
    """A cache hit must be verified content, never a blind pointer read."""
    store = RawPartitionStore(tmp_path)
    published = store.publish(make_batch())
    request = dict(published.request)

    found = store.find_by_request("example", "daily_bars", request)
    assert found == published

    published.data_path.write_bytes(b"not parquet")
    with pytest.raises(QuantError) as error:
        store.find_by_request("example", "daily_bars", request)
    assert error.value.detail.code == "raw_partition_conflict"


def test_verify_partition_rejects_forged_catalog_path(tmp_path: Path) -> None:
    """A partition whose paths do not match its identity cannot pass verification."""
    store = RawPartitionStore(tmp_path)
    published = store.publish(make_batch())
    forged = replace(published, manifest_path=published.data_path.parent / "other.json")

    with pytest.raises(ValueError, match="canonical layout"):
        RawPartitionStore.verify_partition(forged)


def test_publish_normalizes_timestamp_in_partition_and_manifest(tmp_path: Path) -> None:
    """An offset timestamp is represented as UTC in all published metadata."""
    batch = make_batch()
    batch = RawBatch(
        source=batch.source,
        endpoint=batch.endpoint,
        request=batch.request,
        retrieved_at=datetime(2026, 7, 31, 17, 30, tzinfo=timezone(timedelta(hours=8))),
        schema=batch.schema,
        rows=batch.rows,
    )

    published = RawPartitionStore(tmp_path).publish(batch)

    assert published.retrieved_at == datetime(2026, 7, 31, 9, 30, tzinfo=UTC)
    assert json.loads(published.manifest_path.read_text(encoding="utf-8"))["files"][0][
        "retrieved_at"
    ] == ("2026-07-31T09:30:00+00:00")


def test_request_key_insertion_order_is_idempotent(tmp_path: Path) -> None:
    """Equivalent mappings with opposite insertion orders use one immutable partition."""
    original = make_batch()
    reordered = RawBatch(
        source=original.source,
        endpoint=original.endpoint,
        request={"start": "2026-07-31", "end": "2026-07-31"},
        retrieved_at=original.retrieved_at,
        schema=original.schema,
        rows=original.rows,
    )
    store = RawPartitionStore(tmp_path)

    first = store.publish(original)
    second = store.publish(reordered)

    assert second == first


@pytest.mark.parametrize(
    "source,endpoint",
    [
        ("example.", "daily_bars"),
        ("example", "daily/bars"),
    ],
)
def test_publish_rejects_path_tokens_outside_the_partition_grammar(
    tmp_path: Path,
    source: str,
    endpoint: str,
) -> None:
    """Path components cannot escape or change the prescribed raw layout."""
    batch = RawBatch(
        source=source,
        endpoint=endpoint,
        request={"start": "2026-07-31"},
        retrieved_at=datetime(2026, 7, 31, 9, 30, tzinfo=UTC),
        schema=("symbol",),
        rows=({"symbol": "600000.SH"},),
    )

    with pytest.raises(ValueError, match="unsupported|must not"):
        RawPartitionStore(tmp_path).publish(batch)

    assert list(tmp_path.rglob("*.*")) == []


def test_publish_removes_temporary_files_when_write_fails_after_creating_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed temporary Parquet write never leaves files or a publication marker."""
    from quant_research.data import partitions

    write_table = partitions.pq.write_table

    def write_then_fail(*args: object, **kwargs: object) -> None:
        write_table(*args, **kwargs)
        raise OSError("injected write failure")

    monkeypatch.setattr(partitions.pq, "write_table", write_then_fail)

    with pytest.raises(OSError, match="injected write failure"):
        RawPartitionStore(tmp_path).publish(make_batch())

    remaining = list(tmp_path.rglob("*.*"))
    assert len(remaining) == 1
    assert remaining[0].name.endswith(".lock.guard")


def test_manifest_install_failure_retains_only_the_content_addressed_data(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed manifest install keeps no temp files; the data file self-heals."""
    replace = Path.replace

    def fail_manifest_replace(source: Path, target: Path) -> Path:
        if source.name.endswith(".manifest.tmp"):
            raise OSError("injected manifest install failure")
        return replace(source, target)

    monkeypatch.setattr(Path, "replace", fail_manifest_replace)

    with pytest.raises(OSError, match="injected manifest install failure"):
        RawPartitionStore(tmp_path).publish(make_batch())

    table = RawPartitionStore._table_from_batch(make_batch())
    content_hash = RawPartitionStore._content_hash(table)
    remaining = sorted(
        path.name
        for path in tmp_path.rglob("*.*")
        if not path.name.endswith(".lock.guard")
    )
    assert remaining == [f"{content_hash}.parquet"]
    # a retry after the failure installs the manifest over the retained data
    monkeypatch.undo()
    published = RawPartitionStore(tmp_path).publish(make_batch())
    assert published.manifest_path.is_file()
    assert RawPartitionStore.verify_partition(published) is None


def test_republishing_with_invalid_utf8_manifest_is_a_structured_conflict(
    tmp_path: Path,
) -> None:
    """A damaged manifest is reported as a raw conflict rather than leaking decoding."""
    store = RawPartitionStore(tmp_path)
    published = store.publish(make_batch())
    published.manifest_path.write_bytes(b"\xff")

    with pytest.raises(QuantError) as error:
        store.publish(make_batch())

    assert error.value.detail.code == "raw_partition_conflict"


def test_republishing_uses_fast_path_without_revalidating_parquet(
    tmp_path: Path,
) -> None:
    """Localize reuse avoids Parquet validation; Validate owns integrity checks."""
    store = RawPartitionStore(tmp_path)
    published = store.publish(make_batch())
    published.data_path.write_bytes(b"not parquet")

    reused = store.publish(make_batch())

    assert reused == published
    assert reused.data_path.read_bytes() == b"not parquet"


def test_partition_lock_serializes_a_competing_publisher(tmp_path: Path) -> None:
    """A process cannot install a same-partition result while another owns its lock."""
    request_hash = hashlib.sha256(
        b'{"end":"2026-07-31","start":"2026-07-31"}'
    ).hexdigest()
    dataset_dir = tmp_path / "source=example" / "endpoint=daily_bars"
    dataset_dir.mkdir(parents=True)
    lock_path = RawPartitionStore._identity_lock_path(dataset_dir, request_hash)
    context = multiprocessing.get_context("spawn")
    acquired = context.Event()
    release = context.Event()
    started = context.Event()
    completed = context.Event()
    result = context.Queue()
    holder = context.Process(
        target=_hold_partition_lock, args=(str(lock_path), acquired, release)
    )
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


def test_partition_lock_times_out_without_sleeping_when_clock_reaches_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A deadline reached between attempts must not schedule another retry."""
    from quant_research.data import partitions

    lock = partitions._PartitionLock(
        tmp_path / ".partition.lock", timeout_seconds=1.0, poll_seconds=0.25
    )
    clock = iter((100.0, 101.0))
    sleep_calls: list[float] = []
    monkeypatch.setattr(partitions.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(lock, "_install_under_guard", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(partitions.time, "sleep", sleep_calls.append)

    with pytest.raises(TimeoutError, match="timed out waiting for partition lock"):
        lock.__enter__()

    assert sleep_calls == []


def test_partition_lock_rechecks_delay_before_sleep_when_clock_jumps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A post-check clock jump must not pass a negative duration to sleep."""
    from quant_research.data import partitions

    lock = partitions._PartitionLock(
        tmp_path / ".partition.lock", timeout_seconds=1.0, poll_seconds=0.25
    )
    clock = iter((100.0, 100.0, 102.0))
    sleep_calls: list[float] = []

    def record_sleep(duration: float) -> None:
        sleep_calls.append(duration)
        raise AssertionError(f"sleep received {duration}")

    monkeypatch.setattr(partitions.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(lock, "_install_under_guard", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(partitions.time, "sleep", record_sleep)

    with pytest.raises(TimeoutError, match="timed out waiting for partition lock"):
        lock.__enter__()

    assert sleep_calls == []


def test_acquisition_guard_polls_with_a_positive_capped_delay_under_contention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guard contention uses one bounded positive poll without a real wait."""
    from quant_research.data import partitions

    guard = partitions._AcquisitionGuard(
        tmp_path / ".partition.lock", deadline=101.0, poll_seconds=0.25
    )
    clock = iter((100.0, 100.0))
    attempts = iter((False, True))
    sleep_calls: list[float] = []
    monkeypatch.setattr(partitions.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(guard, "_try_acquire_os_guard", lambda: next(attempts))
    monkeypatch.setattr(partitions.time, "sleep", sleep_calls.append)

    with guard:
        assert sleep_calls == [0.25]


def test_partition_guard_is_released_automatically_when_a_process_crashes(
    tmp_path: Path,
) -> None:
    """An abrupt process exit releases the OS guard without recursive recovery."""
    from quant_research.data import partitions

    lock_path = tmp_path / ".partition.lock"
    context = multiprocessing.get_context("spawn")
    acquired = context.Event()
    crash = context.Event()
    holder = context.Process(
        target=_hold_acquisition_guard_until_crash,
        args=(str(lock_path), acquired, crash),
    )
    holder.start()
    assert acquired.wait(timeout=10)

    with (
        pytest.raises(TimeoutError),
        partitions._PartitionLock(lock_path, timeout_seconds=0.05, poll_seconds=0.005),
    ):
        pytest.fail("fixed lock path changed while another process held the guard")

    crash.set()
    holder.join(timeout=10)
    assert holder.exitcode == 0

    with partitions._PartitionLock(lock_path, timeout_seconds=1.0):
        assert (lock_path / "owner.json").is_file()


def test_partition_lock_reclaims_a_dead_owner_lock(tmp_path: Path) -> None:
    """A crashed owner's lock directory is recovered before publication starts."""
    from quant_research.data import partitions

    lock_path = tmp_path / ".partition.lock"
    lock_path.mkdir()
    (lock_path / "owner.json").write_text(
        json.dumps({"pid": 99999999, "token": "0" * 32}), encoding="utf-8"
    )
    os.utime(lock_path, (0, 0))

    with partitions._PartitionLock(lock_path):
        assert (lock_path / "owner.json").exists()

    assert not lock_path.exists()


@pytest.mark.parametrize("owner_bytes", [None, b"{"])
def test_partition_lock_does_not_reclaim_a_fresh_missing_or_damaged_owner(
    tmp_path: Path, owner_bytes: bytes | None
) -> None:
    """A transient owner read failure is not evidence that a live lease is stale."""
    from quant_research.data import partitions

    lock_path = tmp_path / ".partition.lock"
    lock_path.mkdir()
    if owner_bytes is not None:
        (lock_path / "owner.json").write_bytes(owner_bytes)

    with (
        pytest.raises(TimeoutError),
        partitions._PartitionLock(
            lock_path,
            timeout_seconds=0.05,
            poll_seconds=0.005,
            stale_after_seconds=60.0,
        ),
    ):
        pytest.fail("fresh invalid-owner lock was reclaimed")

    assert lock_path.is_dir()


def test_partition_lock_release_cannot_remove_a_later_owners_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cleanup after owner unlink may touch only the releasing owner's tombstone."""
    from quant_research.data import partitions

    lock_path = tmp_path / ".partition.lock"
    holder = partitions._PartitionLock(lock_path, timeout_seconds=1.0)
    holder.__enter__()
    cleanup_started = threading.Event()
    allow_cleanup = threading.Event()
    release_errors: list[Exception] = []
    unlink = Path.unlink

    def block_releasing_owner_cleanup(path: Path, missing_ok: bool = False) -> None:
        unlink(path, missing_ok=missing_ok)
        if threading.current_thread().name == "releasing-owner" and path.name == (
            "owner.json"
        ):
            cleanup_started.set()
            assert allow_cleanup.wait(timeout=5)

    monkeypatch.setattr(Path, "unlink", block_releasing_owner_cleanup)

    def release_holder() -> None:
        try:
            holder.__exit__(None, None, None)
        except (AssertionError, OSError, RuntimeError) as error:
            release_errors.append(error)

    release_thread = threading.Thread(
        target=release_holder, name="releasing-owner", daemon=True
    )
    release_thread.start()
    assert cleanup_started.wait(timeout=5)
    contender = partitions._PartitionLock(lock_path, timeout_seconds=1.0)
    try:
        contender.__enter__()
        assert (lock_path / "owner.json").is_file()
        allow_cleanup.set()
        release_thread.join(timeout=5)
        assert not release_thread.is_alive()
        assert release_errors == []
        assert (lock_path / "owner.json").is_file()
    finally:
        allow_cleanup.set()
        contender.__exit__(None, None, None)
        release_thread.join(timeout=5)


def test_partition_lock_release_waits_past_acquire_timeout_for_guard(
    tmp_path: Path,
) -> None:
    """Release must not abandon a live fixed lock because its guard is busy."""
    from quant_research.data import partitions

    lock_path = tmp_path / ".partition.lock"
    owner = partitions._PartitionLock(
        lock_path, timeout_seconds=0.05, poll_seconds=0.005
    )
    owner.__enter__()
    guard_acquired = threading.Event()
    allow_guard_release = threading.Event()
    release_started = threading.Event()
    release_finished = threading.Event()
    release_errors: list[Exception] = []

    def hold_guard() -> None:
        with partitions._AcquisitionGuard(
            lock_path, deadline=time.monotonic() + 5, poll_seconds=0.005
        ):
            guard_acquired.set()
            assert allow_guard_release.wait(timeout=5)

    def release_owner() -> None:
        release_started.set()
        try:
            owner.__exit__(None, None, None)
        except (AssertionError, OSError, RuntimeError, TimeoutError) as error:
            release_errors.append(error)
        finally:
            release_finished.set()

    guard_thread = threading.Thread(target=hold_guard, daemon=True)
    release_thread = threading.Thread(target=release_owner, daemon=True)
    guard_thread.start()
    assert guard_acquired.wait(timeout=5)
    release_thread.start()
    assert release_started.wait(timeout=5)

    assert not release_finished.wait(timeout=0.15)
    assert (lock_path / "owner.json").is_file()

    allow_guard_release.set()
    guard_thread.join(timeout=5)
    release_thread.join(timeout=5)
    assert not guard_thread.is_alive()
    assert not release_thread.is_alive()
    assert release_errors == []
    assert not lock_path.exists()

    with partitions._PartitionLock(lock_path, timeout_seconds=1.0):
        assert (lock_path / "owner.json").is_file()


def test_partition_lock_release_can_retry_after_identity_check_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-detach release error must retain ownership for an explicit retry."""
    from quant_research.data import partitions

    lock_path = tmp_path / ".partition.lock"
    owner = partitions._PartitionLock(lock_path, timeout_seconds=1.0)
    owner.__enter__()
    path_identity = owner._path_identity
    calls = 0

    def fail_second_identity_check(path: Path) -> tuple[int, int, int]:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected release identity failure")
        return path_identity(path)

    monkeypatch.setattr(owner, "_path_identity", fail_second_identity_check)

    with pytest.raises(OSError, match="injected release identity failure"):
        owner.release()

    assert (lock_path / "owner.json").is_file()
    monkeypatch.setattr(owner, "_path_identity", path_identity)
    owner.release()
    assert not lock_path.exists()

    with partitions._PartitionLock(lock_path, timeout_seconds=1.0):
        assert (lock_path / "owner.json").is_file()


def _assert_stale_reclaim_does_not_displace_later_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owner_bytes: bytes | None,
) -> None:
    """Freeze the stale-observe/detach gap and protect the replacement owner."""
    from quant_research.data import partitions

    lock_path = tmp_path / ".partition.lock"
    lock_path.mkdir()
    if owner_bytes is not None:
        (lock_path / "owner.json").write_bytes(owner_bytes)
    os.utime(lock_path, (0, 0))

    stale_observed = threading.Event()
    allow_first_detach = threading.Event()
    replacement_acquired = threading.Event()
    release_replacement = threading.Event()
    first_errors: list[Exception] = []
    replacement_errors: list[Exception] = []
    rename = Path.rename

    def pause_first_stale_detach(path: Path, target: Path) -> Path:
        target_path = Path(target)
        if (
            threading.current_thread().name == "first-reclaimer"
            and path == lock_path
            and ".stale-" in target_path.name
        ):
            stale_observed.set()
            assert allow_first_detach.wait(timeout=5)
        return rename(path, target)

    monkeypatch.setattr(Path, "rename", pause_first_stale_detach)

    def reclaim_first_observation() -> None:
        try:
            partitions._PartitionLock(
                lock_path, stale_after_seconds=0.0
            )._reclaim_stale_lock("a" * 32)
        except (AssertionError, OSError, RuntimeError) as error:
            first_errors.append(error)

    def replace_old_lock_and_hold_new_owner() -> None:
        try:
            partitions._PartitionLock(
                lock_path, stale_after_seconds=0.0
            )._reclaim_stale_lock("b" * 32)
            with partitions._PartitionLock(lock_path, timeout_seconds=2.0):
                replacement_acquired.set()
                assert release_replacement.wait(timeout=5)
        except (AssertionError, OSError, RuntimeError) as error:
            replacement_errors.append(error)

    first_thread = threading.Thread(
        target=reclaim_first_observation, name="first-reclaimer", daemon=True
    )
    replacement_thread = threading.Thread(
        target=replace_old_lock_and_hold_new_owner,
        name="replacement-owner",
        daemon=True,
    )
    first_thread.start()
    assert stale_observed.wait(timeout=5)
    replacement_thread.start()

    # The vulnerable implementation lets the replacement acquire here. A guarded
    # implementation holds it until the first stale transition has completed.
    replacement_acquired.wait(timeout=0.2)
    allow_first_detach.set()
    assert replacement_acquired.wait(timeout=5)
    first_thread.join(timeout=5)
    assert not first_thread.is_alive()

    try:
        assert first_errors == []
        assert (lock_path / "owner.json").is_file()
        with (
            pytest.raises(TimeoutError),
            partitions._PartitionLock(
                lock_path,
                timeout_seconds=0.05,
                poll_seconds=0.005,
                stale_after_seconds=0.0,
            ),
        ):
            pytest.fail("a third owner acquired while the replacement held the lock")
    finally:
        release_replacement.set()
        replacement_thread.join(timeout=5)

    assert not replacement_thread.is_alive()
    assert replacement_errors == []


@pytest.mark.parametrize("owner_bytes", [None, b"{"])
def test_stale_reclaim_does_not_apply_invalid_owner_observation_to_a_later_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    owner_bytes: bytes | None,
) -> None:
    """An ownerless or malformed stale snapshot cannot detach a later owner."""
    _assert_stale_reclaim_does_not_displace_later_owner(
        tmp_path, monkeypatch, owner_bytes
    )


def test_stale_reclaim_does_not_apply_dead_token_observation_to_a_later_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale dead-token snapshot cannot detach a later live-token owner."""
    owner = json.dumps({"pid": 99999999, "token": "0" * 32}).encode()
    _assert_stale_reclaim_does_not_displace_later_owner(tmp_path, monkeypatch, owner)


def test_publish_recovers_a_stale_ownerless_lock_left_by_interrupted_install(
    tmp_path: Path,
) -> None:
    """An ownerless lock is recoverable only after its lease is demonstrably stale."""
    request_hash = hashlib.sha256(
        b'{"end":"2026-07-31","start":"2026-07-31"}'
    ).hexdigest()
    dataset_dir = tmp_path / "source=example" / "endpoint=daily_bars"
    dataset_dir.mkdir(parents=True)
    lock_path = RawPartitionStore._identity_lock_path(dataset_dir, request_hash)
    lock_path.mkdir()
    os.utime(lock_path, (0, 0))
    published = RawPartitionStore(tmp_path).publish(make_batch())

    assert published.manifest_path.exists()
    assert not lock_path.exists()


def test_partial_owner_write_leaves_no_lock_and_next_publish_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An owner write failure cleans only this acquire attempt's temporary lock state."""
    request_hash = hashlib.sha256(
        b'{"end":"2026-07-31","start":"2026-07-31"}'
    ).hexdigest()
    dataset_dir = tmp_path / "source=example" / "endpoint=daily_bars"
    lock_path = RawPartitionStore._identity_lock_path(dataset_dir, request_hash)
    write_text = Path.write_text

    def write_partially_then_fail(path: Path, text: str, **kwargs: object) -> int:
        if path.name == "owner.json":
            path.write_bytes(b"{")
            raise OSError("injected owner write failure")
        return write_text(path, text, **kwargs)

    monkeypatch.setattr(Path, "write_text", write_partially_then_fail)

    with pytest.raises(OSError, match="injected owner write failure"):
        RawPartitionStore(tmp_path).publish(make_batch())

    assert not lock_path.exists()
    monkeypatch.undo()
    assert RawPartitionStore(tmp_path).publish(make_batch()).manifest_path.exists()
