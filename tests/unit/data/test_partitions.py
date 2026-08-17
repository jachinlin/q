"""Tests for atomic raw Parquet partition publication."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
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

    assert list(tmp_path.rglob("*.*")) == []


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
    remaining = sorted(path.name for path in tmp_path.rglob("*.*"))
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
