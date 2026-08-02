"""Behavioral coverage for immutable snapshot-bound research reads."""

from __future__ import annotations

import gc
import hashlib
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import text

from quant_core.data import repository as repository_module
from quant_core.data.quality.models import QualityRunSpec
from quant_core.data.repository import (
    SnapshotDatasetMissing,
    SnapshotResearchRepository,
    verify_published_dataset,
)
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.domain.enums import DatasetKind, SnapshotStatus
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.errors import QuantError
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.persistence.repositories import (
    DatasetPartitionSpec,
    DatasetVersionSpec,
    MetadataRepository,
)
from tests.fixtures.point_in_time import _write_dataset, point_in_time_fixture


def test_financials_do_not_cross_snapshot_membership(tmp_path: Path) -> None:
    """Changing the late financial version must not change an earlier snapshot read."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)

    result = repository.financials_as_of(
        fixture.early_snapshot_id,
        ["revenue"],
        date(2024, 4, 29),
    ).collect()

    assert result["value"].to_list() == [100.0]
    assert result["revision"].to_list() == [0]


def test_financials_choose_latest_available_revision_before_shanghai_close(
    tmp_path: Path,
) -> None:
    """Dropping the availability cutoff would incorrectly expose revision 2 early."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)

    result = repository.financials_as_of(
        fixture.late_snapshot_id,
        ["revenue"],
        date(2024, 4, 29),
    ).collect()

    assert result["value"].to_list() == [125.0]
    assert result["revision"].to_list() == [2]
    assert result.schema == CANONICAL_SCHEMAS[DatasetKind.FINANCIAL_OBSERVATION].columns


def test_financials_exclude_an_unusable_metric_group(tmp_path: Path) -> None:
    """Removing the PIT usability predicate would expose this otherwise isolated row."""
    fixture = point_in_time_fixture(tmp_path)

    result = (
        SnapshotResearchRepository(fixture.repository)
        .financials_as_of(
            fixture.late_snapshot_id,
            ["unusable_metric"],
            date(2024, 4, 29),
        )
        .collect()
    )

    assert result.is_empty()


def test_financials_exclude_a_metric_group_with_unknown_availability(
    tmp_path: Path,
) -> None:
    """Any report-period fallback would expose this metric despite no availability."""
    fixture = point_in_time_fixture(tmp_path)

    result = (
        SnapshotResearchRepository(fixture.repository)
        .financials_as_of(
            fixture.late_snapshot_id,
            ["unknown_availability_metric"],
            date(2024, 4, 29),
        )
        .collect()
    )

    assert result.is_empty()


def test_financials_fail_closed_when_published_partition_is_replaced(
    tmp_path: Path,
) -> None:
    """A replacement file must not inherit the snapshot's published identity."""
    fixture = point_in_time_fixture(tmp_path)
    catalog = fixture.repository
    record = catalog.get_dataset_version(
        catalog.get_snapshot(fixture.early_snapshot_id).dataset_versions[
            "financial_observation"
        ]
    )
    path = record.partitions[0].path
    replacement = path.with_name("financial-replacement.parquet")
    pl.read_parquet(path).with_columns(pl.lit(999.0).alias("value")).write_parquet(
        replacement
    )
    replacement.replace(path)

    with pytest.raises(ValueError, match="catalog integrity"):
        SnapshotResearchRepository(catalog).financials_as_of(
            fixture.early_snapshot_id,
            ["revenue"],
            date(2024, 4, 29),
        )


def test_security_status_fail_closed_when_published_partition_is_deleted(
    tmp_path: Path,
) -> None:
    """A missing status partition must fail before DuckDB observes its catalog path."""
    fixture = point_in_time_fixture(tmp_path)
    catalog = fixture.repository
    record = catalog.get_dataset_version(
        catalog.get_snapshot(fixture.early_snapshot_id).dataset_versions[
            "security_status"
        ]
    )
    record.partitions[0].path.unlink()

    with pytest.raises(ValueError, match="published partition is unavailable"):
        SnapshotResearchRepository(catalog).security_status(
            fixture.early_snapshot_id,
            date(2024, 4, 29),
        )


def test_instruments_fail_closed_when_published_partition_is_corrupt(
    tmp_path: Path,
) -> None:
    """Corrupt bytes must be rejected as a catalog-integrity failure."""
    fixture = point_in_time_fixture(tmp_path)
    instruments = _write_dataset(
        tmp_path,
        "immutable-instruments",
        DatasetKind.INSTRUMENT,
        [_instrument_row("published")],
    )
    snapshot_id = fixture.repository.bind_dataset(
        fixture.early_snapshot_id, DatasetKind.INSTRUMENT, instruments
    )
    instruments.partitions[0].path.write_bytes(b"not parquet")

    with pytest.raises(ValueError, match="catalog integrity"):
        SnapshotResearchRepository(fixture.repository).instruments(snapshot_id)


def test_dataset_verification_does_not_use_whole_table_parquet_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = point_in_time_fixture(tmp_path)

    def reject_whole_table_read(*_args: object, **_kwargs: object) -> pa.Table:
        raise AssertionError("dataset verification used pq.read_table")

    monkeypatch.setattr(repository_module.pq, "read_table", reject_whole_table_read)

    record = verify_published_dataset(
        fixture.repository, fixture.early_snapshot_id, DatasetKind.DAILY_BAR
    )

    assert record.dataset is DatasetKind.DAILY_BAR


def test_dataset_verification_streams_hash_without_arrow_output_buffer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = point_in_time_fixture(tmp_path)

    def reject_output_buffer() -> pa.BufferOutputStream:
        raise AssertionError("dataset verification materialized the IPC stream")

    monkeypatch.setattr(
        repository_module.pa, "BufferOutputStream", reject_output_buffer
    )

    record = verify_published_dataset(
        fixture.repository, fixture.early_snapshot_id, DatasetKind.DAILY_BAR
    )

    assert record.dataset is DatasetKind.DAILY_BAR


def test_dataset_verification_binds_parquet_reader_to_open_descriptor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = point_in_time_fixture(tmp_path)
    original = repository_module.pq.ParquetFile
    sources: list[object] = []

    def observed_parquet_file(
        source: object, *args: object, **kwargs: object
    ) -> object:
        sources.append(source)
        return original(source, *args, **kwargs)

    monkeypatch.setattr(repository_module.pq, "ParquetFile", observed_parquet_file)

    verify_published_dataset(
        fixture.repository, fixture.early_snapshot_id, DatasetKind.DAILY_BAR
    )

    assert sources
    assert all(not isinstance(source, (str, Path)) for source in sources)
    assert all(callable(getattr(source, "fileno", None)) for source in sources)


def test_dataset_verification_checks_partition_size_before_parquet_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = point_in_time_fixture(tmp_path)
    monkeypatch.setattr(
        repository_module, "_MAX_PARTITION_FILE_BYTES", 1, raising=False
    )

    with pytest.raises(ValueError, match="size limit"):
        verify_published_dataset(
            fixture.repository, fixture.early_snapshot_id, DatasetKind.DAILY_BAR
        )


def test_multi_row_group_verification_preserves_legacy_content_hash(
    tmp_path: Path,
) -> None:
    fixture = point_in_time_fixture(tmp_path)
    catalog = fixture.repository
    original = catalog.get_dataset_version(
        catalog.get_snapshot(fixture.early_snapshot_id).dataset_versions["daily_bar"]
    )
    table = pq.read_table(original.partitions[0].path)
    path = tmp_path / "multi-row-group.parquet"
    pq.write_table(table, path, row_group_size=1)
    legacy = pq.read_table(path)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, legacy.schema) as writer:
        writer.write_table(legacy)
    partition = replace(
        original.partitions[0],
        path=path,
        content_hash=hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest(),
        schema_fingerprint=hashlib.sha256(
            legacy.schema.serialize().to_pybytes()
        ).hexdigest(),
        row_count=legacy.num_rows,
    )
    version = replace(
        original,
        id=type(original.id).new(),
        partitions=(partition,),
    )
    snapshot_id = catalog.bind_dataset(
        fixture.early_snapshot_id, DatasetKind.DAILY_BAR, version
    )

    verified = verify_published_dataset(catalog, snapshot_id, DatasetKind.DAILY_BAR)

    assert verified.partitions[0].content_hash == partition.content_hash


def test_trade_calendar_fail_closed_when_published_partition_is_replaced(
    tmp_path: Path,
) -> None:
    """A valid replacement calendar cannot change a published snapshot query."""
    fixture = point_in_time_fixture(tmp_path)
    calendar = _write_dataset(
        tmp_path,
        "immutable-calendar",
        DatasetKind.TRADE_CALENDAR,
        [_calendar_row(is_trading_day=True)],
    )
    snapshot_id = fixture.repository.bind_dataset(
        fixture.early_snapshot_id, DatasetKind.TRADE_CALENDAR, calendar
    )
    path = calendar.partitions[0].path
    replacement = path.with_name("calendar-replacement.parquet")
    pl.read_parquet(path).with_columns(
        pl.lit(False).alias("is_trading_day")
    ).write_parquet(replacement)
    replacement.replace(path)

    with pytest.raises(ValueError, match="catalog integrity"):
        SnapshotResearchRepository(fixture.repository).trade_calendar(
            snapshot_id,
            date(2024, 4, 29),
            date(2024, 4, 29),
        )


def test_read_query_rejects_a_partition_beneath_a_directory_link(
    tmp_path: Path,
) -> None:
    """A symlink or Windows reparse point must not redirect an eager snapshot read."""
    fixture = point_in_time_fixture(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    calendar = _write_dataset(
        outside,
        "linked-calendar",
        DatasetKind.TRADE_CALENDAR,
        [_calendar_row(is_trading_day=True)],
    )
    link = tmp_path / "linked"
    _create_directory_link(link, outside)
    linked_calendar = replace(
        calendar,
        id=type(calendar.id).new(),
        partitions=(
            replace(
                calendar.partitions[0],
                path=link / calendar.partitions[0].path.name,
            ),
        ),
    )
    snapshot_id = fixture.repository.bind_dataset(
        fixture.early_snapshot_id,
        DatasetKind.TRADE_CALENDAR,
        linked_calendar,
    )

    with pytest.raises(ValueError, match="link|reparse"):
        SnapshotResearchRepository(fixture.repository).trade_calendar(
            snapshot_id,
            date(2024, 4, 29),
            date(2024, 4, 29),
        )


def test_bars_are_snapshot_bound_range_reads_with_canonical_sort(
    tmp_path: Path,
) -> None:
    """Removing the ordered range filter would reorder or leak bar observations."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)

    result = repository.bars(
        fixture.early_snapshot_id,
        [InstrumentId.parse("SSE:600000")],
        date(2024, 4, 28),
        date(2024, 4, 29),
    ).collect()

    assert result["trade_date"].to_list() == [date(2024, 4, 28), date(2024, 4, 29)]
    assert result.schema == CANONICAL_SCHEMAS[DatasetKind.DAILY_BAR].columns


def test_bars_remain_a_lazy_parquet_scan_until_the_consumer_collects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Daily market reads must not eagerly stage DuckDB/Arrow before filtering."""
    fixture = point_in_time_fixture(tmp_path)

    def eager_duckdb_is_forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("daily bars must use a lazy Parquet scan")

    monkeypatch.setattr(repository_module.duckdb, "connect", eager_duckdb_is_forbidden)

    result = (
        SnapshotResearchRepository(fixture.repository)
        .bars(
            fixture.early_snapshot_id,
            [InstrumentId.parse("SSE:600000")],
            date(2024, 4, 28),
            date(2024, 4, 29),
        )
        .collect()
    )

    assert result["trade_date"].to_list() == [date(2024, 4, 28), date(2024, 4, 29)]


def test_bars_never_follows_a_partition_replaced_between_lazy_plan_and_collect(
    tmp_path: Path,
) -> None:
    """A lazy plan must not silently follow a replaced published partition."""
    fixture = point_in_time_fixture(tmp_path)
    catalog = fixture.repository
    record = catalog.get_dataset_version(
        catalog.get_snapshot(fixture.early_snapshot_id).dataset_versions["daily_bar"]
    )
    path = record.partitions[0].path
    planned = SnapshotResearchRepository(catalog).bars(
        fixture.early_snapshot_id,
        [InstrumentId.parse("SSE:600000")],
        date(2024, 4, 28),
        date(2024, 4, 29),
    )
    replacement = path.with_name("bars-replacement.parquet")
    pl.read_parquet(path).with_columns(pl.lit(999.0).alias("close")).write_parquet(
        replacement
    )
    replacement.replace(path)

    assert planned.collect()["close"].to_list() == [10.0, 11.0]


def test_bars_plan_keeps_predicates_pushed_to_the_parquet_scan(tmp_path: Path) -> None:
    """Snapshot integrity fencing must not force an eager or unfiltered read."""
    fixture = point_in_time_fixture(tmp_path)

    plan = (
        SnapshotResearchRepository(fixture.repository)
        .bars(
            fixture.early_snapshot_id,
            [InstrumentId.parse("SSE:600000")],
            date(2024, 4, 28),
            date(2024, 4, 28),
        )
        .explain(optimized=True)
    )

    assert "Parquet SCAN" in plan
    assert "SELECTION:" in plan
    assert "2024-04-28" in plan


def test_bars_remains_bound_when_the_source_partition_is_deleted(
    tmp_path: Path,
) -> None:
    """The scan lease owns its input instead of depending on the catalog pathname."""
    fixture = point_in_time_fixture(tmp_path)
    catalog = fixture.repository
    path = (
        catalog.get_dataset_version(
            catalog.get_snapshot(fixture.early_snapshot_id).dataset_versions[
                "daily_bar"
            ]
        )
        .partitions[0]
        .path
    )
    planned = SnapshotResearchRepository(catalog).bars(
        fixture.early_snapshot_id,
        [InstrumentId.parse("SSE:600000")],
        date(2024, 4, 28),
        date(2024, 4, 29),
    )
    path.unlink()

    assert planned.collect()["close"].to_list() == [10.0, 11.0]


def test_bars_ignores_a_source_hard_link_after_owning_scan_bytes(
    tmp_path: Path,
) -> None:
    """A source hard-link writer cannot mutate an already leased scan input."""
    fixture = point_in_time_fixture(tmp_path)
    catalog = fixture.repository
    path = (
        catalog.get_dataset_version(
            catalog.get_snapshot(fixture.early_snapshot_id).dataset_versions[
                "daily_bar"
            ]
        )
        .partitions[0]
        .path
    )
    planned = SnapshotResearchRepository(catalog).bars(
        fixture.early_snapshot_id,
        [InstrumentId.parse("SSE:600000")],
        date(2024, 4, 28),
        date(2024, 4, 29),
    )
    alias = path.with_name("untracked-bars-link.parquet")
    os.link(path, alias)
    replacement = path.with_name("hard-link-replacement.parquet")
    pl.read_parquet(path).with_columns(pl.lit(77.0).alias("close")).write_parquet(
        replacement
    )
    alias.write_bytes(replacement.read_bytes())

    assert planned.collect()["close"].to_list() == [10.0, 11.0]


def test_bars_rejects_source_content_that_differs_from_the_catalog(
    tmp_path: Path,
) -> None:
    """A valid replacement Parquet cannot borrow the published catalog identity."""
    fixture = point_in_time_fixture(tmp_path)
    catalog = fixture.repository
    path = (
        catalog.get_dataset_version(
            catalog.get_snapshot(fixture.early_snapshot_id).dataset_versions[
                "daily_bar"
            ]
        )
        .partitions[0]
        .path
    )
    replacement = path.with_name("catalog-mismatch.parquet")
    pl.read_parquet(path).with_columns(pl.lit(88.0).alias("close")).write_parquet(
        replacement
    )
    replacement.replace(path)

    with pytest.raises(ValueError, match="catalog integrity"):
        SnapshotResearchRepository(catalog).bars(
            fixture.early_snapshot_id,
            [InstrumentId.parse("SSE:600000")],
            date(2024, 4, 28),
            date(2024, 4, 29),
        )


def test_bars_repeated_collect_reuses_the_same_owned_bytes(tmp_path: Path) -> None:
    """Re-executing one lazy plan stays bound to its verified materialization."""
    fixture = point_in_time_fixture(tmp_path)
    planned = SnapshotResearchRepository(fixture.repository).bars(
        fixture.early_snapshot_id,
        [InstrumentId.parse("SSE:600000")],
        date(2024, 4, 28),
        date(2024, 4, 29),
    )

    assert planned.collect()["close"].to_list() == [10.0, 11.0]
    assert planned.collect()["close"].to_list() == [10.0, 11.0]


def test_bars_supports_two_live_plans_for_the_same_partition(tmp_path: Path) -> None:
    """One live lazy plan must not make the same published partition unavailable."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)

    first = repository.bars(
        fixture.early_snapshot_id,
        [InstrumentId.parse("SSE:600000")],
        date(2024, 4, 28),
        date(2024, 4, 29),
    )
    second = repository.bars(
        fixture.early_snapshot_id,
        [InstrumentId.parse("SSE:600000")],
        date(2024, 4, 28),
        date(2024, 4, 29),
    )

    assert first.collect()["close"].to_list() == [10.0, 11.0]
    assert second.collect()["close"].to_list() == [10.0, 11.0]


def test_bars_owns_bytes_across_same_length_in_place_source_write(
    tmp_path: Path,
) -> None:
    """Restored timestamps cannot hide a same-size write through the source path."""
    fixture = point_in_time_fixture(tmp_path)
    catalog = fixture.repository
    path = (
        catalog.get_dataset_version(
            catalog.get_snapshot(fixture.early_snapshot_id).dataset_versions[
                "daily_bar"
            ]
        )
        .partitions[0]
        .path
    )
    planned = SnapshotResearchRepository(catalog).bars(
        fixture.early_snapshot_id,
        [InstrumentId.parse("SSE:600000")],
        date(2024, 4, 28),
        date(2024, 4, 29),
    )
    before = path.stat()
    replacement = path.with_name("same-size-replacement.parquet")
    pl.read_parquet(path).with_columns(pl.lit(99.0).alias("close")).write_parquet(
        replacement
    )
    assert replacement.stat().st_size == before.st_size
    path.write_bytes(replacement.read_bytes())
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))

    assert planned.collect()["close"].to_list() == [10.0, 11.0]


def test_bars_concurrent_plans_share_one_verified_owned_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Concurrent same-content plans perform one bounded copy and both collect."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)
    copyfileobj = shutil.copyfileobj
    copies = 0

    def recording_copy(source: object, target: object, length: int = 0) -> None:
        nonlocal copies
        copies += 1
        copyfileobj(source, target, length)  # type: ignore[arg-type]

    monkeypatch.setattr(repository_module.shutil, "copyfileobj", recording_copy)

    def collect() -> list[float]:
        return (
            repository.bars(
                fixture.early_snapshot_id,
                [InstrumentId.parse("SSE:600000")],
                date(2024, 4, 28),
                date(2024, 4, 29),
            )
            .collect()["close"]
            .to_list()
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: collect(), range(2)))

    assert results == [[10.0, 11.0], [10.0, 11.0]]
    assert copies == 1


def test_bars_owned_copy_lifetime_follows_the_last_lazy_plan(tmp_path: Path) -> None:
    """Dropping the last plan releases its private immutable scan materialization."""
    fixture = point_in_time_fixture(tmp_path)
    source = (
        fixture.repository.get_dataset_version(
            fixture.repository.get_snapshot(fixture.early_snapshot_id).dataset_versions[
                "daily_bar"
            ]
        )
        .partitions[0]
        .path
    )
    repository = SnapshotResearchRepository(fixture.repository)
    planned = repository.bars(
        fixture.early_snapshot_id,
        [InstrumentId.parse("SSE:600000")],
        date(2024, 4, 28),
        date(2024, 4, 29),
    )
    owned_directories = tuple(source.parent.glob(".snapshot-scan-*"))
    assert len(owned_directories) == 1

    del planned
    gc.collect()

    assert not owned_directories[0].exists()


def test_corporate_actions_as_of_excludes_actions_available_after_shanghai_close(
    tmp_path: Path,
) -> None:
    """Removing available_at cutoff would leak an action not known by the research day."""
    fixture = point_in_time_fixture(tmp_path)
    actions = _write_dataset(
        tmp_path,
        "actions-late-availability",
        DatasetKind.CORPORATE_ACTION,
        [
            _corporate_action_row(
                action_type="late",
                ex_date=date(2024, 4, 29),
                available_at=datetime(2024, 4, 29, 16, tzinfo=UTC),
            )
        ],
    )
    snapshot_id = fixture.repository.bind_dataset(
        fixture.early_snapshot_id, DatasetKind.CORPORATE_ACTION, actions
    )

    result = (
        SnapshotResearchRepository(fixture.repository)
        .corporate_actions_as_of(
            snapshot_id, [InstrumentId.parse("SSE:600000")], date(2024, 4, 29)
        )
        .collect()
    )

    assert result.is_empty()
    assert result.schema == CANONICAL_SCHEMAS[DatasetKind.CORPORATE_ACTION].columns


def test_corporate_actions_as_of_excludes_future_ex_date(tmp_path: Path) -> None:
    """A previously announced but not-yet-effective action is not current PIT input."""
    fixture = point_in_time_fixture(tmp_path)
    actions = _write_dataset(
        tmp_path,
        "actions-future-ex-date",
        DatasetKind.CORPORATE_ACTION,
        [
            _corporate_action_row(
                action_type="future",
                ex_date=date(2024, 4, 30),
                available_at=datetime(2024, 4, 28, tzinfo=UTC),
            )
        ],
    )
    snapshot_id = fixture.repository.bind_dataset(
        fixture.early_snapshot_id, DatasetKind.CORPORATE_ACTION, actions
    )

    result = (
        SnapshotResearchRepository(fixture.repository)
        .corporate_actions_as_of(
            snapshot_id, [InstrumentId.parse("SSE:600000")], date(2024, 4, 29)
        )
        .collect()
    )

    assert result.is_empty()


def test_corporate_actions_as_of_excludes_null_ex_date(tmp_path: Path) -> None:
    """A null ex-date must be excluded at the repository boundary, not deferred."""
    fixture = point_in_time_fixture(tmp_path)
    actions = _write_dataset(
        tmp_path,
        "actions-null-ex-date",
        DatasetKind.CORPORATE_ACTION,
        [
            _corporate_action_row(
                action_type="missing-ex-date",
                ex_date=None,
                available_at=datetime(2024, 4, 28, tzinfo=UTC),
            )
        ],
    )
    snapshot_id = fixture.repository.bind_dataset(
        fixture.early_snapshot_id, DatasetKind.CORPORATE_ACTION, actions
    )

    result = (
        SnapshotResearchRepository(fixture.repository)
        .corporate_actions_as_of(
            snapshot_id, [InstrumentId.parse("SSE:600000")], date(2024, 4, 29)
        )
        .collect()
    )

    assert result.is_empty()


def test_security_status_filters_the_requested_as_of_date(tmp_path: Path) -> None:
    """Treating status as unbounded history would return observations from other dates."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)

    result = repository.security_status(
        fixture.early_snapshot_id,
        date(2024, 4, 29),
        [InstrumentId.parse("SSE:600000")],
    ).collect()

    assert result.select("is_listed", "is_suspended").rows() == [(True, False)]
    assert result.schema == CANONICAL_SCHEMAS[DatasetKind.SECURITY_STATUS].columns


def test_missing_snapshot_dataset_has_a_stable_structured_contract(
    tmp_path: Path,
) -> None:
    """A missing dataset must not be mistaken for an empty, mutable latest dataset."""
    fixture = point_in_time_fixture(tmp_path)
    repository = SnapshotResearchRepository(fixture.repository)
    missing_snapshot_id = fixture.repository.snapshot_without_dataset(
        fixture.early_snapshot_id, "daily_bar"
    )

    with pytest.raises(SnapshotDatasetMissing) as captured:
        repository.bars(
            missing_snapshot_id,
            [InstrumentId.parse("SSE:600000")],
            date(2024, 4, 28),
            date(2024, 4, 29),
        )

    assert captured.value.detail.code == "SNAPSHOT_DATASET_MISSING"
    assert captured.value.detail.context == {
        "dataset": "daily_bar",
        "snapshot_id": str(missing_snapshot_id),
    }


def test_research_rejects_draft_snapshot_from_real_metadata_catalog(
    tmp_path: Path,
) -> None:
    """Removing publication-state validation would read a deliberately re-opened snapshot."""
    database_path = tmp_path / "state" / "quant.db"
    upgrade_database(database_path)
    catalog = MetadataRepository(create_sqlite_engine(database_path))
    partition_path = tmp_path / "curated" / "bars.parquet"
    partition_path.parent.mkdir()
    partition_path.write_bytes(b"fixture")
    version = catalog.register_dataset_version(
        DatasetVersionSpec(
            dataset=DatasetKind.DAILY_BAR,
            source="fixture",
            partitions=(
                DatasetPartitionSpec(
                    content_hash=hashlib.sha256(b"fixture").hexdigest(),
                    path=partition_path,
                    schema_fingerprint="a" * 64,
                    row_count=1,
                ),
            ),
            start_date=date(2024, 4, 29),
            end_date=date(2024, 4, 29),
            created_run_id="fixture",
        )
    )
    quality = catalog.register_quality_run(
        QualityRunSpec(
            dataset_versions={DatasetKind.DAILY_BAR.value: version.id},
            started_at=datetime(2024, 4, 29, tzinfo=UTC),
            completed_at=datetime(2024, 4, 29, 1, tzinfo=UTC),
            issues=(),
        )
    )
    snapshot_id = SnapshotId.new()
    manifest_path = tmp_path / "snapshots" / "draft.json"
    manifest_path.parent.mkdir()
    manifest_path.write_text("{}", encoding="utf-8")
    with create_sqlite_engine(database_path).begin() as connection:
        connection.execute(
            text(
                "INSERT INTO snapshot "
                "(id, publication_fingerprint, as_of, status, manifest_path, "
                "manifest_hash, quality_run_id, created_at, published_at) "
                "VALUES (:id, :fingerprint, :as_of, :status, :path, :hash, "
                ":quality_run_id, :created_at, NULL)"
            ),
            {
                "id": str(snapshot_id),
                "fingerprint": "b" * 64,
                "as_of": "2024-04-29T00:00:00+00:00",
                "status": SnapshotStatus.DRAFT.value,
                "path": manifest_path.as_posix(),
                "hash": "c" * 64,
                "quality_run_id": str(quality.id),
                "created_at": "2024-04-29T00:00:00+00:00",
            },
        )
        connection.execute(
            text(
                "INSERT INTO snapshot_dataset "
                "(snapshot_id, dataset, dataset_version_id) "
                "VALUES (:snapshot_id, :dataset, :version_id)"
            ),
            {
                "snapshot_id": str(snapshot_id),
                "dataset": DatasetKind.DAILY_BAR.value,
                "version_id": str(version.id),
            },
        )

    with pytest.raises(QuantError) as captured:
        SnapshotResearchRepository(catalog).bars(
            snapshot_id,
            [InstrumentId.parse("SSE:600000")],
            date(2024, 4, 29),
            date(2024, 4, 29),
        )

    assert captured.value.detail.code == "SNAP_NOT_PUBLISHED"


def _corporate_action_row(
    *, action_type: str, ex_date: date | None, available_at: datetime
) -> dict[str, object]:
    return {
        "instrument_id": "SSE:600000",
        "action_type": action_type,
        "record_date": ex_date,
        "ex_date": ex_date,
        "pay_date": ex_date,
        "cash_per_share": None,
        "share_ratio": None,
        "rights_price": None,
        "source": "fixture",
        "source_version": "v1",
        "available_at": available_at,
        "availability_source": "announcement",
        "pit_usable": True,
        "ingested_at": datetime(2024, 4, 30, tzinfo=UTC),
    }


def _instrument_row(name: str) -> dict[str, object]:
    available_at = datetime(2024, 4, 29, tzinfo=UTC)
    return {
        "instrument_id": "SSE:600000",
        "exchange": "SSE",
        "board": "MAIN",
        "name": name,
        "instrument_type": "STOCK",
        "listing_status": "LISTED",
        "list_date": date(2020, 1, 1),
        "delist_date": None,
        "source": "fixture",
        "source_version": "v1",
        "available_at": available_at,
        "availability_source": "announcement",
        "pit_usable": True,
        "ingested_at": datetime(2024, 4, 30, tzinfo=UTC),
    }


def _calendar_row(*, is_trading_day: bool) -> dict[str, object]:
    available_at = datetime(2024, 4, 29, tzinfo=UTC)
    return {
        "trade_date": date(2024, 4, 29),
        "is_trading_day": is_trading_day,
        "source": "fixture",
        "source_version": "v1",
        "available_at": available_at,
        "availability_source": "announcement",
        "pit_usable": True,
        "ingested_at": datetime(2024, 4, 30, tzinfo=UTC),
    }


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
                (
                    f"New-Item -ItemType Junction -Path '{link}' -Target '{target}' "
                    "| Out-Null"
                ),
            ],
            check=True,
            capture_output=True,
        )


def test_research_sorts_distinct_catalog_partitions_and_rejects_duplicates(
    tmp_path: Path,
) -> None:
    """UNION ALL over duplicated catalog partitions must not duplicate canonical keys."""
    fixture = point_in_time_fixture(tmp_path)
    catalog = fixture.repository
    original = catalog.get_dataset_version(
        catalog.get_snapshot(fixture.early_snapshot_id).dataset_versions["daily_bar"]
    )
    source = pl.read_parquet(original.partitions[0].path)
    early = _write_dataset(
        tmp_path,
        "bars-early",
        DatasetKind.DAILY_BAR,
        source.filter(pl.col("trade_date") == date(2024, 4, 28)).to_dicts(),
    ).partitions[0]
    late = _write_dataset(
        tmp_path,
        "bars-late",
        DatasetKind.DAILY_BAR,
        source.filter(pl.col("trade_date") == date(2024, 4, 29)).to_dicts(),
    ).partitions[0]
    multiple = replace(
        original,
        id=type(original.id).new(),
        partitions=(late, early),
    )
    multiple_snapshot_id = catalog.bind_dataset(
        fixture.early_snapshot_id, DatasetKind.DAILY_BAR, multiple
    )

    result = (
        SnapshotResearchRepository(catalog)
        .bars(
            multiple_snapshot_id,
            [InstrumentId.parse("SSE:600000")],
            date(2024, 4, 28),
            date(2024, 4, 29),
        )
        .collect()
    )

    assert result["trade_date"].to_list() == [date(2024, 4, 28), date(2024, 4, 29)]
    duplicate = replace(
        original,
        id=type(original.id).new(),
        partitions=(original.partitions[0], original.partitions[0]),
    )
    duplicate_snapshot_id = catalog.bind_dataset(
        fixture.early_snapshot_id, DatasetKind.DAILY_BAR, duplicate
    )

    with pytest.raises(QuantError) as captured:
        SnapshotResearchRepository(catalog).bars(
            duplicate_snapshot_id,
            [InstrumentId.parse("SSE:600000")],
            date(2024, 4, 28),
            date(2024, 4, 29),
        )

    assert captured.value.detail.code == "SNAPSHOT_CATALOG_INVALID"


def test_research_rejects_nonpublished_dataset_version(tmp_path: Path) -> None:
    """Ignoring a catalog version's status would bypass the publication gate."""
    fixture = point_in_time_fixture(tmp_path)
    catalog = fixture.repository
    original = catalog.get_dataset_version(
        catalog.get_snapshot(fixture.early_snapshot_id).dataset_versions["daily_bar"]
    )
    snapshot_id = catalog.bind_dataset(
        fixture.early_snapshot_id,
        DatasetKind.DAILY_BAR,
        replace(
            original, id=type(original.id).new(), status=SnapshotStatus.DRAFT.value
        ),
    )

    with pytest.raises(QuantError) as captured:
        SnapshotResearchRepository(catalog).bars(
            snapshot_id,
            [InstrumentId.parse("SSE:600000")],
            date(2024, 4, 28),
            date(2024, 4, 29),
        )

    assert captured.value.detail.code == "SNAPSHOT_CATALOG_INVALID"


def test_research_rejects_published_snapshot_without_published_at(
    tmp_path: Path,
) -> None:
    """A missing publication timestamp cannot be treated as a published snapshot."""
    fixture = point_in_time_fixture(tmp_path)
    snapshot_id = fixture.repository.published_snapshot_without_timestamp(
        fixture.early_snapshot_id
    )

    with pytest.raises(QuantError) as captured:
        SnapshotResearchRepository(fixture.repository).bars(
            snapshot_id,
            [InstrumentId.parse("SSE:600000")],
            date(2024, 4, 28),
            date(2024, 4, 29),
        )

    assert captured.value.detail.code == "SNAP_NOT_PUBLISHED"
