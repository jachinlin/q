"""Behavioral coverage for immutable snapshot-bound research reads."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from sqlalchemy import text

from quant_core.data.quality.models import QualityRunSpec
from quant_core.data.repository import (
    SnapshotDatasetMissing,
    SnapshotResearchRepository,
)
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.domain.enums import DatasetKind, SnapshotStatus
from quant_core.domain.identifiers import InstrumentId, SnapshotId
from quant_core.errors import QuantError
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.persistence.repositories import (
    DatasetPartitionRecord,
    DatasetPartitionSpec,
    DatasetVersionSpec,
    MetadataRepository,
)
from tests.fixtures.point_in_time import point_in_time_fixture


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
    early_path = tmp_path / "bars-early.parquet"
    late_path = tmp_path / "bars-late.parquet"
    source.filter(pl.col("trade_date") == date(2024, 4, 28)).write_parquet(early_path)
    source.filter(pl.col("trade_date") == date(2024, 4, 29)).write_parquet(late_path)
    early = DatasetPartitionRecord("d" * 64, early_path, "e" * 64, 1)
    late = DatasetPartitionRecord("f" * 64, late_path, "g" * 64, 1)
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
