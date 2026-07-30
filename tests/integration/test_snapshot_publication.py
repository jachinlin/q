"""Integration coverage for catalog, quality gates, and snapshot publication."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError

from quant_core.data.quality.models import QualityIssue, QualityRunSpec
from quant_core.data.quality.runner import QualityRunner
from quant_core.data.snapshots import SnapshotPublisher
from quant_core.domain.enums import DatasetKind, Severity, SnapshotStatus
from quant_core.domain.identifiers import DatasetVersionId, QualityRunId, SnapshotId
from quant_core.errors import QuantError
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.persistence.repositories import (
    DatasetPartitionSpec,
    DatasetVersionSpec,
    MetadataRepository,
)


@pytest.fixture
def repository(tmp_path: Path) -> MetadataRepository:
    database_path = tmp_path / "state" / "quant.db"
    upgrade_database(database_path)
    return MetadataRepository(create_sqlite_engine(database_path))


def _partition(tmp_path: Path, name: str = "bars") -> DatasetPartitionSpec:
    path = tmp_path / "curated" / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(name.encode("ascii"))
    return DatasetPartitionSpec(
        content_hash=hashlib.sha256(name.encode("ascii")).hexdigest(),
        path=path,
        schema_fingerprint="a" * 64,
        row_count=2,
    )


def _version_spec(
    tmp_path: Path,
    dataset: DatasetKind = DatasetKind.DAILY_BAR,
) -> DatasetVersionSpec:
    return DatasetVersionSpec(
        dataset=dataset,
        source="baostock",
        partitions=(_partition(tmp_path, dataset.value),),
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 5),
        created_run_id="ingest-001",
    )


def _completed_quality_run(
    repository: MetadataRepository,
    dataset_versions: dict[str, DatasetVersionId],
    *,
    issues: tuple[QualityIssue, ...] = (),
) -> QualityRunId:
    record = repository.register_quality_run(
        QualityRunSpec(
            dataset_versions=dataset_versions,
            started_at=datetime(2026, 1, 5, 8, tzinfo=UTC),
            completed_at=datetime(2026, 1, 5, 8, 1, tzinfo=UTC),
            issues=issues,
        )
    )
    return record.id


def test_migration_creates_catalog_tables_and_enables_sqlite_safety(
    repository: MetadataRepository,
) -> None:
    """A migrated control database exposes every table and mandatory pragma."""
    expected_tables = {
        "alembic_version",
        "audit_log",
        "dataset_partition",
        "dataset_version",
        "quality_issue",
        "quality_run",
        "quality_run_dataset",
        "snapshot",
        "snapshot_dataset",
    }

    assert expected_tables.issubset(set(inspect(repository.engine).get_table_names()))
    with repository.engine.connect() as connection:
        assert connection.execute(text("PRAGMA foreign_keys")).scalar_one() == 1
        assert (
            connection.execute(text("PRAGMA journal_mode")).scalar_one().lower()
            == "wal"
        )
        assert connection.execute(text("PRAGMA busy_timeout")).scalar_one() == 5000


def test_dataset_registration_is_idempotent_and_scoped_by_dataset(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    """Re-registering one version returns it once without merging another dataset."""
    daily = _version_spec(tmp_path)
    status = replace(daily, dataset=DatasetKind.SECURITY_STATUS)

    first = repository.register_dataset_version(daily)
    repeated = repository.register_dataset_version(daily)
    other_dataset = repository.register_dataset_version(status)

    assert first == repeated
    assert first.id != other_dataset.id
    assert repository.count_dataset_versions() == 2
    assert first.partitions[0].content_hash == daily.partitions[0].content_hash


def _audit_columns() -> dict[str, pl.Series]:
    instant = datetime(2026, 1, 5, 8, tzinfo=UTC)
    return {
        "source": pl.Series(["fixture", "fixture"]),
        "source_version": pl.Series(["v1", "v1"]),
        "available_at": pl.Series([instant, instant], dtype=pl.Datetime("us", "UTC")),
        "availability_source": pl.Series(["RAW", "RAW"]),
        "pit_usable": pl.Series([True, True]),
        "ingested_at": pl.Series([instant, instant], dtype=pl.Datetime("us", "UTC")),
    }


def _quality_frames() -> dict[DatasetKind, tuple[pl.DataFrame, ...]]:
    audit = _audit_columns()
    bars = pl.DataFrame(
        {
            "instrument_id": ["SSE:600000", "SSE:600000"],
            "trade_date": [date(2026, 1, 5), date(2026, 1, 5)],
            "open": [10.0, 10.0],
            "high": [9.0, 10.5],
            "low": [11.0, 9.5],
            "close": [10.0, 10.2],
            "preclose": [9.8, 9.8],
            "volume": [-1, 10],
            "amount": [None, 102.0],
            "adjustment_flag": ["3", "3"],
            "turnover": [1.0, 1.0],
            "pct_change": [2.0, 2.0],
            "pe_ttm": [8.0, 8.0],
            "pb_mrq": [1.0, 1.0],
            "ps_ttm": [2.0, 2.0],
            "pcf_ncf_ttm": [4.0, 4.0],
            **audit,
        }
    )
    mismatched_bar_partition = bars.head(0).drop("amount")
    calendar = pl.DataFrame(
        {
            "trade_date": [date(2026, 1, 2)],
            "is_trading_day": [True],
            **{name: series.head(1) for name, series in audit.items()},
        }
    )
    instruments = pl.DataFrame(
        {
            "instrument_id": ["SZSE:000001"],
            "exchange": ["SZSE"],
            "board": ["MAIN"],
            "name": ["平安银行"],
            "instrument_type": ["STOCK"],
            "listing_status": ["LISTED"],
            "list_date": [date(1991, 4, 3)],
            "delist_date": [None],
            **{name: series.head(1) for name, series in audit.items()},
        }
    )
    announced = datetime(2026, 4, 30, tzinfo=UTC)
    financials = pl.DataFrame(
        {
            "instrument_id": ["SSE:600000"],
            "report_period": [date(2025, 12, 31)],
            "metric": ["roeAvg"],
            "value": [8.0],
            "revision": [0],
            "announced_at": pl.Series([announced], dtype=pl.Datetime("us", "UTC")),
            "source": ["fixture"],
            "source_version": ["v1"],
            "available_at": pl.Series(
                [datetime(2026, 4, 29, tzinfo=UTC)],
                dtype=pl.Datetime("us", "UTC"),
            ),
            "availability_source": ["PUBLICATION_DATE"],
            "pit_usable": [True],
            "ingested_at": pl.Series([announced], dtype=pl.Datetime("us", "UTC")),
        }
    )
    return {
        DatasetKind.DAILY_BAR: (bars, mismatched_bar_partition),
        DatasetKind.TRADE_CALENDAR: (calendar,),
        DatasetKind.INSTRUMENT: (instruments,),
        DatasetKind.FINANCIAL_OBSERVATION: (financials,),
    }


def test_quality_runner_reports_every_required_foundation_rule() -> None:
    """Malformed canonical inputs surface all eight required quality rule IDs."""
    issues = QualityRunner().evaluate(_quality_frames())

    assert {issue.rule_id for issue in issues} == {
        "primary_key_duplicate",
        "required_value_null",
        "ohlc_relationship",
        "negative_volume",
        "trading_day_coverage",
        "instrument_coverage",
        "financial_availability",
        "cross_partition_schema",
    }


def test_null_rule_covers_canonical_lineage_columns() -> None:
    """Missing lineage metadata is a required-value issue, not valid canonical data."""
    (instruments,) = _quality_frames()[DatasetKind.INSTRUMENT]
    instruments = instruments.with_columns(pl.lit(None).cast(pl.String).alias("source"))

    issues = QualityRunner().evaluate({DatasetKind.INSTRUMENT: (instruments,)})

    assert [issue.rule_id for issue in issues] == ["required_value_null"]


@pytest.mark.parametrize("severity", [Severity.SEVERE, Severity.FATAL])
def test_blocking_quality_issue_prevents_snapshot_without_half_product(
    repository: MetadataRepository,
    tmp_path: Path,
    severity: Severity,
) -> None:
    """Severe and fatal issues fail closed before any DB row or manifest appears."""
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    issue = QualityIssue(
        rule_id="primary_key_duplicate",
        severity=severity,
        dataset=DatasetKind.DAILY_BAR,
        scope={"partition": "2026"},
        actual=2,
        threshold=0,
        message="duplicate primary key",
        remediation="deduplicate curated input",
    )
    quality_run_id = _completed_quality_run(repository, versions, issues=(issue,))
    snapshot_root = tmp_path / "snapshots"

    with pytest.raises(QuantError) as raised:
        SnapshotPublisher(repository, snapshot_root).publish(versions, quality_run_id)

    assert raised.value.detail.code == "SNAP_QUALITY_BLOCKED"
    assert repository.count_snapshots() == 0
    assert list(snapshot_root.rglob("*")) == []


def test_snapshot_requires_completed_quality_run_for_exact_version_set(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    """A run that is incomplete or checked another version set cannot authorize publish."""
    daily = repository.register_dataset_version(_version_spec(tmp_path))
    calendar = repository.register_dataset_version(
        _version_spec(tmp_path, DatasetKind.TRADE_CALENDAR)
    )
    checked = {DatasetKind.DAILY_BAR.value: daily.id}
    incomplete = repository.register_quality_run(
        QualityRunSpec(
            dataset_versions=checked,
            started_at=datetime(2026, 1, 5, 8, tzinfo=UTC),
            completed_at=None,
            issues=(),
        )
    )
    publisher = SnapshotPublisher(repository, tmp_path / "snapshots")

    with pytest.raises(QuantError) as incomplete_error:
        publisher.publish(checked, incomplete.id)
    assert incomplete_error.value.detail.code == "SNAP_QUALITY_INCOMPLETE"

    completed_id = _completed_quality_run(repository, checked)
    with pytest.raises(QuantError) as mismatch_error:
        publisher.publish(
            {
                DatasetKind.DAILY_BAR.value: daily.id,
                DatasetKind.TRADE_CALENDAR.value: calendar.id,
            },
            completed_id,
        )
    assert mismatch_error.value.detail.code == "SNAP_QUALITY_SCOPE_MISMATCH"
    assert repository.count_snapshots() == 0


def test_successful_publish_is_atomic_complete_idempotent_and_immutable(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    """A successful snapshot atomically exposes full references once and stays immutable."""
    daily = repository.register_dataset_version(_version_spec(tmp_path))
    calendar = repository.register_dataset_version(
        _version_spec(tmp_path, DatasetKind.TRADE_CALENDAR)
    )
    versions = {
        DatasetKind.TRADE_CALENDAR.value: calendar.id,
        DatasetKind.DAILY_BAR.value: daily.id,
    }
    quality_run_id = _completed_quality_run(repository, versions)
    now = datetime(2026, 1, 5, 9, tzinfo=UTC)
    publisher = SnapshotPublisher(
        repository,
        tmp_path / "snapshots",
        clock=lambda: now,
    )

    snapshot_id = publisher.publish(versions, quality_run_id)
    repeated_id = publisher.publish(
        dict(reversed(tuple(versions.items()))), quality_run_id
    )
    snapshot = repository.get_snapshot(snapshot_id)
    manifest_bytes = snapshot.manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)

    assert repeated_id == snapshot_id
    assert repository.count_snapshots() == 1
    assert snapshot.status is SnapshotStatus.PUBLISHED
    assert snapshot.manifest_hash == hashlib.sha256(manifest_bytes).hexdigest()
    assert manifest == {
        "as_of": "2026-01-05T09:00:00+00:00",
        "created_at": "2026-01-05T09:00:00+00:00",
        "datasets": {
            "daily_bar": {
                "dataset_version_id": str(daily.id),
                "partitions": [
                    {
                        "content_hash": daily.partitions[0].content_hash,
                        "path": daily.partitions[0].path.resolve().as_posix(),
                        "row_count": 2,
                        "schema_fingerprint": "a" * 64,
                    }
                ],
            },
            "trade_calendar": {
                "dataset_version_id": str(calendar.id),
                "partitions": [
                    {
                        "content_hash": calendar.partitions[0].content_hash,
                        "path": calendar.partitions[0].path.resolve().as_posix(),
                        "row_count": 2,
                        "schema_fingerprint": "a" * 64,
                    }
                ],
            },
        },
        "format_version": 1,
        "quality_run_id": str(quality_run_id),
        "snapshot_id": str(snapshot_id),
        "status": "PUBLISHED",
    }
    with repository.engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(
            text("UPDATE snapshot SET status = 'DRAFT' WHERE id = :id"),
            {"id": str(snapshot_id)},
        )


def test_commit_failure_after_manifest_replace_leaves_recoverable_orphan(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    """A post-replace DB failure rolls back SQLite and recovery removes the orphan file."""
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)

    def fail_after_replace() -> None:
        raise RuntimeError("injected commit-window failure")

    publisher = SnapshotPublisher(
        repository,
        tmp_path / "snapshots",
        after_manifest_replace=fail_after_replace,
    )
    with pytest.raises(RuntimeError, match="commit-window"):
        publisher.publish(versions, quality_run_id)

    assert repository.count_snapshots() == 0
    assert len(list((tmp_path / "snapshots").rglob("manifest.json"))) == 1

    publisher.recover()

    assert list((tmp_path / "snapshots").rglob("*.tmp")) == []
    assert list((tmp_path / "snapshots").rglob("manifest.json")) == []


def test_recovery_fails_closed_when_published_manifest_is_missing(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    """Recovery reports a fatal consistency error instead of recreating missing evidence."""
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)
    publisher = SnapshotPublisher(repository, tmp_path / "snapshots")
    snapshot_id = publisher.publish(versions, quality_run_id)
    repository.get_snapshot(snapshot_id).manifest_path.unlink()

    with pytest.raises(QuantError) as raised:
        publisher.recover()

    assert raised.value.detail.code == "SNAP_MANIFEST_MISSING"
    assert raised.value.detail.severity is Severity.FATAL


def test_repository_returns_frozen_dtos_not_orm_instances(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    """Repository callers receive immutable values detached from SQLAlchemy sessions."""
    record = repository.register_dataset_version(_version_spec(tmp_path))

    with pytest.raises(Exception) as raised:
        record.source = "tushare"  # type: ignore[misc]

    assert raised.type.__name__ == "FrozenInstanceError"
    assert not hasattr(record, "_sa_instance_state")
    issue = QualityIssue(
        rule_id="coverage_info",
        severity=Severity.INFO,
        dataset=DatasetKind.DAILY_BAR,
        scope={"year": 2026},
        actual=1,
        threshold=1,
        message="coverage recorded",
        remediation="none",
    )
    quality = repository.register_quality_run(
        QualityRunSpec(
            dataset_versions={DatasetKind.DAILY_BAR.value: record.id},
            started_at=datetime(2026, 1, 5, 8, tzinfo=UTC),
            completed_at=datetime(2026, 1, 5, 8, 1, tzinfo=UTC),
            issues=(issue,),
        )
    )
    with pytest.raises(TypeError):
        quality.dataset_versions[DatasetKind.DAILY_BAR.value] = record.id
    with pytest.raises(TypeError):
        quality.issues[0].scope["year"] = 2025


def test_catalog_and_published_snapshot_references_are_database_immutable(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    """Direct SQL cannot mutate version evidence or a published manifest mapping."""
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)
    snapshot_id = SnapshotPublisher(repository, tmp_path / "snapshots").publish(
        versions, quality_run_id
    )

    statements = (
        (
            "UPDATE dataset_version SET source = 'changed' WHERE id = :id",
            str(version.id),
        ),
        (
            (
                "UPDATE dataset_partition SET row_count = 99 "
                "WHERE dataset_version_id = :id"
            ),
            str(version.id),
        ),
        (
            (
                "UPDATE snapshot_dataset SET dataset_version_id = :replacement "
                "WHERE snapshot_id = :id"
            ),
            str(snapshot_id),
        ),
    )
    for statement, identifier in statements:
        parameters = {
            "id": identifier,
            "replacement": str(version.id),
        }
        with repository.engine.begin() as connection, pytest.raises(IntegrityError):
            connection.execute(text(statement), parameters)
    quality_statements = (
        "UPDATE quality_run SET status = 'RUNNING' WHERE id = :id",
        (
            "UPDATE quality_run_dataset SET dataset_version_id = :version_id "
            "WHERE quality_run_id = :id"
        ),
        (
            "INSERT INTO quality_issue "
            "(quality_run_id, rule_id, severity, dataset, scope_json, actual_json, "
            "threshold_json, message, remediation) VALUES "
            "(:id, 'late_issue', 'INFO', 'daily_bar', '{}', '1', '0', "
            "'late issue', 'do not mutate completed runs')"
        ),
    )
    for statement in quality_statements:
        with repository.engine.begin() as connection, pytest.raises(IntegrityError):
            connection.execute(
                text(statement),
                {"id": str(quality_run_id), "version_id": str(version.id)},
            )
    with repository.engine.begin() as connection, pytest.raises(IntegrityError):
        connection.execute(
            text(
                "INSERT INTO dataset_partition "
                "(dataset_version_id, ordinal, content_hash, path, "
                "schema_fingerprint, row_count) "
                "VALUES (:id, 99, :hash, :path, :schema, 1)"
            ),
            {
                "id": str(version.id),
                "hash": "c" * 64,
                "path": (tmp_path / "late.parquet").resolve().as_posix(),
                "schema": "d" * 64,
            },
        )


def test_recovery_completes_only_a_fully_verified_draft_manifest(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    """A durable DRAFT becomes published only when its temp evidence matches all refs."""
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)
    snapshot_id = SnapshotId.parse("12345678-1234-5678-9234-567812345678")
    snapshot_dir = tmp_path / "snapshots" / f"snapshot_id={snapshot_id}"
    final_path = snapshot_dir / "manifest.json"
    temporary_path = snapshot_dir / ".recovery.manifest.tmp"
    manifest = {
        "as_of": "2026-01-05T09:00:00+00:00",
        "created_at": "2026-01-05T09:00:00+00:00",
        "datasets": {
            "daily_bar": {
                "dataset_version_id": str(version.id),
                "partitions": [
                    {
                        "content_hash": version.partitions[0].content_hash,
                        "path": version.partitions[0].path.resolve().as_posix(),
                        "row_count": 2,
                        "schema_fingerprint": "a" * 64,
                    }
                ],
            }
        },
        "format_version": 1,
        "quality_run_id": str(quality_run_id),
        "snapshot_id": str(snapshot_id),
        "status": "PUBLISHED",
    }
    manifest_bytes = json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
    snapshot_dir.mkdir(parents=True)
    temporary_path.write_bytes(manifest_bytes)
    with repository.engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO snapshot "
                "(id, publication_fingerprint, as_of, status, manifest_path, "
                "manifest_hash, quality_run_id, created_at, published_at) "
                "VALUES (:id, :fingerprint, :as_of, 'DRAFT', :path, :hash, "
                ":quality_run_id, :created_at, NULL)"
            ),
            {
                "id": str(snapshot_id),
                "fingerprint": "b" * 64,
                "as_of": "2026-01-05T09:00:00+00:00",
                "path": final_path.resolve().as_posix(),
                "hash": manifest_hash,
                "quality_run_id": str(quality_run_id),
                "created_at": "2026-01-05T09:00:00+00:00",
            },
        )
        connection.execute(
            text(
                "INSERT INTO snapshot_dataset "
                "(snapshot_id, dataset, dataset_version_id) "
                "VALUES (:snapshot_id, 'daily_bar', :version_id)"
            ),
            {"snapshot_id": str(snapshot_id), "version_id": str(version.id)},
        )

    SnapshotPublisher(repository, tmp_path / "snapshots").recover()

    recovered = repository.get_snapshot(snapshot_id)
    assert recovered.status is SnapshotStatus.PUBLISHED
    assert final_path.read_bytes() == manifest_bytes
    assert not temporary_path.exists()


def test_recovery_reports_structured_error_for_malformed_published_manifest(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    """Malformed manifest structure becomes a fatal catalog error, not a raw exception."""
    version = repository.register_dataset_version(_version_spec(tmp_path))
    quality_run_id = _completed_quality_run(
        repository, {DatasetKind.DAILY_BAR.value: version.id}
    )
    snapshot_id = SnapshotId.parse("22345678-1234-5678-9234-567812345678")
    snapshot_dir = tmp_path / "snapshots" / f"snapshot_id={snapshot_id}"
    final_path = snapshot_dir / "manifest.json"
    malformed = {
        "as_of": "2026-01-05T09:00:00+00:00",
        "created_at": "2026-01-05T09:00:00+00:00",
        "datasets": [{}],
        "format_version": 1,
        "quality_run_id": str(quality_run_id),
        "snapshot_id": str(snapshot_id),
        "status": "PUBLISHED",
    }
    malformed_bytes = json.dumps(
        malformed,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    snapshot_dir.mkdir(parents=True)
    final_path.write_bytes(malformed_bytes)
    with repository.engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO snapshot "
                "(id, publication_fingerprint, as_of, status, manifest_path, "
                "manifest_hash, quality_run_id, created_at, published_at) "
                "VALUES (:id, :fingerprint, :as_of, 'DRAFT', :path, :hash, "
                ":quality_run_id, :created_at, NULL)"
            ),
            {
                "id": str(snapshot_id),
                "fingerprint": "e" * 64,
                "as_of": "2026-01-05T09:00:00+00:00",
                "path": final_path.resolve().as_posix(),
                "hash": hashlib.sha256(malformed_bytes).hexdigest(),
                "quality_run_id": str(quality_run_id),
                "created_at": "2026-01-05T09:00:00+00:00",
            },
        )
        connection.execute(
            text(
                "INSERT INTO snapshot_dataset "
                "(snapshot_id, dataset, dataset_version_id) "
                "VALUES (:snapshot_id, 'daily_bar', :version_id)"
            ),
            {"snapshot_id": str(snapshot_id), "version_id": str(version.id)},
        )
        connection.execute(
            text("UPDATE snapshot SET status = 'PUBLISHED' WHERE id = :id"),
            {"id": str(snapshot_id)},
        )

    with pytest.raises(QuantError) as raised:
        SnapshotPublisher(repository, tmp_path / "snapshots").recover()

    assert raised.value.detail.code == "SNAP_MANIFEST_MISMATCH"
    assert raised.value.detail.severity is Severity.FATAL
