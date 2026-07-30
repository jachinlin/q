"""Integration coverage for catalog, quality gates, and snapshot publication."""

from __future__ import annotations

import hashlib
import json
import subprocess
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

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


def _engine(tmp_path: Path) -> Any:
    return create_sqlite_engine(tmp_path / "state" / "quant.db")


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
    tmp_path: Path,
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

    engine = _engine(tmp_path)
    assert expected_tables.issubset(set(inspect(engine).get_table_names()))
    with engine.connect() as connection:
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


def test_trading_day_coverage_detects_open_dates_without_bars() -> None:
    """Missing bars on the first, middle, or last open date are coverage issues."""
    bars = (
        _quality_frames()[DatasetKind.DAILY_BAR][0]
        .tail(1)
        .with_columns(pl.lit(date(2026, 1, 3)).alias("trade_date"))
    )
    audit = _audit_columns()
    calendar = pl.DataFrame(
        {
            "trade_date": [
                date(2026, 1, 2),
                date(2026, 1, 3),
                date(2026, 1, 5),
            ],
            "is_trading_day": [True, True, True],
            **{name: pl.concat([series.head(1)] * 3) for name, series in audit.items()},
        }
    )

    issues = QualityRunner().evaluate(
        {
            DatasetKind.DAILY_BAR: (bars,),
            DatasetKind.TRADE_CALENDAR: (calendar,),
        }
    )

    coverage = [issue for issue in issues if issue.rule_id == "trading_day_coverage"]
    assert len(coverage) == 1
    assert coverage[0].actual == 2


def test_pit_usable_financial_requires_an_announcement_timestamp() -> None:
    """A financial row cannot be PIT usable when its announcement is unknown."""
    financials = _quality_frames()[DatasetKind.FINANCIAL_OBSERVATION][0].with_columns(
        pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("announced_at")
    )

    issues = QualityRunner().evaluate(
        {DatasetKind.FINANCIAL_OBSERVATION: (financials,)}
    )

    assert [issue.rule_id for issue in issues] == ["financial_availability"]
    assert issues[0].severity in (Severity.SEVERE, Severity.FATAL)


def test_quality_inputs_are_defensively_copied_and_recursively_frozen() -> None:
    """Nested JSON and version mappings cannot mutate a constructed quality model."""
    nested_scope = {"window": {"years": [2025, 2026]}}
    nested_actual = {"samples": [{"count": 2}]}
    nested_threshold = [{"maximum": 0}]
    issue = QualityIssue(
        rule_id="nested",
        severity=Severity.INFO,
        dataset=DatasetKind.DAILY_BAR,
        scope=nested_scope,
        actual=nested_actual,
        threshold=nested_threshold,
        message="nested evidence",
        remediation="none",
    )
    version_id = DatasetVersionId.parse("12345678-1234-5678-9234-567812345678")
    version_mapping = {DatasetKind.DAILY_BAR.value: version_id}
    run = QualityRunSpec(
        dataset_versions=version_mapping,
        started_at=datetime(2026, 1, 5, 8, tzinfo=UTC),
        completed_at=None,
        issues=(issue,),
    )

    nested_scope["window"]["years"].append(2027)
    nested_actual["samples"][0]["count"] = 99
    nested_threshold[0]["maximum"] = 99
    version_mapping.clear()

    assert issue.scope["window"]["years"] == (2025, 2026)  # type: ignore[index]
    assert issue.actual["samples"][0]["count"] == 2  # type: ignore[index]
    assert issue.threshold[0]["maximum"] == 0  # type: ignore[index]
    assert dict(run.dataset_versions) == {DatasetKind.DAILY_BAR.value: version_id}
    with pytest.raises(TypeError):
        issue.scope["window"]["years"] += (2028,)  # type: ignore[index,operator]
    with pytest.raises(TypeError):
        run.dataset_versions[DatasetKind.DAILY_BAR.value] = version_id


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
    with _engine(tmp_path).begin() as connection, pytest.raises(IntegrityError):
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
        with _engine(tmp_path).begin() as connection, pytest.raises(IntegrityError):
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
        with _engine(tmp_path).begin() as connection, pytest.raises(IntegrityError):
            connection.execute(
                text(statement),
                {"id": str(quality_run_id), "version_id": str(version.id)},
            )
    with _engine(tmp_path).begin() as connection, pytest.raises(IntegrityError):
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
    temporary_path = snapshot_dir / f".{uuid.uuid4().hex}.manifest.tmp"
    unrelated_temp = snapshot_dir / "keep.tmp"
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
    unrelated_temp.write_text("do not delete", encoding="utf-8")
    with _engine(tmp_path).begin() as connection:
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
    assert unrelated_temp.read_text(encoding="utf-8") == "do not delete"
    with _engine(tmp_path).connect() as connection:
        assert (
            connection.execute(
                text("SELECT action FROM audit_log WHERE object_id = :id"),
                {"id": str(snapshot_id)},
            ).scalar_one()
            == "SNAPSHOT_RECOVERED"
        )


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
    with _engine(tmp_path).begin() as connection:
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


def test_recovery_structures_non_finite_manifest_values(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    """Non-finite JSON values cannot escape manifest validation as ValueError."""
    version = repository.register_dataset_version(_version_spec(tmp_path))
    quality_run_id = _completed_quality_run(
        repository, {DatasetKind.DAILY_BAR.value: version.id}
    )
    snapshot_id = SnapshotId.parse("32345678-1234-5678-9234-567812345678")
    snapshot_dir = tmp_path / "snapshots" / f"snapshot_id={snapshot_id}"
    final_path = snapshot_dir / "manifest.json"
    malformed_bytes = (
        '{"as_of":NaN,"created_at":"2026-01-05T09:00:00+00:00",'
        '"datasets":{},"format_version":1,"quality_run_id":"'
        + str(quality_run_id)
        + '","snapshot_id":"'
        + str(snapshot_id)
        + '","status":"PUBLISHED"}'
    ).encode("utf-8")
    snapshot_dir.mkdir(parents=True)
    final_path.write_bytes(malformed_bytes)
    with _engine(tmp_path).begin() as connection:
        connection.execute(
            text(
                "INSERT INTO snapshot "
                "(id, publication_fingerprint, as_of, status, manifest_path, "
                "manifest_hash, quality_run_id, created_at, published_at) "
                "VALUES (:id, :fingerprint, :as_of, 'PUBLISHED', :path, :hash, "
                ":quality_run_id, :created_at, :created_at)"
            ),
            {
                "id": str(snapshot_id),
                "fingerprint": "f" * 64,
                "as_of": "2026-01-05T09:00:00+00:00",
                "path": final_path.resolve().as_posix(),
                "hash": hashlib.sha256(malformed_bytes).hexdigest(),
                "quality_run_id": str(quality_run_id),
                "created_at": "2026-01-05T09:00:00+00:00",
            },
        )

    with pytest.raises(QuantError) as raised:
        SnapshotPublisher(repository, tmp_path / "snapshots").recover()

    assert raised.value.detail.code == "SNAP_MANIFEST_MISMATCH"


def test_recovery_structures_manifest_read_errors(
    repository: MetadataRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unreadable present manifest is a structured mismatch, not raw OSError."""
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)
    publisher = SnapshotPublisher(repository, tmp_path / "snapshots")
    snapshot_id = publisher.publish(versions, quality_run_id)
    manifest_path = repository.get_snapshot(snapshot_id).manifest_path
    original_read_bytes = Path.read_bytes

    def fail_manifest_read(path: Path) -> bytes:
        if path == manifest_path:
            raise PermissionError("injected unreadable manifest")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", fail_manifest_read)

    with pytest.raises(QuantError) as raised:
        publisher.recover()

    assert raised.value.detail.code == "SNAP_MANIFEST_MISMATCH"


def test_recovery_structures_manifest_reference_errors(
    repository: MetadataRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reference validation failures remain catalog mismatches."""
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)
    publisher = SnapshotPublisher(repository, tmp_path / "snapshots")
    publisher.publish(versions, quality_run_id)

    def fail_reference_lookup(identifier: DatasetVersionId) -> None:
        raise OSError(f"injected reference failure: {identifier}")

    monkeypatch.setattr(repository, "get_dataset_version", fail_reference_lookup)

    with pytest.raises(QuantError) as raised:
        publisher.recover()

    assert raised.value.detail.code == "SNAP_MANIFEST_MISMATCH"


def _insert_draft_snapshot(
    tmp_path: Path,
    snapshot_id: SnapshotId,
    quality_run_id: QualityRunId,
    manifest_path: Path,
    manifest_bytes: bytes,
    versions: dict[str, DatasetVersionId],
) -> None:
    with _engine(tmp_path).begin() as connection:
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
                "fingerprint": hashlib.sha256(str(snapshot_id).encode()).hexdigest(),
                "as_of": "2026-01-05T09:00:00+00:00",
                "path": manifest_path.absolute().as_posix(),
                "hash": hashlib.sha256(manifest_bytes).hexdigest(),
                "quality_run_id": str(quality_run_id),
                "created_at": "2026-01-05T09:00:00+00:00",
            },
        )
        for dataset, version_id in versions.items():
            connection.execute(
                text(
                    "INSERT INTO snapshot_dataset "
                    "(snapshot_id, dataset, dataset_version_id) "
                    "VALUES (:snapshot_id, :dataset, :version_id)"
                ),
                {
                    "snapshot_id": str(snapshot_id),
                    "dataset": dataset,
                    "version_id": str(version_id),
                },
            )


def _manifest_bytes(
    snapshot_id: SnapshotId,
    quality_run_id: QualityRunId,
    versions: dict[str, object],
) -> bytes:
    manifest = {
        "as_of": "2026-01-05T09:00:00+00:00",
        "created_at": "2026-01-05T09:00:00+00:00",
        "datasets": {
            dataset: {
                "dataset_version_id": str(record.id),
                "partitions": [
                    {
                        "content_hash": partition.content_hash,
                        "path": partition.path.resolve().as_posix(),
                        "row_count": partition.row_count,
                        "schema_fingerprint": partition.schema_fingerprint,
                    }
                    for partition in record.partitions
                ],
            }
            for dataset, record in versions.items()
        },
        "format_version": 1,
        "quality_run_id": str(quality_run_id),
        "snapshot_id": str(snapshot_id),
        "status": "PUBLISHED",
    }
    return json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()


def test_recovery_rejects_catalog_paths_outside_snapshot_root(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    version = repository.register_dataset_version(_version_spec(tmp_path))
    quality_run_id = _completed_quality_run(
        repository, {DatasetKind.DAILY_BAR.value: version.id}
    )
    snapshot_id = SnapshotId.parse("42345678-1234-5678-9234-567812345678")
    victim = tmp_path / "victim" / "manifest.json"
    victim.parent.mkdir()
    victim.write_text("external victim", encoding="utf-8")
    _insert_draft_snapshot(
        tmp_path,
        snapshot_id,
        quality_run_id,
        victim,
        victim.read_bytes(),
        {},
    )

    with pytest.raises(QuantError) as raised:
        SnapshotPublisher(repository, tmp_path / "snapshots").recover()

    assert raised.value.detail.code == "SNAP_PATH_INVALID"
    assert victim.read_text(encoding="utf-8") == "external victim"


def test_recovery_rejects_snapshot_directory_links_without_touching_target(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    version = repository.register_dataset_version(_version_spec(tmp_path))
    quality_run_id = _completed_quality_run(
        repository, {DatasetKind.DAILY_BAR.value: version.id}
    )
    snapshot_id = SnapshotId.parse("43345678-1234-5678-9234-567812345678")
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "manifest.json"
    victim.write_text("linked victim", encoding="utf-8")
    snapshot_root = tmp_path / "snapshots"
    snapshot_root.mkdir()
    linked_directory = snapshot_root / f"snapshot_id={snapshot_id}"
    try:
        linked_directory.symlink_to(outside, target_is_directory=True)
    except OSError:
        subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                (
                    "New-Item -ItemType Junction "
                    f"-Path '{linked_directory}' -Target '{outside}' | Out-Null"
                ),
            ],
            check=True,
            capture_output=True,
        )
    _insert_draft_snapshot(
        tmp_path,
        snapshot_id,
        quality_run_id,
        linked_directory / "manifest.json",
        victim.read_bytes(),
        {},
    )

    with pytest.raises(QuantError) as raised:
        SnapshotPublisher(repository, snapshot_root).recover()

    assert raised.value.detail.code == "SNAP_PATH_INVALID"
    assert victim.read_text(encoding="utf-8") == "linked victim"


def test_recovery_rejects_a_published_manifest_file_link(
    repository: MetadataRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)
    publisher = SnapshotPublisher(repository, tmp_path / "snapshots")
    snapshot_id = publisher.publish(versions, quality_run_id)
    manifest_path = repository.get_snapshot(snapshot_id).manifest_path
    manifest_bytes = manifest_path.read_bytes()
    victim = tmp_path / "outside-final-manifest.json"
    victim.write_bytes(manifest_bytes)
    manifest_path.unlink()
    try:
        manifest_path.symlink_to(victim)
    except OSError:
        manifest_path.write_bytes(manifest_bytes)
        original_is_symlink = Path.is_symlink
        monkeypatch.setattr(
            Path,
            "is_symlink",
            lambda path: path == manifest_path or original_is_symlink(path),
        )

    with pytest.raises(QuantError) as raised:
        publisher.recover()

    assert raised.value.detail.code == "SNAP_PATH_INVALID"
    assert victim.read_bytes() == manifest_bytes


def test_recovery_never_reads_or_publishes_a_linked_protocol_temp(
    repository: MetadataRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)
    snapshot_id = SnapshotId.parse("44345678-1234-5678-9234-567812345678")
    snapshot_dir = tmp_path / "snapshots" / f"snapshot_id={snapshot_id}"
    final_path = snapshot_dir / "manifest.json"
    linked_temp = snapshot_dir / f".{uuid.uuid4().hex}.manifest.tmp"
    payload = _manifest_bytes(
        snapshot_id, quality_run_id, {DatasetKind.DAILY_BAR.value: version}
    )
    snapshot_dir.mkdir(parents=True)
    victim = tmp_path / "outside-temp-manifest.json"
    victim.write_bytes(payload)
    using_real_symlink = True
    try:
        linked_temp.symlink_to(victim)
    except OSError:
        using_real_symlink = False
        linked_temp.write_bytes(payload)
    _insert_draft_snapshot(
        tmp_path, snapshot_id, quality_run_id, final_path, payload, versions
    )
    original_is_symlink = Path.is_symlink
    original_read_bytes = Path.read_bytes
    if not using_real_symlink:
        monkeypatch.setattr(
            Path,
            "is_symlink",
            lambda path: path == linked_temp or original_is_symlink(path),
        )

    def reject_link_read(path: Path) -> bytes:
        if path == linked_temp:
            raise AssertionError("linked protocol temp must not be read")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", reject_link_read)

    with pytest.raises(QuantError) as raised:
        SnapshotPublisher(repository, tmp_path / "snapshots").recover()

    assert raised.value.detail.code == "SNAP_PATH_INVALID"
    assert repository.get_snapshot(snapshot_id).status is SnapshotStatus.DRAFT
    assert linked_temp.exists()
    assert not final_path.exists()
    assert victim.read_bytes() == payload


def test_publish_structures_temporary_manifest_hash_mismatch(
    repository: MetadataRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)
    original_read_bytes = Path.read_bytes

    def corrupt_temp_readback(path: Path) -> bytes:
        if path.name.endswith(".manifest.tmp"):
            return b"corrupt readback"
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", corrupt_temp_readback)

    with pytest.raises(QuantError) as raised:
        SnapshotPublisher(repository, tmp_path / "snapshots").publish(
            versions, quality_run_id
        )

    assert raised.value.detail.code == "SNAP_MANIFEST_MISMATCH"
    assert repository.count_snapshots() == 0


def test_publish_structures_temporary_manifest_read_failure(
    repository: MetadataRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)
    original_read_bytes = Path.read_bytes

    def fail_temp_readback(path: Path) -> bytes:
        if path.name.endswith(".manifest.tmp"):
            raise PermissionError("injected temporary manifest read failure")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", fail_temp_readback)

    with pytest.raises(QuantError) as raised:
        SnapshotPublisher(repository, tmp_path / "snapshots").publish(
            versions, quality_run_id
        )

    assert raised.value.detail.code == "SNAP_MANIFEST_MISMATCH"
    assert repository.count_snapshots() == 0
    assert list((tmp_path / "snapshots").rglob("*.manifest.tmp")) == []


def test_publish_structures_and_cleans_temporary_manifest_write_failure(
    repository: MetadataRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)
    original_open = Path.open
    original_write_bytes = Path.write_bytes

    def fail_after_partial_write(
        path: Path, mode: str = "r", *args: Any, **kwargs: Any
    ) -> Any:
        if path.name.endswith(".manifest.tmp") and mode == "xb":
            original_write_bytes(path, b"partial")
            raise PermissionError("injected temporary manifest write failure")
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_after_partial_write)

    with pytest.raises(QuantError) as raised:
        SnapshotPublisher(repository, tmp_path / "snapshots").publish(
            versions, quality_run_id
        )

    assert raised.value.detail.code == "SNAP_MANIFEST_MISMATCH"
    assert repository.count_snapshots() == 0
    assert list((tmp_path / "snapshots").rglob("*.manifest.tmp")) == []


def test_draft_recovery_discards_quality_scope_mismatch_with_audit(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    version = repository.register_dataset_version(_version_spec(tmp_path))
    quality_run_id = _completed_quality_run(
        repository, {DatasetKind.DAILY_BAR.value: version.id}
    )
    snapshot_id = SnapshotId.parse("52345678-1234-5678-9234-567812345678")
    path = tmp_path / "snapshots" / f"snapshot_id={snapshot_id}" / "manifest.json"
    payload = _manifest_bytes(snapshot_id, quality_run_id, {})
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    _insert_draft_snapshot(tmp_path, snapshot_id, quality_run_id, path, payload, {})

    SnapshotPublisher(repository, tmp_path / "snapshots").recover()

    with pytest.raises(KeyError):
        repository.get_snapshot(snapshot_id)
    assert not path.exists()
    with _engine(tmp_path).connect() as connection:
        actions = connection.execute(
            text("SELECT action FROM audit_log WHERE object_id = :id"),
            {"id": str(snapshot_id)},
        ).scalars()
        assert "SNAPSHOT_RECOVERY_DISCARDED" in set(actions)


def test_draft_recovery_discards_new_blocking_quality_issue(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    issue = QualityIssue(
        rule_id="blocking",
        severity=Severity.SEVERE,
        dataset=DatasetKind.DAILY_BAR,
        scope={},
        actual=1,
        threshold=0,
        message="blocked",
        remediation="repair",
    )
    quality_run_id = _completed_quality_run(repository, versions, issues=(issue,))
    snapshot_id = SnapshotId.parse("62345678-1234-5678-9234-567812345678")
    path = tmp_path / "snapshots" / f"snapshot_id={snapshot_id}" / "manifest.json"
    payload = _manifest_bytes(
        snapshot_id, quality_run_id, {DatasetKind.DAILY_BAR.value: version}
    )
    path.parent.mkdir(parents=True)
    path.write_bytes(payload)
    _insert_draft_snapshot(
        tmp_path, snapshot_id, quality_run_id, path, payload, versions
    )

    SnapshotPublisher(repository, tmp_path / "snapshots").recover()

    with pytest.raises(KeyError):
        repository.get_snapshot(snapshot_id)
    with _engine(tmp_path).connect() as connection:
        assert (
            connection.execute(
                text("SELECT action FROM audit_log WHERE object_id = :id"),
                {"id": str(snapshot_id)},
            ).scalar_one()
            == "SNAPSHOT_RECOVERY_DISCARDED"
        )


def test_publisher_uses_repository_unit_of_work_without_engine_access(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)

    class EngineForbiddenRepository:
        def __getattr__(self, name: str) -> Any:
            if name == "engine":
                raise AssertionError("publisher must not access repository.engine")
            return getattr(repository, name)

    snapshot_id = SnapshotPublisher(
        EngineForbiddenRepository(),  # type: ignore[arg-type]
        tmp_path / "snapshots",
    ).publish(versions, quality_run_id)

    assert repository.get_snapshot(snapshot_id).status is SnapshotStatus.PUBLISHED


def test_concurrent_dataset_registration_is_idempotent(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    spec = _version_spec(tmp_path)
    barrier = threading.Barrier(4)

    def register() -> DatasetVersionId:
        barrier.wait()
        return repository.register_dataset_version(spec).id

    with ThreadPoolExecutor(max_workers=4) as executor:
        identifiers = tuple(executor.map(lambda _: register(), range(4)))

    assert len(set(identifiers)) == 1
    assert repository.count_dataset_versions() == 1


def test_concurrent_snapshot_publication_is_idempotent(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    version = repository.register_dataset_version(_version_spec(tmp_path))
    versions = {DatasetKind.DAILY_BAR.value: version.id}
    quality_run_id = _completed_quality_run(repository, versions)
    barrier = threading.Barrier(4)
    now = datetime(2026, 1, 5, 9, tzinfo=UTC)

    def publish() -> SnapshotId:
        barrier.wait()
        return SnapshotPublisher(
            repository, tmp_path / "snapshots", clock=lambda: now
        ).publish(versions, quality_run_id)

    with ThreadPoolExecutor(max_workers=4) as executor:
        identifiers = tuple(executor.map(lambda _: publish(), range(4)))

    assert len(set(identifiers)) == 1
    assert repository.count_snapshots() == 1
    snapshot = repository.get_snapshot(identifiers[0])
    assert snapshot.manifest_path.is_file()
    assert hashlib.sha256(snapshot.manifest_path.read_bytes()).hexdigest() == (
        snapshot.manifest_hash
    )


def test_relation_rows_cannot_be_reparented_into_sealed_parents(
    repository: MetadataRepository,
    tmp_path: Path,
) -> None:
    daily = repository.register_dataset_version(_version_spec(tmp_path))
    calendar = repository.register_dataset_version(
        _version_spec(tmp_path, DatasetKind.TRADE_CALENDAR)
    )
    source_run = repository.register_quality_run(
        QualityRunSpec(
            dataset_versions={DatasetKind.TRADE_CALENDAR.value: calendar.id},
            started_at=datetime(2026, 1, 5, 8, tzinfo=UTC),
            completed_at=None,
            issues=(),
        )
    )
    target_run = _completed_quality_run(
        repository, {DatasetKind.DAILY_BAR.value: daily.id}
    )
    draft_version_id = "72345678-1234-5678-9234-567812345678"
    source_snapshot_id = "82345678-1234-5678-9234-567812345678"
    target_snapshot_id = "92345678-1234-5678-9234-567812345678"
    timestamp = "2026-01-05T09:00:00+00:00"
    engine = _engine(tmp_path)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO dataset_version "
                "(id, dataset, fingerprint, source, status, start_date, end_date, "
                "created_run_id, created_at) VALUES "
                "(:id, 'daily_bar', :fingerprint, 'fixture', 'DRAFT', NULL, NULL, "
                "'draft', :created_at)"
            ),
            {"id": draft_version_id, "fingerprint": "7" * 64, "created_at": timestamp},
        )
        connection.execute(
            text(
                "INSERT INTO dataset_partition "
                "(dataset_version_id, ordinal, content_hash, path, "
                "schema_fingerprint, row_count) VALUES "
                "(:id, 99, :hash, 'draft.parquet', :schema, 1)"
            ),
            {"id": draft_version_id, "hash": "8" * 64, "schema": "9" * 64},
        )
        connection.execute(
            text(
                "INSERT INTO quality_issue "
                "(quality_run_id, rule_id, severity, dataset, scope_json, "
                "actual_json, threshold_json, message, remediation) VALUES "
                "(:id, 'draft', 'INFO', 'trade_calendar', '{}', '1', '0', "
                "'draft', 'none')"
            ),
            {"id": str(source_run.id)},
        )
        for snapshot_id, status, fingerprint in (
            (source_snapshot_id, "DRAFT", "a" * 64),
            (target_snapshot_id, "PUBLISHED", "b" * 64),
        ):
            connection.execute(
                text(
                    "INSERT INTO snapshot "
                    "(id, publication_fingerprint, as_of, status, manifest_path, "
                    "manifest_hash, quality_run_id, created_at, published_at) "
                    "VALUES (:id, :fingerprint, :created_at, :status, :path, "
                    ":hash, :quality_run_id, :created_at, :published_at)"
                ),
                {
                    "id": snapshot_id,
                    "fingerprint": fingerprint,
                    "created_at": timestamp,
                    "status": status,
                    "path": str(tmp_path / f"{snapshot_id}.json"),
                    "hash": "c" * 64,
                    "quality_run_id": str(target_run),
                    "published_at": timestamp if status == "PUBLISHED" else None,
                },
            )
        connection.execute(
            text(
                "INSERT INTO snapshot_dataset "
                "(snapshot_id, dataset, dataset_version_id) "
                "VALUES (:id, 'trade_calendar', :version_id)"
            ),
            {"id": source_snapshot_id, "version_id": str(calendar.id)},
        )

    statements = (
        (
            (
                "UPDATE dataset_partition SET dataset_version_id = :target "
                "WHERE dataset_version_id = :source"
            ),
            {"source": draft_version_id, "target": str(daily.id)},
        ),
        (
            (
                "UPDATE quality_run_dataset SET quality_run_id = :target "
                "WHERE quality_run_id = :source"
            ),
            {"source": str(source_run.id), "target": str(target_run)},
        ),
        (
            (
                "UPDATE quality_issue SET quality_run_id = :target "
                "WHERE quality_run_id = :source"
            ),
            {"source": str(source_run.id), "target": str(target_run)},
        ),
        (
            (
                "UPDATE snapshot_dataset SET snapshot_id = :target "
                "WHERE snapshot_id = :source"
            ),
            {"source": source_snapshot_id, "target": target_snapshot_id},
        ),
    )
    blocked: list[bool] = []
    with engine.connect() as connection:
        for statement, parameters in statements:
            transaction = connection.begin()
            try:
                connection.execute(text(statement), parameters)
            except IntegrityError:
                blocked.append(True)
            else:
                blocked.append(False)
            finally:
                transaction.rollback()

    assert blocked == [True, True, True, True]
