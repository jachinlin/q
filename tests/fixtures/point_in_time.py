"""Reusable immutable snapshot fixtures for point-in-time repository tests."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.domain.enums import DatasetKind, SnapshotStatus
from quant_core.domain.identifiers import DatasetVersionId, QualityRunId, SnapshotId
from quant_core.persistence.repositories import (
    DatasetPartitionRecord,
    DatasetVersionRecord,
    SnapshotRecord,
)


@dataclass(frozen=True, slots=True)
class PointInTimeFixture:
    """Two snapshots whose financial membership differs by one revision set."""

    repository: FixtureSnapshotRepository
    early_snapshot_id: SnapshotId
    late_snapshot_id: SnapshotId


class FixtureSnapshotRepository:
    """In-memory catalog exposing only records selected by a snapshot identifier."""

    def __init__(
        self,
        snapshots: Mapping[SnapshotId, SnapshotRecord],
        versions: Mapping[DatasetVersionId, DatasetVersionRecord],
    ) -> None:
        self._snapshots = dict(snapshots)
        self._versions = dict(versions)

    def get_snapshot(self, identifier: SnapshotId) -> SnapshotRecord:
        return self._snapshots[identifier]

    def get_dataset_version(self, identifier: DatasetVersionId) -> DatasetVersionRecord:
        return self._versions[identifier]

    def bind_dataset(
        self,
        snapshot_id: SnapshotId,
        dataset: DatasetKind,
        record: DatasetVersionRecord,
    ) -> SnapshotId:
        """Create a test snapshot with one replacement catalog dataset version."""
        original = self._snapshots[snapshot_id]
        identifier = SnapshotId.new()
        self._versions[record.id] = record
        self._snapshots[identifier] = replace(
            original,
            id=identifier,
            dataset_versions={**original.dataset_versions, dataset.value: record.id},
        )
        return identifier

    def snapshot_without_dataset(
        self, identifier: SnapshotId, dataset: str
    ) -> SnapshotId:
        """Register a test-only snapshot that deliberately omits one dataset."""
        original = self._snapshots[identifier]
        missing_identifier = SnapshotId.new()
        self._snapshots[missing_identifier] = replace(
            original,
            id=missing_identifier,
            dataset_versions={
                name: version
                for name, version in original.dataset_versions.items()
                if name != dataset
            },
        )
        return missing_identifier


def point_in_time_fixture(tmp_path: Path) -> PointInTimeFixture:
    """Create canonical Parquet partitions and two catalog-bound snapshots."""
    early_financial = _write_dataset(
        tmp_path,
        "financial-early",
        DatasetKind.FINANCIAL_OBSERVATION,
        [
            _financial_row(
                value=100.0,
                revision=0,
                available_at=datetime(2024, 1, 31, 8, tzinfo=UTC),
            )
        ],
    )
    late_financial = _write_dataset(
        tmp_path,
        "financial-late",
        DatasetKind.FINANCIAL_OBSERVATION,
        [
            _financial_row(
                value=100.0,
                revision=0,
                available_at=datetime(2024, 1, 31, 8, tzinfo=UTC),
            ),
            _financial_row(
                value=120.0,
                revision=1,
                available_at=datetime(2024, 4, 29, 15, 59, 59, 999999, tzinfo=UTC),
            ),
            _financial_row(
                value=125.0,
                revision=2,
                available_at=datetime(2024, 4, 29, 15, 59, 59, 999999, tzinfo=UTC),
            ),
            _financial_row(
                value=130.0,
                revision=3,
                available_at=datetime(2024, 4, 29, 16, tzinfo=UTC),
            ),
            _financial_row(value=140.0, revision=4, available_at=None),
            _financial_row(
                value=150.0,
                revision=5,
                available_at=datetime(2024, 4, 29, 12, tzinfo=UTC),
                pit_usable=False,
            ),
        ],
    )
    bars = _write_dataset(
        tmp_path,
        "bars",
        DatasetKind.DAILY_BAR,
        [
            _bar_row(date_text="2024-04-29", close=11.0),
            _bar_row(date_text="2024-04-28", close=10.0),
        ],
    )
    statuses = _write_dataset(
        tmp_path,
        "status",
        DatasetKind.SECURITY_STATUS,
        [_status_row(date_text="2024-04-29")],
    )
    versions = {
        early_financial.id: early_financial,
        late_financial.id: late_financial,
        bars.id: bars,
        statuses.id: statuses,
    }
    early_snapshot_id = SnapshotId.new()
    late_snapshot_id = SnapshotId.new()
    snapshots = {
        early_snapshot_id: _snapshot(
            tmp_path,
            early_snapshot_id,
            {
                "financial_observation": early_financial.id,
                "daily_bar": bars.id,
                "security_status": statuses.id,
            },
        ),
        late_snapshot_id: _snapshot(
            tmp_path,
            late_snapshot_id,
            {
                "financial_observation": late_financial.id,
                "daily_bar": bars.id,
                "security_status": statuses.id,
            },
        ),
    }
    return PointInTimeFixture(
        repository=FixtureSnapshotRepository(snapshots, versions),
        early_snapshot_id=early_snapshot_id,
        late_snapshot_id=late_snapshot_id,
    )


def _snapshot(
    tmp_path: Path,
    identifier: SnapshotId,
    dataset_versions: Mapping[str, DatasetVersionId],
) -> SnapshotRecord:
    manifest = tmp_path / f"snapshot-{identifier}.json"
    manifest.write_text("{}", encoding="utf-8")
    now = datetime(2024, 4, 29, tzinfo=UTC)
    return SnapshotRecord(
        id=identifier,
        publication_fingerprint="f" * 64,
        as_of=now,
        status=SnapshotStatus.PUBLISHED,
        manifest_path=manifest,
        manifest_hash="e" * 64,
        quality_run_id=QualityRunId.new(),
        dataset_versions=dataset_versions,
        created_at=now,
        published_at=now,
    )


def _write_dataset(
    tmp_path: Path,
    name: str,
    dataset: DatasetKind,
    rows: list[dict[str, object]],
) -> DatasetVersionRecord:
    path = tmp_path / f"{name}.parquet"
    pl.DataFrame(rows, schema=CANONICAL_SCHEMAS[dataset].columns).write_parquet(path)
    return DatasetVersionRecord(
        id=DatasetVersionId.new(),
        dataset=dataset,
        fingerprint="a" * 64,
        source="fixture",
        status="PUBLISHED",
        partitions=(
            DatasetPartitionRecord(
                content_hash="b" * 64,
                path=path,
                schema_fingerprint="c" * 64,
                row_count=len(rows),
            ),
        ),
        start_date=None,
        end_date=None,
        created_run_id="fixture",
        created_at=datetime(2024, 4, 29, tzinfo=UTC),
    )


def _audit(
    available_at: datetime | None, *, pit_usable: bool = True
) -> dict[str, object]:
    return {
        "source": "fixture",
        "source_version": "v1",
        "available_at": available_at,
        "availability_source": "announcement",
        "pit_usable": pit_usable,
        "ingested_at": datetime(2024, 4, 30, tzinfo=UTC),
    }


def _financial_row(
    *,
    value: float,
    revision: int,
    available_at: datetime | None,
    pit_usable: bool = True,
) -> dict[str, object]:
    return {
        "instrument_id": "SSE:600000",
        "report_period": date(2023, 12, 31),
        "metric": "revenue",
        "value": value,
        "revision": revision,
        "announced_at": available_at,
        **_audit(available_at, pit_usable=pit_usable),
    }


def _bar_row(*, date_text: str, close: float) -> dict[str, object]:
    available_at = datetime(2024, 4, 29, tzinfo=UTC)
    return {
        "instrument_id": "SSE:600000",
        "trade_date": date.fromisoformat(date_text),
        "open": close - 0.5,
        "high": close + 0.5,
        "low": close - 1.0,
        "close": close,
        "preclose": close - 0.2,
        "volume": 100,
        "amount": 1_000.0,
        "adjustment_flag": "none",
        "turnover": 1.0,
        "pct_change": 0.1,
        "pe_ttm": 10.0,
        "pb_mrq": 1.0,
        "ps_ttm": 2.0,
        "pcf_ncf_ttm": 3.0,
        **_audit(available_at),
    }


def _status_row(*, date_text: str) -> dict[str, object]:
    available_at = datetime(2024, 4, 29, tzinfo=UTC)
    return {
        "instrument_id": "SSE:600000",
        "trade_date": date.fromisoformat(date_text),
        "is_listed": True,
        "is_suspended": False,
        "is_risk_warning": False,
        "board": "MAIN",
        "price_limit_rule_id": "main",
        "tradable_reason": "normal",
        **_audit(available_at),
    }
