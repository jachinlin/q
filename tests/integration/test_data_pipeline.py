"""Offline integration coverage for the reproducible data pipeline."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import polars as pl
import pytest
from typer.testing import CliRunner

from quant_core.cli import ApplicationServices, create_app
from quant_core.data.contracts import CanonicalBatch, PublishedPartition, RawBatch
from quant_core.data.mappers.baostock import BaoStockMapper
from quant_core.data.partitions import RawPartitionStore
from quant_core.data.pipelines.curate import CuratedPartitionStore
from quant_core.data.pipelines.publish import DataPipeline, PipelineResult
from quant_core.data.quality.runner import QualityRunner
from quant_core.data.snapshots import SnapshotPublisher
from quant_core.domain.enums import SnapshotStatus
from quant_core.domain.identifiers import QualityRunId, SnapshotId
from quant_core.errors import QuantError
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.persistence.repositories import (
    MetadataRepository,
    PipelineRunSpec,
    PipelineStageName,
    SnapshotRecord,
)


def test_pipeline_checkpoint_survives_process_restart(tmp_path: Path) -> None:
    """Removing persistence would make a restarted process lose its checkpoint."""
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    created = repository.register_pipeline_run(
        PipelineRunSpec(
            mode="BOOTSTRAP",
            provider="baostock",
            request_hash="1" * 64,
            requested_start=None,
            requested_end=None,
            resolved_start=date(2006, 1, 4),
            resolved_end=date(2026, 1, 5),
            created_at=datetime(2026, 1, 6, tzinfo=UTC),
        )
    )
    repository.start_pipeline_stage(
        created.id,
        PipelineStageName.INGEST_RAW,
        input_hash="2" * 64,
        started_at=datetime(2026, 1, 6, 0, 1, tzinfo=UTC),
    )
    repository.complete_pipeline_stage(
        created.id,
        PipelineStageName.INGEST_RAW,
        input_hash="2" * 64,
        output_hash="3" * 64,
        output={"manifest_paths": ["C:/runtime/raw/one.manifest.json"]},
        completed_at=datetime(2026, 1, 6, 0, 2, tzinfo=UTC),
    )
    engine.dispose()

    restarted_engine = create_sqlite_engine(database)
    restarted = MetadataRepository(restarted_engine)
    checkpoint = restarted.get_pipeline_stage(created.id, PipelineStageName.INGEST_RAW)

    assert checkpoint.status == "SUCCEEDED"
    assert checkpoint.input_hash == "2" * 64
    assert checkpoint.output_hash == "3" * 64
    assert checkpoint.output == {
        "manifest_paths": ("C:/runtime/raw/one.manifest.json",)
    }
    assert checkpoint.started_at == datetime(2026, 1, 6, 0, 1, tzinfo=UTC)
    assert checkpoint.completed_at == datetime(2026, 1, 6, 0, 2, tzinfo=UTC)
    restarted_engine.dispose()


def test_pipeline_stage_cannot_replace_successful_checkpoint(tmp_path: Path) -> None:
    """Changing a successful stage hash must fail instead of rewriting history."""
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    run = repository.register_pipeline_run(
        PipelineRunSpec(
            mode="UPDATE",
            provider="baostock",
            request_hash="4" * 64,
            requested_start=date(2026, 1, 2),
            requested_end=date(2026, 1, 5),
            resolved_start=date(2026, 1, 2),
            resolved_end=date(2026, 1, 5),
            created_at=datetime(2026, 1, 6, tzinfo=UTC),
        )
    )
    repository.start_pipeline_stage(
        run.id,
        PipelineStageName.CURATE,
        input_hash="5" * 64,
        started_at=datetime(2026, 1, 6, 0, 1, tzinfo=UTC),
    )
    repository.complete_pipeline_stage(
        run.id,
        PipelineStageName.CURATE,
        input_hash="5" * 64,
        output_hash="6" * 64,
        output={"dataset_versions": {"daily_bar": "v1"}},
        completed_at=datetime(2026, 1, 6, 0, 2, tzinfo=UTC),
    )

    try:
        repository.start_pipeline_stage(
            run.id,
            PipelineStageName.CURATE,
            input_hash="7" * 64,
            started_at=datetime(2026, 1, 6, 0, 3, tzinfo=UTC),
        )
    except ValueError as error:
        assert "successful pipeline stage" in str(error)
    else:
        raise AssertionError("successful checkpoint was replaced")
    finally:
        engine.dispose()


class FixedCalendarPolicy:
    def bootstrap_window(self, years: int) -> tuple[date, date]:
        assert years == 20
        return date(2006, 1, 4), date(2026, 1, 5)

    def explicit_window(self, start: date, end: date) -> tuple[date, date]:
        return start, end

    def update_window(self, watermark: date, overlap_days: int) -> tuple[date, date]:
        assert overlap_days == 5
        assert watermark == date(2026, 1, 5)
        return date(2025, 12, 29), date(2026, 1, 6)


class OfflineBaoStockSource:
    provider = "baostock"

    def __init__(
        self,
        *,
        volume: str = "100",
        bar_date: str = "2026-01-05",
        close_price: str = "10.50",
    ) -> None:
        self.fetch_calls = 0
        self.login_calls = 0
        self.close_calls = 0
        self.volume = volume
        self.bar_date = bar_date
        self.close_price = close_price

    def login(self) -> None:
        self.login_calls += 1

    def close(self) -> None:
        self.close_calls += 1

    def fetch_range(self, start: date, end: date) -> Iterable[RawBatch]:
        self.fetch_calls += 1
        retrieved = datetime(2026, 1, 6, tzinfo=UTC)
        yield RawBatch(
            provider=self.provider,
            dataset="instruments",
            request={"scope": "ALL_HISTORICAL"},
            retrieved_at=retrieved,
            schema=("code", "code_name", "ipoDate", "outDate", "type", "status"),
            rows=(
                {
                    "code": "sh.600000",
                    "code_name": "浦发银行",
                    "ipoDate": "1999-11-10",
                    "outDate": "",
                    "type": "1",
                    "status": "1",
                },
            ),
        )
        yield RawBatch(
            provider=self.provider,
            dataset="trade_calendar",
            request={"start_date": start.isoformat(), "end_date": end.isoformat()},
            retrieved_at=retrieved,
            schema=("calendar_date", "is_trading_day"),
            rows=(({"calendar_date": self.bar_date, "is_trading_day": "1"}),),
        )
        yield RawBatch(
            provider=self.provider,
            dataset="daily_bars",
            request={"start_date": start.isoformat(), "end_date": end.isoformat()},
            retrieved_at=retrieved,
            schema=(
                "date",
                "code",
                "open",
                "high",
                "low",
                "close",
                "preclose",
                "volume",
                "amount",
                "adjustflag",
                "turn",
                "tradestatus",
                "pctChg",
                "peTTM",
                "pbMRQ",
                "psTTM",
                "pcfNcfTTM",
                "isST",
            ),
            rows=(
                {
                    "date": self.bar_date,
                    "code": "sh.600000",
                    "open": "10.00",
                    "high": "10.80",
                    "low": "9.90",
                    "close": self.close_price,
                    "preclose": "9.95",
                    "volume": self.volume,
                    "amount": "1050.00",
                    "adjustflag": "3",
                    "turn": "0.42",
                    "tradestatus": "1",
                    "pctChg": "5.53",
                    "peTTM": "8.10",
                    "pbMRQ": "1.20",
                    "psTTM": "2.30",
                    "pcfNcfTTM": "4.50",
                    "isST": "0",
                },
            ),
        )


class FailOnceMapper:
    def __init__(self) -> None:
        self._delegate = BaoStockMapper()
        self.calls = 0

    def normalize(self, raw_partition: PublishedPartition) -> Iterable[CanonicalBatch]:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("simulated mapper crash")
        return self._delegate.normalize(raw_partition)


def make_pipeline(
    tmp_path: Path,
    source: OfflineBaoStockSource,
    *,
    mapper: object | None = None,
    quality_runner: object | None = None,
    publisher_factory: Callable[[MetadataRepository, Path], object] | None = None,
) -> tuple[DataPipeline, MetadataRepository]:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    repository = MetadataRepository(create_sqlite_engine(database))
    snapshot_root = tmp_path / "data" / "snapshots"
    publisher = (
        publisher_factory(repository, snapshot_root)
        if publisher_factory is not None
        else SnapshotPublisher(
            repository,
            snapshot_root,
            clock=lambda: datetime(2026, 1, 6, tzinfo=UTC),
        )
    )
    pipeline = DataPipeline(
        source=source,
        mapper=mapper or BaoStockMapper(),
        calendar=FixedCalendarPolicy(),
        raw_store=RawPartitionStore(tmp_path / "data" / "raw"),
        curated_store=CuratedPartitionStore(tmp_path / "data" / "curated"),
        repository=repository,
        quality_runner=quality_runner or QualityRunner(),  # type: ignore[arg-type]
        snapshot_publisher=publisher,  # type: ignore[arg-type]
        clock=lambda: datetime(2026, 1, 6, tzinfo=UTC),
    )
    return pipeline, repository


class FailOnceQualityRunner:
    def __init__(self) -> None:
        self._delegate = QualityRunner()
        self.calls = 0

    def evaluate(self, inputs: object) -> object:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("simulated quality crash")
        return self._delegate.evaluate(inputs)  # type: ignore[arg-type]


class FailOnceSnapshotPublisher:
    def __init__(self, repository: MetadataRepository, root: Path) -> None:
        self._delegate = SnapshotPublisher(
            repository, root, clock=lambda: datetime(2026, 1, 6, tzinfo=UTC)
        )
        self.calls = 0

    def publish(self, versions: object, quality_run_id: object) -> SnapshotId:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("simulated publish crash")
        return self._delegate.publish(versions, quality_run_id)  # type: ignore[arg-type]


def test_pipeline_runs_real_offline_raw_to_snapshot_and_is_idempotent(
    tmp_path: Path,
) -> None:
    """Skipping a boundary or hashing unstable data changes this observable result."""
    source = OfflineBaoStockSource()
    pipeline, repository = make_pipeline(tmp_path, source)

    first = pipeline.bootstrap()
    second = pipeline.bootstrap()

    assert isinstance(first.snapshot_id, SnapshotId)
    assert second.snapshot_id == first.snapshot_id
    assert source.fetch_calls == 1
    assert source.login_calls == source.close_calls == 1
    assert repository.count_snapshots() == 1
    assert set(first.dataset_versions) == {
        "instrument",
        "trade_calendar",
        "daily_bar",
        "security_status",
    }
    manifest = repository.get_snapshot(first.snapshot_id).manifest_path.read_text(
        encoding="utf-8"
    )
    assert "code_name" not in manifest
    assert "tradestatus" not in manifest


def test_pipeline_recovers_after_curate_failure_without_calling_source_again(
    tmp_path: Path,
) -> None:
    """Ignoring the Raw checkpoint makes the second attempt call the source twice."""
    source = OfflineBaoStockSource()
    mapper = FailOnceMapper()
    pipeline, _ = make_pipeline(tmp_path, source, mapper=mapper)

    with pytest.raises(RuntimeError, match="simulated mapper crash"):
        pipeline.bootstrap()
    recovered = pipeline.bootstrap()

    assert isinstance(recovered.snapshot_id, SnapshotId)
    assert source.fetch_calls == 1


def test_pipeline_rejects_tampered_raw_checkpoint_without_refetching(
    tmp_path: Path,
) -> None:
    """Trusting file existence would silently accept corrupted Raw evidence."""
    source = OfflineBaoStockSource()
    pipeline, repository = make_pipeline(tmp_path, source, mapper=FailOnceMapper())
    with pytest.raises(RuntimeError, match="simulated mapper crash"):
        pipeline.bootstrap()
    run = repository.latest_recoverable_pipeline_run()
    assert run is not None
    checkpoint = repository.get_pipeline_stage(run.id, PipelineStageName.INGEST_RAW)
    assert isinstance(checkpoint.output, Mapping)
    partitions = checkpoint.output["partitions"]
    assert isinstance(partitions, tuple)
    first = partitions[0]
    assert isinstance(first, Mapping)
    Path(str(first["data_path"])).write_bytes(b"corrupt")

    with pytest.raises(QuantError) as captured:
        pipeline.bootstrap()

    assert captured.value.detail.code == "DATA_CHECKPOINT_INVALID"
    assert source.fetch_calls == 1


def test_validate_and_publish_commands_resume_latest_compatible_run(
    tmp_path: Path,
) -> None:
    """Re-running earlier stages would defeat explicit recovery commands."""
    quality = FailOnceQualityRunner()
    source = OfflineBaoStockSource()
    pipeline, _ = make_pipeline(tmp_path, source, quality_runner=quality)
    with pytest.raises(RuntimeError, match="simulated quality crash"):
        pipeline.bootstrap()

    validated = pipeline.validate_latest()
    published = pipeline.publish_latest()

    assert validated["status"] == "VALIDATED"
    assert isinstance(published.snapshot_id, SnapshotId)
    assert source.fetch_calls == 1


def test_publish_command_reuses_successful_validation_checkpoint(
    tmp_path: Path,
) -> None:
    source = OfflineBaoStockSource()
    pipeline, _ = make_pipeline(
        tmp_path,
        source,
        publisher_factory=lambda repository, root: FailOnceSnapshotPublisher(
            repository, root
        ),
    )
    with pytest.raises(RuntimeError, match="simulated publish crash"):
        pipeline.bootstrap()

    published = pipeline.publish_latest()

    assert isinstance(published.snapshot_id, SnapshotId)
    assert source.fetch_calls == 1


def test_blocking_quality_issue_does_not_publish_snapshot(tmp_path: Path) -> None:
    """Publishing despite negative volume would violate the quality gate."""
    pipeline, repository = make_pipeline(tmp_path, OfflineBaoStockSource(volume="-100"))

    with pytest.raises(QuantError) as captured:
        pipeline.bootstrap()

    assert captured.value.detail.code == "SNAP_QUALITY_BLOCKED"
    assert repository.count_snapshots() == 0


def test_update_merges_full_history_and_reuses_untouched_year_partition(
    tmp_path: Path,
) -> None:
    """Replacing a snapshot with only the overlap window would drop the 2025 row."""
    bootstrap, repository = make_pipeline(
        tmp_path, OfflineBaoStockSource(bar_date="2025-12-31", close_price="10.10")
    )
    first = bootstrap.bootstrap()
    first_daily = repository.get_dataset_version(first.dataset_versions["daily_bar"])
    old_partition = next(
        item for item in first_daily.partitions if "year=2025" in item.path.as_posix()
    )

    update, _ = make_pipeline(
        tmp_path, OfflineBaoStockSource(bar_date="2026-01-05", close_price="10.50")
    )
    second = update.update()
    second_daily = repository.get_dataset_version(second.dataset_versions["daily_bar"])
    frames = CuratedPartitionStore(tmp_path / "data" / "curated").read_version(
        second_daily
    )
    complete = pl.concat(frames).sort("trade_date")

    assert complete.get_column("trade_date").to_list() == [
        date(2025, 12, 31),
        date(2026, 1, 5),
    ]
    assert second_daily.start_date == date(2006, 1, 4)
    assert second_daily.end_date == date(2026, 1, 6)
    assert old_partition.path in {item.path for item in second_daily.partitions}


def test_curated_store_rejects_catalog_paths_outside_its_root(tmp_path: Path) -> None:
    """A forged catalog path must not make arbitrary Parquet trusted Curated data."""
    pipeline, repository = make_pipeline(tmp_path, OfflineBaoStockSource())
    result = pipeline.bootstrap()
    version = repository.get_dataset_version(result.dataset_versions["daily_bar"])
    original = version.partitions[0]
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(original.path.read_bytes())
    forged = replace(
        version,
        partitions=(replace(original, path=outside),),
    )

    with pytest.raises(ValueError, match="outside curated root"):
        CuratedPartitionStore(tmp_path / "data" / "curated").read_version(forged)


class FakeCliPipeline:
    def __init__(self) -> None:
        self.update_args: tuple[date | None, date | None] | None = None
        self.result = PipelineResult(
            run_id="run-1",
            snapshot_id=SnapshotId(UUID("00000000-0000-0000-0000-000000000001")),
            quality_run_id=QualityRunId(UUID("00000000-0000-0000-0000-000000000002")),
            dataset_versions={},
        )

    def bootstrap(self) -> PipelineResult:
        return self.result

    def update(self, *, start: date | None, end: date | None) -> PipelineResult:
        self.update_args = (start, end)
        return self.result

    def validate_latest(self) -> PipelineResult:
        return self.result

    def publish_latest(self) -> PipelineResult:
        return self.result


class FakeSnapshotRepository:
    def __init__(self, records: tuple[SnapshotRecord, ...] = ()) -> None:
        self._records = records

    def list_snapshots(self) -> tuple[SnapshotRecord, ...]:
        return self._records


def test_cli_emits_json_success_and_rejects_partial_manual_window() -> None:
    """Plain-text or partial arguments would make automation ambiguous."""
    pipeline = FakeCliPipeline()
    app = create_app(
        lambda: ApplicationServices(
            pipeline=pipeline,  # type: ignore[arg-type]
            repository=FakeSnapshotRepository(),  # type: ignore[arg-type]
        )
    )
    runner = CliRunner()

    success = runner.invoke(
        app,
        ["data", "update", "--start", "2026-01-02", "--end", "2026-01-05"],
    )
    failure = runner.invoke(app, ["data", "update", "--start", "2026-01-02"])

    assert success.exit_code == 0
    assert json.loads(success.stdout) == {
        "dataset_versions": {},
        "quality_run_id": "00000000-0000-0000-0000-000000000002",
        "run_id": "run-1",
        "snapshot_id": "00000000-0000-0000-0000-000000000001",
        "status": "SUCCEEDED",
    }
    assert pipeline.update_args == (date(2026, 1, 2), date(2026, 1, 5))
    assert failure.exit_code != 0
    assert json.loads(failure.stderr)["error"]["code"] == "DATA_PIPELINE_ARGUMENT"


def test_cli_snapshots_is_read_only_json() -> None:
    def record(status: SnapshotStatus, suffix: int) -> SnapshotRecord:
        return SnapshotRecord(
            id=SnapshotId(UUID(f"00000000-0000-0000-0000-{suffix:012d}")),
            publication_fingerprint=str(suffix) * 64,
            as_of=datetime(2026, 1, 6, tzinfo=UTC),
            status=status,
            manifest_path=Path("manifest.json"),
            manifest_hash=str(suffix) * 64,
            quality_run_id=QualityRunId(UUID(f"10000000-0000-0000-0000-{suffix:012d}")),
            dataset_versions={},
            created_at=datetime(2026, 1, 6, tzinfo=UTC),
            published_at=(
                datetime(2026, 1, 6, tzinfo=UTC)
                if status is SnapshotStatus.PUBLISHED
                else None
            ),
        )

    app = create_app(
        lambda: ApplicationServices(
            pipeline=FakeCliPipeline(),  # type: ignore[arg-type]
            repository=FakeSnapshotRepository(
                (record(SnapshotStatus.DRAFT, 1), record(SnapshotStatus.PUBLISHED, 2))
            ),  # type: ignore[arg-type]
        )
    )

    result = CliRunner().invoke(app, ["data", "snapshots"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "snapshots": [
            {
                "as_of": "2026-01-06T00:00:00+00:00",
                "datasets": {},
                "quality_run_id": "10000000-0000-0000-0000-000000000002",
                "snapshot_id": "00000000-0000-0000-0000-000000000002",
                "status": "PUBLISHED",
            }
        ]
    }
