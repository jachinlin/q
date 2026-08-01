"""Offline integration coverage for the reproducible data pipeline."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import threading
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import UUID
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from typer.testing import CliRunner

from quant_core import cli as cli_module
from quant_core.cli import ApplicationServices, create_app
from quant_core.data.contracts import (
    CanonicalBatch,
    PublishedPartition,
    RawBatch,
    canonical_json_bytes,
)
from quant_core.data.mappers.baostock import BAOSTOCK_MAPPER_VERSION, BaoStockMapper
from quant_core.data.partitions import RawPartitionStore
from quant_core.data.pipelines import curate as curate_module
from quant_core.data.pipelines import publish as pipeline_module
from quant_core.data.pipelines.curate import CuratedPartitionStore
from quant_core.data.pipelines.ingest import partition_from_json, partition_to_json
from quant_core.data.pipelines.publish import (
    DataPipeline,
    PipelineResult,
    PipelineVersions,
)
from quant_core.data.quality.rules import QUALITY_RULE_SET_VERSION
from quant_core.data.quality.runner import QualityRunner
from quant_core.data.snapshots import SnapshotPublisher
from quant_core.data.sources.baostock import (
    BAOSTOCK_SOURCE_ADAPTER_VERSION,
    BaoStockConfig,
)
from quant_core.domain.enums import DatasetKind, SnapshotStatus
from quant_core.domain.identifiers import DatasetVersionId, QualityRunId, SnapshotId
from quant_core.errors import QuantError
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.persistence.repositories import (
    MetadataRepository,
    PipelineRunSpec,
    PipelineStageName,
    SnapshotRecord,
)


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
    claim = repository.start_pipeline_stage(
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
        owner_id="legacy-owner",
        attempt=claim.attempt,
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
    claim = repository.start_pipeline_stage(
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
        owner_id="legacy-owner",
        attempt=claim.attempt,
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
        self.daily_market_calls: list[date] = []
        self.selected_calls = 0

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
        daily_rows = self.query_daily_history_k_AStock(
            date.fromisoformat(self.bar_date)
        )
        catalog_ids = ["SSE:600000"]
        response_codes = sorted(str(row["code"]) for row in daily_rows)
        yield RawBatch(
            provider=self.provider,
            dataset="daily_bars",
            request={
                "api": "query_daily_history_k_AStock",
                "scope": "ALL",
                "date": self.bar_date,
                "frequency": "d",
                "catalog_instrument_count": len(catalog_ids),
                "catalog_instruments_sha256": hashlib.sha256(
                    "\n".join(catalog_ids).encode()
                ).hexdigest(),
                "response_instrument_count": len(response_codes),
                "response_instruments_sha256": hashlib.sha256(
                    "\n".join(response_codes).encode()
                ).hexdigest(),
            },
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
            rows=daily_rows,
        )

    def query_daily_history_k_AStock(
        self, trading_day: date
    ) -> tuple[dict[str, str], ...]:
        self.daily_market_calls.append(trading_day)
        return (
            {
                "date": trading_day.isoformat(),
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
        )

    def query_history_k_data_plus(self) -> tuple[dict[str, str], ...]:
        self.selected_calls += 1
        return ()


class OfflineDailyMarketBaoStockSource(OfflineBaoStockSource):
    """Named fixture documenting that pipeline bootstrap uses the daily route."""


class FailOnceMapper:
    def __init__(self) -> None:
        self._delegate = BaoStockMapper()
        self.calls = 0

    def normalize(self, raw_partition: PublishedPartition) -> Iterable[CanonicalBatch]:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("simulated mapper crash")
        return self._delegate.normalize(raw_partition)


class RetrievalTimeBaoStockMapper:
    """Emulate the pre-fix v1 mapper's daily availability semantics."""

    def __init__(self) -> None:
        self._delegate = BaoStockMapper()
        self.calls = 0

    def normalize(self, raw_partition: PublishedPartition) -> Iterable[CanonicalBatch]:
        self.calls += 1
        for batch in self._delegate.normalize(raw_partition):
            if batch.dataset in {
                DatasetKind.DAILY_BAR,
                DatasetKind.SECURITY_STATUS,
            }:
                yield replace(
                    batch,
                    frame=batch.frame.with_columns(
                        pl.lit(raw_partition.retrieved_at).alias("available_at"),
                        pl.lit("RAW_RETRIEVED_AT").alias("availability_source"),
                    ),
                )
            else:
                yield batch


class CountingBaoStockMapper:
    def __init__(self) -> None:
        self._delegate = BaoStockMapper()
        self.calls = 0

    def normalize(self, raw_partition: PublishedPartition) -> Iterable[CanonicalBatch]:
        self.calls += 1
        return self._delegate.normalize(raw_partition)


class FailAfterFirstRawSource(OfflineBaoStockSource):
    """Fail one Raw attempt after publication and timestamp retries from a clock."""

    def __init__(self, clock: Callable[[], datetime]) -> None:
        super().__init__()
        self._clock = clock
        self.attempts = 0

    def fetch_range(self, start: date, end: date) -> Iterable[RawBatch]:
        self.attempts += 1
        attempt = self.attempts
        for batch in super().fetch_range(start, end):
            yield replace(batch, retrieved_at=self._clock())
            if attempt == 1:
                raise RuntimeError("simulated Raw collection crash")


def make_pipeline(
    tmp_path: Path,
    source: OfflineBaoStockSource,
    *,
    mapper: object | None = None,
    quality_runner: object | None = None,
    publisher_factory: Callable[[MetadataRepository, Path], object] | None = None,
    calendar: object | None = None,
    versions: object | None = None,
    curated_store: object | None = None,
    clock: Callable[[], datetime] = lambda: datetime(2026, 1, 6, tzinfo=UTC),
    lease_duration: timedelta = timedelta(minutes=30),
    heartbeat_interval: timedelta | None = None,
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
        calendar=calendar or FixedCalendarPolicy(),  # type: ignore[arg-type]
        raw_store=RawPartitionStore(tmp_path / "data" / "raw"),
        curated_store=curated_store  # type: ignore[arg-type]
        or CuratedPartitionStore(tmp_path / "data" / "curated"),
        repository=repository,
        quality_runner=quality_runner or QualityRunner(),  # type: ignore[arg-type]
        snapshot_publisher=publisher,  # type: ignore[arg-type]
        **({"versions": versions} if versions is not None else {}),
        clock=clock,
        lease_duration=lease_duration,
        **(
            {"heartbeat_interval": heartbeat_interval}
            if heartbeat_interval is not None
            else {}
        ),
    )
    return pipeline, repository


class FailOnceQualityRunner:
    def __init__(self) -> None:
        self._delegate = QualityRunner()
        self.calls = 0

    def evaluate(self, inputs: object, **kwargs: object) -> object:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("simulated quality crash")
        return self._delegate.evaluate(inputs, **kwargs)  # type: ignore[arg-type]


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


def test_pipeline_recovers_same_run_after_partial_raw_publish_and_clock_advance(
    tmp_path: Path,
) -> None:
    """A missing Raw checkpoint must not make a stable request conflict on retry."""
    now = [datetime(2026, 1, 6, 23, 59, tzinfo=UTC)]
    clock = lambda: now[0]
    source = FailAfterFirstRawSource(clock)
    pipeline, repository = make_pipeline(tmp_path, source, clock=clock)

    with pytest.raises(RuntimeError, match="simulated Raw collection crash"):
        pipeline.bootstrap()
    now[0] = datetime(2026, 1, 7, 0, 1, tzinfo=UTC)

    recovered = pipeline.bootstrap()

    checkpoint = repository.get_pipeline_stage(
        recovered.run_id, PipelineStageName.INGEST_RAW
    )
    assert isinstance(checkpoint.output, Mapping)
    partitions = checkpoint.output["partitions"]
    assert isinstance(partitions, tuple)
    first = partitions[0]
    assert isinstance(first, Mapping)
    assert first["retrieved_at"] == "2026-01-06T23:59:00+00:00"
    assert first["ingest_date"] == "2026-01-06"
    assert isinstance(recovered.snapshot_id, SnapshotId)
    assert source.fetch_calls == 2
    assert len(list((tmp_path / "data" / "raw").rglob("*.parquet"))) == 3


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


def test_raw_checkpoint_rebuilds_expected_path_from_root(tmp_path: Path) -> None:
    """Serialized absolute paths cannot redirect Raw checkpoint recovery."""
    raw_root = tmp_path / "raw"
    published = RawPartitionStore(raw_root).publish(
        next(OfflineBaoStockSource().fetch_range(date(2026, 1, 5), date(2026, 1, 5))),
        run_id="run-1",
    )
    serialized = partition_to_json(published)
    victim = tmp_path / "outside.parquet"
    victim.write_bytes(published.data_path.read_bytes())
    serialized["data_path"] = victim.as_posix()

    with pytest.raises(ValueError, match="path"):
        partition_from_json(serialized, raw_root)


def test_curated_publish_rejects_real_directory_link_escape(
    tmp_path: Path,
) -> None:
    """A dataset junction must not redirect Curated files outside its root."""
    curated_root = tmp_path / "data" / "curated"
    curated_root.mkdir(parents=True)
    outside = tmp_path / "outside-curated"
    outside.mkdir()
    victim = outside / "victim.txt"
    victim.write_text("untouched", encoding="utf-8")
    _create_directory_link(curated_root / "dataset=daily_bar", outside)
    pipeline, repository = make_pipeline(tmp_path, OfflineBaoStockSource())

    with pytest.raises((QuantError, ValueError)):
        pipeline.bootstrap()

    assert victim.read_text(encoding="utf-8") == "untouched"
    assert sorted(path.name for path in outside.iterdir()) == ["victim.txt"]
    assert repository.count_snapshots() == 0


def test_curated_checkpoint_wraps_corrupt_parquet_as_checkpoint_error(
    tmp_path: Path,
) -> None:
    pipeline, repository = make_pipeline(tmp_path, OfflineBaoStockSource())
    result = pipeline.bootstrap()
    version = repository.get_dataset_version(result.dataset_versions["daily_bar"])
    version.partitions[0].path.write_bytes(b"corrupt")

    with pytest.raises(QuantError) as captured:
        pipeline.bootstrap()

    assert captured.value.detail.code == "DATA_CHECKPOINT_INVALID"
    assert captured.value.detail.context["stage"] == "CURATE"


def test_quality_checkpoint_wraps_repository_failure_as_checkpoint_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline, repository = make_pipeline(tmp_path, OfflineBaoStockSource())
    pipeline.bootstrap()
    monkeypatch.setattr(
        repository,
        "get_quality_run",
        lambda _: (_ for _ in ()).throw(OSError("quality catalog unavailable")),
    )

    with pytest.raises(QuantError) as captured:
        pipeline.bootstrap()

    assert captured.value.detail.code == "DATA_CHECKPOINT_INVALID"
    assert captured.value.detail.context["stage"] == "VALIDATE"


def test_publish_checkpoint_revalidates_manifest_content(tmp_path: Path) -> None:
    pipeline, repository = make_pipeline(tmp_path, OfflineBaoStockSource())
    result = pipeline.bootstrap()
    manifest = repository.get_snapshot(result.snapshot_id).manifest_path
    manifest.write_text("{}", encoding="utf-8")

    with pytest.raises(QuantError) as captured:
        pipeline.bootstrap()

    assert captured.value.detail.code == "DATA_CHECKPOINT_INVALID"
    assert captured.value.detail.context["stage"] == "PUBLISH_SNAPSHOT"


def test_baostock_availability_upgrade_does_not_reuse_pre_fix_snapshot(
    tmp_path: Path,
) -> None:
    source = OfflineDailyMarketBaoStockSource()
    old_mapper = RetrievalTimeBaoStockMapper()
    old_versions = PipelineVersions(
        source_adapter="baostock-source-adapter-v2",
        mapper="baostock-canonical-mapper-v1",
    )
    old_pipeline, old_repository = make_pipeline(
        tmp_path,
        source,
        mapper=old_mapper,
        versions=old_versions,
    )
    old = old_pipeline.bootstrap()
    new_mapper = CountingBaoStockMapper()
    new_versions = replace(
        old_versions,
        source_adapter=BAOSTOCK_SOURCE_ADAPTER_VERSION,
        mapper=BAOSTOCK_MAPPER_VERSION,
    )
    new_pipeline, new_repository = make_pipeline(
        tmp_path,
        source,
        mapper=new_mapper,
        versions=new_versions,
    )

    new = new_pipeline.bootstrap()

    expected_available_at = datetime(
        2026, 1, 5, 15, 0, tzinfo=ZoneInfo("Asia/Shanghai")
    ).astimezone(UTC)
    assert new.run_id != old.run_id
    assert new.snapshot_id != old.snapshot_id
    assert source.fetch_calls == 2
    assert old_mapper.calls > 0
    assert new_mapper.calls > 0
    assert new_versions.source_adapter != old_versions.source_adapter
    assert new_versions.mapper != old_versions.mapper
    for dataset in (DatasetKind.DAILY_BAR, DatasetKind.SECURITY_STATUS):
        old_version = old_repository.get_dataset_version(
            old.dataset_versions[dataset.value]
        )
        new_version = new_repository.get_dataset_version(
            new.dataset_versions[dataset.value]
        )
        old_frame = pl.concat(old_pipeline._curated_store.read_version(old_version))
        new_frame = pl.concat(new_pipeline._curated_store.read_version(new_version))
        assert old_frame.select("available_at", "availability_source").rows() == [
            (datetime(2026, 1, 6, tzinfo=UTC), "RAW_RETRIEVED_AT")
        ]
        assert new_frame.select("available_at", "availability_source").rows() == [
            (expected_available_at, "MARKET_CLOSE_DERIVED")
        ]


def test_cli_fetch_config_fingerprints_both_routes_and_selected_limits() -> None:
    config = BaoStockConfig(
        max_instruments_per_batch=17,
        max_days_per_batch=31,
        max_attempts=3,
        retry_backoff_seconds=(0.25, 0.5),
        retryable_error_codes=frozenset({"-1", "10002007"}),
    )
    fingerprint = getattr(cli_module, "_baostock_fetch_config_fingerprint", None)
    assert fingerprint is not None
    expected = {
        "all_market_route": "query_daily_history_k_AStock-per-open-date-v1",
        "selected_route": "query_history_k_data_plus-v1",
        "selected_max_days_per_batch": 31,
        "selected_max_instruments_per_batch": 17,
        "max_attempts": 3,
        "retry_backoff_seconds": [0.25, 0.5],
        "retryable_error_codes": ["-1", "10002007"],
    }

    assert (
        fingerprint(config)
        == hashlib.sha256(canonical_json_bytes(expected)).hexdigest()
    )


def test_bootstrap_all_market_source_never_uses_selected_api(tmp_path: Path) -> None:
    source = OfflineDailyMarketBaoStockSource()
    pipeline, _ = make_pipeline(tmp_path, source)

    result = pipeline.bootstrap()

    assert isinstance(result.snapshot_id, SnapshotId)
    assert source.daily_market_calls == [date(2026, 1, 5)]
    assert source.selected_calls == 0


def test_source_adapter_component_version_change_does_not_reuse_ingest_checkpoint(
    tmp_path: Path,
) -> None:
    source = OfflineDailyMarketBaoStockSource()
    v1 = PipelineVersions(source_adapter="baostock-source-adapter-v1")
    first_pipeline, repository = make_pipeline(tmp_path, source, versions=v1)
    first = first_pipeline.bootstrap()
    v2 = replace(v1, source_adapter="baostock-source-adapter-v2")
    second_pipeline, _ = make_pipeline(tmp_path, source, versions=v2)

    second = second_pipeline.bootstrap()

    first_run = repository.get_pipeline_run(first.run_id)
    second_run = repository.get_pipeline_run(second.run_id)
    first_ingest = repository.get_pipeline_stage(
        first.run_id, PipelineStageName.INGEST_RAW
    )
    second_ingest = repository.get_pipeline_stage(
        second.run_id, PipelineStageName.INGEST_RAW
    )
    assert second.run_id != first.run_id
    assert second_run.request_hash != first_run.request_hash
    assert second_run.pipeline_fingerprint != first_run.pipeline_fingerprint
    assert second_ingest.input_hash != first_ingest.input_hash
    assert source.fetch_calls == 2
    assert source.daily_market_calls == [date(2026, 1, 5), date(2026, 1, 5)]
    assert source.selected_calls == 0


@pytest.mark.parametrize(
    "field",
    [
        "source_adapter",
        "fetch_config",
        "mapper",
        "canonical_schema",
        "quality_rules",
        "snapshot_manifest",
    ],
)
def test_component_version_change_creates_a_distinct_run(
    tmp_path: Path,
    field: str,
) -> None:
    versions_type = getattr(pipeline_module, "PipelineVersions", None)
    assert versions_type is not None, "PipelineVersions is required"
    baseline_versions = versions_type()
    source = OfflineBaoStockSource()
    baseline, _ = make_pipeline(tmp_path, source, versions=baseline_versions)
    first = baseline.bootstrap()
    changed_versions = replace(
        baseline_versions,
        **{field: f"{getattr(baseline_versions, field)}-changed"},
    )
    changed, _ = make_pipeline(tmp_path, source, versions=changed_versions)

    second = changed.bootstrap()

    assert second.run_id != first.run_id
    assert source.fetch_calls == 2


class TushareNamedBaoStockFixture(OfflineBaoStockSource):
    provider = "tushare"


def test_update_rejects_mixing_previous_provider_history(tmp_path: Path) -> None:
    bootstrap, _ = make_pipeline(tmp_path, OfflineBaoStockSource())
    bootstrap.bootstrap()
    source = TushareNamedBaoStockFixture()
    update, _ = make_pipeline(tmp_path, source)

    with pytest.raises(QuantError) as captured:
        update.update()

    assert captured.value.detail.code == "DATA_PIPELINE_PROVIDER_MISMATCH"
    assert source.fetch_calls == 0


class DuplicateDailyBatchSource(OfflineBaoStockSource):
    def fetch_range(self, start: date, end: date) -> Iterable[RawBatch]:
        batches = tuple(super().fetch_range(start, end))
        yield from batches
        daily = next(batch for batch in batches if batch.dataset == "daily_bars")
        yield replace(daily, request={**daily.request, "chunk": 2})


def test_new_canonical_batches_reject_duplicate_primary_keys(tmp_path: Path) -> None:
    pipeline, repository = make_pipeline(tmp_path, DuplicateDailyBatchSource())

    with pytest.raises(QuantError) as captured:
        pipeline.bootstrap()

    assert captured.value.detail.code == "DATA_CANONICAL_PRIMARY_KEY_DUPLICATE"
    assert repository.count_snapshots() == 0


class MissingDailySource(OfflineBaoStockSource):
    def fetch_range(self, start: date, end: date) -> Iterable[RawBatch]:
        yield from (
            batch
            for batch in super().fetch_range(start, end)
            if batch.dataset != "daily_bars"
        )


class ClosedCalendarSource(OfflineBaoStockSource):
    def fetch_range(self, start: date, end: date) -> Iterable[RawBatch]:
        for batch in super().fetch_range(start, end):
            if batch.dataset == "trade_calendar":
                row = dict(batch.rows[0])
                row["is_trading_day"] = "0"
                yield replace(batch, rows=(row,))
            else:
                yield batch


@pytest.mark.parametrize("source", [MissingDailySource(), ClosedCalendarSource()])
def test_required_dataset_or_empty_trading_window_blocks_snapshot(
    tmp_path: Path,
    source: OfflineBaoStockSource,
) -> None:
    pipeline, repository = make_pipeline(tmp_path, source)

    with pytest.raises(QuantError) as captured:
        pipeline.bootstrap()

    assert captured.value.detail.code == "SNAP_QUALITY_BLOCKED"
    assert repository.count_snapshots() == 0


class InvalidCalendarPolicy(FixedCalendarPolicy):
    def bootstrap_window(self, years: int) -> tuple[date, date]:
        raise ValueError("calendar has no complete trading day")


def test_calendar_value_error_is_structured_pipeline_argument(tmp_path: Path) -> None:
    pipeline, _ = make_pipeline(
        tmp_path,
        OfflineBaoStockSource(),
        calendar=InvalidCalendarPolicy(),
    )

    with pytest.raises(QuantError) as captured:
        pipeline.bootstrap()

    assert captured.value.detail.code == "DATA_PIPELINE_ARGUMENT"


def test_update_does_not_read_or_write_untouched_year_partition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bootstrap, repository = make_pipeline(
        tmp_path, OfflineBaoStockSource(bar_date="2025-12-31")
    )
    first = bootstrap.bootstrap()
    daily = repository.get_dataset_version(first.dataset_versions["daily_bar"])
    untouched = next(
        partition
        for partition in daily.partitions
        if "year=2025" in str(partition.path)
    )
    reads: list[Path] = []
    writes: list[Path] = []
    original_read = curate_module.pq.read_table
    original_write = curate_module.pq.write_table

    def track_read(path: object, *args: object, **kwargs: object) -> object:
        candidate = Path(str(path))
        if "year=2025" in candidate.as_posix():
            reads.append(candidate)
        return original_read(path, *args, **kwargs)

    def track_write(
        table: object, path: object, *args: object, **kwargs: object
    ) -> None:
        candidate = Path(str(path))
        if "year=2025" in candidate.as_posix():
            writes.append(candidate)
        original_write(table, path, *args, **kwargs)

    monkeypatch.setattr(curate_module.pq, "read_table", track_read)
    monkeypatch.setattr(curate_module.pq, "write_table", track_write)
    update, _ = make_pipeline(tmp_path, OfflineBaoStockSource(bar_date="2026-01-05"))
    update.update()

    assert reads == []
    assert writes == []
    assert untouched.path.exists()


class ObservingCuratedStore:
    def __init__(self, root: Path) -> None:
        self._delegate = CuratedPartitionStore(root)
        self.received_tuple: bool | None = None

    @property
    def root(self) -> Path:
        return self._delegate.root

    def publish(self, batches: object, **kwargs: object) -> object:
        self.received_tuple = isinstance(batches, tuple)
        return self._delegate.publish(batches, **kwargs)  # type: ignore[arg-type]

    def read_version(self, record: object) -> object:
        return self._delegate.read_version(record)  # type: ignore[arg-type]


class LazyAssertingQualityRunner:
    def evaluate(self, inputs: object, **kwargs: object) -> object:
        assert isinstance(inputs, Mapping)
        assert all(
            isinstance(frame, pl.LazyFrame)
            for partitions in inputs.values()
            for frame in partitions
        )
        return QualityRunner().evaluate(inputs, **kwargs)  # type: ignore[arg-type]


def test_pipeline_streams_mapper_batches_and_quality_uses_lazy_partitions(
    tmp_path: Path,
) -> None:
    store = ObservingCuratedStore(tmp_path / "data" / "curated")
    pipeline, _ = make_pipeline(
        tmp_path,
        OfflineBaoStockSource(),
        curated_store=store,
        quality_runner=LazyAssertingQualityRunner(),
    )

    pipeline.bootstrap()

    assert store.received_tuple is False


def test_stage_claim_uses_owner_and_lease_cas(tmp_path: Path) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    repository = MetadataRepository(create_sqlite_engine(database))
    now = datetime(2026, 1, 6, tzinfo=UTC)
    run = repository.register_pipeline_run(
        PipelineRunSpec(
            mode="BOOTSTRAP",
            provider="baostock",
            request_hash="a" * 64,
            requested_start=None,
            requested_end=None,
            resolved_start=date(2006, 1, 4),
            resolved_end=date(2026, 1, 5),
            created_at=now,
        )
    )
    repository.start_pipeline_stage(
        run.id,
        PipelineStageName.INGEST_RAW,
        input_hash="b" * 64,
        started_at=now,
        owner_id="owner-a",
        lease_expires_at=now + timedelta(minutes=1),
    )

    with pytest.raises(QuantError) as busy:
        repository.start_pipeline_stage(
            run.id,
            PipelineStageName.INGEST_RAW,
            input_hash="b" * 64,
            started_at=now + timedelta(seconds=10),
            owner_id="owner-b",
            lease_expires_at=now + timedelta(minutes=2),
        )

    assert busy.value.detail.code == "DATA_PIPELINE_BUSY"
    taken = repository.start_pipeline_stage(
        run.id,
        PipelineStageName.INGEST_RAW,
        input_hash="b" * 64,
        started_at=now + timedelta(minutes=2),
        owner_id="owner-b",
        lease_expires_at=now + timedelta(minutes=3),
    )
    assert taken.owner_id == "owner-b"


def test_stale_stage_owner_cannot_complete_or_fail_after_takeover(
    tmp_path: Path,
) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    repository = MetadataRepository(create_sqlite_engine(database))
    now = datetime(2026, 1, 6, tzinfo=UTC)
    run = repository.register_pipeline_run(
        PipelineRunSpec(
            mode="BOOTSTRAP",
            provider="baostock",
            request_hash="8" * 64,
            requested_start=None,
            requested_end=None,
            resolved_start=date(2006, 1, 4),
            resolved_end=date(2026, 1, 5),
            created_at=now,
        )
    )
    old = repository.start_pipeline_stage(
        run.id,
        PipelineStageName.INGEST_RAW,
        input_hash="9" * 64,
        started_at=now,
        owner_id="owner-old",
        lease_expires_at=now + timedelta(minutes=1),
    )
    new = repository.start_pipeline_stage(
        run.id,
        PipelineStageName.INGEST_RAW,
        input_hash="9" * 64,
        started_at=now + timedelta(minutes=2),
        owner_id="owner-new",
        lease_expires_at=now + timedelta(minutes=3),
    )

    with pytest.raises(QuantError) as completing:
        repository.complete_pipeline_stage(
            run.id,
            PipelineStageName.INGEST_RAW,
            input_hash="9" * 64,
            output_hash="a" * 64,
            output={"owner": "old"},
            completed_at=now + timedelta(minutes=2),
            owner_id="owner-old",
            attempt=old.attempt,
        )
    with pytest.raises(QuantError) as failing:
        repository.fail_pipeline_stage(
            run.id,
            PipelineStageName.INGEST_RAW,
            input_hash="9" * 64,
            error={"owner": "old"},
            completed_at=now + timedelta(minutes=2),
            owner_id="owner-old",
            attempt=old.attempt,
        )

    current = repository.get_pipeline_stage(run.id, PipelineStageName.INGEST_RAW)
    assert completing.value.detail.code == "DATA_PIPELINE_BUSY"
    assert failing.value.detail.code == "DATA_PIPELINE_BUSY"
    assert current.status == "RUNNING"
    assert current.owner_id == "owner-new"
    assert current.attempt == new.attempt == old.attempt + 1


def test_stage_lease_renewal_is_fenced_by_owner_and_attempt(tmp_path: Path) -> None:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    repository = MetadataRepository(create_sqlite_engine(database))
    now = datetime(2026, 1, 6, tzinfo=UTC)
    run = repository.register_pipeline_run(
        PipelineRunSpec(
            mode="BOOTSTRAP",
            provider="baostock",
            request_hash="b" * 64,
            requested_start=None,
            requested_end=None,
            resolved_start=date(2006, 1, 4),
            resolved_end=date(2026, 1, 5),
            created_at=now,
        )
    )
    claim = repository.start_pipeline_stage(
        run.id,
        PipelineStageName.CURATE,
        input_hash="c" * 64,
        started_at=now,
        owner_id="owner-a",
        lease_expires_at=now + timedelta(minutes=1),
    )

    renewed = repository.renew_pipeline_stage_lease(
        run.id,
        PipelineStageName.CURATE,
        owner_id="owner-a",
        attempt=claim.attempt,
        renewed_at=now + timedelta(seconds=30),
        lease_expires_at=now + timedelta(minutes=3),
    )
    with pytest.raises(QuantError) as stale:
        repository.renew_pipeline_stage_lease(
            run.id,
            PipelineStageName.CURATE,
            owner_id="owner-a",
            attempt=claim.attempt + 1,
            renewed_at=now + timedelta(seconds=40),
            lease_expires_at=now + timedelta(minutes=4),
        )
    with pytest.raises(QuantError) as busy:
        repository.start_pipeline_stage(
            run.id,
            PipelineStageName.CURATE,
            input_hash="c" * 64,
            started_at=now + timedelta(minutes=2),
            owner_id="owner-b",
            lease_expires_at=now + timedelta(minutes=4),
        )

    assert renewed.lease_expires_at == now + timedelta(minutes=3)
    assert stale.value.detail.code == "DATA_PIPELINE_BUSY"
    assert busy.value.detail.code == "DATA_PIPELINE_BUSY"


def test_default_pipeline_versions_track_actual_quality_rule_set() -> None:
    assert PipelineVersions().quality_rules == QUALITY_RULE_SET_VERSION


def _register_newer_failed_ingest(
    repository: MetadataRepository,
    *,
    created_at: datetime,
    request_hash: str,
    pipeline_fingerprint: str,
) -> None:
    run = repository.register_pipeline_run(
        PipelineRunSpec(
            mode="BOOTSTRAP",
            provider="baostock",
            request_hash=request_hash,
            requested_start=None,
            requested_end=None,
            resolved_start=date(2006, 1, 4),
            resolved_end=date(2026, 1, 5),
            created_at=created_at,
            pipeline_fingerprint=pipeline_fingerprint,
        )
    )
    claim = repository.start_pipeline_stage(
        run.id,
        PipelineStageName.INGEST_RAW,
        input_hash="c" * 64,
        started_at=created_at,
    )
    repository.fail_pipeline_stage(
        run.id,
        PipelineStageName.INGEST_RAW,
        input_hash="c" * 64,
        error={"code": "newer-ingest-failed"},
        completed_at=created_at,
        owner_id="legacy-owner",
        attempt=claim.attempt,
    )


def test_validate_latest_selects_curated_ready_run_not_newer_ingest_failure(
    tmp_path: Path,
) -> None:
    pipeline, repository = make_pipeline(
        tmp_path,
        OfflineBaoStockSource(),
        quality_runner=FailOnceQualityRunner(),
    )
    with pytest.raises(RuntimeError, match="simulated quality crash"):
        pipeline.bootstrap()
    older = repository.latest_recoverable_pipeline_run("baostock")
    assert older is not None
    _register_newer_failed_ingest(
        repository,
        created_at=datetime(2026, 1, 6, 0, 1, tzinfo=UTC),
        request_hash="d" * 64,
        pipeline_fingerprint=older.pipeline_fingerprint,
    )

    validated = pipeline.validate_latest()

    assert validated["run_id"] == older.id


def test_publish_latest_selects_validate_ready_run_not_newer_ingest_failure(
    tmp_path: Path,
) -> None:
    pipeline, repository = make_pipeline(
        tmp_path,
        OfflineBaoStockSource(),
        publisher_factory=lambda repo, root: FailOnceSnapshotPublisher(repo, root),
    )
    with pytest.raises(RuntimeError, match="simulated publish crash"):
        pipeline.bootstrap()
    older = repository.latest_recoverable_pipeline_run("baostock")
    assert older is not None
    _register_newer_failed_ingest(
        repository,
        created_at=datetime(2026, 1, 6, 0, 1, tzinfo=UTC),
        request_hash="e" * 64,
        pipeline_fingerprint=older.pipeline_fingerprint,
    )

    published = pipeline.publish_latest()

    assert published.run_id == older.id


def test_validate_latest_wraps_corrupt_curated_checkpoint(tmp_path: Path) -> None:
    pipeline, repository = make_pipeline(
        tmp_path,
        OfflineBaoStockSource(),
        quality_runner=FailOnceQualityRunner(),
    )
    with pytest.raises(RuntimeError, match="simulated quality crash"):
        pipeline.bootstrap()
    run = repository.latest_recoverable_pipeline_run("baostock")
    assert run is not None
    checkpoint = repository.get_pipeline_stage(run.id, PipelineStageName.CURATE)
    assert isinstance(checkpoint.output, Mapping)
    versions = checkpoint.output["dataset_versions"]
    assert isinstance(versions, Mapping)
    daily = repository.get_dataset_version(
        DatasetVersionId.parse(str(versions["daily_bar"]))
    )
    daily.partitions[0].path.write_bytes(b"corrupt")

    with pytest.raises(QuantError) as captured:
        pipeline.validate_latest()

    assert captured.value.detail.code == "DATA_CHECKPOINT_INVALID"


class LeaseCoordinatedSource(OfflineBaoStockSource):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.second_collector_entered = threading.Event()
        self._entry_lock = threading.Lock()
        self._entries = 0

    def fetch_range(self, start: date, end: date) -> Iterable[RawBatch]:
        with self._entry_lock:
            self._entries += 1
            if self._entries > 1:
                self.second_collector_entered.set()
        self.fetch_calls += 1
        self.entered.set()
        assert self.release.wait(timeout=5)
        self.fetch_calls -= 1
        yield from super().fetch_range(start, end)


def test_concurrent_identical_request_has_one_collector_and_busy_follower(
    tmp_path: Path,
) -> None:
    source = LeaseCoordinatedSource()
    first, _ = make_pipeline(tmp_path, source)
    second, _ = make_pipeline(tmp_path, source)
    with ThreadPoolExecutor(max_workers=2) as executor:
        leader = executor.submit(first.bootstrap)
        assert source.entered.wait(timeout=5)
        follower = executor.submit(second.bootstrap)
        with pytest.raises(QuantError) as captured:
            follower.result(timeout=5)
        source.release.set()
        result = leader.result(timeout=10)

    assert captured.value.detail.code == "DATA_PIPELINE_BUSY"
    assert isinstance(result.snapshot_id, SnapshotId)
    assert source.fetch_calls == 1


def test_pipeline_renews_every_long_running_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pipeline, repository = make_pipeline(tmp_path, OfflineBaoStockSource())
    original = repository.renew_pipeline_stage_lease
    renewed_stages: list[PipelineStageName] = []

    def observe_renewal(
        run_id: str, stage: PipelineStageName, **kwargs: object
    ) -> object:
        renewed_stages.append(stage)
        return original(run_id, stage, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(repository, "renew_pipeline_stage_lease", observe_renewal)

    pipeline.bootstrap()

    assert set(renewed_stages) == set(PipelineStageName)
    assert renewed_stages.count(PipelineStageName.INGEST_RAW) >= 3
    assert renewed_stages.count(PipelineStageName.VALIDATE) >= 6


def test_background_heartbeat_covers_source_work_before_first_yield(
    tmp_path: Path,
) -> None:
    source = LeaseCoordinatedSource()
    options = {
        "clock": lambda: datetime.now(UTC),
        "lease_duration": timedelta(milliseconds=600),
        "heartbeat_interval": timedelta(milliseconds=20),
    }
    first, _ = make_pipeline(tmp_path, source, **options)  # type: ignore[arg-type]
    second, _ = make_pipeline(tmp_path, source, **options)  # type: ignore[arg-type]

    with ThreadPoolExecutor(max_workers=2) as executor:
        leader = executor.submit(first.bootstrap)
        assert source.entered.wait(timeout=5)
        threading.Event().wait(0.8)
        follower = executor.submit(second.bootstrap)
        try:
            with pytest.raises(QuantError) as captured:
                follower.result(timeout=5)
            assert captured.value.detail.code == "DATA_PIPELINE_BUSY"
            assert source.second_collector_entered.is_set() is False
        finally:
            source.release.set()
        result = leader.result(timeout=10)

    assert isinstance(result.snapshot_id, SnapshotId)


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
