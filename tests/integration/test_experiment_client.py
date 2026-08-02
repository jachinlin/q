"""Integration coverage for the notebook experiment client."""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import yaml
from sqlalchemy import Engine, func, insert, select, text, update

from quant_core.backtest.artifacts import (
    ExperimentArtifactPublication,
    publish_experiment_artifacts,
)
from quant_core.backtest.engine import StrategyRef
from quant_core.backtest.rulebook import AShareRuleBook
from quant_core.data.contracts import (
    JsonValue,
    ProviderCapabilities,
    canonical_json_bytes,
)
from quant_core.data.repository import SnapshotResearchRepository
from quant_core.data.sources.baostock import BaoStockSdkGateway
from quant_core.domain.enums import DatasetKind, SnapshotStatus
from quant_core.domain.identifiers import (
    DatasetVersionId,
    QualityRunId,
    SnapshotId,
)
from quant_core.experiments import ExperimentClient
from quant_core.experiments.fingerprint import (
    ExperimentFingerprintInput,
    SourceTreeSpec,
    capture_environment,
    compute_fingerprint,
)
from quant_core.experiments.models import ExperimentSpec, ExperimentStatus
from quant_core.experiments.query import ExperimentQuery
from quant_core.experiments.registry import ExperimentRegistry
from quant_core.experiments.runner import (
    ExperimentRunner,
)
from quant_core.experiments.runtime import (
    ExperimentRuntimeFactory,
    build_experiment_worker,
)
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.persistence.orm import (
    ExperimentArtifactORM,
    ExperimentORM,
    QualityRunORM,
    SnapshotORM,
    TaskAttemptORM,
    TaskORM,
)
from quant_core.persistence.repositories import (
    DatasetPartitionRecord,
    DatasetVersionRecord,
    MetadataRepository,
    SnapshotRecord,
)
from quant_core.tasks.models import TaskRecord, TaskStatus
from quant_core.tasks.queue import TaskQueue
from quant_core.tasks.worker import Worker
from tests.integration.test_experiment_artifact_contract import (
    _CONFIG as ARTIFACT_CONFIG,
)
from tests.integration.test_experiment_artifact_contract import (
    _EXPERIMENT as ARTIFACT_EXPERIMENT,
)
from tests.integration.test_experiment_artifact_contract import _prepared_bundle
from tests.integration.test_experiment_runtime import (
    _published_offline_snapshot,
    _resolved_etf_config,
)

_NOW = datetime(2026, 8, 3, 7, tzinfo=UTC)
_STRATEGY = StrategyRef("stock_multifactor", "1.0.0")
_SNAPSHOT_ID = SnapshotId.parse("00000000-0000-0000-0000-000000000681")
_DAILY_VERSION_ID = DatasetVersionId.parse("00000000-0000-0000-0000-000000000682")
_QUALITY_RUN_ID = QualityRunId.parse("00000000-0000-0000-0000-000000000683")


class _UnusedCatalog:
    def latest_snapshot(self) -> None:
        raise AssertionError("catalog must not be queried")


class _Catalog:
    def __init__(
        self,
        snapshot: SnapshotRecord,
        daily_version: DatasetVersionRecord,
    ) -> None:
        self._snapshot = snapshot
        self._daily_version = daily_version

    def latest_snapshot(self) -> SnapshotRecord | None:
        return self._snapshot

    def get_snapshot(self, identifier: SnapshotId) -> SnapshotRecord:
        if identifier != self._snapshot.id:
            raise KeyError(str(identifier))
        return self._snapshot

    def get_dataset_version(self, identifier: DatasetVersionId) -> DatasetVersionRecord:
        if identifier != self._daily_version.id:
            raise KeyError(str(identifier))
        return self._daily_version


class _UnusedFinalizer:
    def finalize(self, *_: object) -> ExperimentArtifactPublication:
        raise AssertionError("recovery verification must not finalize artifacts")


def _run_independent_worker(
    database: str,
    artifact_root: str,
    environment: dict[str, JsonValue],
) -> None:
    engine = create_sqlite_engine(Path(database))
    try:
        root = Path(artifact_root)
        catalog = MetadataRepository(engine)
        runtime_factory = ExperimentRuntimeFactory(
            catalog=catalog,
            repository=SnapshotResearchRepository(catalog),
            capabilities=ProviderCapabilities.complete(),
            provider="offline-complete-fixture",
            feature_root=root.parent / "features",
            artifact_root=root,
            snapshot_root=root.parent / "snapshots",
            rulebook=_rulebook(),
            enrichment=None,
        )
        processed = build_experiment_worker(
            engine=engine,
            worker_id="spawned-worker",
            runtime_factory=runtime_factory,
            artifact_root=root,
            environment=environment,
        ).run_once()
        if not processed:
            raise RuntimeError("spawned worker did not claim the submitted task")
    finally:
        engine.dispose()


@pytest.fixture
def engine(tmp_path: Path) -> Engine:
    database = tmp_path / "state" / "quant.db"
    upgrade_database(database)
    value = create_sqlite_engine(database)
    yield value
    value.dispose()


def _rulebook() -> AShareRuleBook:
    root = Path(__file__).resolve().parents[2]
    return AShareRuleBook.load(root / "configs" / "rules" / "a_share_v1.yaml")


def _environment() -> dict[str, JsonValue]:
    return {
        "schema_version": 1,
        "source_identity_mode": "source_tree",
        "source_hash": "1" * 64,
        "git_commit": None,
        "source_tree_hash": "2" * 64,
        "working_tree_dirty": False,
        "lockfile_path": "uv.lock",
        "lockfile_hash": "3" * 64,
        "python_version": "3.12.0",
    }


def _catalog(tmp_path: Path) -> _Catalog:
    partition = tmp_path / "daily.parquet"
    partition.write_bytes(b"fixture identity")
    daily = DatasetVersionRecord(
        id=_DAILY_VERSION_ID,
        dataset=DatasetKind.DAILY_BAR,
        fingerprint="8" * 64,
        source="offline-fixture",
        status=SnapshotStatus.PUBLISHED.value,
        partitions=(
            DatasetPartitionRecord(
                content_hash="9" * 64,
                path=partition,
                schema_fingerprint="a" * 64,
                row_count=10,
            ),
        ),
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 31),
        created_run_id="fixture-run",
        created_at=_NOW,
    )
    manifest = tmp_path / "snapshot-manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    snapshot = SnapshotRecord(
        id=_SNAPSHOT_ID,
        publication_fingerprint="b" * 64,
        as_of=_NOW,
        status=SnapshotStatus.PUBLISHED,
        manifest_path=manifest,
        manifest_hash="c" * 64,
        quality_run_id=_QUALITY_RUN_ID,
        dataset_versions={DatasetKind.DAILY_BAR.value: _DAILY_VERSION_ID},
        created_at=_NOW,
        published_at=_NOW,
    )
    return _Catalog(snapshot, daily)


def _client(
    engine: Engine,
    tmp_path: Path,
    *,
    sleeper: Any | None = None,
) -> ExperimentClient:
    return ExperimentClient(
        registry=ExperimentRegistry(engine, clock=lambda: _NOW),
        query=ExperimentQuery(engine),
        queue=TaskQueue(engine, clock=lambda: _NOW),
        config_root=tmp_path / "configs",
        catalog=_UnusedCatalog(),
        strategies={_STRATEGY: object()},
        rulebook=_rulebook(),
        environment_factory=_environment,
        clock=lambda: _NOW,
        sleeper=sleeper,
    )


def _created_experiment(engine: Engine) -> str:
    config: dict[str, JsonValue] = {
        "schema_version": 1,
        "strategy_id": _STRATEGY.strategy_id,
        "strategy_version": _STRATEGY.version,
    }
    config_hash = hashlib.sha256(canonical_json_bytes(config)).hexdigest()
    spec = ExperimentSpec(
        strategy_id=_STRATEGY.strategy_id,
        strategy_version=_STRATEGY.version,
        config=config,
        config_hash=config_hash,
        snapshot_id=None,
        snapshot_manifest_hash="4" * 64,
        source_tree_hash="2" * 64,
        git_commit_hash=None,
        lockfile_hash="3" * 64,
        rulebook_version="a-share-v1",
        fingerprint="5" * 64,
        created_at=_NOW,
    )
    return ExperimentRegistry(engine, clock=lambda: _NOW).create(
        spec,
        spec.fingerprint,
        now=_NOW,
    )


def _cross_process_experiment(
    engine: Engine,
    tmp_path: Path,
    environment: dict[str, JsonValue],
) -> str:
    snapshot = _published_offline_snapshot(MetadataRepository(engine), tmp_path)
    config = _resolved_etf_config(snapshot.id)
    config_hash = hashlib.sha256(canonical_json_bytes(config)).hexdigest()
    fingerprint = compute_fingerprint(
        ExperimentFingerprintInput(
            strategy_id="etf_rotation",
            strategy_version="1.0.0",
            resolved_config=config,
            snapshot_manifest_hash=snapshot.manifest_hash,
            source_hash=str(environment["source_hash"]),
            lockfile_hash=str(environment["lockfile_hash"]),
            rulebook_version="a-share-v1",
        )
    )
    spec = ExperimentSpec(
        strategy_id="etf_rotation",
        strategy_version="1.0.0",
        config=config,
        config_hash=config_hash,
        snapshot_id=str(snapshot.id),
        snapshot_manifest_hash=snapshot.manifest_hash,
        source_tree_hash=str(environment["source_tree_hash"]),
        git_commit_hash=None,
        lockfile_hash=str(environment["lockfile_hash"]),
        rulebook_version="a-share-v1",
        fingerprint=fingerprint,
        created_at=_NOW,
    )
    return ExperimentRegistry(engine, clock=lambda: _NOW).create(
        spec,
        fingerprint,
        now=_NOW,
    )


def _captured_environment(tmp_path: Path) -> dict[str, JsonValue]:
    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "strategy.py").write_text("SIGNAL = 'timeline'\n", encoding="utf-8")
    lockfile = source_root / "uv.lock"
    lockfile.write_text("version = 1\n", encoding="utf-8")
    return capture_environment(
        source_root,
        lockfile,
        source_tree_spec=SourceTreeSpec(
            schema_version=1,
            include=("strategy.py",),
        ),
    )


def _count(engine: Engine, table: type[Any]) -> int:
    with engine.connect() as connection:
        return int(connection.scalar(select(func.count()).select_from(table)) or 0)


def test_submit_atomically_queues_experiment_and_returns_without_running(
    engine: Engine, tmp_path: Path
) -> None:
    experiment_id = _created_experiment(engine)
    client = _client(engine, tmp_path)

    task = client.submit(experiment_id)

    assert isinstance(task, TaskRecord)
    assert task.status is TaskStatus.QUEUED
    assert task.experiment_id == experiment_id
    assert task.payload == {
        "experiment_id": experiment_id,
        "config_hash": ExperimentQuery(engine).get(experiment_id).record.config_hash,
    }
    assert (
        ExperimentQuery(engine).get(experiment_id).record.status
        is ExperimentStatus.QUEUED
    )
    assert _count(engine, TaskAttemptORM) == 0


def test_submit_rolls_back_experiment_and_task_when_final_audit_write_fails(
    engine: Engine, tmp_path: Path
) -> None:
    experiment_id = _created_experiment(engine)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TRIGGER fail_backtest_enqueue_audit "
                "BEFORE INSERT ON audit_event "
                "WHEN NEW.event_type = 'TASK_ENQUEUED' "
                "BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END"
            )
        )

    with pytest.raises(Exception, match="injected audit failure"):
        _client(engine, tmp_path).submit(experiment_id)

    assert (
        ExperimentQuery(engine).get(experiment_id).record.status
        is ExperimentStatus.CREATED
    )
    assert _count(engine, TaskORM) == 0


def test_concurrent_submit_deduplicates_to_one_active_backtest(
    engine: Engine, tmp_path: Path
) -> None:
    experiment_id = _created_experiment(engine)
    client = _client(engine, tmp_path)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(client.submit, experiment_id) for _ in range(2)]
        tasks = [future.result(timeout=10) for future in futures]

    assert tasks[0].id == tasks[1].id
    assert _count(engine, TaskORM) == 1
    detail = ExperimentQuery(engine).get(experiment_id)
    assert detail.record.status is ExperimentStatus.QUEUED
    event_types = [event.event_type for event in detail.audit]
    assert event_types.count("EXPERIMENT_STATE_TRANSITIONED") == 1
    assert event_types.count("TASK_ENQUEUED") == 1
    assert event_types.count("TASK_ENQUEUE_DEDUPLICATED") == 1


def test_submit_deduplicates_to_the_same_task_after_execution_starts(
    engine: Engine, tmp_path: Path
) -> None:
    experiment_id = _created_experiment(engine)
    client = _client(engine, tmp_path)
    submitted = client.submit(experiment_id)
    queue = TaskQueue(engine, clock=lambda: _NOW)
    claimed = queue.claim("worker-1", _NOW)
    assert claimed is not None
    ExperimentRegistry(engine, clock=lambda: _NOW).transition(
        experiment_id,
        ExperimentStatus.QUEUED,
        ExperimentStatus.RUNNING,
        now=_NOW,
    )

    duplicate = client.submit(experiment_id)

    assert duplicate.id == submitted.id
    assert duplicate.status is TaskStatus.RUNNING


def test_wait_only_polls_persisted_task_state(engine: Engine, tmp_path: Path) -> None:
    experiment_id = _created_experiment(engine)
    sleeps: list[float] = []
    task_id: str | None = None

    def finish_during_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        assert task_id is not None
        with engine.begin() as connection:
            connection.execute(
                update(TaskORM)
                .where(TaskORM.id == task_id)
                .values(
                    status=TaskStatus.SUCCEEDED.value,
                    completed_at=_NOW.isoformat(),
                    updated_at=_NOW.isoformat(),
                )
            )

    client = _client(engine, tmp_path, sleeper=finish_during_sleep)
    task_id = client.submit(experiment_id).id

    completed = client.wait(task_id, poll_seconds=0.01, timeout_seconds=1.0)

    assert completed.status is TaskStatus.SUCCEEDED
    assert sleeps == [0.01]
    assert _count(engine, TaskAttemptORM) == 0


def _register_success_fixture(engine: Engine, tmp_path: Path) -> str:
    staging = _prepared_bundle(tmp_path / "bundle")
    publication = publish_experiment_artifacts(
        staging,
        tmp_path / "published",
        ARTIFACT_EXPERIMENT,
        resolved_config=ARTIFACT_CONFIG,
    )
    config_hash = hashlib.sha256(canonical_json_bytes(ARTIFACT_CONFIG)).hexdigest()
    environment = json.loads(
        (publication.artifact_dir / "environment.json").read_bytes()
    )
    with engine.begin() as connection:
        connection.execute(
            insert(ExperimentORM).values(
                id=str(ARTIFACT_EXPERIMENT),
                strategy_id="timeline",
                strategy_version="1",
                config_json=canonical_json_bytes(ARTIFACT_CONFIG).decode(),
                config_hash=config_hash,
                snapshot_id=None,
                snapshot_manifest_hash="6" * 64,
                source_tree_hash=environment["source_tree_hash"],
                git_commit_hash=environment["git_commit"],
                lockfile_hash=environment["lockfile_hash"],
                rulebook_version="test-v1",
                fingerprint="7" * 64,
                status=ExperimentStatus.SUCCEEDED.value,
                research_mark="UNREVIEWED",
                created_at=_NOW.isoformat(),
                queued_at=_NOW.isoformat(),
                started_at=_NOW.isoformat(),
                completed_at=_NOW.isoformat(),
            )
        )
        for name, entry in publication.entries.items():
            metadata: dict[str, JsonValue] = {"size_bytes": entry.size_bytes}
            if entry.schema is not None:
                metadata["schema"] = entry.schema
                metadata["row_count"] = entry.row_count
            connection.execute(
                insert(ExperimentArtifactORM).values(
                    experiment_id=str(ARTIFACT_EXPERIMENT),
                    name=name,
                    artifact_type=Path(name).suffix.removeprefix(".") or "file",
                    path=str((publication.artifact_dir / entry.path).resolve()),
                    content_hash=entry.sha256,
                    metadata_json=canonical_json_bytes(metadata).decode(),
                    created_at=_NOW.isoformat(),
                )
            )
        manifest_bytes = publication.manifest_path.read_bytes()
        connection.execute(
            insert(ExperimentArtifactORM).values(
                experiment_id=str(ARTIFACT_EXPERIMENT),
                name="manifest.json",
                artifact_type="manifest",
                path=str(publication.manifest_path.resolve()),
                content_hash=hashlib.sha256(manifest_bytes).hexdigest(),
                metadata_json=canonical_json_bytes(
                    {
                        "schema": "quant.experiment.manifest.v1",
                        "size_bytes": len(manifest_bytes),
                    }
                ).decode(),
                created_at=_NOW.isoformat(),
            )
        )
    return str(ARTIFACT_EXPERIMENT)


def test_result_deeply_validates_registered_bundle_and_reads_metrics_and_nav(
    engine: Engine, tmp_path: Path
) -> None:
    experiment_id = _register_success_fixture(engine, tmp_path)

    result = _client(engine, tmp_path).result(experiment_id)

    metrics = result.metrics()
    nav = result.nav()
    assert metrics["metrics_version"] == "1.0.0"
    assert isinstance(nav, pl.DataFrame)
    assert nav.height == 3


def test_result_access_revalidates_after_artifact_changes_post_handoff(
    engine: Engine, tmp_path: Path
) -> None:
    experiment_id = _register_success_fixture(engine, tmp_path)
    client = _client(engine, tmp_path)
    result = client.result(experiment_id)
    detail = ExperimentQuery(engine).get(experiment_id)
    metrics = next(item for item in detail.artifacts if item.name == "metrics.json")
    Path(metrics.path).write_bytes(b"{}")

    with pytest.raises(ValueError):
        result.metrics()


@pytest.mark.parametrize("field", ["content_hash", "metadata_json"])
def test_result_rejects_db_registration_tamper_before_handoff(
    engine: Engine, tmp_path: Path, field: str
) -> None:
    experiment_id = _register_success_fixture(engine, tmp_path)
    changed = (
        "0" * 64
        if field == "content_hash"
        else canonical_json_bytes({"size_bytes": 1}).decode()
    )
    with engine.begin() as connection:
        connection.execute(
            update(ExperimentArtifactORM)
            .where(
                ExperimentArtifactORM.experiment_id == experiment_id,
                ExperimentArtifactORM.name == "metrics.json",
            )
            .values({field: changed})
        )

    with pytest.raises(ValueError, match="registered|metadata|publication"):
        _client(engine, tmp_path).result(experiment_id)


@pytest.mark.parametrize("field", ["content_hash", "metadata_json"])
def test_result_requeries_and_rejects_db_tamper_after_handoff(
    engine: Engine, tmp_path: Path, field: str
) -> None:
    experiment_id = _register_success_fixture(engine, tmp_path)
    result = _client(engine, tmp_path).result(experiment_id)
    changed = (
        "0" * 64
        if field == "content_hash"
        else canonical_json_bytes({"size_bytes": 1}).decode()
    )
    with engine.begin() as connection:
        connection.execute(
            update(ExperimentArtifactORM)
            .where(
                ExperimentArtifactORM.experiment_id == experiment_id,
                ExperimentArtifactORM.name == "metrics.json",
            )
            .values({field: changed})
        )

    with pytest.raises(ValueError, match="registered|metadata|publication"):
        result.metrics()


def test_result_access_rejects_file_reparse_swap_after_handoff(
    engine: Engine, tmp_path: Path
) -> None:
    experiment_id = _register_success_fixture(engine, tmp_path)
    result = _client(engine, tmp_path).result(experiment_id)
    detail = ExperimentQuery(engine).get(experiment_id)
    metrics = next(item for item in detail.artifacts if item.name == "metrics.json")
    path = Path(metrics.path)
    external = tmp_path / "external-same-metrics.json"
    shutil.copyfile(path, external)
    path.unlink()
    try:
        path.symlink_to(external)
    except OSError as error:
        pytest.skip(f"file symlinks are unavailable on this platform: {error}")

    with pytest.raises(ValueError, match="reparse|regular|changed"):
        result.metrics()


@pytest.mark.skipif(os.name != "nt", reason="Windows junction capability test")
def test_result_access_rejects_artifact_root_junction_after_handoff(
    engine: Engine, tmp_path: Path
) -> None:
    experiment_id = _register_success_fixture(engine, tmp_path)
    result = _client(engine, tmp_path).result(experiment_id)
    detail = ExperimentQuery(engine).get(experiment_id)
    manifest = next(item for item in detail.artifacts if item.name == "manifest.json")
    artifact_dir = Path(manifest.path).parent
    published_root = artifact_dir.parent
    displaced = tmp_path / "displaced-published-root"
    published_root.rename(displaced)
    completed = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(published_root), str(displaced)],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        pytest.skip(
            "directory junctions are unavailable: "
            f"{completed.stderr.strip() or completed.stdout.strip()} "
            f"(link={published_root!s}, target={displaced!s})"
        )
    try:
        with pytest.raises(ValueError, match="reparse|artifact_root|plain"):
            result.nav()
    finally:
        published_root.rmdir()


def test_result_rejects_non_successful_experiment(
    engine: Engine, tmp_path: Path
) -> None:
    experiment_id = _created_experiment(engine)

    with pytest.raises(ValueError, match="SUCCEEDED"):
        _client(engine, tmp_path).result(experiment_id)


def test_already_succeeded_runner_recovery_deeply_checks_registered_rows(
    engine: Engine, tmp_path: Path
) -> None:
    experiment_id = _register_success_fixture(engine, tmp_path)
    runner = ExperimentRunner(
        query=ExperimentQuery(engine),
        registry=ExperimentRegistry(engine),
        runtime_factory=lambda _record: None,
        artifact_finalizer=_UnusedFinalizer(),
    )

    publication = runner.verify_success(experiment_id)
    assert publication.artifact_dir.name == f"experiment_id={experiment_id}"

    with engine.begin() as connection:
        connection.execute(
            update(ExperimentArtifactORM)
            .where(
                ExperimentArtifactORM.experiment_id == experiment_id,
                ExperimentArtifactORM.name == "nav.parquet",
            )
            .values(content_hash="0" * 64)
        )
    with pytest.raises(ValueError, match="registered artifact path or hash"):
        runner.verify_success(experiment_id)


@pytest.mark.parametrize("mutation", ["tamper", "unregistered_path"])
def test_result_rejects_corrupt_or_unregistered_artifact_resolution(
    engine: Engine, tmp_path: Path, mutation: str
) -> None:
    experiment_id = _register_success_fixture(engine, tmp_path)
    detail = ExperimentQuery(engine).get(experiment_id)
    metrics = next(item for item in detail.artifacts if item.name == "metrics.json")
    if mutation == "tamper":
        Path(metrics.path).write_bytes(b"{}")
    else:
        external = tmp_path / "external-metrics.json"
        shutil.copyfile(metrics.path, external)
        with engine.begin() as connection:
            connection.execute(
                update(ExperimentArtifactORM)
                .where(
                    ExperimentArtifactORM.experiment_id == experiment_id,
                    ExperimentArtifactORM.name == "metrics.json",
                )
                .values(path=str(external.resolve()))
            )

    with pytest.raises(ValueError):
        _client(engine, tmp_path).result(experiment_id)


def test_create_from_yaml_resolves_identity_and_registers_created_experiment(
    engine: Engine, tmp_path: Path
) -> None:
    config_root = tmp_path / "configs"
    config_root.mkdir()
    path = config_root / "experiment.yaml"
    path.write_text(
        """schema_version: 1
strategy_id: stock_multifactor
strategy_version: 1.0.0
snapshot_id: latest
start_date: snapshot_start
end_date: snapshot_end
benchmark: SSE:000300
initial_cash_fen: 100000000
rulebook_version: a-share-v1
strategy_config: {}
""",
        encoding="utf-8",
    )
    snapshot_catalog = _catalog(tmp_path)
    persisted_snapshot = snapshot_catalog.latest_snapshot()
    assert persisted_snapshot is not None
    with engine.begin() as connection:
        connection.execute(
            insert(QualityRunORM).values(
                id=str(_QUALITY_RUN_ID),
                status="COMPLETED",
                started_at=_NOW.isoformat(),
                completed_at=_NOW.isoformat(),
                created_at=_NOW.isoformat(),
            )
        )
        connection.execute(
            insert(SnapshotORM).values(
                id=str(_SNAPSHOT_ID),
                publication_fingerprint="b" * 64,
                as_of=_NOW.isoformat(),
                status=SnapshotStatus.PUBLISHED.value,
                manifest_path=str(persisted_snapshot.manifest_path),
                manifest_hash="c" * 64,
                quality_run_id=str(_QUALITY_RUN_ID),
                created_at=_NOW.isoformat(),
                published_at=_NOW.isoformat(),
            )
        )
    client = ExperimentClient(
        registry=ExperimentRegistry(engine, clock=lambda: _NOW),
        query=ExperimentQuery(engine),
        queue=TaskQueue(engine, clock=lambda: _NOW),
        config_root=config_root,
        catalog=snapshot_catalog,
        strategies={_STRATEGY: object()},
        rulebook=_rulebook(),
        environment_factory=_environment,
        clock=lambda: _NOW,
    )

    experiment = client.create_from_yaml(path)

    assert experiment.status is ExperimentStatus.CREATED
    assert experiment.strategy_id == "stock_multifactor"
    assert experiment.strategy_version == "1.0.0"
    assert experiment.snapshot_id == str(_SNAPSHOT_ID)
    assert experiment.snapshot_manifest_hash == "c" * 64
    assert experiment.source_tree_hash == "2" * 64
    assert experiment.lockfile_hash == "3" * 64
    assert experiment.config["start_date"] == "2024-01-02"
    assert experiment.config["end_date"] == "2024-01-31"
    assert experiment.config["execution"] == {
        "max_volume_participation": 0.1,
        "reference_price": "OPEN",
        "slippage_bps": 5.0,
    }


def test_from_default_settings_wires_only_local_control_plane(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_root = tmp_path / "runtime"
    monkeypatch.setenv("QUANT_DATA_ROOT", str(data_root))

    def reject_network_gateway(*_: object, **__: object) -> None:
        raise AssertionError(
            "default experiment client must not construct network gateways"
        )

    def reject_worker(*_: object, **__: object) -> None:
        raise AssertionError("default experiment client must not start a worker")

    monkeypatch.setattr(BaoStockSdkGateway, "__init__", reject_network_gateway)
    monkeypatch.setattr(Worker, "__init__", reject_worker)

    client = ExperimentClient.from_default_settings()
    client.close()

    assert (data_root / "state" / "quant.db").is_file()


@pytest.mark.parametrize(
    ("name", "strategy_id"),
    [
        ("multifactor.yaml", "stock_multifactor"),
        ("etf_rotation.yaml", "etf_rotation"),
    ],
)
def test_public_examples_are_complete_experiment_envelopes(
    name: str, strategy_id: str
) -> None:
    root = Path(__file__).resolve().parents[2]
    payload = yaml.safe_load(
        (root / "configs" / "experiments" / "examples" / name).read_text(
            encoding="utf-8"
        )
    )

    assert payload["schema_version"] == 1
    assert payload["strategy_id"] == strategy_id
    assert payload["strategy_version"] == "1.0.0"
    assert isinstance(payload["strategy_config"], dict)


def test_submitted_task_survives_client_close_and_is_finished_by_spawned_worker(
    tmp_path: Path,
) -> None:
    database = tmp_path / "cross-process" / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    environment = _captured_environment(tmp_path)
    experiment_id = _cross_process_experiment(engine, tmp_path, environment)
    client = _client(engine, tmp_path)
    task = client.submit(experiment_id)
    with engine.begin() as connection:
        connection.execute(
            update(TaskORM)
            .where(TaskORM.id == task.id)
            .values(available_at=datetime(2020, 1, 1, tzinfo=UTC).isoformat())
        )
    client.close()
    engine.dispose()

    process = multiprocessing.get_context("spawn").Process(
        target=_run_independent_worker,
        args=(str(database), str(tmp_path / "artifacts"), environment),
    )
    process.start()
    process.join(timeout=30)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("spawned worker did not finish before timeout")
    assert process.exitcode == 0

    verification_engine = create_sqlite_engine(database)
    try:
        completed = TaskQueue(verification_engine).get(task.id)
        experiment = ExperimentQuery(verification_engine).get(experiment_id).record
        verified = _client(verification_engine, tmp_path).result(experiment_id)
        metrics = verified.metrics()
        nav = verified.nav()
    finally:
        verification_engine.dispose()
    assert completed.status is TaskStatus.SUCCEEDED
    assert experiment.status is ExperimentStatus.SUCCEEDED
    assert metrics["metrics_version"] == "1.0.0"
    assert isinstance(nav, pl.DataFrame)
    assert nav.height == 2
