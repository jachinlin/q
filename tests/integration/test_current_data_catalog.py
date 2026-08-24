"""Integration tests for the current canonical catalog and validation gate."""

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from io import StringIO
from pathlib import Path

import polars as pl
import pytest
from sqlalchemy import inspect

from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.contracts import CanonicalBatch, canonical_json_bytes
from quant_research.data.pipeline.curate import CuratedPartitionStore
from quant_research.data.quality.models import QualityIssue, QualityRunSpec
from quant_research.data.repository import CanonicalResearchRepository
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.errors import QuantError
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetSpec,
    CanonicalPartitionSpec,
    MetadataRepository,
    RawHeadIdentity,
    RawHeadSnapshot,
    RawPartitionSpec,
)
from quant_research.logging import StructuredLogger

NOW = datetime(2026, 8, 11, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64
SCHEMA_HASH = "c" * 64


def _dataset(
    path: Path,
    content_hash: str = HASH_A,
    *,
    input_hash: str = "d" * 64,
) -> CanonicalDatasetSpec:
    return CanonicalDatasetSpec(
        dataset=DatasetKind.DAILY_BAR,
        source="baostock",
        partitions=(
            CanonicalPartitionSpec(
                partition_key="year=2026",
                content_hash=content_hash,
                path=path,
                schema_fingerprint=SCHEMA_HASH,
                input_hash=input_hash,
                row_count=10,
            ),
        ),
        start_date=date(2026, 1, 1),
        end_date=date(2026, 8, 11),
    )


def test_fresh_database_contains_only_final_tables(tmp_path: Path) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)

    assert set(inspect(engine).get_table_names()) == {
        "alembic_version",
        "audit_event",
        "canonical_dataset",
        "canonical_partition",
        "data_catalog_state",
        "data_initialization_state",
        "dataset_operational_state",
        "quality_issue",
        "quality_rule_result",
        "quality_run",
        "quality_run_dataset",
        "raw_object",
        "raw_request",
        "experiment",
        "experiment_tag",
        "factor_study",
        "factor_study_artifact",
        "factor_study_decision",
        "factor_study_metric",
        "factor_study_tag",
        "run",
        "run_artifact",
        "run_metric",
        "run_tag",
        "task",
        "task_attempt",
    }
    engine.dispose()


def test_content_identity_ignores_path_and_pointer_change_invalidates_gate(
    tmp_path: Path,
) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    first = repository.replace_canonical_dataset(
        _dataset(tmp_path / "first.parquet"), updated_at=NOW
    )
    initial_hash = first.record.content_hash

    same_content = repository.replace_canonical_dataset(
        _dataset(tmp_path / "relocated.parquet"), updated_at=NOW
    )

    assert same_content.changed is False
    assert same_content.record.content_hash == initial_hash
    assert (
        same_content.record.partitions[0].path == (tmp_path / "first.parquet").resolve()
    )

    state = repository.catalog_state()
    quality = repository.register_quality_run(
        QualityRunSpec(
            dataset_hashes={DatasetKind.DAILY_BAR.value: initial_hash},
            input_hash=state.catalog_hash,
            scope="ALL",
            started_at=NOW,
            completed_at=NOW,
            issues=(),
        )
    )
    repository.mark_catalog_validated(quality.id, validated_at=NOW)
    assert repository.require_validated_catalog().is_validated

    checkpoint_only = repository.replace_canonical_dataset(
        _dataset(tmp_path / "ignored-relocation.parquet", input_hash="e" * 64),
        updated_at=NOW,
    )
    assert checkpoint_only.changed is False
    assert checkpoint_only.record.partitions[0].input_hash == "e" * 64
    assert repository.require_validated_catalog().is_validated

    changed = repository.replace_canonical_dataset(
        _dataset(tmp_path / "changed.parquet", HASH_B), updated_at=NOW
    )

    assert changed.changed is True
    assert changed.orphan_paths == ((tmp_path / "first.parquet").resolve(),)
    with pytest.raises(QuantError, match="validate-all"):
        repository.require_validated_catalog()
    engine.dispose()


def test_validate_all_rejects_catalog_change_during_validation(tmp_path: Path) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    repository.replace_canonical_dataset(
        _dataset(tmp_path / "a.parquet"), updated_at=NOW
    )
    started_state = repository.catalog_state()
    dataset_hash = repository.get_canonical_dataset(DatasetKind.DAILY_BAR).content_hash
    run = repository.register_quality_run(
        QualityRunSpec(
            dataset_hashes={DatasetKind.DAILY_BAR.value: dataset_hash},
            input_hash=started_state.catalog_hash,
            scope="ALL",
            started_at=NOW,
            completed_at=NOW,
            issues=(),
        )
    )
    repository.replace_canonical_dataset(
        replace(_dataset(tmp_path / "b.parquet"), source="corrected"),
        updated_at=NOW,
    )

    with pytest.raises(QuantError) as caught:
        repository.mark_catalog_validated(run.id, validated_at=NOW)

    assert caught.value.detail.code == "DATA_VALIDATE_INPUT_CHANGED"
    engine.dispose()


def test_blocking_quality_issue_never_opens_research_gate(tmp_path: Path) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    repository.replace_canonical_dataset(
        _dataset(tmp_path / "a.parquet"), updated_at=NOW
    )
    state = repository.catalog_state()
    dataset_hash = repository.get_canonical_dataset(DatasetKind.DAILY_BAR).content_hash
    run = repository.register_quality_run(
        QualityRunSpec(
            dataset_hashes={DatasetKind.DAILY_BAR.value: dataset_hash},
            input_hash=state.catalog_hash,
            scope="ALL",
            started_at=NOW,
            completed_at=NOW,
            issues=(
                QualityIssue(
                    rule_id="test_blocking",
                    severity=Severity.SEVERE,
                    dataset=DatasetKind.DAILY_BAR,
                    scope={},
                    actual=1,
                    threshold=0,
                    message="blocking test issue",
                    remediation="repair the current data",
                ),
            ),
        )
    )

    assert run.status == "FAILED"
    with pytest.raises(ValueError, match="passed validate-all"):
        repository.mark_catalog_validated(run.id, validated_at=NOW)
    with pytest.raises(QuantError, match="validate-all"):
        repository.require_validated_catalog()
    engine.dispose()


def _canonical_batch(dataset: DatasetKind) -> CanonicalBatch:
    common = {
        "source": ["test"],
        "available_at": [NOW],
        "availability_source": ["test"],
        "pit_usable": [True],
        "ingested_at": [NOW],
    }
    if dataset is DatasetKind.INSTRUMENT:
        values = {
            "instrument_id": ["600000.SH"],
            "exchange": ["SSE"],
            "board": ["MAIN"],
            "name": ["浦发银行"],
            "instrument_type": ["STOCK"],
            "listing_status": ["LISTED"],
            "list_date": [date(1999, 11, 10)],
            "delist_date": [None],
        }
    elif dataset is DatasetKind.TRADE_CALENDAR:
        values = {
            "trade_date": [date(2026, 8, 11)],
            "is_trading_day": [True],
        }
    else:  # pragma: no cover - the helper intentionally supports two datasets
        raise AssertionError(dataset)
    return CanonicalBatch(
        dataset,
        pl.DataFrame(
            {**values, **common},
            schema=CANONICAL_SCHEMAS[dataset].columns,
        ),
        (HASH_A,),
    )


def _publish_batch(
    store: CuratedPartitionStore,
    repository: MetadataRepository,
    batch: CanonicalBatch,
    *,
    previous: bool = False,
    logger: StructuredLogger | None = None,
) -> None:
    current = repository.find_canonical_dataset(batch.dataset) if previous else None
    store.publish(
        (batch,),
        previous_datasets=(
            {batch.dataset.value: current} if current is not None else {}
        ),
        run_id="test-run",
        source="test",
        start=date(2026, 8, 11),
        end=date(2026, 8, 11),
        repository=repository,
        logger=logger,
    )


def test_curate_cleanup_preserves_partitions_referenced_by_other_datasets(
    tmp_path: Path,
) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    store = CuratedPartitionStore(tmp_path / "canonical")

    _publish_batch(store, repository, _canonical_batch(DatasetKind.INSTRUMENT))
    instrument_path = (
        repository.get_canonical_dataset(DatasetKind.INSTRUMENT).partitions[0].path
    )
    _publish_batch(store, repository, _canonical_batch(DatasetKind.TRADE_CALENDAR))

    assert instrument_path.is_file()
    engine.dispose()


def test_curate_rebuilds_a_missing_current_partition_from_raw_batches(
    tmp_path: Path,
) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    store = CuratedPartitionStore(tmp_path / "canonical")
    batch = _canonical_batch(DatasetKind.INSTRUMENT)
    _publish_batch(store, repository, batch)
    partition = repository.get_canonical_dataset(DatasetKind.INSTRUMENT).partitions[0]
    partition.path.unlink()

    _publish_batch(store, repository, batch, previous=True)

    assert partition.path.is_file()
    store.verify_dataset(repository.get_canonical_dataset(DatasetKind.INSTRUMENT))
    engine.dispose()


def test_repository_accepts_the_exact_multi_chunk_hash_published_by_curate(
    tmp_path: Path,
) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    metadata = MetadataRepository(engine)
    store = CuratedPartitionStore(tmp_path / "canonical")
    first = _canonical_batch(DatasetKind.INSTRUMENT)
    row_count = 140_000
    expanded = first.frame.select(pl.all().repeat_by(row_count).explode()).with_columns(
        (
            pl.int_range(0, row_count, eager=True).cast(pl.String).str.zfill(6) + ".SZ"
        ).alias("instrument_id")
    )
    combined = replace(
        first,
        frame=expanded,
    )
    _publish_batch(store, metadata, combined)
    state = metadata.catalog_state()
    record = metadata.get_canonical_dataset(DatasetKind.INSTRUMENT)
    quality = metadata.register_quality_run(
        QualityRunSpec(
            dataset_hashes={DatasetKind.INSTRUMENT.value: record.content_hash},
            input_hash=state.catalog_hash,
            scope="ALL",
            started_at=NOW,
            completed_at=NOW,
            issues=(),
        )
    )
    metadata.mark_catalog_validated(quality.id, validated_at=NOW)

    repository = CanonicalResearchRepository(
        metadata, trusted_curated_root=tmp_path / "canonical"
    )

    assert repository.instruments().collect().height == row_count
    engine.dispose()


def test_canonical_commit_rejects_a_changed_raw_head_snapshot(tmp_path: Path) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    request = {"date": "2026-08-11"}
    request_hash = hashlib.sha256(canonical_json_bytes(request)).hexdigest()

    first = repository.register_raw_partition(
        RawPartitionSpec(
            source="baostock",
            endpoint="daily",
            request=request,
            request_hash=request_hash,
            content_hash=HASH_A,
            data_path=tmp_path / "raw-a.parquet",
            manifest_path=tmp_path / "manifest.json",
            schema_fingerprint=SCHEMA_HASH,
            row_count=1,
            retrieved_at=NOW,
        )
    )
    snapshot = RawHeadSnapshot(
        source="baostock",
        endpoints=("daily",),
        heads=(RawHeadIdentity.from_record(first),),
    )
    repository.register_raw_partition(
        RawPartitionSpec(
            source="baostock",
            endpoint="daily",
            request=request,
            request_hash=request_hash,
            content_hash=HASH_B,
            data_path=tmp_path / "raw-b.parquet",
            manifest_path=tmp_path / "manifest.json",
            schema_fingerprint=SCHEMA_HASH,
            row_count=1,
            retrieved_at=NOW,
        )
    )

    with pytest.raises(QuantError) as caught:
        repository.replace_canonical_dataset(
            _dataset(tmp_path / "canonical.parquet"),
            updated_at=NOW,
            expected_raw_heads=snapshot,
        )

    assert caught.value.detail.code == "DATA_CURATE_INPUT_CHANGED"
    assert repository.find_canonical_dataset(DatasetKind.DAILY_BAR) is None
    engine.dispose()


def test_curate_partition_log_contains_committed_partition_business_data(
    tmp_path: Path,
) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    store = CuratedPartitionStore(tmp_path / "canonical")
    stream = StringIO()

    _publish_batch(
        store,
        repository,
        _canonical_batch(DatasetKind.INSTRUMENT),
        logger=StructuredLogger(stream),
    )

    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    completed = next(
        record for record in records if record["event"] == "curate.partition_completed"
    )
    context = completed["context"]
    partition = context["partition"]
    assert completed["stage"] == "CURATE"
    assert "request" not in context
    assert context["dataset"] == "instrument"
    assert context["pointer_changed"] is True
    assert partition["partition_key"] == "all"
    assert partition["disposition"] == "file_written"
    assert "source_content_hashes" not in partition
    assert "source_lineage" not in partition
    assert partition["schema_fingerprint"]
    assert Path(partition["path"]).is_file()
    engine.dispose()


def test_curate_logs_ignore_large_raw_content_hash_collections(tmp_path: Path) -> None:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    repository = MetadataRepository(engine)
    store = CuratedPartitionStore(tmp_path / "canonical")
    stream = StringIO()
    batch = replace(
        _canonical_batch(DatasetKind.INSTRUMENT),
        source_content_hashes=tuple(f"{value:064x}" for value in range(1_000)),
    )

    _publish_batch(
        store,
        repository,
        batch,
        logger=StructuredLogger(stream),
    )

    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert any(record["event"] == "curate.partition_completed" for record in records)
    assert all(
        "source_content_hash" not in json.dumps(record["context"]) for record in records
    )
    engine.dispose()
