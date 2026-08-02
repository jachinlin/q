"""Integration contract for the durable experiment registry and query service."""

from __future__ import annotations

import hashlib
import json
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import Engine, event, text

import quant_core.experiments.registry as registry_module
import tests.integration.test_backtest_timeline as timeline_contract
import tests.integration.test_experiment_artifact_contract as artifact_contract
from quant_core.backtest.artifacts import (
    ExperimentArtifactPublication,
    publish_experiment_artifacts,
)
from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.domain.enums import Severity
from quant_core.errors import ErrorDetail
from quant_core.experiments.models import (
    ExperimentSpec,
    ExperimentStatus,
    ResearchMark,
)
from quant_core.experiments.query import ExperimentQuery
from quant_core.experiments.registry import (
    DuplicateResearchWarning,
    ExperimentNotFound,
    ExperimentRegistry,
    StateConflict,
)
from quant_core.persistence.database import create_sqlite_engine, upgrade_database

NOW = datetime(2026, 8, 2, 8, tzinfo=UTC)
SNAPSHOT_ID = str(timeline_contract._SNAPSHOT)
CONFIG: dict[str, JsonValue] = {
    "strategy": {"lookback": 20, "winsorize": True},
    "universe": "CSI300",
}


@pytest.fixture
def engine(tmp_path: Path) -> Engine:
    database = tmp_path / "registry.db"
    upgrade_database(database)
    value = create_sqlite_engine(database)
    with value.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO quality_run "
                "(id, status, started_at, completed_at, created_at) VALUES "
                "('quality-task3', 'SUCCEEDED', :now, :now, :now)"
            ),
            {"now": NOW.isoformat()},
        )
        connection.execute(
            text(
                "INSERT INTO snapshot "
                "(id, publication_fingerprint, as_of, status, manifest_path, "
                "manifest_hash, quality_run_id, created_at, published_at) VALUES "
                "(:id, :fingerprint, :now, 'PUBLISHED', 'snapshot.json', :hash, "
                "'quality-task3', :now, :now)"
            ),
            {
                "id": SNAPSHOT_ID,
                "fingerprint": "9" * 64,
                "now": NOW.isoformat(),
                "hash": "a" * 64,
            },
        )
    yield value
    value.dispose()


def _spec(
    *,
    fingerprint: str = "f" * 64,
    created_at: datetime = NOW,
    strategy_id: str = "timeline",
    strategy_version: str = "1",
    snapshot_id: str | None = SNAPSHOT_ID,
) -> ExperimentSpec:
    return ExperimentSpec(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        config=CONFIG,
        config_hash=hashlib.sha256(canonical_json_bytes(CONFIG)).hexdigest(),
        snapshot_id=snapshot_id,
        snapshot_manifest_hash="a" * 64,
        source_tree_hash=(
            "53fd8112fee36c83793bd60832189d656"
            "c084b824a031717589647d32c4647bd"
        ),
        lockfile_hash=(
            "dbab12665d98aef021ba64953c61b0ed"
            "8a908cfb56a1c01e2fcb4b052b71a2a1"
        ),
        rulebook_version="test-v1",
        fingerprint=fingerprint,
        created_at=created_at,
    )


def _rows(engine: Engine, statement: str, **parameters: object) -> list[Any]:
    with engine.connect() as connection:
        return list(connection.execute(text(statement), parameters).mappings())


def _audit(engine: Engine, experiment_id: str) -> list[dict[str, Any]]:
    rows = _rows(
        engine,
        "SELECT event_type, actor, details_json, created_at FROM audit_event "
        "WHERE experiment_id=:experiment_id ORDER BY id",
        experiment_id=experiment_id,
    )
    return [
        {
            "event_type": row["event_type"],
            "actor": row["actor"],
            "details": json.loads(row["details_json"]),
            "details_json": row["details_json"],
            "created_at": datetime.fromisoformat(row["created_at"]),
        }
        for row in rows
    ]


def _running_experiment(
    engine: Engine, *, fingerprint: str = "f" * 64
) -> tuple[ExperimentRegistry, str]:
    registry = ExperimentRegistry(engine)
    experiment_id = registry.create(
        _spec(fingerprint=fingerprint),
        fingerprint,
        actor="researcher",
        request_id="create-running",
        now=NOW,
    )
    registry.transition(
        experiment_id,
        ExperimentStatus.CREATED,
        ExperimentStatus.QUEUED,
        actor="scheduler",
        request_id="queue-running",
        now=NOW + timedelta(minutes=1),
    )
    registry.transition(
        experiment_id,
        ExperimentStatus.QUEUED,
        ExperimentStatus.RUNNING,
        actor="worker-1",
        request_id="start-running",
        now=NOW + timedelta(minutes=2),
    )
    return registry, experiment_id


def _publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    experiment_id: str,
) -> ExperimentArtifactPublication:
    identifier = UUID(experiment_id)
    monkeypatch.setattr(artifact_contract, "_EXPERIMENT", identifier)
    monkeypatch.setattr(timeline_contract, "_EXPERIMENT", identifier)
    staging = artifact_contract._prepared_bundle(tmp_path / "bundle")
    return publish_experiment_artifacts(
        staging,
        tmp_path / "published",
        identifier,
        resolved_config=CONFIG,
    )


def _finite_metrics(publication: ExperimentArtifactPublication) -> dict[str, float]:
    payload = json.loads((publication.artifact_dir / "metrics.json").read_bytes())
    return {
        name: float(value)
        for name, value in payload.items()
        if not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    }


def test_create_always_uses_a_new_id_and_audits_duplicate_research(
    engine: Engine,
) -> None:
    """Repeated fingerprints must warn without reusing an experiment identity."""
    registry = ExperimentRegistry(engine)
    first = registry.create(
        _spec(),
        "f" * 64,
        actor="alice",
        request_id="request-create-1",
        now=NOW,
    )
    with pytest.warns(DuplicateResearchWarning) as captured:
        second = registry.create(
            _spec(created_at=NOW + timedelta(seconds=1)),
            "f" * 64,
            actor="alice",
            request_id="request-create-2",
            now=NOW + timedelta(seconds=1),
        )

    warning = captured.list[0].message
    assert isinstance(warning, DuplicateResearchWarning)
    assert warning.fingerprint == "f" * 64
    assert warning.existing_count == 1
    assert warning.experiment_id == second
    assert first != second
    assert len(_rows(engine, "SELECT id FROM experiment")) == 2
    details = _audit(engine, second)[0]
    assert details["event_type"] == "EXPERIMENT_CREATED"
    assert details["actor"] == "alice"
    assert details["details"]["duplicate_count"] == 1
    assert details["details"]["request_id"] == "request-create-2"
    assert details["details_json"] == canonical_json_bytes(
        details["details"]
    ).decode("utf-8")
    assert details["created_at"] == NOW + timedelta(seconds=1)

    with pytest.raises(ValueError, match="fingerprint"):
        registry.create(_spec(), "e" * 64)


def test_two_independent_registry_sessions_make_one_cas_winner(
    engine: Engine,
) -> None:
    """Two real SQLite sessions with the same expected state must not both commit."""
    registry = ExperimentRegistry(engine)
    experiment_id = registry.create(
        _spec(), "f" * 64, request_id="create-cas", now=NOW
    )
    registry.transition(
        experiment_id,
        ExperimentStatus.CREATED,
        ExperimentStatus.QUEUED,
        request_id="queue-cas",
        now=NOW + timedelta(seconds=1),
    )
    barrier = Barrier(2)

    def compete(request_id: str) -> BaseException | None:
        contender = ExperimentRegistry(engine)
        barrier.wait(timeout=10)
        try:
            contender.transition(
                experiment_id,
                ExperimentStatus.QUEUED,
                ExperimentStatus.RUNNING,
                actor=request_id,
                request_id=request_id,
                now=NOW + timedelta(seconds=2),
            )
        except BaseException as error:  # noqa: BLE001 - outcome is asserted below.
            return error
        return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(compete, ("cas-a", "cas-b")))

    assert sum(outcome is None for outcome in outcomes) == 1
    conflicts = [outcome for outcome in outcomes if isinstance(outcome, StateConflict)]
    assert len(conflicts) == 1
    assert conflicts[0].detail.context["actual"] == "RUNNING"
    record = ExperimentQuery(engine).get(experiment_id).record
    assert record.status is ExperimentStatus.RUNNING
    events = [event["event_type"] for event in _audit(engine, experiment_id)]
    assert events.count("EXPERIMENT_STATE_TRANSITIONED") == 2
    assert events.count("EXPERIMENT_STATE_CONFLICT") == 1


def test_stale_transition_missing_expected_timestamp_is_audited_conflict(
    engine: Engine,
) -> None:
    """A stale expected state must not leak a timestamp-prerequisite ValueError."""
    registry = ExperimentRegistry(engine)
    experiment_id = registry.create(
        _spec(), "f" * 64, request_id="stale-create", now=NOW
    )

    with pytest.raises(StateConflict) as captured:
        registry.transition(
            experiment_id,
            ExperimentStatus.QUEUED,
            ExperimentStatus.RUNNING,
            request_id="stale-transition",
            now=NOW + timedelta(seconds=1),
        )

    assert captured.value.detail.context["actual"] == "CREATED"
    assert _audit(engine, experiment_id)[-1]["event_type"] == (
        "EXPERIMENT_STATE_CONFLICT"
    )


def test_transition_distinguishes_not_found_and_sanitizes_failure_reason(
    engine: Engine,
) -> None:
    """Missing identity is not a stale state, and audit cannot disclose secrets."""
    registry = ExperimentRegistry(engine)
    with pytest.raises(ExperimentNotFound) as missing:
        registry.transition(
            "missing-experiment",
            ExperimentStatus.CREATED,
            ExperimentStatus.QUEUED,
            request_id="missing-cas",
            now=NOW,
        )
    assert missing.value.detail.code == "EXPERIMENT_NOT_FOUND"
    assert len(
        _rows(
            engine,
            "SELECT id FROM audit_event WHERE event_type='EXPERIMENT_STATE_CONFLICT'",
        )
    ) == 1

    registry, experiment_id = _running_experiment(engine, fingerprint="e" * 64)
    reason = ErrorDetail(
        code="BACKTEST_FAILED",
        severity=Severity.SEVERE,
        message="source rejected input",
        context={
            "dataset": "prices",
            "access_token": "do-not-store",
            "connection": {"password": "nested-secret", "host": "localhost"},
        },
        remediation="inspect the disclosed dataset",
        retryable=False,
    )
    registry.transition(
        experiment_id,
        ExperimentStatus.RUNNING,
        ExperimentStatus.FAILED,
        reason,
        actor="worker-1",
        request_id="fail-safe",
        now=NOW + timedelta(minutes=3),
    )
    audit = _audit(engine, experiment_id)[-1]
    assert audit["event_type"] == "EXPERIMENT_STATE_TRANSITIONED"
    assert audit["details"]["reason"]["code"] == "BACKTEST_FAILED"
    assert audit["details"]["reason"]["context"] == {
        "access_token": "[REDACTED]",
        "connection": {"host": "localhost", "password": "[REDACTED]"},
        "dataset": "prices",
    }
    assert "do-not-store" not in audit["details_json"]
    assert "nested-secret" not in audit["details_json"]


def test_register_success_revalidates_before_writes_and_indexes_eighteen_files(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real final bundle is deep-checked before one atomic success transaction."""
    registry, experiment_id = _running_experiment(engine)
    publication = _publication(tmp_path, monkeypatch, experiment_id)
    metrics = _finite_metrics(publication)
    write_seen = False
    validation_saw_write = False
    real_validate = registry_module.validate_experiment_artifacts

    def observe_validation(*args: Any, **kwargs: Any) -> ExperimentArtifactPublication:
        nonlocal validation_saw_write
        validation_saw_write = write_seen
        return real_validate(*args, **kwargs)

    def observe_write(
        _connection: object,
        _cursor: object,
        statement: str,
        _parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        nonlocal write_seen
        if statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE")):
            write_seen = True

    monkeypatch.setattr(registry_module, "validate_experiment_artifacts", observe_validation)
    event.listen(engine, "before_cursor_execute", observe_write)
    try:
        registry.register_success(
            experiment_id,
            publication,
            metrics,
            actor="worker-1",
            request_id="success-1",
            now=NOW + timedelta(minutes=3),
        )
    finally:
        event.remove(engine, "before_cursor_execute", observe_write)

    detail = ExperimentQuery(engine).get(experiment_id)
    assert validation_saw_write is False
    assert detail.record.status is ExperimentStatus.SUCCEEDED
    assert detail.record.completed_at == NOW + timedelta(minutes=3)
    assert len(detail.artifacts) == 18
    assert {artifact.name for artifact in detail.artifacts} == {
        *publication.entries,
        "manifest.json",
    }
    assert len(detail.metrics) == len(metrics)
    assert {metric.name: metric.value for metric in detail.metrics} == metrics
    metric_json = json.loads((publication.artifact_dir / "metrics.json").read_bytes())
    assert all(metric.name not in metric_json or metric_json[metric.name] is not None for metric in detail.metrics)
    assert _audit(engine, experiment_id)[-1]["event_type"] == "EXPERIMENT_SUCCEEDED"

    with pytest.raises(StateConflict):
        registry.register_success(
            experiment_id,
            publication,
            metrics,
            request_id="success-again",
            now=NOW + timedelta(minutes=4),
        )
    assert len(ExperimentQuery(engine).get(experiment_id).artifacts) == 18


def test_register_success_rejects_mutable_dto_and_rolls_back_partial_writes(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DTO drift and a mid-transaction uniqueness failure must leave RUNNING intact."""
    registry, experiment_id = _running_experiment(engine)
    publication = _publication(tmp_path, monkeypatch, experiment_id)
    metrics = _finite_metrics(publication)
    changed_manifest = dict(publication.manifest)
    changed_manifest["schema_version"] = 999
    changed = replace(publication, manifest=changed_manifest)

    with pytest.raises(ValueError, match="publication"):
        registry.register_success(
            experiment_id,
            changed,
            metrics,
            request_id="dto-drift",
            now=NOW + timedelta(minutes=3),
        )
    assert ExperimentQuery(engine).get(experiment_id).record.status is ExperimentStatus.RUNNING

    first_name, first_entry = next(iter(publication.entries.items()))
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO experiment_artifact "
                "(experiment_id, name, artifact_type, path, content_hash, "
                "metadata_json, created_at) VALUES "
                "(:experiment_id, :name, 'injected', :path, :hash, '{}', :created_at)"
            ),
            {
                "experiment_id": experiment_id,
                "name": first_name,
                "path": str(publication.artifact_dir / first_entry.path),
                "hash": first_entry.sha256,
                "created_at": NOW.isoformat(),
            },
        )
    with pytest.raises(Exception, match="UNIQUE|unique"):
        registry.register_success(
            experiment_id,
            publication,
            metrics,
            request_id="rollback-success",
            now=NOW + timedelta(minutes=3),
        )

    detail = ExperimentQuery(engine).get(experiment_id)
    assert detail.record.status is ExperimentStatus.RUNNING
    assert detail.metrics == ()
    assert len(detail.artifacts) == 1
    assert "EXPERIMENT_SUCCEEDED" not in {
        audit["event_type"] for audit in _audit(engine, experiment_id)
    }


def test_register_success_rejects_identity_metric_mismatch_and_nonfinite_values(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bundle identity and exact finite metric equality are pre-transaction gates."""
    registry, experiment_id = _running_experiment(engine)
    publication = _publication(tmp_path, monkeypatch, experiment_id)
    metrics = _finite_metrics(publication)
    _, other = _running_experiment(engine, fingerprint="d" * 64)
    with pytest.raises(ValueError, match="experiment ID"):
        registry.register_success(other, publication, metrics)
    name = next(iter(metrics))
    with pytest.raises(ValueError, match="metrics.json"):
        registry.register_success(experiment_id, publication, {name: metrics[name] + 1.0})
    for invalid in (True, math.nan, math.inf):
        with pytest.raises((TypeError, ValueError), match="finite|bool"):
            registry.register_success(experiment_id, publication, {name: invalid})  # type: ignore[dict-item]
    zero_name = next(metric for metric, value in metrics.items() if value == 0.0)
    with pytest.raises(TypeError, match="float"):
        registry.register_success(
            experiment_id,
            publication,
            {zero_name: 0},  # type: ignore[dict-item]
        )
    assert ExperimentQuery(engine).get(experiment_id).record.status is ExperimentStatus.RUNNING


def test_register_success_rejects_experiment_without_snapshot_identity(
    engine: Engine,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful bundle cannot supply a snapshot absent from the registry row."""
    registry = ExperimentRegistry(engine)
    experiment_id = registry.create(
        _spec(fingerprint="8" * 64, snapshot_id=None),
        "8" * 64,
        request_id="snapshotless-create",
        now=NOW,
    )
    registry.transition(
        experiment_id,
        ExperimentStatus.CREATED,
        ExperimentStatus.QUEUED,
        request_id="snapshotless-queue",
        now=NOW + timedelta(seconds=1),
    )
    registry.transition(
        experiment_id,
        ExperimentStatus.QUEUED,
        ExperimentStatus.RUNNING,
        request_id="snapshotless-start",
        now=NOW + timedelta(seconds=2),
    )
    publication = _publication(tmp_path, monkeypatch, experiment_id)

    with pytest.raises(ValueError, match="snapshot identity"):
        registry.register_success(
            experiment_id, publication, _finite_metrics(publication)
        )

    assert ExperimentQuery(engine).get(experiment_id).record.status is ExperimentStatus.RUNNING


def test_research_update_normalizes_tags_and_preserves_note_in_audit(
    engine: Engine,
) -> None:
    """Only the research layer changes, atomically with old/new audit facts."""
    registry = ExperimentRegistry(engine)
    experiment_id = registry.create(
        _spec(), "f" * 64, request_id="research-create", now=NOW
    )
    before = ExperimentQuery(engine).get(experiment_id).record
    registry.update_research(
        experiment_id,
        ResearchMark.CANDIDATE,
        ["  alpha ", "quality", "alpha"],
        "Promising out-of-sample profile.",
        "reviewer",
        request_id="research-1",
        now=NOW + timedelta(minutes=1),
    )

    detail = ExperimentQuery(engine).get(experiment_id)
    assert detail.record.research_mark is ResearchMark.CANDIDATE
    assert detail.tags == ("alpha", "quality")
    assert detail.note == "Promising out-of-sample profile."
    assert detail.record.config == before.config
    assert detail.record.fingerprint == before.fingerprint
    audit = _audit(engine, experiment_id)[-1]
    assert audit["event_type"] == "EXPERIMENT_RESEARCH_UPDATED"
    assert audit["actor"] == "reviewer"
    assert audit["details"]["old_value"] == {
        "note": None,
        "research_mark": "UNREVIEWED",
        "tags": [],
    }
    assert audit["details"]["new_value"] == {
        "note": "Promising out-of-sample profile.",
        "research_mark": "CANDIDATE",
        "tags": ["alpha", "quality"],
    }
    assert audit["details"]["request_id"] == "research-1"

    with pytest.raises(ValueError, match="tag"):
        registry.update_research(
            experiment_id, ResearchMark.BASELINE, ["  "], "note", "reviewer"
        )
    with pytest.raises(ValueError, match="UTF-8|note"):
        registry.update_research(
            experiment_id, ResearchMark.BASELINE, [], "\ud800", "reviewer"
        )


def test_query_filters_all_tags_stable_pagination_duplicates_and_detail(
    engine: Engine,
) -> None:
    """Read APIs validate filters and assemble stable non-ORM DTO results."""
    registry = ExperimentRegistry(engine)
    first = registry.create(
        _spec(
            fingerprint="1" * 64,
            created_at=NOW,
            strategy_id="momentum",
            strategy_version="1",
        ),
        "1" * 64,
        request_id="create-query-0",
        now=NOW,
    )
    with pytest.warns(DuplicateResearchWarning):
        second = registry.create(
            _spec(
                fingerprint="1" * 64,
                created_at=NOW,
                strategy_id="momentum",
                strategy_version="2",
            ),
            "1" * 64,
            request_id="create-query-1",
            now=NOW,
        )
    third = registry.create(
        _spec(
            fingerprint="2" * 64,
            created_at=NOW - timedelta(days=1),
            strategy_id="value",
            strategy_version="1",
        ),
        "2" * 64,
        request_id="create-query-2",
        now=NOW - timedelta(days=1),
    )
    ids = [first, second, third]
    registry.update_research(
        ids[0], ResearchMark.CANDIDATE, ["alpha", "shared"], "first", "reviewer"
    )
    registry.update_research(
        ids[1], ResearchMark.BASELINE, ["beta", "shared"], "second", "reviewer"
    )
    registry.transition(ids[0], ExperimentStatus.CREATED, ExperimentStatus.QUEUED)
    query = ExperimentQuery(engine)

    assert [item.id for item in query.list()] == sorted(ids[:2], reverse=True) + [ids[2]]
    assert [item.id for item in query.list(limit=1, offset=1)] == [
        sorted(ids[:2], reverse=True)[1]
    ]
    assert {item.id for item in query.list(statuses=ExperimentStatus.QUEUED)} == {
        ids[0]
    }
    assert {item.id for item in query.list(statuses=(ExperimentStatus.CREATED,))} == {
        ids[1],
        ids[2],
    }
    assert {item.id for item in query.list(strategy_id="momentum")} == set(ids[:2])
    assert [item.id for item in query.list(strategy_id="momentum", strategy_version="2")] == [ids[1]]
    assert [item.id for item in query.list(research_mark=ResearchMark.CANDIDATE)] == [ids[0]]
    assert {item.id for item in query.list(tags=("shared",))} == set(ids[:2])
    assert [item.id for item in query.list(tags=("alpha", "shared"))] == [ids[0]]
    assert {item.id for item in query.list(created_from=NOW, created_to=NOW)} == set(ids[:2])
    assert {item.id for item in query.find_duplicates("1" * 64)} == set(ids[:2])
    detail = query.get(ids[1])
    assert detail.note == "second"
    assert detail.tags == ("beta", "shared")
    assert [entry.event_type for entry in detail.audit] == [
        "EXPERIMENT_CREATED",
        "EXPERIMENT_RESEARCH_UPDATED",
    ]
    assert not hasattr(detail.record, "_sa_instance_state")

    with pytest.raises(ValueError, match="limit"):
        query.list(limit=0)
    with pytest.raises(ValueError, match="offset"):
        query.list(offset=-1)
    with pytest.raises(ValueError, match="timezone-aware"):
        query.list(
            created_from=datetime(2026, 8, 2, tzinfo=UTC).replace(tzinfo=None)
        )
    with pytest.raises(ValueError, match="strategy_id"):
        query.list(strategy_version="1")
    with pytest.raises(ExperimentNotFound):
        query.get("missing")
