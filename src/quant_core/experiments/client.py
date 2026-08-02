"""Notebook-facing durable experiment submission and result access."""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Protocol, cast

import polars as pl
from sqlalchemy import Engine

from quant_core.backtest.engine import StrategyRef
from quant_core.backtest.rulebook import AShareRuleBook, MarketRuleBook
from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.domain.enums import Severity
from quant_core.errors import ErrorDetail, QuantError
from quant_core.experiments.config import (
    ExperimentSnapshotCatalog,
    resolve_experiment_yaml,
)
from quant_core.experiments.fingerprint import (
    ExperimentFingerprintInput,
    capture_environment,
    compute_fingerprint,
)
from quant_core.experiments.models import (
    ExperimentRecord,
    ExperimentSpec,
    ExperimentStatus,
)
from quant_core.experiments.query import ExperimentDetail, ExperimentQuery
from quant_core.experiments.registry import ExperimentRegistry
from quant_core.experiments.verification import validate_registered_publication
from quant_core.persistence.database import create_sqlite_engine, upgrade_database
from quant_core.persistence.repositories import MetadataRepository
from quant_core.settings import Settings
from quant_core.tasks.models import TaskRecord, TaskStatus
from quant_core.tasks.queue import TaskQueue

_TERMINAL_TASK_STATUSES = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
        TaskStatus.ORPHANED,
    }
)


class _ExperimentQuery(Protocol):
    def get(self, experiment_id: str) -> ExperimentDetail: ...


class _ExperimentRegistry(Protocol):
    def create(
        self,
        config: ExperimentSpec,
        fingerprint: str,
        *,
        actor: str = "system",
        request_id: str | None = None,
        now: datetime | None = None,
    ) -> str: ...


class _TaskQueue(Protocol):
    def submit_backtest(
        self,
        experiment_id: str,
        config_hash: str,
        *,
        priority: int = 0,
        actor: str = "notebook",
        request_id: str | None = None,
    ) -> str: ...

    def get(self, task_id: str) -> TaskRecord: ...


@dataclass(frozen=True, slots=True)
class ExperimentResult:
    """A deeply verified successful experiment publication."""

    _query: _ExperimentQuery
    _experiment_id: str

    def metrics(self) -> dict[str, JsonValue]:
        """Read the registered metrics payload for this experiment only."""
        try:
            payload = json.loads(self._registered_bytes("metrics.json"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(
                "registered metrics.json must be valid UTF-8 JSON"
            ) from error
        if not isinstance(payload, dict):
            raise TypeError("registered metrics.json must be a JSON object")
        canonical_json_bytes(cast(JsonValue, payload))
        return cast(dict[str, JsonValue], payload)

    def nav(self) -> pl.DataFrame:
        """Read the registered NAV table for this experiment only."""
        try:
            return pl.read_parquet(BytesIO(self._registered_bytes("nav.parquet")))
        except (OSError, pl.exceptions.PolarsError) as error:
            raise ValueError("registered nav.parquet cannot be read") from error

    def _registered_bytes(self, name: str) -> bytes:
        detail = self._query.get(self._experiment_id)
        if detail.record.status is not ExperimentStatus.SUCCEEDED:
            raise ValueError("experiment result requires SUCCEEDED status")
        publication = validate_registered_publication(detail)
        registered = {artifact.name: artifact for artifact in detail.artifacts}
        artifact = registered.get(name)
        entry = publication.entries.get(name)
        if artifact is None or entry is None:
            raise ValueError(f"successful experiment has no registered {name}")
        expected = (publication.artifact_dir / entry.path).resolve()
        if Path(artifact.path).resolve() != expected:
            raise ValueError(f"registered {name} path changed after verification")
        return _read_registered_file(expected, entry.size_bytes, entry.sha256)


class ExperimentClient:
    """Submit and inspect experiments without starting an in-process worker."""

    def __init__(
        self,
        *,
        registry: _ExperimentRegistry,
        query: _ExperimentQuery,
        queue: _TaskQueue,
        config_root: Path,
        catalog: ExperimentSnapshotCatalog,
        strategies: Mapping[StrategyRef, object],
        rulebook: MarketRuleBook,
        environment_factory: Callable[[], Mapping[str, JsonValue]],
        clock: Callable[[], datetime],
        sleeper: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] | None = None,
        owned_engine: Engine | None = None,
    ) -> None:
        if registry is None or query is None or queue is None:
            raise TypeError("registry, query, and queue must be supplied")
        if not isinstance(config_root, Path):
            raise TypeError("config_root must be a Path")
        if not isinstance(strategies, Mapping):
            raise TypeError("strategies must be a mapping")
        if not callable(environment_factory) or not callable(clock):
            raise TypeError("environment_factory and clock must be callable")
        self._registry = registry
        self._query = query
        self._queue = queue
        self._config_root = config_root
        self._catalog = catalog
        self._strategies = dict(strategies)
        self._rulebook = rulebook
        self._environment_factory = environment_factory
        self._clock = clock
        self._sleeper = sleeper or time.sleep
        self._monotonic = monotonic or time.monotonic
        self._owned_engine = owned_engine

    @classmethod
    def from_default_settings(cls) -> ExperimentClient:
        """Build local SDK services without constructing a worker or provider."""
        from quant_core.experiments.runtime import strategy_factories

        source_root = Path(__file__).resolve().parents[3]
        data_root_text = os.environ.get("QUANT_DATA_ROOT")
        if not data_root_text:
            raise QuantError(
                ErrorDetail(
                    code="CFG_DATA_ROOT_REQUIRED",
                    severity=Severity.FATAL,
                    message="QUANT_DATA_ROOT is required",
                    context={},
                    remediation="set QUANT_DATA_ROOT outside the source tree",
                    retryable=False,
                )
            )
        config_path = Path(
            os.environ.get("QUANT_CONFIG", source_root / "configs" / "base.yaml")
        )
        settings = Settings.load(
            config_path,
            data_root=Path(data_root_text),
            source_root=source_root,
        )
        upgrade_database(settings.state_db)
        engine = create_sqlite_engine(settings.state_db)
        repository = MetadataRepository(engine)
        config_root = source_root / "configs"
        rulebook = AShareRuleBook.load(config_root / "rules" / "a_share_v1.yaml")
        return cls(
            registry=ExperimentRegistry(engine),
            query=ExperimentQuery(engine),
            queue=TaskQueue(engine),
            config_root=config_root,
            catalog=repository,
            strategies=strategy_factories(),
            rulebook=rulebook,
            environment_factory=lambda: capture_environment(
                source_root, source_root / "uv.lock"
            ),
            clock=lambda: datetime.now(UTC),
            owned_engine=engine,
        )

    def close(self) -> None:
        """Dispose an engine created by ``from_default_settings`` once."""
        engine = self._owned_engine
        if engine is not None:
            self._owned_engine = None
            engine.dispose()

    def create_from_yaml(
        self,
        path: str | Path,
        *,
        actor: str = "notebook",
        request_id: str | None = None,
    ) -> ExperimentRecord:
        """Resolve one safe YAML config and register its immutable identity."""
        config_path = _config_path(path, self._config_root)
        resolved = resolve_experiment_yaml(
            config_path,
            config_root=self._config_root,
            catalog=self._catalog,
            strategies=self._strategies,
            rulebook=self._rulebook,
        )
        environment = _environment(self._environment_factory())
        mapping = resolved.mapping
        strategy_id = cast(str, mapping["strategy_id"])
        strategy_version = cast(str, mapping["strategy_version"])
        rulebook_version = cast(str, mapping["rulebook_version"])
        source_hash = cast(str, environment["source_hash"])
        lockfile_hash = cast(str, environment["lockfile_hash"])
        fingerprint = compute_fingerprint(
            ExperimentFingerprintInput(
                strategy_id=strategy_id,
                strategy_version=strategy_version,
                resolved_config=mapping,
                snapshot_manifest_hash=resolved.snapshot_manifest_hash,
                source_hash=source_hash,
                lockfile_hash=lockfile_hash,
                rulebook_version=rulebook_version,
            )
        )
        config_hash = hashlib.sha256(canonical_json_bytes(mapping)).hexdigest()
        created_at = self._clock()
        spec = ExperimentSpec(
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            config=mapping,
            config_hash=config_hash,
            snapshot_id=cast(str, mapping["snapshot_id"]),
            snapshot_manifest_hash=resolved.snapshot_manifest_hash,
            source_tree_hash=cast(str | None, environment["source_tree_hash"]),
            git_commit_hash=cast(str | None, environment["git_commit"]),
            lockfile_hash=lockfile_hash,
            rulebook_version=rulebook_version,
            fingerprint=fingerprint,
            created_at=created_at,
        )
        experiment_id = self._registry.create(
            spec,
            fingerprint,
            actor=actor,
            request_id=request_id,
            now=created_at,
        )
        return self._query.get(experiment_id).record

    def submit(
        self,
        experiment_id: str,
        *,
        priority: int = 0,
        actor: str = "notebook",
        request_id: str | None = None,
    ) -> TaskRecord:
        """Durably enqueue work and return immediately without executing it."""
        experiment = self._query.get(experiment_id).record
        task_id = self._queue.submit_backtest(
            experiment.id,
            experiment.config_hash,
            priority=priority,
            actor=actor,
            request_id=request_id,
        )
        return self._queue.get(task_id)

    def wait(
        self,
        task_id: str,
        *,
        poll_seconds: float = 2.0,
        timeout_seconds: float | None = None,
    ) -> TaskRecord:
        """Poll durable task state until terminal without running worker code."""
        poll = _positive_seconds(poll_seconds, "poll_seconds")
        timeout = (
            _positive_seconds(timeout_seconds, "timeout_seconds")
            if timeout_seconds is not None
            else None
        )
        deadline = self._monotonic() + timeout if timeout is not None else None
        while True:
            task = self._queue.get(task_id)
            if task.status in _TERMINAL_TASK_STATUSES:
                return task
            if deadline is None:
                self._sleeper(poll)
                continue
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise TimeoutError(f"task did not finish before timeout: {task_id}")
            self._sleeper(min(poll, remaining))

    def result(self, experiment_id: str) -> ExperimentResult:
        """Return a result only after status and registered artifacts verify."""
        detail = self._query.get(experiment_id)
        if detail.record.status is not ExperimentStatus.SUCCEEDED:
            raise ValueError("experiment result requires SUCCEEDED status")
        validate_registered_publication(detail)
        return ExperimentResult(self._query, experiment_id)


def _positive_seconds(value: float, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{label} must be a finite positive number")
    return float(value)


def _config_path(value: str | Path, config_root: Path) -> Path:
    if not isinstance(value, (str, Path)):
        raise TypeError("experiment config path must be text or a Path")
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate
    if candidate.parts and candidate.parts[0] == config_root.name:
        return config_root.parent / candidate
    return config_root / candidate


def _environment(value: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    if not isinstance(value, Mapping):
        raise TypeError("environment_factory must return a mapping")
    encoded = canonical_json_bytes(value)
    plain = json.loads(encoded)
    expected = {
        "schema_version",
        "source_identity_mode",
        "source_hash",
        "git_commit",
        "source_tree_hash",
        "working_tree_dirty",
        "lockfile_path",
        "lockfile_hash",
        "python_version",
    }
    if not isinstance(plain, dict) or set(plain) != expected:
        raise ValueError("environment_factory returned invalid fields")
    return cast(dict[str, JsonValue], plain)


def _read_registered_file(path: Path, size_bytes: int, sha256: str) -> bytes:
    """Read one regular file without following a swapped filesystem identity."""
    try:
        before = path.stat(follow_symlinks=False)
    except OSError as error:
        raise ValueError("registered artifact cannot be inspected") from error
    if not stat.S_ISREG(before.st_mode) or _is_reparse(before):
        raise ValueError("registered artifact must be a plain regular file")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise ValueError("registered artifact cannot be opened safely") from error
    try:
        opened = os.fstat(descriptor)
        if _file_identity(opened) != _file_identity(before):
            raise ValueError("registered artifact changed before open")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if _file_identity(after) != _file_identity(opened):
        raise ValueError("registered artifact changed while being read")
    payload = b"".join(chunks)
    if len(payload) != size_bytes or hashlib.sha256(payload).hexdigest() != sha256:
        raise ValueError("registered artifact size or hash changed")
    return payload


def _file_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        int(getattr(value, "st_file_attributes", 0)),
    )


def _is_reparse(value: os.stat_result) -> bool:
    return bool(int(getattr(value, "st_file_attributes", 0)) & 0x400)


__all__ = [
    "ExperimentClient",
    "ExperimentResult",
    "validate_registered_publication",
]
