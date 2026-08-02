"""Streaming, validated, reproducible artifacts for a daily backtest."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from html.parser import HTMLParser
from math import isfinite
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import yaml

from quant_core.backtest.accounting import AccountSnapshot
from quant_core.backtest.models import ExecutionBatch, ExecutionConfig, FillResult
from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.data.partitions import _PartitionLock
from quant_core.domain.identifiers import InstrumentId
from quant_core.experiments.fingerprint import SourceIdentity
from quant_core.portfolio.constructor import TargetPortfolio

_COMPRESSION = "zstd"
_NAV_SCHEMA = pa.schema(
    [
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("cash_fen", pa.int64(), nullable=False),
        pa.field("market_value_fen", pa.int64(), nullable=False),
        pa.field("nav_fen", pa.int64(), nullable=False),
        pa.field("benchmark_close", pa.float64(), nullable=False),
    ]
)
_HOLDINGS_SCHEMA = pa.schema(
    [
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("total_quantity", pa.int64(), nullable=False),
        pa.field("sellable_quantity", pa.int64(), nullable=False),
        pa.field("cost_basis_fen", pa.int64(), nullable=False),
        pa.field("market_value_fen", pa.int64(), nullable=False),
    ]
)
_TARGETS_SCHEMA = pa.schema(
    [
        pa.field("signal_date", pa.date32(), nullable=False),
        pa.field("execute_date", pa.date32(), nullable=False),
        pa.field("position_index", pa.int32(), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=True),
        pa.field("target_weight", pa.float64(), nullable=False),
        pa.field("score", pa.float64(), nullable=True),
        pa.field("reason_code", pa.string(), nullable=False),
        pa.field("cash_weight", pa.float64(), nullable=False),
    ]
)
_FILLS_SCHEMA = pa.schema(
    [
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("result_index", pa.int32(), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("requested_quantity", pa.int64(), nullable=False),
        pa.field("filled_quantity", pa.int64(), nullable=False),
        pa.field("unfilled_quantity", pa.int64(), nullable=False),
        pa.field("price", pa.float64(), nullable=True),
        pa.field("gross_value_fen", pa.int64(), nullable=False),
        pa.field("reason_code", pa.string(), nullable=False),
        pa.field("detail", pa.string(), nullable=True),
    ]
)
_COSTS_SCHEMA = pa.schema(
    [
        pa.field("trade_date", pa.date32(), nullable=False),
        pa.field("result_index", pa.int32(), nullable=False),
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("commission_fen", pa.int64(), nullable=False),
        pa.field("stamp_tax_fen", pa.int64(), nullable=False),
        pa.field("transfer_fee_fen", pa.int64(), nullable=False),
        pa.field("total_fees_fen", pa.int64(), nullable=False),
    ]
)
_SCHEMAS = {
    "nav.parquet": _NAV_SCHEMA,
    "holdings.parquet": _HOLDINGS_SCHEMA,
    "targets.parquet": _TARGETS_SCHEMA,
    "fills.parquet": _FILLS_SCHEMA,
    "costs.parquet": _COSTS_SCHEMA,
}
FACTOR_METRICS_SCHEMA_VERSION = 1
_EXPERIMENT_MANIFEST_SCHEMA_VERSION = 1
FACTOR_METRICS_SCHEMA = pa.schema(
    [
        pa.field("factor_ref", pa.string(), nullable=False),
        pa.field("signal_date", pa.date32(), nullable=False),
        pa.field("metric_type", pa.string(), nullable=False),
        pa.field("metric_value", pa.float64(), nullable=True),
        pa.field("sample_count", pa.int64(), nullable=False),
        pa.field("is_valid", pa.bool_(), nullable=False),
        pa.field("quality_reason", pa.string(), nullable=True),
    ]
)
_ANALYTICS_ARTIFACT_NAMES = (
    "metrics.json",
    "drawdown.parquet",
    "monthly_returns.parquet",
    "exposure_summary.parquet",
    "factor_summary.parquet",
    "attribution.parquet",
    "quality_disclosure.json",
)
_EXPERIMENT_LAYER_NAMES = (
    "resolved_config.yaml",
    "environment.json",
    "factor_metrics.parquet",
    "report.html",
    "run.log",
)
_EXPERIMENT_PAYLOAD_NAMES = (
    *_SCHEMAS,
    *_ANALYTICS_ARTIFACT_NAMES,
    *_EXPERIMENT_LAYER_NAMES,
)
_EXPERIMENT_PARQUET_NAMES = {
    name for name in _EXPERIMENT_PAYLOAD_NAMES if name.endswith(".parquet")
}
_ENVIRONMENT_FIELDS = {
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


class WriterState(StrEnum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    VALIDATED = "VALIDATED"
    PUBLISHED = "PUBLISHED"
    ABORTED = "ABORTED"


@dataclass(frozen=True, slots=True)
class ArtifactEntry:
    path: str
    schema: str
    row_count: int
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        relative = Path(self.path)
        if (
            not self.path
            or relative.is_absolute()
            or ".." in relative.parts
            or len(relative.parts) != 1
        ):
            raise ValueError("artifact path must be a safe relative file path")
        if not isinstance(self.schema, str) or not self.schema:
            raise ValueError("artifact schema must be nonempty")
        for value, name in (
            (self.row_count, "row_count"),
            (self.size_bytes, "size_bytes"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if (
            not isinstance(self.sha256, str)
            or len(self.sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.sha256)
        ):
            raise ValueError("sha256 must be a lowercase SHA-256 hex digest")


@dataclass(frozen=True, slots=True)
class ExperimentArtifactEntry:
    """Integrity index entry for one file in the experiment success superset."""

    path: str
    size_bytes: int
    sha256: str
    schema: str | None = None
    row_count: int | None = None

    def __post_init__(self) -> None:
        _validate_safe_file_path(self.path)
        if type(self.size_bytes) is not int or self.size_bytes < 0:
            raise ValueError("size_bytes must be a nonnegative integer")
        if not _is_sha256(self.sha256):
            raise ValueError("sha256 must be a lowercase SHA-256 hex digest")
        parquet = self.path.endswith(".parquet")
        if parquet and (
            not isinstance(self.schema, str)
            or not self.schema
            or type(self.row_count) is not int
            or self.row_count < 0
        ):
            raise ValueError("Parquet entries require schema and row count")
        if not parquet and (self.schema is not None or self.row_count is not None):
            raise ValueError("non-Parquet entries cannot declare schema or row count")

    def manifest_value(self) -> dict[str, JsonValue]:
        """Return the exact versioned experiment-index representation."""
        value: dict[str, JsonValue] = {
            "path": self.path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
        }
        if self.schema is not None and self.row_count is not None:
            value["schema"] = self.schema
            value["row_count"] = self.row_count
        return value


@dataclass(frozen=True, slots=True)
class ExperimentArtifactPublication:
    """A fully validated immutable bundle ready for scalar registration."""

    artifact_dir: Path
    manifest_path: Path
    entries: dict[str, ExperimentArtifactEntry]
    manifest: dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class _PathIdentity:
    device: int
    inode: int
    mode: int
    size: int
    modified_ns: int
    file_attributes: int


@dataclass(frozen=True, slots=True)
class ManifestContext:
    experiment_id: UUID
    snapshot_id: UUID
    strategy_id: str
    strategy_version: str
    start_date: date
    end_date: date
    benchmark: InstrumentId
    initial_cash_fen: int
    rulebook_version: str
    execution_config: ExecutionConfig
    schema_version: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.experiment_id, UUID) or not isinstance(
            self.snapshot_id, UUID
        ):
            raise TypeError("manifest IDs must be UUID values")
        if any(
            not isinstance(value, str) or not value
            for value in (
                self.strategy_id,
                self.strategy_version,
                self.rulebook_version,
            )
        ):
            raise ValueError("manifest string identifiers must be nonempty")
        if (
            not isinstance(self.start_date, date)
            or isinstance(self.start_date, datetime)
            or not isinstance(self.end_date, date)
            or isinstance(self.end_date, datetime)
        ):
            raise TypeError("manifest dates must be dates")
        if self.start_date > self.end_date:
            raise ValueError("manifest start_date must not follow end_date")
        if not isinstance(self.benchmark, InstrumentId):
            raise TypeError("manifest benchmark must be an InstrumentId")
        if type(self.initial_cash_fen) is not int or self.initial_cash_fen < 0:
            raise ValueError("manifest initial_cash_fen must be nonnegative")
        if not isinstance(self.execution_config, ExecutionConfig):
            raise TypeError("manifest execution_config must be an ExecutionConfig")
        if self.schema_version != 1:
            raise ValueError("manifest schema_version must be 1")

    def build_manifest(
        self, entries: dict[str, ArtifactEntry], completed_sessions: int
    ) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "experiment_id": str(self.experiment_id),
            "snapshot_id": str(self.snapshot_id),
            "strategy": {
                "strategy_id": self.strategy_id,
                "version": self.strategy_version,
            },
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "benchmark": self.benchmark.canonical(),
            "initial_cash_fen": self.initial_cash_fen,
            "rulebook_version": self.rulebook_version,
            "execution_config": {
                "reference_price": self.execution_config.reference_price.value,
                "slippage_bps": self.execution_config.slippage_bps,
                "max_volume_participation": self.execution_config.max_volume_participation,
            },
            "completed_sessions": completed_sessions,
            "artifacts": {
                name: {
                    "path": entry.path,
                    "schema": entry.schema,
                    "row_count": entry.row_count,
                    "size_bytes": entry.size_bytes,
                    "sha256": entry.sha256,
                }
                for name, entry in entries.items()
            },
        }


class BacktestArtifactWriter:
    """Append fixed-schema daily rows to isolated staging, then publish once."""

    def __init__(self, artifact_root: Path, experiment_id: UUID) -> None:
        if not isinstance(artifact_root, Path):
            raise TypeError("artifact_root must be a Path")
        if not isinstance(experiment_id, UUID):
            raise TypeError("experiment_id must be a UUID")
        artifact_root.mkdir(parents=True, exist_ok=True)
        self._root = artifact_root
        self._final_dir_experiment_id = experiment_id
        self._final_dir = artifact_root / f"experiment_id={experiment_id}"
        if (self._final_dir / "manifest.json").exists():
            raise FileExistsError("a successful artifact already exists for experiment")
        if self._final_dir.exists():
            raise FileExistsError("experiment artifact directory already exists")
        self._staging_dir = Path(
            tempfile.mkdtemp(prefix=f".staging-{experiment_id}-", dir=artifact_root)
        )
        self._writers: dict[str, Any] = {}
        self._state = WriterState.OPEN
        self._entries: dict[str, ArtifactEntry] | None = None
        self._expected_sessions: tuple[date, ...] | None = None
        self._context: ManifestContext | None = None
        try:
            for name, schema in _SCHEMAS.items():
                self._writers[name] = pq.ParquetWriter(
                    self._staging_dir / name, schema, compression=_COMPRESSION
                )
        except BaseException as error:
            self._close_all()
            self._state = WriterState.ABORTED
            self._safe_diagnostic(error)
            raise

    @property
    def staging_dir(self) -> Path:
        return self._staging_dir

    @property
    def artifact_dir(self) -> Path:
        return self._final_dir

    @property
    def state(self) -> WriterState:
        return self._state

    def append_snapshot(
        self, snapshot: AccountSnapshot, benchmark_close: float
    ) -> None:
        if not isinstance(snapshot, AccountSnapshot):
            raise TypeError("snapshot must be an AccountSnapshot")
        if (
            not isinstance(benchmark_close, float)
            or not isfinite(benchmark_close)
            or benchmark_close <= 0
        ):
            raise ValueError("benchmark_close must be finite and positive")
        self._append(
            "nav.parquet",
            [
                {
                    "trade_date": snapshot.trade_date,
                    "cash_fen": snapshot.cash_fen,
                    "market_value_fen": snapshot.total_market_value_fen,
                    "nav_fen": snapshot.nav_fen,
                    "benchmark_close": benchmark_close,
                }
            ],
        )
        self._append(
            "holdings.parquet",
            [
                {
                    "trade_date": snapshot.trade_date,
                    "instrument_id": position.instrument_id.canonical(),
                    "total_quantity": position.total_quantity,
                    "sellable_quantity": position.sellable_quantity,
                    "cost_basis_fen": position.cost_basis_fen,
                    "market_value_fen": position.market_value_fen,
                }
                for position in snapshot.positions
                if position.total_quantity
            ],
        )

    def append_target(self, target: TargetPortfolio) -> None:
        rows: list[dict[str, Any]] = []
        for index, position in enumerate(target.positions):
            rows.append(
                {
                    "signal_date": target.signal_date,
                    "execute_date": target.execute_date,
                    "position_index": index,
                    "instrument_id": position.instrument_id.canonical(),
                    "target_weight": position.target_weight,
                    "score": position.score,
                    "reason_code": position.reason_code,
                    "cash_weight": target.cash_weight,
                }
            )
        rows.append(
            {
                "signal_date": target.signal_date,
                "execute_date": target.execute_date,
                "position_index": len(target.positions),
                "instrument_id": None,
                "target_weight": target.cash_weight,
                "score": None,
                "reason_code": "CASH",
                "cash_weight": target.cash_weight,
            }
        )
        self._append("targets.parquet", rows)

    def append_execution(self, execution: ExecutionBatch) -> None:
        fills: list[dict[str, Any]] = []
        costs: list[dict[str, Any]] = []
        for index, result in enumerate(execution.results):
            base = {
                "trade_date": execution.trade_date,
                "result_index": index,
                "instrument_id": result.intent.instrument_id.canonical(),
                "side": result.intent.side.value,
                "requested_quantity": result.requested_quantity,
                "reason_code": result.reason_code.value,
            }
            if isinstance(result, FillResult):
                fills.append(
                    {
                        **base,
                        "filled_quantity": result.filled_quantity,
                        "unfilled_quantity": result.unfilled_quantity,
                        "price": result.price,
                        "gross_value_fen": result.gross_value_fen,
                        "detail": None,
                    }
                )
                costs.append(
                    {
                        "trade_date": execution.trade_date,
                        "result_index": index,
                        "instrument_id": result.intent.instrument_id.canonical(),
                        "commission_fen": result.fees.commission_cents,
                        "stamp_tax_fen": result.fees.stamp_duty_cents,
                        "transfer_fee_fen": result.fees.transfer_fee_cents,
                        "total_fees_fen": result.fees.total_cents,
                    }
                )
            else:
                fills.append(
                    {
                        **base,
                        "filled_quantity": 0,
                        "unfilled_quantity": result.requested_quantity,
                        "price": None,
                        "gross_value_fen": 0,
                        "detail": result.detail,
                    }
                )
        self._append("fills.parquet", fills)
        self._append("costs.parquet", costs)

    def close(self) -> None:
        self._require(WriterState.OPEN, "close")
        first_error = self._close_all()
        self._state = WriterState.CLOSED
        if first_error is not None:
            raise first_error

    def abort(self, error: BaseException) -> None:
        if self._state is WriterState.PUBLISHED:
            return
        close_error = None
        if self._state is WriterState.OPEN:
            close_error = self._close_all()
        self._state = WriterState.ABORTED
        self._safe_diagnostic(error, close_error)

    def validate(
        self, expected_sessions: tuple[date, ...], context: ManifestContext
    ) -> dict[str, ArtifactEntry]:
        self._require(WriterState.CLOSED, "validate")
        if not isinstance(expected_sessions, tuple) or not expected_sessions:
            raise ValueError("expected_sessions must be a nonempty tuple of dates")
        if any(not isinstance(value, date) for value in expected_sessions):
            raise TypeError("expected_sessions must contain dates")
        if expected_sessions != tuple(sorted(expected_sessions)) or len(
            set(expected_sessions)
        ) != len(expected_sessions):
            raise ValueError("expected_sessions must be strictly ascending and unique")
        if not isinstance(context, ManifestContext):
            raise TypeError("context must be a ManifestContext")
        if context.experiment_id != self._final_dir_experiment_id:
            raise ValueError("manifest context experiment does not match writer")
        if (
            expected_sessions[0] < context.start_date
            or expected_sessions[-1] > context.end_date
        ):
            raise ValueError("manifest context does not cover sessions")
        entries = _collect_entries(self._staging_dir)
        _validate_content(self._staging_dir, expected_sessions)
        self._entries = entries
        self._expected_sessions = expected_sessions
        self._context = context
        self._state = WriterState.VALIDATED
        return dict(entries)

    def publish(self) -> Path:
        self._require(WriterState.VALIDATED, "publish")
        if (
            self._entries is None
            or self._expected_sessions is None
            or self._context is None
        ):
            raise RuntimeError("validated writer is missing cached state")
        entries = _collect_entries(self._staging_dir)
        _validate_content(self._staging_dir, self._expected_sessions)
        if entries != self._entries:
            raise ValueError("artifact bytes changed after validation")
        manifest = self._context.build_manifest(entries, len(self._expected_sessions))
        if (self._final_dir / "manifest.json").exists() or self._final_dir.exists():
            raise FileExistsError("experiment artifact directory already exists")
        os.replace(self._staging_dir, self._final_dir)
        manifest_path = self._final_dir / "manifest.json"
        try:
            _write_json(manifest_path, manifest)
        except BaseException as error:
            try:
                os.replace(self._final_dir, self._staging_dir)
            except BaseException as restore_error:  # noqa: BLE001
                error.add_note(f"failed to restore staging: {restore_error}")
            raise
        self._state = WriterState.PUBLISHED
        return manifest_path

    def _append(self, name: str, rows: list[dict[str, Any]]) -> None:
        self._require(WriterState.OPEN, "append")
        table = pa.Table.from_pylist(rows, schema=_SCHEMAS[name])
        self._writers[name].write_table(table)

    def _require(self, expected: WriterState, action: str) -> None:
        if self._state is not expected:
            raise ValueError(f"{action} requires {expected.value} state")

    def _close_all(self) -> BaseException | None:
        first_error: BaseException | None = None
        for writer in self._writers.values():
            try:
                writer.close()
            except BaseException as error:  # noqa: BLE001 - close every writer.
                if first_error is None:
                    first_error = error
        return first_error

    def _safe_diagnostic(
        self, error: BaseException, close_error: BaseException | None = None
    ) -> None:
        diagnostic: dict[str, object] = {
            "error_type": type(error).__name__,
            "message": str(error),
        }
        if close_error is not None:
            diagnostic["close_error_type"] = type(close_error).__name__
            diagnostic["close_error"] = str(close_error)
        try:
            _write_json(self._staging_dir / "diagnostic.json", diagnostic)
        except BaseException as diagnostic_error:  # noqa: BLE001
            error.add_note(f"failed to write diagnostic: {diagnostic_error}")


def validate_experiment_artifacts(
    artifact_dir: Path,
    *,
    resolved_config: Mapping[str, JsonValue],
) -> ExperimentArtifactPublication:
    """Deeply validate a visible experiment bundle and its success index."""
    if not isinstance(artifact_dir, Path):
        raise TypeError("artifact_dir must be a Path")
    if not artifact_dir.name.startswith("experiment_id="):
        raise ValueError("artifact_dir must have a published experiment identity")
    _require_plain_directory(artifact_dir.parent, "artifact_root")
    _require_plain_directory(artifact_dir, "artifact_dir")
    lock_path = artifact_dir.parent / ".experiment-publish.lock"
    with _PartitionLock(
        lock_path, timeout_seconds=60.0, stale_after_seconds=0.0
    ):
        manifest_path = artifact_dir / "manifest.json"
        raw, manifest = _read_experiment_manifest(manifest_path)
        if raw != canonical_json_bytes(cast(JsonValue, manifest)):
            raise ValueError("experiment manifest must be canonical UTF-8 JSON")
        entries = _validate_experiment_bundle(
            artifact_dir,
            manifest,
            resolved_config,
            marker_present=True,
            require_experiment_index=True,
        )
    return ExperimentArtifactPublication(
        artifact_dir=artifact_dir,
        manifest_path=manifest_path,
        entries=entries,
        manifest=cast(dict[str, JsonValue], manifest),
    )


def publish_experiment_artifacts(
    staging_dir: Path,
    artifact_root: Path,
    experiment_id: UUID,
    *,
    resolved_config: Mapping[str, JsonValue],
) -> ExperimentArtifactPublication:
    """Copy into an opaque candidate and publish it with one no-replace rename."""
    if not isinstance(staging_dir, Path) or not isinstance(artifact_root, Path):
        raise TypeError("staging_dir and artifact_root must be Paths")
    if not isinstance(experiment_id, UUID):
        raise TypeError("experiment_id must be a UUID")
    final_dir = artifact_root / f"experiment_id={experiment_id}"
    if staging_dir.name != final_dir.name:
        raise ValueError("staging directory identity does not match experiment_id")
    artifact_root.mkdir(parents=True, exist_ok=True)
    _require_plain_directory(artifact_root, "artifact_root")
    staging_identity = _require_plain_directory(staging_dir, "staging_dir")
    if _path_lexists(final_dir):
        raise FileExistsError("experiment artifact directory already exists")
    if staging_dir.resolve() == final_dir.resolve():
        raise ValueError("staging and final directories must be distinct")
    if staging_identity.device != artifact_root.stat().st_dev:
        raise ValueError("staging and final directories must share one filesystem")

    manifest_path = staging_dir / "manifest.json"
    _, manifest = _read_experiment_manifest(manifest_path)
    if "experiment" in manifest:
        raise ValueError("staging manifest already contains an experiment index")
    entries = _validate_experiment_bundle(
        staging_dir,
        manifest,
        resolved_config,
        marker_present=True,
        require_experiment_index=False,
        expected_experiment_id=experiment_id,
    )
    extended_manifest = dict(manifest)
    extended_manifest["experiment"] = {
        "schema_version": _EXPERIMENT_MANIFEST_SCHEMA_VERSION,
        "artifacts": {
            name: entries[name].manifest_value()
            for name in _EXPERIMENT_PAYLOAD_NAMES
        },
    }
    final_manifest_bytes = canonical_json_bytes(cast(JsonValue, extended_manifest))
    candidate_parent = Path(
        tempfile.mkdtemp(prefix=".experiment-candidate-", dir=artifact_root)
    )
    candidate_dir = candidate_parent / f".bundle-{uuid4().hex}"
    consumed_staging = candidate_parent / f".source-{uuid4().hex}"
    success_marker = candidate_parent / f".success-marker-{uuid4().hex}"
    candidate_dir.mkdir()
    candidate_identity = _require_plain_directory(candidate_dir, "candidate_dir")
    staging_consumed = False
    payload_committed = False
    try:
        _copy_bundle_no_follow(staging_dir, candidate_dir)
        _write_atomic_bytes(candidate_dir / "manifest.json", final_manifest_bytes)
        candidate_entries = _validate_private_candidate(
            candidate_dir,
            experiment_id,
            resolved_config,
            final_manifest_bytes,
        )
        if candidate_entries != entries:
            raise ValueError("experiment artifacts changed while building candidate")
        lock_path = artifact_root / ".experiment-publish.lock"
        with _PartitionLock(
            lock_path, timeout_seconds=60.0, stale_after_seconds=0.0
        ):
            _require_same_directory(candidate_dir, candidate_identity, "candidate")
            if _path_lexists(final_dir):
                raise FileExistsError("experiment artifact directory already exists")
            _validate_private_candidate(
                candidate_dir,
                experiment_id,
                resolved_config,
                final_manifest_bytes,
                expected_entries=entries,
            )
            _require_same_directory(staging_dir, staging_identity, "staging")
            _validate_experiment_file_set(staging_dir, marker_present=True)
            _atomic_directory_publish_no_replace(staging_dir, consumed_staging)
            staging_consumed = True
            _atomic_directory_publish_no_replace(
                candidate_dir / "manifest.json", success_marker
            )
            marker_identity = _require_plain_file(success_marker, "success marker")
            marker_manifest = _read_expected_manifest(
                success_marker, final_manifest_bytes
            )
            _validate_private_payload(
                candidate_dir,
                marker_manifest,
                experiment_id,
                resolved_config,
                expected_entries=entries,
            )
            _atomic_directory_publish_no_replace(candidate_dir, final_dir)
            payload_committed = True
            _require_same_file(success_marker, marker_identity, "success marker")
            marker_manifest = _read_expected_manifest(
                success_marker, final_manifest_bytes
            )
            _validate_private_payload(
                final_dir,
                marker_manifest,
                experiment_id,
                resolved_config,
                expected_entries=entries,
            )
            _require_same_file(success_marker, marker_identity, "success marker")
            if success_marker.read_bytes() != final_manifest_bytes:
                raise ValueError("success marker changed before publication")
            _atomic_directory_publish_no_replace(
                success_marker, final_dir / "manifest.json"
            )
    except BaseException as error:
        if not payload_committed and staging_consumed:
            try:
                _require_same_directory(
                    consumed_staging, staging_identity, "consumed staging"
                )
                if _path_lexists(staging_dir):
                    raise FileExistsError("staging path was recreated during recovery")
                _atomic_directory_publish_no_replace(consumed_staging, staging_dir)
                staging_consumed = False
            except BaseException as restore_error:  # noqa: BLE001
                error.add_note(f"failed to restore experiment staging: {restore_error}")
        if payload_committed or staging_consumed:
            _quarantine_manifest(candidate_dir, error)
            _quarantine_manifest(consumed_staging, error)
            _write_quarantine_diagnostic(candidate_parent, error)
        else:
            _remove_private_tree(candidate_parent, error)
        raise
    _remove_private_tree(consumed_staging)
    _remove_empty_directory(candidate_parent)
    return ExperimentArtifactPublication(
        artifact_dir=final_dir,
        manifest_path=final_dir / "manifest.json",
        entries=dict(entries),
        manifest=cast(dict[str, JsonValue], extended_manifest),
    )


def _validate_private_candidate(
    candidate_dir: Path,
    experiment_id: UUID,
    resolved_config: Mapping[str, JsonValue],
    expected_manifest_bytes: bytes,
    *,
    expected_entries: dict[str, ExperimentArtifactEntry] | None = None,
) -> dict[str, ExperimentArtifactEntry]:
    """Validate opaque candidate bytes without granting public identity."""
    _require_plain_directory(candidate_dir, "candidate_dir")
    raw, manifest = _read_experiment_manifest(candidate_dir / "manifest.json")
    if raw != expected_manifest_bytes:
        raise ValueError("candidate manifest changed before publication")
    if raw != canonical_json_bytes(cast(JsonValue, manifest)):
        raise ValueError("candidate manifest must be canonical UTF-8 JSON")
    entries = _validate_experiment_bundle(
        candidate_dir,
        manifest,
        resolved_config,
        marker_present=True,
        require_experiment_index=True,
        expected_experiment_id=experiment_id,
    )
    if expected_entries is not None and entries != expected_entries:
        raise ValueError("candidate artifacts changed before publication")
    return entries


def _read_expected_manifest(
    marker_path: Path, expected_manifest_bytes: bytes
) -> dict[str, Any]:
    raw, manifest = _read_experiment_manifest(marker_path)
    if raw != expected_manifest_bytes:
        raise ValueError("success marker changed before publication")
    if raw != canonical_json_bytes(cast(JsonValue, manifest)):
        raise ValueError("success marker must be canonical UTF-8 JSON")
    return manifest


def _validate_private_payload(
    payload_dir: Path,
    manifest: dict[str, Any],
    experiment_id: UUID,
    resolved_config: Mapping[str, JsonValue],
    *,
    expected_entries: dict[str, ExperimentArtifactEntry],
) -> None:
    """Validate payload bytes while no public success marker exists."""
    _require_plain_directory(payload_dir, "payload_dir")
    entries = _validate_experiment_bundle(
        payload_dir,
        manifest,
        resolved_config,
        marker_present=False,
        require_experiment_index=True,
        expected_experiment_id=experiment_id,
    )
    if entries != expected_entries:
        raise ValueError("candidate artifacts changed before marker publication")


def _copy_bundle_no_follow(source: Path, target: Path) -> None:
    """Copy one exact bundle into publisher-owned files without following aliases."""
    _validate_experiment_file_set(source, marker_present=True)
    expected = ("manifest.json", *_EXPERIMENT_PAYLOAD_NAMES)
    for name in expected:
        _copy_file_no_follow(source / name, target / name)
    _validate_experiment_file_set(target, marker_present=True)


def _copy_file_no_follow(source: Path, target: Path) -> None:
    before = _require_plain_file(source, source.name)
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    binary = getattr(os, "O_BINARY", 0)
    source_fd = os.open(source, os.O_RDONLY | nofollow | binary)
    target_fd: int | None = None
    try:
        opened = _identity_from_stat(os.fstat(source_fd))
        if opened != before:
            raise ValueError(f"experiment artifact {source.name} changed before copy")
        target_fd = os.open(
            target,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | binary,
            0o600,
        )
        while True:
            chunk = os.read(source_fd, 1024 * 1024)
            if not chunk:
                break
            view = memoryview(chunk)
            while view:
                written = os.write(target_fd, view)
                view = view[written:]
        os.fsync(target_fd)
        after = _identity_from_stat(os.fstat(source_fd))
        if after != before or _require_plain_file(source, source.name) != before:
            raise ValueError(f"experiment artifact {source.name} changed during copy")
    except BaseException:
        if target_fd is not None:
            os.close(target_fd)
            target_fd = None
        target.unlink(missing_ok=True)
        raise
    finally:
        os.close(source_fd)
        if target_fd is not None:
            os.close(target_fd)
    copied = _require_plain_file(target, target.name)
    if copied.inode == before.inode and copied.device == before.device:
        target.unlink(missing_ok=True)
        raise ValueError("publisher copy retained a source file alias")


def _require_plain_directory(path: Path, label: str) -> _PathIdentity:
    try:
        status = path.lstat()
    except OSError as error:
        raise ValueError(f"{label} must be an existing directory") from error
    if _is_reparse(status) or not stat.S_ISDIR(status.st_mode):
        raise ValueError(f"{label} must be a plain non-reparse directory")
    return _identity_from_stat(status)


def _require_plain_file(path: Path, label: str) -> _PathIdentity:
    try:
        status = path.lstat()
    except OSError as error:
        raise ValueError(f"experiment artifact {label} is missing") from error
    if _is_reparse(status) or not stat.S_ISREG(status.st_mode):
        raise ValueError(f"experiment artifact {label} must be a plain regular file")
    return _identity_from_stat(status)


def _identity_from_stat(status: os.stat_result) -> _PathIdentity:
    return _PathIdentity(
        device=status.st_dev,
        inode=status.st_ino,
        mode=status.st_mode,
        size=status.st_size,
        modified_ns=status.st_mtime_ns,
        file_attributes=getattr(status, "st_file_attributes", 0),
    )


def _is_reparse(status: os.stat_result) -> bool:
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return stat.S_ISLNK(status.st_mode) or bool(
        getattr(status, "st_file_attributes", 0) & reparse_flag
    )


def _require_same_directory(
    path: Path, expected: _PathIdentity, label: str
) -> None:
    actual = _require_plain_directory(path, label)
    if (actual.device, actual.inode) != (expected.device, expected.inode):
        raise ValueError(f"{label} directory identity changed")


def _require_same_file(path: Path, expected: _PathIdentity, label: str) -> None:
    if _require_plain_file(path, label) != expected:
        raise ValueError(f"{label} identity changed")


def _path_lexists(path: Path) -> bool:
    return os.path.lexists(path)


def _atomic_directory_publish_no_replace(source: Path, target: Path) -> None:
    """Atomically rename one filesystem object while refusing an existing target."""
    if _path_lexists(target):
        raise FileExistsError(f"publish target already exists: {target}")
    if os.name == "nt":
        os.rename(source, target)
        return

    import ctypes
    import errno

    if not hasattr(os, "uname") or os.uname().sysname != "Linux":
        raise RuntimeError("atomic no-replace directory publication is unsupported")
    library = cast(Any, ctypes.CDLL(None, use_errno=True))
    renameat2 = getattr(library, "renameat2", None)
    if renameat2 is None:
        raise RuntimeError("atomic no-replace directory publication is unavailable")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        -100,
        os.fsencode(source),
        -100,
        os.fsencode(target),
        1,
    )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(error_number, os.strerror(error_number), target)
    raise OSError(error_number, os.strerror(error_number), target)


def _quarantine_manifest(
    directory: Path, error: BaseException | None = None
) -> None:
    manifest = directory / "manifest.json"
    if not _path_lexists(manifest):
        return
    quarantine = directory / f".manifest.quarantine-{uuid4().hex}.json"
    try:
        os.replace(manifest, quarantine)
    except BaseException as quarantine_error:  # noqa: BLE001
        if error is not None:
            error.add_note(f"failed to quarantine manifest marker: {quarantine_error}")


def _write_quarantine_diagnostic(directory: Path, error: BaseException) -> None:
    try:
        _write_json(
            directory / "diagnostic.json",
            {"error_type": type(error).__name__, "message": str(error)},
        )
    except BaseException as diagnostic_error:  # noqa: BLE001
        error.add_note(f"failed to write quarantine diagnostic: {diagnostic_error}")


def _remove_private_tree(
    path: Path, error: BaseException | None = None
) -> None:
    if not _path_lexists(path):
        return
    try:
        _require_plain_directory(path, "private directory")
        shutil.rmtree(path)
    except BaseException as cleanup_error:  # noqa: BLE001
        if error is not None:
            error.add_note(f"failed to remove private directory: {cleanup_error}")


def _validate_experiment_bundle(
    artifact_dir: Path,
    manifest: dict[str, Any],
    resolved_config: Mapping[str, JsonValue],
    *,
    marker_present: bool,
    require_experiment_index: bool,
    expected_experiment_id: UUID | None = None,
) -> dict[str, ExperimentArtifactEntry]:
    _validate_experiment_file_set(artifact_dir, marker_present=marker_present)
    raw_entries = _collect_entries(artifact_dir)
    nav_dates = tuple(
        pq.read_table(artifact_dir / "nav.parquet", columns=["trade_date"])
        .column("trade_date")
        .to_pylist()
    )
    if not nav_dates or any(not isinstance(value, date) for value in nav_dates):
        raise ValueError("nav trade dates must be a nonempty sequence of dates")
    _validate_nav_timeline_against_manifest(
        cast(tuple[date, ...], nav_dates), manifest
    )
    _validate_content(artifact_dir, cast(tuple[date, ...], nav_dates))
    raw_manifest = {
        name: manifest[name]
        for name in (
            "schema_version",
            "experiment_id",
            "snapshot_id",
            "strategy",
            "start_date",
            "end_date",
            "benchmark",
            "initial_cash_fen",
            "rulebook_version",
            "execution_config",
            "completed_sessions",
            "artifacts",
        )
        if name in manifest
    }
    _validate_manifest(raw_manifest, raw_entries)

    from quant_core.analytics.materialize import validate_published_analytics

    validate_published_analytics(
        artifact_dir, manifest, expected_experiment_id=expected_experiment_id
    )
    _validate_resolved_config(artifact_dir / "resolved_config.yaml", resolved_config)
    _validate_environment(artifact_dir / "environment.json")
    _validate_factor_metrics(artifact_dir / "factor_metrics.parquet")
    _validate_report(artifact_dir / "report.html")
    _validate_utf8_text(artifact_dir / "run.log", "run.log")
    entries = _collect_experiment_entries(artifact_dir)
    experiment_index = manifest.get("experiment")
    if require_experiment_index or experiment_index is not None:
        _validate_experiment_index(experiment_index, entries)
    return entries


def _validate_nav_timeline_against_manifest(
    nav_dates: tuple[date, ...], manifest: dict[str, Any]
) -> None:
    if nav_dates != tuple(sorted(nav_dates)) or len(set(nav_dates)) != len(nav_dates):
        raise ValueError("nav trade dates must be strictly ascending and unique")
    completed = manifest.get("completed_sessions")
    if type(completed) is not int or completed != len(nav_dates):
        raise ValueError("nav timeline does not match completed_sessions")
    try:
        start = date.fromisoformat(cast(str, manifest.get("start_date")))
        end = date.fromisoformat(cast(str, manifest.get("end_date")))
    except (TypeError, ValueError) as error:
        raise ValueError("manifest date range is invalid") from error
    if start > end or nav_dates[0] < start or nav_dates[-1] > end:
        raise ValueError("nav timeline is outside manifest date range")


def _validate_experiment_file_set(artifact_dir: Path, *, marker_present: bool) -> None:
    _require_plain_directory(artifact_dir, "artifact_dir")
    expected = set(_EXPERIMENT_PAYLOAD_NAMES)
    if marker_present:
        expected.add("manifest.json")
    actual: set[str] = set()
    for path in artifact_dir.iterdir():
        _require_plain_file(path, path.name)
        actual.add(path.name)
    missing = sorted(expected - actual)
    if missing:
        raise ValueError(f"missing experiment artifact {missing[0]}")
    unexpected = sorted(actual - expected)
    if unexpected:
        raise ValueError(f"unexpected experiment artifact {unexpected[0]}")


def _collect_experiment_entries(
    artifact_dir: Path,
) -> dict[str, ExperimentArtifactEntry]:
    entries: dict[str, ExperimentArtifactEntry] = {}
    for name in _EXPERIMENT_PAYLOAD_NAMES:
        path = artifact_dir / name
        _require_plain_file(path, name)
        if name in _EXPERIMENT_PARQUET_NAMES:
            try:
                schema = pq.read_schema(path)
                row_count = pq.read_metadata(path).num_rows
            except Exception as error:
                raise ValueError(f"experiment artifact {name} is not valid Parquet") from error
            entries[name] = ExperimentArtifactEntry(
                name,
                path.stat().st_size,
                _sha256(path),
                schema.to_string(show_field_metadata=False),
                row_count,
            )
        else:
            entries[name] = ExperimentArtifactEntry(
                name, path.stat().st_size, _sha256(path)
            )
    return entries


def _validate_experiment_index(
    value: object, entries: dict[str, ExperimentArtifactEntry]
) -> None:
    if not isinstance(value, dict) or set(value) != {"schema_version", "artifacts"}:
        raise ValueError("experiment manifest index is invalid")
    if value.get("schema_version") != _EXPERIMENT_MANIFEST_SCHEMA_VERSION:
        raise ValueError("experiment manifest schema version is invalid")
    indexed = value.get("artifacts")
    if not isinstance(indexed, dict) or set(indexed) != set(entries):
        raise ValueError("experiment manifest artifacts are incomplete")
    for name, expected in entries.items():
        _validate_experiment_index_entry(name, indexed[name], expected)


def _validate_experiment_index_entry(
    name: str, value: object, expected: ExperimentArtifactEntry
) -> None:
    if not isinstance(value, dict):
        raise TypeError(f"experiment artifact entry {name} is invalid")
    expected_fields = {"path", "size_bytes", "sha256"}
    if name.endswith(".parquet"):
        expected_fields |= {"schema", "row_count"}
    if set(value) != expected_fields:
        raise ValueError(f"experiment artifact entry {name} fields are invalid")
    path = value.get("path")
    if not isinstance(path, str):
        raise TypeError("experiment artifact path must be a safe relative file path")
    _validate_safe_file_path(path)
    if path != name:
        raise ValueError("experiment artifact path must match its registered name")
    if value.get("size_bytes") != expected.size_bytes:
        raise ValueError(f"experiment artifact {name} size mismatch")
    if value.get("sha256") != expected.sha256:
        raise ValueError(f"experiment artifact {name} hash mismatch")
    if name.endswith(".parquet"):
        if value.get("schema") != expected.schema:
            raise ValueError(f"experiment artifact {name} schema mismatch")
        if value.get("row_count") != expected.row_count:
            raise ValueError(f"experiment artifact {name} row count mismatch")


def _validate_resolved_config(
    path: Path, resolved_config: Mapping[str, JsonValue]
) -> None:
    if not isinstance(resolved_config, Mapping):
        raise TypeError("resolved_config must be a mapping")
    try:
        text = path.read_bytes().decode("utf-8")
        loaded = yaml.safe_load(text)
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as error:
        raise ValueError("resolved_config.yaml must be safe UTF-8 YAML") from error
    if not isinstance(loaded, dict):
        raise TypeError("resolved_config.yaml YAML root must be a mapping")
    try:
        actual = canonical_json_bytes(cast(JsonValue, loaded))
        expected = canonical_json_bytes(resolved_config)
    except ValueError as error:
        raise ValueError("resolved_config.yaml YAML must contain finite JSON values") from error
    if actual != expected:
        raise ValueError("resolved_config.yaml YAML does not match resolved_config")


def _validate_environment(path: Path) -> None:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("environment.json must be valid UTF-8 JSON") from error
    if not isinstance(payload, dict) or set(payload) != _ENVIRONMENT_FIELDS:
        raise ValueError("environment.json environment fields are invalid")
    try:
        canonical = canonical_json_bytes(cast(JsonValue, payload))
    except ValueError as error:
        raise ValueError("environment.json environment values must be finite") from error
    if raw != canonical:
        raise ValueError("environment.json must be canonical UTF-8 JSON")
    if payload.get("schema_version") != 1:
        raise ValueError("environment.json schema version is invalid")
    lockfile_path = payload.get("lockfile_path")
    if not isinstance(lockfile_path, str):
        raise TypeError("environment.json lockfile path is invalid")
    relative = Path(lockfile_path)
    if (
        not lockfile_path
        or relative.is_absolute()
        or ".." in relative.parts
        or not _is_sha256(payload.get("lockfile_hash"))
    ):
        raise ValueError("environment.json lockfile identity is invalid")
    python_version = payload.get("python_version")
    if not isinstance(python_version, str) or not python_version:
        raise ValueError("environment.json Python version is invalid")
    try:
        SourceIdentity(
            cast(str, payload["source_identity_mode"]),
            cast(str, payload["source_hash"]),
            cast(str | None, payload["git_commit"]),
            cast(str | None, payload["source_tree_hash"]),
            cast(bool, payload["working_tree_dirty"]),
        )
    except (TypeError, ValueError) as error:
        raise ValueError("environment.json source identity is invalid") from error


def _validate_factor_metrics(path: Path) -> None:
    try:
        parquet = pq.ParquetFile(path)
        table = parquet.read()
    except Exception as error:
        raise ValueError("factor_metrics.parquet is not valid Parquet") from error
    if parquet.schema_arrow != FACTOR_METRICS_SCHEMA:
        raise ValueError("factor_metrics.parquet schema mismatch")
    for row in table.to_pylist():
        factor_ref = row["factor_ref"]
        metric_type = row["metric_type"]
        metric_value = row["metric_value"]
        sample_count = row["sample_count"]
        valid = row["is_valid"]
        reason = row["quality_reason"]
        if (
            not isinstance(factor_ref, str)
            or not factor_ref
            or not isinstance(metric_type, str)
            or not metric_type
            or type(sample_count) is not int
            or sample_count < 0
            or type(valid) is not bool
            or (
                metric_value is not None
                and (
                    not isinstance(metric_value, float) or not isfinite(metric_value)
                )
            )
        ):
            raise ValueError("factor_metrics.parquet content is invalid")
        if (valid and (metric_value is None or reason is not None)) or (
            not valid
            and (
                metric_value is not None
                or not isinstance(reason, str)
                or not reason
            )
        ):
            raise ValueError("factor_metrics.parquet validity fields are inconsistent")


class _HtmlDocumentParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.has_html_root = False

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag.casefold() == "html":
            self.has_html_root = True


def _validate_report(path: Path) -> None:
    text = _validate_utf8_text(path, "report.html")
    parser = _HtmlDocumentParser()
    try:
        parser.feed(text)
        parser.close()
    except Exception as error:
        raise ValueError("report.html must be valid UTF-8 HTML") from error
    if not text.strip() or not parser.has_html_root:
        raise ValueError("report.html must be nonempty UTF-8 HTML")


def _validate_utf8_text(path: Path, label: str) -> str:
    try:
        return path.read_bytes().decode("utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise ValueError(f"{label} must be UTF-8 text") from error


def _read_experiment_manifest(path: Path) -> tuple[bytes, dict[str, Any]]:
    if not path.is_file():
        raise ValueError("missing experiment artifact manifest.json")
    try:
        raw = path.read_bytes()
        parsed = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("experiment manifest must be valid UTF-8 JSON") from error
    if not isinstance(parsed, dict):
        raise TypeError("experiment manifest must be a JSON object")
    try:
        canonical_json_bytes(cast(JsonValue, parsed))
    except ValueError as error:
        raise ValueError("experiment manifest must contain finite JSON values") from error
    return raw, cast(dict[str, Any], parsed)


def _write_temporary_bytes(directory: Path, *, prefix: str, payload: bytes) -> Path:
    with tempfile.NamedTemporaryFile(
        mode="wb", prefix=prefix, suffix=".tmp", dir=directory, delete=False
    ) as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        return Path(handle.name)


def _write_atomic_bytes(path: Path, payload: bytes) -> None:
    temporary = _write_temporary_bytes(
        path.parent, prefix=f".{path.name}.restore-", payload=payload
    )
    try:
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _remove_empty_directory(
    path: Path, error: BaseException | None = None
) -> None:
    try:
        path.rmdir()
    except OSError as cleanup_error:
        if error is not None:
            error.add_note(f"failed to remove private candidate directory: {cleanup_error}")


def _validate_safe_file_path(value: str) -> None:
    relative = Path(value)
    if (
        not value
        or relative.is_absolute()
        or ".." in relative.parts
        or len(relative.parts) != 1
    ):
        raise ValueError("experiment artifact path must be a safe relative file path")


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _collect_entries(staging_dir: Path) -> dict[str, ArtifactEntry]:
    entries: dict[str, ArtifactEntry] = {}
    for name, expected_schema in _SCHEMAS.items():
        path = staging_dir / name
        if not path.is_file():
            raise ValueError(f"missing artifact {name}")
        parquet = pq.ParquetFile(path)
        if parquet.schema_arrow != expected_schema:
            raise ValueError(f"artifact schema mismatch for {name}")
        entries[name] = ArtifactEntry(
            name,
            expected_schema.to_string(show_field_metadata=False),
            parquet.metadata.num_rows,
            path.stat().st_size,
            _sha256(path),
        )
    return entries


def _write_json(path: Path, payload: dict[str, object]) -> None:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(encoded, encoding="utf-8")
    os.replace(temporary, path)


def _validate_manifest(
    manifest: object, entries: dict[str, ArtifactEntry] | None
) -> None:
    if entries is None or not isinstance(manifest, dict):
        raise ValueError("manifest must match validated artifacts")
    required = {
        "schema_version",
        "experiment_id",
        "snapshot_id",
        "strategy",
        "start_date",
        "end_date",
        "benchmark",
        "initial_cash_fen",
        "rulebook_version",
        "execution_config",
        "completed_sessions",
        "artifacts",
    }
    if set(manifest) != required:
        raise ValueError("manifest has invalid fields")
    if (
        type(manifest["schema_version"]) is not int
        or type(manifest["initial_cash_fen"]) is not int
        or type(manifest["completed_sessions"]) is not int
        or manifest["completed_sessions"] <= 0
        or any(
            not isinstance(manifest[name], str) or not manifest[name]
            for name in (
                "experiment_id",
                "snapshot_id",
                "start_date",
                "end_date",
                "benchmark",
                "rulebook_version",
            )
        )
        or not isinstance(manifest["strategy"], dict)
        or not isinstance(manifest["execution_config"], dict)
        or not isinstance(manifest["artifacts"], dict)
    ):
        raise ValueError("manifest metadata is invalid")
    strategy = manifest["strategy"]
    execution_config = manifest["execution_config"]
    if (
        set(strategy) != {"strategy_id", "version"}
        or any(not isinstance(value, str) or not value for value in strategy.values())
        or set(execution_config)
        != {"reference_price", "slippage_bps", "max_volume_participation"}
        or execution_config["reference_price"] not in {"OPEN", "CLOSE"}
        or any(
            not isinstance(execution_config[key], float)
            or not isfinite(execution_config[key])
            for key in ("slippage_bps", "max_volume_participation")
        )
    ):
        raise ValueError("manifest strategy or execution config is invalid")
    artifacts = manifest["artifacts"]
    if set(artifacts) != set(entries):
        raise ValueError("manifest artifacts do not match validation")
    for name, entry in entries.items():
        value = artifacts[name]
        expected = {
            "path": entry.path,
            "schema": entry.schema,
            "row_count": entry.row_count,
            "size_bytes": entry.size_bytes,
            "sha256": entry.sha256,
        }
        if value != expected:
            raise ValueError("manifest artifact entry does not match validation")


def _validate_content(staging_dir: Path, expected_sessions: tuple[date, ...]) -> None:
    nav = pq.read_table(staging_dir / "nav.parquet").to_pylist()
    holdings = pq.read_table(staging_dir / "holdings.parquet").to_pylist()
    targets = pq.read_table(staging_dir / "targets.parquet").to_pylist()
    fills = pq.read_table(staging_dir / "fills.parquet").to_pylist()
    costs = pq.read_table(staging_dir / "costs.parquet").to_pylist()
    if tuple(row["trade_date"] for row in nav) != expected_sessions:
        raise ValueError("nav trade dates must exactly equal expected sessions")
    nav_by_date: dict[date, dict[str, Any]] = {}
    for row in nav:
        trade_date = row["trade_date"]
        if not isinstance(trade_date, date):
            raise TypeError("nav trade dates are invalid")
        nav_by_date[trade_date] = row
        if row["nav_fen"] != row["cash_fen"] + row["market_value_fen"]:
            raise ValueError("nav identity is invalid")
        benchmark = row["benchmark_close"]
        if (
            not isinstance(benchmark, float)
            or not isfinite(benchmark)
            or benchmark <= 0
        ):
            raise ValueError("nav benchmark close is invalid")
    _validate_holdings(holdings, nav_by_date)
    _validate_targets(targets)
    _validate_execution(fills, costs)


def _validate_holdings(
    rows: list[dict[str, Any]], nav_by_date: dict[date, dict[str, Any]]
) -> None:
    previous: tuple[date, str] | None = None
    values_by_date: dict[date, int] = {trade_date: 0 for trade_date in nav_by_date}
    for row in rows:
        trade_date = row["trade_date"]
        raw_id = row["instrument_id"]
        if not isinstance(trade_date, date) or not isinstance(raw_id, str):
            raise TypeError("holding date or instrument is invalid")
        try:
            if InstrumentId.parse(raw_id).canonical() != raw_id:
                raise ValueError("holding instrument is not canonical")
        except (TypeError, ValueError) as error:
            raise ValueError("holding instrument is not canonical") from error
        key = (trade_date, raw_id)
        if previous is not None and key <= previous:
            raise ValueError("holdings must be date and canonical-ID sorted uniquely")
        previous = key
        if row["total_quantity"] <= 0:
            raise ValueError("holdings must exclude zero quantities")
        if not 0 <= row["sellable_quantity"] <= row["total_quantity"]:
            raise ValueError("holding sellable quantity is invalid")
        if row["cost_basis_fen"] < 0 or row["market_value_fen"] < 0:
            raise ValueError("holding monetary values are invalid")
        values_by_date[trade_date] += row["market_value_fen"]
    for trade_date, market_value in values_by_date.items():
        if market_value != nav_by_date[trade_date]["market_value_fen"]:
            raise ValueError("holdings market values must equal nav")


def _validate_targets(rows: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[object, object], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((row["signal_date"], row["execute_date"]), []).append(row)
    for rows_for_target in grouped.values():
        signal_date = rows_for_target[0]["signal_date"]
        execute_date = rows_for_target[0]["execute_date"]
        if (
            not isinstance(signal_date, date)
            or not isinstance(execute_date, date)
            or execute_date <= signal_date
        ):
            raise ValueError("target dates are invalid")
        cash_rows = [row for row in rows_for_target if row["instrument_id"] is None]
        if len(cash_rows) != 1 or cash_rows[0]["reason_code"] != "CASH":
            raise ValueError("target requires exactly one CASH row")
        cash = cash_rows[0]
        if rows_for_target[-1] is not cash:
            raise ValueError("target CASH row must be last")
        positions = [row for row in rows_for_target if row["instrument_id"] is not None]
        if [row["position_index"] for row in positions] != list(range(len(positions))):
            raise ValueError("target position indexes are invalid")
        if cash["position_index"] != len(positions):
            raise ValueError("target CASH position index is invalid")
        if cash["target_weight"] != cash["cash_weight"] or cash["score"] is not None:
            raise ValueError("target CASH row is invalid")
        seen: set[str] = set()
        for row in positions:
            raw_id = row["instrument_id"]
            weight = row["target_weight"]
            score = row["score"]
            if (
                not isinstance(raw_id, str)
                or raw_id in seen
                or not isinstance(weight, float)
                or not isfinite(weight)
                or weight < 0
                or (
                    score is not None
                    and (not isinstance(score, float) or not isfinite(score))
                )
            ):
                raise ValueError("target position is invalid")
            if (
                not isinstance(row["cash_weight"], float)
                or not isfinite(row["cash_weight"])
                or row["cash_weight"] < 0
                or row["cash_weight"] != cash["cash_weight"]
            ):
                raise ValueError("target position cash_weight is invalid")
            try:
                if InstrumentId.parse(raw_id).canonical() != raw_id:
                    raise ValueError("target instrument is not canonical")
            except (TypeError, ValueError) as error:
                raise ValueError("target instrument is not canonical") from error
            seen.add(raw_id)
        if (
            not isinstance(cash["cash_weight"], float)
            or not isfinite(cash["cash_weight"])
            or cash["cash_weight"] < 0
        ):
            raise ValueError("target cash weight is invalid")
        total = sum(row["target_weight"] for row in positions) + cash["cash_weight"]
        if abs(total - 1.0) > 1e-10:
            raise ValueError("target weights are invalid")


def _validate_execution(
    fills: list[dict[str, Any]], costs: list[dict[str, Any]]
) -> None:
    filled_instruments: dict[tuple[date, int], str] = {}
    previous: tuple[date, int] | None = None
    expected_index: dict[date, int] = {}
    for row in fills:
        trade_date = row["trade_date"]
        if not isinstance(trade_date, date) or not isinstance(row["result_index"], int):
            raise TypeError("execution index is invalid")
        key = (trade_date, row["result_index"])
        if previous is not None and key <= previous:
            raise ValueError("fills must be ordered and uniquely indexed")
        previous = key
        if row["result_index"] != expected_index.get(trade_date, 0):
            raise ValueError("execution result indexes must be contiguous")
        expected_index[trade_date] = row["result_index"] + 1
        try:
            if (
                InstrumentId.parse(row["instrument_id"]).canonical()
                != row["instrument_id"]
            ):
                raise ValueError("fill instrument is not canonical")
        except (TypeError, ValueError) as error:
            raise ValueError("fill instrument is not canonical") from error
        if row["price"] is None:
            if (
                row["filled_quantity"] != 0
                or row["gross_value_fen"] != 0
                or not isinstance(row["requested_quantity"], int)
                or row["requested_quantity"] <= 0
                or not isinstance(row["unfilled_quantity"], int)
                or row["unfilled_quantity"] < 0
                or row["requested_quantity"] != row["unfilled_quantity"]
            ):
                raise ValueError("reject fill rows must have zero execution values")
            continue
        if (
            not isinstance(row["price"], float)
            or not isfinite(row["price"])
            or row["price"] <= 0
            or row["gross_value_fen"] <= 0
            or not isinstance(row["requested_quantity"], int)
            or row["requested_quantity"] <= 0
            or row["filled_quantity"] <= 0
            or row["unfilled_quantity"] < 0
        ):
            raise ValueError("filled quantities are invalid")
        if (
            row["requested_quantity"]
            != row["filled_quantity"] + row["unfilled_quantity"]
        ):
            raise ValueError("fill quantities do not reconcile")
        filled_instruments[key] = row["instrument_id"]
    cost_keys: set[tuple[date, int]] = set()
    for row in costs:
        key = (row["trade_date"], row["result_index"])
        if key in cost_keys or key not in filled_instruments:
            raise ValueError("cost rows must map one-to-one to fills")
        raw_id = row["instrument_id"]
        try:
            if InstrumentId.parse(raw_id).canonical() != raw_id:
                raise ValueError("cost instrument is not canonical")
        except (TypeError, ValueError) as error:
            raise ValueError("cost instrument is not canonical") from error
        if raw_id != filled_instruments[key]:
            raise ValueError("cost instrument must match fill")
        cost_keys.add(key)
        if any(
            not isinstance(row[name], int) or row[name] < 0
            for name in (
                "commission_fen",
                "stamp_tax_fen",
                "transfer_fee_fen",
                "total_fees_fen",
            )
        ):
            raise ValueError("cost fees are invalid")
        if row["total_fees_fen"] != (
            row["commission_fen"] + row["stamp_tax_fen"] + row["transfer_fee_fen"]
        ):
            raise ValueError("cost fee identity is invalid")
    if cost_keys != set(filled_instruments):
        raise ValueError("every fill requires one cost row")
