"""Streaming, validated, reproducible artifacts for a daily backtest."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from math import isfinite
from pathlib import Path
from typing import Any
from uuid import UUID

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.backtest.accounting import AccountSnapshot
from quant_core.backtest.models import ExecutionBatch, ExecutionConfig, FillResult
from quant_core.domain.identifiers import InstrumentId
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
