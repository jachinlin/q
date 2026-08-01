"""Streaming, validated, reproducible artifacts for a daily backtest."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from typing import Any
from uuid import UUID

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.backtest.accounting import AccountSnapshot
from quant_core.backtest.models import ExecutionBatch, FillResult
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


class BacktestArtifactWriter:
    """Append fixed-schema daily rows to isolated staging, then publish once."""

    def __init__(self, artifact_root: Path, experiment_id: UUID) -> None:
        if not isinstance(artifact_root, Path):
            raise TypeError("artifact_root must be a Path")
        if not isinstance(experiment_id, UUID):
            raise TypeError("experiment_id must be a UUID")
        artifact_root.mkdir(parents=True, exist_ok=True)
        self._root = artifact_root
        self._final_dir = artifact_root / f"experiment_id={experiment_id}"
        if (self._final_dir / "manifest.json").exists():
            raise FileExistsError("a successful artifact already exists for experiment")
        if self._final_dir.exists():
            raise FileExistsError("experiment artifact directory already exists")
        self._staging_dir = Path(
            tempfile.mkdtemp(prefix=f".staging-{experiment_id}-", dir=artifact_root)
        )
        self._writers = {
            name: pq.ParquetWriter(
                self._staging_dir / name, schema, compression=_COMPRESSION
            )
            for name, schema in _SCHEMAS.items()
        }
        self._closed = False
        self._published = False

    @property
    def staging_dir(self) -> Path:
        return self._staging_dir

    @property
    def artifact_dir(self) -> Path:
        return self._final_dir

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
        if self._closed:
            return
        first_error: BaseException | None = None
        for writer in self._writers.values():
            try:
                writer.close()
            except BaseException as error:  # noqa: BLE001 - close every writer.
                if first_error is None:
                    first_error = error
        self._closed = True
        if first_error is not None:
            raise first_error

    def abort(self, error: BaseException) -> None:
        close_error: BaseException | None = None
        try:
            self.close()
        except BaseException as caught:  # noqa: BLE001 - preserve original error.
            close_error = caught
        diagnostic: dict[str, object] = {
            "error_type": type(error).__name__,
            "message": str(error),
        }
        if close_error is not None:
            diagnostic["close_error_type"] = type(close_error).__name__
            diagnostic["close_error"] = str(close_error)
        _write_json(
            self._staging_dir / "diagnostic.json",
            diagnostic,
        )

    def validate(self, expected_sessions: int) -> dict[str, ArtifactEntry]:
        if not self._closed:
            raise ValueError("artifacts must be closed before validation")
        if type(expected_sessions) is not int or expected_sessions <= 0:
            raise ValueError("expected_sessions must be a positive integer")
        entries: dict[str, ArtifactEntry] = {}
        for name, expected_schema in _SCHEMAS.items():
            path = self._staging_dir / name
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
        if entries["nav.parquet"].row_count != expected_sessions:
            raise ValueError("nav row count must equal completed sessions")
        _validate_content(self._staging_dir)
        return entries

    def publish(self, manifest: dict[str, object]) -> Path:
        if not self._closed:
            raise ValueError("artifacts must be closed before publish")
        if self._published:
            raise ValueError("artifacts have already been published")
        if (self._final_dir / "manifest.json").exists() or self._final_dir.exists():
            raise FileExistsError("experiment artifact directory already exists")
        os.replace(self._staging_dir, self._final_dir)
        manifest_path = self._final_dir / "manifest.json"
        _write_json(manifest_path, manifest)
        self._published = True
        return manifest_path

    def _append(self, name: str, rows: list[dict[str, Any]]) -> None:
        if self._closed:
            raise ValueError("cannot append closed artifacts")
        table = pa.Table.from_pylist(rows, schema=_SCHEMAS[name])
        self._writers[name].write_table(table)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def _validate_content(staging_dir: Path) -> None:
    nav = pq.read_table(staging_dir / "nav.parquet").to_pylist()
    holdings = pq.read_table(staging_dir / "holdings.parquet").to_pylist()
    targets = pq.read_table(staging_dir / "targets.parquet").to_pylist()
    fills = pq.read_table(staging_dir / "fills.parquet").to_pylist()
    costs = pq.read_table(staging_dir / "costs.parquet").to_pylist()
    previous_date: object | None = None
    for row in nav:
        trade_date = row["trade_date"]
        if previous_date is not None and trade_date <= previous_date:
            raise ValueError("nav trade dates must be strictly ascending")
        previous_date = trade_date
        if row["nav_fen"] != row["cash_fen"] + row["market_value_fen"]:
            raise ValueError("nav identity is invalid")
        benchmark = row["benchmark_close"]
        if (
            not isinstance(benchmark, float)
            or not isfinite(benchmark)
            or benchmark <= 0
        ):
            raise ValueError("nav benchmark close is invalid")
    _validate_holdings(holdings)
    _validate_targets(targets)
    _validate_execution(fills, costs)


def _validate_holdings(rows: list[dict[str, Any]]) -> None:
    previous: tuple[object, str] | None = None
    for row in rows:
        key = (row["trade_date"], row["instrument_id"])
        if previous is not None and key <= previous:
            raise ValueError("holdings must be date and canonical-ID sorted uniquely")
        previous = key
        if row["total_quantity"] <= 0:
            raise ValueError("holdings must exclude zero quantities")
        if not 0 <= row["sellable_quantity"] <= row["total_quantity"]:
            raise ValueError("holding sellable quantity is invalid")
        if row["cost_basis_fen"] < 0 or row["market_value_fen"] < 0:
            raise ValueError("holding monetary values are invalid")


def _validate_targets(rows: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[object, object], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((row["signal_date"], row["execute_date"]), []).append(row)
    for rows_for_target in grouped.values():
        cash_rows = [row for row in rows_for_target if row["instrument_id"] is None]
        if len(cash_rows) != 1 or cash_rows[0]["reason_code"] != "CASH":
            raise ValueError("target requires exactly one CASH row")
        cash = cash_rows[0]
        positions = [row for row in rows_for_target if row["instrument_id"] is not None]
        if [row["position_index"] for row in positions] != list(range(len(positions))):
            raise ValueError("target position indexes are invalid")
        if cash["position_index"] != len(positions):
            raise ValueError("target CASH position index is invalid")
        if cash["target_weight"] != cash["cash_weight"] or cash["score"] is not None:
            raise ValueError("target CASH row is invalid")
        total = sum(row["target_weight"] for row in positions) + cash["cash_weight"]
        if abs(total - 1.0) > 1e-10:
            raise ValueError("target weights are invalid")


def _validate_execution(
    fills: list[dict[str, Any]], costs: list[dict[str, Any]]
) -> None:
    filled_keys: set[tuple[object, int]] = set()
    previous: tuple[object, int] | None = None
    for row in fills:
        key = (row["trade_date"], row["result_index"])
        if previous is not None and key <= previous:
            raise ValueError("fills must be ordered and uniquely indexed")
        previous = key
        if row["price"] is None:
            if row["filled_quantity"] != 0 or row["gross_value_fen"] != 0:
                raise ValueError("reject fill rows must have zero execution values")
            continue
        if row["filled_quantity"] <= 0 or row["unfilled_quantity"] < 0:
            raise ValueError("filled quantities are invalid")
        if (
            row["requested_quantity"]
            != row["filled_quantity"] + row["unfilled_quantity"]
        ):
            raise ValueError("fill quantities do not reconcile")
        filled_keys.add(key)
    cost_keys: set[tuple[object, int]] = set()
    for row in costs:
        key = (row["trade_date"], row["result_index"])
        if key in cost_keys or key not in filled_keys:
            raise ValueError("cost rows must map one-to-one to fills")
        cost_keys.add(key)
        if row["total_fees_fen"] != (
            row["commission_fen"] + row["stamp_tax_fen"] + row["transfer_fee_fen"]
        ):
            raise ValueError("cost fee identity is invalid")
    if cost_keys != filled_keys:
        raise ValueError("every fill requires one cost row")
