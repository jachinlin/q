"""原子发布固定 Schema 的 Run 回测产物。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

import polars as pl

from quant_research.data.contracts import JsonValue, canonical_json_bytes

_PARQUET_SCHEMAS: dict[str, dict[str, object]] = {
    "signals": {
        "signal_date": pl.Date,
        "instrument_id": pl.String,
        "signal": pl.String,
        "value": pl.Float64,
        "state_changed": pl.Boolean,
        "invalid_reason": pl.String,
    },
    "orders": {
        "signal_date": pl.Date,
        "execute_date": pl.Date,
        "order_index": pl.Int64,
        "instrument_id": pl.String,
        "side": pl.String,
        "quantity": pl.Int64,
        "reason": pl.String,
    },
    "fills": {
        "trade_date": pl.Date,
        "result_index": pl.Int64,
        "instrument_id": pl.String,
        "side": pl.String,
        "requested_quantity": pl.Int64,
        "filled_quantity": pl.Int64,
        "unfilled_quantity": pl.Int64,
        "reference_price": pl.Float64,
        "price": pl.Float64,
        "gross_value_fen": pl.Int64,
        "reason_code": pl.String,
    },
    "holdings": {
        "trade_date": pl.Date,
        "instrument_id": pl.String,
        "total_quantity": pl.Int64,
        "sellable_quantity": pl.Int64,
        "cost_basis_fen": pl.Int64,
        "market_value_fen": pl.Int64,
    },
    "costs": {
        "trade_date": pl.Date,
        "result_index": pl.Int64,
        "instrument_id": pl.String,
        "rule_fees_fen": pl.Int64,
        "slippage_fen": pl.Int64,
        "total_cost_fen": pl.Int64,
    },
    "nav": {
        "trade_date": pl.Date,
        "cash_fen": pl.Int64,
        "long_market_value_fen": pl.Int64,
        "short_market_value_fen": pl.Int64,
        "accrued_fees_fen": pl.Int64,
        "margin_used_fen": pl.Int64,
        "equity_fen": pl.Int64,
        "benchmark_close": pl.Float64,
    },
    "performance": {
        "trade_date": pl.Date,
        "return": pl.Float64,
        "cumulative_return": pl.Float64,
        "drawdown": pl.Float64,
    },
    "attribution": {
        "trade_date": pl.Date,
        "component": pl.String,
        "contribution": pl.Float64,
    },
}
_PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "signals": ("signal_date", "instrument_id"),
    "orders": ("signal_date", "order_index"),
    "fills": ("trade_date", "result_index"),
    "holdings": ("trade_date", "instrument_id"),
    "costs": ("trade_date", "result_index"),
    "nav": ("trade_date",),
    "performance": ("trade_date",),
    "attribution": ("trade_date", "component"),
}


class RunArtifactPublisher:
    """在同一文件系统暂存、复核并一次性发布 Run 产物。

    入参：产物根、实验 ID 和 Run ID。返回值：最终目录、Manifest 哈希及登记项。异常：路径越界、目录冲突或完整性失败时抛出错误。
    """

    def __init__(self, root: Path, experiment_id: str, run_id: str) -> None:
        self._final = (root / "experiments" / experiment_id / run_id).resolve()
        trusted = root.resolve()
        if trusted not in self._final.parents:
            raise ValueError("artifact directory escaped trusted root")
        if self._final.exists():
            raise FileExistsError("immutable Run artifact directory already exists")
        self._final.parent.mkdir(parents=True, exist_ok=True)
        self._staging = Path(
            tempfile.mkdtemp(prefix=f".staging-{run_id}-", dir=self._final.parent)
        )

    @property
    def staging_dir(self) -> Path:
        """返回失败时可安全清理的同文件系统暂存目录。

        入参：无。返回值：尚未发布的暂存路径。异常：无。
        """
        return self._staging

    def publish(
        self,
        tables: Mapping[str, Sequence[Mapping[str, object]] | pl.DataFrame],
        *,
        config: Mapping[str, JsonValue],
        metrics: Mapping[str, JsonValue],
        identities: Mapping[str, JsonValue],
    ) -> tuple[Path, str, tuple[dict[str, JsonValue], ...]]:
        """写入全部固定产物、生成 Manifest、原子发布并从最终目录复核。

        入参：固定类型表、冻结配置、指标和输入身份。返回值：最终目录、Manifest 哈希和产物登记元组。异常：Schema、写入、原子发布或复核失败时清理暂存并重抛。
        """
        try:
            entries: list[dict[str, JsonValue]] = []
            for name, schema in _PARQUET_SCHEMAS.items():
                value = tables.get(name, ())
                frame = (
                    value
                    if isinstance(value, pl.DataFrame)
                    else pl.DataFrame(value, schema=cast(Any, schema))
                )
                frame = self._normalize(frame, schema)
                path = self._staging / f"{name}.parquet"
                frame.write_parquet(path, compression="zstd")
                entries.append(self._entry(path, name, frame))
            for name, json_value in (("config", config), ("metrics", metrics)):
                path = self._staging / f"{name}.json"
                path.write_bytes(canonical_json_bytes(json_value))
                entries.append(self._entry(path, name, None))
            manifest = cast(
                dict[str, JsonValue],
                {
                    "identities": dict(identities),
                    "artifacts": entries,
                },
            )
            manifest_path = self._staging / "manifest.json"
            manifest_path.write_bytes(canonical_json_bytes(manifest))
            manifest_hash = self._hash(manifest_path)
            os.replace(self._staging, self._final)
            final_manifest = self._final / "manifest.json"
            if self._hash(final_manifest) != manifest_hash:
                raise ValueError("manifest changed during atomic publication")
            for entry in entries:
                path = self._final / cast(str, entry["relative_path"])
                if (
                    self._hash(path) != entry["content_hash"]
                    or path.stat().st_size != entry["byte_count"]
                ):
                    raise ValueError("artifact changed after publication")
                if path.suffix == ".parquet":
                    frame = pl.read_parquet(path)
                    if len(frame) != entry["row_count"]:
                        raise ValueError("artifact row count changed after publication")
                    expected_schema = entry["schema"]
                    actual_schema = {
                        name: str(dtype) for name, dtype in frame.schema.items()
                    }
                    if actual_schema != expected_schema:
                        raise ValueError("artifact schema changed after publication")
                    artifact_type = cast(str, entry["artifact_type"])
                    schema = _PARQUET_SCHEMAS[artifact_type]
                    keys = _PRIMARY_KEYS[artifact_type]
                    if frame.select(pl.struct(keys).is_duplicated().any()).item():
                        raise ValueError("artifact primary key is not unique")
                    if not frame.equals(self._normalize(frame, schema)):
                        raise ValueError("artifact rows are not canonically sorted")
                else:
                    canonical_json_bytes(cast(JsonValue, json.loads(path.read_bytes())))
            return self._final, manifest_hash, tuple(entries)
        except BaseException:
            if self._staging.exists():
                shutil.rmtree(self._staging, ignore_errors=True)
            if self._final.exists():
                shutil.rmtree(self._final, ignore_errors=True)
            raise

    @staticmethod
    def _normalize(frame: pl.DataFrame, schema: Mapping[str, object]) -> pl.DataFrame:
        missing = set(schema) - set(frame.columns)
        if missing:
            raise ValueError(f"artifact columns missing: {min(missing)}")
        output = frame.select(tuple(schema)).cast(cast(Any, schema))
        artifact_type = next(
            name for name, expected in _PARQUET_SCHEMAS.items() if expected is schema
        )
        sort_keys = list(_PRIMARY_KEYS[artifact_type])
        return output.sort(sort_keys) if sort_keys else output

    @staticmethod
    def _entry(
        path: Path, artifact_type: str, frame: pl.DataFrame | None
    ) -> dict[str, JsonValue]:
        return {
            "artifact_type": artifact_type,
            "relative_path": path.name,
            "content_hash": RunArtifactPublisher._hash(path),
            "byte_count": path.stat().st_size,
            "row_count": len(frame) if frame is not None else None,
            "schema": (
                {name: str(dtype) for name, dtype in frame.schema.items()}
                if frame is not None
                else None
            ),
            "primary_key": list(_PRIMARY_KEYS[artifact_type])
            if frame is not None
            else None,
            "sort_key": list(_PRIMARY_KEYS[artifact_type])
            if frame is not None
            else None,
        }

    @staticmethod
    def _hash(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()


__all__ = ["RunArtifactPublisher"]
