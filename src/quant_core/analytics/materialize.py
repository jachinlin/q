"""Transactional publication of analytics beside canonical backtest artifacts."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import date
from math import isfinite
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.analytics.attribution import AttributionResult, calculate_attribution
from quant_core.analytics.performance import (
    METRICS_VERSION,
    PerformanceResult,
    calculate_performance,
)
from quant_core.domain.identifiers import InstrumentId

_SCHEMA_VERSION = 1
_RAW_SCHEMAS = {
    "nav.parquet": pa.schema(
        [
            pa.field("trade_date", pa.date32(), nullable=False),
            pa.field("cash_fen", pa.int64(), nullable=False),
            pa.field("market_value_fen", pa.int64(), nullable=False),
            pa.field("nav_fen", pa.int64(), nullable=False),
            pa.field("benchmark_close", pa.float64(), nullable=False),
        ]
    ),
    "holdings.parquet": pa.schema(
        [
            pa.field("trade_date", pa.date32(), nullable=False),
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("total_quantity", pa.int64(), nullable=False),
            pa.field("sellable_quantity", pa.int64(), nullable=False),
            pa.field("cost_basis_fen", pa.int64(), nullable=False),
            pa.field("market_value_fen", pa.int64(), nullable=False),
        ]
    ),
    "targets.parquet": pa.schema(
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
    ),
    "fills.parquet": pa.schema(
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
    ),
    "costs.parquet": pa.schema(
        [
            pa.field("trade_date", pa.date32(), nullable=False),
            pa.field("result_index", pa.int32(), nullable=False),
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("commission_fen", pa.int64(), nullable=False),
            pa.field("stamp_tax_fen", pa.int64(), nullable=False),
            pa.field("transfer_fee_fen", pa.int64(), nullable=False),
            pa.field("total_fees_fen", pa.int64(), nullable=False),
        ]
    ),
}
_ANALYTICS_SCHEMAS = {
    "drawdown.parquet": pa.schema(
        [
            pa.field("trade_date", pa.date32(), nullable=False),
            pa.field("nav", pa.float64(), nullable=False),
            pa.field("benchmark_nav", pa.float64(), nullable=False),
            pa.field("portfolio_daily_return", pa.float64(), nullable=False),
            pa.field("benchmark_daily_return", pa.float64(), nullable=False),
            pa.field("running_peak_nav", pa.float64(), nullable=False),
            pa.field("drawdown", pa.float64(), nullable=False),
        ]
    ),
    "monthly_returns.parquet": pa.schema(
        [
            pa.field("year", pa.int32(), nullable=False),
            pa.field("month", pa.int8(), nullable=False),
            pa.field("period_start", pa.date32(), nullable=False),
            pa.field("period_end", pa.date32(), nullable=False),
            pa.field("portfolio_return", pa.float64(), nullable=False),
            pa.field("benchmark_return", pa.float64(), nullable=False),
            pa.field("relative_return", pa.float64(), nullable=False),
        ]
    ),
    "exposure_summary.parquet": pa.schema(
        [
            pa.field("trade_date", pa.date32(), nullable=False),
            pa.field("dimension", pa.string(), nullable=False),
            pa.field("key", pa.string(), nullable=False),
            pa.field("weight", pa.float64(), nullable=False),
        ]
    ),
    "factor_summary.parquet": pa.schema(
        [
            pa.field("factor_ref", pa.string(), nullable=False),
            pa.field("observation_count", pa.int64(), nullable=False),
            pa.field("rank_ic_mean", pa.float64(), nullable=False),
            pa.field("rank_ic_std", pa.float64(), nullable=False),
            pa.field("top_quantile_return", pa.float64(), nullable=False),
            pa.field("bottom_quantile_return", pa.float64(), nullable=False),
            pa.field("quality_code", pa.string(), nullable=False),
        ]
    ),
    "attribution.parquet": pa.schema(
        [
            pa.field("trade_date", pa.date32(), nullable=False),
            pa.field("dimension", pa.string(), nullable=False),
            pa.field("key", pa.string(), nullable=False),
            pa.field("pnl_fen", pa.int64(), nullable=False),
            pa.field("contribution_return", pa.float64(), nullable=False),
        ]
    ),
}
_JSON_SCHEMAS = {
    "metrics.json": "quant.analytics.metrics.v1",
    "quality_disclosure.json": "quant.analytics.quality-disclosure.v1",
}
_METRIC_NUMBER_FIELDS = {
    "cumulative_return",
    "annualized_return",
    "annualized_volatility",
    "sharpe_ratio",
    "sortino_ratio",
    "max_drawdown",
    "calmar_ratio",
    "one_way_turnover",
    "fee_rate",
    "failed_fill_rate",
    "benchmark_cumulative_return",
    "relative_cumulative_return",
    "information_ratio",
}
_METRIC_DATE_FIELDS = {
    "start_date",
    "end_date",
    "max_drawdown_peak_date",
    "max_drawdown_trough_date",
    "max_drawdown_recovery_date",
}
_METRICS_FIELDS = {
    "metrics_version",
    "observations",
    "annual_returns",
    *_METRIC_NUMBER_FIELDS,
    *_METRIC_DATE_FIELDS,
}
_ANNUAL_RETURN_FIELDS = {
    "year",
    "period_start",
    "period_end",
    "portfolio_return",
    "benchmark_return",
    "relative_return",
}
_QUALITY_FIELDS = {
    "schema_version",
    "metrics_version",
    "calculation_mode",
    "undefined_metrics",
    "unavailable_dimensions",
    "attribution_method",
    "warnings",
}
_QUALITY_WARNINGS = {
    "FACTOR_EXPOSURE_NOT_AVAILABLE",
    "INDUSTRY_CLASSIFICATION_NOT_AVAILABLE",
    "STYLE_EXPOSURE_NOT_AVAILABLE",
}
_NEW_ARTIFACTS = (
    "metrics.json",
    "drawdown.parquet",
    "monthly_returns.parquet",
    "exposure_summary.parquet",
    "factor_summary.parquet",
    "attribution.parquet",
    "quality_disclosure.json",
)
_RAW_MANIFEST_FIELDS = {
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


@dataclass(frozen=True, slots=True)
class AnalyticsResult:
    artifact_dir: Path
    manifest_path: Path
    metrics_path: Path
    metrics_version: str


def materialize_analytics(artifact_dir: Path) -> AnalyticsResult:
    """Validate one published backtest and atomically add its analytics index."""
    if not isinstance(artifact_dir, Path):
        raise TypeError("artifact_dir must be a Path")
    if not artifact_dir.is_dir():
        raise ValueError("artifact_dir must be a published artifact directory")
    manifest_path = artifact_dir / "manifest.json"
    original_manifest_bytes = _read_manifest_bytes(manifest_path)
    manifest = _parse_manifest(original_manifest_bytes)
    raw_entries = _validate_raw_manifest(artifact_dir, manifest)
    if "analytics" in manifest:
        _validate_registered_analytics(artifact_dir, manifest["analytics"], raw_entries)
        return AnalyticsResult(
            artifact_dir, manifest_path, artifact_dir / "metrics.json", METRICS_VERSION
        )

    frames = {
        name: pl.read_parquet(artifact_dir / name)
        for name in (
            "nav.parquet",
            "holdings.parquet",
            "fills.parquet",
            "costs.parquet",
        )
    }
    performance = calculate_performance(
        frames["nav.parquet"], frames["fills.parquet"], frames["costs.parquet"]
    )
    attribution = calculate_attribution(
        frames["nav.parquet"],
        frames["holdings.parquet"],
        frames["fills.parquet"],
        frames["costs.parquet"],
    )
    staging = Path(
        tempfile.mkdtemp(
            prefix=f".analytics-{artifact_dir.name}-", dir=artifact_dir.parent
        )
    )
    _write_staging(staging, performance, attribution)
    new_entries = _validate_staged_analytics(staging)
    analytics_entries = {"nav.parquet": raw_entries["nav.parquet"], **new_entries}
    analytics_index: dict[str, object] = {
        "schema_version": _SCHEMA_VERSION,
        "metrics_version": METRICS_VERSION,
        "artifacts": analytics_entries,
    }
    _publish_staged_files(staging, artifact_dir, new_entries)
    if _read_manifest_bytes(manifest_path) != original_manifest_bytes:
        raise ValueError("manifest changed during analytics calculation")
    published_manifest = dict(manifest)
    published_manifest["analytics"] = analytics_index
    _publish_manifest(manifest_path, published_manifest)
    _validate_registered_analytics(artifact_dir, analytics_index, raw_entries)
    try:
        staging.rmdir()
    except OSError:
        pass
    return AnalyticsResult(
        artifact_dir, manifest_path, artifact_dir / "metrics.json", METRICS_VERSION
    )


def _read_manifest_bytes(path: Path) -> bytes:
    if not path.is_file():
        raise ValueError("artifact directory must contain manifest.json")
    return path.read_bytes()


def _parse_manifest(raw: bytes) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("manifest must be valid UTF-8 JSON") from error
    if not isinstance(parsed, dict):
        raise TypeError("manifest must be a JSON object")
    _require_json_finite(parsed, "manifest")
    return cast(dict[str, Any], parsed)


def _validate_raw_manifest(
    artifact_dir: Path, manifest: dict[str, Any]
) -> dict[str, dict[str, object]]:
    fields = set(manifest)
    if fields != _RAW_MANIFEST_FIELDS and fields != _RAW_MANIFEST_FIELDS | {
        "analytics"
    }:
        raise ValueError("manifest has invalid fields")
    if manifest.get("schema_version") != 1:
        raise ValueError("manifest schema_version must be 1")
    if (
        type(manifest.get("completed_sessions")) is not int
        or manifest["completed_sessions"] <= 0
    ):
        raise ValueError("manifest completed_sessions must be positive")
    for key in (
        "experiment_id",
        "snapshot_id",
        "start_date",
        "end_date",
        "benchmark",
        "rulebook_version",
    ):
        if not isinstance(manifest.get(key), str) or not manifest[key]:
            raise ValueError(f"manifest {key} must be nonempty")
    try:
        UUID(manifest["experiment_id"])
        UUID(manifest["snapshot_id"])
        start = date.fromisoformat(manifest["start_date"])
        end = date.fromisoformat(manifest["end_date"])
        InstrumentId.parse(manifest["benchmark"])
    except ValueError as error:
        raise ValueError("manifest identity is invalid") from error
    if start > end:
        raise ValueError("manifest date range is invalid")
    if artifact_dir.name != f"experiment_id={manifest['experiment_id']}":
        raise ValueError(
            "manifest experiment identity does not match artifact directory"
        )
    if (
        type(manifest.get("initial_cash_fen")) is not int
        or manifest["initial_cash_fen"] < 0
    ):
        raise ValueError("manifest initial_cash_fen is invalid")
    strategy = manifest.get("strategy")
    execution = manifest.get("execution_config")
    if (
        not isinstance(strategy, dict)
        or set(strategy) != {"strategy_id", "version"}
        or any(not isinstance(value, str) or not value for value in strategy.values())
    ):
        raise ValueError("manifest strategy is invalid")
    if not isinstance(execution, dict) or set(execution) != {
        "reference_price",
        "slippage_bps",
        "max_volume_participation",
    }:
        raise ValueError("manifest execution_config is invalid")
    slippage = execution["slippage_bps"]
    participation = execution["max_volume_participation"]
    if (
        execution["reference_price"] not in {"OPEN", "CLOSE"}
        or not isinstance(slippage, float)
        or not isfinite(slippage)
        or slippage < 0
        or not isinstance(participation, float)
        or not isfinite(participation)
        or participation <= 0
        or participation > 1
    ):
        raise ValueError("manifest execution_config values are invalid")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != set(_RAW_SCHEMAS):
        raise ValueError("manifest raw artifacts are invalid")
    entries: dict[str, dict[str, object]] = {}
    for name, schema in _RAW_SCHEMAS.items():
        entry = artifacts[name]
        entries[name] = _validate_parquet_entry(
            artifact_dir / name,
            entry,
            schema,
            error_label="raw artifact",
        )
    if entries["nav.parquet"]["row_count"] != manifest["completed_sessions"]:
        raise ValueError("raw artifact NAV row count does not match manifest")
    return entries


def _validate_parquet_entry(
    path: Path,
    entry: object,
    schema: pa.Schema,
    *,
    error_label: str,
) -> dict[str, object]:
    if not path.is_file() or not isinstance(entry, dict):
        raise ValueError(f"{error_label} is missing or unregistered")
    expected_keys = {"path", "schema", "row_count", "size_bytes", "sha256"}
    if set(entry) != expected_keys or entry.get("path") != path.name:
        raise ValueError(f"{error_label} manifest entry is invalid")
    digest = _sha256(path)
    if entry.get("size_bytes") != path.stat().st_size or entry.get("sha256") != digest:
        raise ValueError(f"{error_label} hash or size mismatch")
    try:
        actual_schema = pq.read_schema(path)
        row_count = pq.read_metadata(path).num_rows
    except Exception as error:
        raise ValueError(f"{error_label} is not valid Parquet") from error
    schema_text = schema.to_string(show_field_metadata=False)
    if actual_schema != schema or entry.get("schema") != schema_text:
        raise ValueError(f"{error_label} schema mismatch")
    if entry.get("row_count") != row_count:
        raise ValueError(f"{error_label} row count mismatch")
    return {
        "path": path.name,
        "schema": schema_text,
        "row_count": row_count,
        "size_bytes": path.stat().st_size,
        "sha256": digest,
    }


def _write_staging(
    staging: Path,
    performance: PerformanceResult,
    attribution: AttributionResult,
) -> None:
    metrics_payload: dict[str, object] = {
        "metrics_version": performance.metrics_version,
        **dict(performance.metrics),
        "annual_returns": _json_rows(performance.annual_returns),
    }
    quality_payload: dict[str, object] = {
        "schema_version": _SCHEMA_VERSION,
        "metrics_version": performance.metrics_version,
        "calculation_mode": "CASH_EXACT",
        "undefined_metrics": dict(performance.undefined_metrics),
        "unavailable_dimensions": {
            "factor": "UNAVAILABLE",
            "industry": "UNKNOWN",
            "style": "UNAVAILABLE",
        },
        "attribution_method": (
            "market_value_t-minus-market_value_t-1-plus-sell-gross-minus-buy-gross;"
            " residual-is-UNEXPLAINED"
        ),
        "warnings": list(attribution.disclosures),
    }
    _write_json(staging / "metrics.json", metrics_payload)
    _write_json(staging / "quality_disclosure.json", quality_payload)
    frames = {
        "drawdown.parquet": performance.drawdown,
        "monthly_returns.parquet": performance.monthly_returns,
        "exposure_summary.parquet": attribution.exposure_summary,
        "factor_summary.parquet": attribution.factor_summary,
        "attribution.parquet": attribution.attribution,
    }
    for name, schema in _ANALYTICS_SCHEMAS.items():
        _write_parquet(staging / name, frames[name], schema)


def _json_rows(frame: pl.DataFrame) -> list[dict[str, object]]:
    rows = frame.to_dicts()
    for row in rows:
        for name, value in tuple(row.items()):
            if isinstance(value, date):
                row[name] = value.isoformat()
    return cast(list[dict[str, object]], rows)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    path.write_text(encoded, encoding="utf-8")


def _write_parquet(path: Path, frame: pl.DataFrame, schema: pa.Schema) -> None:
    table = pa.Table.from_pylist(frame.to_dicts(), schema=schema)
    pq.write_table(table, path, compression="zstd")


def _validate_staged_analytics(staging: Path) -> dict[str, dict[str, object]]:
    entries: dict[str, dict[str, object]] = {}
    for name, schema in _ANALYTICS_SCHEMAS.items():
        path = staging / name
        entries[name] = _validate_parquet_entry(
            path,
            {
                "path": name,
                "schema": schema.to_string(show_field_metadata=False),
                "row_count": pq.read_metadata(path).num_rows,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256(path),
            },
            schema,
            error_label="staged analytics artifact",
        )
    for name, logical_schema in _JSON_SCHEMAS.items():
        path = staging / name
        payload = _read_json(path, f"staged analytics artifact {name}")
        entries[name] = {
            "path": name,
            "schema": logical_schema,
            "row_count": 1,
            "size_bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        if not isinstance(payload, dict):
            raise TypeError(f"staged analytics artifact {name} must be an object")
        if name == "metrics.json":
            _validate_metrics_payload(payload)
        else:
            _validate_quality_payload(payload)
    return entries


def _publish_staged_files(
    staging: Path,
    artifact_dir: Path,
    entries: dict[str, dict[str, object]],
) -> None:
    for name in _NEW_ARTIFACTS:
        staged = staging / name
        final = artifact_dir / name
        if final.exists():
            entry = entries[name]
            if (
                not final.is_file()
                or final.stat().st_size != entry["size_bytes"]
                or _sha256(final) != entry["sha256"]
            ):
                raise ValueError("unregistered analytics artifact conflict")
            staged.unlink()
            continue
        os.replace(staged, final)


def _publish_manifest(path: Path, manifest: dict[str, object]) -> None:
    encoded = json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    with tempfile.NamedTemporaryFile(
        mode="wb",
        prefix=".manifest.analytics-",
        suffix=".tmp",
        dir=path.parent,
        delete=False,
    ) as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
        temporary = Path(handle.name)
    os.replace(temporary, path)


def _validate_registered_analytics(
    artifact_dir: Path,
    analytics: object,
    raw_entries: dict[str, dict[str, object]],
) -> None:
    if not isinstance(analytics, dict) or set(analytics) != {
        "schema_version",
        "metrics_version",
        "artifacts",
    }:
        raise ValueError("registered analytics index is invalid")
    if (
        analytics.get("schema_version") != 1
        or analytics.get("metrics_version") != METRICS_VERSION
    ):
        raise ValueError("registered analytics version is invalid")
    entries = analytics.get("artifacts")
    expected_names = {"nav.parquet", *_NEW_ARTIFACTS}
    if not isinstance(entries, dict) or set(entries) != expected_names:
        raise ValueError("registered analytics artifacts are invalid")
    if entries["nav.parquet"] != raw_entries["nav.parquet"]:
        raise ValueError("registered analytics NAV entry is invalid")
    for name, schema in _ANALYTICS_SCHEMAS.items():
        _validate_parquet_entry(
            artifact_dir / name,
            entries[name],
            schema,
            error_label="analytics artifact",
        )
    payloads: dict[str, dict[str, Any]] = {}
    for name, logical_schema in _JSON_SCHEMAS.items():
        payloads[name] = _validate_json_entry(
            artifact_dir / name,
            entries[name],
            logical_schema,
            error_label="analytics artifact",
        )
    _validate_metrics_payload(payloads["metrics.json"])
    _validate_quality_payload(payloads["quality_disclosure.json"])
    null_metrics = {
        name
        for name in _METRIC_NUMBER_FIELDS | {"max_drawdown_recovery_date"}
        if payloads["metrics.json"][name] is None
    }
    undefined = payloads["quality_disclosure.json"]["undefined_metrics"]
    if not isinstance(undefined, dict) or set(undefined) != null_metrics:
        raise ValueError("metrics nulls must match quality undefined_metrics")


def _validate_json_entry(
    path: Path,
    entry: object,
    logical_schema: str,
    *,
    error_label: str,
) -> dict[str, Any]:
    if not path.is_file() or not isinstance(entry, dict):
        raise ValueError(f"{error_label} is missing or unregistered")
    expected = {
        "path": path.name,
        "schema": logical_schema,
        "row_count": 1,
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }
    if entry != expected:
        raise ValueError(f"{error_label} hash, schema, or size mismatch")
    payload = _read_json(path, error_label)
    if not isinstance(payload, dict):
        raise TypeError(f"{error_label} JSON must be an object")
    return cast(dict[str, Any], payload)


def _validate_metrics_payload(payload: dict[str, Any]) -> None:
    if set(payload) != _METRICS_FIELDS:
        raise ValueError("metrics logical schema has invalid fields")
    if payload["metrics_version"] != METRICS_VERSION:
        raise ValueError("metrics logical schema has invalid version")
    if type(payload["observations"]) is not int or payload["observations"] <= 0:
        raise ValueError("metrics logical schema has invalid observations")
    for name in _METRIC_NUMBER_FIELDS:
        value = payload[name]
        if value is not None and (
            not isinstance(value, (int, float)) or isinstance(value, bool)
        ):
            raise ValueError(f"metrics logical schema has invalid {name}")
    for name in _METRIC_DATE_FIELDS:
        value = payload[name]
        if value is None and name == "max_drawdown_recovery_date":
            continue
        if not isinstance(value, str):
            raise TypeError(f"metrics logical schema has invalid {name}")
        try:
            date.fromisoformat(value)
        except ValueError as error:
            raise ValueError(f"metrics logical schema has invalid {name}") from error
    if date.fromisoformat(payload["start_date"]) > date.fromisoformat(
        payload["end_date"]
    ):
        raise ValueError("metrics logical schema has invalid date range")
    annual = payload["annual_returns"]
    if not isinstance(annual, list):
        raise TypeError("metrics logical schema annual_returns must be a list")
    for row in annual:
        if not isinstance(row, dict) or set(row) != _ANNUAL_RETURN_FIELDS:
            raise ValueError("metrics logical schema has invalid annual return row")
        if type(row["year"]) is not int:
            raise ValueError("metrics logical schema has invalid annual return year")
        for name in ("period_start", "period_end"):
            if not isinstance(row[name], str):
                raise TypeError("metrics logical schema has invalid annual return date")
            try:
                date.fromisoformat(row[name])
            except ValueError as error:
                raise ValueError(
                    "metrics logical schema has invalid annual return date"
                ) from error
        for name in ("portfolio_return", "benchmark_return", "relative_return"):
            if not isinstance(row[name], (int, float)) or isinstance(row[name], bool):
                raise TypeError("metrics logical schema has invalid annual return")


def _validate_quality_payload(payload: dict[str, Any]) -> None:
    if set(payload) != _QUALITY_FIELDS:
        raise ValueError("quality disclosure logical schema has invalid fields")
    if (
        payload["schema_version"] != 1
        or payload["metrics_version"] != METRICS_VERSION
        or payload["calculation_mode"] != "CASH_EXACT"
    ):
        raise ValueError("quality disclosure logical schema has invalid version")
    undefined = payload["undefined_metrics"]
    if not isinstance(undefined, dict) or any(
        not isinstance(name, str) or not isinstance(reason, str) or not reason
        for name, reason in undefined.items()
    ):
        raise ValueError(
            "quality disclosure logical schema has invalid undefined metrics"
        )
    if payload["unavailable_dimensions"] != {
        "factor": "UNAVAILABLE",
        "industry": "UNKNOWN",
        "style": "UNAVAILABLE",
    }:
        raise ValueError("quality disclosure logical schema has invalid dimensions")
    method = payload["attribution_method"]
    if not isinstance(method, str) or "UNEXPLAINED" not in method:
        raise ValueError(
            "quality disclosure logical schema has invalid attribution method"
        )
    warnings = payload["warnings"]
    if not isinstance(warnings, list) or set(warnings) != _QUALITY_WARNINGS:
        raise ValueError("quality disclosure logical schema has invalid warnings")


def _read_json(path: Path, error_label: str) -> object:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"{error_label} must be valid UTF-8 JSON") from error
    _require_json_finite(value, error_label)
    return value


def _require_json_finite(value: object, label: str) -> None:
    if isinstance(value, float) and not isfinite(value):
        raise ValueError(f"{label} contains a nonfinite JSON number")
    if isinstance(value, dict):
        for nested in value.values():
            _require_json_finite(nested, label)
    elif isinstance(value, list):
        for nested in value:
            _require_json_finite(nested, label)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
