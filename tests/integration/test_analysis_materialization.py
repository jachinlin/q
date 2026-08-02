"""Analytics attribution and publication contract tests."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import quant_core.analytics.materialize as materialize_module
from quant_core.analytics.attribution import calculate_attribution
from quant_core.analytics.materialize import materialize_analytics
from quant_core.backtest.engine import BacktestEngine
from quant_core.portfolio import RebalancePlanner
from tests.integration.test_backtest_timeline import (
    _Data,
    _NeverCancelled,
    _Progress,
    _request,
    _RuleBook,
    _Targets,
)

NAV_SCHEMA = {
    "trade_date": pl.Date,
    "cash_fen": pl.Int64,
    "market_value_fen": pl.Int64,
    "nav_fen": pl.Int64,
    "benchmark_close": pl.Float64,
}
HOLDINGS_SCHEMA = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "total_quantity": pl.Int64,
    "sellable_quantity": pl.Int64,
    "cost_basis_fen": pl.Int64,
    "market_value_fen": pl.Int64,
}
FILLS_SCHEMA = {
    "trade_date": pl.Date,
    "result_index": pl.Int32,
    "instrument_id": pl.String,
    "side": pl.String,
    "requested_quantity": pl.Int64,
    "filled_quantity": pl.Int64,
    "unfilled_quantity": pl.Int64,
    "price": pl.Float64,
    "gross_value_fen": pl.Int64,
    "reason_code": pl.String,
    "detail": pl.String,
}
COSTS_SCHEMA = {
    "trade_date": pl.Date,
    "result_index": pl.Int32,
    "instrument_id": pl.String,
    "commission_fen": pl.Int64,
    "stamp_tax_fen": pl.Int64,
    "transfer_fee_fen": pl.Int64,
    "total_fees_fen": pl.Int64,
}
EXPOSURE_SCHEMA = {
    "trade_date": pl.Date,
    "dimension": pl.String,
    "key": pl.String,
    "weight": pl.Float64,
}
FACTOR_SCHEMA = {
    "factor_ref": pl.String,
    "observation_count": pl.Int64,
    "rank_ic_mean": pl.Float64,
    "rank_ic_std": pl.Float64,
    "top_quantile_return": pl.Float64,
    "bottom_quantile_return": pl.Float64,
    "quality_code": pl.String,
}
ATTRIBUTION_SCHEMA = {
    "trade_date": pl.Date,
    "dimension": pl.String,
    "key": pl.String,
    "pnl_fen": pl.Int64,
    "contribution_return": pl.Float64,
}


def _attribution_inputs() -> tuple[pl.DataFrame, ...]:
    first = date(2024, 1, 2)
    second = date(2024, 1, 3)
    instruments = [f"SSE:{600_001 + index}" for index in range(22)]
    nav = pl.DataFrame(
        {
            "trade_date": [first, second],
            "cash_fen": [10_000, 7_800],
            "market_value_fen": [0, 2_222],
            "nav_fen": [10_000, 10_022],
            "benchmark_close": [100.0, 100.0],
        },
        schema=NAV_SCHEMA,
    )
    holdings = pl.DataFrame(
        {
            "trade_date": [second] * 22,
            "instrument_id": instruments,
            "total_quantity": [1] * 22,
            "sellable_quantity": [0] * 22,
            "cost_basis_fen": [100] * 22,
            "market_value_fen": [101] * 22,
        },
        schema=HOLDINGS_SCHEMA,
    )
    fills = pl.DataFrame(
        {
            "trade_date": [second] * 22,
            "result_index": list(range(22)),
            "instrument_id": instruments,
            "side": ["BUY"] * 22,
            "requested_quantity": [1] * 22,
            "filled_quantity": [1] * 22,
            "unfilled_quantity": [0] * 22,
            "price": [1.0] * 22,
            "gross_value_fen": [100] * 22,
            "reason_code": ["FILLED"] * 22,
            "detail": [None] * 22,
        },
        schema=FILLS_SCHEMA,
    )
    costs = pl.DataFrame(
        {
            "trade_date": [second] * 22,
            "result_index": list(range(22)),
            "instrument_id": instruments,
            "commission_fen": [0] * 22,
            "stamp_tax_fen": [0] * 22,
            "transfer_fee_fen": [0] * 22,
            "total_fees_fen": [0] * 22,
        },
        schema=COSTS_SCHEMA,
    )
    return nav, holdings, fills, costs


def test_attribution_conserves_daily_return_and_bounds_security_detail() -> None:
    """Residual smearing or unbounded holdings detail breaks the daily cash identity."""
    nav, holdings, fills, costs = _attribution_inputs()

    result = calculate_attribution(nav, holdings, fills, costs)

    assert result.exposure_summary.schema == pl.Schema(EXPOSURE_SCHEMA)
    assert result.factor_summary.schema == pl.Schema(FACTOR_SCHEMA)
    assert result.factor_summary.is_empty()
    assert result.attribution.schema == pl.Schema(ATTRIBUTION_SCHEMA)
    assert result.attribution.equals(
        result.attribution.sort(["trade_date", "dimension", "key"])
    )
    assert (
        result.attribution.unique(["trade_date", "dimension", "key"]).height
        == result.attribution.height
    )

    daily_returns = {date(2024, 1, 2): 0.0, date(2024, 1, 3): 0.0022}
    totals = result.attribution.group_by(["trade_date", "dimension"]).agg(
        pl.col("contribution_return").sum().alias("total")
    )
    for row in totals.iter_rows(named=True):
        assert row["total"] == pytest.approx(daily_returns[row["trade_date"]])

    security = result.attribution.filter(
        (pl.col("trade_date") == date(2024, 1, 3)) & (pl.col("dimension") == "SECURITY")
    )
    security_keys = set(security["key"].to_list())
    assert "OTHER" in security_keys
    assert "UNEXPLAINED" in security_keys
    assert security.filter(pl.col("key") == "OTHER")["pnl_fen"].item() == 2
    assert security.filter(~pl.col("key").is_in(["OTHER", "UNEXPLAINED"])).height == 20

    industry = result.attribution.filter(pl.col("dimension") == "INDUSTRY")
    style = result.attribution.filter(pl.col("dimension") == "STYLE")
    assert set(industry["key"].to_list()) == {"UNKNOWN", "UNEXPLAINED"}
    assert set(style["key"].to_list()) == {"UNAVAILABLE", "UNEXPLAINED"}
    assert set(result.disclosures) == {
        "FACTOR_EXPOSURE_NOT_AVAILABLE",
        "INDUSTRY_CLASSIFICATION_NOT_AVAILABLE",
        "STYLE_EXPOSURE_NOT_AVAILABLE",
    }


def test_holdings_schema_and_identity_fail_closed() -> None:
    """Attribution must not accept a holdings table detached from canonical NAV."""
    nav, holdings, fills, costs = _attribution_inputs()
    wrong_schema = holdings.with_columns(pl.col("market_value_fen").cast(pl.Int32))
    with pytest.raises(ValueError, match="holdings schema"):
        calculate_attribution(nav, wrong_schema, fills, costs)

    wrong_identity = holdings.with_columns(
        pl.when(pl.col("instrument_id") == "SSE:600001")
        .then(102)
        .otherwise(pl.col("market_value_fen"))
        .alias("market_value_fen")
    )
    with pytest.raises(ValueError, match="market value identity"):
        calculate_attribution(nav, wrong_identity, fills, costs)


def _published_backtest(root: Path) -> Path:
    result = BacktestEngine(
        _Data(), _Targets(), _RuleBook(), RebalancePlanner(), artifact_root=root
    ).run(_request(), _Progress(), _NeverCancelled())
    return result.artifact_dir


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_materialize_publishes_exact_analytics_and_is_idempotent(
    tmp_path: Path,
) -> None:
    """Partial files, schema drift, or raw rewrites cannot masquerade as analytics."""
    artifact_dir = _published_backtest(tmp_path)
    manifest_path = artifact_dir / "manifest.json"
    raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw_hashes = {
        name: _sha256(artifact_dir / name) for name in raw_manifest["artifacts"]
    }

    first = materialize_analytics(artifact_dir)
    first_manifest_bytes = manifest_path.read_bytes()
    second = materialize_analytics(artifact_dir)

    assert first == second
    assert first.metrics_version == "1.0.0"
    assert first.manifest_path == manifest_path
    assert first.metrics_path == artifact_dir / "metrics.json"
    assert manifest_path.read_bytes() == first_manifest_bytes
    manifest = json.loads(first_manifest_bytes)
    assert manifest["artifacts"] == raw_manifest["artifacts"]
    assert manifest["analytics"]["schema_version"] == 1
    assert manifest["analytics"]["metrics_version"] == "1.0.0"
    assert set(manifest["analytics"]["artifacts"]) == {
        "nav.parquet",
        "metrics.json",
        "drawdown.parquet",
        "monthly_returns.parquet",
        "exposure_summary.parquet",
        "factor_summary.parquet",
        "attribution.parquet",
        "quality_disclosure.json",
    }
    expected_schemas = {
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
    for name, schema in expected_schemas.items():
        assert pq.read_schema(artifact_dir / name) == schema
    for name, entry in manifest["analytics"]["artifacts"].items():
        path = artifact_dir / name
        assert path.is_file()
        assert entry["path"] == name
        assert entry["size_bytes"] == path.stat().st_size
        assert entry["sha256"] == _sha256(path)
        assert entry["sha256"] == entry["sha256"].lower()
        if name.endswith(".parquet"):
            assert entry["row_count"] == pq.read_table(path).num_rows
            assert entry["schema"] == pq.read_schema(path).to_string(
                show_field_metadata=False
            )
    assert {
        name: _sha256(artifact_dir / name) for name in raw_manifest["artifacts"]
    } == raw_hashes
    metrics = json.loads((artifact_dir / "metrics.json").read_text(encoding="utf-8"))
    quality = json.loads(
        (artifact_dir / "quality_disclosure.json").read_text(encoding="utf-8")
    )
    assert metrics["metrics_version"] == "1.0.0"
    assert metrics["annual_returns"] == [
        {
            "benchmark_return": 0.0,
            "period_end": "2024-01-09",
            "period_start": "2024-01-05",
            "portfolio_return": pytest.approx(-0.051),
            "relative_return": pytest.approx(-0.051),
            "year": 2024,
        }
    ]
    assert quality["schema_version"] == 1
    assert quality["metrics_version"] == "1.0.0"
    assert quality["calculation_mode"] == "CASH_EXACT"
    assert quality["unavailable_dimensions"] == {
        "factor": "UNAVAILABLE",
        "industry": "UNKNOWN",
        "style": "UNAVAILABLE",
    }
    assert "FACTOR_EXPOSURE_NOT_AVAILABLE" in quality["warnings"]


def test_materialize_rejects_raw_or_registered_analytics_tampering(
    tmp_path: Path,
) -> None:
    """Neither raw evidence nor a registered analytics file may be overwritten."""
    raw_artifact = _published_backtest(tmp_path / "raw")
    raw_manifest_before = (raw_artifact / "manifest.json").read_bytes()
    (raw_artifact / "nav.parquet").write_bytes(
        (raw_artifact / "nav.parquet").read_bytes() + b"tampered"
    )
    with pytest.raises(ValueError, match="raw artifact"):
        materialize_analytics(raw_artifact)
    assert (raw_artifact / "manifest.json").read_bytes() == raw_manifest_before
    assert "analytics" not in json.loads(raw_manifest_before)

    analytics_artifact = _published_backtest(tmp_path / "analytics")
    materialize_analytics(analytics_artifact)
    manifest_before = (analytics_artifact / "manifest.json").read_bytes()
    (analytics_artifact / "metrics.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="analytics artifact"):
        materialize_analytics(analytics_artifact)
    assert (analytics_artifact / "manifest.json").read_bytes() == manifest_before


def test_manifest_replace_failure_never_publishes_analytics_success_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The manifest must remain the final visible atomic success marker."""
    artifact_dir = _published_backtest(tmp_path)
    manifest_path = artifact_dir / "manifest.json"
    manifest_before = manifest_path.read_bytes()
    real_replace = os.replace

    def fail_manifest_replace(source: str | Path, target: str | Path) -> None:
        if Path(target) == manifest_path:
            raise OSError("injected analytics manifest failure")
        real_replace(source, target)

    monkeypatch.setattr(materialize_module.os, "replace", fail_manifest_replace)
    with pytest.raises(OSError, match="injected analytics manifest failure"):
        materialize_analytics(artifact_dir)

    assert manifest_path.read_bytes() == manifest_before
    assert "analytics" not in json.loads(manifest_before)


def test_registered_json_rebound_hash_still_rejects_logical_schema_tampering(
    tmp_path: Path,
) -> None:
    """Rebinding a damaged JSON hash must not make its logical schema trustworthy."""
    artifact_dir = _published_backtest(tmp_path)
    materialize_analytics(artifact_dir)
    quality_path = artifact_dir / "quality_disclosure.json"
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    del quality["warnings"]
    quality_path.write_text(
        json.dumps(quality, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    manifest_path = artifact_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    entry = manifest["analytics"]["artifacts"]["quality_disclosure.json"]
    entry["size_bytes"] = quality_path.stat().st_size
    entry["sha256"] = _sha256(quality_path)
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="quality disclosure logical schema"):
        materialize_analytics(artifact_dir)


def test_raw_manifest_identity_value_tampering_fails_closed(tmp_path: Path) -> None:
    """Raw hashes do not make an invalid execution identity trustworthy."""
    artifact_dir = _published_backtest(tmp_path)
    manifest_path = artifact_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["execution_config"]["reference_price"] = "INVALID"
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="manifest execution_config"):
        materialize_analytics(artifact_dir)
