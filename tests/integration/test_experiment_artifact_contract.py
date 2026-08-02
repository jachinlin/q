"""Fail-closed publication contract for a complete successful experiment."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

import quant_core.backtest.artifacts as artifacts_module
from quant_core.analytics.materialize import materialize_analytics
from quant_core.backtest.artifacts import (
    FACTOR_METRICS_SCHEMA,
    ExperimentArtifactPublication,
    publish_experiment_artifacts,
    validate_experiment_artifacts,
)
from quant_core.backtest.engine import BacktestEngine
from quant_core.data.contracts import JsonValue, canonical_json_bytes
from quant_core.experiments.fingerprint import capture_environment
from quant_core.portfolio import RebalancePlanner
from tests.integration.test_backtest_timeline import (
    _EXPERIMENT,
    _Data,
    _NeverCancelled,
    _Progress,
    _request,
    _RuleBook,
    _Targets,
)

_RAW_NAMES = {
    "nav.parquet",
    "holdings.parquet",
    "targets.parquet",
    "fills.parquet",
    "costs.parquet",
}
_ANALYTICS_NAMES = {
    "metrics.json",
    "drawdown.parquet",
    "monthly_returns.parquet",
    "exposure_summary.parquet",
    "factor_summary.parquet",
    "attribution.parquet",
    "quality_disclosure.json",
}
_EXPERIMENT_LAYER_NAMES = {
    "resolved_config.yaml",
    "environment.json",
    "factor_metrics.parquet",
    "report.html",
    "run.log",
}
_ALL_NAMES = {"manifest.json", *_RAW_NAMES, *_ANALYTICS_NAMES, *_EXPERIMENT_LAYER_NAMES}
_EXPECTED_FACTOR_METRICS_SCHEMA = pa.schema(
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
_CONFIG: dict[str, JsonValue] = {
    "strategy": {"lookback": 20, "winsorize": True},
    "universe": "CSI300",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _manifest(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_bytes())
    assert isinstance(value, dict)
    return value


def _write_manifest(path: Path, manifest: Mapping[str, JsonValue]) -> None:
    path.write_bytes(canonical_json_bytes(manifest))


def _rebind_entry(
    artifact_dir: Path, namespace: str, name: str, *, schema_from_file: bool = True
) -> None:
    manifest_path = artifact_dir / "manifest.json"
    manifest = _manifest(manifest_path)
    entries = manifest[namespace]["artifacts"] if namespace else manifest["artifacts"]
    entry = entries[name]
    path = artifact_dir / name
    entry["size_bytes"] = path.stat().st_size
    entry["sha256"] = _sha256(path)
    if name.endswith(".parquet") and schema_from_file:
        entry["schema"] = pq.read_schema(path).to_string(show_field_metadata=False)
        entry["row_count"] = pq.read_metadata(path).num_rows
    _write_manifest(manifest_path, manifest)


def _prepared_bundle(root: Path) -> Path:
    staging_root = root / ".experiment-staging"
    result = BacktestEngine(
        _Data(),
        _Targets(),
        _RuleBook(),
        RebalancePlanner(),
        artifact_root=staging_root,
    ).run(_request(), _Progress(), _NeverCancelled())
    materialize_analytics(result.artifact_dir)

    source_root = root / "source"
    source_root.mkdir()
    (source_root / "strategy.py").write_bytes(b"SIGNAL = 'quality-value'\n")
    lockfile = source_root / "uv.lock"
    lockfile.write_bytes(b"version = 1\n")
    environment = capture_environment(source_root, lockfile)

    result.artifact_dir.joinpath("resolved_config.yaml").write_text(
        yaml.safe_dump(_CONFIG, allow_unicode=True, sort_keys=True),
        encoding="utf-8",
        newline="\n",
    )
    result.artifact_dir.joinpath("environment.json").write_bytes(
        canonical_json_bytes(environment)
    )
    factor_metrics = pa.Table.from_pylist(
        [
            {
                "factor_ref": "factor:value-bp:v1",
                "signal_date": date(2024, 1, 5),
                "metric_type": "RANK_IC",
                "metric_value": 0.125,
                "sample_count": 300,
                "is_valid": True,
                "quality_reason": None,
            },
            {
                "factor_ref": "factor:quality-roe:v2",
                "signal_date": date(2024, 1, 8),
                "metric_type": "RANK_IC",
                "metric_value": None,
                "sample_count": 0,
                "is_valid": False,
                "quality_reason": "INSUFFICIENT_SAMPLE",
            },
        ],
        schema=_EXPECTED_FACTOR_METRICS_SCHEMA,
    )
    pq.write_table(
        factor_metrics,
        result.artifact_dir / "factor_metrics.parquet",
        compression="zstd",
    )
    result.artifact_dir.joinpath("report.html").write_text(
        "<!doctype html><html><body><h1>Experiment</h1></body></html>",
        encoding="utf-8",
        newline="\n",
    )
    result.artifact_dir.joinpath("run.log").write_text(
        "experiment completed\n", encoding="utf-8", newline="\n"
    )
    return result.artifact_dir


def _publish(root: Path, staging: Path) -> ExperimentArtifactPublication:
    return publish_experiment_artifacts(
        staging,
        root / "published",
        _EXPERIMENT,
        resolved_config=_CONFIG,
    )


def test_complete_bundle_is_validated_then_atomically_published(tmp_path: Path) -> None:
    """A shallow wrapper would miss indexes, schema, bytes, or marker ordering."""
    staging = _prepared_bundle(tmp_path)
    original_manifest = _manifest(staging / "manifest.json")
    original_sections = {
        "artifacts": original_manifest["artifacts"],
        "analytics": original_manifest["analytics"],
    }
    payload_hashes = {
        name: _sha256(staging / name) for name in _ALL_NAMES - {"manifest.json"}
    }

    publication = _publish(tmp_path, staging)
    validated = validate_experiment_artifacts(
        publication.artifact_dir, resolved_config=_CONFIG
    )
    manifest_bytes = publication.manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)

    assert FACTOR_METRICS_SCHEMA == _EXPECTED_FACTOR_METRICS_SCHEMA
    assert not staging.exists()
    assert publication.artifact_dir == (
        tmp_path / "published" / f"experiment_id={_EXPERIMENT}"
    )
    assert publication.manifest_path == publication.artifact_dir / "manifest.json"
    assert validated.artifact_dir == publication.artifact_dir
    assert validated.entries == publication.entries
    assert publication.manifest == manifest
    assert validated.manifest == manifest
    assert {path.name for path in publication.artifact_dir.iterdir()} == _ALL_NAMES
    assert manifest_bytes == canonical_json_bytes(manifest)
    assert manifest["artifacts"] == original_sections["artifacts"]
    assert manifest["analytics"] == original_sections["analytics"]
    assert manifest["experiment"]["schema_version"] == 1
    assert set(manifest["experiment"]["artifacts"]) == _ALL_NAMES - {
        "manifest.json"
    }
    assert set(publication.entries) == _ALL_NAMES - {"manifest.json"}
    for name, entry in manifest["experiment"]["artifacts"].items():
        path = publication.artifact_dir / name
        assert entry["path"] == name
        assert entry["size_bytes"] == path.stat().st_size
        assert entry["sha256"] == _sha256(path) == payload_hashes[name]
        if name.endswith(".parquet"):
            assert set(entry) == {
                "path",
                "schema",
                "row_count",
                "size_bytes",
                "sha256",
            }
            assert entry["schema"] == pq.read_schema(path).to_string(
                show_field_metadata=False
            )
            assert entry["row_count"] == pq.read_metadata(path).num_rows
        else:
            assert set(entry) == {"path", "size_bytes", "sha256"}
    assert {
        name: _sha256(publication.artifact_dir / name)
        for name in _ALL_NAMES - {"manifest.json"}
    } == payload_hashes


@pytest.mark.parametrize("missing", sorted(_EXPERIMENT_LAYER_NAMES))
def test_each_experiment_layer_file_is_required(tmp_path: Path, missing: str) -> None:
    """No experiment-only evidence may silently disappear from a success bundle."""
    staging = _prepared_bundle(tmp_path)
    (staging / missing).unlink()

    with pytest.raises(ValueError, match="missing"):
        _publish(tmp_path, staging)

    assert staging.is_dir()
    assert (staging / "manifest.json").is_file()
    assert not (tmp_path / "published" / f"experiment_id={_EXPERIMENT}").exists()


@pytest.mark.parametrize("missing", sorted(_RAW_NAMES | _ANALYTICS_NAMES))
def test_each_standard_raw_or_analytics_file_is_required(
    tmp_path: Path, missing: str
) -> None:
    """The experiment layer must retain the complete standard artifact contract."""
    staging = _prepared_bundle(tmp_path)
    (staging / missing).unlink()

    with pytest.raises(ValueError):
        _publish(tmp_path, staging)

    assert staging.is_dir()
    assert (staging / "manifest.json").is_file()
    assert not (tmp_path / "published" / f"experiment_id={_EXPERIMENT}").exists()


def test_standard_schema_drift_fails_even_after_manifest_is_rebound(
    tmp_path: Path,
) -> None:
    """Coordinated hash edits must not replace the canonical raw Arrow schema."""
    staging = _prepared_bundle(tmp_path)
    holdings_path = staging / "holdings.parquet"
    table = pq.read_table(holdings_path).drop(["cost_basis_fen"])
    pq.write_table(table, holdings_path, compression="zstd")
    _rebind_entry(staging, "", "holdings.parquet")

    with pytest.raises(ValueError, match="schema"):
        _publish(tmp_path, staging)

    assert not (tmp_path / "published" / f"experiment_id={_EXPERIMENT}").exists()


def test_raw_logical_tampering_fails_after_exact_schema_and_hash_rebind(
    tmp_path: Path,
) -> None:
    """Rehashing a broken holdings identity must not make raw evidence valid."""
    staging = _prepared_bundle(tmp_path)
    holdings_path = staging / "holdings.parquet"
    table = pq.read_table(holdings_path)
    values = table["market_value_fen"].to_pylist()
    values[0] += 1
    index = table.schema.get_field_index("market_value_fen")
    tampered = table.set_column(
        index,
        table.schema.field(index),
        pa.array(values, type=pa.int64()),
    )
    pq.write_table(tampered, holdings_path, compression="zstd")
    _rebind_entry(staging, "", "holdings.parquet")

    with pytest.raises(ValueError, match="holdings market values"):
        _publish(tmp_path, staging)

    assert not (tmp_path / "published" / f"experiment_id={_EXPERIMENT}").exists()


def test_analytics_logical_tampering_fails_after_exact_schema_and_hash_rebind(
    tmp_path: Path,
) -> None:
    """Analytics must still be recomputed from raw evidence, not trusted by hash."""
    staging = _prepared_bundle(tmp_path)
    drawdown_path = staging / "drawdown.parquet"
    table = pq.read_table(drawdown_path)
    values = table["drawdown"].to_pylist()
    values[0] = -0.5
    index = table.schema.get_field_index("drawdown")
    tampered = table.set_column(
        index,
        table.schema.field(index),
        pa.array(values, type=pa.float64()),
    )
    pq.write_table(tampered, drawdown_path, compression="zstd")
    _rebind_entry(staging, "analytics", "drawdown.parquet")

    with pytest.raises(ValueError, match="does not match raw artifacts"):
        _publish(tmp_path, staging)

    assert not (tmp_path / "published" / f"experiment_id={_EXPERIMENT}").exists()


def test_registered_standard_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    """Registered standard artifacts cannot change without manifest detection."""
    staging = _prepared_bundle(tmp_path)
    metrics_path = staging / "metrics.json"
    metrics_path.write_bytes(metrics_path.read_bytes() + b" ")

    with pytest.raises(ValueError, match="analytics artifact"):
        _publish(tmp_path, staging)

    assert not (tmp_path / "published" / f"experiment_id={_EXPERIMENT}").exists()


@pytest.mark.parametrize(
    ("name", "payload", "message"),
    [
        ("resolved_config.yaml", b"!!python/object:os.system {}", "YAML"),
        ("environment.json", b'{"schema_version":1, "source_hash":NaN}', "environment"),
        ("report.html", b"plain nonempty text", "HTML"),
        ("run.log", b"\xff\xfe", "UTF-8"),
    ],
)
def test_experiment_text_formats_fail_closed(
    tmp_path: Path, name: str, payload: bytes, message: str
) -> None:
    """Unsafe YAML, noncanonical JSON, non-HTML, or invalid UTF-8 cannot publish."""
    staging = _prepared_bundle(tmp_path)
    (staging / name).write_bytes(payload)

    with pytest.raises(ValueError, match=message):
        _publish(tmp_path, staging)

    assert (staging / "manifest.json").is_file()
    assert not (tmp_path / "published" / f"experiment_id={_EXPERIMENT}").exists()


def test_factor_metrics_requires_exact_schema_and_validity_contract(
    tmp_path: Path,
) -> None:
    """Arbitrary Parquet and contradictory validity fields must be rejected."""
    schema_root = tmp_path / "schema"
    schema_staging = _prepared_bundle(schema_root)
    wrong_schema = _EXPECTED_FACTOR_METRICS_SCHEMA.remove(6)
    table = pa.Table.from_pylist(
        [
            {
                "factor_ref": "factor:value-bp:v1",
                "signal_date": date(2024, 1, 5),
                "metric_type": "RANK_IC",
                "metric_value": 0.1,
                "sample_count": 10,
                "is_valid": True,
            }
        ],
        schema=wrong_schema,
    )
    pq.write_table(table, schema_staging / "factor_metrics.parquet")
    with pytest.raises(ValueError, match="factor_metrics.*schema"):
        _publish(schema_root, schema_staging)

    content_root = tmp_path / "content"
    content_staging = _prepared_bundle(content_root)
    contradictory = pa.Table.from_pylist(
        [
            {
                "factor_ref": "factor:value-bp:v1",
                "signal_date": date(2024, 1, 5),
                "metric_type": "RANK_IC",
                "metric_value": None,
                "sample_count": 10,
                "is_valid": True,
                "quality_reason": None,
            }
        ],
        schema=_EXPECTED_FACTOR_METRICS_SCHEMA,
    )
    pq.write_table(contradictory, content_staging / "factor_metrics.parquet")
    with pytest.raises(ValueError, match="factor_metrics.*validity"):
        _publish(content_root, content_staging)


@pytest.mark.parametrize("invalid_path", ["../escape.html", "C:/escape.html"])
def test_experiment_manifest_rejects_path_escape_and_absolute_paths(
    tmp_path: Path, invalid_path: str
) -> None:
    """A rebound index cannot make a path outside the final directory trustworthy."""
    staging = _prepared_bundle(tmp_path)
    publication = _publish(tmp_path, staging)
    manifest = _manifest(publication.manifest_path)
    manifest["experiment"]["artifacts"]["report.html"]["path"] = invalid_path
    _write_manifest(publication.manifest_path, manifest)

    with pytest.raises(ValueError, match="safe relative"):
        validate_experiment_artifacts(publication.artifact_dir, resolved_config=_CONFIG)


def test_experiment_manifest_rejects_hash_size_schema_and_row_count_mismatch(
    tmp_path: Path,
) -> None:
    """Every experiment-layer index field must agree with the immutable payload."""
    staging = _prepared_bundle(tmp_path)
    publication = _publish(tmp_path, staging)
    original = _manifest(publication.manifest_path)
    mutations: tuple[tuple[str, Callable[[dict[str, Any]], None]], ...] = (
        (
            "size",
            lambda manifest: manifest["experiment"]["artifacts"]["report.html"].__setitem__(
                "size_bytes", 0
            ),
        ),
        (
            "hash",
            lambda manifest: manifest["experiment"]["artifacts"]["report.html"].__setitem__(
                "sha256", "0" * 64
            ),
        ),
        (
            "schema",
            lambda manifest: manifest["experiment"]["artifacts"]
            ["factor_metrics.parquet"].__setitem__("schema", "wrong"),
        ),
        (
            "row count",
            lambda manifest: manifest["experiment"]["artifacts"]
            ["factor_metrics.parquet"].__setitem__("row_count", 999),
        ),
    )
    for message, mutate in mutations:
        candidate = json.loads(canonical_json_bytes(original))
        mutate(candidate)
        _write_manifest(publication.manifest_path, candidate)
        with pytest.raises(ValueError, match=message):
            validate_experiment_artifacts(
                publication.artifact_dir, resolved_config=_CONFIG
            )


def test_extra_unregistered_file_never_reaches_final_directory(tmp_path: Path) -> None:
    """Success completeness must reject ordinary files absent from the index."""
    staging = _prepared_bundle(tmp_path)
    (staging / "debug.dump").write_bytes(b"unregistered")

    with pytest.raises(ValueError, match="unexpected"):
        _publish(tmp_path, staging)

    assert staging.is_dir()
    assert not (tmp_path / "published" / f"experiment_id={_EXPERIMENT}").exists()


def test_existing_success_directory_is_never_overwritten(tmp_path: Path) -> None:
    """A second publication attempt must preserve the first success marker."""
    staging = _prepared_bundle(tmp_path)
    final = tmp_path / "published" / f"experiment_id={_EXPERIMENT}"
    final.mkdir(parents=True)
    sentinel = final / "manifest.json"
    sentinel.write_bytes(b"existing-success")

    with pytest.raises(FileExistsError, match="already exists"):
        _publish(tmp_path, staging)

    assert sentinel.read_bytes() == b"existing-success"
    assert staging.is_dir()


def test_manifest_publish_failure_restores_staging_without_final_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The final path and success marker must stay invisible if final publish fails."""
    staging = _prepared_bundle(tmp_path)
    original_manifest_bytes = (staging / "manifest.json").read_bytes()
    final = tmp_path / "published" / f"experiment_id={_EXPERIMENT}"
    real_replace = os.replace
    injected = False

    def fail_final_manifest(source: str | Path, target: str | Path) -> None:
        nonlocal injected
        target_path = Path(target)
        if (
            not injected
            and
            target_path.name == "manifest.json"
            and target_path.parent.name == f"experiment_id={_EXPERIMENT}"
        ):
            injected = True
            raise OSError("injected experiment manifest failure")
        real_replace(source, target)

    monkeypatch.setattr(artifacts_module.os, "replace", fail_final_manifest)

    with pytest.raises(OSError, match="injected experiment manifest failure"):
        _publish(tmp_path, staging)

    assert not final.exists()
    assert staging.is_dir()
    assert (staging / "manifest.json").read_bytes() == original_manifest_bytes
    assert "experiment" not in json.loads(original_manifest_bytes)


def test_payload_tamper_immediately_before_success_marker_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No writer may change validated bytes in the last marker-publication window."""
    staging = _prepared_bundle(tmp_path)
    original_manifest_bytes = (staging / "manifest.json").read_bytes()
    final = tmp_path / "published" / f"experiment_id={_EXPERIMENT}"
    real_replace = os.replace

    def tamper_before_marker(source: str | Path, target: str | Path) -> None:
        target_path = Path(target)
        if (
            target_path.name == "manifest.json"
            and target_path.parent.name == f"experiment_id={_EXPERIMENT}"
        ):
            (target_path.parent / "report.html").write_text(
                "<!doctype html><html><body>tampered</body></html>",
                encoding="utf-8",
            )
        real_replace(source, target)

    monkeypatch.setattr(artifacts_module.os, "replace", tamper_before_marker)

    with pytest.raises(ValueError, match="hash|size"):
        _publish(tmp_path, staging)

    assert not final.exists()
    assert staging.is_dir()
    assert (staging / "manifest.json").read_bytes() == original_manifest_bytes
