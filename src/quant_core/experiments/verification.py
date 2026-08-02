"""Complete durable registration verification for successful experiments."""

from __future__ import annotations

import hashlib
from pathlib import Path

from quant_core.backtest.artifacts import (
    ExperimentArtifactPublication,
    validate_experiment_artifacts,
)
from quant_core.data.contracts import JsonValue
from quant_core.experiments.models import ExperimentArtifact, ExperimentStatus
from quant_core.experiments.query import ExperimentDetail


def validate_registered_publication(
    detail: ExperimentDetail,
) -> ExperimentArtifactPublication:
    """Authenticate every durable artifact row against the canonical bundle."""
    if not isinstance(detail, ExperimentDetail):
        raise TypeError("detail must be an ExperimentDetail")
    if detail.record.status is not ExperimentStatus.SUCCEEDED:
        raise ValueError("registered publication requires SUCCEEDED status")
    registered = {artifact.name: artifact for artifact in detail.artifacts}
    if len(registered) != len(detail.artifacts):
        raise ValueError("registered artifact names must be unique")
    if any(item.experiment_id != detail.record.id for item in detail.artifacts):
        raise ValueError("registered artifact experiment identity mismatch")
    manifest = registered.get("manifest.json")
    if manifest is None:
        raise ValueError("SUCCEEDED experiment has no registered manifest")
    publication = validate_experiment_artifacts(
        Path(manifest.path).parent,
        resolved_config=detail.record.config,
    )
    expected_names = {*publication.entries, "manifest.json"}
    if set(registered) != expected_names:
        raise ValueError("registered artifact names do not match publication")
    for name, entry in publication.entries.items():
        expected_metadata: dict[str, JsonValue] = {"size_bytes": entry.size_bytes}
        if entry.schema is not None:
            expected_metadata["schema"] = entry.schema
        if entry.row_count is not None:
            expected_metadata["row_count"] = entry.row_count
        _require_row(
            registered[name],
            expected_path=publication.artifact_dir / entry.path,
            expected_hash=entry.sha256,
            expected_type=Path(name).suffix.removeprefix(".") or "file",
            expected_metadata=expected_metadata,
        )
    manifest_bytes = publication.manifest_path.read_bytes()
    _require_row(
        manifest,
        expected_path=publication.manifest_path,
        expected_hash=hashlib.sha256(manifest_bytes).hexdigest(),
        expected_type="manifest",
        expected_metadata={
            "schema": "quant.experiment.manifest.v1",
            "size_bytes": len(manifest_bytes),
        },
    )
    return publication


def _require_row(
    artifact: ExperimentArtifact,
    *,
    expected_path: Path,
    expected_hash: str,
    expected_type: str,
    expected_metadata: dict[str, JsonValue],
) -> None:
    if (
        Path(artifact.path).resolve() != expected_path.resolve()
        or artifact.content_hash != expected_hash
        or artifact.artifact_type != expected_type
        or artifact.metadata != expected_metadata
    ):
        raise ValueError(
            "registered artifact path or hash or metadata does not match publication"
        )


__all__ = ["validate_registered_publication"]
