"""提供实验与产物验证相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import hashlib
from pathlib import Path

from quant_research.backtest.artifacts import (
    ExperimentArtifactPublication,
    validate_experiment_artifacts,
)
from quant_research.data.contracts import JsonValue
from quant_research.experiments.models import ExperimentArtifact, ExperimentStatus
from quant_research.experiments.query import ExperimentDetail


def validate_registered_publication(
    detail: ExperimentDetail,
) -> ExperimentArtifactPublication:
    """校验``registered``不可变发布物；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        detail：供调用者诊断失败原因的可选安全文本。
    返回值：
        返回校验``registered``不可变发布物；该函数作为稳定公开 API 或框架入口保留在模块级后的``registered``不可变发布物（``ExperimentArtifactPublication``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Authenticate every durable artifact row against the canonical bundle.
    """
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
        _VerificationSupport._require_row(
            registered[name],
            expected_path=publication.artifact_dir / entry.path,
            expected_hash=entry.sha256,
            expected_type=Path(name).suffix.removeprefix(".") or "file",
            expected_metadata=expected_metadata,
        )
    manifest_bytes = publication.manifest_path.read_bytes()
    _VerificationSupport._require_row(
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


class _VerificationSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
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
