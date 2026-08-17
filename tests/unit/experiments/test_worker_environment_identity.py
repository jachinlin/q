"""验证实验在昂贵计算前拒绝来源身份不一致的 Worker。"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from quant_research.data.contracts import canonical_json_bytes
from quant_research.experiments.models import (
    ExperimentRecord,
    ExperimentStatus,
    ResearchMark,
)
from quant_research.experiments.runner import ExperimentArtifactFinalizer


def test_artifact_finalizer_rejects_stale_worker_environment_during_validation(
    tmp_path: Path,
) -> None:
    """旧 Worker 的 Git 身份应在 VALIDATE 阶段可被直接拒绝。"""
    config = {"strategy_id": "stock_multifactor"}
    experiment = ExperimentRecord(
        id="00000000-0000-0000-0000-000000000901",
        strategy_id="stock_multifactor",
        config=config,
        config_hash=hashlib.sha256(canonical_json_bytes(config)).hexdigest(),
        data_hash="a" * 64,
        source_tree_hash=None,
        git_commit_hash="b" * 40,
        lockfile_hash="c" * 64,
        rulebook_hash="d" * 64,
        fingerprint="e" * 64,
        status=ExperimentStatus.RUNNING,
        research_mark=ResearchMark.UNREVIEWED,
        created_at=datetime(2026, 8, 15, tzinfo=UTC),
        queued_at=datetime(2026, 8, 15, tzinfo=UTC),
        started_at=datetime(2026, 8, 15, tzinfo=UTC),
        completed_at=None,
    )
    finalizer = ExperimentArtifactFinalizer(
        artifact_root=tmp_path / "artifacts",
        environment={
            "source_identity_mode": "git_commit",
            "source_hash": "f" * 64,
            "git_commit": "9" * 40,
            "source_tree_hash": None,
            "working_tree_dirty": False,
            "lockfile_path": "uv.lock",
            "lockfile_hash": "c" * 64,
            "python_version": "3.12.0",
        },
    )

    with pytest.raises(ValueError, match="Git identity"):
        finalizer.validate_environment(experiment)
