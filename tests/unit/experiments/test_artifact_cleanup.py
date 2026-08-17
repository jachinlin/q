"""最终产物发布后中间副本的清理时机测试。"""

from pathlib import Path
from uuid import UUID

from quant_research.experiments.runner import ExperimentArtifactFinalizer

_EXPERIMENT_ID = UUID("00000000-0000-0000-0000-000000000092")


def test_intermediate_bundle_is_cleaned_only_by_post_register_cleanup(
    tmp_path: Path,
) -> None:
    intermediate = tmp_path / ".experiment-staging" / f"experiment_id={_EXPERIMENT_ID}"
    intermediate.mkdir(parents=True)
    (intermediate / "manifest.json").write_text("{}", encoding="utf-8")
    finalizer = object.__new__(ExperimentArtifactFinalizer)
    finalizer._artifact_root = tmp_path

    assert intermediate.is_dir()
    finalizer.cleanup_intermediate(str(_EXPERIMENT_ID))

    assert not intermediate.exists()
