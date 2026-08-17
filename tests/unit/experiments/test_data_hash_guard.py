"""验证当前 Canonical 数据漂移时实验各阶段会以关闭方式失败。"""

import pytest

from quant_research.bootstrap.worker import (
    ExperimentDataDrift,
    _ConcreteExperimentRuntime,
)
from quant_research.data.repository import CanonicalDatasetMissing
from quant_research.domain.enums import DatasetKind
from quant_research.experiments.runner import (
    EXPERIMENT_STAGES,
    ExperimentRunner,
    ExperimentStage,
)

EXPECTED = "a" * 64
ACTUAL = "b" * 64


class _ChangingRuntime:
    def __init__(self) -> None:
        self.checks: list[str] = []

    def assert_current_data(self, stage: str) -> None:
        self.checks.append(stage)
        if len(self.checks) == 2:
            raise ExperimentDataDrift(EXPECTED, ACTUAL, stage=stage)


class _MissingCatalog:
    """模拟缺少实验必需数据集的当前目录。"""

    def get_canonical_dataset(self, dataset: DatasetKind) -> object:
        raise KeyError(dataset)


@pytest.mark.parametrize("stage", EXPERIMENT_STAGES[1:])
def test_every_post_validation_stage_checks_data_before_and_after_operation(
    stage: ExperimentStage,
) -> None:
    runtime = _ChangingRuntime()
    operated = False

    def operation() -> object:
        nonlocal operated
        operated = True
        return object()

    with pytest.raises(ExperimentDataDrift) as caught:
        ExperimentRunner._with_data_guard(runtime, stage, operation)  # type: ignore[arg-type]

    assert operated
    assert runtime.checks == [stage.value, stage.value]
    assert caught.value.detail.context == {
        "expected": EXPECTED,
        "actual": ACTUAL,
        "stage": stage.value,
    }


def test_required_dataset_metadata_preserves_structured_missing_error() -> None:
    """实验覆盖检查仍应将目录缺失转换成结构化数据集错误。"""
    runtime = object.__new__(_ConcreteExperimentRuntime)
    runtime._catalog = _MissingCatalog()  # type: ignore[assignment]

    with pytest.raises(CanonicalDatasetMissing) as caught:
        runtime._required_dataset_records((DatasetKind.DAILY_BAR,))

    assert caught.value.detail.context == {"dataset": DatasetKind.DAILY_BAR.value}
