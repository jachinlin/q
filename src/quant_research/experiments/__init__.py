"""提供实验配置、身份、执行阶段与产物验证能力。"""

from quant_research.experiments.models import (
    ExperimentDefinition,
    ExperimentRecord,
    RunRecord,
    RunStatus,
)
from quant_research.experiments.statistics import MultipleTestingCorrector

__all__ = [
    "ExperimentDefinition",
    "ExperimentRecord",
    "MultipleTestingCorrector",
    "RunRecord",
    "RunStatus",
]
