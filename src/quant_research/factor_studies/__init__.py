"""提供独立因子研究模型、配置、执行与分析契约。"""

from quant_research.factor_studies.config import FactorStudyConfigParser
from quant_research.factor_studies.models import (
    FactorStudyDefinition,
    FactorStudyRecord,
)
from quant_research.factor_studies.statistics import MultipleTestingCorrector

__all__ = [
    "FactorStudyConfigParser",
    "FactorStudyDefinition",
    "FactorStudyRecord",
    "MultipleTestingCorrector",
]
