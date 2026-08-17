"""提供python-module-conventions与因子相关的公开模型、协议与处理流程。"""

from quant_research.factors.analysis import (
    IcMetricSummary,
    InformationCoefficientAnalyzer,
)
from quant_research.factors.base import (
    FACTOR_OUTPUT_SCHEMA,
    Factor,
    FactorArtifact,
    FactorContext,
    FactorSpec,
)
from quant_research.factors.execution import (
    FactorExecutionDescriptor,
    FactorExecutionNode,
)
from quant_research.factors.partitioned import (
    FactorPartition,
    PartitionedFactorEngine,
    PartitionedFactorResult,
    PartitionEngineFactory,
)
from quant_research.factors.registry import FactorEngine, FactorRegistry

__all__ = [
    "FACTOR_OUTPUT_SCHEMA",
    "Factor",
    "FactorArtifact",
    "FactorContext",
    "FactorEngine",
    "FactorExecutionDescriptor",
    "FactorExecutionNode",
    "FactorPartition",
    "FactorRegistry",
    "FactorSpec",
    "IcMetricSummary",
    "InformationCoefficientAnalyzer",
    "PartitionEngineFactory",
    "PartitionedFactorEngine",
    "PartitionedFactorResult",
]
