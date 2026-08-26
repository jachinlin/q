"""汇总数据层对外公开的供应商无关契约与不可变存储接口。"""

from quant_research.data.canonical.mapper import CanonicalMapper
from quant_research.data.catalog import DATASET_CATALOG, DatasetCatalog, DatasetSpec
from quant_research.data.contracts import (
    CanonicalBatch,
    ProviderCapabilities,
    PublishedPartition,
    RawBatch,
)
from quant_research.data.sources.routing import Route, RoutingTable
from quant_research.data.storage.partitions import RawPartitionStore

__all__ = [
    "DATASET_CATALOG",
    "CanonicalBatch",
    "CanonicalMapper",
    "DatasetCatalog",
    "DatasetSpec",
    "ProviderCapabilities",
    "PublishedPartition",
    "RawBatch",
    "RawPartitionStore",
    "Route",
    "RoutingTable",
]
