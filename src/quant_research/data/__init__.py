"""汇总数据层对外公开的供应商无关契约与不可变存储接口。"""

from quant_research.data.catalog import DATASET_CATALOG, DatasetCatalog, DatasetSpec
from quant_research.data.contracts import (
    CanonicalBatch,
    CanonicalMapper,
    ProviderCapabilities,
    PublishedPartition,
    RawBatch,
    SourceClient,
)
from quant_research.data.partitions import RawPartitionStore
from quant_research.data.routing import Route, RoutingTable

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
    "SourceClient",
]
