"""定义 BaoStock 对 Canonical 数据集的供应商路由。"""

from quant_research.data.catalog import DATASET_CATALOG
from quant_research.data.sources.routing import Route, RoutingTable

BAOSTOCK_ROUTES = RoutingTable(
    {dataset: (Route(1, "baostock"),) for dataset in DATASET_CATALOG}
)
