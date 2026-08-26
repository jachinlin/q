"""定义 Tushare 对全部 Canonical 数据集的唯一供应商路由。"""

from quant_research.data.catalog import DATASET_CATALOG
from quant_research.data.sources.routing import Route, RoutingTable

TUSHARE_ROUTES = RoutingTable(
    {dataset: (Route(1, "tushare"),) for dataset in DATASET_CATALOG}
)
