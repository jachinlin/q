"""提供 BaoStock 数据源、映射器与路由装配。"""

from quant_research.infrastructure.baostock.client import (
    BaoStockCalendarPolicy,
    BaoStockClient,
    BaoStockConfig,
    BaoStockSdkGateway,
)
from quant_research.infrastructure.baostock.mapper import BaoStockMapper
from quant_research.infrastructure.baostock.routing import BAOSTOCK_ROUTES

__all__ = [
    "BAOSTOCK_ROUTES",
    "BaoStockCalendarPolicy",
    "BaoStockClient",
    "BaoStockConfig",
    "BaoStockMapper",
    "BaoStockSdkGateway",
]
