"""验证数据目录声明与抓取计划注册表保持完整一致。"""

from quant_research.data.catalog import DATASET_CATALOG, FetchPlan
from quant_research.data.pipeline.localize import LocalizePlanExecutor
from quant_research.domain.enums import DatasetKind


def test_every_catalog_fetch_plan_has_a_registered_executor() -> None:
    declared = {spec.fetch_plan for spec in DATASET_CATALOG.values()}

    assert declared == set(FetchPlan)
    assert LocalizePlanExecutor.supported_plans() == frozenset(FetchPlan)


def test_catalog_assigns_dataset_specific_fetch_plans() -> None:
    assert {
        dataset: DATASET_CATALOG[dataset].fetch_plan for dataset in DatasetKind
    } == {
        DatasetKind.STOCK_MASTER: FetchPlan.MARKET_SNAPSHOT,
        DatasetKind.FUND_MASTER: FetchPlan.MARKET_SNAPSHOT,
        DatasetKind.INDEX_MASTER: FetchPlan.MARKET_SNAPSHOT,
        DatasetKind.TRADE_CALENDAR: FetchPlan.TRADE_CALENDAR_RANGE,
        DatasetKind.STOCK_DAILY_BAR: FetchPlan.MARKET_TRADE_DATE,
        DatasetKind.STOCK_ADJUSTMENT_FACTOR: FetchPlan.MARKET_TRADE_DATE,
        DatasetKind.FUND_DAILY_BAR: FetchPlan.MARKET_TRADE_DATE,
        DatasetKind.FUND_ADJUSTMENT_FACTOR: FetchPlan.MARKET_TRADE_DATE,
        DatasetKind.STOCK_DAILY_BASIC: FetchPlan.MARKET_TRADE_DATE,
        DatasetKind.STOCK_SUSPENSION: FetchPlan.MARKET_TRADE_DATE,
        DatasetKind.STOCK_RISK_WARNING: FetchPlan.MARKET_TRADE_DATE,
        DatasetKind.INDEX_DAILY_BAR: FetchPlan.INDEX_RANGE_EXCEPTION,
        DatasetKind.STOCK_FINANCIAL_INDICATOR: FetchPlan.REPORT_PERIOD,
        DatasetKind.INDUSTRY_CATALOG: FetchPlan.MARKET_SNAPSHOT,
        DatasetKind.INDUSTRY_MEMBERSHIP: FetchPlan.INDUSTRY_L1,
    }
