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
        DatasetKind.DAILY_BAR: FetchPlan.DAILY_MARKET,
        DatasetKind.DAILY_BASIC: FetchPlan.DAILY_MARKET,
        DatasetKind.SECURITY_STATUS: FetchPlan.DAILY_MARKET,
        DatasetKind.TRADE_CALENDAR: FetchPlan.TRADE_CALENDAR_RANGE,
        DatasetKind.INSTRUMENT: FetchPlan.INSTRUMENT_SNAPSHOT,
        DatasetKind.FINANCIAL_OBSERVATION: FetchPlan.FINANCIAL_CELL,
        DatasetKind.INDUSTRY_CLASSIFICATION: FetchPlan.INDUSTRY_AS_OF,
        DatasetKind.INDEX_BAR: FetchPlan.INDEX_RANGE,
    }
