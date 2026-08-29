"""验证数据中心新鲜度策略的边界与非阻断状态。"""

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

from quant_research.data.catalog import DATASET_CATALOG
from quant_research.data.freshness import FreshnessEvaluator, FreshnessStatus
from quant_research.domain.enums import DatasetKind
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
    DatasetOperationalStateRecord,
)


def _canonical(dataset: DatasetKind, end: date | None) -> CanonicalDatasetRecord:
    return CanonicalDatasetRecord(
        dataset=dataset,
        content_hash="a" * 64,
        source="baostock",
        partitions=(),
        start_date=end,
        end_date=end,
        updated_at=datetime(2026, 8, 14, tzinfo=UTC),
    )


def _operational(
    dataset: DatasetKind,
    localized_at: datetime,
    localized_through: date | None = None,
) -> DatasetOperationalStateRecord:
    return DatasetOperationalStateRecord(
        dataset=dataset,
        last_localized_at=localized_at,
        localized_through=localized_through,
        last_curated_at=None,
        last_validated_at=None,
        updated_at=localized_at,
    )


def _evaluate(
    *,
    canonical: tuple[CanonicalDatasetRecord, ...],
    operational: tuple[DatasetOperationalStateRecord, ...] = (),
    evaluated_at: datetime,
    session: date | None,
) -> dict[DatasetKind, FreshnessStatus]:
    result = FreshnessEvaluator(
        DATASET_CATALOG,
        timezone=ZoneInfo("Asia/Shanghai"),
    ).evaluate(
        canonical=canonical,
        operational=operational,
        evaluated_at=evaluated_at,
        latest_complete_session=session,
    )
    return {item.dataset: item.status for item in result}


def test_trading_session_policy_handles_1800_cutoff_evidence() -> None:
    statuses = _evaluate(
        canonical=(
            _canonical(DatasetKind.STOCK_DAILY_BAR, date(2026, 8, 14)),
            _canonical(DatasetKind.STOCK_DAILY_BASIC, date(2026, 8, 13)),
        ),
        evaluated_at=datetime(2026, 8, 14, 10, tzinfo=ZoneInfo("Asia/Shanghai")),
        session=date(2026, 8, 14),
    )
    assert statuses[DatasetKind.STOCK_DAILY_BAR] is FreshnessStatus.CURRENT
    assert statuses[DatasetKind.STOCK_DAILY_BASIC] is FreshnessStatus.STALE


def test_industry_freshness_requires_successful_refresh_evidence() -> None:
    statuses = _evaluate(
        canonical=(_canonical(DatasetKind.INDUSTRY_MEMBERSHIP, date(2026, 8, 14)),),
        evaluated_at=datetime(2026, 8, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai")),
        session=date(2026, 8, 14),
    )

    assert statuses[DatasetKind.INDUSTRY_MEMBERSHIP] is FreshnessStatus.UNKNOWN


def test_calendar_horizon_changes_at_beijing_1800() -> None:
    calendar = (_canonical(DatasetKind.TRADE_CALENDAR, date(2026, 9, 13)),)
    before = _evaluate(
        canonical=calendar,
        evaluated_at=datetime(2026, 8, 15, 17, 59, tzinfo=ZoneInfo("Asia/Shanghai")),
        session=date(2026, 8, 14),
    )
    after = _evaluate(
        canonical=calendar,
        evaluated_at=datetime(2026, 8, 15, 18, tzinfo=ZoneInfo("Asia/Shanghai")),
        session=date(2026, 8, 14),
    )
    assert before[DatasetKind.TRADE_CALENDAR] is FreshnessStatus.CURRENT
    assert after[DatasetKind.TRADE_CALENDAR] is FreshnessStatus.STALE


def test_calendar_event_freshness_uses_successful_localize_watermark() -> None:
    evaluator = FreshnessEvaluator(
        DATASET_CATALOG,
        timezone=ZoneInfo("Asia/Shanghai"),
    )
    result = evaluator.evaluate(
        canonical=(
            _canonical(DatasetKind.STOCK_DIVIDEND, date(2026, 8, 28)),
            _canonical(DatasetKind.FUND_DIVIDEND, date(2026, 8, 28)),
        ),
        operational=(
            _operational(
                DatasetKind.STOCK_DIVIDEND,
                datetime(2026, 8, 29, 11, tzinfo=UTC),
                date(2026, 8, 29),
            ),
            _operational(
                DatasetKind.FUND_DIVIDEND,
                datetime(2026, 8, 29, 11, tzinfo=UTC),
                date(2026, 8, 28),
            ),
        ),
        evaluated_at=datetime(
            2026, 8, 29, 23, tzinfo=ZoneInfo("Asia/Shanghai")
        ),
        latest_complete_session=date(2026, 8, 28),
    )
    by_dataset = {item.dataset: item for item in result}

    stock = by_dataset[DatasetKind.STOCK_DIVIDEND]
    assert stock.status is FreshnessStatus.CURRENT
    assert stock.actual_watermark == date(2026, 8, 29)
    assert stock.expected_watermark == date(2026, 8, 29)
    fund = by_dataset[DatasetKind.FUND_DIVIDEND]
    assert fund.status is FreshnessStatus.STALE
    assert fund.actual_watermark == date(2026, 8, 28)


def test_calendar_refresh_and_missing_evidence_cover_all_states() -> None:
    evaluated_at = datetime(2026, 8, 15, 20, tzinfo=ZoneInfo("Asia/Shanghai"))
    statuses = _evaluate(
        canonical=(
            _canonical(DatasetKind.TRADE_CALENDAR, date(2026, 9, 14)),
            _canonical(DatasetKind.STOCK_MASTER, date(2026, 8, 14)),
            _canonical(DatasetKind.STOCK_FINANCIAL_INDICATOR, date(2026, 6, 30)),
        ),
        operational=(
            _operational(
                DatasetKind.STOCK_MASTER,
                datetime(2026, 8, 14, 11, tzinfo=UTC),
                date(2026, 8, 14),
            ),
            _operational(
                DatasetKind.STOCK_FINANCIAL_INDICATOR,
                datetime(2026, 8, 1, 11, tzinfo=UTC),
            ),
        ),
        evaluated_at=evaluated_at,
        session=date(2026, 8, 14),
    )
    assert statuses[DatasetKind.TRADE_CALENDAR] is FreshnessStatus.CURRENT
    assert statuses[DatasetKind.STOCK_MASTER] is FreshnessStatus.CURRENT
    assert statuses[DatasetKind.STOCK_FINANCIAL_INDICATOR] is FreshnessStatus.CURRENT
    assert statuses[DatasetKind.INDUSTRY_MEMBERSHIP] is FreshnessStatus.MISSING


def test_calendar_shortage_marks_dependent_datasets_unknown() -> None:
    statuses = _evaluate(
        canonical=(_canonical(DatasetKind.INDEX_DAILY_BAR, date(2026, 8, 14)),),
        evaluated_at=datetime(2026, 8, 15, 20, tzinfo=ZoneInfo("Asia/Shanghai")),
        session=None,
    )
    assert statuses[DatasetKind.INDEX_DAILY_BAR] is FreshnessStatus.UNKNOWN


def test_financial_freshness_turns_stale_only_after_disclosure_deadline() -> None:
    evaluator = FreshnessEvaluator(
        DATASET_CATALOG,
        timezone=ZoneInfo("Asia/Shanghai"),
    )
    result = evaluator.evaluate(
        canonical=(_canonical(DatasetKind.STOCK_FINANCIAL_INDICATOR, date(2026, 6, 30)),),
        operational=(
            _operational(
                DatasetKind.STOCK_FINANCIAL_INDICATOR,
                datetime(2026, 8, 1, tzinfo=UTC),
            ),
        ),
        evaluated_at=datetime(2026, 9, 1, 10, tzinfo=ZoneInfo("Asia/Shanghai")),
        latest_complete_session=date(2026, 8, 31),
    )
    financial = next(
        item for item in result if item.dataset is DatasetKind.STOCK_FINANCIAL_INDICATOR
    )

    assert financial.status is FreshnessStatus.STALE
    assert financial.actual_watermark is None
    assert financial.expected_watermark is None
    assert financial.trigger_date == date(2026, 8, 31)
    assert financial.update_required is True
