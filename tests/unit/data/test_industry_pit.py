"""验证行业分类年度事件压缩与 tombstone 字面量语义。"""

from datetime import UTC, date, datetime, time
from zoneinfo import ZoneInfo

import polars as pl

from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.pipeline.curate import CuratedPartitionStore
from quant_research.data.quality.runner import QualityRunner
from quant_research.domain.enums import DatasetKind
from quant_research.infrastructure.baostock.mapper import BaoStockMapper

_TAXONOMY = "证监会行业分类"
_INGESTED = datetime(2026, 8, 16, tzinfo=UTC)


class _IndustryFixture:
    """生成符合最终 Canonical schema 的逐日供应商快照。"""

    @staticmethod
    def row(
        as_of: date,
        supplier_update: date,
        instrument: str,
        industry: str | None,
    ) -> dict[str, object | None]:
        available_at = datetime.combine(
            as_of, time.max, tzinfo=ZoneInfo("Asia/Shanghai")
        ).astimezone(UTC)
        return {
            "as_of_date": as_of,
            "supplier_update_date": supplier_update,
            "instrument_id": instrument,
            "taxonomy": _TAXONOMY,
            "industry_code": industry,
            "industry_name": industry,
            "is_classified": industry is not None,
            "source": "baostock",
            "available_at": available_at,
            "availability_source": "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED",
            "pit_usable": True,
            "ingested_at": _INGESTED,
        }


def test_industry_snapshots_compress_to_annual_baseline_and_state_events() -> None:
    rows = [
        _IndustryFixture.row(date(2026, 1, 5), date(2025, 12, 29), "600000.SH", "J66"),
        _IndustryFixture.row(date(2026, 1, 5), date(2025, 12, 29), "600001.SH", "C39"),
        _IndustryFixture.row(date(2026, 1, 5), date(2025, 12, 29), "600002.SH", None),
        _IndustryFixture.row(date(2026, 1, 6), date(2025, 12, 29), "600000.SH", "J66"),
        _IndustryFixture.row(date(2026, 1, 6), date(2025, 12, 29), "600002.SH", None),
        _IndustryFixture.row(date(2026, 1, 6), date(2025, 12, 29), "600003.SH", "I65"),
        _IndustryFixture.row(date(2026, 1, 12), date(2026, 1, 12), "600000.SH", "C39"),
        _IndustryFixture.row(date(2026, 1, 12), date(2026, 1, 12), "600001.SH", "C39"),
        _IndustryFixture.row(date(2026, 1, 12), date(2026, 1, 12), "600002.SH", "J66"),
        _IndustryFixture.row(date(2026, 1, 12), date(2026, 1, 12), "600003.SH", "I65"),
        _IndustryFixture.row(date(2026, 1, 13), date(2026, 1, 12), "600000.SH", "C39"),
        _IndustryFixture.row(date(2026, 1, 13), date(2026, 1, 12), "600001.SH", None),
        _IndustryFixture.row(date(2026, 1, 13), date(2026, 1, 12), "600002.SH", "J66"),
        _IndustryFixture.row(date(2026, 1, 13), date(2026, 1, 12), "600003.SH", "I65"),
    ]
    schema = CANONICAL_SCHEMAS[DatasetKind.INDUSTRY_CLASSIFICATION].columns

    events = BaoStockMapper.consolidate_partition(
        DatasetKind.INDUSTRY_CLASSIFICATION,
        (pl.DataFrame(rows, schema=schema),),
    )

    assert events.select(
        "as_of_date", "instrument_id", "industry_code", "is_classified"
    ).rows() == [
        (date(2026, 1, 5), "600000.SH", "J66", True),
        (date(2026, 1, 5), "600001.SH", "C39", True),
        (date(2026, 1, 5), "600002.SH", None, False),
        (date(2026, 1, 6), "600003.SH", "I65", True),
        (date(2026, 1, 12), "600000.SH", "C39", True),
        (date(2026, 1, 12), "600002.SH", "J66", True),
        (date(2026, 1, 13), "600001.SH", None, False),
    ]


def test_industry_partition_uses_request_as_of_year() -> None:
    schema = CANONICAL_SCHEMAS[DatasetKind.INDUSTRY_CLASSIFICATION].columns
    frame = pl.DataFrame(
        [
            _IndustryFixture.row(
                date(2026, 1, 5), date(2025, 12, 29), "600000.SH", "J66"
            )
        ],
        schema=schema,
    )

    ((partition_key, partition),) = CuratedPartitionStore.partition_frame(
        DatasetKind.INDUSTRY_CLASSIFICATION, frame
    )

    assert partition_key == "year=2026"
    assert partition["supplier_update_date"].item() == date(2025, 12, 29)


def test_industry_quality_rejects_inconsistent_tombstone() -> None:
    schema = CANONICAL_SCHEMAS[DatasetKind.INDUSTRY_CLASSIFICATION].columns
    invalid = pl.DataFrame(
        [
            {
                **_IndustryFixture.row(
                    date(2026, 1, 5), date(2025, 12, 29), "600000.SH", None
                ),
                "is_classified": True,
            }
        ],
        schema=schema,
    )

    evaluation = QualityRunner().evaluate(
        {DatasetKind.INDUSTRY_CLASSIFICATION: (invalid,)}
    )

    assert [issue.rule_id for issue in evaluation.issues] == ["industry_state"]


def test_future_or_incomplete_industry_raw_is_excluded_before_curate_identity() -> None:
    request = {
        "api": "query_stock_industry",
        "scope": "ALL",
        "date": "2026-08-14",
        "as_of": "2026-08-14",
    }
    assert not BaoStockMapper.raw_head_is_usable(
        DatasetKind.INDUSTRY_CLASSIFICATION,
        request,
        datetime(2026, 8, 14, 17, 59, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    assert BaoStockMapper.raw_head_is_usable(
        DatasetKind.INDUSTRY_CLASSIFICATION,
        request,
        datetime(2026, 8, 14, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
