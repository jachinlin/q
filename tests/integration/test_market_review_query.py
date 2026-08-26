"""市场全景通过真实 Canonical 仓库读取的集成测试。"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from quant_research.backtest.rulebook import AShareRuleBook
from quant_research.dashboard.market_review import MarketReviewService
from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.contracts import CanonicalBatch
from quant_research.data.pipeline.curate import CuratedPartitionStore
from quant_research.data.quality.models import QualityRunSpec
from quant_research.data.repository import CanonicalResearchRepository
from quant_research.domain.enums import DatasetKind
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.repositories import MetadataRepository

_DAY = date(2026, 8, 14)
_AVAILABLE = datetime(2026, 8, 14, 7, tzinfo=UTC)
_INGESTED = datetime(2026, 8, 14, 8, tzinfo=UTC)


def _canonical_frame(
    dataset: DatasetKind,
    rows: list[dict[str, object]],
) -> pl.DataFrame:
    """以当前 Schema 补齐场景未关注的可空字段。"""
    schema = CANONICAL_SCHEMAS[dataset].columns
    return pl.DataFrame(
        [dict.fromkeys(schema.names()) | row for row in rows],
        schema=schema,
        strict=False,
    )


class _MarketReviewHarness:
    """发布最小完整 Canonical 目录并组装真实市场全景服务。"""

    def __init__(self, root: Path) -> None:
        database = root / "state" / "quant.db"
        upgrade_database(database)
        self.engine = create_sqlite_engine(database)
        self.catalog = MetadataRepository(self.engine)
        self.store = CuratedPartitionStore(root / "canonical")
        result = self.store.publish(
            self._batches(),
            previous_datasets={},
            run_id="market-review-integration",
            source="tushare",
            start=_DAY,
            end=_DAY,
            repository=self.catalog,
        )
        state = self.catalog.catalog_state()
        quality = self.catalog.register_quality_run(
            QualityRunSpec(
                dataset_hashes={
                    name: record.content_hash
                    for name, record in result.datasets.items()
                },
                input_hash=state.catalog_hash,
                scope="ALL",
                started_at=_INGESTED,
                completed_at=_INGESTED,
                issues=(),
            )
        )
        self.catalog.mark_catalog_validated(quality.id, validated_at=_INGESTED)
        self.repository = CanonicalResearchRepository.from_sqlite(
            self.engine,
            trusted_curated_root=self.store.root,
        )
        self.service = MarketReviewService(
            self.repository,
            AShareRuleBook.load(
                Path(__file__).resolve().parents[2]
                / "configs"
                / "rules"
                / "a_share.yaml"
            ),
        )

    def close(self) -> None:
        """释放测试数据库引擎。"""

        self.engine.dispose()

    @classmethod
    def _batches(cls) -> tuple[CanonicalBatch, ...]:
        audits = {
            "source": "test",
            "available_at": _AVAILABLE,
            "availability_source": "test",
            "pit_usable": True,
            "ingested_at": _INGESTED,
        }
        indexes = (
            "399317.SZ",
            "000016.SH",
            "000300.SH",
            "000905.SH",
            "000852.SH",
        )
        instruments = _canonical_frame(
            DatasetKind.STOCK_MASTER,
            [
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "board": "MAIN",
                    "name": "浦发银行",
                    "list_status": "L",
                    "list_date": date(1999, 11, 10),
                    **audits,
                },
            ],
        )
        calendar = _canonical_frame(
            DatasetKind.TRADE_CALENDAR,
            [
                {
                    "exchange": "SSE",
                    "trade_date": _DAY,
                    "is_trading_day": True,
                    "previous_trade_date": date(2026, 8, 13),
                    **audits,
                }
            ],
        )
        bar = _canonical_frame(
            DatasetKind.STOCK_DAILY_BAR,
            [
                {
                    "instrument_id": "600000.SH",
                    "trade_date": _DAY,
                    "open": 10.0,
                    "high": 11.0,
                    "low": 10.0,
                    "close": 11.0,
                    "preclose": 10.0,
                    "change": 1.0,
                    "volume": 1_000_000,
                    "amount": 10_000_000.0,
                    "pct_change": 0.1,
                    **audits,
                }
            ],
        )
        basic = _canonical_frame(
            DatasetKind.STOCK_DAILY_BASIC,
            [
                {
                    "instrument_id": "600000.SH",
                    "trade_date": _DAY,
                    "pe_ttm": 10.0,
                    "pb": 1.0,
                    "ps_ttm": 2.0,
                    "turnover_rate": 0.03,
                    **audits,
                }
            ],
        )
        suspension = _canonical_frame(
            DatasetKind.STOCK_SUSPENSION,
            [
                {
                    "instrument_id": "600001.SH",
                    "trade_date": _DAY,
                    "suspend_type": "S",
                    **audits,
                }
            ],
        )
        warning = _canonical_frame(
            DatasetKind.STOCK_RISK_WARNING,
            [
                {
                    "instrument_id": "600001.SH",
                    "trade_date": _DAY,
                    "risk_type": "S",
                    **audits,
                }
            ],
        )
        industry = _canonical_frame(
            DatasetKind.INDUSTRY_MEMBERSHIP,
            [
                {
                    "level1_code": "801780.SI",
                    "level1_name": "银行",
                    "instrument_id": "600000.SH",
                    "instrument_name": "浦发银行",
                    "in_date": date(1999, 11, 10),
                    "is_current": True,
                    "in_available_at": _AVAILABLE,
                    **audits,
                }
            ],
        )
        index_bar = _canonical_frame(
            DatasetKind.INDEX_DAILY_BAR,
            [
                {
                    "index_id": identifier,
                    "trade_date": _DAY,
                    "open": 100.0,
                    "high": 102.0,
                    "low": 99.0,
                    "close": 101.0,
                    "preclose": 100.0,
                    "change": 1.0,
                    "volume": 1_000_000,
                    "amount": 10_000_000.0,
                    "pct_change": 0.01,
                    **audits,
                }
                for identifier in indexes
            ],
        )
        frames = (
            (DatasetKind.STOCK_MASTER, instruments),
            (DatasetKind.TRADE_CALENDAR, calendar),
            (DatasetKind.STOCK_DAILY_BAR, bar),
            (DatasetKind.STOCK_DAILY_BASIC, basic),
            (DatasetKind.STOCK_SUSPENSION, suspension),
            (DatasetKind.STOCK_RISK_WARNING, warning),
            (DatasetKind.INDUSTRY_MEMBERSHIP, industry),
            (DatasetKind.INDEX_DAILY_BAR, index_bar),
        )
        return tuple(
            CanonicalBatch(dataset, frame, (str(index + 1) * 64,)[:1])
            for index, (dataset, frame) in enumerate(frames)
        )


def test_market_review_uses_verified_repository_and_strict_pit_industry(
    tmp_path: Path,
) -> None:
    harness = _MarketReviewHarness(tmp_path)
    try:
        result = harness.service.review(None, exclude_st=False)

        assert result.trade_date == _DAY
        assert result.data_quality.coverage_rate == 1.0
        assert result.breadth.median_return == 0.1
        assert result.sentiment.limit_up_count == 1
        assert result.industries.taxonomy == "SW2021"
        assert result.valuation.turnover_median == 0.03
        assert tuple(item.index_id for item in result.indexes) == (
            "399317.SZ",
            "000016.SH",
            "000300.SH",
            "000905.SH",
            "000852.SH",
        )
    finally:
        harness.close()
