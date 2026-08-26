"""研究数据仓库首次读取校验与校验缓存的集成测试。"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import pytest
from sqlalchemy import Engine

import quant_research.data.repository as repository_module
from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.contracts import CanonicalBatch
from quant_research.data.pipeline.curate import CuratedPartitionStore
from quant_research.data.quality.models import QualityRunSpec
from quant_research.data.repository import CanonicalResearchRepository
from quant_research.domain.enums import DatasetKind
from quant_research.domain.identifiers import InstrumentId
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
    CanonicalPartitionRecord,
    DataCatalogState,
    MetadataRepository,
)

_NOW = datetime(2026, 8, 11, tzinfo=UTC)


class _StaticCatalog:
    """为单个已验证数据集提供固定目录记录。"""

    def __init__(
        self,
        state: DataCatalogState,
        record: CanonicalDatasetRecord,
    ) -> None:
        self._state = state
        self._record = record

    def require_validated_catalog(self) -> DataCatalogState:
        return self._state

    def get_canonical_dataset(self, dataset: DatasetKind) -> CanonicalDatasetRecord:
        if dataset is not self._record.dataset:
            raise KeyError(dataset)
        return self._record

    def list_canonical_datasets(self) -> tuple[CanonicalDatasetRecord, ...]:
        return (self._record,)


class _UnusedCatalog:
    """验证入口调用顺序时使用的不可访问目录。"""

    def require_validated_catalog(self) -> DataCatalogState:
        raise AssertionError("catalog must not be reached before internal verification")

    def get_canonical_dataset(self, dataset: DatasetKind) -> CanonicalDatasetRecord:
        raise AssertionError(dataset)

    def list_canonical_datasets(self) -> tuple[CanonicalDatasetRecord, ...]:
        raise AssertionError("catalog must not be listed before internal verification")


class _ResearchRepositoryHarness:
    """创建包含证券、行情、指数和交易日历的已验证本地仓库。"""

    def __init__(self, root: Path) -> None:
        database = root / "quant.db"
        upgrade_database(database)
        self.engine: Engine = create_sqlite_engine(database)
        self.metadata = MetadataRepository(self.engine)
        self.store = CuratedPartitionStore(root / "canonical")
        result = self.store.publish(
            (
                self._instrument_batch(),
                self._daily_bar_batch(),
                self._index_bar_batch(),
                self._trade_calendar_batch(),
                self._industry_batch(),
            ),
            previous_datasets={},
            run_id="research-repository-test",
            source="tushare",
            start=date(2026, 8, 11),
            end=date(2026, 8, 11),
            repository=self.metadata,
        )
        self.record = result.datasets[DatasetKind.STOCK_MASTER.value]
        state = self.metadata.catalog_state()
        quality = self.metadata.register_quality_run(
            QualityRunSpec(
                dataset_hashes={
                    name: record.content_hash
                    for name, record in result.datasets.items()
                },
                input_hash=state.catalog_hash,
                scope="ALL",
                started_at=_NOW,
                completed_at=_NOW,
                issues=(),
            )
        )
        self.state = self.metadata.mark_catalog_validated(
            quality.id,
            validated_at=_NOW,
        )
        self.repository = CanonicalResearchRepository.from_sqlite(
            self.engine,
            trusted_curated_root=self.store.root,
        )

    def close(self) -> None:
        """释放测试数据库引擎。"""
        self.engine.dispose()

    @staticmethod
    def _instrument_batch() -> CanonicalBatch:
        frame = pl.DataFrame(
            {
                "instrument_id": ["600000.SH"],
                "exchange": ["SSE"],
                "board": ["MAIN"],
                "name": ["浦发银行"],
                "instrument_type": ["STOCK"],
                "listing_status": ["LISTED"],
                "list_date": [date(1999, 11, 10)],
                "delist_date": [None],
                "source": ["tushare"],
                "available_at": [_NOW],
                "availability_source": ["test"],
                "pit_usable": [True],
                "ingested_at": [_NOW],
            },
            schema=CANONICAL_SCHEMAS[DatasetKind.STOCK_MASTER].columns,
        )
        return CanonicalBatch(DatasetKind.STOCK_MASTER, frame, ("a" * 64,))

    @staticmethod
    def _index_bar_batch() -> CanonicalBatch:
        frame = pl.DataFrame(
            {
                "index_id": ["000300.SH"],
                "trade_date": [date(2026, 8, 11)],
                "open": [4100.0],
                "high": [4120.0],
                "low": [4090.0],
                "close": [4110.0],
                "preclose": [4080.0],
                "volume": [1_000_000],
                "amount": [10_000_000.0],
                "pct_change": [0.735294],
                "source": ["tushare"],
                "available_at": [_NOW],
                "availability_source": ["test"],
                "pit_usable": [True],
                "ingested_at": [_NOW],
            },
            schema=CANONICAL_SCHEMAS[DatasetKind.INDEX_DAILY_BAR].columns,
        )
        return CanonicalBatch(DatasetKind.INDEX_DAILY_BAR, frame, ("b" * 64,))

    @staticmethod
    def _daily_bar_batch() -> CanonicalBatch:
        days = [date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12)]
        frame = pl.DataFrame(
            {
                "instrument_id": ["600000.SH"] * 3,
                "trade_date": days,
                "open": [10.0, 10.0, 11.0],
                "high": [10.0, 11.0, 12.0],
                "low": [10.0, 10.0, 11.0],
                "close": [10.0, 11.0, 12.0],
                "preclose": [10.0, 10.0, 11.0],
                "volume": [1_000, 1_100, 1_200],
                "amount": [10_000.0, 12_000.0, 14_000.0],
                "adjustment_flag": ["3"] * 3,
                "pct_change": [0.0, 10.0, 9.090909],
                "source": ["tushare"] * 3,
                "available_at": [
                    datetime(2026, 8, day.day, tzinfo=UTC) for day in days
                ],
                "availability_source": ["test"] * 3,
                "pit_usable": [True] * 3,
                "ingested_at": [_NOW] * 3,
            },
            schema=CANONICAL_SCHEMAS[DatasetKind.STOCK_DAILY_BAR].columns,
        )
        return CanonicalBatch(DatasetKind.STOCK_DAILY_BAR, frame, ("c" * 64,))

    @staticmethod
    def _trade_calendar_batch() -> CanonicalBatch:
        days = [date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12)]
        frame = pl.DataFrame(
            {
                "trade_date": days,
                "is_trading_day": [True] * 3,
                "source": ["tushare"] * 3,
                "available_at": [
                    datetime(2026, 8, day.day, tzinfo=UTC) for day in days
                ],
                "availability_source": ["exchange_calendar"] * 3,
                "pit_usable": [True] * 3,
                "ingested_at": [_NOW] * 3,
            },
            schema=CANONICAL_SCHEMAS[DatasetKind.TRADE_CALENDAR].columns,
        )
        return CanonicalBatch(DatasetKind.TRADE_CALENDAR, frame, ("d" * 64,))

    @staticmethod
    def _industry_batch() -> CanonicalBatch:
        taxonomy = "证监会行业分类"
        events = (
            (date(2026, 1, 5), "600000.SH", "J66"),
            (date(2026, 1, 5), "600001.SH", "C39"),
            (date(2026, 1, 5), "600002.SH", None),
            (date(2026, 1, 6), "600003.SH", "I65"),
            (date(2026, 1, 12), "600000.SH", "C39"),
            (date(2026, 1, 12), "600002.SH", "J66"),
            (date(2026, 1, 13), "600001.SH", None),
        )
        rows = []
        for as_of_date, instrument_id, industry in events:
            rows.append(
                {
                    "as_of_date": as_of_date,
                    "supplier_update_date": as_of_date,
                    "instrument_id": instrument_id,
                    "taxonomy": taxonomy,
                    "industry_code": industry,
                    "industry_name": industry,
                    "is_classified": industry is not None,
                    "source": "baostock",
                    "available_at": datetime.combine(
                        as_of_date,
                        datetime.max.time(),
                        tzinfo=ZoneInfo("Asia/Shanghai"),
                    ).astimezone(UTC),
                    "availability_source": "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED",
                    "pit_usable": True,
                    "ingested_at": _NOW,
                }
            )
        frame = pl.DataFrame(
            rows,
            schema=CANONICAL_SCHEMAS[DatasetKind.INDUSTRY_MEMBERSHIP].columns,
        )
        return CanonicalBatch(DatasetKind.INDUSTRY_MEMBERSHIP, frame, ("e" * 64,))


class _FinancialRepositoryHarness:
    """创建包含多次财务修订的已验证本地仓库。"""

    def __init__(self, root: Path) -> None:
        database = root / "financial.db"
        upgrade_database(database)
        self.engine: Engine = create_sqlite_engine(database)
        self.metadata = MetadataRepository(self.engine)
        self.store = CuratedPartitionStore(root / "financial-canonical")
        result = self.store.publish(
            (self._financial_batch(),),
            previous_datasets={},
            run_id="financial-history-test",
            source="tushare",
            start=date(2025, 12, 31),
            end=date(2025, 12, 31),
            repository=self.metadata,
        )
        record = result.datasets[DatasetKind.STOCK_FINANCIAL_INDICATOR.value]
        state = self.metadata.catalog_state()
        quality = self.metadata.register_quality_run(
            QualityRunSpec(
                dataset_hashes={
                    DatasetKind.STOCK_FINANCIAL_INDICATOR.value: record.content_hash
                },
                input_hash=state.catalog_hash,
                scope="ALL",
                started_at=_NOW,
                completed_at=_NOW,
                issues=(),
            )
        )
        self.metadata.mark_catalog_validated(quality.id, validated_at=_NOW)
        self.repository = CanonicalResearchRepository(
            self.metadata,
            trusted_curated_root=self.store.root,
        )

    def close(self) -> None:
        """释放测试数据库引擎。"""
        self.engine.dispose()

    @staticmethod
    def _financial_batch() -> CanonicalBatch:
        available = [
            datetime(2026, 4, 1, tzinfo=UTC),
            datetime(2026, 4, 10, tzinfo=UTC),
            datetime(2026, 5, 2, tzinfo=UTC),
            datetime(2026, 4, 20, tzinfo=UTC),
        ]
        frame = pl.DataFrame(
            {
                "instrument_id": ["600000.SH"] * 4,
                "report_period": [date(2025, 12, 31)] * 4,
                "metric": ["dupont_roe"] * 4,
                "value": [0.10, 0.11, 0.12, 0.99],
                "revision": [0, 1, 2, 3],
                "announced_at": available,
                "source": ["tushare"] * 4,
                "available_at": available,
                "availability_source": ["announcement"] * 4,
                "pit_usable": [True, True, True, False],
                "ingested_at": [_NOW] * 4,
            },
            schema=CANONICAL_SCHEMAS[DatasetKind.STOCK_FINANCIAL_INDICATOR].columns,
        )
        return CanonicalBatch(DatasetKind.STOCK_FINANCIAL_INDICATOR, frame, ("b" * 64,))


def test_every_public_read_api_enters_internal_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """所有公开读取 API 都必须先进入仓库内部的数据集校验。"""
    repository = CanonicalResearchRepository(
        _UnusedCatalog(),
        trusted_curated_root=tmp_path,
    )
    observed: list[DatasetKind] = []

    def reject(dataset: DatasetKind) -> None:
        observed.append(dataset)
        raise RuntimeError("internal verification reached")

    monkeypatch.setattr(repository, "_verify_current_dataset", reject)
    query_date = date(2026, 8, 11)
    calls = (
        (DatasetKind.STOCK_MASTER, repository.instruments),
        (
            DatasetKind.TRADE_CALENDAR,
            lambda: repository.trade_calendar(query_date, query_date),
        ),
        (
            DatasetKind.STOCK_DAILY_BAR,
            lambda: repository.bars((), query_date, query_date),
        ),
        (
            DatasetKind.STOCK_DAILY_BAR,
            lambda: repository.adjusted_bars((), query_date, query_date),
        ),
        (
            DatasetKind.TRADE_CALENDAR,
            lambda: repository.log_returns(
                (InstrumentId.parse("600000.SH"),),
                query_date,
                query_date,
                lookback_sessions=0,
            ),
        ),
        (
            DatasetKind.INDEX_DAILY_BAR,
            lambda: repository.index_bars((), query_date, query_date),
        ),
        (
            DatasetKind.STOCK_DAILY_BASIC,
            lambda: repository.daily_basics((), query_date, query_date),
        ),
        (
            DatasetKind.STOCK_FINANCIAL_INDICATOR,
            lambda: repository.financials_as_of((), query_date),
        ),
        (
            DatasetKind.STOCK_FINANCIAL_INDICATOR,
            lambda: repository.financial_history((), query_date),
        ),
        (
            DatasetKind.INDUSTRY_MEMBERSHIP,
            lambda: repository.industry_classifications_as_of(None, query_date),
        ),
        (
            DatasetKind.INDUSTRY_MEMBERSHIP,
            lambda: repository.industry_classifications_on_dates(None, (query_date,)),
        ),
        (
            DatasetKind.STOCK_SUSPENSION,
            lambda: repository.security_status(query_date),
        ),
    )

    for _, call in calls:
        with pytest.raises(RuntimeError, match="internal verification reached"):
            call()

    assert observed == [dataset for dataset, _ in calls]
    assert not hasattr(repository, "verify_current_dataset")


def test_from_sqlite_exposes_bound_read_only_catalog(tmp_path: Path) -> None:
    """SQLite 工厂应复用外部 Engine，并通过研究仓库暴露只读目录。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        catalog = harness.repository.catalog()

        assert catalog.require_validated_catalog() == harness.state
        assert {record.dataset for record in catalog.list_canonical_datasets()} == {
            DatasetKind.STOCK_MASTER,
            DatasetKind.STOCK_DAILY_BAR,
            DatasetKind.INDEX_DAILY_BAR,
            DatasetKind.TRADE_CALENDAR,
            DatasetKind.INDUSTRY_MEMBERSHIP,
        }
        assert harness.repository.instruments().collect().height == 1
    finally:
        harness.close()


def test_industry_single_and_batch_queries_rebuild_identical_tombstone_state(
    tmp_path: Path,
) -> None:
    """单日期和批量入口必须按请求日重建同一行业事件状态。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        instruments = tuple(
            InstrumentId.parse(value)
            for value in ("600000.SH", "600001.SH", "600002.SH", "600003.SH")
        )
        query_dates = (
            date(2026, 1, 5),
            date(2026, 1, 6),
            date(2026, 1, 11),
            date(2026, 1, 12),
            date(2026, 1, 13),
        )

        batch = harness.repository.industry_classifications_on_dates(
            instruments, query_dates
        ).collect()

        assert {
            (row["query_date"], row["instrument_id"]): row["industry_code"]
            for row in batch.iter_rows(named=True)
        } == {
            (date(2026, 1, 5), "600000.SH"): "J66",
            (date(2026, 1, 5), "600001.SH"): "C39",
            (date(2026, 1, 5), "600002.SH"): None,
            (date(2026, 1, 6), "600000.SH"): "J66",
            (date(2026, 1, 6), "600001.SH"): "C39",
            (date(2026, 1, 6), "600002.SH"): None,
            (date(2026, 1, 6), "600003.SH"): "I65",
            (date(2026, 1, 11), "600000.SH"): "J66",
            (date(2026, 1, 11), "600001.SH"): "C39",
            (date(2026, 1, 11), "600002.SH"): None,
            (date(2026, 1, 11), "600003.SH"): "I65",
            (date(2026, 1, 12), "600000.SH"): "C39",
            (date(2026, 1, 12), "600001.SH"): "C39",
            (date(2026, 1, 12), "600002.SH"): "J66",
            (date(2026, 1, 12), "600003.SH"): "I65",
            (date(2026, 1, 13), "600000.SH"): "C39",
            (date(2026, 1, 13), "600001.SH"): None,
            (date(2026, 1, 13), "600002.SH"): "J66",
            (date(2026, 1, 13), "600003.SH"): "I65",
        }
        for query_date in query_dates:
            single = harness.repository.industry_classifications_as_of(
                instruments, query_date
            ).collect()
            from_batch = batch.filter(pl.col("query_date") == query_date).drop(
                "query_date"
            )
            assert single.equals(from_batch)

        empty = harness.repository.industry_classifications_on_dates(
            instruments, ()
        ).collect()
        assert empty.is_empty()
        assert empty.columns[0] == "query_date"
    finally:
        harness.close()


def test_derived_price_queries_use_end_as_information_cutoff(tmp_path: Path) -> None:
    """派生行情只使用结束日以内的信息，并输出会话级前复权对数收益。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        instrument = (InstrumentId.parse("600000.SH"),)
        start = date(2026, 8, 10)
        end = date(2026, 8, 11)

        adjusted = harness.repository.adjusted_bars(instrument, start, end).collect()
        returns = harness.repository.log_returns(
            instrument,
            end,
            end,
            lookback_sessions=1,
        ).collect()

        assert adjusted["trade_date"].to_list() == [start, end]
        assert adjusted["adjustment_as_of"].to_list() == [end, end]
        assert adjusted["adjustment_mode"].to_list() == ["FORWARD", "FORWARD"]
        assert returns["trade_date"].to_list() == [start, end]
        assert returns["forward_log_return"].to_list() == pytest.approx(
            [0.0, 0.09531017980432493]
        )
    finally:
        harness.close()


def test_index_bars_reads_only_requested_canonical_index(tmp_path: Path) -> None:
    """指数行情入口应读取独立的已验证 ``index_bar`` 数据集。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        frame = harness.repository.index_bars(
            (InstrumentId.parse("000300.SH"),),
            date(2026, 8, 11),
            date(2026, 8, 11),
        ).collect()

        assert frame.select("index_id", "trade_date", "close").rows() == [
            ("000300.SH", date(2026, 8, 11), 4110.0)
        ]
    finally:
        harness.close()


def test_financial_history_keeps_visible_revisions_without_pit_collapse(
    tmp_path: Path,
) -> None:
    """历史读取保留全部可见修订，而 as-of 读取只返回最新修订。"""
    harness = _FinancialRepositoryHarness(tmp_path)
    try:
        instrument = (InstrumentId.parse("600000.SH"),)

        history = harness.repository.financial_history(
            ("dupont_roe",), date(2026, 4, 30), instrument
        ).collect()
        latest = harness.repository.financials_as_of(
            ("dupont_roe",), date(2026, 4, 30), instrument
        ).collect()

        assert history.select("revision", "value").rows() == [
            (0, 0.10),
            (1, 0.11),
        ]
        assert latest.select("revision", "value").rows() == [(1, 0.11)]
    finally:
        harness.close()


def test_first_read_rejects_corrupted_partition(tmp_path: Path) -> None:
    """分区字节损坏应在对应 API 首次读取时失败。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        harness.record.partitions[0].path.write_bytes(b"not parquet")

        with pytest.raises(ValueError, match="canonical partition is unavailable"):
            harness.repository.instruments()
    finally:
        harness.close()


def test_first_read_rejects_catalog_integrity_mismatches(tmp_path: Path) -> None:
    """行数、Schema 指纹和内容哈希不一致均不得通过首次读取。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        partition = harness.record.partitions[0]
        variants = (
            replace(partition, row_count=partition.row_count + 1),
            replace(partition, schema_fingerprint="0" * 64),
            replace(partition, content_hash="0" * 64),
        )
        for variant in variants:
            record = replace(harness.record, partitions=(variant,))
            repository = CanonicalResearchRepository(
                _StaticCatalog(harness.state, record),
                trusted_curated_root=harness.store.root,
            )

            with pytest.raises(ValueError, match="canonical partition is unavailable"):
                repository.instruments()
    finally:
        harness.close()


def test_repeated_read_reuses_verified_partition_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """相同分区的重复读取不得重复计算完整内容哈希。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        verifier = harness.repository._partition_verifier
        original = verifier.verify
        calls = 0

        def counted_verify(
            partition: CanonicalPartitionRecord,
            *,
            max_bytes: int,
        ) -> int:
            nonlocal calls
            calls += 1
            return original(partition, max_bytes=max_bytes)

        monkeypatch.setattr(verifier, "verify", counted_verify)

        assert harness.repository.instruments().collect().height == 1
        assert harness.repository.instruments().collect().height == 1
        assert calls == 1
    finally:
        harness.close()


def test_dataset_size_limit_applies_on_first_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """首次校验必须使用数据集剩余大小额度限制文件读取。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        file_size = harness.record.partitions[0].path.stat().st_size
        monkeypatch.setattr(
            repository_module,
            "_MAX_DATASET_FILE_BYTES",
            file_size - 1,
        )

        with pytest.raises(ValueError, match="size limit"):
            harness.repository.instruments()
    finally:
        harness.close()


def test_dataset_size_limit_applies_to_cached_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """缓存命中的 Lease 也必须重新检查本次数据集大小额度。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        assert harness.repository.instruments().collect().height == 1
        file_size = harness.record.partitions[0].path.stat().st_size
        monkeypatch.setattr(
            repository_module,
            "_MAX_DATASET_FILE_BYTES",
            file_size - 1,
        )

        with pytest.raises(ValueError, match="size limit"):
            harness.repository.instruments()
    finally:
        harness.close()
