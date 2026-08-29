"""研究数据仓库首次读取校验与校验缓存的集成测试。"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest
from sqlalchemy import Engine

import quant_research.data.repository as repository_module
from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.contracts import CanonicalBatch
from quant_research.data.pipeline.curate import CuratedPartitionStore
from quant_research.data.pipeline.dataset import DataPipeline
from quant_research.data.quality.models import QualityRunSpec
from quant_research.data.quality.runner import QualityRunner
from quant_research.data.repository import CanonicalResearchRepository
from quant_research.data.storage.partitions import RawPartitionStore
from quant_research.domain.enums import DatasetKind
from quant_research.domain.errors import QuantError
from quant_research.domain.identifiers import IndexId, InstrumentId
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
from quant_research.infrastructure.tushare.routing import TUSHARE_ROUTES

_NOW = datetime(2026, 8, 11, tzinfo=UTC)


def _partition_record(
    path: Path, partition_key: str, *, content_hash: str = "a" * 64
) -> CanonicalPartitionRecord:
    """构造目录分区记录以验证研究仓库的分区身份边界。"""
    return CanonicalPartitionRecord(
        partition_key=partition_key,
        content_hash=content_hash,
        path=path,
        schema_fingerprint="b" * 64,
        input_hash="c" * 64,
        row_count=0,
    )


def _dataset_record(
    partitions: tuple[CanonicalPartitionRecord, ...],
) -> CanonicalDatasetRecord:
    """构造只用于目录身份校验的 Canonical 数据集记录。"""
    return CanonicalDatasetRecord(
        dataset=DatasetKind.STOCK_RISK_WARNING,
        content_hash="d" * 64,
        source="tushare",
        partitions=partitions,
        start_date=date(2006, 1, 1),
        end_date=date(2007, 12, 31),
        updated_at=_NOW,
    )


def test_repository_allows_identical_empty_content_across_partition_keys(
    tmp_path: Path,
) -> None:
    """不同年份的空分区可以具有相同内容哈希，但必须保留不同路径。"""
    record = _dataset_record(
        (
            _partition_record(tmp_path / "year=2006" / "empty.parquet", "year=2006"),
            _partition_record(tmp_path / "year=2007" / "empty.parquet", "year=2007"),
        )
    )

    CanonicalResearchRepository._validate_catalog_partition_identities(
        DatasetKind.STOCK_RISK_WARNING, record
    )


def test_repository_rejects_duplicate_partition_paths(tmp_path: Path) -> None:
    """两个目录分区不得指向同一个物理文件。"""
    path = tmp_path / "shared.parquet"
    record = _dataset_record(
        (
            _partition_record(path, "year=2006"),
            _partition_record(path, "year=2007", content_hash="e" * 64),
        )
    )

    with pytest.raises(QuantError, match="duplicate partition path"):
        CanonicalResearchRepository._validate_catalog_partition_identities(
            DatasetKind.STOCK_RISK_WARNING, record
        )


def _canonical_frame(
    dataset: DatasetKind,
    rows: list[dict[str, object]],
) -> pl.DataFrame:
    """以当前 Canonical Schema 补齐测试未关注的可空字段。"""
    schema = CANONICAL_SCHEMAS[dataset].columns
    normalized = [dict.fromkeys(schema.names()) | row for row in rows]
    return pl.DataFrame(normalized, schema=schema, strict=False)


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
                self._adjustment_factor_batch(),
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
        dataset = DatasetKind.STOCK_MASTER
        frame = _canonical_frame(
            dataset,
            [
                {
                    "instrument_id": "600000.SH",
                    "symbol": "600000",
                    "exchange": "SSE",
                    "board": "MAIN",
                    "name": "浦发银行",
                    "list_status": "L",
                    "list_date": date(1999, 11, 10),
                    "source": "tushare",
                    "available_at": _NOW,
                    "availability_source": "test",
                    "pit_usable": True,
                    "ingested_at": _NOW,
                }
            ],
        )
        return CanonicalBatch(dataset, frame, ("a" * 64,))

    @staticmethod
    def _index_bar_batch() -> CanonicalBatch:
        dataset = DatasetKind.INDEX_DAILY_BAR
        frame = _canonical_frame(
            dataset,
            [
                {
                    "index_id": "000300.SH",
                    "trade_date": date(2026, 8, 11),
                    "open": 4100.0,
                    "high": 4120.0,
                    "low": 4090.0,
                    "close": 4110.0,
                    "preclose": 4080.0,
                    "change": 30.0,
                    "volume": 1_000_000,
                    "amount": 10_000_000.0,
                    "pct_change": 4110.0 / 4080.0 - 1.0,
                    "source": "tushare",
                    "available_at": _NOW,
                    "availability_source": "test",
                    "pit_usable": True,
                    "ingested_at": _NOW,
                }
            ],
        )
        return CanonicalBatch(dataset, frame, ("b" * 64,))

    @staticmethod
    def _daily_bar_batch() -> CanonicalBatch:
        days = [date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12)]
        dataset = DatasetKind.STOCK_DAILY_BAR
        rows = []
        for index, day in enumerate(days):
            close = [10.0, 11.0, 12.0][index]
            preclose = [10.0, 10.0, 11.0][index]
            rows.append(
                {
                    "instrument_id": "600000.SH",
                    "trade_date": day,
                    "open": [10.0, 10.0, 11.0][index],
                    "high": [10.0, 11.0, 12.0][index],
                    "low": [10.0, 10.0, 11.0][index],
                    "close": close,
                    "preclose": preclose,
                    "change": close - preclose,
                    "volume": [1_000, 1_100, 1_200][index],
                    "amount": [10_000.0, 12_000.0, 14_000.0][index],
                    "pct_change": close / preclose - 1.0,
                    "source": "tushare",
                    "available_at": datetime(2026, 8, day.day, tzinfo=UTC),
                    "availability_source": "test",
                    "pit_usable": True,
                    "ingested_at": _NOW,
                }
            )
        frame = _canonical_frame(
            dataset,
            rows,
        )
        return CanonicalBatch(dataset, frame, ("c" * 64,))

    @staticmethod
    def _adjustment_factor_batch() -> CanonicalBatch:
        dataset = DatasetKind.STOCK_ADJUSTMENT_FACTOR
        days = [date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12)]
        frame = _canonical_frame(
            dataset,
            [
                {
                    "instrument_id": "600000.SH",
                    "trade_date": day,
                    "adjustment_factor": 1.0,
                    "source": "tushare",
                    "available_at": datetime(2026, 8, day.day, tzinfo=UTC),
                    "availability_source": "test",
                    "pit_usable": True,
                    "ingested_at": _NOW,
                }
                for day in days
            ],
        )
        return CanonicalBatch(dataset, frame, ("f" * 64,))

    @staticmethod
    def _trade_calendar_batch() -> CanonicalBatch:
        days = [date(2026, 8, 10), date(2026, 8, 11), date(2026, 8, 12)]
        dataset = DatasetKind.TRADE_CALENDAR
        frame = _canonical_frame(
            dataset,
            [
                {
                    "exchange": "SSE",
                    "trade_date": day,
                    "is_trading_day": True,
                    "previous_trade_date": days[max(0, index - 1)],
                    "source": "tushare",
                    "available_at": datetime(2026, 8, day.day, tzinfo=UTC),
                    "availability_source": "exchange_calendar",
                    "pit_usable": True,
                    "ingested_at": _NOW,
                }
                for index, day in enumerate(days)
            ],
        )
        return CanonicalBatch(dataset, frame, ("d" * 64,))

    @staticmethod
    def _industry_batch() -> CanonicalBatch:
        dataset = DatasetKind.INDUSTRY_MEMBERSHIP
        memberships = (
            (
                "600000.SH",
                "J66",
                date(2026, 1, 5),
                date(2026, 1, 12),
                datetime(2026, 1, 5, tzinfo=UTC),
            ),
            (
                "600000.SH",
                "C39",
                date(2026, 1, 12),
                None,
                datetime(2026, 1, 12, tzinfo=UTC),
            ),
            (
                "600001.SH",
                "C39",
                date(2026, 1, 5),
                date(2026, 1, 13),
                datetime(2026, 1, 5, tzinfo=UTC),
            ),
            (
                "600002.SH",
                "J66",
                date(2026, 1, 12),
                None,
                datetime(2026, 1, 12, 8, tzinfo=UTC),
            ),
            (
                "600002.SH",
                "C39",
                date(2026, 1, 12),
                None,
                datetime(2026, 1, 12, 9, tzinfo=UTC),
            ),
            (
                "600003.SH",
                "I65",
                date(2026, 1, 6),
                None,
                datetime(2026, 1, 6, tzinfo=UTC),
            ),
            (
                "600003.SH",
                "C39",
                date(2026, 1, 12),
                None,
                datetime(2026, 1, 12, tzinfo=UTC),
            ),
        )
        rows: list[dict[str, object]] = [
                {
                    "level1_code": industry,
                    "level1_name": industry,
                    "instrument_id": instrument_id,
                    "instrument_name": instrument_id,
                    "in_date": in_date,
                    "out_date": out_date,
                    "is_current": out_date is None,
                    "in_available_at": in_available_at,
                    "out_available_at": (
                        datetime(
                            out_date.year,
                            out_date.month,
                            out_date.day,
                            tzinfo=UTC,
                        )
                        if out_date is not None
                        else None
                    ),
                    "source": "tushare",
                    "available_at": _NOW,
                    "availability_source": "tushare_retrieved_at",
                    "pit_usable": True,
                    "ingested_at": _NOW,
                }
            for instrument_id, industry, in_date, out_date, in_available_at in memberships
        ]
        rows.append(
            {
                "level1_code": "J66",
                "level1_name": "J66",
                "instrument_id": "600004.SH",
                "instrument_name": "600004.SH",
                "in_date": date(2026, 1, 5),
                "out_date": date(2026, 1, 10),
                "is_current": False,
                "in_available_at": datetime(2026, 1, 5, tzinfo=UTC),
                "out_available_at": datetime(2026, 1, 12, 17, tzinfo=UTC),
                "source": "tushare",
                "available_at": datetime(2026, 1, 5, tzinfo=UTC),
                "availability_source": "retrieved_at_no_supplier_announcement",
                "pit_usable": True,
                "ingested_at": _NOW,
            }
        )
        frame = _canonical_frame(dataset, rows)
        return CanonicalBatch(dataset, frame, ("e" * 64,))


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
        dataset = DatasetKind.STOCK_FINANCIAL_INDICATOR
        frame = _canonical_frame(
            dataset,
            [
                {
                    "instrument_id": "600000.SH",
                    "announcement_date": timestamp.date(),
                    "report_period": date(2025, 12, 31),
                    "roe": value,
                    "update_flag": str(revision),
                    "revision": revision,
                    "source": "tushare",
                    "available_at": timestamp,
                    "availability_source": "announcement",
                    "pit_usable": usable,
                    "ingested_at": _NOW,
                }
                for timestamp, value, revision, usable in zip(
                    available,
                    (0.10, 0.11, 0.12, 0.99),
                    (0, 1, 2, 3),
                    (True, True, True, False),
                    strict=True,
                )
            ],
        )
        return CanonicalBatch(dataset, frame, ("b" * 64,))


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
        (DatasetKind.STOCK_MASTER, repository.stocks),
        (DatasetKind.FUND_MASTER, repository.funds),
        (DatasetKind.INDEX_MASTER, repository.indexes),
        (
            DatasetKind.TRADE_CALENDAR,
            lambda: repository.trade_calendar(query_date, query_date),
        ),
        (
            DatasetKind.STOCK_DAILY_BAR,
            lambda: repository.stock_bars((), query_date, query_date),
        ),
        (
            DatasetKind.STOCK_DAILY_BAR,
            lambda: repository.adjusted_stock_bars((), query_date, query_date),
        ),
        (
            DatasetKind.FUND_DAILY_BAR,
            lambda: repository.fund_bars((), query_date, query_date),
        ),
        (
            DatasetKind.FUND_DAILY_BAR,
            lambda: repository.adjusted_fund_bars((), query_date, query_date),
        ),
        (
            DatasetKind.TRADE_CALENDAR,
            lambda: repository.stock_log_returns(
                (InstrumentId.parse("600000.SH"),),
                query_date,
                query_date,
                lookback_sessions=0,
            ),
        ),
        (
            DatasetKind.TRADE_CALENDAR,
            lambda: repository.fund_log_returns(
                (InstrumentId.parse("510300.SH"),),
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
            lambda: repository.stock_daily_basics((), query_date, query_date),
        ),
        (
            DatasetKind.STOCK_FINANCIAL_INDICATOR,
            lambda: repository.stock_financial_indicators(query_date, ()),
        ),
        (
            DatasetKind.STOCK_INCOME_STATEMENT,
            lambda: repository.stock_income_statements(query_date, ()),
        ),
        (
            DatasetKind.STOCK_BALANCE_SHEET,
            lambda: repository.stock_balance_sheets(query_date, ()),
        ),
        (
            DatasetKind.STOCK_CASH_FLOW_STATEMENT,
            lambda: repository.stock_cash_flow_statements(query_date, ()),
        ),
        (
            DatasetKind.STOCK_DIVIDEND,
            lambda: repository.stock_dividends(query_date, ()),
        ),
        (
            DatasetKind.FUND_DIVIDEND,
            lambda: repository.fund_dividends(query_date, ()),
        ),
        (
            DatasetKind.INDUSTRY_CATALOG,
            repository.industry_catalog,
        ),
        (
            DatasetKind.INDUSTRY_MEMBERSHIP,
            lambda: repository.industry_memberships_on_dates(None, (query_date,)),
        ),
        (
            DatasetKind.STOCK_SUSPENSION,
            lambda: repository.stock_suspensions(query_date, query_date),
        ),
        (
            DatasetKind.STOCK_RISK_WARNING,
            lambda: repository.stock_risk_warnings(query_date, query_date),
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
            DatasetKind.STOCK_ADJUSTMENT_FACTOR,
            DatasetKind.INDEX_DAILY_BAR,
            DatasetKind.TRADE_CALENDAR,
            DatasetKind.INDUSTRY_MEMBERSHIP,
        }
        assert harness.repository.stocks().collect().height == 1
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

        batch = harness.repository.industry_memberships_on_dates(
            instruments, query_dates
        ).collect()

        assert not batch.select("query_date", "instrument_id").is_duplicated().any()
        assert {
            (row["query_date"], row["instrument_id"]): row["level1_code"]
            for row in batch.iter_rows(named=True)
        } == {
            (date(2026, 1, 5), "600000.SH"): "J66",
            (date(2026, 1, 5), "600001.SH"): "C39",
            (date(2026, 1, 6), "600000.SH"): "J66",
            (date(2026, 1, 6), "600001.SH"): "C39",
            (date(2026, 1, 6), "600003.SH"): "I65",
            (date(2026, 1, 11), "600000.SH"): "J66",
            (date(2026, 1, 11), "600001.SH"): "C39",
            (date(2026, 1, 11), "600003.SH"): "I65",
            (date(2026, 1, 12), "600000.SH"): "C39",
            (date(2026, 1, 12), "600001.SH"): "C39",
            (date(2026, 1, 12), "600002.SH"): "C39",
            (date(2026, 1, 12), "600003.SH"): "C39",
            (date(2026, 1, 13), "600000.SH"): "C39",
            (date(2026, 1, 13), "600002.SH"): "C39",
            (date(2026, 1, 13), "600003.SH"): "C39",
        }

        empty = harness.repository.industry_memberships_on_dates(
            instruments, ()
        ).collect()
        assert empty.is_empty()
        assert empty.columns[0] == "query_date"
    finally:
        harness.close()


def test_industry_exit_only_applies_after_its_first_observation(tmp_path: Path) -> None:
    """已生效但尚未被观测的退出事件不得回写此前查询日。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        instrument = (InstrumentId.parse("600004.SH"),)
        query_dates = tuple(date(2026, 1, day) for day in (9, 10, 11, 12, 13))

        rows = harness.repository.industry_memberships_on_dates(
            instrument, query_dates
        ).collect()

        assert rows.get_column("query_date").to_list() == list(query_dates[:-1])
        assert rows.get_column("level1_code").to_list() == ["J66"] * 4
    finally:
        harness.close()


def test_derived_price_queries_use_end_as_information_cutoff(tmp_path: Path) -> None:
    """派生行情只使用结束日以内的信息，并输出会话级前复权对数收益。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        instrument = (InstrumentId.parse("600000.SH"),)
        start = date(2026, 8, 10)
        end = date(2026, 8, 11)

        adjusted = harness.repository.adjusted_stock_bars(
            instrument, start, end
        ).collect()
        returns = harness.repository.stock_log_returns(
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
            (IndexId.parse("000300.SH"),),
            date(2026, 8, 11),
            date(2026, 8, 11),
        ).collect()

        assert frame.select("index_id", "trade_date", "close").rows() == [
            ("000300.SH", date(2026, 8, 11), 4110.0)
        ]
    finally:
        harness.close()


def test_financial_indicators_select_latest_visible_revision(
    tmp_path: Path,
) -> None:
    """财务读取按报告期返回观察日可见的最新修订。"""
    harness = _FinancialRepositoryHarness(tmp_path)
    try:
        instrument = (InstrumentId.parse("600000.SH"),)

        latest = harness.repository.stock_financial_indicators(
            date(2026, 4, 30), instrument
        ).collect()

        assert latest.select("revision", "roe").rows() == [(1, 0.11)]
    finally:
        harness.close()


def test_income_statements_select_latest_visible_revision(tmp_path: Path) -> None:
    """利润表读取按股票、报告期和报表类型返回观察日可见的最新修订。"""
    database = tmp_path / "income.db"
    upgrade_database(database)
    engine = create_sqlite_engine(database)
    metadata = MetadataRepository(engine)
    store = CuratedPartitionStore(tmp_path / "income-canonical")
    dataset = DatasetKind.STOCK_INCOME_STATEMENT
    available = (
        datetime(2026, 4, 1, tzinfo=UTC),
        datetime(2026, 4, 10, tzinfo=UTC),
        datetime(2026, 5, 2, tzinfo=UTC),
    )
    frame = _canonical_frame(
        dataset,
        [
            {
                "instrument_id": "600000.SH",
                "announcement_date": timestamp.date(),
                "actual_announcement_date": timestamp.date(),
                "report_period": date(2025, 12, 31),
                "report_type": "1",
                "company_type": "2",
                "report_period_type": "4",
                "total_revenue": value,
                "update_flag": "1",
                "revision": revision,
                "source": "tushare",
                "available_at": timestamp,
                "availability_source": "actual_announcement_date_eod",
                "pit_usable": True,
                "ingested_at": _NOW,
            }
            for timestamp, value, revision in zip(
                available,
                (100.0, 101.0, 102.0),
                (0, 1, 2),
                strict=True,
            )
        ],
    )
    try:
        result = store.publish(
            (CanonicalBatch(dataset, frame, ("c" * 64,)),),
            previous_datasets={},
            run_id="income-history-test",
            source="tushare",
            start=date(2025, 12, 31),
            end=date(2025, 12, 31),
            repository=metadata,
        )
        record = result.datasets[dataset.value]
        state = metadata.catalog_state()
        quality = metadata.register_quality_run(
            QualityRunSpec(
                dataset_hashes={dataset.value: record.content_hash},
                input_hash=state.catalog_hash,
                scope="ALL",
                started_at=_NOW,
                completed_at=_NOW,
                issues=(),
            )
        )
        metadata.mark_catalog_validated(quality.id, validated_at=_NOW)
        repository = CanonicalResearchRepository(
            metadata,
            trusted_curated_root=store.root,
        )

        latest = repository.stock_income_statements(
            date(2026, 4, 30),
            (InstrumentId.parse("600000.SH"),),
        ).collect()

        assert latest.select("revision", "total_revenue").rows() == [(1, 101.0)]
    finally:
        engine.dispose()


def test_first_read_rejects_corrupted_partition(tmp_path: Path) -> None:
    """分区字节损坏应在对应 API 首次读取时失败。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        harness.record.partitions[0].path.write_bytes(b"not parquet")

        with pytest.raises(ValueError, match="canonical partition is unavailable"):
            harness.repository.stocks()
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
                repository.stocks()
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

        assert harness.repository.stocks().collect().height == 1
        assert harness.repository.stocks().collect().height == 1
        assert calls == 1
    finally:
        harness.close()


def test_validate_rejects_quality_valid_partition_with_changed_bytes(
    tmp_path: Path,
) -> None:
    """VALIDATE 必须把实际 Parquet 字节绑定到当前目录身份。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        partition = harness.record.partitions[0]
        changed = harness._instrument_batch().frame.with_columns(
            pl.lit("600001.SH").alias("instrument_id"),
            pl.lit("600001").alias("symbol"),
        )
        pq.write_table(changed.to_arrow(), partition.path, compression="zstd")
        pipeline = DataPipeline(
            source=SimpleNamespace(provider="tushare"),  # type: ignore[arg-type]
            mapper=object(),  # type: ignore[arg-type]
            calendar=object(),  # type: ignore[arg-type]
            raw_store=RawPartitionStore(tmp_path / "raw"),
            curated_store=harness.store,
            repository=harness.metadata,
            quality_runner=QualityRunner(),
            routes=TUSHARE_ROUTES,
        )

        with pytest.raises(ValueError, match="integrity checks"):
            pipeline.validate(DatasetKind.STOCK_MASTER)
    finally:
        harness.close()


def test_lazy_bar_query_survives_canonical_pointer_change(tmp_path: Path) -> None:
    """已返回的惰性查询必须继续读取发布时绑定的不可变文件。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        dataset = DatasetKind.STOCK_DAILY_BAR
        instrument = (InstrumentId.parse("600000.SH"),)
        query_date = date(2026, 8, 11)
        old_record = harness.metadata.get_canonical_dataset(dataset)
        old_path = old_record.partitions[0].path
        lazy = harness.repository.stock_bars(instrument, query_date, query_date)
        original = harness._daily_bar_batch()
        changed = CanonicalBatch(
            dataset,
            original.frame.with_columns((pl.col("amount") + 1.0).alias("amount")),
            ("9" * 64,),
        )

        harness.store.publish(
            (changed,),
            previous_datasets={dataset.value: old_record},
            run_id="lazy-reader-pointer-change",
            source="tushare",
            start=query_date,
            end=query_date,
            repository=harness.metadata,
        )

        assert old_path.is_file()
        assert lazy.collect().get_column("amount").to_list() == [12_000.0]
    finally:
        harness.close()


def test_partition_lease_cache_verifies_each_physical_path(tmp_path: Path) -> None:
    """相同内容身份位于不同路径时，每个物理路径仍须独立验证。"""
    dataset = DatasetKind.STOCK_RISK_WARNING
    store = CuratedPartitionStore(tmp_path / "canonical")
    empty = pl.DataFrame(schema=CANONICAL_SCHEMAS[dataset].columns)
    first, _ = store._publish_partition(dataset, "year=2006", empty)
    second, _ = store._publish_partition(dataset, "year=2007", empty)
    second.path.write_bytes(b"not parquet")
    repository = CanonicalResearchRepository(
        _UnusedCatalog(),
        trusted_curated_root=store.root,
    )

    repository._partition_leases.acquire(
        CanonicalPartitionRecord(
            partition_key=first.partition_key,
            content_hash=first.content_hash,
            path=first.path,
            schema_fingerprint=first.schema_fingerprint,
            input_hash="1" * 64,
            row_count=first.row_count,
        ),
        max_bytes=1024 * 1024,
    )
    with pytest.raises(ValueError, match="unavailable"):
        repository._partition_leases.acquire(
            CanonicalPartitionRecord(
                partition_key=second.partition_key,
                content_hash=second.content_hash,
                path=second.path,
                schema_fingerprint=second.schema_fingerprint,
                input_hash="2" * 64,
                row_count=second.row_count,
            ),
            max_bytes=1024 * 1024,
        )


def test_financial_query_remains_a_parquet_lazy_plan(tmp_path: Path) -> None:
    """财务查询返回前不得先物化为内存 DataFrame。"""
    harness = _FinancialRepositoryHarness(tmp_path)
    try:
        plan = harness.repository.stock_financial_indicators(
            date(2026, 4, 30),
            (InstrumentId.parse("600000.SH"),),
        )

        assert "Parquet SCAN" in plan.explain()
        assert plan.select("roe").collect().get_column("roe").to_list() == [0.11]
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
            harness.repository.stocks()
    finally:
        harness.close()


def test_dataset_size_limit_applies_to_cached_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """缓存命中的 Lease 也必须重新检查本次数据集大小额度。"""
    harness = _ResearchRepositoryHarness(tmp_path)
    try:
        assert harness.repository.stocks().collect().height == 1
        file_size = harness.record.partitions[0].path.stat().st_size
        monkeypatch.setattr(
            repository_module,
            "_MAX_DATASET_FILE_BYTES",
            file_size - 1,
        )

        with pytest.raises(ValueError, match="size limit"):
            harness.repository.stocks()
    finally:
        harness.close()
