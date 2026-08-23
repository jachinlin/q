from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from quant_research.data.catalog import DATASET_CATALOG
from quant_research.data.contracts import (
    CanonicalBatch,
    JsonValue,
    PublishedPartition,
    RawBatch,
)
from quant_research.data.pipeline.curate import CuratedPartitionStore
from quant_research.data.pipeline.dataset import DataPipeline
from quant_research.data.quality.runner import QualityRunner
from quant_research.data.sources.routing import Route, RoutingTable
from quant_research.data.storage.partitions import RawPartitionStore
from quant_research.domain.enums import DatasetKind
from quant_research.infrastructure.baostock.client import (
    DAILY_BAR_FIELDS,
    DUPONT_FIELDS,
)
from quant_research.infrastructure.baostock.mapper import BaoStockMapper
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.repositories import (
    MetadataRepository,
    RawPartitionSpec,
)

DAILY_DATASETS = {
    DatasetKind.DAILY_BAR,
    DatasetKind.DAILY_BASIC,
    DatasetKind.SECURITY_STATUS,
}
RETRIEVED_AT = datetime(2026, 8, 11, 9, tzinfo=UTC)


class _Source:
    provider = "baostock"


class _FinancialSource:
    provider = "baostock"

    def __init__(
        self,
        request: Mapping[str, JsonValue],
        row: Mapping[str, JsonValue],
    ) -> None:
        self.request = request
        self.row = row
        self.fetch_calls = 0

    def login(self) -> None:
        pass

    def close(self) -> None:
        pass

    def financial_requests(
        self, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        del start, end
        return (self.request,)

    def fetch_financials(
        self, request: Mapping[str, JsonValue]
    ) -> tuple[RawBatch, ...]:
        self.fetch_calls += 1
        return (
            RawBatch(
                source="baostock",
                endpoint="query_dupont_data",
                request=request,
                retrieved_at=datetime(2026, 8, 12, 1, tzinfo=UTC),
                schema=DUPONT_FIELDS,
                rows=(self.row,),
            ),
        )


class _Calendar:
    def bootstrap_window(self, years: int) -> tuple[date, date]:
        del years
        return date(2000, 1, 1), date(2026, 8, 11)

    def latest_complete_day(self) -> date:
        return date(2026, 8, 11)

    def explicit_window(self, start: date, end: date) -> tuple[date, date]:
        return start, end


class _CountingMapper:
    def __init__(self) -> None:
        self.delegate = BaoStockMapper()
        self.normalized_requests: list[str] = []
        self.transform_salt = ""

    def accepts_raw_schema(self, endpoint: str, schema_fingerprint: str) -> bool:
        return self.delegate.accepts_raw_schema(endpoint, schema_fingerprint)

    def normalize(self, raw_partition: PublishedPartition) -> Iterable[CanonicalBatch]:
        self.normalized_requests.append(raw_partition.request_hash)
        return self.delegate.normalize(raw_partition)

    def candidate_partition_keys(
        self, dataset: DatasetKind, raw_partition: PublishedPartition
    ) -> tuple[str, ...]:
        return self.delegate.candidate_partition_keys(dataset, raw_partition)

    def raw_head_is_usable(
        self,
        dataset: DatasetKind,
        request: Mapping[str, JsonValue],
        observed_at: datetime,
    ) -> bool:
        return self.delegate.raw_head_is_usable(dataset, request, observed_at)

    def requires_raw_history(self, dataset: DatasetKind) -> bool:
        return self.delegate.requires_raw_history(dataset)

    def consolidate_partition(
        self, dataset: DatasetKind, frames: Sequence[pl.DataFrame]
    ) -> pl.DataFrame:
        return self.delegate.consolidate_partition(dataset, frames)

    def transform_hash(self, dataset: DatasetKind) -> str:
        digest = hashlib.sha256()
        digest.update(bytes.fromhex(self.delegate.transform_hash(dataset)))
        digest.update(self.transform_salt.encode("utf-8"))
        return digest.hexdigest()


def _routes() -> RoutingTable:
    return RoutingTable(
        {
            dataset: ((Route(1, "baostock"),) if dataset in DAILY_DATASETS else ())
            for dataset in DATASET_CATALOG
        }
    )


def _financial_routes() -> RoutingTable:
    return RoutingTable(
        {
            dataset: (
                (Route(1, "baostock"),)
                if dataset is DatasetKind.FINANCIAL_OBSERVATION
                else ()
            )
            for dataset in DATASET_CATALOG
        }
    )


def _row(trade_date: date, code: str, *, close: str = "10.50") -> dict[str, str]:
    values = {field: "" for field in DAILY_BAR_FIELDS}
    values.update(
        {
            "date": trade_date.isoformat(),
            "code": code,
            "open": "10.00",
            "high": "11.00",
            "low": "9.50",
            "close": close,
            "preclose": "10.00",
            "volume": "1000",
            "amount": "10500.00",
            "adjustflag": "3",
            "turn": "1.25",
            "tradestatus": "1",
            "pctChg": "5.00",
            "peTTM": "12.00",
            "pbMRQ": "1.50",
            "psTTM": "2.00",
            "isST": "0",
        }
    )
    return values


def _dupont_row(*, roe: str, asset_turn: str = "0.75") -> dict[str, str]:
    values = {field: "" for field in DUPONT_FIELDS}
    values.update(
        {
            "code": "sz.000001",
            "pubDate": "2026-04-30",
            "statDate": "2025-12-31",
            "dupontROE": roe,
            "dupontAssetTurn": asset_turn,
        }
    )
    return values


def _publish_daily(
    raw_store: RawPartitionStore,
    repository: MetadataRepository,
    trade_date: date,
    rows: tuple[dict[str, str], ...],
    *,
    retrieved_at: datetime = RETRIEVED_AT,
) -> None:
    request = {
        "api": "query_daily_history_k_AStock",
        "scope": "ALL",
        "date": trade_date.isoformat(),
        "frequency": "d",
    }
    published = raw_store.publish(
        RawBatch(
            source="baostock",
            endpoint="query_daily_history_k_AStock",
            request=request,
            retrieved_at=retrieved_at,
            schema=DAILY_BAR_FIELDS,
            rows=rows,
        )
    )
    repository.register_raw_partition(
        RawPartitionSpec(
            source=published.source,
            endpoint=published.endpoint,
            request=published.request,
            request_hash=published.request_hash,
            content_hash=published.content_hash,
            data_path=published.data_path,
            manifest_path=published.manifest_path,
            schema_fingerprint=published.schema_fingerprint,
            row_count=published.row_count,
            retrieved_at=published.retrieved_at,
        )
    )


def _pipeline(
    tmp_path: Path,
) -> tuple[
    DataPipeline,
    _CountingMapper,
    RawPartitionStore,
    MetadataRepository,
    CuratedPartitionStore,
]:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    repository = MetadataRepository(create_sqlite_engine(database))
    raw_store = RawPartitionStore(tmp_path / "raw")
    curated_store = CuratedPartitionStore(tmp_path / "canonical")
    mapper = _CountingMapper()
    pipeline = DataPipeline(
        source=_Source(),  # type: ignore[arg-type]
        mapper=mapper,
        calendar=_Calendar(),
        raw_store=raw_store,
        curated_store=curated_store,
        repository=repository,
        quality_runner=QualityRunner(),
        routes=_routes(),
    )
    return pipeline, mapper, raw_store, repository, curated_store


def _financial_pipeline(
    tmp_path: Path,
    source: _FinancialSource,
) -> tuple[DataPipeline, RawPartitionStore, MetadataRepository, CuratedPartitionStore]:
    database = tmp_path / "quant.db"
    upgrade_database(database)
    repository = MetadataRepository(create_sqlite_engine(database))
    raw_store = RawPartitionStore(tmp_path / "raw")
    curated_store = CuratedPartitionStore(tmp_path / "canonical")
    pipeline = DataPipeline(
        source=source,  # type: ignore[arg-type]
        mapper=BaoStockMapper(),
        calendar=_Calendar(),
        raw_store=raw_store,
        curated_store=curated_store,
        repository=repository,
        quality_runner=QualityRunner(),
        routes=_financial_routes(),
    )
    return pipeline, raw_store, repository, curated_store


def _publish_dupont(
    raw_store: RawPartitionStore,
    repository: MetadataRepository,
    request: Mapping[str, JsonValue],
    row: Mapping[str, JsonValue],
    retrieved_at: datetime,
) -> None:
    published = raw_store.publish(
        RawBatch(
            source="baostock",
            endpoint="query_dupont_data",
            request=request,
            retrieved_at=retrieved_at,
            schema=DUPONT_FIELDS,
            rows=(row,),
        )
    )
    repository.register_raw_partition(
        RawPartitionSpec(
            source=published.source,
            endpoint=published.endpoint,
            request=published.request,
            request_hash=published.request_hash,
            content_hash=published.content_hash,
            data_path=published.data_path,
            manifest_path=published.manifest_path,
            schema_fingerprint=published.schema_fingerprint,
            row_count=published.row_count,
            retrieved_at=published.retrieved_at,
        )
    )


def test_curate_all_skips_unchanged_raw_and_maps_daily_fanout_once(
    tmp_path: Path,
) -> None:
    pipeline, mapper, raw_store, repository, _ = _pipeline(tmp_path)
    _publish_daily(
        raw_store,
        repository,
        date(2025, 12, 31),
        (_row(date(2025, 12, 31), "sh.600000"),),
    )
    _publish_daily(
        raw_store,
        repository,
        date(2026, 1, 2),
        (
            _row(date(2026, 1, 2), "sh.600000"),
            _row(date(2026, 1, 2), "sz.000001"),
        ),
    )

    first = pipeline.curate_all()

    assert len(mapper.normalized_requests) == 2
    assert all(result.rebuilt_partitions == 2 for result in first)
    mapper.normalized_requests.clear()

    second = pipeline.curate_all()

    assert mapper.normalized_requests == []
    assert all(result.rebuilt_partitions == 0 for result in second)
    assert all(result.raw_inputs_read == 0 for result in second)

    _publish_daily(
        raw_store,
        repository,
        date(2026, 1, 5),
        (_row(date(2026, 1, 5), "sh.600000", close="10.80"),),
    )
    mapper.normalized_requests.clear()

    third = pipeline.curate_all()

    assert len(mapper.normalized_requests) == 2
    assert all(result.rebuilt_partitions == 1 for result in third)
    assert all(result.raw_inputs_read == 2 for result in third)


def test_changed_raw_head_rebuilds_partition_and_removes_disappeared_rows(
    tmp_path: Path,
) -> None:
    pipeline, mapper, raw_store, repository, curated_store = _pipeline(tmp_path)
    trade_date = date(2026, 1, 2)
    _publish_daily(
        raw_store,
        repository,
        trade_date,
        (
            _row(trade_date, "sh.600000"),
            _row(trade_date, "sz.000001"),
        ),
    )
    pipeline.curate(DatasetKind.DAILY_BAR)

    _publish_daily(
        raw_store,
        repository,
        trade_date,
        (_row(trade_date, "sh.600000", close="10.70"),),
        retrieved_at=datetime(2026, 8, 12, 9, tzinfo=UTC),
    )
    mapper.normalized_requests.clear()

    result = pipeline.curate(DatasetKind.DAILY_BAR)
    current = repository.get_canonical_dataset(DatasetKind.DAILY_BAR)
    frame = curated_store.read_partition(DatasetKind.DAILY_BAR, current.partitions[0])

    assert result.rebuilt_partitions == 1
    assert len(mapper.normalized_requests) == 1
    assert frame.select("instrument_id", "close").rows() == [("600000.SH", 10.7)]


def test_transform_change_and_missing_file_force_selected_rebuilds(
    tmp_path: Path,
) -> None:
    pipeline, mapper, raw_store, repository, _ = _pipeline(tmp_path)
    for trade_date in (date(2025, 12, 31), date(2026, 1, 2)):
        _publish_daily(
            raw_store,
            repository,
            trade_date,
            (_row(trade_date, "sh.600000"),),
        )
    pipeline.curate(DatasetKind.DAILY_BAR)
    mapper.normalized_requests.clear()

    mapper.transform_salt = "mapping-changed"
    transformed = pipeline.curate(DatasetKind.DAILY_BAR)

    assert transformed.rebuilt_partitions == 2
    assert len(mapper.normalized_requests) == 2

    current = repository.get_canonical_dataset(DatasetKind.DAILY_BAR)
    year_2025 = next(
        item for item in current.partitions if item.partition_key == "year=2025"
    )
    year_2025.path.unlink()
    mapper.normalized_requests.clear()

    repaired = pipeline.curate(
        DatasetKind.DAILY_BAR,
        start=date(2025, 1, 1),
        end=date(2025, 12, 31),
    )

    assert repaired.rebuilt_partitions == 1
    assert len(mapper.normalized_requests) == 1
    assert year_2025.path.is_file()


def test_localize_refetches_a_schema_incompatible_financial_checkpoint(
    tmp_path: Path,
) -> None:
    request: Mapping[str, JsonValue] = {
        "endpoint": "query_dupont_data",
        "instrument_id": "000001.SZ",
        "report_year": 2025,
        "report_quarter": 4,
    }
    source = _FinancialSource(request, _dupont_row(roe="0.12"))
    pipeline, raw_store, repository, _ = _financial_pipeline(tmp_path, source)
    incompatible = raw_store.publish(
        RawBatch(
            source="baostock",
            endpoint="query_dupont_data",
            request=request,
            retrieved_at=datetime(2026, 8, 11, 1, tzinfo=UTC),
            schema=("code", "pubDate", "statDate", "metric", "value"),
            rows=(
                {
                    "code": "sz.000001",
                    "pubDate": "2026-04-30",
                    "statDate": "2025-12-31",
                    "metric": "dupont_roe",
                    "value": "0.12",
                },
            ),
        )
    )
    repository.register_raw_partition(
        RawPartitionSpec(
            source=incompatible.source,
            endpoint=incompatible.endpoint,
            request=incompatible.request,
            request_hash=incompatible.request_hash,
            content_hash=incompatible.content_hash,
            data_path=incompatible.data_path,
            manifest_path=incompatible.manifest_path,
            schema_fingerprint=incompatible.schema_fingerprint,
            row_count=incompatible.row_count,
            retrieved_at=incompatible.retrieved_at,
        )
    )

    result = pipeline.localize(
        DatasetKind.FINANCIAL_OBSERVATION,
        start=date(2025, 1, 1),
        end=date(2026, 5, 1),
    )
    current = repository.find_raw_partition(
        "baostock", "query_dupont_data", incompatible.request_hash
    )

    assert result.fetched == 1
    assert result.skipped == 0
    assert source.fetch_calls == 1
    assert current is not None
    assert BaoStockMapper.accepts_raw_schema(
        current.endpoint, current.schema_fingerprint
    )


def test_localize_always_reuses_a_compatible_raw_head(tmp_path: Path) -> None:
    """相同请求的兼容 Raw 当前头始终复用，不提供强制重抓开关。"""
    request: Mapping[str, JsonValue] = {
        "endpoint": "query_dupont_data",
        "instrument_id": "000001.SZ",
        "report_year": 2025,
        "report_quarter": 4,
    }
    source = _FinancialSource(request, _dupont_row(roe="0.12"))
    pipeline, raw_store, repository, _ = _financial_pipeline(tmp_path, source)
    _publish_dupont(
        raw_store,
        repository,
        request,
        _dupont_row(roe="0.10"),
        datetime(2026, 5, 1, tzinfo=UTC),
    )

    result = pipeline.localize(
        DatasetKind.FINANCIAL_OBSERVATION,
        start=date(2025, 1, 1),
        end=date(2026, 5, 1),
    )

    assert result.fetched == 0
    assert result.skipped == 1
    assert source.fetch_calls == 0


def test_curate_preserves_successive_financial_restatements(
    tmp_path: Path,
) -> None:
    request: Mapping[str, JsonValue] = {
        "endpoint": "query_dupont_data",
        "instrument_id": "000001.SZ",
        "report_year": 2025,
        "report_quarter": 4,
    }
    source = _FinancialSource(request, _dupont_row(roe="0.12"))
    pipeline, raw_store, repository, curated_store = _financial_pipeline(
        tmp_path, source
    )
    _publish_dupont(
        raw_store,
        repository,
        request,
        _dupont_row(roe="0.10"),
        datetime(2026, 5, 1, tzinfo=UTC),
    )
    _publish_dupont(
        raw_store,
        repository,
        request,
        _dupont_row(roe="0.12"),
        datetime(2026, 6, 1, tzinfo=UTC),
    )

    first = pipeline.curate(DatasetKind.FINANCIAL_OBSERVATION)
    current = repository.get_canonical_dataset(DatasetKind.FINANCIAL_OBSERVATION)
    frame = curated_store.read_partition(
        DatasetKind.FINANCIAL_OBSERVATION, current.partitions[0]
    )

    assert first.raw_inputs_read == 2
    assert frame.filter(pl.col("metric") == "dupont_roe").select(
        "revision", "value"
    ).rows() == [(0, 0.1), (1, 0.12)]
    assert frame.filter(pl.col("metric") == "dupont_asset_turn").select(
        "revision", "value"
    ).rows() == [(0, 0.75)]

    second = pipeline.curate(DatasetKind.FINANCIAL_OBSERVATION)

    assert second.rebuilt_partitions == 0
    assert second.raw_inputs_read == 0
