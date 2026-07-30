"""Prove that supplier-specific Raw fields stop at the mapper boundary."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_core.data.contracts import CanonicalBatch, PublishedPartition, RawBatch
from quant_core.data.schemas import CANONICAL_SCHEMAS
from quant_core.domain.enums import DatasetKind
from tests.integration.test_data_pipeline import (
    OfflineBaoStockSource,
    make_pipeline,
)


class FakeTushareSourceClient:
    """Offline source with deliberately TuShare-shaped Raw names and codes."""

    provider = "tushare"

    def login(self) -> None:
        pass

    def close(self) -> None:
        pass

    def fetch_range(self, start: date, end: date) -> Iterable[RawBatch]:
        yield RawBatch(
            provider=self.provider,
            dataset="tushare_bundle",
            request={"from": start.isoformat(), "to": end.isoformat()},
            retrieved_at=datetime(2026, 1, 6, tzinfo=UTC),
            schema=(
                "ts_code",
                "security_name",
                "listed_on",
                "trade_day",
                "px_open",
                "px_high",
                "px_low",
                "px_close",
                "px_preclose",
                "vol_shares",
                "turnover_cny",
            ),
            rows=(
                {
                    "ts_code": "600000.SH",
                    "security_name": "浦发银行",
                    "listed_on": "1999-11-10",
                    "trade_day": "2026-01-05",
                    "px_open": "10.00",
                    "px_high": "10.80",
                    "px_low": "9.90",
                    "px_close": "10.50",
                    "px_preclose": "9.95",
                    "vol_shares": "100",
                    "turnover_cny": "1050.00",
                },
            ),
        )


class FakeTushareCanonicalMapper:
    """Test-only mapper proving the public canonical contract is reusable."""

    def normalize(self, raw_partition: PublishedPartition) -> Iterable[CanonicalBatch]:
        row = pq.read_table(raw_partition.data_path).to_pylist()[0]
        common = {
            "source": "tushare",
            "source_version": raw_partition.content_hash,
            "available_at": raw_partition.retrieved_at,
            "availability_source": "RAW_RETRIEVED_AT",
            "pit_usable": True,
            "ingested_at": raw_partition.retrieved_at,
        }
        instrument = {
            "instrument_id": "SSE:600000",
            "exchange": "SSE",
            "board": "MAIN",
            "name": row["security_name"],
            "instrument_type": "STOCK",
            "listing_status": "LISTED",
            "list_date": date.fromisoformat(row["listed_on"]),
            "delist_date": None,
            **common,
        }
        trade_date = date.fromisoformat(row["trade_day"])
        calendar = {"trade_date": trade_date, "is_trading_day": True, **common}
        bar = {
            "instrument_id": "SSE:600000",
            "trade_date": trade_date,
            "open": float(row["px_open"]),
            "high": float(row["px_high"]),
            "low": float(row["px_low"]),
            "close": float(row["px_close"]),
            "preclose": float(row["px_preclose"]),
            "volume": int(row["vol_shares"]),
            "amount": float(row["turnover_cny"]),
            "adjustment_flag": "3",
            "turnover": 0.42,
            "pct_change": 5.53,
            "pe_ttm": 8.10,
            "pb_mrq": 1.20,
            "ps_ttm": 2.30,
            "pcf_ncf_ttm": 4.50,
            **common,
        }
        status = {
            "instrument_id": "SSE:600000",
            "trade_date": trade_date,
            "is_listed": True,
            "is_suspended": False,
            "is_risk_warning": False,
            "board": "MAIN",
            "price_limit_rule_id": "UNRESOLVED",
            "tradable_reason": "NORMAL",
            **common,
        }
        for dataset, record in (
            (DatasetKind.INSTRUMENT, instrument),
            (DatasetKind.TRADE_CALENDAR, calendar),
            (DatasetKind.DAILY_BAR, bar),
            (DatasetKind.SECURITY_STATUS, status),
        ):
            definition = CANONICAL_SCHEMAS[dataset]
            yield CanonicalBatch(
                dataset=dataset,
                frame=pl.DataFrame([record], schema=definition.columns),
                source_content_hashes=(raw_partition.content_hash,),
            )


def test_fake_tushare_runs_through_same_pipeline_and_canonical_contract(
    tmp_path: Path,
) -> None:
    bao_root = tmp_path / "bao"
    tushare_root = tmp_path / "tushare"
    bao_pipeline, bao_repository = make_pipeline(bao_root, OfflineBaoStockSource())
    bao = bao_pipeline.bootstrap()
    tushare_pipeline, tushare_repository = make_pipeline(
        tushare_root,
        FakeTushareSourceClient(),  # type: ignore[arg-type]
        mapper=FakeTushareCanonicalMapper(),
    )
    tushare = tushare_pipeline.bootstrap()

    for dataset in (
        DatasetKind.INSTRUMENT,
        DatasetKind.TRADE_CALENDAR,
        DatasetKind.DAILY_BAR,
        DatasetKind.SECURITY_STATUS,
    ):
        bao_version = bao_repository.get_dataset_version(
            bao.dataset_versions[dataset.value]
        )
        tushare_version = tushare_repository.get_dataset_version(
            tushare.dataset_versions[dataset.value]
        )
        bao_frame = pl.concat(
            bao_pipeline._curated_store.read_version(bao_version)
        ).drop("source", "source_version", "ingested_at")
        tushare_frame = pl.concat(
            tushare_pipeline._curated_store.read_version(tushare_version)
        ).drop("source", "source_version", "ingested_at")
        assert tushare_frame.equals(bao_frame)

    manifest = json.loads(
        tushare_repository.get_snapshot(tushare.snapshot_id).manifest_path.read_text(
            encoding="utf-8"
        )
    )
    serialized = json.dumps(manifest, sort_keys=True)
    for provider_field in (
        "ts_code",
        "security_name",
        "px_open",
        "code_name",
        "tradestatus",
    ):
        assert provider_field not in serialized
    assert set(manifest["datasets"]) == set(
        json.loads(
            bao_repository.get_snapshot(bao.snapshot_id).manifest_path.read_text(
                encoding="utf-8"
            )
        )["datasets"]
    )
