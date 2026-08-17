"""将已发布的 BaoStock Raw 分区纯映射为 Canonical 数据帧。"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Never, cast
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from quant_research.data.contracts import (
    CanonicalBatch,
    JsonValue,
    PublishedPartition,
    canonical_json_bytes,
)
from quant_research.data.partitions import RawPartitionStore
from quant_research.data.schemas import CANONICAL_SCHEMAS
from quant_research.domain.enums import DatasetKind, Exchange, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.infrastructure.baostock.client import (
    DAILY_BAR_FIELDS,
    DUPONT_FIELDS,
    INDEX_BAR_FIELDS,
    INDUSTRY_FIELDS,
    from_baostock_code,
)

INSTRUMENT_FIELDS = ("code", "code_name", "ipoDate", "outDate", "type", "status")
TRADE_CALENDAR_FIELDS = ("calendar_date", "is_trading_day")

BAOSTOCK_RAW_SCHEMAS: dict[str, tuple[str, ...]] = {
    "query_stock_basic": INSTRUMENT_FIELDS,
    "query_trade_dates": TRADE_CALENDAR_FIELDS,
    "query_daily_history_k_AStock": DAILY_BAR_FIELDS,
    "query_etf_history_k_data_plus": DAILY_BAR_FIELDS,
    "query_history_k_data_plus": INDEX_BAR_FIELDS,
    "query_dupont_data": DUPONT_FIELDS,
    "query_stock_industry": INDUSTRY_FIELDS,
}

_INTEGER = re.compile(r"[+-]?[0-9]+\Z")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_DAILY_MARKET_CLOSE = time(15, 0)
_TYPE_NAMES = {
    "1": "STOCK",
    "2": "INDEX",
    "4": "CONVERTIBLE_BOND",
    "5": "ETF",
}
_FINANCIAL_METRICS: Mapping[str, Mapping[str, str]] = {
    "query_dupont_data": {
        "dupontROE": "dupont_roe",
        "dupontAssetStoEquity": "dupont_assets_to_equity",
        "dupontAssetTurn": "dupont_asset_turn",
        "dupontPnitoni": "dupont_pnitoni",
        "dupontNitogr": "dupont_nitogr",
        "dupontTaxBurden": "dupont_tax_burden",
        "dupontIntburden": "dupont_interest_burden",
        "dupontEbittogr": "dupont_ebit_to_gr",
    },
}

type RawRow = Mapping[str, Any]
type MapperResult = tuple[DatasetKind, list[dict[str, object | None]]]


class BaoStockMapper:
    """在无供应商会话的情况下规范化不可变 BaoStock 证据。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``BaoStockMapper`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    @staticmethod
    def accepts_raw_schema(endpoint: str, schema_fingerprint: str) -> bool:
        """判断 Raw 元数据是否符合当前端点契约。

        入参：
            endpoint：供应商原生端点名称。
            schema_fingerprint：BaoStock 响应字段名称和类型形成的确定性身份。
        返回值：
            当前 mapper 支持该端点及 Raw Schema 时返回 ``True``，否则返回 ``False``。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        fields = BAOSTOCK_RAW_SCHEMAS.get(endpoint)
        return fields is not None and schema_fingerprint == (
            RawPartitionStore.schema_fingerprint_for_fields(fields)
        )

    def normalize(self, raw_partition: PublishedPartition) -> Iterable[CanonicalBatch]:
        """读取并校验已发布 Raw 分区，再生成 Canonical 批次。

        入参：
            raw_partition：已发布且不可变的 Raw 分区。
        返回值：
            返回规范化基础设施后的``normalize``（``Iterable[CanonicalBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        expected_fields = BAOSTOCK_RAW_SCHEMAS.get(raw_partition.endpoint, ())
        if raw_partition.source != "baostock" or not expected_fields:
            _BaoStockMappingSupport._raise_mismatch(
                raw_partition,
                expected_fields,
                (),
                "unsupported BaoStock raw partition",
            )
        table = _BaoStockMappingSupport._validated_table(raw_partition, expected_fields)
        rows = table.to_pylist()
        mapper = _BaoStockMappingSupport._mapper_for(raw_partition.endpoint)
        for dataset, records in mapper(raw_partition, rows):
            yield _BaoStockMappingSupport._canonical_batch(
                raw_partition, dataset, records
            )

    @staticmethod
    def candidate_partition_keys(
        dataset: DatasetKind, raw_partition: PublishedPartition
    ) -> tuple[str, ...]:
        """在不读取 Raw 文件的情况下推导候选 Canonical 分区键。

        入参：
            dataset：目标 Canonical 数据集标识。
            raw_partition：已发布且不可变的 Raw 分区。
        返回值：
            返回分区``keys``（``tuple[str, ...]``）。
        异常：
            ValueError：输入、供应商响应、目录状态或文件完整性不满足契约时抛出。
        """
        request = raw_partition.request
        if (
            dataset in {DatasetKind.DAILY_BAR, DatasetKind.SECURITY_STATUS}
            and raw_partition.endpoint == "query_etf_history_k_data_plus"
        ):
            start = date.fromisoformat(str(request["start_date"]))
            end = date.fromisoformat(str(request["end_date"]))
            if start > end:
                raise ValueError("ETF Raw request start_date follows end_date")
            return tuple(f"year={year}" for year in range(start.year, end.year + 1))
        if dataset in {
            DatasetKind.DAILY_BAR,
            DatasetKind.DAILY_BASIC,
            DatasetKind.SECURITY_STATUS,
        }:
            return (f"year={date.fromisoformat(str(request['date'])).year}",)
        if dataset is DatasetKind.INDEX_BAR:
            if "date" in request:
                return (f"year={date.fromisoformat(str(request['date'])).year}",)
            start = date.fromisoformat(str(request["start_date"]))
            end = date.fromisoformat(str(request["end_date"]))
            if start > end:
                raise ValueError("index Raw request start_date follows end_date")
            return tuple(f"year={year}" for year in range(start.year, end.year + 1))
        if dataset is DatasetKind.FINANCIAL_OBSERVATION:
            return (f"report_year={int(str(request['report_year']))}",)
        if dataset is DatasetKind.INDUSTRY_CLASSIFICATION:
            as_of = date.fromisoformat(str(request.get("as_of", request["date"])))
            return (f"year={as_of.year}",)
        return ("all",)

    @staticmethod
    def raw_head_is_usable(
        dataset: DatasetKind,
        request: Mapping[str, JsonValue],
        observed_at: datetime,
    ) -> bool:
        """判断 Raw 当前头在本次 Curate 时点是否已经完整可用。

        入参：目标数据集、规范化 Raw 请求和本次 Curate 观察时点。
        返回值：普通数据集恒为 ``True``；行业快照日期完整结束后为 ``True``。
        异常：行业请求缺少合法时点时传播 ``TypeError`` 或 ``ValueError``。
        """
        if dataset is not DatasetKind.INDUSTRY_CLASSIFICATION:
            return True
        value = request.get("as_of", request.get("date"))
        if not isinstance(value, str):
            raise TypeError("industry Raw request requires an as_of date")
        as_of_date = date.fromisoformat(value)
        local_now = observed_at.astimezone(_SHANGHAI)
        latest_possible_date = local_now.date()
        if local_now.hour < 18:
            latest_possible_date -= timedelta(days=1)
        return as_of_date <= latest_possible_date

    @staticmethod
    def requires_raw_history(dataset: DatasetKind) -> bool:
        """判断重建分区时是否需要读取同一请求的历史 Raw 对象。

        入参：目标数据集。返回值：财务观测返回 ``True``，其余返回 ``False``。异常：无。
        """
        return dataset is DatasetKind.FINANCIAL_OBSERVATION

    @staticmethod
    def consolidate_partition(
        dataset: DatasetKind, frames: Sequence[pl.DataFrame]
    ) -> pl.DataFrame:
        """把映射片段折叠为符合目标 Schema 的最终 Canonical 分区。

        入参：目标数据集与按 Raw 身份稳定排序的映射片段。
        返回值：完成去重、修订编号或行业事件压缩的数据帧。
        异常：输入字段或类型不满足目标契约时传播 Polars 异常。
        """
        definition = CANONICAL_SCHEMAS[dataset]
        if not frames:
            return pl.DataFrame(schema=definition.columns)
        combined = pl.concat(frames, how="vertical")
        if dataset is DatasetKind.FINANCIAL_OBSERVATION:
            return _BaoStockMappingSupport._financial_revision_frame(
                combined, definition.columns
            )
        if dataset is DatasetKind.INDUSTRY_CLASSIFICATION:
            return _BaoStockMappingSupport._industry_event_frame(
                combined, definition.columns
            )
        return (
            combined.unique(
                subset=list(definition.primary_key),
                keep="last",
                maintain_order=True,
            )
            .sort(list(definition.sort_key))
            .cast(definition.columns)
        )

    @staticmethod
    def transform_hash(dataset: DatasetKind) -> str:
        """返回映射代码与目标 Canonical 契约的确定性身份。

        入参：
            dataset：目标 Canonical 数据集标识。
        返回值：
            返回哈希（``str``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        definition = CANONICAL_SCHEMAS[dataset]
        contract = cast(
            JsonValue,
            {
                "dataset": dataset.value,
                "columns": [
                    {"name": name, "type": str(dtype)}
                    for name, dtype in definition.columns.items()
                ],
                "primary_key": list(definition.primary_key),
                "sort_key": list(definition.sort_key),
            },
        )
        digest = hashlib.sha256()
        digest.update(Path(__file__).read_bytes())
        digest.update(canonical_json_bytes(contract))
        return digest.hexdigest()


class _BaoStockMappingSupport:
    """集中承载 BaoStock 映射器内部、无独立公开语义的转换逻辑。"""

    @staticmethod
    def _financial_revision_frame(
        frame: pl.DataFrame, columns: pl.Schema
    ) -> pl.DataFrame:
        """合并相同财务抓取状态并为连续修订编号。"""
        base_key = ["instrument_id", "report_period", "metric"]
        state_columns = [
            "value",
            "announced_at",
            "source",
            "available_at",
            "availability_source",
            "pit_usable",
        ]
        ordered = frame.with_row_index("_input_order").sort(
            [*base_key, "ingested_at", "_input_order"]
        )
        state_changed = pl.any_horizontal(
            pl.col(name).ne_missing(pl.col(name).shift().over(base_key))
            for name in state_columns
        )
        return (
            ordered.filter(state_changed)
            .with_columns(
                (pl.col("ingested_at").cum_count().over(base_key) - 1)
                .cast(pl.Int64)
                .alias("revision")
            )
            .drop("_input_order")
            .sort([*base_key, "revision"])
            .cast(columns)
        )

    @staticmethod
    def _industry_event_frame(frame: pl.DataFrame, columns: pl.Schema) -> pl.DataFrame:
        """将逐交易日行业快照压缩为年度基线与状态变化事件。"""
        ordered = frame.cast(columns).sort(
            ["as_of_date", "instrument_id", "taxonomy", "ingested_at"]
        )
        events: list[dict[str, object | None]] = []
        states: dict[tuple[str, str], tuple[str | None, str | None, bool]] = {}
        active_year: int | None = None
        baseline_date: date | None = None
        for row in ordered.iter_rows(named=True):
            as_of_date = cast(date, row["as_of_date"])
            if active_year != as_of_date.year:
                active_year = as_of_date.year
                baseline_date = as_of_date
                states.clear()
            key = (cast(str, row["instrument_id"]), cast(str, row["taxonomy"]))
            state = (
                cast(str | None, row["industry_code"]),
                cast(str | None, row["industry_name"]),
                cast(bool, row["is_classified"]),
            )
            if as_of_date == baseline_date or states.get(key) != state:
                events.append(row)
            states[key] = state
        if not events:
            return pl.DataFrame(schema=columns)
        return pl.DataFrame(events, schema=columns).sort(
            ["as_of_date", "instrument_id", "taxonomy"]
        )

    @staticmethod
    def _validated_table(
        partition: PublishedPartition,
        expected_fields: tuple[str, ...],
    ) -> pa.Table:
        try:
            RawPartitionStore.verify_partition(partition)
        except (OSError, TypeError, pa.ArrowException, ValueError) as error:
            _BaoStockMappingSupport._raise_mismatch(
                partition,
                expected_fields,
                (),
                "published raw partition failed integrity verification",
                cause=error,
            )
        table = pq.read_table(partition.data_path)
        actual_fields = tuple(table.column_names)
        if actual_fields != expected_fields:
            _BaoStockMappingSupport._raise_mismatch(
                partition,
                expected_fields,
                actual_fields,
                "BaoStock raw fields do not match the declared schema",
            )
        return table

    @staticmethod
    def _canonical_batch(
        partition: PublishedPartition,
        dataset: DatasetKind,
        records: list[dict[str, object | None]],
    ) -> CanonicalBatch:
        definition = CANONICAL_SCHEMAS[dataset]
        try:
            frame = pl.DataFrame(records, schema=definition.columns, strict=True)
        except (TypeError, ValueError, pl.exceptions.PolarsError) as error:
            _BaoStockMappingSupport._raise_mismatch(
                partition,
                BAOSTOCK_RAW_SCHEMAS[partition.endpoint],
                BAOSTOCK_RAW_SCHEMAS[partition.endpoint],
                f"canonical {dataset.value} frame does not match its declared schema",
                cause=error,
            )
        duplicate_count = (
            frame.group_by(list(definition.primary_key))
            .len()
            .filter(pl.col("len") > 1)
            .height
            if frame.height
            else 0
        )
        if duplicate_count:
            _BaoStockMappingSupport._raise_mismatch(
                partition,
                BAOSTOCK_RAW_SCHEMAS[partition.endpoint],
                BAOSTOCK_RAW_SCHEMAS[partition.endpoint],
                f"canonical {dataset.value} primary key is not unique",
                duplicate_primary_key=list(definition.primary_key),
            )
        return CanonicalBatch(
            dataset=dataset,
            frame=frame.sort(list(definition.sort_key), nulls_last=True),
            source_content_hashes=(partition.content_hash,),
        )

    @staticmethod
    def _instrument_rows(
        partition: PublishedPartition, rows: Sequence[RawRow]
    ) -> tuple[MapperResult]:
        records: list[dict[str, object | None]] = []
        for row in rows:
            code, exchange, board = _BaoStockMappingSupport._identity(
                partition, row, "code"
            )
            status = _BaoStockMappingSupport._flag(partition, row, "status")
            records.append(
                {
                    "instrument_id": code,
                    "exchange": exchange.value,
                    "board": board,
                    "name": _BaoStockMappingSupport._required_text(
                        partition, row, "code_name"
                    ),
                    "instrument_type": _TYPE_NAMES.get(
                        _BaoStockMappingSupport._required_text(partition, row, "type"),
                        "UNKNOWN",
                    ),
                    "listing_status": "LISTED" if status else "DELISTED",
                    "list_date": _BaoStockMappingSupport._optional_date(
                        partition, row, "ipoDate"
                    ),
                    "delist_date": _BaoStockMappingSupport._optional_date(
                        partition, row, "outDate"
                    ),
                    **_BaoStockMappingSupport._raw_availability(partition),
                }
            )
        return ((DatasetKind.INSTRUMENT, records),)

    @staticmethod
    def _calendar_rows(
        partition: PublishedPartition, rows: Sequence[RawRow]
    ) -> tuple[MapperResult]:
        records = [
            {
                "trade_date": _BaoStockMappingSupport._required_date(
                    partition, row, "calendar_date"
                ),
                "is_trading_day": _BaoStockMappingSupport._flag(
                    partition, row, "is_trading_day"
                ),
                **_BaoStockMappingSupport._raw_availability(partition),
            }
            for row in rows
        ]
        return ((DatasetKind.TRADE_CALENDAR, records),)

    @staticmethod
    def _daily_rows(
        partition: PublishedPartition, rows: Sequence[RawRow]
    ) -> tuple[MapperResult, MapperResult, MapperResult]:
        bars: list[dict[str, object | None]] = []
        statuses: list[dict[str, object | None]] = []
        valuations: list[dict[str, object | None]] = []
        for row in rows:
            code, _, board = _BaoStockMappingSupport._identity(partition, row, "code")
            trade_date = _BaoStockMappingSupport._required_date(partition, row, "date")
            trade_status = _BaoStockMappingSupport._flag(partition, row, "tradestatus")
            risk_warning = _BaoStockMappingSupport._flag(partition, row, "isST")
            audit = _BaoStockMappingSupport._daily_availability(partition, trade_date)
            bars.append(
                {
                    "instrument_id": code,
                    "trade_date": trade_date,
                    "open": _BaoStockMappingSupport._optional_float(
                        partition, row, "open"
                    ),
                    "high": _BaoStockMappingSupport._optional_float(
                        partition, row, "high"
                    ),
                    "low": _BaoStockMappingSupport._optional_float(
                        partition, row, "low"
                    ),
                    "close": _BaoStockMappingSupport._optional_float(
                        partition, row, "close"
                    ),
                    "preclose": _BaoStockMappingSupport._optional_float(
                        partition, row, "preclose"
                    ),
                    "volume": _BaoStockMappingSupport._optional_int(
                        partition, row, "volume"
                    ),
                    "amount": _BaoStockMappingSupport._optional_float(
                        partition, row, "amount"
                    ),
                    "adjustment_flag": _BaoStockMappingSupport._required_text(
                        partition, row, "adjustflag"
                    ),
                    "pct_change": _BaoStockMappingSupport._optional_float(
                        partition, row, "pctChg"
                    ),
                    **audit,
                }
            )
            valuations.append(
                {
                    "instrument_id": code,
                    "trade_date": trade_date,
                    "pe_ttm": _BaoStockMappingSupport._optional_float(
                        partition, row, "peTTM"
                    ),
                    "pb_mrq": _BaoStockMappingSupport._optional_float(
                        partition, row, "pbMRQ"
                    ),
                    "ps_ttm": _BaoStockMappingSupport._optional_float(
                        partition, row, "psTTM"
                    ),
                    "turnover": _BaoStockMappingSupport._optional_float(
                        partition, row, "turn"
                    ),
                    **audit,
                }
            )
            suspended = not trade_status
            reason = (
                "SUSPENDED"
                if suspended
                else "RISK_WARNING"
                if risk_warning
                else "NORMAL"
            )
            statuses.append(
                {
                    "instrument_id": code,
                    "trade_date": trade_date,
                    "is_listed": True,
                    "is_suspended": suspended,
                    "is_st": risk_warning,
                    "board": board,
                    "price_limit_rule_id": "UNRESOLVED",
                    "tradable_reason": reason,
                    **audit,
                }
            )
        return (
            (DatasetKind.DAILY_BAR, bars),
            (DatasetKind.SECURITY_STATUS, statuses),
            (DatasetKind.DAILY_BASIC, valuations),
        )

    @classmethod
    def _etf_rows(
        cls, partition: PublishedPartition, rows: Sequence[RawRow]
    ) -> tuple[MapperResult, ...]:
        """将独立 ETF Raw 区间证据合并为日线和共享证券状态。"""
        mapped = dict(cls._daily_rows(partition, rows))
        statuses = mapped[DatasetKind.SECURITY_STATUS]
        for status in statuses:
            status["is_st"] = False
            status["tradable_reason"] = (
                "SUSPENDED" if status["is_suspended"] is True else "NORMAL"
            )
        return (
            (DatasetKind.DAILY_BAR, mapped[DatasetKind.DAILY_BAR]),
            (DatasetKind.SECURITY_STATUS, statuses),
        )

    @staticmethod
    def _financial_rows(
        partition: PublishedPartition, rows: Sequence[RawRow]
    ) -> tuple[MapperResult]:
        metrics = _FINANCIAL_METRICS[partition.endpoint]
        revisions: dict[tuple[str, date, str], int] = {}
        records: list[dict[str, object | None]] = []
        for row in rows:
            code, _, _ = _BaoStockMappingSupport._identity(partition, row, "code")
            report_period = _BaoStockMappingSupport._required_date(
                partition, row, "statDate"
            )
            announced_at = _BaoStockMappingSupport._announcement_time(
                partition, row, "pubDate"
            )
            for field, metric in metrics.items():
                raw_value = row.get(field)
                if raw_value is None or raw_value == "":
                    continue
                key = (code, report_period, metric)
                revision = revisions.get(key, 0)
                revisions[key] = revision + 1
                records.append(
                    {
                        "instrument_id": code,
                        "report_period": report_period,
                        "metric": metric,
                        "value": _BaoStockMappingSupport._optional_float(
                            partition, row, field
                        ),
                        "revision": revision,
                        "announced_at": announced_at,
                        **_BaoStockMappingSupport._announcement_availability(
                            partition, announced_at
                        ),
                    }
                )
        return ((DatasetKind.FINANCIAL_OBSERVATION, records),)

    @staticmethod
    def _industry_rows(
        partition: PublishedPartition, rows: Sequence[RawRow]
    ) -> tuple[MapperResult]:
        if not rows:
            _BaoStockMappingSupport._raise_mismatch(
                partition,
                INDUSTRY_FIELDS,
                INDUSTRY_FIELDS,
                "BaoStock industry response must not be empty",
            )
        records: list[dict[str, object | None]] = []
        seen: dict[tuple[str, str], tuple[str | None, str | None, bool]] = {}
        as_of_dates: set[date] = set()
        supplier_update_dates: set[date] = set()
        taxonomies: set[str] = set()
        for row in rows:
            code, _, _ = _BaoStockMappingSupport._identity(partition, row, "code")
            taxonomy = _BaoStockMappingSupport._required_text(
                partition, row, "industryClassification"
            )
            as_of_date = _BaoStockMappingSupport._required_date(
                partition, row, "as_of_date"
            )
            supplier_update_date = _BaoStockMappingSupport._required_date(
                partition, row, "updateDate"
            )
            if supplier_update_date > as_of_date:
                _BaoStockMappingSupport._raise_mismatch(
                    partition,
                    INDUSTRY_FIELDS,
                    INDUSTRY_FIELDS,
                    "BaoStock industry updateDate follows the request as_of_date",
                    updateDate=supplier_update_date.isoformat(),
                    as_of_date=as_of_date.isoformat(),
                )
            industry = _BaoStockMappingSupport._optional_text(
                partition, row, "industry"
            )
            is_classified = industry is not None
            state = (industry, industry, is_classified)
            key = (code, taxonomy)
            previous = seen.get(key)
            if previous is not None:
                if previous != state:
                    _BaoStockMappingSupport._raise_mismatch(
                        partition,
                        INDUSTRY_FIELDS,
                        INDUSTRY_FIELDS,
                        "BaoStock industry response contains conflicting states",
                        instrument_id=code,
                        taxonomy=taxonomy,
                    )
                continue
            seen[key] = state
            as_of_dates.add(as_of_date)
            supplier_update_dates.add(supplier_update_date)
            taxonomies.add(taxonomy)
            records.append(
                {
                    "as_of_date": as_of_date,
                    "supplier_update_date": supplier_update_date,
                    "instrument_id": code,
                    "taxonomy": taxonomy,
                    "industry_code": industry,
                    "industry_name": industry,
                    "is_classified": is_classified,
                    **_BaoStockMappingSupport._reconstructed_classification_availability(
                        partition, as_of_date
                    ),
                }
            )
        if (
            len(as_of_dates) > 1
            or len(supplier_update_dates) > 1
            or len(taxonomies) > 1
        ):
            _BaoStockMappingSupport._raise_mismatch(
                partition,
                INDUSTRY_FIELDS,
                INDUSTRY_FIELDS,
                "BaoStock industry response is not one internally consistent snapshot",
                as_of_dates=sorted(item.isoformat() for item in as_of_dates),
                supplier_update_dates=sorted(
                    item.isoformat() for item in supplier_update_dates
                ),
                taxonomies=sorted(taxonomies),
            )
        return ((DatasetKind.INDUSTRY_CLASSIFICATION, records),)

    @staticmethod
    def _index_rows(
        partition: PublishedPartition, rows: Sequence[RawRow]
    ) -> tuple[MapperResult]:
        records: list[dict[str, object | None]] = []
        for row in rows:
            index_id, _, _ = _BaoStockMappingSupport._identity(partition, row, "code")
            trade_date = _BaoStockMappingSupport._required_date(partition, row, "date")
            records.append(
                {
                    "index_id": index_id,
                    "trade_date": trade_date,
                    "open": _BaoStockMappingSupport._optional_float(
                        partition, row, "open"
                    ),
                    "high": _BaoStockMappingSupport._optional_float(
                        partition, row, "high"
                    ),
                    "low": _BaoStockMappingSupport._optional_float(
                        partition, row, "low"
                    ),
                    "close": _BaoStockMappingSupport._optional_float(
                        partition, row, "close"
                    ),
                    "preclose": _BaoStockMappingSupport._optional_float(
                        partition, row, "preclose"
                    ),
                    "volume": _BaoStockMappingSupport._optional_int(
                        partition, row, "volume"
                    ),
                    "amount": _BaoStockMappingSupport._optional_float(
                        partition, row, "amount"
                    ),
                    "pct_change": _BaoStockMappingSupport._optional_float(
                        partition, row, "pctChg"
                    ),
                    **_BaoStockMappingSupport._daily_availability(
                        partition, trade_date
                    ),
                }
            )
        return ((DatasetKind.INDEX_BAR, records),)

    @classmethod
    def _mapper_for(
        cls, endpoint: str
    ) -> Callable[[PublishedPartition, Sequence[RawRow]], tuple[MapperResult, ...]]:
        return {
            "query_stock_basic": cls._instrument_rows,
            "query_trade_dates": cls._calendar_rows,
            "query_daily_history_k_AStock": cls._daily_rows,
            "query_etf_history_k_data_plus": cls._etf_rows,
            "query_history_k_data_plus": cls._index_rows,
            "query_dupont_data": cls._financial_rows,
            "query_stock_industry": cls._industry_rows,
        }[endpoint]

    @staticmethod
    def _identity(
        partition: PublishedPartition,
        row: RawRow,
        field: str,
    ) -> tuple[str, Exchange, str]:
        raw_code = _BaoStockMappingSupport._required_text(partition, row, field)
        try:
            instrument = from_baostock_code(raw_code)
        except ValueError as error:
            _BaoStockMappingSupport._raise_value(partition, field, raw_code, error)
        board = (
            "STAR"
            if instrument.exchange is Exchange.SSE
            and instrument.symbol.startswith("688")
            else "CHINEXT"
            if instrument.exchange is Exchange.SZSE
            and instrument.symbol.startswith(("300", "301"))
            else "MAIN"
        )
        return instrument.canonical(), instrument.exchange, board

    @staticmethod
    def _raw_availability(partition: PublishedPartition) -> dict[str, object | None]:
        retrieved_at = partition.retrieved_at.astimezone(UTC)
        return {
            "source": partition.source,
            "available_at": retrieved_at,
            "availability_source": "RAW_RETRIEVED_AT",
            "pit_usable": True,
            "ingested_at": retrieved_at,
        }

    @staticmethod
    def _daily_availability(
        partition: PublishedPartition, trade_date: date
    ) -> dict[str, object | None]:
        retrieved_at = partition.retrieved_at.astimezone(UTC)
        market_close = datetime.combine(
            trade_date, _DAILY_MARKET_CLOSE, _SHANGHAI
        ).astimezone(UTC)
        complete = retrieved_at >= market_close
        return {
            "source": partition.source,
            "available_at": market_close if complete else None,
            "availability_source": (
                "MARKET_CLOSE_DERIVED" if complete else "MARKET_SESSION_INCOMPLETE"
            ),
            "pit_usable": complete,
            "ingested_at": retrieved_at,
        }

    @staticmethod
    def _announcement_availability(
        partition: PublishedPartition,
        announced_at: datetime | None,
    ) -> dict[str, object | None]:
        return {
            "source": partition.source,
            "available_at": announced_at,
            "availability_source": (
                "INFERRED_PUBLICATION_DATE"
                if announced_at is not None
                else "UNKNOWN_ANNOUNCEMENT_DATE"
            ),
            "pit_usable": announced_at is not None,
            "ingested_at": partition.retrieved_at.astimezone(UTC),
        }

    @staticmethod
    def _reconstructed_classification_availability(
        partition: PublishedPartition, as_of_date: date
    ) -> dict[str, object | None]:
        """按供应商历史查询日期暴露重建行业快照。"""
        retrieved_at = partition.retrieved_at.astimezone(UTC)
        available_at = datetime.combine(
            as_of_date, time.max, tzinfo=_SHANGHAI
        ).astimezone(UTC)
        return {
            "source": partition.source,
            "available_at": available_at,
            "availability_source": "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED",
            "pit_usable": True,
            "ingested_at": retrieved_at,
        }

    @staticmethod
    def _required_text(partition: PublishedPartition, row: RawRow, field: str) -> str:
        value = row.get(field)
        if not isinstance(value, str) or not value:
            _BaoStockMappingSupport._raise_value(partition, field, value)
        return value

    @staticmethod
    def _flag(partition: PublishedPartition, row: RawRow, field: str) -> bool:
        value = row.get(field)
        if not isinstance(value, str) or value not in ("0", "1"):
            _BaoStockMappingSupport._raise_value(partition, field, value)
        return value == "1"

    @staticmethod
    def _required_date(partition: PublishedPartition, row: RawRow, field: str) -> date:
        value = _BaoStockMappingSupport._required_text(partition, row, field)
        try:
            parsed = date.fromisoformat(value)
        except ValueError as error:
            _BaoStockMappingSupport._raise_value(partition, field, value, error)
        if parsed.isoformat() != value:
            _BaoStockMappingSupport._raise_value(partition, field, value)
        return parsed

    @staticmethod
    def _optional_date(
        partition: PublishedPartition,
        row: RawRow,
        field: str,
    ) -> date | None:
        value = row.get(field)
        if value == "":
            return None
        return _BaoStockMappingSupport._required_date(partition, row, field)

    @staticmethod
    def _optional_text(
        partition: PublishedPartition,
        row: RawRow,
        field: str,
    ) -> str | None:
        value = row.get(field)
        if value == "" or value is None:
            return None
        if not isinstance(value, str):
            _BaoStockMappingSupport._raise_value(partition, field, value)
        return value

    @staticmethod
    def _announcement_time(
        partition: PublishedPartition,
        row: RawRow,
        field: str,
    ) -> datetime | None:
        published_on = _BaoStockMappingSupport._optional_date(partition, row, field)
        if published_on is None:
            return None
        local_end = datetime.combine(published_on, time.max, tzinfo=_SHANGHAI)
        return local_end.astimezone(UTC)

    @staticmethod
    def _optional_float(
        partition: PublishedPartition,
        row: RawRow,
        field: str,
    ) -> float | None:
        value = row.get(field)
        if value == "":
            return None
        if not isinstance(value, str):
            _BaoStockMappingSupport._raise_value(partition, field, value)
        try:
            parsed = float(value)
        except ValueError as error:
            _BaoStockMappingSupport._raise_value(partition, field, value, error)
        if not math.isfinite(parsed):
            _BaoStockMappingSupport._raise_value(partition, field, value)
        return parsed

    @staticmethod
    def _optional_int(
        partition: PublishedPartition,
        row: RawRow,
        field: str,
    ) -> int | None:
        value = row.get(field)
        if value == "":
            return None
        if not isinstance(value, str) or not _INTEGER.fullmatch(value):
            _BaoStockMappingSupport._raise_value(partition, field, value)
        return int(value)

    @staticmethod
    def _raise_value(
        partition: PublishedPartition,
        field: str,
        value: object,
        cause: Exception | None = None,
    ) -> Never:
        _BaoStockMappingSupport._raise_mismatch(
            partition,
            BAOSTOCK_RAW_SCHEMAS[partition.endpoint],
            BAOSTOCK_RAW_SCHEMAS[partition.endpoint],
            f"BaoStock raw field {field!r} contains an invalid value",
            cause=cause,
            field=field,
            value=value,
        )

    @staticmethod
    def _raise_mismatch(
        partition: PublishedPartition,
        expected_fields: Sequence[str],
        actual_fields: Sequence[str],
        message: str,
        *,
        cause: Exception | None = None,
        **extra_context: object,
    ) -> Never:
        expected = list(expected_fields)
        actual = list(actual_fields)
        detail = ErrorDetail(
            code="DATA_SCHEMA_MISMATCH",
            severity=Severity.SEVERE,
            message=message,
            context={
                "data_path": str(partition.data_path),
                "endpoint": partition.endpoint,
                "expected_fields": expected,
                "actual_fields": actual,
                "missing_fields": sorted(set(expected).difference(actual)),
                "extra_fields": sorted(set(actual).difference(expected)),
                **extra_context,
            },
            remediation="inspect the published raw partition and provider schema",
            retryable=False,
        )
        error = QuantError(detail)
        if cause is None:
            raise error
        raise error from cause
