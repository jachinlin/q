"""把 Tushare Raw 端点一对一规范化为 Canonical 数据集。"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, time
from pathlib import Path
from typing import cast

import numpy as np
import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]

from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS, UTC_TIMESTAMP
from quant_research.data.contracts import CanonicalBatch, JsonValue, PublishedPartition
from quant_research.data.storage.partitions import RawPartitionStore
from quant_research.domain.enums import DatasetKind
from quant_research.infrastructure.tushare.client import _FIELDS

_ENDPOINT_DATASET: Mapping[str, DatasetKind] = {
    "stock_basic": DatasetKind.STOCK_MASTER,
    "fund_basic": DatasetKind.FUND_MASTER,
    "index_basic": DatasetKind.INDEX_MASTER,
    "trade_cal": DatasetKind.TRADE_CALENDAR,
    "daily": DatasetKind.STOCK_DAILY_BAR,
    "adj_factor": DatasetKind.STOCK_ADJUSTMENT_FACTOR,
    "fund_daily": DatasetKind.FUND_DAILY_BAR,
    "fund_adj": DatasetKind.FUND_ADJUSTMENT_FACTOR,
    "index_daily": DatasetKind.INDEX_DAILY_BAR,
    "daily_basic": DatasetKind.STOCK_DAILY_BASIC,
    "suspend_d": DatasetKind.STOCK_SUSPENSION,
    "stock_st": DatasetKind.STOCK_RISK_WARNING,
    "fina_indicator_vip": DatasetKind.STOCK_FINANCIAL_INDICATOR,
    "income_vip": DatasetKind.STOCK_INCOME_STATEMENT,
    "balancesheet_vip": DatasetKind.STOCK_BALANCE_SHEET,
    "cashflow_vip": DatasetKind.STOCK_CASH_FLOW_STATEMENT,
    "dividend": DatasetKind.STOCK_DIVIDEND,
    "fund_div": DatasetKind.FUND_DIVIDEND,
    "index_classify": DatasetKind.INDUSTRY_CATALOG,
    "index_member_all": DatasetKind.INDUSTRY_MEMBERSHIP,
}
_RENAMES: Mapping[str, Mapping[str, str]] = {
    "stock_basic": {"ts_code": "instrument_id"},
    "fund_basic": {"ts_code": "instrument_id"},
    "index_basic": {"ts_code": "index_id", "desc": "description"},
    "trade_cal": {
        "cal_date": "trade_date",
        "is_open": "is_trading_day",
        "pretrade_date": "previous_trade_date",
    },
    "daily": {
        "ts_code": "instrument_id",
        "pre_close": "preclose",
        "pct_chg": "pct_change",
        "vol": "volume",
        "ah_vol": "after_hours_volume",
        "ah_amount": "after_hours_amount",
    },
    "adj_factor": {"ts_code": "instrument_id", "adj_factor": "adjustment_factor"},
    "fund_daily": {
        "ts_code": "instrument_id",
        "pre_close": "preclose",
        "pct_chg": "pct_change",
        "vol": "volume",
    },
    "fund_adj": {"ts_code": "instrument_id", "adj_factor": "adjustment_factor"},
    "index_daily": {
        "ts_code": "index_id",
        "pre_close": "preclose",
        "pct_chg": "pct_change",
        "vol": "volume",
    },
    "daily_basic": {
        "ts_code": "instrument_id",
        "turnover_rate_f": "turnover_rate_free_float",
        "dv_ratio": "dividend_yield",
        "dv_ttm": "dividend_yield_ttm",
        "total_mv": "total_market_value",
        "circ_mv": "circulating_market_value",
    },
    "suspend_d": {"ts_code": "instrument_id"},
    "stock_st": {
        "ts_code": "instrument_id",
        "type": "risk_type",
        "type_name": "risk_type_name",
    },
    "fina_indicator_vip": {
        "ts_code": "instrument_id",
        "ann_date": "announcement_date",
        "end_date": "report_period",
    },
    "income_vip": {
        "ts_code": "instrument_id",
        "ann_date": "announcement_date",
        "f_ann_date": "actual_announcement_date",
        "end_date": "report_period",
        "comp_type": "company_type",
        "end_type": "report_period_type",
    },
    "balancesheet_vip": {
        "ts_code": "instrument_id",
        "ann_date": "announcement_date",
        "f_ann_date": "actual_announcement_date",
        "end_date": "report_period",
        "comp_type": "company_type",
        "end_type": "report_period_type",
    },
    "cashflow_vip": {
        "ts_code": "instrument_id",
        "ann_date": "announcement_date",
        "f_ann_date": "actual_announcement_date",
        "end_date": "report_period",
        "comp_type": "company_type",
        "end_type": "report_period_type",
    },
    "dividend": {
        "ts_code": "instrument_id",
        "end_date": "report_period",
        "ann_date": "announcement_date",
        "div_proc": "status",
        "stk_div": "stock_dividend_per_share",
        "stk_bo_rate": "stock_bonus_rate_per_share",
        "stk_co_rate": "stock_conversion_rate_per_share",
        "cash_div": "cash_dividend_after_tax_per_share",
        "cash_div_tax": "cash_dividend_before_tax_per_share",
        "div_listdate": "stock_listing_date",
        "imp_ann_date": "implementation_announcement_date",
        "base_share": "base_share_count",
    },
    "fund_div": {
        "ts_code": "instrument_id",
        "ann_date": "announcement_date",
        "imp_anndate": "implementation_announcement_date",
        "div_proc": "status",
        "earpay_date": "earnings_payment_date",
        "net_ex_date": "net_value_ex_date",
        "div_cash": "cash_dividend_per_unit",
        "base_unit": "base_unit_count",
        "ear_distr": "distributable_income",
        "ear_amount": "distribution_amount",
        "account_date": "reinvestment_credit_date",
    },
    "index_classify": {
        "index_code": "industry_index_id",
        "is_pub": "is_published",
        "src": "taxonomy",
    },
    "index_member_all": {
        "l1_code": "level1_code",
        "l1_name": "level1_name",
        "l2_code": "level2_code",
        "l2_name": "level2_name",
        "l3_code": "level3_code",
        "l3_name": "level3_name",
        "ts_code": "instrument_id",
        "name": "instrument_name",
        "is_new": "is_current",
    },
}
_DATE_FIELDS = frozenset(
    {
        "list_date",
        "delist_date",
        "found_date",
        "due_date",
        "issue_date",
        "purc_startdate",
        "redm_startdate",
        "base_date",
        "exp_date",
        "trade_date",
        "previous_trade_date",
        "announcement_date",
        "report_period",
        "in_date",
        "out_date",
        "actual_announcement_date",
        "record_date",
        "ex_date",
        "pay_date",
        "stock_listing_date",
        "implementation_announcement_date",
        "earnings_payment_date",
        "net_value_ex_date",
        "reinvestment_credit_date",
    }
)

_STATEMENT_DATASETS = frozenset(
    {
        DatasetKind.STOCK_INCOME_STATEMENT,
        DatasetKind.STOCK_BALANCE_SHEET,
        DatasetKind.STOCK_CASH_FLOW_STATEMENT,
    }
)
_DIVIDEND_DATASETS = frozenset({DatasetKind.STOCK_DIVIDEND, DatasetKind.FUND_DIVIDEND})
_REVISION_DATASETS = frozenset(
    {DatasetKind.STOCK_FINANCIAL_INDICATOR, *_STATEMENT_DATASETS, *_DIVIDEND_DATASETS}
)
_AUDIT_FIELDS = frozenset(
    {"source", "available_at", "availability_source", "pit_usable", "ingested_at"}
)
_PERCENT_FIELDS = frozenset(
    {
        "pct_change",
        "turnover_rate",
        "turnover_rate_free_float",
        "dividend_yield",
        "dividend_yield_ttm",
        "m_fee",
        "c_fee",
        "exp_return",
    }
)


class TushareMapper:
    """映射 Raw。入参：Tushare 分区。返回值：Canonical 批次。异常：Schema 非法时抛出。"""

    def accepts_raw_schema(self, endpoint: str, schema_fingerprint: str) -> bool:
        """判断 Schema。入参：端点和指纹。返回值：是否接受。异常：无。"""
        fields = _FIELDS.get(endpoint)
        return fields is not None and schema_fingerprint == (
            RawPartitionStore.schema_fingerprint_for_fields(fields)
        )

    def normalize(
        self,
        raw_partition: PublishedPartition,
        raw_table: pa.Table,
    ) -> tuple[CanonicalBatch, ...]:
        """把一张已校验 Tushare Raw 表规范化为 Canonical 批次。

        入参：
            raw_partition：提供请求、采集时点和内容身份的不可变 Raw 分区。
            raw_table：存储层完成内容、Schema 和行数校验后的 Arrow 表。
        返回值：
            返回与端点一一对应且符合 Canonical Schema 的单元素批次元组。
        异常：
            ValueError：端点、证券标识、日期或供应商字段值不符合契约时抛出。
        """
        try:
            dataset = _ENDPOINT_DATASET[raw_partition.endpoint]
        except KeyError as error:
            raise ValueError(
                f"unsupported Tushare endpoint: {raw_partition.endpoint}"
            ) from error
        raw_frame = cast(pl.DataFrame, pl.from_arrow(raw_table, rechunk=False))
        if dataset is DatasetKind.FUND_DIVIDEND and not raw_frame.is_empty():
            raw_frame = raw_frame.filter(
                pl.col("ts_code").str.contains(r"\.(?:SH|SZ|BJ)$")
            )
        frame = self._normalize_frame(raw_partition, dataset, raw_frame)
        if dataset in _REVISION_DATASETS:
            invalid = frame.filter(
                ~pl.col("instrument_id").str.contains(r"\.(?:SH|SZ|BJ)$")
            )
            if invalid.height:
                raise ValueError(
                    f"Tushare {raw_partition.endpoint} returned a non-tradable "
                    "instrument_id"
                )
            normalized = self._deduplicate_and_assign(
                dataset,
                cast(list[dict[str, object | None]], frame.to_dicts()),
            )
            frame = pl.DataFrame(
                normalized,
                schema=CANONICAL_SCHEMAS[dataset].columns,
                strict=False,
            )
        return (CanonicalBatch(dataset, frame, (raw_partition.content_hash,)),)

    def candidate_partition_keys_many(
        self,
        dataset: DatasetKind,
        raw_partitions: Sequence[PublishedPartition],
    ) -> tuple[tuple[str, ...], ...]:
        """批量推导每个 Tushare Raw 对应的候选 Canonical 分区键。

        入参：
            dataset：目标 Canonical 数据集。
            raw_partitions：按稳定顺序排列的不可变 Raw 分区。
        返回值：
            返回与输入一一对应的候选分区键元组。
        异常：
            ValueError：请求日期或批量投影得到的公告年不符合分区契约时抛出。
        """
        if not raw_partitions:
            return ()
        dividend_years = (
            self._dividend_partition_years(raw_partitions)
            if dataset in _DIVIDEND_DATASETS
            else {}
        )
        return tuple(
            self._candidate_partition_keys(
                dataset,
                raw_partition,
                dividend_years.get(str(raw_partition.data_path)),
            )
            for raw_partition in raw_partitions
        )

    def _candidate_partition_keys(
        self,
        dataset: DatasetKind,
        raw_partition: PublishedPartition,
        dividend_years: tuple[str, ...] | None,
    ) -> tuple[str, ...]:
        if dataset is DatasetKind.TRADE_CALENDAR:
            return ("all",)
        schema = CANONICAL_SCHEMAS[dataset]
        if "report_period" in schema.columns:
            value = raw_partition.request.get("period")
            if value:
                return (f"report_year={str(value)[:4]}",)
        if dataset in _DIVIDEND_DATASETS:
            if dividend_years:
                return tuple(
                    f"announcement_year={year}" for year in dividend_years
                )
            request_date = next(
                (
                    str(raw_partition.request[field])
                    for field in ("ann_date", "imp_ann_date", "ex_date", "pay_date")
                    if raw_partition.request.get(field)
                ),
                "0000",
            )
            return (f"announcement_year={request_date[:4]}",)
        if "trade_date" not in schema.columns:
            return ("all",)
        direct = raw_partition.request.get("trade_date")
        if direct:
            return (f"year={str(direct)[:4]}",)
        start = str(raw_partition.request.get("start_date") or "")
        end = str(raw_partition.request.get("end_date") or "")
        if len(start) >= 4 and len(end) >= 4:
            return tuple(
                f"year={year}" for year in range(int(start[:4]), int(end[:4]) + 1)
            )
        return ("all",)

    @staticmethod
    def _dividend_partition_years(
        raw_partitions: Sequence[PublishedPartition],
    ) -> dict[str, tuple[str, ...]]:
        paths = [str(partition.data_path) for partition in raw_partitions]
        years = (
            pl.scan_parquet(
                paths,
                glob=False,
                include_file_paths="__raw_path",
            )
            .select(
                pl.col("__raw_path"),
                pl.col("ann_date").str.slice(0, 4).alias("__year"),
            )
            .filter(pl.col("__year").str.contains(r"^\d{4}$"))
            .group_by("__raw_path")
            .agg(pl.col("__year").unique().sort())
            .collect()
        )
        return {
            str(row["__raw_path"]): tuple(cast(list[str], row["__year"]))
            for row in years.iter_rows(named=True)
        }

    def raw_head_is_usable(
        self,
        dataset: DatasetKind,
        request: Mapping[str, JsonValue],
        observed_at: datetime,
    ) -> bool:
        """判断当前头。入参：数据集、请求和时点。返回值：是否可用。异常：无。"""
        del observed_at
        endpoint = str(request.get("endpoint") or "")
        return _ENDPOINT_DATASET.get(endpoint) is dataset

    def requires_raw_history(self, dataset: DatasetKind) -> bool:
        """声明历史需求。入参：数据集。返回值：修订型数据集为真。异常：无。"""
        return dataset in _REVISION_DATASETS

    def consolidate_partition(
        self, dataset: DatasetKind, frames: Sequence[pl.DataFrame]
    ) -> pl.DataFrame:
        """合并切片。入参：数据集和帧。返回值：规范帧。异常：主键冲突时抛出。"""
        schema = CANONICAL_SCHEMAS[dataset]
        if not frames:
            return pl.DataFrame(schema=schema.columns)
        frame = pl.concat(frames, how="vertical_relaxed")
        if dataset in _REVISION_DATASETS:
            rows = cast(list[dict[str, object | None]], frame.to_dicts())
            rows = self._deduplicate_and_assign(dataset, rows)
            frame = pl.DataFrame(rows, schema=schema.columns, strict=False)
        return frame.unique(subset=schema.primary_key, keep="last").sort(
            schema.sort_key
        )

    def transform_hash(self, dataset: DatasetKind) -> str:
        """返回转换身份。入参：数据集。返回值：哈希。异常：读取源码失败时传播。"""
        source = Path(__file__).read_bytes()
        schema = repr(CANONICAL_SCHEMAS[dataset]).encode("utf-8")
        return hashlib.sha256(source + b"\0" + schema).hexdigest()

    def _normalize_frame(
        self,
        partition: PublishedPartition,
        dataset: DatasetKind,
        raw_frame: pl.DataFrame,
    ) -> pl.DataFrame:
        schema = CANONICAL_SCHEMAS[dataset]
        renames = {
            source: target
            for source, target in _RENAMES.get(partition.endpoint, {}).items()
            if source in raw_frame.columns and source != target
        }
        renamed = raw_frame.rename(renames)
        expressions = [
            self._conversion_expression(
                dataset,
                field,
                dtype,
                pl.col(field)
                if field in renamed.columns
                else pl.lit(None, dtype=pl.String),
            ).alias(field)
            for field, dtype in schema.columns.items()
            if field not in _AUDIT_FIELDS
        ]
        frame = renamed.select(expressions)
        retrieved = partition.retrieved_at.astimezone(UTC)
        retrieved_expression = pl.lit(retrieved, dtype=UTC_TIMESTAMP)
        if dataset is DatasetKind.STOCK_MASTER:
            frame = frame.with_columns(
                pl.when(pl.col("instrument_id").str.ends_with(".BJ"))
                .then(pl.lit("BSE"))
                .when(pl.col("market") == "创业板")
                .then(pl.lit("CHINEXT"))
                .when(pl.col("market") == "科创板")
                .then(pl.lit("STAR"))
                .otherwise(pl.lit("MAIN"))
                .alias("board")
            )
        if dataset is DatasetKind.INDUSTRY_MEMBERSHIP:
            frame = frame.with_columns(
                retrieved_expression.alias("in_available_at"),
                pl.when(pl.col("out_date").is_not_null())
                .then(retrieved_expression)
                .otherwise(pl.lit(None, dtype=UTC_TIMESTAMP))
                .alias("out_available_at"),
            )
        available_at, availability_source = self._availability_expressions(
            dataset,
            retrieved_expression,
        )
        frame = frame.with_columns(
            pl.lit("tushare").alias("source"),
            available_at.alias("available_at"),
            availability_source.alias("availability_source"),
            available_at.is_not_null().alias("pit_usable"),
            retrieved_expression.alias("ingested_at"),
        )
        return frame.select(
            pl.col(field).cast(dtype, strict=True).alias(field)
            for field, dtype in schema.columns.items()
        )

    def _conversion_expression(
        self,
        dataset: DatasetKind,
        field: str,
        dtype: pl.DataType,
        value: pl.Expr,
    ) -> pl.Expr:
        text = value.cast(pl.String).str.strip_chars()
        cleaned = pl.when(
            text.is_null() | text.is_in(["", "None", "nan", "NaN"])
        ).then(pl.lit(None, dtype=pl.String)).otherwise(text)
        if field in _DATE_FIELDS:
            return cleaned.str.replace_all("-", "").str.strptime(
                pl.Date,
                format="%Y%m%d",
                strict=True,
            )
        if dtype == pl.String:
            return cleaned
        if dtype == pl.Boolean:
            return cleaned.is_in(["1", "Y", "true", "True"])
        if dtype == pl.Int64:
            number = cleaned.cast(pl.Float64, strict=True)
            if field in {"volume", "after_hours_volume"}:
                number *= 100.0
            return number.round(0).cast(pl.Int64, strict=True)
        if dtype == pl.Float64:
            number = cleaned.cast(pl.Float64, strict=True)
            if field in _PERCENT_FIELDS or (
                dataset is DatasetKind.STOCK_FINANCIAL_INDICATOR
                and self._financial_percent_field(field)
            ):
                number = number.map_batches(
                    self._divide_percent_batch,
                    return_dtype=pl.Float64,
                )
            if field in {"amount", "after_hours_amount"}:
                number *= 1000.0
            if field in {
                "total_share",
                "float_share",
                "free_share",
                "total_market_value",
                "circulating_market_value",
                "base_share_count",
                "base_unit_count",
            }:
                number *= 10_000.0
            return number
        if dtype == UTC_TIMESTAMP:
            return pl.lit(None, dtype=UTC_TIMESTAMP)
        raise TypeError(f"unsupported Canonical type for {field}: {dtype}")

    @staticmethod
    def _divide_percent_batch(series: pl.Series) -> pl.Series:
        missing = series.is_null()
        values = np.divide(series.fill_null(0.0).to_numpy(), 100.0)
        return pl.Series(values).set(missing, None)

    @staticmethod
    def _availability_expressions(
        dataset: DatasetKind,
        retrieved: pl.Expr,
    ) -> tuple[pl.Expr, pl.Expr]:
        if dataset is DatasetKind.STOCK_RISK_WARNING:
            return (
                TushareMapper._market_time_expression("trade_date", time(9, 20)),
                pl.lit("tushare_documented_0920"),
            )
        if dataset in {
            DatasetKind.STOCK_DAILY_BAR,
            DatasetKind.STOCK_ADJUSTMENT_FACTOR,
            DatasetKind.FUND_DAILY_BAR,
            DatasetKind.FUND_ADJUSTMENT_FACTOR,
            DatasetKind.INDEX_DAILY_BAR,
            DatasetKind.STOCK_DAILY_BASIC,
            DatasetKind.STOCK_SUSPENSION,
        }:
            return (
                TushareMapper._market_time_expression("trade_date", time(18, 0)),
                pl.lit("reconstructed_market_eod"),
            )
        if dataset in _STATEMENT_DATASETS:
            actual = pl.col("actual_announcement_date")
            announced = pl.col("announcement_date")
            return (
                actual.fill_null(announced)
                .dt.combine(time(18, 0))
                .dt.replace_time_zone("Asia/Shanghai")
                .dt.convert_time_zone("UTC"),
                pl.when(actual.is_not_null())
                .then(pl.lit("actual_announcement_date_eod"))
                .otherwise(pl.lit("announcement_date_eod")),
            )
        if dataset is DatasetKind.STOCK_FINANCIAL_INDICATOR:
            return (
                TushareMapper._market_time_expression(
                    "announcement_date", time(18, 0)
                ),
                pl.lit("announcement_date_eod"),
            )
        if dataset in _DIVIDEND_DATASETS:
            implemented = pl.col("implementation_announcement_date")
            announced = pl.col("announcement_date")
            return (
                implemented.fill_null(announced)
                .dt.combine(time(18, 0))
                .dt.replace_time_zone("Asia/Shanghai")
                .dt.convert_time_zone("UTC"),
                pl.when(implemented.is_not_null())
                .then(pl.lit("implementation_announcement_date_eod"))
                .otherwise(pl.lit("announcement_date_eod")),
            )
        if dataset is DatasetKind.INDUSTRY_MEMBERSHIP:
            return pl.col("in_available_at"), pl.lit(
                "retrieved_at_no_supplier_announcement"
            )
        return retrieved, pl.lit("retrieved_at")

    @staticmethod
    def _market_time_expression(field: str, value: time) -> pl.Expr:
        return (
            pl.col(field)
            .dt.combine(value)
            .dt.replace_time_zone("Asia/Shanghai")
            .dt.convert_time_zone("UTC")
        )

    @staticmethod
    def _financial_percent_field(field: str) -> bool:
        return (
            field.startswith(("roe", "roa", "q_roe", "q_dt_roe", "q_npta"))
            or field.endswith(("_yoy", "_qoq", "_margin"))
            or "_to_" in field
            or field
            in {"npta", "roic", "gross_margin", "cogs_of_sales", "expense_of_sales"}
        )

    @classmethod
    def _deduplicate_and_assign(
        cls,
        dataset: DatasetKind,
        rows: list[dict[str, object | None]],
    ) -> list[dict[str, object | None]]:
        unique: dict[str, dict[str, object | None]] = {}
        for row in rows:
            content = {
                key: value
                for key, value in row.items()
                if key not in _AUDIT_FIELDS and key != "revision"
            }
            identity = json.dumps(content, default=str, sort_keys=True)
            unique[identity] = row
        deduplicated = list(unique.values())
        cls._assign_revisions(dataset, deduplicated)
        return deduplicated

    @classmethod
    def _assign_revisions(
        cls,
        dataset: DatasetKind,
        rows: list[dict[str, object | None]],
    ) -> None:
        rows.sort(
            key=lambda item: (
                cls._revision_key(dataset, item),
                str(item.get("available_at") or ""),
                str(item.get("announcement_date") or ""),
                str(item.get("ingested_at") or ""),
                json.dumps(
                    {
                        key: value
                        for key, value in item.items()
                        if key not in _AUDIT_FIELDS and key != "revision"
                    },
                    default=str,
                    sort_keys=True,
                ),
            )
        )
        revisions: defaultdict[tuple[str, ...], int] = defaultdict(int)
        for row in rows:
            key = cls._revision_key(dataset, row)
            row["revision"] = revisions[key]
            revisions[key] += 1

    @staticmethod
    def _revision_key(
        dataset: DatasetKind,
        row: Mapping[str, object | None],
    ) -> tuple[str, ...]:
        instrument = str(row.get("instrument_id") or "")
        if dataset in _STATEMENT_DATASETS:
            return (
                instrument,
                str(row.get("report_period") or ""),
                str(row.get("report_type") or ""),
            )
        if dataset is DatasetKind.STOCK_FINANCIAL_INDICATOR:
            return (instrument, str(row.get("report_period") or ""))
        if dataset is DatasetKind.STOCK_DIVIDEND:
            return (
                instrument,
                str(row.get("report_period") or ""),
                str(row.get("announcement_date") or ""),
            )
        if dataset is DatasetKind.FUND_DIVIDEND:
            return (
                instrument,
                str(row.get("announcement_date") or ""),
                str(row.get("base_date") or ""),
            )
        raise ValueError(f"dataset {dataset.value} does not support revisions")
