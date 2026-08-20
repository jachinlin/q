"""定义供应商无关的 Canonical 数据帧 Schema 与确定性键。"""

from dataclasses import dataclass

import polars as pl

from quant_research.domain.enums import DatasetKind

UTC_TIMESTAMP = pl.Datetime("us", "UTC")
type PolarsDataType = pl.DataType | type[pl.DataType]


@dataclass(frozen=True, slots=True)
class CanonicalSchema:
    """描述一个 Canonical 数据集的物理列、主键与排序键。

    入参：
        columns：Canonical Parquet 必须具有的列名及 Polars 数据类型。
        primary_key：用于唯一性校验和去重的业务键列。
        sort_key：发布前用于形成确定性行顺序的列。
    返回值：
        构造并返回 ``CanonicalSchema`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    columns: pl.Schema
    primary_key: tuple[str, ...]
    sort_key: tuple[str, ...]


_AUDIT_COLUMNS: dict[str, PolarsDataType] = {
    "source": pl.String,
    "available_at": UTC_TIMESTAMP,
    "availability_source": pl.String,
    "pit_usable": pl.Boolean,
    "ingested_at": UTC_TIMESTAMP,
}


class _CanonicalSchemaFactory:
    """集中为业务列追加统一的 Canonical 审计列。"""

    @staticmethod
    def columns(**domain_columns: PolarsDataType) -> pl.Schema:
        return pl.Schema({**domain_columns, **_AUDIT_COLUMNS})


CANONICAL_SCHEMAS: dict[DatasetKind, CanonicalSchema] = {
    DatasetKind.INSTRUMENT: CanonicalSchema(
        columns=_CanonicalSchemaFactory.columns(
            instrument_id=pl.String,
            exchange=pl.String,
            board=pl.String,
            name=pl.String,
            instrument_type=pl.String,
            listing_status=pl.String,
            list_date=pl.Date,
            delist_date=pl.Date,
        ),
        primary_key=("instrument_id",),
        sort_key=("instrument_id",),
    ),
    DatasetKind.TRADE_CALENDAR: CanonicalSchema(
        columns=_CanonicalSchemaFactory.columns(
            trade_date=pl.Date, is_trading_day=pl.Boolean
        ),
        primary_key=("trade_date",),
        sort_key=("trade_date",),
    ),
    DatasetKind.DAILY_BAR: CanonicalSchema(
        columns=_CanonicalSchemaFactory.columns(
            instrument_id=pl.String,
            trade_date=pl.Date,
            open=pl.Float64,
            high=pl.Float64,
            low=pl.Float64,
            close=pl.Float64,
            preclose=pl.Float64,
            volume=pl.Int64,
            amount=pl.Float64,
            adjustment_flag=pl.String,
            pct_change=pl.Float64,
        ),
        primary_key=("instrument_id", "trade_date"),
        sort_key=("instrument_id", "trade_date"),
    ),
    DatasetKind.DAILY_BASIC: CanonicalSchema(
        columns=_CanonicalSchemaFactory.columns(
            instrument_id=pl.String,
            trade_date=pl.Date,
            pe_ttm=pl.Float64,
            pb_mrq=pl.Float64,
            ps_ttm=pl.Float64,
            turnover=pl.Float64,
        ),
        primary_key=("instrument_id", "trade_date"),
        sort_key=("instrument_id", "trade_date"),
    ),
    DatasetKind.SECURITY_STATUS: CanonicalSchema(
        columns=_CanonicalSchemaFactory.columns(
            instrument_id=pl.String,
            trade_date=pl.Date,
            is_listed=pl.Boolean,
            is_suspended=pl.Boolean,
            is_st=pl.Boolean,
            board=pl.String,
            price_limit_rule_id=pl.String,
            tradable_reason=pl.String,
        ),
        primary_key=("instrument_id", "trade_date"),
        sort_key=("instrument_id", "trade_date"),
    ),
    DatasetKind.FINANCIAL_OBSERVATION: CanonicalSchema(
        columns=_CanonicalSchemaFactory.columns(
            instrument_id=pl.String,
            report_period=pl.Date,
            metric=pl.String,
            value=pl.Float64,
            revision=pl.Int64,
            announced_at=UTC_TIMESTAMP,
        ),
        primary_key=("instrument_id", "report_period", "metric", "revision"),
        sort_key=("instrument_id", "report_period", "metric", "revision"),
    ),
    DatasetKind.INDUSTRY_CLASSIFICATION: CanonicalSchema(
        columns=_CanonicalSchemaFactory.columns(
            as_of_date=pl.Date,
            supplier_update_date=pl.Date,
            instrument_id=pl.String,
            taxonomy=pl.String,
            industry_code=pl.String,
            industry_name=pl.String,
            is_classified=pl.Boolean,
        ),
        primary_key=("as_of_date", "instrument_id", "taxonomy"),
        sort_key=("as_of_date", "instrument_id", "taxonomy"),
    ),
    DatasetKind.INDEX_BAR: CanonicalSchema(
        columns=_CanonicalSchemaFactory.columns(
            index_id=pl.String,
            trade_date=pl.Date,
            open=pl.Float64,
            high=pl.Float64,
            low=pl.Float64,
            close=pl.Float64,
            preclose=pl.Float64,
            volume=pl.Int64,
            amount=pl.Float64,
            pct_change=pl.Float64,
        ),
        primary_key=("index_id", "trade_date"),
        sort_key=("index_id", "trade_date"),
    ),
}
