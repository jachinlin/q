"""Vendor-neutral canonical dataframe schemas and key definitions."""

from dataclasses import dataclass

import polars as pl

from quant_core.domain.enums import DatasetKind

UTC_TIMESTAMP = pl.Datetime("us", "UTC")
CANONICAL_SCHEMA_VERSION = "canonical-schema-v1"
type PolarsDataType = pl.DataType | type[pl.DataType]


@dataclass(frozen=True, slots=True)
class CanonicalSchema:
    """One canonical dataset's physical columns and deterministic keys."""

    columns: pl.Schema
    primary_key: tuple[str, ...]
    sort_key: tuple[str, ...]


_AUDIT_COLUMNS: dict[str, PolarsDataType] = {
    "source": pl.String,
    "source_version": pl.String,
    "available_at": UTC_TIMESTAMP,
    "availability_source": pl.String,
    "pit_usable": pl.Boolean,
    "ingested_at": UTC_TIMESTAMP,
}


def _columns(**domain_columns: PolarsDataType) -> pl.Schema:
    return pl.Schema({**domain_columns, **_AUDIT_COLUMNS})


CANONICAL_SCHEMAS: dict[DatasetKind, CanonicalSchema] = {
    DatasetKind.INSTRUMENT: CanonicalSchema(
        columns=_columns(
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
        columns=_columns(trade_date=pl.Date, is_trading_day=pl.Boolean),
        primary_key=("trade_date",),
        sort_key=("trade_date",),
    ),
    DatasetKind.DAILY_BAR: CanonicalSchema(
        columns=_columns(
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
            turnover=pl.Float64,
            pct_change=pl.Float64,
            pe_ttm=pl.Float64,
            pb_mrq=pl.Float64,
            ps_ttm=pl.Float64,
            pcf_ncf_ttm=pl.Float64,
        ),
        primary_key=("instrument_id", "trade_date"),
        sort_key=("instrument_id", "trade_date"),
    ),
    DatasetKind.SECURITY_STATUS: CanonicalSchema(
        columns=_columns(
            instrument_id=pl.String,
            trade_date=pl.Date,
            is_listed=pl.Boolean,
            is_suspended=pl.Boolean,
            is_risk_warning=pl.Boolean,
            board=pl.String,
            price_limit_rule_id=pl.String,
            tradable_reason=pl.String,
        ),
        primary_key=("instrument_id", "trade_date"),
        sort_key=("instrument_id", "trade_date"),
    ),
    DatasetKind.FINANCIAL_OBSERVATION: CanonicalSchema(
        columns=_columns(
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
    DatasetKind.CORPORATE_ACTION: CanonicalSchema(
        columns=_columns(
            instrument_id=pl.String,
            action_type=pl.String,
            record_date=pl.Date,
            ex_date=pl.Date,
            pay_date=pl.Date,
            cash_per_share=pl.Float64,
            share_ratio=pl.Float64,
            rights_price=pl.Float64,
        ),
        primary_key=(
            "instrument_id",
            "action_type",
            "record_date",
            "ex_date",
            "pay_date",
        ),
        sort_key=(
            "instrument_id",
            "action_type",
            "record_date",
            "ex_date",
            "pay_date",
        ),
    ),
}
