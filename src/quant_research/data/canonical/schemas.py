"""定义 Tushare 端点一对一的 Canonical Schema 与确定性键。"""

from dataclasses import dataclass

import polars as pl

from quant_research.domain.enums import DatasetKind

UTC_TIMESTAMP = pl.Datetime("us", "UTC")
type PolarsDataType = pl.DataType | type[pl.DataType]


@dataclass(frozen=True, slots=True)
class CanonicalSchema:
    """描述 Schema。入参：列、主键和排序键。返回值：不可变定义。异常：字段非法时抛出。"""

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


_FINANCIAL_FLOAT_COLUMNS = (
    "eps", "dt_eps", "total_revenue_ps", "revenue_ps", "capital_rese_ps",
    "surplus_rese_ps", "undist_profit_ps", "extra_item", "profit_dedt",
    "gross_margin", "current_ratio", "quick_ratio", "cash_ratio",
    "invturn_days", "arturn_days", "inv_turn", "ar_turn", "ca_turn",
    "fa_turn", "assets_turn", "op_income", "valuechange_income",
    "interst_income", "daa", "ebit", "ebitda", "fcff", "fcfe",
    "current_exint", "noncurrent_exint", "interestdebt", "netdebt",
    "tangible_asset", "working_capital", "networking_capital",
    "invest_capital", "retained_earnings", "diluted2_eps", "bps", "ocfps",
    "retainedps", "cfps", "ebit_ps", "fcff_ps", "fcfe_ps",
    "netprofit_margin", "grossprofit_margin", "cogs_of_sales",
    "expense_of_sales", "profit_to_gr", "saleexp_to_gr", "adminexp_of_gr",
    "finaexp_of_gr", "impai_ttm", "gc_of_gr", "op_of_gr", "ebit_of_gr",
    "roe", "roe_waa", "roe_dt", "roa", "npta", "roic", "roe_yearly",
    "roa2_yearly", "roe_avg", "opincome_of_ebt", "investincome_of_ebt",
    "n_op_profit_of_ebt", "tax_to_ebt", "dtprofit_to_profit",
    "salescash_to_or", "ocf_to_or", "ocf_to_opincome", "capitalized_to_da",
    "debt_to_assets", "assets_to_eqt", "dp_assets_to_eqt", "ca_to_assets",
    "nca_to_assets", "tbassets_to_totalassets", "int_to_talcap",
    "eqt_to_talcapital", "currentdebt_to_debt", "longdeb_to_debt",
    "ocf_to_shortdebt", "debt_to_eqt", "eqt_to_debt",
    "eqt_to_interestdebt", "tangibleasset_to_debt", "tangasset_to_intdebt",
    "tangibleasset_to_netdebt", "ocf_to_debt", "ocf_to_interestdebt",
    "ocf_to_netdebt", "ebit_to_interest", "longdebt_to_workingcapital",
    "ebitda_to_debt", "turn_days", "roa_yearly", "roa_dp", "fixed_assets",
    "profit_to_op", "q_saleexp_to_gr", "q_gc_to_gr", "q_roe", "q_dt_roe",
    "q_npta", "q_ocf_to_sales", "basic_eps_yoy", "dt_eps_yoy", "cfps_yoy",
    "op_yoy", "ebt_yoy", "netprofit_yoy", "dt_netprofit_yoy", "ocf_yoy",
    "roe_yoy", "bps_yoy", "assets_yoy", "eqt_yoy", "tr_yoy", "or_yoy",
    "q_gr_yoy", "q_gr_qoq", "q_sales_yoy", "q_sales_qoq", "q_op_yoy",
    "q_op_qoq", "q_profit_yoy", "q_profit_qoq", "q_netprofit_yoy",
    "q_netprofit_qoq", "equity_yoy", "rd_exp",
)


CANONICAL_SCHEMAS: dict[DatasetKind, CanonicalSchema] = {
    DatasetKind.STOCK_MASTER: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            instrument_id=pl.String, symbol=pl.String, name=pl.String,
            area=pl.String, industry=pl.String, fullname=pl.String,
            enname=pl.String, cnspell=pl.String, market=pl.String,
            exchange=pl.String, curr_type=pl.String, list_status=pl.String,
            list_date=pl.Date, delist_date=pl.Date, is_hs=pl.String,
            act_name=pl.String, act_ent_type=pl.String, board=pl.String,
        ),
        ("instrument_id",), ("instrument_id",),
    ),
    DatasetKind.FUND_MASTER: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            instrument_id=pl.String, name=pl.String, management=pl.String,
            custodian=pl.String, fund_type=pl.String, found_date=pl.Date,
            due_date=pl.Date, list_date=pl.Date, issue_date=pl.Date,
            delist_date=pl.Date, issue_amount=pl.Float64, m_fee=pl.Float64,
            c_fee=pl.Float64, duration_year=pl.Float64, p_value=pl.Float64,
            min_amount=pl.Float64, exp_return=pl.Float64, benchmark=pl.String,
            status=pl.String, invest_type=pl.String, type=pl.String,
            trustee=pl.String, purc_startdate=pl.Date, redm_startdate=pl.Date,
            market=pl.String,
        ),
        ("instrument_id",), ("instrument_id",),
    ),
    DatasetKind.INDEX_MASTER: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            index_id=pl.String, name=pl.String, fullname=pl.String,
            market=pl.String, publisher=pl.String, index_type=pl.String,
            category=pl.String, base_date=pl.Date, base_point=pl.Float64,
            list_date=pl.Date, weight_rule=pl.String, description=pl.String,
            exp_date=pl.Date,
        ),
        ("index_id",), ("index_id",),
    ),
    DatasetKind.TRADE_CALENDAR: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            exchange=pl.String, trade_date=pl.Date, is_trading_day=pl.Boolean,
            previous_trade_date=pl.Date,
        ),
        ("exchange", "trade_date"), ("trade_date", "exchange"),
    ),
    DatasetKind.STOCK_DAILY_BAR: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            instrument_id=pl.String, trade_date=pl.Date, open=pl.Float64,
            high=pl.Float64, low=pl.Float64, close=pl.Float64,
            preclose=pl.Float64, change=pl.Float64, pct_change=pl.Float64,
            volume=pl.Int64, amount=pl.Float64, after_hours_volume=pl.Int64,
            after_hours_amount=pl.Float64,
        ),
        ("instrument_id", "trade_date"), ("instrument_id", "trade_date"),
    ),
    DatasetKind.STOCK_ADJUSTMENT_FACTOR: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            instrument_id=pl.String, trade_date=pl.Date, adjustment_factor=pl.Float64,
        ),
        ("instrument_id", "trade_date"), ("instrument_id", "trade_date"),
    ),
    DatasetKind.FUND_DAILY_BAR: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            instrument_id=pl.String, trade_date=pl.Date, open=pl.Float64,
            high=pl.Float64, low=pl.Float64, close=pl.Float64,
            preclose=pl.Float64, change=pl.Float64, pct_change=pl.Float64,
            volume=pl.Int64, amount=pl.Float64,
        ),
        ("instrument_id", "trade_date"), ("instrument_id", "trade_date"),
    ),
    DatasetKind.FUND_ADJUSTMENT_FACTOR: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            instrument_id=pl.String, trade_date=pl.Date, adjustment_factor=pl.Float64,
        ),
        ("instrument_id", "trade_date"), ("instrument_id", "trade_date"),
    ),
    DatasetKind.INDEX_DAILY_BAR: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            index_id=pl.String, trade_date=pl.Date, close=pl.Float64,
            open=pl.Float64, high=pl.Float64, low=pl.Float64,
            preclose=pl.Float64, change=pl.Float64, pct_change=pl.Float64,
            volume=pl.Int64, amount=pl.Float64,
        ),
        ("index_id", "trade_date"), ("index_id", "trade_date"),
    ),
    DatasetKind.STOCK_DAILY_BASIC: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            instrument_id=pl.String, trade_date=pl.Date, close=pl.Float64,
            turnover_rate=pl.Float64, turnover_rate_free_float=pl.Float64,
            volume_ratio=pl.Float64, pe=pl.Float64, pe_ttm=pl.Float64,
            pb=pl.Float64, ps=pl.Float64, ps_ttm=pl.Float64,
            dividend_yield=pl.Float64, dividend_yield_ttm=pl.Float64,
            total_share=pl.Float64, float_share=pl.Float64,
            free_share=pl.Float64, total_market_value=pl.Float64,
            circulating_market_value=pl.Float64, limit_status=pl.Int64,
        ),
        ("instrument_id", "trade_date"), ("instrument_id", "trade_date"),
    ),
    DatasetKind.STOCK_SUSPENSION: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            instrument_id=pl.String, trade_date=pl.Date,
            suspend_timing=pl.String, suspend_type=pl.String,
        ),
        ("instrument_id", "trade_date", "suspend_type"),
        ("instrument_id", "trade_date", "suspend_type"),
    ),
    DatasetKind.STOCK_RISK_WARNING: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            instrument_id=pl.String, name=pl.String, trade_date=pl.Date,
            risk_type=pl.String, risk_type_name=pl.String,
        ),
        ("instrument_id", "trade_date", "risk_type"),
        ("instrument_id", "trade_date", "risk_type"),
    ),
    DatasetKind.STOCK_FINANCIAL_INDICATOR: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            instrument_id=pl.String, announcement_date=pl.Date,
            report_period=pl.Date,
            **{name: pl.Float64 for name in _FINANCIAL_FLOAT_COLUMNS},
            update_flag=pl.String, revision=pl.Int64,
        ),
        ("instrument_id", "report_period", "revision"),
        ("instrument_id", "report_period", "revision"),
    ),
    DatasetKind.INDUSTRY_CATALOG: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            industry_index_id=pl.String, industry_name=pl.String,
            level=pl.String, industry_code=pl.String, is_published=pl.String,
            parent_code=pl.String, taxonomy=pl.String,
        ),
        ("industry_index_id",), ("industry_index_id",),
    ),
    DatasetKind.INDUSTRY_MEMBERSHIP: CanonicalSchema(
        _CanonicalSchemaFactory.columns(
            level1_code=pl.String, level1_name=pl.String,
            level2_code=pl.String, level2_name=pl.String,
            level3_code=pl.String, level3_name=pl.String,
            instrument_id=pl.String, instrument_name=pl.String,
            in_date=pl.Date, out_date=pl.Date, is_current=pl.Boolean,
            in_available_at=UTC_TIMESTAMP, out_available_at=UTC_TIMESTAMP,
        ),
        ("level1_code", "instrument_id", "in_date"),
        ("level1_code", "instrument_id", "in_date"),
    ),
}
