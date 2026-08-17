"""验证无行业输入的证券收益归因与暴露契约。"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from quant_research.analytics.attribution import calculate_attribution

NAV_SCHEMA = {
    "trade_date": pl.Date,
    "cash_fen": pl.Int64,
    "market_value_fen": pl.Int64,
    "nav_fen": pl.Int64,
    "benchmark_close": pl.Float64,
}
HOLDINGS_SCHEMA = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "total_quantity": pl.Int64,
    "sellable_quantity": pl.Int64,
    "cost_basis_fen": pl.Int64,
    "market_value_fen": pl.Int64,
}
FILLS_SCHEMA = {
    "trade_date": pl.Date,
    "result_index": pl.Int32,
    "instrument_id": pl.String,
    "side": pl.String,
    "requested_quantity": pl.Int64,
    "reference_price": pl.Float64,
    "requested_reference_value_fen": pl.Int64,
    "filled_quantity": pl.Int64,
    "unfilled_quantity": pl.Int64,
    "price": pl.Float64,
    "gross_value_fen": pl.Int64,
    "reason_code": pl.String,
    "detail": pl.String,
}
COSTS_SCHEMA = {
    "trade_date": pl.Date,
    "result_index": pl.Int32,
    "instrument_id": pl.String,
    "commission_fen": pl.Int64,
    "stamp_tax_fen": pl.Int64,
    "transfer_fee_fen": pl.Int64,
    "total_fees_fen": pl.Int64,
}


def _nav() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 4),
                date(2024, 1, 5),
            ],
            "cash_fen": [10_000, 4_000, 4_000, 4_000],
            "market_value_fen": [0, 6_000, 6_600, 6_600],
            "nav_fen": [10_000, 10_000, 10_600, 10_600],
            "benchmark_close": [100.0, 100.0, 100.0, 100.0],
        },
        schema=NAV_SCHEMA,
    )


def _holdings(instrument_id: str = "600001.SH") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)],
            "instrument_id": [instrument_id] * 3,
            "total_quantity": [100, 100, 100],
            "sellable_quantity": [100, 100, 100],
            "cost_basis_fen": [6_000, 6_000, 6_000],
            "market_value_fen": [6_000, 6_600, 6_600],
        },
        schema=HOLDINGS_SCHEMA,
    )


def _fills(instrument_id: str = "600001.SH") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [date(2024, 1, 3), date(2024, 1, 5)],
            "result_index": [0, 0],
            "instrument_id": [instrument_id, instrument_id],
            "side": ["BUY", "SELL"],
            "requested_quantity": [100, 100],
            "reference_price": [0.6, None],
            "requested_reference_value_fen": [6_000, None],
            "filled_quantity": [100, 0],
            "unfilled_quantity": [0, 100],
            "price": [0.6, None],
            "gross_value_fen": [6_000, 0],
            "reason_code": ["FILLED", "SUSPENDED"],
            "detail": [None, "suspended"],
        },
        schema=FILLS_SCHEMA,
    )


def _costs(instrument_id: str = "600001.SH") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": [date(2024, 1, 3)],
            "result_index": [0],
            "instrument_id": [instrument_id],
            "commission_fen": [0],
            "stamp_tax_fen": [0],
            "transfer_fee_fen": [0],
            "total_fees_fen": [0],
        },
        schema=COSTS_SCHEMA,
    )


def test_attribution_is_cash_exact_without_industry_dimension() -> None:
    """证券和风格归因应各自与实际日损益保持一致。"""
    result = calculate_attribution(
        _nav(),
        _holdings(),
        _fills(),
        _costs(),
    )

    assert "INDUSTRY" not in set(result.exposure_summary["dimension"].to_list())
    assert "INDUSTRY" not in set(result.attribution["dimension"].to_list())
    cash = result.exposure_summary.filter(pl.col("dimension") == "CASH")
    assert cash["weight"].to_list() == pytest.approx(
        [1.0, 0.4, 4_000 / 10_600, 4_000 / 10_600]
    )
    security_gain = result.attribution.filter(
        (pl.col("trade_date") == date(2024, 1, 4))
        & (pl.col("dimension") == "SECURITY")
        & (pl.col("key") == "600001.SH")
    )
    assert security_gain["pnl_fen"].to_list() == [600]
    assert security_gain["contribution_return"].to_list() == pytest.approx([0.06])
    assert result.disclosures == (
        "FACTOR_EXPOSURE_NOT_AVAILABLE",
        "STYLE_EXPOSURE_NOT_AVAILABLE",
    )

    nav_values = dict(
        zip(_nav()["trade_date"].to_list(), _nav()["nav_fen"].to_list(), strict=True)
    )
    previous_nav: int | None = None
    for trade_date, nav_fen in nav_values.items():
        expected = 0 if previous_nav is None else nav_fen - previous_nav
        for dimension in ("SECURITY", "STYLE"):
            total = result.attribution.filter(
                (pl.col("trade_date") == trade_date)
                & (pl.col("dimension") == dimension)
            )["pnl_fen"].sum()
            assert total == expected
        previous_nav = nav_fen
