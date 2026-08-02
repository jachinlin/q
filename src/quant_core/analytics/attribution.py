"""Cash-exact attribution from canonical backtest artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl

from quant_core.analytics.performance import calculate_performance

_HOLDINGS_SCHEMA = pl.Schema(
    {
        "trade_date": pl.Date,
        "instrument_id": pl.String,
        "total_quantity": pl.Int64,
        "sellable_quantity": pl.Int64,
        "cost_basis_fen": pl.Int64,
        "market_value_fen": pl.Int64,
    }
)
_EXPOSURE_SCHEMA = pl.Schema(
    {
        "trade_date": pl.Date,
        "dimension": pl.String,
        "key": pl.String,
        "weight": pl.Float64,
    }
)
_FACTOR_SCHEMA = pl.Schema(
    {
        "factor_ref": pl.String,
        "observation_count": pl.Int64,
        "rank_ic_mean": pl.Float64,
        "rank_ic_std": pl.Float64,
        "top_quantile_return": pl.Float64,
        "bottom_quantile_return": pl.Float64,
        "quality_code": pl.String,
    }
)
_ATTRIBUTION_SCHEMA = pl.Schema(
    {
        "trade_date": pl.Date,
        "dimension": pl.String,
        "key": pl.String,
        "pnl_fen": pl.Int64,
        "contribution_return": pl.Float64,
    }
)

_DISCLOSURES = (
    "FACTOR_EXPOSURE_NOT_AVAILABLE",
    "INDUSTRY_CLASSIFICATION_NOT_AVAILABLE",
    "STYLE_EXPOSURE_NOT_AVAILABLE",
)


@dataclass(frozen=True, slots=True)
class AttributionResult:
    exposure_summary: pl.DataFrame
    factor_summary: pl.DataFrame
    attribution: pl.DataFrame
    disclosures: tuple[str, ...]


def calculate_attribution(
    nav: pl.DataFrame,
    holdings: pl.DataFrame,
    fills: pl.DataFrame,
    costs: pl.DataFrame,
) -> AttributionResult:
    """Calculate bounded cash-exact attribution without invented classifications."""
    performance = calculate_performance(nav, fills, costs)
    _validate_holdings(nav, holdings)

    nav_by_date = {
        row["trade_date"]: (row["nav_fen"], row["market_value_fen"])
        for row in nav.iter_rows(named=True)
    }
    holdings_by_date: dict[date, dict[str, int]] = {
        trade_date: {} for trade_date in nav_by_date
    }
    for row in holdings.iter_rows(named=True):
        holdings_by_date[row["trade_date"]][row["instrument_id"]] = row[
            "market_value_fen"
        ]
    flows: dict[date, dict[str, list[int]]] = {}
    for row in fills.iter_rows(named=True):
        if row["filled_quantity"] <= 0:
            continue
        trade_date = row["trade_date"]
        instrument = row["instrument_id"]
        buy_sell = flows.setdefault(trade_date, {}).setdefault(instrument, [0, 0])
        if row["side"] == "BUY":
            buy_sell[0] += row["gross_value_fen"]
        elif row["side"] == "SELL":
            buy_sell[1] += row["gross_value_fen"]
        else:
            raise ValueError("fill side must be BUY or SELL")

    exposure_rows: list[dict[str, object]] = []
    attribution_rows: list[dict[str, object]] = []
    previous_values: dict[str, int] = {}
    previous_nav: int | None = None
    return_by_date = dict(
        zip(
            performance.nav["trade_date"].to_list(),
            performance.nav["portfolio_daily_return"].to_list(),
            strict=True,
        )
    )
    for trade_date, (current_nav, _) in nav_by_date.items():
        current_values = holdings_by_date[trade_date]
        denominator = previous_nav if previous_nav is not None else current_nav
        instruments = (
            set(previous_values) | set(current_values) | set(flows.get(trade_date, {}))
        )
        security_pnl: dict[str, int] = {}
        for instrument in instruments:
            buy, sell = flows.get(trade_date, {}).get(instrument, [0, 0])
            contribution = (
                current_values.get(instrument, 0)
                - previous_values.get(instrument, 0)
                + sell
                - buy
            )
            if contribution != 0:
                security_pnl[instrument] = contribution

        actual_pnl = 0 if previous_nav is None else current_nav - previous_nav
        explained_pnl = sum(security_pnl.values())
        unexplained_pnl = actual_pnl - explained_pnl
        _append_security_attribution(
            attribution_rows,
            trade_date,
            security_pnl,
            unexplained_pnl,
            denominator,
        )
        for dimension, key in (
            ("INDUSTRY", "UNKNOWN"),
            ("STYLE", "UNAVAILABLE"),
        ):
            _append_attribution_row(
                attribution_rows,
                trade_date,
                dimension,
                key,
                explained_pnl,
                denominator,
            )
            _append_attribution_row(
                attribution_rows,
                trade_date,
                dimension,
                "UNEXPLAINED",
                unexplained_pnl,
                denominator,
            )
        if abs(return_by_date[trade_date] - actual_pnl / denominator) > 1e-12:
            raise ValueError("attribution return identity does not match NAV")

        _append_exposures(exposure_rows, trade_date, current_values, current_nav)
        previous_values = current_values
        previous_nav = current_nav

    exposure = pl.DataFrame(exposure_rows, schema=_EXPOSURE_SCHEMA).sort(
        ["trade_date", "dimension", "key"]
    )
    attribution = pl.DataFrame(attribution_rows, schema=_ATTRIBUTION_SCHEMA).sort(
        ["trade_date", "dimension", "key"]
    )
    return AttributionResult(
        exposure,
        pl.DataFrame(schema=_FACTOR_SCHEMA),
        attribution,
        _DISCLOSURES,
    )


def _validate_holdings(nav: pl.DataFrame, holdings: pl.DataFrame) -> None:
    if not isinstance(holdings, pl.DataFrame):
        raise TypeError("holdings must be a Polars DataFrame")
    if holdings.schema != _HOLDINGS_SCHEMA:
        raise ValueError("holdings schema must match the canonical backtest schema")
    if any(holdings[column].null_count() for column in holdings.columns):
        raise ValueError("holdings fields must be non-null")
    keys = list(holdings.select("trade_date", "instrument_id").iter_rows())
    if len(keys) != len(set(keys)):
        raise ValueError("holdings primary key must be unique")
    if keys != sorted(keys):
        raise ValueError("holdings rows must be canonically sorted")

    nav_market = {
        row["trade_date"]: row["market_value_fen"] for row in nav.iter_rows(named=True)
    }
    holdings_market = {trade_date: 0 for trade_date in nav_market}
    for row in holdings.iter_rows(named=True):
        trade_date = row["trade_date"]
        if trade_date not in nav_market:
            raise ValueError("holdings identity must reference a NAV trade date")
        if row["total_quantity"] <= 0:
            raise ValueError("holdings total quantity must be positive")
        if not 0 <= row["sellable_quantity"] <= row["total_quantity"]:
            raise ValueError("holdings sellable quantity is invalid")
        if row["cost_basis_fen"] < 0 or row["market_value_fen"] < 0:
            raise ValueError("holdings cash fields must be nonnegative")
        holdings_market[trade_date] += row["market_value_fen"]
    if holdings_market != nav_market:
        raise ValueError("holdings market value identity must match NAV")


def _append_security_attribution(
    rows: list[dict[str, object]],
    trade_date: date,
    security_pnl: dict[str, int],
    unexplained_pnl: int,
    denominator: int,
) -> None:
    ranked = sorted(security_pnl.items(), key=lambda item: (-abs(item[1]), item[0]))
    for instrument, pnl_fen in ranked[:20]:
        _append_attribution_row(
            rows, trade_date, "SECURITY", instrument, pnl_fen, denominator
        )
    if len(ranked) > 20:
        _append_attribution_row(
            rows,
            trade_date,
            "SECURITY",
            "OTHER",
            sum(value for _, value in ranked[20:]),
            denominator,
        )
    _append_attribution_row(
        rows,
        trade_date,
        "SECURITY",
        "UNEXPLAINED",
        unexplained_pnl,
        denominator,
    )


def _append_attribution_row(
    rows: list[dict[str, object]],
    trade_date: date,
    dimension: str,
    key: str,
    pnl_fen: int,
    denominator: int,
) -> None:
    rows.append(
        {
            "trade_date": trade_date,
            "dimension": dimension,
            "key": key,
            "pnl_fen": pnl_fen,
            "contribution_return": pnl_fen / denominator,
        }
    )


def _append_exposures(
    rows: list[dict[str, object]],
    trade_date: date,
    current_values: dict[str, int],
    nav_fen: int,
) -> None:
    ranked = sorted(current_values.items(), key=lambda item: (-abs(item[1]), item[0]))
    for instrument, market_value in ranked[:20]:
        rows.append(
            {
                "trade_date": trade_date,
                "dimension": "SECURITY",
                "key": instrument,
                "weight": market_value / nav_fen,
            }
        )
    if len(ranked) > 20:
        rows.append(
            {
                "trade_date": trade_date,
                "dimension": "SECURITY",
                "key": "OTHER",
                "weight": sum(value for _, value in ranked[20:]) / nav_fen,
            }
        )
    total_weight = sum(current_values.values()) / nav_fen
    rows.extend(
        [
            {
                "trade_date": trade_date,
                "dimension": "INDUSTRY",
                "key": "UNKNOWN",
                "weight": total_weight,
            },
            {
                "trade_date": trade_date,
                "dimension": "STYLE",
                "key": "UNAVAILABLE",
                "weight": total_weight,
            },
        ]
    )
