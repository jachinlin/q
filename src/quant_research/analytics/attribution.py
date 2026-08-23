"""提供分析与收益归因相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import polars as pl

from quant_research.analytics.performance import calculate_performance

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
    "STYLE_EXPOSURE_NOT_AVAILABLE",
)


@dataclass(frozen=True, slots=True)
class AttributionResult:
    """记录一次回测绩效与归因操作的结果、业务指标和审计身份。

    入参：
        exposure_summary：按行业、风格或其他维度汇总的组合暴露表。
        attribution：各暴露维度对组合收益贡献的归因明细表。
        disclosures：参与本次处理的指标披露；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    exposure_summary: pl.DataFrame
    attribution: pl.DataFrame
    disclosures: tuple[str, ...]


def calculate_attribution(
    nav: pl.DataFrame,
    holdings: pl.DataFrame,
    fills: pl.DataFrame,
    costs: pl.DataFrame,
) -> AttributionResult:
    """计算归因；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        nav：按交易日排序的账户净值序列，用于计算收益、回撤和归因。
        holdings：逐交易日、逐证券的收盘持仓快照。
        fills：回测撮合产生的逐笔成交及拒绝记录。
        costs：按交易日汇总的佣金、印花税和其他交易成本。
    返回值：
        返回计算归因后的归因（``AttributionResult``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Calculate bounded cash-exact attribution without invented classifications.
    """
    performance = calculate_performance(nav, holdings, fills, costs)
    _AttributionSupport._validate_holdings(nav, holdings)

    nav_by_date = {
        row["trade_date"]: (row["equity_fen"], row["long_market_value_fen"])
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
        _AttributionSupport._append_security_attribution(
            attribution_rows,
            trade_date,
            security_pnl,
            unexplained_pnl,
            denominator,
        )
        _AttributionSupport._append_attribution_row(
            attribution_rows,
            trade_date,
            "STYLE",
            "UNAVAILABLE",
            explained_pnl,
            denominator,
        )
        _AttributionSupport._append_attribution_row(
            attribution_rows,
            trade_date,
            "STYLE",
            "UNEXPLAINED",
            unexplained_pnl,
            denominator,
        )
        if abs(return_by_date[trade_date] - actual_pnl / denominator) > 1e-12:
            raise ValueError("attribution return identity does not match NAV")

        _AttributionSupport._append_exposures(
            exposure_rows,
            trade_date,
            current_values,
            current_nav,
        )
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
        attribution,
        _DISCLOSURES,
    )


class _AttributionSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
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
            row["trade_date"]: row["long_market_value_fen"]
            for row in nav.iter_rows(named=True)
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

    @staticmethod
    def _append_security_attribution(
        rows: list[dict[str, object]],
        trade_date: date,
        security_pnl: dict[str, int],
        unexplained_pnl: int,
        denominator: int,
    ) -> None:
        ranked = sorted(security_pnl.items(), key=lambda item: (-abs(item[1]), item[0]))
        for instrument, pnl_fen in ranked[:20]:
            _AttributionSupport._append_attribution_row(
                rows, trade_date, "SECURITY", instrument, pnl_fen, denominator
            )
        if len(ranked) > 20:
            _AttributionSupport._append_attribution_row(
                rows,
                trade_date,
                "SECURITY",
                "OTHER",
                sum(value for _, value in ranked[20:]),
                denominator,
            )
        _AttributionSupport._append_attribution_row(
            rows,
            trade_date,
            "SECURITY",
            "UNEXPLAINED",
            unexplained_pnl,
            denominator,
        )

    @staticmethod
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

    @staticmethod
    def _append_exposures(
        rows: list[dict[str, object]],
        trade_date: date,
        current_values: dict[str, int],
        nav_fen: int,
    ) -> None:
        ranked = sorted(
            current_values.items(), key=lambda item: (-abs(item[1]), item[0])
        )
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
                    "dimension": "CASH",
                    "key": "CASH",
                    "weight": 1.0 - total_weight,
                },
                {
                    "trade_date": trade_date,
                    "dimension": "STYLE",
                    "key": "UNAVAILABLE",
                    "weight": total_weight,
                },
            ]
        )
