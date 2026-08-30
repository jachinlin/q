"""验证策略研究报告从可信产物读取完整、类型化的数据。"""

from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any, cast

import polars as pl
import pytest

from quant_research.backtest.run_schema import RUN_PARQUET_SCHEMAS
from quant_research.backtest.study_artifacts import StrategyStudyArtifactPublisher
from quant_research.dashboard.strategy_studies import StrategyStudyDashboardService


class _Studies:
    """返回单个已发布研究的测试替身。"""

    def __init__(self, artifact_dir: str, manifest_hash: str) -> None:
        self._record = SimpleNamespace(
            id="study-1",
            artifact_dir=artifact_dir,
            manifest_hash=manifest_hash,
        )

    def show(self, study_id: str) -> Any:
        """读取固定研究。"""

        assert study_id == "study-1"
        return self._record


def test_report_reads_past_artifact_page_limit_and_aggregates_attribution(
    tmp_path: Any,
) -> None:
    """报告必须覆盖第 1000 行之后的数据，并确定性汇总证券归因。"""

    dates = [date(2020, 1, 1) + timedelta(days=index) for index in range(1001)]
    zeros = [0.0] * len(dates)
    nav = [1.0 + index / 10_000 for index in range(len(dates))]
    tables = StrategyStudyArtifactPublisher.canonical_tables(
        {
            "performance": pl.DataFrame(
                {
                    "trade_date": dates,
                    "return": zeros,
                    "benchmark_return": zeros,
                    "cumulative_return": [value - 1.0 for value in nav],
                    "benchmark_cumulative_return": zeros,
                    "active_return": zeros,
                    "nav": nav,
                    "benchmark_nav": [1.0] * len(dates),
                    "gross_nav": nav,
                    "gross_cumulative_return": [value - 1.0 for value in nav],
                    "cumulative_cost_drag": zeros,
                    "drawdown": zeros,
                    "active_drawdown": zeros,
                },
                schema=RUN_PARQUET_SCHEMAS["performance"],
            ),
            "rolling_performance": pl.DataFrame(
                {
                    "trade_date": [dates[-1]],
                    "window_sessions": [252],
                    "annualized_return": [0.1],
                    "benchmark_annualized_return": [0.02],
                    "annualized_excess_return": [0.08],
                    "annualized_volatility": [0.2],
                    "sharpe_ratio": [0.5],
                    "max_drawdown": [-0.1],
                    "tracking_error": [0.15],
                    "information_ratio": [0.4],
                    "beta": [0.8],
                },
                schema=RUN_PARQUET_SCHEMAS["rolling_performance"],
            ),
            "monthly_returns": pl.DataFrame(
                {
                    "year": [2022],
                    "month": [9],
                    "period_start": [dates[-30]],
                    "period_end": [dates[-1]],
                    "portfolio_return": [0.01],
                    "benchmark_return": [0.0],
                    "relative_return": [0.01],
                },
                schema=RUN_PARQUET_SCHEMAS["monthly_returns"],
            ),
            "annual_returns": pl.DataFrame(
                {
                    "year": [2022],
                    "period_start": [dates[-365]],
                    "period_end": [dates[-1]],
                    "portfolio_return": [0.1],
                    "benchmark_return": [0.02],
                    "relative_return": [0.08],
                },
                schema=RUN_PARQUET_SCHEMAS["annual_returns"],
            ),
            "drawdown_episodes": pl.DataFrame(
                {
                    "episode_index": [1],
                    "peak_date": [dates[10]],
                    "trough_date": [dates[11]],
                    "recovery_date": [dates[12]],
                    "max_drawdown": [-0.1],
                    "underwater_sessions": [1],
                    "recovery_sessions": [1],
                    "is_recovered": [True],
                },
                schema=RUN_PARQUET_SCHEMAS["drawdown_episodes"],
            ),
            "exposure_summary": pl.DataFrame(
                {
                    "trade_date": [dates[0], dates[0], dates[0]],
                    "dimension": ["CASH", "SECURITY", "STYLE"],
                    "key": ["CASH", "510300.SH", "UNAVAILABLE"],
                    "weight": [0.4, 0.6, 0.6],
                },
                schema=RUN_PARQUET_SCHEMAS["exposure_summary"],
            ),
            "attribution": pl.DataFrame(
                {
                    "trade_date": [dates[0], dates[0], dates[1]],
                    "dimension": ["SECURITY", "SECURITY", "SECURITY"],
                    "key": ["510300.SH", "513100.SH", "510300.SH"],
                    "pnl_fen": [10, -5, 20],
                    "contribution_return": [0.1, -0.4, 0.2],
                },
                schema=RUN_PARQUET_SCHEMAS["attribution"],
            ),
            "execution_summary": pl.DataFrame(
                {
                    "side": ["BUY"],
                    "reason_code": ["FILLED"],
                    "order_count": [1],
                    "requested_quantity": [100],
                    "filled_quantity": [100],
                    "unfilled_quantity": [0],
                    "priced_requested_notional_fen": [100_000],
                    "priced_filled_notional_fen": [100_000],
                    "unpriced_order_count": [0],
                },
                schema=RUN_PARQUET_SCHEMAS["execution_summary"],
            ),
        }
    )
    final, manifest_hash, _ = StrategyStudyArtifactPublisher(
        tmp_path, "study-1"
    ).publish(
        tables,
        config={"kind": "STRATEGY_BACKTEST"},
        metrics={"annualized_return": 0.1},
        quality_disclosure={
            "calculation_mode": "CASH_EXACT",
            "rolling_window_sessions": 252,
            "tail_risk_method": "HISTORICAL_95",
            "risk_free_rate_annual": 0.0,
            "undefined_metrics": {},
            "unavailable_dimensions": {"style": "UNAVAILABLE"},
            "attribution_method": "CASH_EXACT_SECURITY",
            "warnings": ["STYLE_EXPOSURE_NOT_AVAILABLE"],
        },
        identities={"catalog_hash": "a" * 64},
    )
    service = StrategyStudyDashboardService(
        cast(Any, _Studies(str(final), manifest_hash)),
        cast(Any, object()),
        tmp_path,
    )

    report = service.report("study-1")

    assert len(report.performance) == 1001
    assert report.performance[0].trade_date == dates[0]
    assert report.performance[-1].trade_date == dates[-1]
    assert [item.key for item in report.attribution] == ["513100.SH", "510300.SH"]
    assert report.attribution[1].pnl_fen == 30
    assert report.attribution[1].contribution_return == pytest.approx(0.3)
    assert [item.dimension for item in report.exposure] == ["CASH", "SECURITY"]
    assert report.quality.rolling_window_sessions == 252
