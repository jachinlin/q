"""验证独立 FactorStudy Worker 的输入适配语义。"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

import polars as pl

from quant_research.bootstrap.worker import _FactorPublisher, _FactorStudySession
from quant_research.domain.enums import MultipleTestingMethod
from quant_research.factor_studies.models import (
    FactorIndustrySettings,
    FactorStudyDefinition,
    FactorStudyUniverse,
    IndustryUnclassifiedPolicy,
)
from quant_research.factors import FACTOR_OUTPUT_SCHEMA, FactorArtifact


class _Artifact:
    """提供标准因子产物形状的最小测试替身。"""

    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame

    def lazy_frame(self) -> pl.LazyFrame:
        """返回待适配的标准因子表。"""
        return self._frame.lazy()


class _IndustryRepository:
    """返回固定的申万 PIT 行业成员表。"""

    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame

    def industry_memberships_on_dates(self, _: object, __: object) -> pl.LazyFrame:
        """返回固定行业状态。

        入参：
            两个参数为本测试不使用的证券与日期范围。
        返回值：
            返回固定 LazyFrame。
        异常：
            无。
        """
        return self._frame.lazy()


class _EmptyExecutableRepository:
    """提供缺失证券状态时仍具固定 Schema 的研究输入。"""

    def stocks(self) -> pl.LazyFrame:
        """返回一个合法证券元数据行。"""
        return pl.DataFrame(
            {
                "instrument_id": ["000001.SZ"],
                "board": ["MAIN"],
                "list_date": [date(2000, 1, 1)],
                "delist_date": pl.Series([None], dtype=pl.Date),
            }
        ).lazy()

    def stock_bars(self, _: object, __: date, ___: date) -> pl.LazyFrame:
        """返回固定未复权行情 Schema。"""
        return pl.DataFrame(
            schema={
                "instrument_id": pl.String,
                "trade_date": pl.Date,
                "low": pl.Float64,
                "preclose": pl.Float64,
            }
        ).lazy()

    def stock_suspensions(self, _: date, __: date, ___: object) -> pl.LazyFrame:
        """返回固定空停牌事件 Schema。"""
        return pl.DataFrame(
            schema={
                "instrument_id": pl.String,
                "trade_date": pl.Date,
            }
        ).lazy()

    def stock_risk_warnings(self, _: date, __: date, ___: object) -> pl.LazyFrame:
        """返回固定空风险警示事件 Schema。"""
        return pl.DataFrame(
            schema={"instrument_id": pl.String, "trade_date": pl.Date}
        ).lazy()

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        """返回请求区间内的固定开市日。"""
        return (
            pl.DataFrame(
                {
                    "trade_date": [start, end],
                    "is_trading_day": [True, True],
                },
                schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
            )
            .unique()
            .lazy()
        )


def test_factor_study_maps_date_applies_direction_and_pit_scope() -> None:
    """研究边界必须改名、按方向翻转并排除不在 PIT 股票池的证券。"""
    signal_date = date(2026, 1, 5)
    frame = pl.DataFrame(
        {
            "trade_date": [signal_date],
            "instrument_id": ["000001.SZ"],
            "factor_id": ["momentum_120_20"],
            "value": [1.5],
            "available_at": [datetime(2026, 1, 5, 7, 0, tzinfo=UTC)],
            "is_valid": [True],
        },
        schema=FACTOR_OUTPUT_SCHEMA,
    )
    artifacts = {
        "momentum_120_20": cast(FactorArtifact, _Artifact(frame)),
    }

    result = _FactorStudySession._analysis_factor_frame(
        artifacts,
        ("momentum_120_20",),
        {"momentum_120_20": -1},
        pl.DataFrame(
            {
                "signal_date": [signal_date],
                "instrument_id": ["000001.SZ"],
                "eligible": [True],
            }
        ),
    )

    assert "trade_date" not in result.columns
    assert result.select("signal_date", "instrument_id", "factor_id").row(0) == (
        signal_date,
        "000001.SZ",
        "momentum_120_20",
    )
    assert result["value"].item() == -1.5
    assert result["signal_variant"].item() == "DIRECTION_ADJUSTED"


def test_industry_policies_publish_coverage_and_distinct_unclassified_scope() -> None:
    day = date(2026, 1, 5)
    instruments = [f"00000{index}.SZ" for index in range(1, 6)]
    state = pl.DataFrame(
        {
            "query_date": [day] * 3,
            "instrument_id": instruments[:3],
            "level1_code": ["A", "A", "B"],
        }
    )
    session = _FactorStudySession(
        cast(Any, object()),
        cast(Any, _IndustryRepository(state)),
        cast(Any, object()),
        cast(Any, object()),
        Path("."),
    )
    factor_frame = pl.DataFrame(
        {
            "signal_date": [day] * 5,
            "instrument_id": instruments,
            "factor_id": ["value"] * 5,
            "value": [1.0, 3.0, 5.0, 7.0, 9.0],
            "is_valid": [True] * 5,
            "invalid_reason": pl.Series([None] * 5, dtype=pl.String),
            "signal_variant": ["DIRECTION_ADJUSTED"] * 5,
        }
    )
    eligible = pl.DataFrame(
        {
            "signal_date": [day] * 5,
            "instrument_id": instruments,
            "eligible": [True] * 5,
        }
    )

    def config(policy: IndustryUnclassifiedPolicy) -> FactorStudyDefinition:
        return FactorStudyDefinition(
            name="industry policy",
            description="",
            tags=(),
            start_date=day,
            end_date=day,
            correction=MultipleTestingMethod.BH_FDR,
            factor_ids=("value",),
            universe=FactorStudyUniverse(name="CN_STOCK_STANDARD"),
            horizons=(1,),
            quantiles=5,
            industry=FactorIndustrySettings(
                taxonomy="SW2021",
                unclassified_policy=policy,
            ),
            cost_bps_scenarios=(5, 10, 20),
        )

    excluded, exclude_coverage = session._industry_variants(
        factor_frame,
        eligible,
        (),
        (day,),
        config(IndustryUnclassifiedPolicy.EXCLUDE),
    )
    unclassified, unclassified_coverage = session._industry_variants(
        factor_frame,
        eligible,
        (),
        (day,),
        config(IndustryUnclassifiedPolicy.UNCLASSIFIED),
    )

    assert exclude_coverage.select(
        "eligible_count",
        "classified_count",
        "tombstone_count",
        "missing_state_count",
        "usable_count",
        "classified_coverage",
        "usable_coverage",
    ).row(0) == (5, 3, 0, 2, 3, 0.6, 0.6)
    assert unclassified_coverage["usable_count"].item() == 5
    exclude_neutral = excluded.filter(
        pl.col("signal_variant") == "INDUSTRY_NEUTRALIZED"
    )
    unclassified_neutral = unclassified.filter(
        pl.col("signal_variant") == "INDUSTRY_NEUTRALIZED"
    )
    assert exclude_neutral["is_valid"].to_list() == [True, True, False, False, False]
    assert unclassified_neutral["is_valid"].to_list() == [True, True, False, True, True]


def test_factor_metrics_keep_rank_and_spread_corrections_in_separate_families() -> None:
    summary = pl.DataFrame(
        {
            "signal_variant": ["DIRECTION_ADJUSTED"] * 2,
            "label_kind": [
                "THEORETICAL_FORWARD_RETURN",
                "EXECUTABLE_FORWARD_RETURN",
            ],
            "factor_ref": ["value", "value"],
            "horizon": [1, 1],
            "rank_ic_mean": [0.1, 0.2],
            "rank_ic_hac_p_value": [0.03, 0.04],
            "long_short_mean": [0.01, 0.02],
            "long_short_hac_p_value": [0.01, 0.06],
        }
    )
    tables = {
        "summary": summary,
        "coverage": pl.DataFrame({"coverage": [0.8, 1.0]}),
        "ic": pl.DataFrame({"pair_coverage": [0.6, 0.8]}),
    }

    metrics = _FactorPublisher.metrics(tables, MultipleTestingMethod.BONFERRONI)

    rank_names = sorted(name for name in metrics if name.startswith("rank_ic_mean/"))
    spread_names = sorted(
        name for name in metrics if name.startswith("long_short_mean/")
    )
    assert [metrics[name][3] for name in rank_names] == [0.08, 0.06]
    assert [metrics[name][3] for name in spread_names] == [0.12, 0.02]
    assert metrics["mean_factor_coverage"][0] == 0.9
    assert metrics["mean_pair_coverage"][0] == 0.7
    assert metrics["tested_rank_ic_count"][0] == 2.0
    assert metrics["significant_rank_ic_count"][0] == 0.0


def test_empty_security_status_keeps_executable_state_fixed_schema() -> None:
    session = _FactorStudySession(
        cast(Any, object()),
        cast(Any, _EmptyExecutableRepository()),
        cast(Any, object()),
        cast(Any, object()),
        Path("."),
    )

    state = session._executable_state((), date(2026, 1, 5), date(2026, 1, 6))

    assert state.is_empty()
    assert state.schema == {
        "instrument_id": pl.String,
        "trade_date": pl.Date,
        "is_listed": pl.Boolean,
        "is_suspended": pl.Boolean,
        "entry_limit_up": pl.Boolean,
    }
