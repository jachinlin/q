"""验证独立 FactorStudy Worker 的输入适配语义。"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import polars as pl

from quant_research.backtest.rulebook import AShareRuleBook
from quant_research.bootstrap.worker import _FactorPublisher, _FactorStudySession
from quant_research.domain.enums import MultipleTestingMethod
from quant_research.domain.identifiers import InstrumentId
from quant_research.factor_studies.models import (
    FactorIndustrySettings,
    FactorMarketCapSettings,
    FactorStudyDefinition,
    FactorStudyUniverse,
    IndustryUnclassifiedPolicy,
)
from quant_research.factor_studies.streaming import StreamingForwardReturnBuilder
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


class _SignalRepository(_IndustryRepository):
    """同时返回固定行业和 PIT 市值输入。"""

    def __init__(self, industry: pl.DataFrame, basics: pl.DataFrame) -> None:
        super().__init__(industry)
        self._basics = basics

    def stock_daily_basics(
        self, _: object, __: date, ___: date
    ) -> pl.LazyFrame:
        """返回固定日频市值输入。"""
        return self._basics.lazy()


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


class _ExecutableRepository:
    """提供可核对真实上市序号与精确涨停价的最小行情。"""

    def __init__(
        self,
        listing: date,
        sessions: tuple[date, ...],
        price: float,
        *,
        preclose: float,
        warned: bool = False,
    ) -> None:
        self._listing = listing
        self._sessions = sessions
        self._price = price
        self._preclose = preclose
        self._warned = warned

    def stocks(self) -> pl.LazyFrame:
        """返回固定主板股票。"""
        return pl.DataFrame(
            {
                "instrument_id": ["000001.SZ"],
                "board": ["MAIN"],
                "list_date": [self._listing],
                "delist_date": pl.Series([None], dtype=pl.Date),
            }
        ).lazy()

    def stock_bars(self, _: object, start: date, end: date) -> pl.LazyFrame:
        """返回请求区间内固定为涨停价的最低价。"""
        selected = [day for day in self._sessions if start <= day <= end]
        return pl.DataFrame(
            {
                "instrument_id": ["000001.SZ"] * len(selected),
                "trade_date": selected,
                "low": [self._price] * len(selected),
                "preclose": [self._preclose] * len(selected),
            },
            schema={
                "instrument_id": pl.String,
                "trade_date": pl.Date,
                "low": pl.Float64,
                "preclose": pl.Float64,
            },
        ).lazy()

    def stock_suspensions(self, _: date, __: date, ___: object) -> pl.LazyFrame:
        """返回空停牌状态。"""
        return pl.DataFrame(
            schema={"instrument_id": pl.String, "trade_date": pl.Date}
        ).lazy()

    def stock_risk_warnings(self, _: date, __: date, ___: object) -> pl.LazyFrame:
        """按配置返回风险警示状态。"""
        if not self._warned:
            return pl.DataFrame(
                schema={"instrument_id": pl.String, "trade_date": pl.Date}
            ).lazy()
        return pl.DataFrame(
            {
                "instrument_id": ["000001.SZ"] * len(self._sessions),
                "trade_date": self._sessions,
            },
            schema={"instrument_id": pl.String, "trade_date": pl.Date},
        ).lazy()

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        """返回覆盖上市日起真实序号的工作日历。"""
        values: list[date] = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                values.append(current)
            current += timedelta(days=1)
        return pl.DataFrame(
            {"trade_date": values, "is_trading_day": [True] * len(values)},
            schema={"trade_date": pl.Date, "is_trading_day": pl.Boolean},
        ).lazy()


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

    excluded, exclude_coverage = session._signal_variants(
        factor_frame,
        eligible,
        (),
        (day,),
        config(IndustryUnclassifiedPolicy.EXCLUDE),
    )
    unclassified, unclassified_coverage = session._signal_variants(
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


def test_market_cap_and_joint_variants_use_daily_pit_inputs() -> None:
    day = date(2026, 1, 5)
    instruments = [f"00000{index}.SZ" for index in range(1, 7)]
    factor_frame = pl.DataFrame(
        {
            "signal_date": [day] * 6,
            "instrument_id": instruments,
            "factor_id": ["value"] * 6,
            "value": [1.0, 4.0, 2.0, 8.0, 3.0, 9.0],
            "is_valid": [True] * 6,
            "invalid_reason": pl.Series([None] * 6, dtype=pl.String),
            "signal_variant": ["DIRECTION_ADJUSTED"] * 6,
        }
    )
    eligible = factor_frame.select("signal_date", "instrument_id").with_columns(
        pl.lit(True).alias("eligible")
    )
    basics = pl.DataFrame(
        {
            "trade_date": [day] * 6,
            "instrument_id": instruments,
            "total_market_value": [1.0, 2.0, 4.0, 2.0, 4.0, 8.0],
            "available_at": [
                datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
                datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
                datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
                datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
                datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
                datetime(2026, 1, 6, 10, 0, tzinfo=UTC),
            ],
            "pit_usable": [True] * 6,
        }
    )
    industry = pl.DataFrame(
        {
            "query_date": [day] * 6,
            "instrument_id": instruments,
            "level1_code": ["A", "A", "A", "B", "B", "B"],
        }
    )
    session = _FactorStudySession(
        cast(Any, object()),
        cast(Any, _SignalRepository(industry, basics)),
        cast(Any, object()),
        cast(Any, object()),
        Path("."),
    )

    def config(*, with_industry: bool) -> FactorStudyDefinition:
        return FactorStudyDefinition(
            name="market cap",
            start_date=day,
            end_date=day,
            correction=MultipleTestingMethod.BH_FDR,
            factor_ids=("value",),
            universe=FactorStudyUniverse(name="CN_STOCK_STANDARD"),
            horizons=(1,),
            industry=(
                FactorIndustrySettings(
                    taxonomy="SW2021",
                    unclassified_policy=IndustryUnclassifiedPolicy.EXCLUDE,
                )
                if with_industry
                else None
            ),
            market_cap=FactorMarketCapSettings(
                exposure="LOG_TOTAL_MARKET_VALUE"
            ),
        )

    universe = tuple(InstrumentId.parse(item) for item in instruments)
    market, _ = session._signal_variants(
        factor_frame, eligible, universe, (day,), config(with_industry=False)
    )
    joint, coverage = session._signal_variants(
        factor_frame, eligible, universe, (day,), config(with_industry=True)
    )

    market_variant = market.filter(
        pl.col("signal_variant") == "MARKET_CAP_NEUTRALIZED"
    )
    assert market_variant["invalid_reason"].to_list()[-1] == "MISSING_MARKET_CAP"
    assert set(joint["signal_variant"].to_list()) == {
        "DIRECTION_ADJUSTED",
        "INDUSTRY_MARKET_CAP_NEUTRALIZED",
    }
    assert coverage["usable_count"].item() == 6


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


def test_executable_state_uses_global_listing_sessions_and_exact_half_up() -> None:
    """老股在窗口首日仍须按精确 HALF_UP 上限识别一字涨停。"""
    sessions = (date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7))
    repository = _ExecutableRepository(
        date(2025, 12, 1), sessions, 1.05, preclose=0.95
    )
    session = _FactorStudySession(
        cast(Any, object()),
        cast(Any, repository),
        cast(Any, object()),
        AShareRuleBook.load(Path("configs/rules/a_share.yaml")),
        Path("."),
    )

    state = session._executable_state(
        (InstrumentId.parse("000001.SZ"),), sessions[0], sessions[-1]
    )

    assert state["entry_limit_up"].to_list() == [True, True, True]
    adjusted_bars = pl.DataFrame(
        {
            "instrument_id": ["000001.SZ"] * 3,
            "trade_date": sessions,
            "open": [1.05, 1.05, 1.05],
            "close": [1.05, 1.05, 1.05],
        }
    )
    labels = StreamingForwardReturnBuilder(
        adjusted_bars,
        sessions,
        pl.DataFrame(
            {
                "signal_date": [sessions[0]],
                "instrument_id": ["000001.SZ"],
                "eligible": [True],
            }
        ),
        state,
    ).build(2)
    assert labels["executable_invalid_reason"].item() == "ENTRY_LIMIT_UP"


def test_executable_state_exempts_only_first_five_true_listing_sessions() -> None:
    """上市初期豁免必须以真实上市日起的交易日序号为准。"""
    sessions = tuple(
        day
        for offset in range(8)
        if (day := date(2026, 1, 1) + timedelta(days=offset)).weekday() < 5
    )
    repository = _ExecutableRepository(
        sessions[0], sessions, 11.0, preclose=10.0
    )
    session = _FactorStudySession(
        cast(Any, object()),
        cast(Any, repository),
        cast(Any, object()),
        AShareRuleBook.load(Path("configs/rules/a_share.yaml")),
        Path("."),
    )

    state = session._executable_state(
        (InstrumentId.parse("000001.SZ"),), sessions[0], sessions[-1]
    )

    assert state["entry_limit_up"].to_list() == [False] * 5 + [True]


def test_executable_state_uses_exact_st_half_up_boundary() -> None:
    """ST 的 1.90×105% 半分边界必须舍入为 2.00 并识别一字板。"""
    sessions = (date(2026, 1, 5),)
    repository = _ExecutableRepository(
        date(2025, 1, 1),
        sessions,
        2.0,
        preclose=1.9,
        warned=True,
    )
    session = _FactorStudySession(
        cast(Any, object()),
        cast(Any, repository),
        cast(Any, object()),
        AShareRuleBook.load(Path("configs/rules/a_share.yaml")),
        Path("."),
    )

    state = session._executable_state(
        (InstrumentId.parse("000001.SZ"),), sessions[0], sessions[0]
    )

    assert state["entry_limit_up"].item() is True
