from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl

from quant_research.data.quality.models import QualityRuleStatus
from quant_research.data.quality.rules import coverage_issues, required_value_issues
from quant_research.data.quality.runner import QualityRunner
from quant_research.domain.enums import DatasetKind


def _daily_basic(*, turnover: float | None) -> pl.DataFrame:
    now = datetime(2026, 8, 13, tzinfo=UTC)
    return pl.DataFrame(
        {
            "instrument_id": ["600000.SH"],
            "trade_date": [date(2026, 8, 13)],
            "turnover": [turnover],
            "source": ["baostock"],
            "available_at": [now],
            "availability_source": ["trade_date"],
            "pit_usable": [True],
            "ingested_at": [now],
        },
        schema={
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "turnover": pl.Float64,
            "source": pl.String,
            "available_at": pl.Datetime("us", "UTC"),
            "availability_source": pl.String,
            "pit_usable": pl.Boolean,
            "ingested_at": pl.Datetime("us", "UTC"),
        },
    )


def _security_status(*, suspended: bool) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "instrument_id": ["600000.SH"],
            "trade_date": [date(2026, 8, 13)],
            "is_suspended": [suspended],
        },
        schema={
            "instrument_id": pl.String,
            "trade_date": pl.Date,
            "is_suspended": pl.Boolean,
        },
    )


def test_daily_basic_allows_missing_turnover_on_confirmed_suspension() -> None:
    issues = required_value_issues(
        {
            DatasetKind.DAILY_BASIC: (_daily_basic(turnover=None),),
            DatasetKind.SECURITY_STATUS: (_security_status(suspended=True),),
        }
    )

    assert issues == []


def test_daily_basic_requires_turnover_when_security_is_not_suspended() -> None:
    (issue,) = required_value_issues(
        {
            DatasetKind.DAILY_BASIC: (_daily_basic(turnover=None),),
            DatasetKind.SECURITY_STATUS: (_security_status(suspended=False),),
        }
    )

    assert issue.rule_id == "required_value_null"
    assert issue.dataset is DatasetKind.DAILY_BASIC
    assert issue.actual == 1


def test_daily_basic_requires_turnover_when_security_status_is_missing() -> None:
    (issue,) = required_value_issues(
        {DatasetKind.DAILY_BASIC: (_daily_basic(turnover=None),)}
    )

    assert issue.actual == 1


def test_daily_bar_coverage_ignores_calendar_dates_after_latest_bar() -> None:
    issues = coverage_issues(
        {
            DatasetKind.DAILY_BAR: (
                pl.DataFrame(
                    {
                        "instrument_id": ["600000.SH", "600000.SH"],
                        "trade_date": [date(2026, 8, 13), date(2026, 8, 14)],
                    }
                ),
            ),
            DatasetKind.TRADE_CALENDAR: (
                pl.DataFrame(
                    {
                        "trade_date": [
                            date(2026, 8, 13),
                            date(2026, 8, 14),
                            date(2026, 8, 17),
                        ],
                        "is_trading_day": [True, True, True],
                    }
                ),
            ),
        }
    )

    assert issues == []


def test_daily_bar_coverage_reports_missing_date_before_latest_bar() -> None:
    (issue,) = coverage_issues(
        {
            DatasetKind.DAILY_BAR: (
                pl.DataFrame(
                    {
                        "instrument_id": ["600000.SH", "600000.SH"],
                        "trade_date": [date(2026, 8, 12), date(2026, 8, 14)],
                    }
                ),
            ),
            DatasetKind.TRADE_CALENDAR: (
                pl.DataFrame(
                    {
                        "trade_date": [
                            date(2026, 8, 12),
                            date(2026, 8, 13),
                            date(2026, 8, 14),
                            date(2026, 8, 17),
                        ],
                        "is_trading_day": [True, True, True, True],
                    }
                ),
            ),
        }
    )

    assert issue.rule_id == "trading_day_coverage"
    assert issue.dataset is DatasetKind.DAILY_BAR
    assert issue.actual == 1


def _trade_calendar(
    *, duplicate: bool = False, conforming: bool = True
) -> pl.DataFrame:
    now = datetime(2026, 8, 13, tzinfo=UTC)
    dates = [date(2026, 8, 13), date(2026, 8, 13)] if duplicate else [date(2026, 8, 13)]
    values: dict[str, object] = {
        "trade_date": dates,
        "is_trading_day": [True] * len(dates),
    }
    if conforming:
        values.update(
            {
                "source": ["test"] * len(dates),
                "available_at": [now] * len(dates),
                "availability_source": ["calendar"] * len(dates),
                "pit_usable": [True] * len(dates),
                "ingested_at": [now] * len(dates),
            }
        )
    return pl.DataFrame(values)


def test_quality_runner_records_every_applicable_rule_as_pass() -> None:
    evaluation = QualityRunner().evaluate(
        {DatasetKind.TRADE_CALENDAR: (_trade_calendar(),)}
    )

    assert evaluation.issues == ()
    assert evaluation.rule_results
    assert {item.status for item in evaluation.rule_results} == {QualityRuleStatus.PASS}
    assert tuple(item.rule_id for item in evaluation.rule_results) == (
        "required_dataset_missing",
        "required_dataset_empty",
        "canonical_schema",
        "trading_window_empty",
        "cross_partition_schema",
        "primary_key_duplicate",
        "required_value_null",
    )


def test_quality_runner_records_fail_and_schema_dependency_skips() -> None:
    duplicate = QualityRunner().evaluate(
        {DatasetKind.TRADE_CALENDAR: (_trade_calendar(duplicate=True),)}
    )
    malformed = QualityRunner().evaluate(
        {DatasetKind.TRADE_CALENDAR: (_trade_calendar(conforming=False),)}
    )

    duplicate_result = next(
        item
        for item in duplicate.rule_results
        if item.rule_id == "primary_key_duplicate"
    )
    assert duplicate_result.status is QualityRuleStatus.FAIL
    assert duplicate_result.actual == 1
    malformed_by_rule = {item.rule_id: item for item in malformed.rule_results}
    assert malformed_by_rule["canonical_schema"].status is QualityRuleStatus.FAIL
    assert malformed_by_rule["required_value_null"].status is QualityRuleStatus.SKIPPED
    assert "canonical_schema" in (
        malformed_by_rule["required_value_null"].skip_reason or ""
    )
