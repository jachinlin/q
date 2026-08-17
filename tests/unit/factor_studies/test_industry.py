"""验证因子研究 PIT 行业对齐和中性化契约。"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from pydantic import ValidationError

from quant_research.application.factor_studies import _FactorIndustrySupport
from quant_research.factor_studies.models import (
    DIRECTION_ADJUSTED,
    INDUSTRY_NEUTRALIZED,
    FactorStudyConfig,
    FactorStudyIndustryConfig,
)


def _inputs() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    day = date(2026, 8, 4)
    instruments = [f"00000{index}.SZ" for index in range(1, 6)]
    eligible = pl.DataFrame(
        {
            "signal_date": [day] * 5,
            "instrument_id": instruments,
            "eligible": [True] * 5,
        }
    )
    factors = pl.DataFrame(
        {
            "signal_date": [day] * 5,
            "instrument_id": instruments,
            "factor_id": ["roe_pit"] * 5,
            "value": [1.0, 3.0, 10.0, 4.0, 8.0],
            "is_valid": [True] * 5,
        }
    )
    classifications = pl.DataFrame(
        {
            "query_date": [day] * 4,
            "instrument_id": instruments[:4],
            "taxonomy": ["证监会行业分类"] * 4,
            "industry_code": ["A", "A", "B", None],
            "is_classified": [True, True, True, False],
        },
        schema={
            "query_date": pl.Date,
            "instrument_id": pl.String,
            "taxonomy": pl.String,
            "industry_code": pl.String,
            "is_classified": pl.Boolean,
        },
    )
    return factors, eligible, classifications


def test_exclude_policy_discloses_tombstone_and_missing_state() -> None:
    factors, eligible, classifications = _inputs()

    result, coverage, evidence = _FactorIndustrySupport.build(
        factors=factors,
        eligible=eligible,
        classifications=classifications,
        config=FactorStudyIndustryConfig(
            taxonomy="证监会行业分类", unclassified_policy="EXCLUDE"
        ),
    )

    assert result["signal_variant"].unique().to_list() == [INDUSTRY_NEUTRALIZED]
    assert result["value"].to_list() == [-1.0, 1.0, None, None, None]
    assert result["invalid_reason"].to_list() == [
        None,
        None,
        "SINGLE_MEMBER_INDUSTRY",
        "MISSING_INDUSTRY",
        "MISSING_INDUSTRY",
    ]
    assert coverage.schema == _FactorIndustrySupport._COVERAGE_SCHEMA
    assert coverage.select(
        "eligible_count",
        "classified_count",
        "tombstone_count",
        "missing_state_count",
        "usable_count",
        "classified_coverage",
        "usable_coverage",
    ).row(0) == pytest.approx((5, 3, 1, 1, 3, 0.6, 0.6))
    assert evidence["date_basis"] == "SIGNAL_DATE"
    assert evidence["availability_source"] == "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED"


def test_direction_adjusted_and_industry_neutralized_variants_share_schema() -> None:
    factors, eligible, classifications = _inputs()

    baseline = _FactorIndustrySupport.direction_adjusted(factors)
    neutralized, _, _ = _FactorIndustrySupport.build(
        factors=factors,
        eligible=eligible,
        classifications=classifications,
        config=FactorStudyIndustryConfig(
            taxonomy="证监会行业分类", unclassified_policy="EXCLUDE"
        ),
    )

    combined = pl.concat((baseline, neutralized), how="vertical")

    assert baseline.schema == neutralized.schema
    assert combined.height == factors.height * 2
    assert combined.group_by("signal_variant").len().sort("signal_variant").rows() == [
        (DIRECTION_ADJUSTED, factors.height),
        (INDUSTRY_NEUTRALIZED, factors.height),
    ]


def test_unclassified_policy_forms_one_stable_group() -> None:
    factors, eligible, classifications = _inputs()

    result, coverage, _ = _FactorIndustrySupport.build(
        factors=factors,
        eligible=eligible,
        classifications=classifications,
        config=FactorStudyIndustryConfig(
            taxonomy="证监会行业分类", unclassified_policy="UNCLASSIFIED"
        ),
    )

    assert result["value"].to_list() == [-1.0, 1.0, None, -2.0, 2.0]
    assert coverage["usable_count"].item() == 5
    assert coverage["usable_coverage"].item() == 1.0


def test_factor_study_industry_config_is_strict_and_changes_identity_payload() -> None:
    base = {
        "factor_refs": ("roe_pit",),
        "start_date": date(2026, 8, 4),
        "end_date": date(2026, 8, 4),
    }
    without = FactorStudyConfig(**base)
    with_industry = FactorStudyConfig(
        **base,
        industry=FactorStudyIndustryConfig(
            taxonomy="证监会行业分类", unclassified_policy="EXCLUDE"
        ),
    )

    assert without.model_dump(mode="json") != with_industry.model_dump(mode="json")
    with pytest.raises(ValidationError, match="taxonomy"):
        FactorStudyIndustryConfig(
            taxonomy="申万一级", unclassified_policy="EXCLUDE"
        )
    with pytest.raises(ValidationError, match="policy"):
        FactorStudyIndustryConfig(
            taxonomy="证监会行业分类", unclassified_policy="DROP"
        )
