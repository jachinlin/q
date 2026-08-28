"""验证无行业依赖的确定性截面因子变换。"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from quant_research.factors.transforms import (
    MIN_CROSS_SECTION_SIZE,
    neutralize_industry,
    neutralize_industry_market_cap,
    neutralize_market_cap,
    winsorize_mad,
    zscore,
)


def _frame(**columns: object) -> pl.DataFrame:
    return pl.DataFrame(columns)


def _assert_audit_schema(result: pl.DataFrame) -> None:
    assert result.schema["value"] == pl.Float64
    assert result.schema["is_valid"] == pl.Boolean
    assert result.schema["invalid_reason"] == pl.String


def test_winsorize_mad_matches_explicit_numpy_reference_and_preserves_order() -> None:
    source = _frame(
        bucket=["b", "a", "a", "a", "a", "a", "b", "b", "b"],
        value=[10.0, 100.0, 1.0, 2.0, 4.0, None, 10.0, 10.0, 10.0],
        label=list("ABCDEFGHI"),
    )
    original = source.clone()

    result = winsorize_mad(source, "value", ["bucket"], n_mad=2.0)

    values = np.array([100.0, 1.0, 2.0, 4.0])
    median = np.median(values)
    mad = np.median(np.abs(values - median))
    expected = np.clip(values, median - 2.0 * mad, median + 2.0 * mad)
    assert result["label"].to_list() == list("ABCDEFGHI")
    assert result["value"].to_list() == [
        10.0,
        *expected.tolist(),
        None,
        10.0,
        10.0,
        10.0,
    ]
    assert result["invalid_reason"].to_list()[5] == "MISSING_VALUE"
    assert source.equals(original)
    _assert_audit_schema(result)


def test_winsorize_mad_preserves_zero_mad_and_existing_invalid_rows() -> None:
    source = _frame(
        group=["x", "x", "x", "x"],
        value=[5.0, 5.0, 50.0, 7.0],
        is_valid=[True, True, True, False],
        invalid_reason=[None, None, None, "UPSTREAM_REJECTED"],
    )

    result = winsorize_mad(source, "value", ["group"])

    assert result["value"].to_list() == [5.0, 5.0, 50.0, None]
    assert result["invalid_reason"].to_list() == [None, None, None, "UPSTREAM_REJECTED"]


def test_zscore_matches_population_reference_and_invalidates_constant_group() -> None:
    source = _frame(
        group=["b", "a", "a", "a", "a", "b", "b", "b"],
        value=[9.0, 1.0, 2.0, 4.0, 7.0, 9.0, 9.0, None],
        marker=list("ABCDEFGH"),
    )

    result = zscore(source, "value", ["group"])

    values = np.array([1.0, 2.0, 4.0, 7.0])
    expected = (values - np.mean(values)) / np.std(values, ddof=0)
    assert result["marker"].to_list() == list("ABCDEFGH")
    assert result["value"].to_list()[1:5] == pytest.approx(expected.tolist(), abs=1e-12)
    assert result["invalid_reason"].to_list() == [
        "ZERO_VARIANCE",
        None,
        None,
        None,
        None,
        "ZERO_VARIANCE",
        "ZERO_VARIANCE",
        "MISSING_VALUE",
    ]
    _assert_audit_schema(result)


def test_cross_section_below_exported_threshold_is_invalid() -> None:
    source = _frame(group=["x", "x", "y", "y"], value=[1.0, 2.0, 9.0, 10.0])

    result = zscore(source, "value", ["group"])

    assert MIN_CROSS_SECTION_SIZE == 3
    assert result["value"].to_list() == [None] * 4
    assert result["invalid_reason"].to_list() == ["INSUFFICIENT_CROSS_SECTION"] * 4


def test_nonfinite_and_invalid_values_never_reenter_cross_section() -> None:
    source = _frame(
        group=["a"] * 7,
        value=[1.0, 2.0, 4.0, None, float("nan"), float("inf"), 7.0],
        is_valid=[True, True, True, True, True, True, False],
        invalid_reason=[None, None, None, None, None, None, "UPSTREAM_REJECTED"],
    )

    result = zscore(source, "value", ["group"])

    reference = np.array([1.0, 2.0, 4.0])
    expected = (reference - reference.mean()) / reference.std(ddof=0)
    assert result["value"].to_list()[:3] == pytest.approx(expected.tolist(), abs=1e-12)
    assert result["invalid_reason"].to_list()[3:] == [
        "MISSING_VALUE",
        "NONFINITE_VALUE",
        "NONFINITE_VALUE",
        "UPSTREAM_REJECTED",
    ]


def test_zscore_handles_extreme_finite_values_without_overflow() -> None:
    source = _frame(group=["a", "a", "a"], value=[-1e308, 0.0, 1e308])

    result = zscore(source, "value", ["group"])

    np.testing.assert_allclose(
        result["value"].to_numpy(),
        np.array([-np.sqrt(1.5), 0.0, np.sqrt(1.5)]),
        rtol=0.0,
        atol=1e-12,
    )


def test_winsorize_mad_uses_scaled_even_sample_median_and_bounds() -> None:
    source = _frame(
        group=["a"] * 4,
        value=[1.5e308, 1.6e308, 1.7e308, 1.79e308],
    )

    result = winsorize_mad(source, "value", ["group"], n_mad=1.0)

    values = np.asarray(source["value"].to_list(), dtype=np.float64)
    scale = np.abs(values).max()
    scaled = values / scale
    median = np.median(scaled)
    mad = np.median(np.abs(scaled - median))
    expected = np.clip(scaled, median - mad, median + mad) * scale
    np.testing.assert_allclose(
        result["value"].to_numpy(), expected, rtol=0.0, atol=1e292
    )


def test_empty_frame_receives_auditable_transform_columns() -> None:
    source = pl.DataFrame(schema={"group": pl.String, "value": pl.Float64})

    result = winsorize_mad(source, "value", ["group"])

    assert result.height == 0
    _assert_audit_schema(result)


def test_industry_neutralization_uses_equal_weight_group_means() -> None:
    source = _frame(
        signal_date=["2026-01-05"] * 6,
        factor_id=["value"] * 6,
        industry=["A", "A", "A", "B", "B", "B"],
        value=[1.0, 2.0, 6.0, -4.0, 2.0, 8.0],
        marker=list("ABCDEF"),
    )

    result = neutralize_industry(
        source, "value", "industry", ("signal_date", "factor_id")
    )

    np.testing.assert_allclose(
        result["value"].to_numpy(),
        np.array([-2.0, -1.0, 3.0, -6.0, 0.0, 6.0]),
        atol=1e-12,
    )
    grouped = result.group_by("industry").agg(pl.col("value").mean()).sort("industry")
    assert grouped["value"].to_list() == pytest.approx([0.0, 0.0], abs=1e-12)
    assert result["marker"].to_list() == list("ABCDEF")
    assert result["is_valid"].to_list() == [True] * 6


def test_industry_neutralization_preserves_upstream_invalidity_and_rejects_singletons() -> (
    None
):
    source = _frame(
        day=[1] * 7,
        industry=["A", "A", "B", "B", "C", None, "D"],
        value=[1.0, 3.0, 8.0, float("inf"), 4.0, 5.0, 9.0],
        is_valid=[True, True, True, True, True, True, False],
        invalid_reason=[None, None, None, None, None, None, "UPSTREAM_REJECTED"],
    )

    result = neutralize_industry(source, "value", "industry", ("day",))

    assert result["value"].to_list() == [-1.0, 1.0, None, None, None, None, None]
    assert result["invalid_reason"].to_list() == [
        None,
        None,
        "SINGLE_MEMBER_INDUSTRY",
        "NONFINITE_VALUE",
        "SINGLE_MEMBER_INDUSTRY",
        "MISSING_INDUSTRY",
        "UPSTREAM_REJECTED",
    ]


def test_market_cap_neutralization_matches_literal_ols_residuals() -> None:
    source = _frame(
        day=[1] * 4,
        value=[1.0, 4.0, 2.0, 8.0],
        total_market_value=[1.0, np.e, np.e**2, np.e**3],
        marker=list("ABCD"),
    )

    result = neutralize_market_cap(
        source, "value", "total_market_value", ("day",)
    )

    x = np.arange(4, dtype=np.float64)
    y = np.array([1.0, 4.0, 2.0, 8.0])
    slope = np.sum((x - x.mean()) * (y - y.mean())) / np.sum(
        (x - x.mean()) ** 2
    )
    expected = y - y.mean() - slope * (x - x.mean())
    np.testing.assert_allclose(result["value"].to_numpy(), expected, atol=1e-12)
    assert result["marker"].to_list() == list("ABCD")
    assert result["is_valid"].to_list() == [True] * 4


def test_joint_neutralization_matches_industry_fixed_effect_reference() -> None:
    source = _frame(
        day=[1] * 6,
        industry=["A", "A", "A", "B", "B", "B"],
        value=[1.0, 5.0, 4.0, 10.0, 8.0, 15.0],
        total_market_value=[1.0, np.e, np.e**2, np.e, np.e**2, np.e**3],
    )

    result = neutralize_industry_market_cap(
        source,
        "value",
        "total_market_value",
        "industry",
        ("day",),
    )

    x = np.log(np.asarray(source["total_market_value"].to_list()))
    y = np.asarray(source["value"].to_list())
    industries = source["industry"].to_list()
    x_centered = np.empty(6)
    y_centered = np.empty(6)
    for industry in ("A", "B"):
        mask = np.asarray([item == industry for item in industries])
        x_centered[mask] = x[mask] - x[mask].mean()
        y_centered[mask] = y[mask] - y[mask].mean()
    slope = np.sum(x_centered * y_centered) / np.sum(x_centered**2)
    expected = y_centered - slope * x_centered
    np.testing.assert_allclose(result["value"].to_numpy(), expected, atol=1e-12)


def test_market_cap_neutralization_has_stable_invalid_reasons() -> None:
    source = _frame(
        day=[1] * 8,
        value=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, float("inf"), 8.0],
        total_market_value=[1.0, np.e, np.e**2, None, float("inf"), 0.0, 9.0, -1.0],
        is_valid=[True, True, True, True, True, True, True, False],
        invalid_reason=[None, None, None, None, None, None, None, "UPSTREAM"],
    )

    result = neutralize_market_cap(
        source, "value", "total_market_value", ("day",)
    )

    assert result["invalid_reason"].to_list()[3:] == [
        "MISSING_MARKET_CAP",
        "NONFINITE_MARKET_CAP",
        "NONPOSITIVE_MARKET_CAP",
        "NONFINITE_VALUE",
        "UPSTREAM",
    ]


def test_joint_neutralization_rejects_singletons_and_zero_exposure_variance() -> None:
    singleton = _frame(
        day=[1] * 4,
        industry=["A", "A", "A", "B"],
        value=[1.0, 2.0, 3.0, 4.0],
        total_market_value=[1.0, np.e, np.e**2, np.e**3],
    )
    constant = _frame(
        day=[1] * 4,
        value=[1.0, 2.0, 3.0, 4.0],
        total_market_value=[10.0] * 4,
    )

    singleton_result = neutralize_industry_market_cap(
        singleton, "value", "total_market_value", "industry", ("day",)
    )
    constant_result = neutralize_market_cap(
        constant, "value", "total_market_value", ("day",)
    )

    assert singleton_result["invalid_reason"].to_list()[-1] == "SINGLE_MEMBER_INDUSTRY"
    assert constant_result["invalid_reason"].to_list() == [
        "ZERO_MARKET_CAP_VARIANCE"
    ] * 4


@pytest.mark.parametrize(
    ("function", "args"),
    [
        (winsorize_mad, ("missing", ["group"])),
        (zscore, ("value", ["group", "group"])),
    ],
)
def test_transforms_reject_missing_or_duplicate_columns(
    function: object, args: tuple[object, ...]
) -> None:
    source = _frame(group=["a"] * 3, value=[1.0, 2.0, 3.0])

    with pytest.raises(ValueError, match="column|duplicate"):
        function(source, *args)  # type: ignore[operator]


@pytest.mark.parametrize(
    ("function", "args"),
    [
        (winsorize_mad, ("value", ["is_valid"])),
        (zscore, ("value", ["invalid_reason"])),
        (zscore, ("is_valid", ["group"])),
    ],
)
def test_transforms_reject_reserved_audit_columns(
    function: object, args: tuple[object, ...]
) -> None:
    source = _frame(
        group=["a"] * 3,
        value=[1.0, 2.0, 3.0],
        is_valid=[True] * 3,
        invalid_reason=[None] * 3,
    )

    with pytest.raises(ValueError, match="reserved"):
        function(source, *args)  # type: ignore[operator]


@pytest.mark.parametrize("transform", [winsorize_mad, zscore])
def test_group_transforms_reject_nested_group_dtype(transform: object) -> None:
    source = pl.DataFrame(
        {"group": [["a"], ["a"], ["a"]], "value": [1.0, 2.0, 3.0]},
        schema={"group": pl.List(pl.String), "value": pl.Float64},
    )

    with pytest.raises(ValueError, match="unsupported group dtype"):
        transform(source, "value", ["group"])  # type: ignore[operator]


def test_transforms_reject_boolean_values_and_invalid_multiplier() -> None:
    boolean_values = _frame(group=["a"] * 3, value=[True, False, True])
    numeric_values = _frame(group=["a"] * 3, value=[1.0, 2.0, 3.0])

    with pytest.raises(TypeError, match="numeric"):
        winsorize_mad(boolean_values, "value", ["group"])
    with pytest.raises(ValueError, match="n_mad"):
        winsorize_mad(numeric_values, "value", ["group"], n_mad=0.0)
    with pytest.raises(ValueError, match="n_mad"):
        winsorize_mad(numeric_values, "value", ["group"], n_mad=10**400)
