"""Numerical contracts for deterministic cross-sectional factor transforms."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import polars as pl
import pytest

from quant_core.factors.transforms import (
    MIN_CROSS_SECTION_SIZE,
    neutralize_wls,
    winsorize_mad,
    zscore,
)


def _frame(**columns: object) -> pl.DataFrame:
    return pl.DataFrame(columns)


def _assert_audit_schema(result: pl.DataFrame) -> None:
    assert result.schema["value"] == pl.Float64
    assert result.schema["is_valid"] == pl.Boolean
    assert result.schema["invalid_reason"] == pl.String


def test_winsorize_mad_matches_explicit_numpy_reference_and_preserves_input_order() -> (
    None
):
    """Changing MAD clipping bounds or row restoration must fail this test."""
    source = _frame(
        bucket=["b", "a", "a", "a", "a", "a", "b", "b", "b"],
        value=[10.0, 100.0, 1.0, 2.0, 4.0, None, 10.0, 10.0, 10.0],
        label=list("ABCDEFGHI"),
    )
    original = source.clone()

    result = winsorize_mad(source, "value", ["bucket"], n_mad=2.0)

    a = np.array([100.0, 1.0, 2.0, 4.0])
    median = np.median(a)
    mad = np.median(np.abs(a - median))
    expected_a = np.clip(a, median - 2.0 * mad, median + 2.0 * mad)
    assert result["label"].to_list() == list("ABCDEFGHI")
    assert result.columns == ["bucket", "value", "label", "is_valid", "invalid_reason"]
    assert result["value"].to_list() == [
        10.0,
        *expected_a.tolist(),
        None,
        10.0,
        10.0,
        10.0,
    ]
    assert result["is_valid"].to_list() == [
        True,
        True,
        True,
        True,
        True,
        False,
        True,
        True,
        True,
    ]
    assert result["invalid_reason"].to_list() == [
        None,
        None,
        None,
        None,
        None,
        "MISSING_VALUE",
        None,
        None,
        None,
    ]
    assert source.equals(original)
    _assert_audit_schema(result)


def test_winsorize_mad_preserves_zero_mad_and_existing_invalid_rows() -> None:
    """Replacing a zero MAD group or reviving invalid input rows is a bug."""
    source = _frame(
        group=["x", "x", "x", "x"],
        value=[5.0, 5.0, 50.0, 7.0],
        is_valid=[True, True, True, False],
        invalid_reason=[None, None, None, "UPSTREAM_REJECTED"],
    )

    result = winsorize_mad(source, "value", ["group"])

    assert result["value"].to_list() == [5.0, 5.0, 50.0, None]
    assert result["is_valid"].to_list() == [True, True, True, False]
    assert result["invalid_reason"].to_list() == [None, None, None, "UPSTREAM_REJECTED"]


def test_zscore_matches_population_numpy_reference_and_invalidates_constant_group() -> (
    None
):
    """Using sample deviation or returning zero for a constant group must fail."""
    source = _frame(
        group=["b", "a", "a", "a", "a", "b", "b", "b"],
        value=[9.0, 1.0, 2.0, 4.0, 7.0, 9.0, 9.0, None],
        marker=list("ABCDEFGH"),
    )

    result = zscore(source, "value", ["group"])

    a = np.array([1.0, 2.0, 4.0, 7.0])
    expected_a = (a - np.mean(a)) / np.std(a, ddof=0)
    assert result["marker"].to_list() == list("ABCDEFGH")
    assert result["value"].to_list()[1:5] == pytest.approx(
        expected_a.tolist(), abs=1e-12
    )
    assert result["value"].to_list()[0] is None
    assert result["value"].to_list()[5:] == [None, None, None]
    assert result["is_valid"].to_list() == [
        False,
        True,
        True,
        True,
        True,
        False,
        False,
        False,
    ]
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


def test_cross_section_below_exported_threshold_is_invalid_not_synthetic_zero() -> None:
    """Lowering the minimum sample guard or substituting zero must fail."""
    source = _frame(group=["x", "x", "y", "y"], value=[1.0, 2.0, 9.0, 10.0])

    result = zscore(source, "value", ["group"])

    assert MIN_CROSS_SECTION_SIZE == 3
    assert result["value"].to_list() == [None, None, None, None]
    assert result["is_valid"].to_list() == [False, False, False, False]
    assert result["invalid_reason"].to_list() == ["INSUFFICIENT_CROSS_SECTION"] * 4


def test_nonfinite_and_previously_invalid_values_never_reenter_a_cross_section() -> (
    None
):
    """Accepting NaN, infinity, or false validity flags would contaminate statistics."""
    source = _frame(
        group=["a", "a", "a", "a", "a", "a", "a"],
        value=[1.0, 2.0, 4.0, None, float("nan"), float("inf"), 7.0],
        is_valid=[True, True, True, True, True, True, False],
        invalid_reason=[None, None, None, None, None, None, "UPSTREAM_REJECTED"],
    )

    result = zscore(source, "value", ["group"])

    reference = np.array([1.0, 2.0, 4.0])
    expected = (reference - reference.mean()) / reference.std(ddof=0)
    assert result["value"].to_list()[:3] == pytest.approx(expected.tolist(), abs=1e-12)
    assert result["value"].to_list()[3:] == [None, None, None, None]
    assert result["invalid_reason"].to_list()[3:] == [
        "MISSING_VALUE",
        "NONFINITE_VALUE",
        "NONFINITE_VALUE",
        "UPSTREAM_REJECTED",
    ]


def test_zscore_handles_extreme_finite_values_without_overflow() -> None:
    """Overflow while computing a finite group must become an auditable result."""
    source = _frame(group=["a", "a", "a"], value=[-1e308, 0.0, 1e308])

    result = zscore(source, "value", ["group"])

    np.testing.assert_allclose(
        result["value"].to_numpy(),
        np.array([-np.sqrt(1.5), 0.0, np.sqrt(1.5)]),
        rtol=0.0,
        atol=1e-12,
    )
    assert result["is_valid"].to_list() == [True, True, True]
    assert result["invalid_reason"].to_list() == [None, None, None]


def test_empty_frame_receives_auditable_empty_transform_columns() -> None:
    """An empty universe must retain a deterministic usable output schema."""
    source = pl.DataFrame(
        schema={"group": pl.String, "value": pl.Float64, "label": pl.String}
    )

    result = winsorize_mad(source, "value", ["group"])

    assert result.columns == ["group", "value", "label", "is_valid", "invalid_reason"]
    assert result.height == 0
    _assert_audit_schema(result)


def test_neutralize_wls_matches_explicit_numpy_design_matrix_with_stable_baseline() -> (
    None
):
    """Changing baseline, weights, intercept, or residual calculation must fail."""
    source = _frame(
        industry=[
            "Tech",
            "Banks",
            "Tech",
            "Utilities",
            "Banks",
            "Utilities",
            "Tech",
            "Banks",
        ],
        log_cap=[2.0, 1.0, 3.0, 1.5, 2.5, 2.2, 0.5, 3.5],
        value=[9.2, 2.5, 10.0, 6.5, 5.4, 7.9, 6.1, 8.4],
        instrument=["T1", "B1", "T2", "U1", "B2", "U2", "T3", "B3"],
    )
    original = source.clone()

    result = neutralize_wls(source, "value", "industry", "log_cap")

    industries = np.asarray(source["industry"].to_list())
    log_cap = np.asarray(source["log_cap"].to_list(), dtype=float)
    response = np.asarray(source["value"].to_list(), dtype=float)
    # Alphabetical baseline is Banks; remaining dummy columns are Tech then Utilities.
    design = np.column_stack(
        [
            np.ones(response.size),
            (industries == "Tech").astype(float),
            (industries == "Utilities").astype(float),
            log_cap,
        ]
    )
    weights = np.exp(log_cap - log_cap.max())
    weights /= weights.sum()
    beta = np.linalg.lstsq(
        design * np.sqrt(weights)[:, None],
        response * np.sqrt(weights),
        rcond=np.finfo(np.float64).eps * max(design.shape),
    )[0]
    expected = response - design @ beta
    assert np.linalg.matrix_rank(design) == design.shape[1]
    np.testing.assert_allclose(
        result["value"].to_numpy(), expected, rtol=0.0, atol=1e-12
    )
    assert result["is_valid"].to_list() == [True] * source.height
    assert result["invalid_reason"].to_list() == [None] * source.height
    assert result["instrument"].to_list() == source["instrument"].to_list()
    assert source.equals(original)
    _assert_audit_schema(result)


def test_neutralize_wls_matches_input_order_numpy_reference_at_large_scale() -> None:
    """Reordering rows internally must not alter the requested WLS residuals."""
    source = _frame(
        industry=[
            "Tech",
            "Banks",
            "Tech",
            "Utilities",
            "Banks",
            "Utilities",
            "Tech",
            "Banks",
        ],
        log_cap=[2.0, 1.0, 3.0, 1.5, 2.5, 2.2, 0.5, 3.5],
        value=[9.2e6, 2.5e6, 10e6, 6.5e6, 5.4e6, 7.9e6, 6.1e6, 8.4e6],
    )

    result = neutralize_wls(source, "value", "industry", "log_cap")

    industries = np.asarray(source["industry"].to_list())
    log_cap = np.asarray(source["log_cap"].to_list(), dtype=float)
    response = np.asarray(source["value"].to_list(), dtype=float)
    design = np.column_stack(
        [
            np.ones(response.size),
            (industries == "Tech").astype(float),
            (industries == "Utilities").astype(float),
            log_cap,
        ]
    )
    weights = np.exp(log_cap - log_cap.max())
    weights /= weights.sum()
    beta = np.linalg.lstsq(
        design * np.sqrt(weights)[:, None],
        response * np.sqrt(weights),
        rcond=np.finfo(np.float64).eps * max(design.shape),
    )[0]
    np.testing.assert_allclose(
        result["value"].to_numpy(), response - design @ beta, rtol=0.0, atol=1e-12
    )


def test_neutralize_wls_matches_max_shift_weight_reference() -> None:
    """A common finite log-cap shift must not be treated as invalid weight."""
    source = _frame(
        industry=[
            "Tech",
            "Banks",
            "Tech",
            "Utilities",
            "Banks",
            "Utilities",
            "Tech",
            "Banks",
        ],
        log_cap=[1002.0, 1001.0, 1003.0, 1001.5, 1002.5, 1002.2, 1000.5, 1003.5],
        value=[9.2, 2.5, 10.0, 6.5, 5.4, 7.9, 6.1, 8.4],
    )

    result = neutralize_wls(source, "value", "industry", "log_cap")

    industries = np.asarray(source["industry"].to_list())
    log_cap = np.asarray(source["log_cap"].to_list(), dtype=np.float64)
    response = np.asarray(source["value"].to_list(), dtype=np.float64)
    design = np.column_stack(
        [
            np.ones(response.size),
            (industries == "Tech").astype(float),
            (industries == "Utilities").astype(float),
            log_cap,
        ]
    )
    weights = np.exp(log_cap - log_cap.max())
    weights /= weights.sum()
    rcond = np.finfo(np.float64).eps * max(design.shape)
    beta, _, rank, _ = np.linalg.lstsq(
        design * np.sqrt(weights)[:, None], response * np.sqrt(weights), rcond=rcond
    )
    assert rank == design.shape[1]
    np.testing.assert_allclose(
        result["value"].to_numpy(), response - design @ beta, rtol=0.0, atol=1e-12
    )
    assert result["is_valid"].to_list() == [True] * source.height


def test_neutralize_wls_invalidates_actual_underflowed_weight() -> None:
    """A shifted exponent that is exactly zero must not enter weighted least squares."""
    source = _frame(
        industry=["Banks", "Tech", "Tech", "Banks"],
        log_cap=[1000.0, 1.0, 2.0, 3.0],
        value=[1.0, 2.0, 3.0, 4.0],
    )

    result = neutralize_wls(source, "value", "industry", "log_cap")

    assert result["value"].to_list() == [None] * source.height
    assert result["is_valid"].to_list() == [False] * source.height
    assert result["invalid_reason"].to_list() == ["INVALID_WEIGHT"] * source.height


def test_neutralize_wls_invalidates_weighted_rank_loss() -> None:
    """Full rank before weighting cannot mask rank loss in sqrt(W) times X."""
    source = _frame(
        industry=["Banks", "Banks", "Tech", "Tech"],
        log_cap=[0.0, -700.0, -700.0, -700.0],
        value=[1.0, 2.0, 3.0, 4.0],
    )
    industries = np.asarray(source["industry"].to_list())
    log_cap = np.asarray(source["log_cap"].to_list(), dtype=np.float64)
    design = np.column_stack(
        [np.ones(source.height), (industries == "Tech").astype(float), log_cap]
    )
    weights = np.exp(log_cap - log_cap.max())
    weights /= weights.sum()
    weighted_design = design * np.sqrt(weights)[:, None]
    rcond = np.finfo(np.float64).eps * max(weighted_design.shape)
    assert np.linalg.matrix_rank(design) == design.shape[1]
    assert (
        np.linalg.lstsq(weighted_design, np.arange(source.height), rcond=rcond)[2]
        < design.shape[1]
    )

    result = neutralize_wls(source, "value", "industry", "log_cap")

    assert result["value"].to_list() == [None] * source.height
    assert result["is_valid"].to_list() == [False] * source.height
    assert (
        result["invalid_reason"].to_list() == ["RANK_DEFICIENT_DESIGN"] * source.height
    )


def test_winsorize_mad_uses_scaled_even_sample_median_and_bounds() -> None:
    """Directly averaging same-sign finite extremes overflows the median and MAD bounds."""
    source = _frame(
        group=["a", "a", "a", "a"],
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
    assert result["is_valid"].to_list() == [True] * source.height


@pytest.mark.parametrize(
    ("industry", "log_cap", "reason"),
    [
        (
            ["Banks", "Banks", "Banks", "Banks"],
            [1.0, 1.0, 1.0, 1.0],
            "RANK_DEFICIENT_DESIGN",
        ),
        (
            ["Banks", "Tech", "Tech", "Banks"],
            [1.0, 1.0, 1.0, 1.0],
            "RANK_DEFICIENT_DESIGN",
        ),
    ],
)
def test_neutralize_wls_rejects_rank_deficiency_and_invalid_weights(
    industry: Sequence[str], log_cap: Sequence[float], reason: str
) -> None:
    """A singular or nonfinite weighted regression must never invent residuals."""
    source = _frame(industry=industry, log_cap=log_cap, value=[1.0, 2.0, 3.0, 4.0])

    result = neutralize_wls(source, "value", "industry", "log_cap")

    assert result["value"].to_list() == [None] * source.height
    assert result["is_valid"].to_list() == [False] * source.height
    assert result["invalid_reason"].to_list() == [reason] * source.height


def test_neutralize_wls_rejects_single_member_industry_without_residuals() -> None:
    """A one-observation industry dummy must not yield an apparently valid residual."""
    source = _frame(
        industry=["Banks", "Banks", "Tech", "Tech", "Utilities"],
        log_cap=[1.0, 2.0, 1.5, 2.5, 3.0],
        value=[1.0, 2.0, 3.0, 4.0, 5.0],
    )

    result = neutralize_wls(source, "value", "industry", "log_cap")

    assert result["value"].to_list() == [None] * source.height
    assert result["is_valid"].to_list() == [False] * source.height
    assert (
        result["invalid_reason"].to_list() == ["SINGLE_MEMBER_INDUSTRY"] * source.height
    )


@pytest.mark.parametrize(
    ("function", "args"),
    [
        (winsorize_mad, ("missing", ["group"])),
        (zscore, ("value", ["group", "group"])),
        (neutralize_wls, ("value", "industry", "industry")),
    ],
)
def test_transforms_reject_missing_or_duplicate_column_references(
    function: object, args: tuple[object, ...]
) -> None:
    """Accepting ambiguous or absent columns would make factor outputs unauditable."""
    source = _frame(
        group=["a", "a", "a"], industry=["I", "I", "I"], value=[1.0, 2.0, 3.0]
    )

    with pytest.raises(ValueError, match="column|duplicate"):
        function(source, *args)  # type: ignore[operator]


@pytest.mark.parametrize(
    ("function", "args"),
    [
        (winsorize_mad, ("value", ["is_valid"])),
        (zscore, ("value", ["invalid_reason"])),
        (neutralize_wls, ("value", "is_valid", "size")),
        (neutralize_wls, ("value", "industry", "invalid_reason")),
        (zscore, ("is_valid", ["group"])),
    ],
)
def test_transforms_reject_reserved_audit_columns_as_semantic_inputs(
    function: object, args: tuple[object, ...]
) -> None:
    """Audit output names cannot also select values, groups, industries, or sizes."""
    source = _frame(
        group=["a", "a", "a"],
        industry=["I", "I", "I"],
        size=[1.0, 2.0, 3.0],
        value=[1.0, 2.0, 3.0],
        is_valid=[True, True, True],
        invalid_reason=[None, None, None],
    )

    with pytest.raises(ValueError, match="reserved"):
        function(source, *args)  # type: ignore[operator]


def test_transforms_reject_boolean_values_and_invalid_mad_multiplier() -> None:
    """Treating booleans as numeric or accepting invalid clipping controls is unsafe."""
    boolean_values = _frame(group=["a", "a", "a"], value=[True, False, True])
    numeric_values = _frame(group=["a", "a", "a"], value=[1.0, 2.0, 3.0])

    with pytest.raises(TypeError, match="numeric"):
        winsorize_mad(boolean_values, "value", ["group"])
    with pytest.raises(ValueError, match="n_mad"):
        winsorize_mad(numeric_values, "value", ["group"], n_mad=0.0)


def test_winsorize_mad_rejects_huge_integer_multiplier_with_value_error() -> None:
    """Converting an arbitrarily large integer multiplier must not leak OverflowError."""
    source = _frame(group=["a", "a", "a"], value=[1.0, 2.0, 3.0])

    with pytest.raises(ValueError, match="n_mad"):
        winsorize_mad(source, "value", ["group"], n_mad=10**400)


@pytest.mark.parametrize("transform", [winsorize_mad, zscore])
def test_group_transforms_reject_nested_polars_group_dtype_stably(
    transform: object,
) -> None:
    """Nested keys are unhashable, so grouping must fail predictably before execution."""
    source = pl.DataFrame(
        {"group": [["a"], ["a"], ["a"]], "value": [1.0, 2.0, 3.0]},
        schema={"group": pl.List(pl.String), "value": pl.Float64},
    )

    with pytest.raises(ValueError, match="unsupported group dtype"):
        transform(source, "value", ["group"])  # type: ignore[operator]


def test_neutralize_wls_rejects_nested_industry_dtype_stably() -> None:
    """Nested industry labels cannot define deterministic WLS dummy columns."""
    source = pl.DataFrame(
        {
            "industry": [["A"], ["A"], ["A"], ["A"]],
            "size": [1.0, 2.0, 3.0, 4.0],
            "value": [1.0, 2.0, 3.0, 4.0],
        },
        schema={
            "industry": pl.List(pl.String),
            "size": pl.Float64,
            "value": pl.Float64,
        },
    )

    with pytest.raises(ValueError, match="unsupported industry dtype"):
        neutralize_wls(source, "value", "industry", "size")
