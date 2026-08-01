"""Immutable constraint definitions for target portfolio construction."""

from dataclasses import dataclass
from math import isfinite


class ConstraintViolation(ValueError):
    """Raised when a requested portfolio violates a declared constraint."""

    constraint_name: str
    actual_value: float | int
    boundary: float | int | tuple[float | int, ...]

    def __init__(
        self,
        constraint_name: str,
        actual_value: float | int,  # noqa: PYI041
        boundary: float | int | tuple[float | int, ...],  # noqa: PYI041
    ) -> None:
        self.constraint_name = constraint_name
        self.actual_value = actual_value
        self.boundary = boundary
        super().__init__(
            f"{constraint_name}: actual_value={actual_value}, boundary={boundary}"
        )


@dataclass(frozen=True, slots=True)
class PortfolioConstraints:
    """Hard allocation, liquidity, and turnover limits for a target portfolio."""

    max_position_weight: float
    max_industry_weight: float
    min_positions: int
    max_positions: int
    min_adv_amount: float
    max_turnover: float

    def __post_init__(self) -> None:
        _require_probability("max_position_weight", self.max_position_weight, lower=0.0)
        _require_probability("max_industry_weight", self.max_industry_weight, lower=0.0)
        if not isinstance(self.min_positions, int) or isinstance(
            self.min_positions, bool
        ):
            raise TypeError("min_positions must be an integer")
        if self.min_positions <= 0:
            raise ValueError("min_positions must be greater than zero")
        if not isinstance(self.max_positions, int) or isinstance(
            self.max_positions, bool
        ):
            raise TypeError("max_positions must be an integer")
        if self.max_positions < self.min_positions:
            raise ValueError("max_positions must be at least min_positions")
        _require_finite_number("min_adv_amount", self.min_adv_amount)
        if self.min_adv_amount < 0:
            raise ValueError("min_adv_amount must be nonnegative")
        _require_finite_number("max_turnover", self.max_turnover)
        if self.max_turnover < 0 or self.max_turnover > 1:
            raise ValueError("max_turnover must be in [0, 1]")


def _require_probability(name: str, value: float, *, lower: float) -> None:
    _require_finite_number(name, value)
    if value <= lower or value > 1.0:
        raise ValueError(f"{name} must be in ({lower}, 1]")


def _require_finite_number(name: str, value: object) -> None:
    if not isinstance(value, (float, int)) or isinstance(value, bool):
        raise TypeError(f"{name} must be a finite number")
    if not isfinite(value):
        raise ValueError(f"{name} must be a finite number")
