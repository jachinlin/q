"""Validated eligibility controls for historical stock universes."""

from dataclasses import dataclass
from math import isfinite

from quant_core.domain.enums import Board


@dataclass(frozen=True, slots=True)
class UniverseRules:
    """Immutable rules used to decide a snapshot's eligible instruments."""

    min_listing_days: int = 120
    allowed_boards: frozenset[Board] = frozenset(
        {Board.MAIN, Board.CHINEXT, Board.STAR}
    )
    exclude_st: bool = True
    exclude_suspended: bool = True
    min_avg_amount_20d: float | None = None

    def __post_init__(self) -> None:
        if type(self.min_listing_days) is not int or self.min_listing_days < 0:
            raise ValueError("min_listing_days must be a nonnegative integer")
        try:
            allowed_boards = frozenset(self.allowed_boards)
        except TypeError as error:
            raise ValueError(
                "allowed_boards must be an iterable of Board values"
            ) from error
        if not allowed_boards:
            raise ValueError("allowed_boards must not be empty")
        if not all(isinstance(board, Board) for board in allowed_boards):
            raise ValueError("allowed_boards must contain Board values")
        if type(self.exclude_st) is not bool:
            raise ValueError("exclude_st must be a bool")
        if type(self.exclude_suspended) is not bool:
            raise ValueError("exclude_suspended must be a bool")
        if self.min_avg_amount_20d is not None and (
            type(self.min_avg_amount_20d) not in {int, float}
            or not isfinite(self.min_avg_amount_20d)
            or self.min_avg_amount_20d < 0
        ):
            raise ValueError("min_avg_amount_20d must be finite and nonnegative")
        object.__setattr__(self, "allowed_boards", allowed_boards)
        if self.min_avg_amount_20d is not None:
            object.__setattr__(
                self, "min_avg_amount_20d", float(self.min_avg_amount_20d)
            )
