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
        if isinstance(self.min_listing_days, bool) or self.min_listing_days < 0:
            raise ValueError("min_listing_days must be a nonnegative integer")
        if not self.allowed_boards:
            raise ValueError("allowed_boards must not be empty")
        if not all(isinstance(board, Board) for board in self.allowed_boards):
            raise ValueError("allowed_boards must contain Board values")
        if self.min_avg_amount_20d is not None and (
            not isfinite(self.min_avg_amount_20d) or self.min_avg_amount_20d < 0
        ):
            raise ValueError("min_avg_amount_20d must be finite and nonnegative")
