"""定义 PIT 股票池构建时使用的证券准入规则。"""

from dataclasses import dataclass
from math import isfinite

from quant_research.domain.enums import Board


@dataclass(frozen=True, slots=True)
class UniverseRules:
    """定义单个交易日构建 PIT 股票池时使用的准入条件。

    入参：
        min_listing_days：证券获准进入股票池前至少经历的上市交易日数。
        allowed_boards：允许进入股票池的 A 股板块集合；集合不能为空。
        exclude_st：是否排除观察日带有 ST 风险警示的证券。
        exclude_suspended：是否排除观察日停牌的证券。
        min_avg_amount_20d：可选的最近 20 个交易日日均成交额下限；``None`` 表示不启用流动性门槛。
    返回值：
        返回经校验并规范化为不可变板块集合和浮点成交额门槛的规则对象。
    异常：
        ValueError：上市交易日数或成交额下限为负、成交额不是有限数、板块集合为空或含有
        非 ``Board`` 值，或两个排除开关不是布尔值时抛出。
    """

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
