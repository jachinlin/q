"""提供组合构建与组合约束相关的公开模型、协议与处理流程。"""

from dataclasses import dataclass
from math import isfinite


class ConstraintViolation(ValueError):
    """表示 ``ConstraintViolation`` 对应的领域异常。

    入参：
        返回完成字段规范化和不变量校验的对象。
        返回完成字段规范化和不变量校验的对象。
        返回完成字段规范化和不变量校验的对象。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    Raised when a requested portfolio violates a declared constraint.
    """

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
    """定义目标组合判定使用的规则及其取值约束。

    入参：
        max_position_weight：限制资源使用、数量或等待时间的上限持仓权重。
        min_positions：判定输入或结果有效所需达到的下限持仓集合。
        max_positions：限制资源使用、数量或等待时间的上限持仓集合。
        返回完成字段规范化和不变量校验的对象。
        max_turnover：限制资源使用、数量或等待时间的上限换手率。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Hard allocation, liquidity, and turnover limits for a target portfolio.
    """

    max_position_weight: float
    min_positions: int
    max_positions: int
    min_adv_amount: float
    max_turnover: float

    def __post_init__(self) -> None:
        _ConstraintsSupport._require_probability(
            "max_position_weight", self.max_position_weight, lower=0.0
        )
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
        _ConstraintsSupport._require_finite_number(
            "min_adv_amount", self.min_adv_amount
        )
        if self.min_adv_amount < 0:
            raise ValueError("min_adv_amount must be nonnegative")
        _ConstraintsSupport._require_finite_number("max_turnover", self.max_turnover)
        if self.max_turnover < 0 or self.max_turnover > 1:
            raise ValueError("max_turnover must be in [0, 1]")


class _ConstraintsSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _require_probability(name: str, value: float, *, lower: float) -> None:
        _ConstraintsSupport._require_finite_number(name, value)
        if value <= lower or value > 1.0:
            raise ValueError(f"{name} must be in ({lower}, 1]")

    @staticmethod
    def _require_finite_number(name: str, value: object) -> None:
        if not isinstance(value, (float, int)) or isinstance(value, bool):
            raise TypeError(f"{name} must be a finite number")
        if not isfinite(value):
            raise ValueError(f"{name} must be a finite number")
