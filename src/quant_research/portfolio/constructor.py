"""提供组合构建与组合构造相关的公开模型、协议与处理流程。"""

from dataclasses import dataclass
from datetime import date
from math import isfinite

import polars as pl

from quant_research.domain.identifiers import InstrumentId
from quant_research.portfolio.constraints import (
    ConstraintViolation,
    PortfolioConstraints,
)

_REQUIRED_SCHEMA = {
    "instrument_id": pl.String,
    "score": pl.Float64,
    "adv_amount": pl.Float64,
    "current_weight": pl.Float64,
}
_EPSILON = 1e-10


@dataclass(frozen=True, slots=True)
class TargetPosition:
    """定义目标组合中单只证券的权重和用于审计的综合得分。

    入参：
        instrument_id：目标证券标识，类型为 ``InstrumentId``。
        target_weight：目标组合权重。
        score：综合得分。
        reason_code：说明成交、拒绝或排除原因的稳定机器码。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    instrument_id: InstrumentId
    target_weight: float
    score: float | None
    reason_code: str


@dataclass(frozen=True, slots=True)
class TargetPortfolio:
    """绑定调仓日、按证券排序的目标持仓以及现金权重。

    入参：
        signal_date：只允许使用当日收盘前已知信息的策略信号日。
        execute_date：使用上一交易日信号生成委托并撮合的交易日。
        positions：参与本次处理的持仓集合；调用方不得依赖未声明的顺序。
        cash_weight：``cash``权重。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    signal_date: date
    execute_date: date
    positions: tuple[TargetPosition, ...]
    cash_weight: float


def validate_target_portfolio(
    target: object, signal_date: date, execute_date: date
) -> TargetPortfolio:
    """校验目标组合组合；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        target：目标组合。
        signal_date：只允许使用当日收盘前已知信息的策略信号日。
        execute_date：使用上一交易日信号生成委托并撮合的交易日。
    返回值：
        返回校验目标组合组合；该函数作为稳定公开 API 或框架入口保留在模块级后的目标组合组合（``TargetPortfolio``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Validate the shared target contract before it crosses an engine boundary.
    """
    if not isinstance(target, TargetPortfolio):
        raise TypeError("target generator must return TargetPortfolio or None")
    if type(signal_date) is not date or type(execute_date) is not date:
        raise TypeError("target schedule dates must be dates")
    if target.signal_date != signal_date or target.execute_date != execute_date:
        raise ValueError("target dates do not match generated schedule")
    if not isinstance(target.positions, tuple):
        raise TypeError("target positions must be a tuple")
    if not _ConstructorSupport._weight(target.cash_weight):
        raise ValueError("target cash_weight is invalid")
    seen: set[InstrumentId] = set()
    total = target.cash_weight
    for position in target.positions:
        if not isinstance(position, TargetPosition):
            raise TypeError("target positions must contain TargetPosition")
        if not isinstance(position.instrument_id, InstrumentId):
            raise TypeError("target position instrument_id must be an InstrumentId")
        if position.instrument_id in seen:
            raise ValueError("target positions must be unique")
        seen.add(position.instrument_id)
        if not _ConstructorSupport._weight(position.target_weight):
            raise ValueError("target weight is invalid")
        if position.score is not None and (
            not isinstance(position.score, float) or not isfinite(position.score)
        ):
            raise ValueError("target score is invalid")
        if (
            not isinstance(position.reason_code, str)
            or not position.reason_code.strip()
        ):
            raise ValueError("target reason_code is invalid")
        total += position.target_weight
    if abs(total - 1.0) > _EPSILON:
        raise ValueError("target weights plus cash_weight must equal one")
    return target


@dataclass(frozen=True, slots=True)
class _Candidate:
    instrument_id: InstrumentId
    score: float | None
    adv_amount: float
    current_weight: float


class PortfolioConstructor:
    """根据已校验输入构建组合组合。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ConstraintViolation``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Translate a validated candidate cross-section into a target portfolio.
    """

    def construct(
        self,
        candidates: pl.DataFrame,
        constraints: PortfolioConstraints,
        signal_date: date,
        execute_date: date,
    ) -> TargetPortfolio:
        """构建目标组合。

        入参：
            candidates：``candidates``。
            constraints：组合约束。
            signal_date：只允许使用当日收盘前已知信息的策略信号日。
            execute_date：使用上一交易日信号生成委托并撮合的交易日。
        返回值：
            返回``construct``（``TargetPortfolio``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``ConstraintViolation``、``ValueError``。
        """
        if not execute_date > signal_date:
            raise ValueError("execute_date must be strictly after signal_date")
        parsed = _ConstructorSupport._parse_candidates(candidates)
        current_cash = 1.0 - sum(candidate.current_weight for candidate in parsed)
        eligible = [
            candidate
            for candidate in parsed
            if candidate.score is not None
            and candidate.adv_amount >= constraints.min_adv_amount
        ]
        eligible.sort(key=_ConstructorSupport._candidate_sort_key)
        selected = eligible[: constraints.max_positions]
        if len(selected) < constraints.min_positions:
            raise ConstraintViolation(
                "min_positions", len(selected), constraints.min_positions
            )

        base_weight = 1.0 / len(selected)
        weights = [min(base_weight, constraints.max_position_weight) for _ in selected]
        positions = tuple(
            TargetPosition(
                instrument_id=candidate.instrument_id,
                target_weight=weight,
                score=candidate.score,
                reason_code="SELECTED",
            )
            for candidate, weight in zip(selected, weights, strict=True)
            if weight > 0.0
        )
        cash_weight = _ConstructorSupport._normalize_zero(
            1.0 - sum(position.target_weight for position in positions)
        )
        if cash_weight < -_EPSILON:
            raise ValueError("target weights exceed one")
        cash_weight = max(cash_weight, 0.0)
        target = TargetPortfolio(signal_date, execute_date, positions, cash_weight)
        if not _ConstructorSupport._is_initial_cash_deployment(parsed):
            _ConstructorSupport._require_turnover_within_limit(
                target, parsed, current_cash, constraints.max_turnover
            )
        return target


class _ConstructorSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _is_initial_cash_deployment(candidates: list[_Candidate]) -> bool:
        """判断当前组合是否尚未持有任何风险资产。

        入参：
            candidates：包含全部当前持仓权重的候选证券集合。
        返回值：
            当前风险资产总权重不超过数值容差时返回 ``True``。
        异常：
            无。

        从全现金状态投入初始资本不属于再平衡换手，``max_turnover`` 从首次
            建仓完成后的下一次调仓开始约束。
        """
        return sum(candidate.current_weight for candidate in candidates) <= _EPSILON

    @staticmethod
    def _parse_candidates(candidates: pl.DataFrame) -> list[_Candidate]:
        if not isinstance(candidates, pl.DataFrame):
            raise TypeError("candidates must be a Polars DataFrame")
        if candidates.schema != _REQUIRED_SCHEMA:
            missing = sorted(set(_REQUIRED_SCHEMA) - set(candidates.columns))
            mismatched = sorted(
                name
                for name, dtype in _REQUIRED_SCHEMA.items()
                if name in candidates.columns and candidates.schema[name] != dtype
            )
            details = ", ".join(missing + mismatched) or "unexpected schema"
            raise ValueError(f"invalid candidate columns: {details}")
        parsed: list[_Candidate] = []
        seen: set[InstrumentId] = set()
        for row in candidates.iter_rows(named=True):
            raw_id = row["instrument_id"]
            if not isinstance(raw_id, str) or raw_id == "":
                raise ValueError(
                    "instrument_id must be a nonempty canonical identifier"
                )
            try:
                instrument_id = InstrumentId.parse(raw_id)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    "instrument_id must be a canonical identifier"
                ) from error
            if instrument_id in seen:
                raise ValueError("instrument_id must be unique")
            seen.add(instrument_id)
            score = _ConstructorSupport._optional_finite_float("score", row["score"])
            adv_amount = _ConstructorSupport._finite_nonnegative_float(
                "adv_amount", row["adv_amount"]
            )
            current_weight = _ConstructorSupport._finite_nonnegative_float(
                "current_weight", row["current_weight"]
            )
            parsed.append(_Candidate(instrument_id, score, adv_amount, current_weight))
        current_total = sum(candidate.current_weight for candidate in parsed)
        if current_total > 1.0 + _EPSILON:
            raise ValueError("current_weight sum must not exceed one")
        return parsed

    @staticmethod
    def _optional_finite_float(name: str, value: object) -> float | None:
        if value is None:
            return None
        return _ConstructorSupport._finite_float(name, value)

    @staticmethod
    def _finite_nonnegative_float(name: str, value: object) -> float:
        number = _ConstructorSupport._finite_float(name, value)
        if number < 0:
            raise ValueError(f"{name} must be nonnegative")
        return number

    @staticmethod
    def _finite_float(name: str, value: object) -> float:
        if not isinstance(value, float) or not isfinite(value):
            raise ValueError(f"{name} must be finite Float64")
        return value

    @staticmethod
    def _candidate_sort_key(candidate: _Candidate) -> tuple[float, str]:
        if candidate.score is None:
            raise ValueError("target candidates require a score")
        return (-candidate.score, candidate.instrument_id.canonical())

    @staticmethod
    def _require_turnover_within_limit(
        target: TargetPortfolio,
        candidates: list[_Candidate],
        current_cash: float,
        max_turnover: float,
    ) -> None:
        current_weights = {
            candidate.instrument_id: candidate.current_weight
            for candidate in candidates
        }
        target_weights = {
            position.instrument_id: position.target_weight
            for position in target.positions
        }
        identifiers = current_weights.keys() | target_weights.keys()
        turnover = 0.5 * (
            sum(
                abs(
                    target_weights.get(instrument_id, 0.0)
                    - current_weights.get(instrument_id, 0.0)
                )
                for instrument_id in identifiers
            )
            + abs(target.cash_weight - current_cash)
        )
        if turnover > max_turnover + _EPSILON:
            raise ConstraintViolation("max_turnover", turnover, max_turnover)

    @staticmethod
    def _normalize_zero(value: float) -> float:
        return 0.0 if abs(value) <= _EPSILON else value

    @staticmethod
    def _weight(value: object) -> bool:
        return isinstance(value, float) and isfinite(value) and value >= 0.0
