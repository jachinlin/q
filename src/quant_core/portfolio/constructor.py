"""Deterministic construction of constrained target portfolios."""

from dataclasses import dataclass
from datetime import date
from math import isfinite

import polars as pl

from quant_core.domain.identifiers import InstrumentId
from quant_core.portfolio.constraints import ConstraintViolation, PortfolioConstraints

_REQUIRED_SCHEMA = {
    "instrument_id": pl.String,
    "score": pl.Float64,
    "industry": pl.String,
    "adv_amount": pl.Float64,
    "current_weight": pl.Float64,
}
_EPSILON = 1e-10


@dataclass(frozen=True, slots=True)
class TargetPosition:
    instrument_id: InstrumentId
    target_weight: float
    score: float | None
    reason_code: str


@dataclass(frozen=True, slots=True)
class TargetPortfolio:
    signal_date: date
    execute_date: date
    positions: tuple[TargetPosition, ...]
    cash_weight: float


def validate_target_portfolio(
    target: object, signal_date: date, execute_date: date
) -> TargetPortfolio:
    """Validate the shared target contract before it crosses an engine boundary."""
    if not isinstance(target, TargetPortfolio):
        raise TypeError("target generator must return TargetPortfolio or None")
    if type(signal_date) is not date or type(execute_date) is not date:
        raise TypeError("target schedule dates must be dates")
    if target.signal_date != signal_date or target.execute_date != execute_date:
        raise ValueError("target dates do not match generated schedule")
    if not isinstance(target.positions, tuple):
        raise TypeError("target positions must be a tuple")
    if not _weight(target.cash_weight):
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
        if not _weight(position.target_weight):
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
    industry: str | None
    adv_amount: float
    current_weight: float


class PortfolioConstructor:
    """Translate a validated candidate cross-section into a target portfolio."""

    def construct(
        self,
        candidates: pl.DataFrame,
        constraints: PortfolioConstraints,
        signal_date: date,
        execute_date: date,
    ) -> TargetPortfolio:
        if not execute_date > signal_date:
            raise ValueError("execute_date must be strictly after signal_date")
        parsed = _parse_candidates(candidates)
        current_cash = 1.0 - sum(candidate.current_weight for candidate in parsed)
        for candidate in parsed:
            if (
                candidate.score is not None
                and candidate.adv_amount >= constraints.min_adv_amount
                and (candidate.industry is None or candidate.industry == "")
            ):
                raise ValueError("industry is required for target candidates")
        eligible = [
            candidate
            for candidate in parsed
            if candidate.score is not None
            and candidate.adv_amount >= constraints.min_adv_amount
        ]
        eligible.sort(key=_candidate_sort_key)
        selected = eligible[: constraints.max_positions]
        if len(selected) < constraints.min_positions:
            raise ConstraintViolation(
                "min_positions", len(selected), constraints.min_positions
            )

        base_weight = 1.0 / len(selected)
        weights = [min(base_weight, constraints.max_position_weight) for _ in selected]
        _apply_industry_caps(selected, weights, constraints.max_industry_weight)
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
        cash_weight = _normalize_zero(
            1.0 - sum(position.target_weight for position in positions)
        )
        if cash_weight < -_EPSILON:
            raise ValueError("target weights exceed one")
        cash_weight = max(cash_weight, 0.0)
        target = TargetPortfolio(signal_date, execute_date, positions, cash_weight)
        _require_turnover_within_limit(
            target, parsed, current_cash, constraints.max_turnover
        )
        return target


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
            raise ValueError("instrument_id must be a nonempty canonical identifier")
        try:
            instrument_id = InstrumentId.parse(raw_id)
        except (TypeError, ValueError) as error:
            raise ValueError("instrument_id must be a canonical identifier") from error
        if instrument_id in seen:
            raise ValueError("instrument_id must be unique")
        seen.add(instrument_id)
        score = _optional_finite_float("score", row["score"])
        industry = row["industry"]
        if industry is not None and not isinstance(industry, str):
            raise ValueError("industry must be a string or null")
        adv_amount = _finite_nonnegative_float("adv_amount", row["adv_amount"])
        current_weight = _finite_nonnegative_float(
            "current_weight", row["current_weight"]
        )
        parsed.append(
            _Candidate(instrument_id, score, industry, adv_amount, current_weight)
        )
    current_total = sum(candidate.current_weight for candidate in parsed)
    if current_total > 1.0 + _EPSILON:
        raise ValueError("current_weight sum must not exceed one")
    return parsed


def _optional_finite_float(name: str, value: object) -> float | None:
    if value is None:
        return None
    return _finite_float(name, value)


def _finite_nonnegative_float(name: str, value: object) -> float:
    number = _finite_float(name, value)
    if number < 0:
        raise ValueError(f"{name} must be nonnegative")
    return number


def _finite_float(name: str, value: object) -> float:
    if not isinstance(value, float) or not isfinite(value):
        raise ValueError(f"{name} must be finite Float64")
    return value


def _apply_industry_caps(
    selected: list[_Candidate], weights: list[float], max_industry_weight: float
) -> None:
    industry_totals: dict[str, float] = {}
    for candidate, weight in zip(selected, weights, strict=True):
        if candidate.industry is None:
            raise ValueError("industry is required for target candidates")
        industry_totals[candidate.industry] = (
            industry_totals.get(candidate.industry, 0.0) + weight
        )
    for industry, total in industry_totals.items():
        if total > max_industry_weight:
            scale = max_industry_weight / total
            for index, candidate in enumerate(selected):
                if candidate.industry == industry:
                    weights[index] *= scale


def _candidate_sort_key(candidate: _Candidate) -> tuple[float, str]:
    if candidate.score is None:
        raise ValueError("target candidates require a score")
    return (-candidate.score, candidate.instrument_id.canonical())


def _require_turnover_within_limit(
    target: TargetPortfolio,
    candidates: list[_Candidate],
    current_cash: float,
    max_turnover: float,
) -> None:
    current_weights = {
        candidate.instrument_id: candidate.current_weight for candidate in candidates
    }
    target_weights = {
        position.instrument_id: position.target_weight for position in target.positions
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


def _normalize_zero(value: float) -> float:
    return 0.0 if abs(value) <= _EPSILON else value


def _weight(value: object) -> bool:
    return isinstance(value, float) and isfinite(value) and value >= 0.0
