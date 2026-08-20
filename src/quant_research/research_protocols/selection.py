"""依据验证集指标执行确定性候选选择和多重检验校正。"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from quant_research.research_protocols.models import (
    ConstraintOperator,
    MetricDirection,
    MultipleTestingMethod,
    SelectionPolicy,
)


@dataclass(frozen=True, slots=True)
class CandidateEvaluation:
    """表示一个候选仅来自 VALIDATION 的指标快照。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    variant_id: str
    metrics: dict[str, float]
    primary_p_value: float | None = None


@dataclass(frozen=True, slots=True)
class CandidateSelection:
    """表示锁定候选及每个候选的接受或拒绝证据。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    selected_variant_id: str
    reason: str
    adjusted_p_values: dict[str, float]
    rejected: dict[str, tuple[str, ...]]


class ResearchSelector:
    """只消费验证集指标，按显式规则选择唯一候选。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    def select(
        self,
        candidates: tuple[CandidateEvaluation, ...],
        policy: SelectionPolicy,
    ) -> CandidateSelection:
        """过滤无效候选并使用稳定破同分规则返回选择结果。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
        """
        if not candidates:
            raise ValueError("candidate evaluations must not be empty")
        if len({item.variant_id for item in candidates}) != len(candidates):
            raise ValueError("candidate variant_id must be unique")
        adjusted = self._adjust(candidates, policy.multiple_testing_method)
        rejected: dict[str, tuple[str, ...]] = {}
        eligible: list[CandidateEvaluation] = []
        for candidate in sorted(candidates, key=lambda item: item.variant_id):
            reasons: list[str] = []
            primary = candidate.metrics.get(policy.primary_metric)
            if primary is None or not isfinite(primary):
                reasons.append(f"MISSING_PRIMARY:{policy.primary_metric}")
            for constraint in policy.constraints:
                actual = candidate.metrics.get(constraint.metric)
                if actual is None or not isfinite(actual):
                    reasons.append(f"MISSING_CONSTRAINT:{constraint.metric}")
                elif constraint.operator is ConstraintOperator.LTE and actual > constraint.threshold:
                    reasons.append(f"CONSTRAINT_LTE:{constraint.metric}")
                elif constraint.operator is ConstraintOperator.GTE and actual < constraint.threshold:
                    reasons.append(f"CONSTRAINT_GTE:{constraint.metric}")
            if policy.multiple_testing_method is not MultipleTestingMethod.NONE:
                value = adjusted.get(candidate.variant_id)
                if value is None:
                    reasons.append("MISSING_P_VALUE")
                elif policy.adjusted_alpha is not None and value > policy.adjusted_alpha:
                    reasons.append("ADJUSTED_P_VALUE")
            if reasons:
                rejected[candidate.variant_id] = tuple(reasons)
            else:
                eligible.append(candidate)
        if not eligible:
            raise ValueError("no candidate satisfies the validation selection policy")

        direction = 1.0 if policy.direction is MetricDirection.MINIMIZE else -1.0

        def rank(item: CandidateEvaluation) -> tuple[float | str, ...]:
            primary = item.metrics[policy.primary_metric]
            secondary: list[float | str] = [direction * primary]
            for metric in policy.tie_breakers:
                value = item.metrics.get(metric)
                secondary.append(float("inf") if value is None else -value)
            secondary.append(item.variant_id)
            return tuple(secondary)

        selected = min(eligible, key=rank)
        return CandidateSelection(
            selected_variant_id=selected.variant_id,
            reason=(
                f"selected by VALIDATION {policy.direction.value.lower()} "
                f"{policy.primary_metric}={selected.metrics[policy.primary_metric]:.12g}"
            ),
            adjusted_p_values=adjusted,
            rejected=rejected,
        )

    @staticmethod
    def _adjust(
        candidates: tuple[CandidateEvaluation, ...],
        method: MultipleTestingMethod,
    ) -> dict[str, float]:
        if method is MultipleTestingMethod.NONE:
            return {}
        values: list[tuple[str, float]] = []
        for item in candidates:
            value = item.primary_p_value
            if value is None:
                continue
            if not isfinite(value) or not 0.0 <= value <= 1.0:
                raise ValueError("primary p-values must be finite and between zero and one")
            values.append((item.variant_id, value))
        values.sort(key=lambda item: (item[1], item[0]))
        count = len(values)
        if method is MultipleTestingMethod.HOLM_BONFERRONI:
            result: dict[str, float] = {}
            running = 0.0
            for index, (variant_id, value) in enumerate(values):
                running = max(running, min(1.0, value * (count - index)))
                result[variant_id] = running
            return result
        adjusted_descending = 1.0
        result = {}
        for reverse_index in range(count - 1, -1, -1):
            variant_id, value = values[reverse_index]
            rank = reverse_index + 1
            adjusted_descending = min(adjusted_descending, value * count / rank)
            result[variant_id] = min(1.0, adjusted_descending)
        return result
