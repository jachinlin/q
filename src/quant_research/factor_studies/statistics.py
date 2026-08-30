"""实现因子研究候选显著性的多重检验校正。"""

from __future__ import annotations

from math import erfc, isfinite, sqrt

from quant_research.domain.enums import MultipleTestingMethod


class MultipleTestingCorrector:
    """校正因子显著性。入参：无构造参数。返回值：统计工具实例。异常：构造不主动抛出异常。"""

    @staticmethod
    def normal_mean_p_value(mean: float, sample_std: float, count: int) -> float:
        """计算双侧 p-value。入参：均值、标准差和样本数。返回值：零到一的数值。异常：非有限值或样本不足时抛出值错误。"""

        if not all(isfinite(value) for value in (mean, sample_std)):
            raise ValueError("mean and sample_std must be finite")
        if sample_std <= 0 or type(count) is not int or count < 2:
            raise ValueError("p-value requires positive sample_std and count >= 2")
        statistic = abs(mean) / (sample_std / sqrt(count))
        return min(1.0, max(0.0, erfc(statistic / sqrt(2.0))))

    @staticmethod
    def adjust(
        method: MultipleTestingMethod, p_values: tuple[float, ...]
    ) -> tuple[float, ...]:
        """校正一组 p-value。入参：方法与数值元组。返回值：保持顺序的校正值。异常：方法或数值非法时抛出。"""

        if not isinstance(method, MultipleTestingMethod):
            raise TypeError("method must be a MultipleTestingMethod")
        if any(not isfinite(value) or not 0 <= value <= 1 for value in p_values):
            raise ValueError("p-values must be finite values in [0, 1]")
        size = len(p_values)
        if method is MultipleTestingMethod.BONFERRONI:
            return tuple(min(1.0, value * size) for value in p_values)
        ranked = sorted(enumerate(p_values), key=lambda item: (item[1], item[0]))
        adjusted = [1.0] * size
        running = 1.0
        for rank_index in range(size - 1, -1, -1):
            original_index, value = ranked[rank_index]
            rank = rank_index + 1
            running = min(running, value * size / rank)
            adjusted[original_index] = min(1.0, running)
        return tuple(adjusted)


__all__ = ["MultipleTestingCorrector"]
