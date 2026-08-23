"""实现实验内候选显著性的确定性多重检验校正。"""

from __future__ import annotations

from math import erfc, isfinite, sqrt

from quant_research.experiments.models import MultipleTestingMethod


class MultipleTestingCorrector:
    """计算均值零假设 p-value，并应用 Bonferroni 或 BH-FDR。

    入参：方法均来自实验冻结治理配置。
    返回值：与输入顺序一致的原始或校正后 p-value。
    异常：样本统计或 p-value 非有限、越界时抛出值错误。
    """

    @staticmethod
    def normal_mean_p_value(mean: float, sample_std: float, count: int) -> float:
        """按双侧正态近似计算样本均值为零的 p-value。

        入参：样本均值、样本标准差和有效样本数。
        返回值：闭区间 ``[0, 1]`` 内的双侧 p-value。
        异常：统计量非有限、标准差非正或样本不足二时抛出值错误。
        """
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
        """按冻结方法校正一组候选 p-value。

        入参：校正方法和原始 p-value 元组。
        返回值：保持输入顺序的校正结果元组。
        异常：方法类型错误或任一 p-value 不在 ``[0, 1]`` 时抛出错误。
        """
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
