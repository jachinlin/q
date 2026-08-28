"""提供因子研究四阶段与内部子步骤的统一任务进度。"""

from __future__ import annotations

from collections.abc import Mapping

from quant_research.data.contracts import JsonValue
from quant_research.factor_studies.models import FACTOR_STUDY_STAGES, FactorStudyStage
from quant_research.tasks.handlers import ProgressSink
from quant_research.tasks.models import TaskProgress


class FactorStudyProgressReporter:
    """将因子研究子步骤映射为稳定的四阶段任务进度。

    入参：sink：持久化并记录任务进度的消费者侧端口。
    返回值：可报告阶段、子步骤和确定性采样进度的实例。
    异常：非法阶段、子步骤或进度违反任务模型时抛出值错误。
    """

    _SAMPLE_BUCKETS = 20

    def __init__(self, sink: ProgressSink) -> None:
        self._sink = sink
        self._stage = FactorStudyStage.VALIDATE
        self._current_substage: str | None = None
        self._last_completed_substage: str | None = None
        self._last_completed_evidence: dict[str, JsonValue] = {}
        self._sample_buckets: dict[str, int] = {}

    @property
    def current_substage(self) -> str | None:
        """读取当前子步骤。入参：无。返回值：子步骤或空值。异常：无。"""
        return self._current_substage

    def stage_started(self, stage: FactorStudyStage) -> None:
        """报告公开阶段开始。入参：阶段。返回值：无。异常：阶段未知时抛出。"""
        self._stage = stage
        self._current_substage = None
        index = self._stage_index(stage)
        self._emit(
            completed=index,
            message=f"开始 {stage.value}",
            context=self._with_last_completed({"stage_state": "STARTED"}),
        )

    def stage_completed(
        self,
        stage: FactorStudyStage,
        evidence: Mapping[str, JsonValue] | None = None,
    ) -> None:
        """报告公开阶段完成。入参：阶段及安全证据。返回值：无。异常：阶段未知时抛出。"""
        self._stage = stage
        self._current_substage = None
        context: dict[str, JsonValue] = {"stage_state": "COMPLETED"}
        if evidence is not None:
            context.update(evidence)
        self._emit(
            completed=self._stage_index(stage) + 1,
            message=f"完成 {stage.value}",
            context=self._with_last_completed(context),
        )

    def substage_started(
        self,
        substage: str,
        message: str,
        evidence: Mapping[str, JsonValue] | None = None,
    ) -> None:
        """报告内部子步骤开始。入参：身份、消息及安全证据。返回值：无。异常：身份为空时抛出。"""
        self._current_substage = self._substage(substage)
        self._sample_buckets.pop(self._current_substage, None)
        values = dict(evidence) if evidence is not None else {}
        self._substage_event(
            "STARTED",
            message,
            self._with_last_completed(values),
        )

    def substage_progress(
        self,
        substage: str,
        message: str,
        *,
        item_completed: int,
        item_total: int,
        evidence: Mapping[str, JsonValue] | None = None,
    ) -> bool:
        """按固定百分比桶报告内部进度。

        入参：子步骤、消息、已完成量、总量及安全证据。
        返回值：本次调用是否实际发布进度。
        异常：计数越界或子步骤与当前步骤不一致时抛出值错误。
        """
        name = self._substage(substage)
        if name != self._current_substage:
            raise ValueError("factor study progress substage is not active")
        if item_total <= 0 or not 1 <= item_completed <= item_total:
            raise ValueError("factor study item progress is out of bounds")
        bucket = item_completed * self._SAMPLE_BUCKETS // item_total
        previous = self._sample_buckets.get(name, -1)
        if item_completed != 1 and item_completed != item_total and bucket <= previous:
            return False
        self._sample_buckets[name] = max(previous, bucket)
        context: dict[str, JsonValue] = {
            "item_completed": item_completed,
            "item_total": item_total,
        }
        if evidence is not None:
            context.update(evidence)
        self._substage_event("PROGRESS", message, context)
        return True

    def substage_completed(
        self,
        substage: str,
        message: str,
        evidence: Mapping[str, JsonValue] | None = None,
    ) -> None:
        """报告内部子步骤完成。入参：身份、消息及安全证据。返回值：无。异常：步骤不匹配时抛出。"""
        name = self._substage(substage)
        if name != self._current_substage:
            raise ValueError("factor study progress substage is not active")
        self._substage_event("COMPLETED", message, evidence)
        self._last_completed_substage = name
        self._last_completed_evidence = (
            dict(evidence) if evidence is not None else {}
        )

    def _substage_event(
        self,
        state: str,
        message: str,
        evidence: Mapping[str, JsonValue] | None,
    ) -> None:
        substage = self._current_substage
        if substage is None:
            raise ValueError("factor study progress has no active substage")
        context: dict[str, JsonValue] = {
            "substage": substage,
            "substage_state": state,
        }
        if evidence is not None:
            context.update(evidence)
        self._emit(
            completed=self._stage_index(self._stage),
            message=message,
            context=context,
        )

    def _emit(
        self,
        *,
        completed: int,
        message: str,
        context: dict[str, JsonValue],
    ) -> None:
        self._sink.update(
            TaskProgress(
                stage=self._stage.value,
                completed=completed,
                total=len(FACTOR_STUDY_STAGES),
                message=message,
                context=context,
            )
        )

    def _with_last_completed(
        self,
        context: dict[str, JsonValue],
    ) -> dict[str, JsonValue]:
        if self._last_completed_substage is None:
            return context
        return {
            **context,
            "last_completed_substage": self._last_completed_substage,
            "last_completed_evidence": dict(self._last_completed_evidence),
        }

    @staticmethod
    def _stage_index(stage: FactorStudyStage) -> int:
        try:
            return FACTOR_STUDY_STAGES.index(stage)
        except ValueError as error:
            raise ValueError(f"unknown factor study stage: {stage}") from error

    @staticmethod
    def _substage(value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("factor study substage must be nonempty")
        return value


__all__ = ["FactorStudyProgressReporter"]
