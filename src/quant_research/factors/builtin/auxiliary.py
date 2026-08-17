"""提供内置实现与辅助因子相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from math import isfinite

import polars as pl

from quant_research.data.contracts import JsonValue
from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import (
    FactorContext,
    FactorSpec,
)
from quant_research.factors.builtin._stock_common import (
    BarRepository,
    _StockCommonSupport,
    canonical_scope,
    output_frame,
)


class _AuxiliarySupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _aux_spec(
        factor_id: str, lookback: int, parameters: dict[str, JsonValue]
    ) -> FactorSpec:
        return FactorSpec(
            factor_id,
            "daily",
            lookback,
            (),
            1,
            {**parameters, "role": "auxiliary", "eligible_for_alpha": False},
        )

    @staticmethod
    def _needs_amount_full_history(
        frame: pl.DataFrame, ctx: FactorContext, required_observations: int
    ) -> bool:
        for group in frame.partition_by("instrument_id", maintain_order=True):
            dates = group["trade_date"].to_list()
            for index, trade_date in enumerate(dates):
                if ctx.start <= trade_date <= ctx.end:
                    if index + 1 < required_observations:
                        return True
                    break
        return False

    @staticmethod
    def _nonnegative(value: object) -> bool:
        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and isfinite(value)
            and value >= 0
        )


def assert_alpha_eligible(specs: Sequence[FactorSpec]) -> None:
    """处理因子计算中的``assert``Alpha 因子准入证券；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        specs：参与本次处理的规格集合；调用方不得依赖未声明的顺序。
    返回值：
        无。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Fail closed before auxiliary identities can enter an alpha composite.
    """
    rejected = [
        spec.canonical_ref
        for spec in specs
        if spec.parameters.get("eligible_for_alpha") is False
    ]
    if rejected:
        raise ValueError(
            f"auxiliary factors are not eligible for alpha: {', '.join(rejected)}"
        )


class AvgAmount20dFactor:
    """表示因子计算流程中的平均值``amount20d``因子及其业务不变量。

    入参：
        repository：提供持久化访问的仓储，类型为 ``BarRepository``。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    def __init__(
        self, repository: BarRepository, instruments: Sequence[InstrumentId]
    ) -> None:
        self._repository = repository
        self._instruments = canonical_scope(instruments)
        self._spec = _AuxiliarySupport._aux_spec(
            "avg_amount_20d", 19, {"source_field": "amount", "window_sessions": 20}
        )

    @property
    def spec(self) -> FactorSpec:
        """处理因子计算中的不可变规格。

        入参：
            无。
        返回值：
            返回不可变规格（``FactorSpec``）。
        异常：
            无。
        """
        return self._spec

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """计算因子计算。

        入参：
            ctx：本次计算的上下文，类型为 ``FactorContext``。
        返回值：
            返回计算因子计算后的``compute``（``pl.LazyFrame``）。
        异常：
            无。
        """
        from quant_research.factors.builtin.momentum import _MomentumSupport

        history_start = _MomentumSupport._expanded_history_start(ctx.start, 20)
        frame = self._load(ctx, history_start)
        if history_start != date.min and _AuxiliarySupport._needs_amount_full_history(
            frame, ctx, 20
        ):
            frame = self._load(ctx, date.min)
        rows = []
        for group in frame.partition_by("instrument_id", maintain_order=True):
            data = group.to_dicts()
            for index, row in enumerate(data):
                day = row["trade_date"]
                if not ctx.start <= day <= ctx.end:
                    continue
                window = data[max(0, index - 19) : index + 1]
                amounts = [item["amount"] for item in window]
                availability = [item["available_at"] for item in window]
                valid = (
                    len(window) == 20
                    and all(_AuxiliarySupport._nonnegative(item) for item in amounts)
                    and all(
                        _StockCommonSupport._known_availability(item)
                        for item in availability
                    )
                )
                value = sum(float(item) for item in amounts) / 20.0 if valid else None
                available = max(availability) if valid else None
                rows.append((day, row["instrument_id"], value, available))
        return output_frame(self.spec, rows)

    def _load(self, ctx: FactorContext, start: date) -> pl.DataFrame:
        frame = self._repository.bars(self._instruments, start, ctx.end).collect()
        required = {"instrument_id", "trade_date", "amount", "available_at"}
        if not required.issubset(frame.columns):
            raise ValueError("amount bars missing required columns")
        if frame.select(
            pl.struct("instrument_id", "trade_date").is_duplicated().any()
        ).item():
            raise ValueError("duplicate amount bar key")
        return frame.sort("instrument_id", "trade_date")
