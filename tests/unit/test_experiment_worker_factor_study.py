"""验证 Experiment Worker 的因子研究输入适配语义。"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import cast

import polars as pl

from quant_research.bootstrap.worker import _FactorRunSession
from quant_research.factors import FACTOR_OUTPUT_SCHEMA, FactorArtifact


class _Artifact:
    """提供标准因子产物形状的最小测试替身。"""

    def __init__(self, frame: pl.DataFrame) -> None:
        self._frame = frame

    def lazy_frame(self) -> pl.LazyFrame:
        """返回待适配的标准因子表。"""
        return self._frame.lazy()


def test_factor_study_maps_trade_date_to_signal_date_at_worker_boundary() -> None:
    """真实因子产物的 trade_date 必须在进入研究内核前改名。"""
    signal_date = date(2026, 1, 5)
    frame = pl.DataFrame(
        {
            "trade_date": [signal_date],
            "instrument_id": ["000001.SZ"],
            "factor_id": ["momentum_120_20"],
            "value": [1.5],
            "available_at": [datetime(2026, 1, 5, 7, 0, tzinfo=UTC)],
            "is_valid": [True],
        },
        schema=FACTOR_OUTPUT_SCHEMA,
    )
    artifacts = {
        "momentum_120_20": cast(FactorArtifact, _Artifact(frame)),
    }

    result = _FactorRunSession._analysis_factor_frame(
        artifacts, ("momentum_120_20",)
    )

    assert "trade_date" not in result.columns
    assert result.select("signal_date", "instrument_id", "factor_id").row(0) == (
        signal_date,
        "000001.SZ",
        "momentum_120_20",
    )
