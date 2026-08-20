"""验证三种信号到组合的类型化构建器。"""

from datetime import UTC, date, datetime

from quant_research.costs import CostEstimate, PreTradeCostSlice
from quant_research.portfolio.research import (
    AllocationProjector,
    AlphaRiskCostOptimizer,
    DirectionalExposureMapper,
)
from quant_research.risk import RiskSlice
from quant_research.signals import (
    AllocationSignalRow,
    CrossSectionalScoreRow,
    Direction,
    DirectionalSignalRow,
)


def test_directional_and_allocation_constructors() -> None:
    day = date(2024, 1, 2)
    execute = date(2024, 1, 3)
    available = datetime(2024, 1, 2, tzinfo=UTC)
    directional = DirectionalSignalRow(day, "510300.SH", "ma", Direction.LONG, 1.0, True, available, True, None)
    target = DirectionalExposureMapper(long_weight=0.8).construct(directional, execute)
    assert target.positions[0].target_weight == 0.8
    assert target.cash_weight == 0.2
    allocation = (
        AllocationSignalRow(day, "510050.SH", "rotation", 0.25, available, True, None),
        AllocationSignalRow(day, "510300.SH", "rotation", 0.75, available, True, None),
    )
    projected = AllocationProjector(max_position_weight=0.6).construct(allocation, execute)
    assert [item.target_weight for item in projected.positions] == [0.25, 0.6]
    assert projected.cash_weight == 0.15


def test_alpha_risk_cost_optimizer_respects_position_caps() -> None:
    day = date(2024, 1, 2)
    execute = date(2024, 1, 3)
    available = datetime(2024, 1, 2, tzinfo=UTC)
    signals = tuple(
        CrossSectionalScoreRow(day, instrument, "alpha", score, 1.0, available, True, None)
        for instrument, score in (("600000.SH", 2.0), ("600001.SH", 1.0))
    )
    risk = RiskSlice(day, ("600000.SH", "600001.SH"), (0.2, 0.2), ((0.04, 0.0), (0.0, 0.04)), (1e8, 1e8))
    costs = PreTradeCostSlice(day, tuple(CostEstimate(item, 0.0, 0.0, 0.0, 1.0) for item in risk.instruments))
    result = AlphaRiskCostOptimizer(
        min_positions=2,
        max_positions=2,
        max_position_weight=0.6,
        max_turnover=1.0,
        risk_aversion=0.1,
        cost_aversion=0.0,
    ).construct(signal_date=day, execute_date=execute, signals=signals, risk=risk, costs=costs, current_weights={})
    assert len(result.target.positions) == 2
    assert all(item.target_weight <= 0.6 for item in result.target.positions)
    assert result.objective.alpha > 0.0
