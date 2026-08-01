"""Cancellation is atomic at the daily backtest boundary."""

from pathlib import Path

import pytest

from quant_core.backtest.engine import BacktestCancelled, BacktestEngine
from quant_core.portfolio import RebalancePlanner
from tests.integration.test_backtest_timeline import (
    _Data,
    _Progress,
    _request,
    _RuleBook,
    _Targets,
)


class _CancelAfterOne(_Progress):
    def is_cancelled(self) -> bool:
        return len(self.calls) >= 1


def test_cancellation_preserves_closed_staging_without_manifest(tmp_path: Path) -> None:
    token = _CancelAfterOne()
    engine = BacktestEngine(
        _Data(), _Targets(), _RuleBook(), RebalancePlanner(), artifact_root=tmp_path
    )

    with pytest.raises(BacktestCancelled) as caught:
        engine.run(_request(), token, token)

    cancelled = caught.value
    assert cancelled.sessions_completed == 1
    assert cancelled.staging_dir.exists()
    assert not (cancelled.staging_dir / "manifest.json").exists()
    assert (cancelled.staging_dir / "diagnostic.json").exists()
    assert not list(tmp_path.glob("experiment_id=*/manifest.json"))
