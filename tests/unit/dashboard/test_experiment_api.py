"""验证目标实验 HTTP 路由存在且旧研究族路由消失。"""

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from quant_research.dashboard.app import create_dashboard_app
from quant_research.dashboard.experiments import (
    ExperimentDashboardService,
    ExperimentRoutes,
)
from quant_research.experiments.config import ExperimentConfigParser
from quant_research.experiments.models import (
    ResearchMark,
    RunMetricRecord,
    RunStage,
    RunStatus,
)
from quant_research.strategies.registry import StrategyRegistry


def test_strategy_catalog_and_hard_cut_routes() -> None:
    app = FastAPI()
    service = ExperimentDashboardService(
        cast(Any, object()),
        StrategyRegistry.builtins(commission_bps=3.0, commission_minimum_fen=500),
        Path.cwd(),
    )
    ExperimentRoutes.mount(app, service)
    client = TestClient(app)
    response = client.get("/api/v1/strategies")
    assert response.status_code == 200
    assert response.json()["strategies"] == [
        "dual_ma_trend",
        "etf_rotation",
        "stock_multifactor",
    ]
    paths = {route.path for route in app.routes}
    assert "/api/v1/experiments" in paths
    assert all(not path.startswith("/api/v1/research") for path in paths)


class _RejectedExperimentService:
    """为 HTTP 错误边界提供可控的实验校验失败。"""

    def validate(self, _: str) -> dict[str, object]:
        """模拟配置字段值错误。"""
        raise ValueError("max_positions must be positive")


def test_experiment_validation_returns_actionable_422(tmp_path: Path) -> None:
    app = create_dashboard_app(
        service=cast(Any, object()),
        commands=cast(Any, object()),
        experiment_service=cast(Any, _RejectedExperimentService()),
        notebook_probe=cast(Any, object()),
        static_dir=tmp_path,
        allowed_hosts=("testserver",),
    )
    response = TestClient(app).post(
        "/api/v1/experiments/validate",
        json={"yaml": "name: invalid"},
        headers={
            "X-Request-ID": "request-422",
            "Origin": "http://testserver",
        },
    )
    assert response.status_code == 422
    assert response.json()["error"] == {
        "code": "DASHBOARD_INPUT_INVALID",
        "message": "max_positions must be positive",
        "severity": "SEVERE",
        "retryable": False,
        "remediation": "修改实验配置后重新校验；若校验已通过，请刷新页面以避免提交旧配置。",
        "context": {},
        "request_id": "request-422",
    }


@dataclass(frozen=True, slots=True)
class _ArtifactRun:
    """提供产物读取测试需要的最小 Run 快照。"""

    artifact_dir: str
    manifest_hash: str


class _ArtifactExperimentService:
    """返回一个已发布产物的可控 Run。"""

    def __init__(self, run: _ArtifactRun) -> None:
        self._run = run

    def get_run(self, _: str) -> _ArtifactRun:
        """返回测试固定的 Run 快照。"""
        return self._run


class _DeletionExperimentService:
    """记录删除调用并提供一个已发布终态 Run。"""

    def __init__(self, run: SimpleNamespace) -> None:
        self.run = run
        self.deleted_run: tuple[str, str] | None = None

    def get_run(self, run_id: str) -> SimpleNamespace:
        """返回固定 Run 并校验标识。"""
        assert run_id == self.run.id
        return self.run

    def delete_run(self, run_id: str, *, actor: str) -> None:
        """记录应用服务收到的 Run 删除请求。"""
        self.deleted_run = (run_id, actor)


def test_delete_run_removes_only_identity_bound_artifact_directory(
    tmp_path: Path,
) -> None:
    experiment_id, run_id = "experiment-1", "run-1"
    artifact_dir = tmp_path / "experiments" / experiment_id / run_id
    artifact_dir.mkdir(parents=True)
    (artifact_dir / "manifest.json").write_text("{}", encoding="utf-8")
    application = _DeletionExperimentService(
        SimpleNamespace(
            id=run_id,
            experiment_id=experiment_id,
            artifact_dir=str(artifact_dir),
        )
    )
    service = ExperimentDashboardService(
        cast(Any, application),
        StrategyRegistry.builtins(commission_bps=3.0, commission_minimum_fen=500),
        tmp_path,
    )

    result = service.delete_run(run_id, "request-delete")

    assert result == {
        "experiment_id": experiment_id,
        "run_id": run_id,
        "status": "DELETED",
    }
    assert application.deleted_run == (run_id, "request-delete")
    assert not artifact_dir.exists()


def test_delete_run_rejects_artifact_directory_outside_run_identity(
    tmp_path: Path,
) -> None:
    other = tmp_path / "other"
    other.mkdir()
    application = _DeletionExperimentService(
        SimpleNamespace(
            id="run-1",
            experiment_id="experiment-1",
            artifact_dir=str(other),
        )
    )
    service = ExperimentDashboardService(
        cast(Any, application),
        StrategyRegistry.builtins(commission_bps=3.0, commission_minimum_fen=500),
        tmp_path,
    )

    with pytest.raises(ValueError, match="trusted identity"):
        service.delete_run("run-1", "request-delete")

    assert application.deleted_run is None
    assert other.exists()


def test_artifact_route_serializes_parquet_dates_as_iso_strings(
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "experiment" / "run"
    artifact_dir.mkdir(parents=True)
    artifact_path = artifact_dir / "signals.parquet"
    frame = pl.DataFrame(
        {
            "signal_date": [date(2025, 1, 2)],
            "instrument_id": ["510300.SH"],
            "signal": ["LONG"],
        },
        schema={
            "signal_date": pl.Date,
            "instrument_id": pl.String,
            "signal": pl.String,
        },
    )
    frame.write_parquet(artifact_path)
    content = artifact_path.read_bytes()
    manifest = {
        "artifacts": [
            {
                "artifact_type": "signals",
                "relative_path": "signals.parquet",
                "content_hash": hashlib.sha256(content).hexdigest(),
                "byte_count": len(content),
                "row_count": 1,
                "schema": {name: str(dtype) for name, dtype in frame.schema.items()},
                "primary_key": ["signal_date", "instrument_id"],
                "sort_key": ["signal_date", "instrument_id"],
            }
        ]
    }
    manifest_path = artifact_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    manifest_hash = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    service = ExperimentDashboardService(
        cast(
            Any,
            _ArtifactExperimentService(_ArtifactRun(str(artifact_dir), manifest_hash)),
        ),
        StrategyRegistry.builtins(commission_bps=3.0, commission_minimum_fen=500),
        tmp_path,
    )
    app = FastAPI()
    ExperimentRoutes.mount(app, service)

    response = TestClient(app).get("/api/v1/runs/run/artifacts/signals")

    assert response.status_code == 200
    assert response.json() == {
        "items": [
            {
                "signal_date": "2025-01-02",
                "instrument_id": "510300.SH",
                "signal": "LONG",
            }
        ],
        "page": 1,
        "page_size": 100,
        "total": 1,
    }


def test_artifact_filters_after_integrity_validation_and_before_paging(
    tmp_path: Path,
) -> None:
    artifact_dir = tmp_path / "experiment" / "run"
    artifact_dir.mkdir(parents=True)
    artifact_path = artifact_dir / "summary.parquet"
    frame = pl.DataFrame(
        {
            "signal_variant": ["RAW", "RAW"],
            "factor_ref": ["momentum", "value"],
            "horizon": [5, 5],
            "rank_ic_mean": [0.1, 0.2],
        }
    ).sort(["signal_variant", "factor_ref", "horizon"])
    frame.write_parquet(artifact_path)
    content = artifact_path.read_bytes()
    manifest = {
        "artifacts": [
            {
                "artifact_type": "summary",
                "relative_path": "summary.parquet",
                "content_hash": hashlib.sha256(content).hexdigest(),
                "byte_count": len(content),
                "row_count": 2,
                "schema": {name: str(dtype) for name, dtype in frame.schema.items()},
                "primary_key": ["signal_variant", "factor_ref", "horizon"],
                "sort_key": ["signal_variant", "factor_ref", "horizon"],
            }
        ]
    }
    manifest_path = artifact_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    service = ExperimentDashboardService(
        cast(
            Any,
            _ArtifactExperimentService(
                _ArtifactRun(
                    str(artifact_dir),
                    hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                )
            ),
        ),
        StrategyRegistry.builtins(commission_bps=3.0, commission_minimum_fen=500),
        tmp_path,
    )
    app = FastAPI()
    ExperimentRoutes.mount(app, service)
    client = TestClient(app)

    response = client.get(
        "/api/v1/runs/run/artifacts/summary",
        params={"factor_ref": "value", "horizon": 5},
    )

    assert response.status_code == 200
    assert response.json()["total"] == 1
    assert response.json()["items"][0]["factor_ref"] == "value"
    with pytest.raises(ValueError, match="unsupported filter for signals"):
        service.artifact("run", "signals", 1, 100, factor_ref="value")


class _ComparisonExperimentService:
    """返回同一实验中的两个固定 Run。"""

    def __init__(self, runs: tuple[SimpleNamespace, ...]) -> None:
        self._runs = {item.id: item for item in runs}

    def get_run(self, run_id: str) -> SimpleNamespace:
        """按 ID 返回固定 Run。"""
        return self._runs[run_id]

    def show(self, experiment_id: str) -> SimpleNamespace:
        """返回含 baseline 指针的聚合。"""
        assert experiment_id == "experiment-1"
        return SimpleNamespace(experiment=SimpleNamespace(baseline_run_id="baseline"))


def _comparison_runs() -> tuple[SimpleNamespace, SimpleNamespace]:
    parsed = ExperimentConfigParser().parse_experiment(
        """name: compare
kind: STRATEGY_BACKTEST
sample_windows:
  train: {start: 2020-01-01, end: 2020-12-31}
  validation: {start: 2021-01-01, end: 2021-12-31}
  test: {start: 2022-01-01, end: 2022-12-31}
governance: {test_budget: 1, correction: BONFERRONI}
initial_run:
  kind: STRATEGY_BACKTEST
  start_date: 2020-01-01
  end_date: 2021-12-31
  strategy:
    strategy_id: dual_ma_trend
    parameters: {instrument_id: 510300.SH, short_window: 5, long_window: 20}
  benchmark: 000300.SH
  initial_cash_fen: 1000000
  execution: {reference_price: OPEN, slippage_bps: 0.0, max_volume_participation: 0.1, limit_order_policy: REJECT}
"""
    )
    config = parsed.definition.initial_run
    base_fields = {
        "experiment_id": "experiment-1",
        "config": config,
        "status": RunStatus.SUCCEEDED,
        "research_mark": ResearchMark.CANDIDATE,
        "stage": RunStage.PERSIST,
        "created_at": datetime(2026, 8, 23, tzinfo=UTC),
    }
    baseline = SimpleNamespace(
        id="baseline",
        metrics=(
            RunMetricRecord(
                name="annualized_return",
                value=0.1,
                unit="ratio",
                p_value=None,
                adjusted_p_value=None,
            ),
            RunMetricRecord(
                name="trade_count",
                value=10.0,
                unit="count",
                p_value=None,
                adjusted_p_value=None,
            ),
        ),
        **base_fields,
    )
    current = SimpleNamespace(
        id="current",
        metrics=(
            RunMetricRecord(
                name="annualized_return",
                value=0.13,
                unit="ratio",
                p_value=None,
                adjusted_p_value=None,
            ),
            RunMetricRecord(
                name="trade_count",
                value=12.0,
                unit="trades",
                p_value=None,
                adjusted_p_value=None,
            ),
        ),
        **base_fields,
    )
    return baseline, current


def test_compare_aligns_metrics_and_preserves_unit_mismatch() -> None:
    runs = _comparison_runs()
    service = ExperimentDashboardService(
        cast(Any, _ComparisonExperimentService(runs)),
        StrategyRegistry.builtins(commission_bps=3.0, commission_minimum_fen=500),
        Path.cwd(),
    )

    result = service.compare(("baseline", "current"))

    assert result["baseline_run_id"] == "baseline"
    metrics = {
        item["name"]: item for item in cast(list[dict[str, Any]], result["metrics"])
    }
    annualized = metrics["annualized_return"]
    assert annualized["values"][1]["delta_from_baseline"] == pytest.approx(0.03)
    assert metrics["trade_count"]["values"][1]["delta_from_baseline"] is None
    assert any(
        item["differs"] is False
        for item in cast(list[dict[str, Any]], result["configs"])
    )
