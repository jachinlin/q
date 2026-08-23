"""验证目标实验 HTTP 路由存在且旧研究族路由消失。"""

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import polars as pl
from fastapi import FastAPI
from fastapi.testclient import TestClient

from quant_research.dashboard.app import create_dashboard_app
from quant_research.dashboard.experiments import (
    ExperimentDashboardService,
    ExperimentRoutes,
)
from quant_research.strategies.registry import StrategyRegistry


def test_strategy_catalog_and_hard_cut_routes() -> None:
    app = FastAPI()
    service = ExperimentDashboardService(
        cast(Any, object()),
        StrategyRegistry.builtins(
            commission_bps=3.0, commission_minimum_fen=500
        ),
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
        cast(Any, _ArtifactExperimentService(_ArtifactRun(str(artifact_dir), manifest_hash))),
        StrategyRegistry.builtins(
            commission_bps=3.0, commission_minimum_fen=500
        ),
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
