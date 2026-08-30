"""验证策略研究 HTTP 路由只暴露单项生命周期。"""

from typing import Any, cast

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from quant_research.dashboard.models import (
    StrategyStudyQualityDisclosure,
    StrategyStudyReportResponse,
)
from quant_research.dashboard.strategy_studies import StrategyStudyRoutes


class _Service:
    def strategies(self) -> dict[str, object]:
        return {"strategies": ["dual_ma_trend"], "components": {}}

    def validate(self, yaml_text: str) -> dict[str, object]:
        return {"config_hash": "a" * 64, "normalized": {"name": yaml_text}}

    def submit(self, yaml_text: str, actor: str) -> dict[str, object]:
        return {"id": "study-1", "name": yaml_text, "actor": actor}

    def list(self, limit: int, offset: int, status: object) -> dict[str, object]:
        return {"items": [], "limit": limit, "offset": offset, "status": status}

    def show(self, study_id: str) -> dict[str, object]:
        return {"id": study_id}

    def report(self, study_id: str) -> StrategyStudyReportResponse:
        del study_id
        return StrategyStudyReportResponse(
            performance=(),
            rolling_performance=(),
            monthly_returns=(),
            annual_returns=(),
            drawdown_episodes=(),
            exposure=(),
            attribution=(),
            execution=(),
            quality=StrategyStudyQualityDisclosure(
                calculation_mode="CASH_EXACT",
                rolling_window_sessions=252,
                tail_risk_method="HISTORICAL_95",
                risk_free_rate_annual=0.0,
                undefined_metrics={},
                unavailable_dimensions={},
                attribution_method="CASH_EXACT_SECURITY",
                warnings=(),
            ),
        )

    def delete(self, study_id: str, actor: str) -> dict[str, object]:
        return {"strategy_study_id": study_id, "actor": actor, "status": "DELETED"}

    def artifact(self, study_id: str, artifact_type: str, page: int, page_size: int, *, dimension: str | None = None) -> dict[str, object]:
        return {"study_id": study_id, "artifact_type": artifact_type, "page": page, "page_size": page_size, "dimension": dimension}


def _client() -> TestClient:
    app = FastAPI()

    @app.middleware("http")
    async def request_id(request: Request, call_next: Any) -> Any:
        request.state.request_id = "request-1"
        return await call_next(request)

    StrategyStudyRoutes.register(app, cast(Any, _Service()))
    return TestClient(app)


def test_final_routes_validate_submit_query_delete_and_artifacts() -> None:
    """新 API 应覆盖计划中的全部单项操作。"""
    client = _client()
    assert client.get("/api/v1/strategies").status_code == 200
    assert client.post("/api/v1/strategy-studies/validate", json={"yaml": "study"}).status_code == 200
    submitted = client.post("/api/v1/strategy-studies", json={"yaml": "study"})
    assert submitted.status_code == 202
    assert submitted.json()["id"] == "study-1"
    assert client.get("/api/v1/strategy-studies").status_code == 200
    assert client.get("/api/v1/strategy-studies/study-1").json() == {"id": "study-1"}
    report = client.get("/api/v1/strategy-studies/study-1/report")
    assert report.status_code == 200
    assert report.json()["performance"] == []
    assert client.get("/api/v1/strategy-studies/study-1/artifacts/nav").status_code == 200
    assert client.delete("/api/v1/strategy-studies/study-1").status_code == 200


def test_old_experiment_run_and_compare_routes_do_not_exist() -> None:
    """旧 Experiment、Run 和比较端点不得残留。"""
    client = _client()
    assert client.get("/api/v1/experiments").status_code == 404
    assert client.post("/api/v1/experiments/compare", json={"run_ids": ["a", "b"]}).status_code == 404
    assert client.post("/api/v1/runs/run-1/rerun").status_code == 404
    assert client.patch("/api/v1/runs/run-1/research", json={"mark": "BASELINE"}).status_code == 404
