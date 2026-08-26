"""注册 Dashboard 的版本化 HTTP API 路由。"""

from __future__ import annotations

from datetime import date

from fastapi import FastAPI, Query, Request

from quant_research.application.operations import OperationalCommandService
from quant_research.application.settings import DashboardSettingsService
from quant_research.dashboard.models import (
    DashboardSettingsPatchRequest,
    DashboardSettingsResponse,
    DataBootstrapRequest,
    DatasetDetailResponse,
    DatasetListResponse,
    DataSummaryResponse,
    DataUpdatePlanRequest,
    DataUpdateRequest,
    MarketReviewDates,
    MarketReviewResponse,
    NotebookStatusResponse,
    OverviewResponse,
    QualityRunDetailResponse,
    QualityRunListResponse,
    QualityRunRequest,
    RetryRequest,
)
from quant_research.dashboard.notebook import NotebookProbe
from quant_research.dashboard.views import DashboardViewService
from quant_research.data.contracts import JsonValue


class _DashboardRoutes:
    @staticmethod
    def mount(
        app: FastAPI,
        dashboard: DashboardViewService,
        mutation: OperationalCommandService,
        notebook: NotebookProbe,
        settings: DashboardSettingsService,
    ) -> None:
        @app.get("/api/v1/health")
        def health() -> dict[str, object]:
            return dashboard.health()

        @app.get("/api/v1/notebook/status", response_model=NotebookStatusResponse)
        def notebook_status() -> NotebookStatusResponse:
            return NotebookStatusResponse(
                status="READY" if notebook.is_ready() else "UNAVAILABLE"
            )

        @app.get("/api/v1/overview", response_model=OverviewResponse)
        def overview() -> dict[str, object]:
            return dashboard.overview()

        @app.get("/api/v1/market-review/dates")
        def market_review_dates() -> MarketReviewDates:
            return dashboard.market_review_dates()

        @app.get("/api/v1/market-review")
        def market_review(
            trade_date: date | None = None,
            exclude_st: bool = False,
        ) -> MarketReviewResponse:
            return dashboard.market_review(trade_date, exclude_st=exclude_st)

        @app.get("/api/v1/data/summary", response_model=DataSummaryResponse)
        def data_summary() -> dict[str, object]:
            return dashboard.data_summary()

        @app.post("/api/v1/data/bootstrap", status_code=202)
        def data_bootstrap(
            request: Request,
            body: DataBootstrapRequest,
        ) -> dict[str, JsonValue]:
            return mutation.enqueue_data_bootstrap(
                years=body.years,
                request_id=request.state.request_id,
            )

        @app.get("/api/v1/settings", response_model=DashboardSettingsResponse)
        def dashboard_settings() -> dict[str, object]:
            return settings.view()

        @app.patch("/api/v1/settings", response_model=DashboardSettingsResponse)
        def change_dashboard_settings(
            body: DashboardSettingsPatchRequest,
        ) -> dict[str, object]:
            change = body.data_source_token
            if change is None:
                raise ValueError("settings patch must contain a data source token change")
            return settings.change_data_source_token(
                operation=change.operation,
                value=change.value,
            )

        @app.get("/api/v1/data/datasets", response_model=DatasetListResponse)
        def data_datasets() -> dict[str, object]:
            return dashboard.data_datasets()

        @app.get(
            "/api/v1/data/datasets/{dataset}", response_model=DatasetDetailResponse
        )
        def data_dataset(dataset: str) -> dict[str, object]:
            return dashboard.data_dataset(dataset)

        @app.get("/api/v1/data/quality-runs", response_model=QualityRunListResponse)
        def quality_runs(
            scope: str | None = None,
            status: str | None = None,
            dataset: str | None = None,
            severity: str | None = None,
            rule: str | None = None,
            page: int = Query(default=1, ge=1),
            page_size: int = Query(default=25, ge=1, le=200),
        ) -> dict[str, object]:
            return dashboard.quality_runs(
                scope=scope,
                status=status,
                dataset=dataset,
                severity=severity,
                rule=rule,
                page=page,
                page_size=page_size,
            )

        @app.post("/api/v1/data/quality-runs", status_code=202)
        def create_quality_run(
            request: Request,
            body: QualityRunRequest,
        ) -> dict[str, object]:
            return mutation.enqueue_data_validation(
                dataset=body.dataset,
                request_id=request.state.request_id,
            )

        @app.get(
            "/api/v1/data/quality-runs/{run_id}",
            response_model=QualityRunDetailResponse,
        )
        def quality_run(run_id: str) -> dict[str, object]:
            return dashboard.quality_run(run_id)

        @app.post("/api/v1/data/update-plans/preview")
        def data_update_plan(body: DataUpdatePlanRequest) -> dict[str, JsonValue]:
            return mutation.preview_data_update(
                start=body.start,
                end=body.end,
                datasets=body.datasets,
            )

        @app.post("/api/v1/data/updates", status_code=202)
        def data_update(
            request: Request, body: DataUpdateRequest
        ) -> dict[str, JsonValue]:
            return mutation.enqueue_data_update(
                start=body.start,
                end=body.end,
                datasets=body.datasets,
                expected_plan_hash=body.plan_hash,
                request_id=request.state.request_id,
            )

        @app.get("/api/v1/tasks")
        def tasks(
            status: str | None = None,
            task_type: str | None = None,
            page: int = Query(default=1, ge=1),
            page_size: int = Query(default=25, ge=1, le=200),
        ) -> dict[str, object]:
            return dashboard.task_list(
                status=status,
                task_type=task_type,
                page=page,
                page_size=page_size,
            )

        @app.get("/api/v1/tasks/{task_id}")
        def task_detail(task_id: str) -> dict[str, object]:
            return dashboard.task_detail(task_id)

        @app.get("/api/v1/tasks/{task_id}/attempts/{attempt_id}/log")
        def task_log(
            task_id: str,
            attempt_id: str,
            tail_lines: int = Query(default=500, ge=1, le=5000),
        ) -> dict[str, object]:
            return dashboard.task_log(task_id, attempt_id, tail_lines)

        @app.post("/api/v1/tasks/{task_id}/cancel")
        def cancel_task(task_id: str, request: Request) -> dict[str, object]:
            return mutation.cancel_task(task_id, request_id=request.state.request_id)

        @app.post("/api/v1/tasks/{task_id}/retry")
        def retry_task(
            task_id: str, request: Request, body: RetryRequest
        ) -> dict[str, JsonValue]:
            return mutation.retry_task(
                task_id,
                confirm_orphaned=body.confirm_orphaned,
                request_id=request.state.request_id,
            )

        @app.delete("/api/v1/tasks/{task_id}")
        def delete_task(task_id: str, request: Request) -> dict[str, object]:
            return mutation.delete_task(
                task_id,
                request_id=request.state.request_id,
            )
