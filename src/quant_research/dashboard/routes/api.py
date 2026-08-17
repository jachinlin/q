"""注册 Dashboard 的版本化 HTTP API 路由。"""

from __future__ import annotations

from datetime import date

from fastapi import FastAPI, Query, Request

from quant_research.application.research import ResearchApplicationService
from quant_research.dashboard.models import (
    CompareRequest,
    DatasetDetailResponse,
    DatasetListResponse,
    DataSummaryResponse,
    DataUpdatePlanRequest,
    DataUpdateRequest,
    ExperimentCloneRequest,
    ExperimentSubmitRequest,
    FactorStudyCreateRequest,
    MarketReviewDates,
    MarketReviewResponse,
    NotebookStatusResponse,
    OverviewResponse,
    QualityRunDetailResponse,
    QualityRunListResponse,
    QualityRunRequest,
    ResearchUpdateRequest,
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
        mutation: ResearchApplicationService,
        notebook: NotebookProbe,
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

        @app.get("/api/v1/factors/catalog")
        def factor_catalog() -> dict[str, object]:
            return dashboard.factor_catalog()

        @app.post("/api/v1/factor-studies", status_code=201)
        def create_factor_study(body: FactorStudyCreateRequest) -> dict[str, object]:
            return mutation.create_factor_study(body.name, body.config())

        @app.get("/api/v1/factor-studies")
        def factor_studies(
            page: int = Query(default=1, ge=1),
            page_size: int = Query(default=25, ge=1, le=200),
        ) -> dict[str, object]:
            return dashboard.factor_studies(page, page_size)

        @app.get("/api/v1/factor-studies/{study_id}")
        def factor_study(study_id: str) -> dict[str, object]:
            return dashboard.factor_study(study_id)

        @app.post("/api/v1/factor-studies/{study_id}/runs", status_code=202)
        def create_factor_run(study_id: str, request: Request) -> dict[str, object]:
            return mutation.enqueue_factor_run(
                study_id, request_id=request.state.request_id
            )

        @app.get("/api/v1/factor-runs/{run_id}")
        def factor_run(run_id: str) -> dict[str, object]:
            return dashboard.factor_run(run_id)

        @app.get("/api/v1/factor-runs/{run_id}/series")
        def factor_series(
            run_id: str,
            factor_ref: str,
            horizon: int = Query(20),
            signal_variant: str = Query(...),
        ) -> dict[str, object]:
            return dashboard.factor_series(
                run_id, factor_ref, horizon, signal_variant
            )

        @app.get("/api/v1/factor-runs/{run_id}/correlation")
        def factor_correlation(
            run_id: str,
            signal_variant: str = Query(...),
        ) -> dict[str, object]:
            return dashboard.factor_correlation(run_id, signal_variant)

        @app.get("/api/v1/factor-runs/{run_id}/industry-coverage")
        def factor_industry_coverage(run_id: str) -> dict[str, object]:
            return dashboard.factor_industry_coverage(run_id)

        @app.post("/api/v1/data/update-plans/preview")
        def data_update_plan(body: DataUpdatePlanRequest) -> dict[str, JsonValue]:
            return mutation.preview_data_update(
                start=body.start,
                end=body.end,
                datasets=body.datasets,
            )

        @app.post("/api/v1/data/updates", status_code=202)
        def data_update(request: Request, body: DataUpdateRequest) -> dict[str, object]:
            return mutation.enqueue_data_update(
                start=body.start,
                end=body.end,
                datasets=body.datasets,
                expected_plan_hash=body.plan_hash,
                request_id=request.state.request_id,
            )

        @app.get("/api/v1/experiments")
        def experiments(
            status: str | None = None,
            strategy_id: str | None = None,
            research_mark: str | None = None,
            page: int = Query(default=1, ge=1),
            page_size: int = Query(default=25, ge=1, le=200),
        ) -> dict[str, object]:
            return dashboard.experiment_list(
                status=status,
                strategy_id=strategy_id,
                research_mark=research_mark,
                page=page,
                page_size=page_size,
            )

        @app.post("/api/v1/experiments", status_code=202)
        def submit_experiment(
            request: Request,
            body: ExperimentSubmitRequest,
        ) -> dict[str, object]:
            return mutation.submit_experiment(
                body.config_yaml,
                request_id=request.state.request_id,
            )

        @app.get("/api/v1/experiments/{experiment_id}")
        def experiment_detail(experiment_id: str) -> dict[str, object]:
            return dashboard.experiment_detail(experiment_id)

        @app.delete("/api/v1/experiments/{experiment_id}")
        def delete_experiment(
            experiment_id: str, request: Request
        ) -> dict[str, object]:
            return mutation.delete_experiment(
                experiment_id,
                request_id=request.state.request_id,
            )

        @app.post("/api/v1/experiments/compare")
        def compare(body: CompareRequest) -> dict[str, object]:
            return dashboard.compare_experiments(body.experiment_ids)

        @app.patch("/api/v1/experiments/{experiment_id}/research")
        def update_research(
            experiment_id: str,
            request: Request,
            body: ResearchUpdateRequest,
        ) -> dict[str, object]:
            return mutation.update_research(
                experiment_id,
                mark=body.mark,
                tags=body.tags,
                note=body.note,
                request_id=request.state.request_id,
            )

        @app.post("/api/v1/experiments/{experiment_id}/clone", status_code=201)
        def clone_experiment(
            experiment_id: str,
            request: Request,
            body: ExperimentCloneRequest,
        ) -> dict[str, object]:
            return mutation.clone_experiment(
                experiment_id,
                submit=body.submit,
                priority=body.priority,
                request_id=request.state.request_id,
            )

        @app.get("/api/v1/experiments/{experiment_id}/backtest")
        def backtest(experiment_id: str) -> dict[str, object]:
            return dashboard.backtest(experiment_id)

        @app.get("/api/v1/experiments/{experiment_id}/holdings")
        def holdings(
            experiment_id: str,
            start: str | None = None,
            end: str | None = None,
            instrument_id: str | None = None,
            page: int = Query(default=1, ge=1),
            page_size: int = Query(default=50, ge=1, le=200),
        ) -> dict[str, object]:
            return dashboard.drilldown(
                experiment_id,
                kind="holdings",
                start=_RouteSupport.date_from_text(start),
                end=_RouteSupport.date_from_text(end),
                instrument_id=instrument_id,
                page=page,
                page_size=page_size,
            )

        @app.get("/api/v1/experiments/{experiment_id}/fills")
        def fills(
            experiment_id: str,
            start: str | None = None,
            end: str | None = None,
            instrument_id: str | None = None,
            page: int = Query(default=1, ge=1),
            page_size: int = Query(default=50, ge=1, le=200),
        ) -> dict[str, object]:
            return dashboard.drilldown(
                experiment_id,
                kind="fills",
                start=_RouteSupport.date_from_text(start),
                end=_RouteSupport.date_from_text(end),
                instrument_id=instrument_id,
                page=page,
                page_size=page_size,
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
        ) -> dict[str, object]:
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


class _RouteSupport:
    @staticmethod
    def date_from_text(value: str | None) -> date | None:
        if value is None:
            return None
        return date.fromisoformat(value)
