from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Never, cast

import pytest
from fastapi.testclient import TestClient

from quant_research.application.research import ResearchApplicationService
from quant_research.bootstrap.dashboard import DashboardBootstrap, _LocalNotebookProbe
from quant_research.dashboard.app import create_dashboard_app
from quant_research.dashboard.models import (
    DataUpdatePlanRequest,
    DataUpdateRequest,
    ExperimentSubmitRequest,
    MarketReviewBreadth,
    MarketReviewDataQuality,
    MarketReviewDates,
    MarketReviewIndustries,
    MarketReviewLiquidity,
    MarketReviewResponse,
    MarketReviewSentiment,
    MarketReviewValuation,
    QualityRunRequest,
)
from quant_research.dashboard.views import DashboardViewService
from quant_research.domain.enums import DatasetKind


class _Service:
    def market_review_dates(self) -> MarketReviewDates:
        return MarketReviewDates(
            catalog_hash="a" * 64,
            validated_at=datetime(2026, 8, 15, tzinfo=UTC),
            latest_trade_date=date(2026, 8, 14),
            dates=(date(2026, 8, 14),),
        )

    def market_review(
        self, trade_date: date | None, *, exclude_st: bool
    ) -> MarketReviewResponse:
        return MarketReviewResponse(
            trade_date=trade_date or date(2026, 8, 14),
            catalog_hash="a" * 64,
            validated_at=datetime(2026, 8, 15, tzinfo=UTC),
            exclude_st=exclude_st,
            data_quality=MarketReviewDataQuality(
                expected_count=1,
                priced_count=1,
                suspended_count=0,
                st_count=0,
                missing_bar_count=0,
                coverage_rate=1.0,
            ),
            indexes=(),
            liquidity=MarketReviewLiquidity(
                amount=1.0,
                change_vs_previous=None,
                average_5d=1.0,
                average_20d=1.0,
                percentile_20d=1.0,
                series=(),
            ),
            breadth=MarketReviewBreadth(
                up_count=1,
                down_count=0,
                flat_count=0,
                advance_rate=1.0,
                net_advance_count=1,
                equal_weight_return=0.01,
                median_return=0.01,
                p10_return=0.01,
                p25_return=0.01,
                p75_return=0.01,
                p90_return=0.01,
                buckets=(),
            ),
            sentiment=MarketReviewSentiment(
                limit_up_count=0,
                limit_down_count=0,
                broken_limit_up_count=0,
                one_price_limit_up_count=0,
                eligible_count=1,
                unresolved_count=0,
                coverage_rate=1.0,
                events=(),
                note="test",
            ),
            industries=MarketReviewIndustries(
                available=False,
                taxonomy=None,
                coverage_rate=None,
                unavailable_reason="test",
                items=(),
            ),
            valuation=MarketReviewValuation(
                metrics=(), turnover_median=None, turnover_valid_count=0
            ),
        )

    def factor_catalog(self) -> dict[str, object]:
        return {"items": [], "horizons": [1, 5, 20]}

    def factor_studies(self, page: int, page_size: int) -> dict[str, object]:
        return {"items": [], "page": page, "page_size": page_size, "total": 0}

    def factor_series(
        self, run_id: str, factor_ref: str, horizon: int, signal_variant: str
    ) -> dict[str, object]:
        return {
            "run_id": run_id,
            "factor_ref": factor_ref,
            "horizon": horizon,
            "signal_variant": signal_variant,
            "ic": [{"pearson_ic": 0.2, "rank_ic": 0.1}],
        }

    def health(self) -> dict[str, object]:
        return {"status": "ok"}

    def overview(self) -> dict[str, object]:
        return {
            "gate": {
                "status": "READY",
                "reason": "VALIDATED",
                "catalog_hash": "a" * 64,
                "validated_catalog_hash": "a" * 64,
                "quality_run_id": "run-gate",
                "updated_at": "2026-08-14T10:00:00+00:00",
                "validated_at": "2026-08-14T10:00:00+00:00",
            },
            "freshness": {
                "status": "CURRENT",
                "counts": {
                    "CURRENT": 8,
                    "STALE": 0,
                    "MISSING": 0,
                    "UNKNOWN": 0,
                },
                "evaluated_at": "2026-08-15T02:00:00+00:00",
                "latest_complete_session": "2026-08-14",
            },
            "latest_trade_date": "2026-08-14",
            "dataset_count": 8,
            "gate_quality_run": None,
            "latest_quality_run": None,
            "worker": None,
            "last_successful_update": None,
            "tasks": {
                "status_counts": {
                    "QUEUED": 0,
                    "RUNNING": 0,
                    "SUCCEEDED": 0,
                    "FAILED": 0,
                    "CANCEL_REQUESTED": 0,
                    "CANCELLED": 0,
                    "ORPHANED": 0,
                },
                "active": (),
            },
            "experiments": {
                "status_counts": {
                    "CREATED": 0,
                    "QUEUED": 0,
                    "RUNNING": 0,
                    "SUCCEEDED": 0,
                    "FAILED": 0,
                    "CANCELLED": 0,
                },
                "recent": (),
                "benchmarks": (
                    {"strategy_id": "etf_rotation", "experiment": None},
                    {"strategy_id": "stock_multifactor", "experiment": None},
                ),
            },
        }

    def data_catalog(self) -> dict[str, object]:
        return {"datasets": []}

    def quality(self) -> dict[str, object]:
        return {"run": None, "issues": []}

    def data_summary(self) -> dict[str, object]:
        return {
            "gate": {
                "status": "READY",
                "reason": "VALIDATED",
                "catalog_hash": "a" * 64,
                "validated_catalog_hash": "a" * 64,
                "quality_run_id": "run-gate",
                "updated_at": "2026-08-14T10:00:00+00:00",
                "validated_at": "2026-08-14T10:00:00+00:00",
            },
            "freshness": {
                "status": "STALE",
                "counts": {"CURRENT": 7, "STALE": 1, "MISSING": 0, "UNKNOWN": 0},
                "evaluated_at": "2026-08-14T10:00:00+00:00",
                "latest_complete_session": "2026-08-13",
            },
            "gate_quality_run": None,
            "latest_quality_run": None,
            "active_update": {
                "id": "task-1",
                "experiment_id": None,
                "factor_run_id": None,
                "task_type": "DATA_UPDATE",
                "status": "RUNNING",
                "priority": 0,
                "progress": {"stage": "LOCALIZE"},
                "created_at": "2026-08-15T02:14:20+00:00",
                "started_at": "2026-08-15T02:14:27+00:00",
                "updated_at": "2026-08-15T02:14:30+00:00",
                "heartbeat_at": "2026-08-15T02:14:30+00:00",
                "completed_at": None,
                "worker_id": "worker-1",
                "error": None,
                "result": None,
            },
            "last_successful_update": None,
            "worker": None,
            "active_research_task_count": 1,
        }

    def data_datasets(self) -> dict[str, object]:
        return {"items": ()}

    def data_dataset(self, dataset: str) -> dict[str, object]:
        raise ValueError(f"unknown dataset: {dataset}")

    def quality_runs(self, **_: object) -> dict[str, object]:
        return {"items": (), "page": 1, "page_size": 25, "total": 0}

    def quality_run(self, run_id: str) -> dict[str, object]:
        raise ValueError(f"unknown quality run: {run_id}")

    def experiment_list(self, **_: object) -> dict[str, object]:
        return {"items": [], "page": 1, "page_size": 25, "total": 0}

    def task_list(self, **_: object) -> dict[str, object]:
        return {"items": [], "page": 1, "page_size": 25, "total": 0}

    def task_detail(self, task_id: str) -> dict[str, object]:
        return {
            "id": task_id,
            "payload": {"end": "2026-08-15", "start": "2026-08-01"},
            "attempts": [],
        }


class _NotebookProbe:
    def __init__(self, ready: bool) -> None:
        self._ready = ready

    def is_ready(self) -> bool:
        return self._ready


class _Commands:
    def __init__(self) -> None:
        self.preview_arguments: dict[str, object] | None = None
        self.enqueue_arguments: dict[str, object] | None = None
        self.validation_arguments: dict[str, object] | None = None

    def preview_data_update(self, **arguments: object) -> dict[str, object]:
        self.preview_arguments = arguments
        return {
            "window_mode": "AUTO_INCREMENTAL",
            "planned_at": "2026-08-15T03:00:00+00:00",
            "start": "2026-08-01",
            "end": "2026-08-15",
            "dataset_windows": [],
            "plan_hash": "a" * 64,
        }

    def enqueue_data_update(self, **arguments: object) -> dict[str, object]:
        self.enqueue_arguments = arguments
        return {"task_id": "task-1", "status": "QUEUED"}

    def enqueue_data_validation(self, **arguments: object) -> dict[str, object]:
        self.validation_arguments = arguments
        dataset = arguments.get("dataset")
        return {
            "task_id": "quality-task-1",
            "request_id": arguments["request_id"],
            "status": "QUEUED",
            "scope": "ALL" if dataset is None else "DATASET",
            **({} if dataset is None else {"dataset": cast(DatasetKind, dataset).value}),
        }

    def create_factor_study(self, name: str, config: object) -> dict[str, object]:
        return {"id": "study-1", "name": name, "config": config}

    def enqueue_factor_run(self, study_id: str, **_: object) -> dict[str, object]:
        return {"run_id": "run-1", "task_id": "task-2", "status": "QUEUED"}

    def delete_task(self, task_id: str, **_: object) -> dict[str, object]:
        return {"task_id": task_id, "status": "DELETED"}

    def delete_experiment(self, experiment_id: str, **_: object) -> dict[str, object]:
        return {"experiment_id": experiment_id, "status": "DELETED"}

    def submit_experiment(self, config_yaml: str, **_: object) -> dict[str, object]:
        assert "strategy_id: etf_rotation" in config_yaml
        return {
            "experiment_id": "experiment-1",
            "task_id": "task-3",
            "status": "QUEUED",
        }


def _client(
    tmp_path: Path,
    commands: _Commands | None = None,
    *,
    notebook_ready: bool = False,
) -> TestClient:
    app = create_dashboard_app(
        service=cast(DashboardViewService, _Service()),
        commands=cast(ResearchApplicationService, commands or _Commands()),
        notebook_probe=_NotebookProbe(notebook_ready),
        static_dir=tmp_path,
        allowed_hosts=("testserver",),
    )
    return TestClient(app)


def test_read_api_is_local_and_returns_dashboard_payload(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        response = client.get("/api/v1/overview")
    assert response.status_code == 200
    assert response.json()["gate"]["status"] == "READY"
    assert response.json()["tasks"]["status_counts"]["FAILED"] == 0
    assert response.headers["x-content-type-options"] == "nosniff"


@pytest.mark.parametrize(
    ("ready", "expected"),
    ((True, "READY"), (False, "UNAVAILABLE")),
)
def test_notebook_status_api_reports_injected_probe(
    tmp_path: Path,
    ready: bool,
    expected: str,
) -> None:
    with _client(tmp_path, notebook_ready=ready) as client:
        response = client.get("/api/v1/notebook/status")

    assert response.status_code == 200
    assert response.json() == {"status": expected}


def test_local_notebook_probe_turns_timeout_into_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_timeout(*_: object, **__: object) -> Never:
        raise TimeoutError

    monkeypatch.setattr("urllib.request.urlopen", raise_timeout)

    assert _LocalNotebookProbe(timeout=0.01).is_ready() is False


def test_task_detail_api_exposes_read_only_parameters(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        response = client.get("/api/v1/tasks/task-1")
    assert response.status_code == 200
    assert response.json() == {
        "id": "task-1",
        "payload": {"end": "2026-08-15", "start": "2026-08-01"},
        "attempts": [],
    }


def test_task_delete_api_is_a_controlled_mutation(tmp_path: Path) -> None:
    headers = {"X-Request-ID": "delete-request", "Origin": "http://testserver"}
    with _client(tmp_path) as client:
        response = client.request(
            "DELETE",
            "/api/v1/tasks/task-1",
            json={},
            headers=headers,
        )
    assert response.status_code == 200
    assert response.json() == {"task_id": "task-1", "status": "DELETED"}


def test_experiment_delete_api_is_a_controlled_mutation(tmp_path: Path) -> None:
    headers = {"X-Request-ID": "delete-experiment", "Origin": "http://testserver"}
    with _client(tmp_path) as client:
        response = client.request(
            "DELETE",
            "/api/v1/experiments/experiment-1",
            json={},
            headers=headers,
        )
    assert response.status_code == 200
    assert response.json() == {
        "experiment_id": "experiment-1",
        "status": "DELETED",
    }


def test_market_review_api_exposes_dates_and_scope(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        dates = client.get("/api/v1/market-review/dates")
        review = client.get(
            "/api/v1/market-review",
            params={"trade_date": "2026-08-14", "exclude_st": "true"},
        )

    assert dates.status_code == 200
    assert dates.json()["latest_trade_date"] == "2026-08-14"
    assert review.status_code == 200
    assert review.json()["trade_date"] == "2026-08-14"
    assert review.json()["exclude_st"] is True


def test_default_dashboard_composes_text_submission_dependencies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("QUANT_DATA_ROOT", str(tmp_path / "data"))
    app = DashboardBootstrap.build_app(
        static_dir=tmp_path,
        allowed_hosts=("testserver",),
    )

    with TestClient(app) as client:
        response = client.get("/api/v1/health")

    assert response.status_code == 200


def test_write_api_requires_json_request_id_and_same_origin(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        missing = client.post("/api/v1/data/updates", json={})
        foreign = client.post(
            "/api/v1/data/updates",
            json={},
            headers={"X-Request-ID": "request-1", "Origin": "https://example.com"},
        )
        accepted = client.post(
            "/api/v1/data/updates",
            json={"plan_hash": "a" * 64},
            headers={"X-Request-ID": "request-2", "Origin": "http://testserver"},
        )
        preview = client.post(
            "/api/v1/data/update-plans/preview",
            json={},
            headers={"X-Request-ID": "request-3", "Origin": "http://testserver"},
        )
    assert missing.status_code == 400
    assert missing.json()["error"]["code"] == "REQUEST_ID_REQUIRED"
    assert foreign.status_code == 403
    assert foreign.json()["error"]["code"] == "ORIGIN_REJECTED"
    assert accepted.status_code == 202
    assert accepted.json() == {"task_id": "task-1", "status": "QUEUED"}
    assert preview.status_code == 200
    assert preview.json()["plan_hash"] == "a" * 64


def test_data_update_api_normalizes_and_forwards_selected_datasets(
    tmp_path: Path,
) -> None:
    commands = _Commands()
    headers = {"X-Request-ID": "request-1", "Origin": "http://testserver"}
    selection = ["daily_basic", "daily_bar"]

    with _client(tmp_path, commands) as client:
        preview = client.post(
            "/api/v1/data/update-plans/preview",
            json={"datasets": selection},
            headers=headers,
        )
        submitted = client.post(
            "/api/v1/data/updates",
            json={"datasets": selection, "plan_hash": "a" * 64},
            headers=headers,
        )

    assert preview.status_code == 200
    assert submitted.status_code == 202
    expected = (DatasetKind.DAILY_BAR, DatasetKind.DAILY_BASIC)
    assert commands.preview_arguments == {
        "start": None,
        "end": None,
        "datasets": expected,
    }
    assert commands.enqueue_arguments == {
        "start": None,
        "end": None,
        "datasets": expected,
        "expected_plan_hash": "a" * 64,
        "request_id": "request-1",
    }


@pytest.mark.parametrize(
    ("payload", "expected_dataset", "expected_scope"),
    (
        ({}, None, "ALL"),
        ({"dataset": "daily_bar"}, DatasetKind.DAILY_BAR, "DATASET"),
    ),
)
def test_quality_run_api_enqueues_all_or_one_dataset(
    tmp_path: Path,
    payload: dict[str, str],
    expected_dataset: DatasetKind | None,
    expected_scope: str,
) -> None:
    commands = _Commands()
    headers = {"X-Request-ID": "request-1", "Origin": "http://testserver"}

    with _client(tmp_path, commands) as client:
        response = client.post(
            "/api/v1/data/quality-runs",
            json=payload,
            headers=headers,
        )

    assert response.status_code == 202
    assert response.json()["scope"] == expected_scope
    assert commands.validation_arguments == {
        "dataset": expected_dataset,
        "request_id": "request-1",
    }


def test_quality_run_api_rejects_an_unknown_dataset(tmp_path: Path) -> None:
    headers = {"X-Request-ID": "request-1", "Origin": "http://testserver"}
    with _client(tmp_path) as client:
        response = client.post(
            "/api/v1/data/quality-runs",
            json={"dataset": "not-a-dataset"},
            headers=headers,
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "REQUEST_VALIDATION_FAILED"


def test_spa_fallback_never_swallows_unknown_api_routes(tmp_path: Path) -> None:
    (tmp_path / "index.html").write_text("<main>Q·LAB</main>", encoding="utf-8")
    with _client(tmp_path) as client:
        page = client.get("/experiments/experiment-1")
        missing_api = client.get("/api/v1/not-real")
    assert page.status_code == 200
    assert "Q·LAB" in page.text
    assert missing_api.status_code == 404


def test_data_center_uses_new_contract_and_removes_legacy_routes(
    tmp_path: Path,
) -> None:
    with _client(tmp_path) as client:
        summary = client.get("/api/v1/data/summary")
        datasets = client.get("/api/v1/data/datasets")
        quality_runs = client.get("/api/v1/data/quality-runs")
        legacy_catalog = client.get("/api/v1/data/catalog")
        legacy_quality = client.get("/api/v1/data/quality")

    assert summary.status_code == 200
    assert summary.json()["gate"]["status"] == "READY"
    assert summary.json()["freshness"]["status"] == "STALE"
    assert summary.json()["active_update"]["started_at"] == (
        "2026-08-15T02:14:27+00:00"
    )
    assert datasets.json() == {"items": []}
    assert quality_runs.json()["items"] == []
    assert legacy_catalog.status_code == 404
    assert legacy_quality.status_code == 404


def test_factor_study_creation_and_run_enqueue_are_controlled_mutations(
    tmp_path: Path,
) -> None:
    headers = {"X-Request-ID": "factor-request", "Origin": "http://testserver"}
    payload = {
        "name": "价值因子",
        "factor_refs": ["earnings_yield_ttm"],
        "start_date": "2024-01-02",
        "end_date": "2024-12-31",
        "industry": {
            "taxonomy": "证监会行业分类",
            "unclassified_policy": "EXCLUDE",
        },
    }
    with _client(tmp_path) as client:
        created = client.post("/api/v1/factor-studies", json=payload, headers=headers)
        queued = client.post(
            "/api/v1/factor-studies/study-1/runs", json={}, headers=headers
        )
    assert created.status_code == 201
    assert created.json()["id"] == "study-1"
    assert created.json()["config"]["industry"] == payload["industry"]
    assert queued.status_code == 202
    assert queued.json()["run_id"] == "run-1"


def test_factor_series_api_exposes_unified_ic_payload(tmp_path: Path) -> None:
    with _client(tmp_path) as client:
        response = client.get(
            "/api/v1/factor-runs/run-1/series",
            params={"factor_ref": "momentum_120_20", "horizon": 20},
        )
        valid = client.get(
            "/api/v1/factor-runs/run-1/series",
            params={
                "factor_ref": "momentum_120_20",
                "horizon": 20,
                "signal_variant": "DIRECTION_ADJUSTED",
            },
        )

    assert response.status_code == 422
    assert valid.status_code == 200
    assert valid.json()["ic"] == [{"pearson_ic": 0.2, "rank_ic": 0.1}]
    assert valid.json()["signal_variant"] == "DIRECTION_ADJUSTED"
    assert "rank_ic" not in valid.json()


def test_dashboard_submits_yaml_text_without_accepting_a_path(tmp_path: Path) -> None:
    headers = {"X-Request-ID": "experiment-request", "Origin": "http://testserver"}
    with _client(tmp_path) as client:
        response = client.post(
            "/api/v1/experiments",
            json={"config_yaml": "strategy_id: etf_rotation\n"},
            headers=headers,
        )
        rejected = client.post(
            "/api/v1/experiments",
            json={"config_path": "C:/secrets/experiment.yaml"},
            headers=headers,
        )

    assert response.status_code == 202
    assert response.json() == {
        "experiment_id": "experiment-1",
        "task_id": "task-3",
        "status": "QUEUED",
    }
    assert rejected.status_code == 422
    assert rejected.json()["error"]["code"] == "REQUEST_VALIDATION_FAILED"


def test_data_update_request_requires_a_complete_ordered_window() -> None:
    DataUpdatePlanRequest.model_validate({"start": "2026-01-01", "end": "2026-01-31"})
    DataUpdateRequest.model_validate(
        {
            "start": "2026-01-01",
            "end": "2026-01-31",
            "plan_hash": "a" * 64,
        }
    )
    selected = DataUpdatePlanRequest.model_validate_json(
        '{"datasets":["daily_basic","daily_bar"]}'
    )
    assert selected.datasets == (
        DatasetKind.DAILY_BAR,
        DatasetKind.DAILY_BASIC,
    )
    for payload in (
        {"start": "2026-01-01"},
        {"start": "2026-02-01", "end": "2026-01-01"},
        {"datasets": ()},
        {"datasets": (DatasetKind.DAILY_BAR, DatasetKind.DAILY_BAR)},
    ):
        try:
            DataUpdatePlanRequest.model_validate(payload)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid update window was accepted")
    for payload in (
        '{"datasets":["not-a-dataset"]}',
        '{"datasets":[1]}',
    ):
        try:
            DataUpdatePlanRequest.model_validate_json(payload)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid dataset name was accepted")

    assert QualityRunRequest.model_validate_json("{}").dataset is None
    assert (
        QualityRunRequest.model_validate_json('{"dataset":"daily_bar"}').dataset
        is DatasetKind.DAILY_BAR
    )


def test_experiment_submit_request_limits_utf8_payload_to_one_mibibyte() -> None:
    ExperimentSubmitRequest.model_validate({"config_yaml": "a" * 1_048_576})
    try:
        ExperimentSubmitRequest.model_validate({"config_yaml": "沪" * 400_000})
    except ValueError:
        pass
    else:
        raise AssertionError("oversized UTF-8 experiment YAML was accepted")
