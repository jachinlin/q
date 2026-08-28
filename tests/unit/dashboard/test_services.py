from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

from quant_research.config import Settings
from quant_research.dashboard.market_review import MarketReviewService
from quant_research.dashboard.models import (
    DatasetListResponse,
    DataSummaryResponse,
    OverviewResponse,
    QualityRunDetailResponse,
)
from quant_research.dashboard.views import DashboardViewService
from quant_research.data.catalog import DATASET_CATALOG
from quant_research.data.quality.models import (
    QualityIssue,
    QualityRuleResult,
    QualityRuleStatus,
    QualityRunSpec,
)
from quant_research.data.repository import ResearchDataRepository
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.repositories import MetadataRepository
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.infrastructure.tushare.routing import TUSHARE_ROUTES
from quant_research.logging import MAX_TASK_LOG_BYTES, LogContext, TaskLogManager
from quant_research.tasks.models import TaskOutcome, TaskProgress, TaskStatus

_REPOSITORY = cast(ResearchDataRepository, object())


def test_data_datasets_lists_every_defined_dataset_before_curate(
    tmp_path: Path,
) -> None:
    settings = Settings(
        timezone=ZoneInfo("Asia/Shanghai"),
        data_root=tmp_path,
        max_partition_size=100,
    )
    upgrade_database(settings.state_db)
    engine = create_sqlite_engine(settings.state_db)
    try:
        payload = DashboardViewService(
            engine,
            settings,
            _REPOSITORY,
            cast(MarketReviewService, object()),
            TUSHARE_ROUTES,
        ).data_datasets()
    finally:
        engine.dispose()

    datasets = payload["items"]
    assert isinstance(datasets, tuple)
    assert [item["dataset"] for item in datasets] == [
        dataset.value for dataset in DATASET_CATALOG
    ]
    assert all(
        item[field] is None
        for item in datasets
        for field in (
            "start_date",
            "end_date",
            "content_hash",
            "updated_at",
        )
    )
    assert all(item["partition_count"] == 0 for item in datasets)
    assert all(item["row_count"] == 0 for item in datasets)
    assert (
        next(item for item in datasets if item["dataset"] == "stock_daily_bar")["source"]
        == "tushare"
    )
    DatasetListResponse.model_validate(payload)


def test_data_summary_separates_gate_and_freshness_without_run_evidence(
    tmp_path: Path,
) -> None:
    settings = Settings(
        timezone=ZoneInfo("Asia/Shanghai"),
        data_root=tmp_path,
        max_partition_size=100,
    )
    upgrade_database(settings.state_db)
    engine = create_sqlite_engine(settings.state_db)
    try:
        payload = DashboardViewService(
            engine,
            settings,
            _REPOSITORY,
            cast(MarketReviewService, object()),
            TUSHARE_ROUTES,
        ).data_summary()
    finally:
        engine.dispose()

    response = DataSummaryResponse.model_validate(payload)
    assert response.initialization.status == "NOT_STARTED"
    assert response.initialization.years is None
    assert response.gate.status == "BLOCKED"
    assert response.gate.reason == "NEVER_VALIDATED"
    assert response.freshness.status == "MISSING"
    assert response.gate_quality_run is None
    assert response.latest_quality_run is None


def test_data_summary_exposes_resumable_initialization_and_active_task(
    tmp_path: Path,
) -> None:
    settings = Settings(
        timezone=ZoneInfo("Asia/Shanghai"),
        data_root=tmp_path,
        max_partition_size=100,
    )
    upgrade_database(settings.state_db)
    engine = create_sqlite_engine(settings.state_db)
    repository = MetadataRepository(engine)
    started_at = datetime(2026, 8, 15, 3, tzinfo=UTC)
    repository.begin_data_initialization(
        years=5,
        start_date=date(2021, 8, 14),
        end_date=date(2026, 8, 14),
        started_at=started_at,
    )
    task_id = TaskQueue(
        engine,
        clock=lambda: started_at,
        task_log_root=settings.data_root / "state" / "task-logs",
    ).enqueue("DATA_BOOTSTRAP", {"years": 5}, 0)
    try:
        payload = DashboardViewService(
            engine,
            settings,
            _REPOSITORY,
            cast(MarketReviewService, object()),
            TUSHARE_ROUTES,
        ).data_summary()
    finally:
        engine.dispose()

    response = DataSummaryResponse.model_validate(payload)
    assert response.initialization.status == "IN_PROGRESS"
    assert response.initialization.years == 5
    assert response.initialization.start_date == "2021-08-14"
    assert response.initialization.end_date == "2026-08-14"
    assert response.active_update is not None
    assert response.active_update.id == task_id
    assert response.active_update.task_type == "DATA_BOOTSTRAP"


def test_quality_run_detail_marks_unpersisted_legacy_results_unknown(
    tmp_path: Path,
) -> None:
    settings = Settings(
        timezone=ZoneInfo("Asia/Shanghai"),
        data_root=tmp_path,
        max_partition_size=100,
    )
    upgrade_database(settings.state_db)
    engine = create_sqlite_engine(settings.state_db)
    repository = MetadataRepository(engine)
    now = datetime(2026, 8, 14, 10, tzinfo=UTC)
    run = repository.register_quality_run(
        QualityRunSpec(
            dataset_hashes={DatasetKind.STOCK_DAILY_BAR.value: "b" * 64},
            input_hash="a" * 64,
            scope="DATASET",
            started_at=now,
            completed_at=now,
            issues=(
                QualityIssue(
                    rule_id="primary_key_duplicate",
                    severity=Severity.FATAL,
                    dataset=DatasetKind.STOCK_DAILY_BAR,
                    scope={"partition": "year=2026"},
                    actual=2,
                    threshold=0,
                    message="duplicate key",
                    remediation="rebuild partition",
                ),
            ),
        )
    )
    service = DashboardViewService(
        engine,
        settings,
        _REPOSITORY,
        cast(MarketReviewService, object()),
        TUSHARE_ROUTES,
    )
    try:
        response = QualityRunDetailResponse.model_validate(
            service.quality_run(str(run.id))
        )
        complete = repository.register_quality_run(
            QualityRunSpec(
                dataset_hashes={DatasetKind.STOCK_DAILY_BAR.value: "c" * 64},
                input_hash="d" * 64,
                scope="DATASET",
                started_at=now,
                completed_at=now,
                issues=(),
                rule_results=(
                    QualityRuleResult(
                        rule_id="canonical_schema",
                        dataset=DatasetKind.STOCK_DAILY_BAR,
                        status=QualityRuleStatus.PASS,
                        severity=Severity.FATAL,
                        title="运行时标题",
                        description="运行时规则说明。",
                        pass_criterion="不匹配分区数为 0。",
                        scope={},
                        actual=0,
                        threshold=0,
                    ),
                ),
                results_complete=True,
            )
        )
        complete_response = QualityRunDetailResponse.model_validate(
            service.quality_run(str(complete.id))
        )
    finally:
        engine.dispose()

    assert response.results_complete is False
    assert response.result_counts.FAIL == 1
    assert response.result_counts.UNKNOWN > 0
    failed = next(
        item
        for item in response.rule_results
        if item.rule_id == "primary_key_duplicate"
    )
    assert failed.status == "FAIL"
    assert failed.evidence == "LEGACY_ISSUE"
    assert failed.title == "主键唯一"
    unknown = next(item for item in response.rule_results if item.status == "UNKNOWN")
    assert unknown.evidence == "MISSING"
    assert unknown.actual is None
    assert complete_response.results_complete is True
    assert complete_response.result_counts.PASS == 1
    assert complete_response.result_counts.UNKNOWN == 0
    assert complete_response.rule_results[0].title == "运行时标题"
    assert complete_response.rule_results[0].evidence == "RUN_SNAPSHOT"


def test_overview_uses_global_task_counts_without_legacy_research_payloads(
    tmp_path: Path,
) -> None:
    settings = Settings(
        timezone=ZoneInfo("Asia/Shanghai"),
        data_root=tmp_path,
        max_partition_size=100,
    )
    upgrade_database(settings.state_db)
    engine = create_sqlite_engine(settings.state_db)
    queue = TaskQueue(
        engine,
        task_log_root=settings.data_root / "state" / "task-logs",
    )
    for index in range(7):
        queue.enqueue(
            "EXPERIMENT_RUN",
            {},
            0,
            subject_kind="EXPERIMENT_RUN",
            subject_id=f"run-{index}",
        )

    service = DashboardViewService(
        engine,
        settings,
        _REPOSITORY,
        cast(MarketReviewService, object()),
        TUSHARE_ROUTES,
    )
    try:
        response = OverviewResponse.model_validate(service.overview())
    finally:
        engine.dispose()

    assert response.tasks.status_counts.QUEUED == 7
    assert len(response.tasks.active) == 5
    assert all(item.subject_kind == "EXPERIMENT_RUN" for item in response.tasks.active)


def test_task_views_expose_global_counts_runtime_and_structured_diagnostic(
    tmp_path: Path,
) -> None:
    settings = Settings(
        timezone=ZoneInfo("Asia/Shanghai"),
        data_root=tmp_path,
        max_partition_size=100,
    )
    upgrade_database(settings.state_db)
    engine = create_sqlite_engine(settings.state_db)
    now = datetime(2026, 8, 15, 3, tzinfo=UTC)
    log_root = settings.data_root / "state" / "task-logs"
    queue = TaskQueue(engine, clock=lambda: now, task_log_root=log_root)
    queued_id = queue.enqueue("DATA_UPDATE", {}, 0)
    failed_id = queue.enqueue(
        "TEST_FAILURE",
        {
            "z_nested": {"enabled": True, "items": [1, "two"]},
            "api_token": "direct-secret-value",
            "label": "original task parameter",
            "start": "2026-08-01",
        },
        1,
    )
    claimed = queue.claim("worker-1", now)
    assert claimed is not None
    assert claimed.id == failed_id
    manager = TaskLogManager(
        diagnostic_root=log_root,
        artifact_root=settings.artifact_root,
    )
    context = LogContext(
        task_id=claimed.id,
        attempt_id=claimed.attempt_id,
        worker_id=claimed.worker_id,
    )
    queue.heartbeat(
        claimed.attempt_id,
        claimed.worker_id,
        TaskProgress(
            stage="ANALYZE_FACTORS",
            completed=2,
            total=4,
            message="正在构建远期收益标签",
            context={
                "substage": "BUILD_FORWARD_RETURNS",
                "substage_state": "STARTED",
            },
        ),
        now,
    )
    with manager.open(context) as task_log:
        queue.bind_log_path(
            claimed.attempt_id,
            claimed.worker_id,
            str(task_log.path),
        )
        task_log.logger.emit("INFO", "unparseable-neighbor")
        task_log.logger.emit(
            "ERROR",
            "task.handler_failed",
            context={
                "exception_type": "quant_research.domain.errors.QuantError",
                "exception_message": "validated catalog is stale",
                "retryable": False,
                "remediation": "run validate-all before retrying",
                "traceback": "Traceback: catalog is stale",
                "last_progress": {
                    "stage": "ANALYZE_FACTORS",
                    "completed": 2,
                    "total": 4,
                    "message": "正在重新计算研究因子",
                    "context": {
                        "substage": "COMPUTE_FACTORS",
                        "substage_state": "STARTED",
                    },
                },
            },
            error_code="DATA_HASH_DRIFT",
            stage="VALIDATE",
        )
    queue.finish(
        claimed.attempt_id,
        claimed.worker_id,
        TaskOutcome(
            status=TaskStatus.FAILED,
            error={"code": "DATA_HASH_DRIFT", "retryable": False},
        ),
    )
    research_run_id = "research-run-0001"
    research_task_id = queue.enqueue(
        "EXPERIMENT_RUN",
        {"run_id": research_run_id, "config_hash": "a" * 64},
        0,
        subject_kind="EXPERIMENT_RUN",
        subject_id=research_run_id,
    )
    service = DashboardViewService(
        engine,
        settings,
        _REPOSITORY,
        cast(MarketReviewService, object()),
        TUSHARE_ROUTES,
    )
    try:
        filtered = service.task_list(status="FAILED", page=1, page_size=1)
        assert filtered["total"] == 1
        assert filtered["status_counts"] == {
            "QUEUED": 2,
            "RUNNING": 0,
            "SUCCEEDED": 0,
            "FAILED": 1,
            "CANCEL_REQUESTED": 0,
            "CANCELLED": 0,
            "ORPHANED": 0,
        }
        item = filtered["items"][0]
        assert item["started_at"] == now.isoformat()
        assert item["subject_kind"] is None
        assert item["subject_id"] is None
        assert "payload" not in item

        all_tasks = service.task_list(status=None, page=1, page_size=10)
        tasks_by_id = {item["id"]: item for item in all_tasks["items"]}
        assert tasks_by_id[research_task_id]["subject_kind"] == "EXPERIMENT_RUN"
        assert tasks_by_id[research_task_id]["subject_id"] == research_run_id
        assert tasks_by_id[queued_id]["subject_kind"] is None
        assert all("payload" not in task for task in tasks_by_id.values())

        detail = service.task_detail(failed_id)
        assert detail["subject_kind"] is None
        assert detail["subject_id"] is None
        assert detail["payload"] == {
            "api_token": "direct-secret-value",
            "label": "original task parameter",
            "start": "2026-08-01",
            "z_nested": {"enabled": True, "items": [1, "two"]},
        }
        assert list(cast(dict[str, object], detail["payload"])) == [
            "api_token",
            "label",
            "start",
            "z_nested",
        ]
        assert service.task_detail(queued_id)["payload"] == {}
        research_detail = service.task_detail(research_task_id)
        assert research_detail["subject_kind"] == "EXPERIMENT_RUN"
        assert research_detail["subject_id"] == research_run_id
        attempt = detail["attempts"][0]
        assert attempt["has_log"] is True
        payload = service.task_log(failed_id, attempt["id"], tail_lines=1)
        assert payload["available"] is True
        assert payload["total_lines"] == 2
        assert payload["truncated"] is True
        assert len(payload["lines"]) == 1
        assert payload["diagnostic"] == {
            "code": "DATA_HASH_DRIFT",
            "message": "validated catalog is stale",
            "exception_type": "quant_research.domain.errors.QuantError",
            "stage": "VALIDATE",
            "substage": "COMPUTE_FACTORS",
            "retryable": False,
            "remediation": "run validate-all before retrying",
            "traceback": "Traceback: catalog is stale",
        }
        assert queued_id != failed_id
    finally:
        engine.dispose()


def test_task_log_degrades_for_missing_file_and_ignores_malformed_jsonl(
    tmp_path: Path,
) -> None:
    settings = Settings(
        timezone=ZoneInfo("Asia/Shanghai"),
        data_root=tmp_path,
        max_partition_size=100,
    )
    upgrade_database(settings.state_db)
    engine = create_sqlite_engine(settings.state_db)
    now = datetime(2026, 8, 15, 3, tzinfo=UTC)
    log_root = settings.data_root / "state" / "task-logs"
    queue = TaskQueue(engine, clock=lambda: now, task_log_root=log_root)
    task_id = queue.enqueue("TEST_FAILURE", {}, 0)
    claimed = queue.claim("worker-1", now)
    assert claimed is not None
    manager = TaskLogManager(
        diagnostic_root=log_root,
        artifact_root=settings.artifact_root,
    )
    context = LogContext(
        task_id=claimed.id,
        attempt_id=claimed.attempt_id,
        worker_id=claimed.worker_id,
    )
    queue.heartbeat(
        claimed.attempt_id,
        claimed.worker_id,
        TaskProgress(
            stage="ANALYZE_FACTORS",
            completed=2,
            total=4,
            message="正在构建远期收益标签",
            context={
                "substage": "BUILD_FORWARD_RETURNS",
                "substage_state": "STARTED",
            },
        ),
        now,
    )
    with manager.open(context) as task_log:
        queue.bind_log_path(
            claimed.attempt_id,
            claimed.worker_id,
            str(task_log.path),
        )
        path = task_log.path
        task_log.logger.emit(
            "ERROR",
            "task.handler_failed",
            context={"exception_message": "boom"},
            error_code="WORKER_UNHANDLED_ERROR",
        )
    path.write_text(
        path.read_text(encoding="utf-8") + "not-json\n",
        encoding="utf-8",
    )
    queue.finish(
        claimed.attempt_id,
        claimed.worker_id,
        TaskOutcome(
            status=TaskStatus.FAILED,
            error={"code": "WORKER_UNHANDLED_ERROR", "retryable": False},
        ),
    )
    service = DashboardViewService(
        engine,
        settings,
        _REPOSITORY,
        cast(MarketReviewService, object()),
        TUSHARE_ROUTES,
    )
    try:
        parsed = service.task_log(task_id, claimed.attempt_id, tail_lines=500)
        assert parsed["diagnostic"]["message"] == "boom"
        assert parsed["lines"][-1] == "not-json"

        path.unlink()
        missing = service.task_log(task_id, claimed.attempt_id, tail_lines=500)
        assert missing["available"] is False
        assert missing["lines"] == []
        assert missing["diagnostic"] == {
            "code": "WORKER_UNHANDLED_ERROR",
            "message": None,
            "exception_type": None,
            "stage": "ANALYZE_FACTORS",
            "substage": "BUILD_FORWARD_RETURNS",
            "retryable": False,
            "remediation": "查看该次尝试日志，修复原因后再安全重试。",
            "traceback": None,
        }
    finally:
        engine.dispose()


def test_task_log_keeps_trusted_path_and_file_size_boundaries(tmp_path: Path) -> None:
    settings = Settings(
        timezone=ZoneInfo("Asia/Shanghai"),
        data_root=tmp_path,
        max_partition_size=100,
    )
    upgrade_database(settings.state_db)
    engine = create_sqlite_engine(settings.state_db)
    now = datetime(2026, 8, 15, 3, tzinfo=UTC)
    log_root = settings.data_root / "state" / "task-logs"
    queue = TaskQueue(engine, clock=lambda: now, task_log_root=log_root)
    task_id = queue.enqueue("TEST_FAILURE", {}, 0)
    claimed = queue.claim("worker-1", now)
    assert claimed is not None
    manager = TaskLogManager(
        diagnostic_root=log_root,
        artifact_root=settings.artifact_root,
    )
    context = LogContext(
        task_id=claimed.id,
        attempt_id=claimed.attempt_id,
        worker_id=claimed.worker_id,
    )
    with manager.open(context) as task_log:
        queue.bind_log_path(
            claimed.attempt_id,
            claimed.worker_id,
            str(task_log.path),
        )
        trusted_path = task_log.path
    queue.finish(
        claimed.attempt_id,
        claimed.worker_id,
        TaskOutcome(
            status=TaskStatus.FAILED,
            error={"code": "WORKER_UNHANDLED_ERROR", "retryable": False},
        ),
    )
    service = DashboardViewService(
        engine,
        settings,
        _REPOSITORY,
        cast(MarketReviewService, object()),
        TUSHARE_ROUTES,
    )
    outside = tmp_path / "outside.log"
    outside.write_text("outside", encoding="utf-8")
    try:
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE task_attempt SET log_path = :path WHERE id = :id"),
                {"path": str(outside), "id": claimed.attempt_id},
            )
        with pytest.raises(ValueError, match="outside the trusted root"):
            service.task_log(task_id, claimed.attempt_id, tail_lines=500)

        trusted_path.write_bytes(b"x" * (MAX_TASK_LOG_BYTES + 1))
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE task_attempt SET log_path = :path WHERE id = :id"),
                {"path": str(trusted_path), "id": claimed.attempt_id},
            )
        with pytest.raises(ValueError, match="safe size limit"):
            service.task_log(task_id, claimed.attempt_id, tail_lines=500)
    finally:
        engine.dispose()
