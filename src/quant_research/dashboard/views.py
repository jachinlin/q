"""提供研究界面与查询服务相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import duckdb
import polars as pl
from sqlalchemy import Engine, desc, func, select
from sqlalchemy.orm import Session

from quant_research.config import Settings
from quant_research.dashboard.market_review import MarketReviewService
from quant_research.dashboard.models import MarketReviewDates, MarketReviewResponse
from quant_research.data.catalog import DATASET_CATALOG
from quant_research.data.contracts import JsonValue, canonical_json_bytes
from quant_research.data.freshness import DatasetFreshness, FreshnessEvaluator
from quant_research.data.quality.catalog import QUALITY_RULE_CATALOG
from quant_research.data.quality.models import QualityIssue, thaw_json
from quant_research.data.repository import ResearchDataRepository
from quant_research.data.routing import RoutingTable
from quant_research.domain.enums import DatasetKind
from quant_research.domain.errors import QuantError
from quant_research.domain.identifiers import QualityRunId
from quant_research.experiments.models import (
    ExperimentRecord,
    ExperimentStatus,
    ResearchMark,
)
from quant_research.experiments.query import ExperimentQuery
from quant_research.experiments.verification import validate_registered_publication
from quant_research.factor_studies.models import (
    HORIZONS,
    INDUSTRY_TAXONOMY,
    INDUSTRY_UNCLASSIFIED_POLICIES,
    SIGNAL_VARIANTS,
    STOCK_FACTOR_REFS,
)
from quant_research.infrastructure.persistence.factor_studies import (
    FactorStudyRepository,
)
from quant_research.infrastructure.persistence.orm import ExperimentORM, TaskORM
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
    DatasetOperationalStateRecord,
    MetadataRepository,
    QualityRunRecord,
)
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.logging import MAX_TASK_LOG_BYTES
from quant_research.tasks.models import TaskRecord, TaskStatus

_BACKTEST_FILES = frozenset(
    {
        "metrics.json",
        "nav.parquet",
        "drawdown.parquet",
        "monthly_returns.parquet",
        "exposure_summary.parquet",
        "attribution.parquet",
        "execution_summary.parquet",
        "quality_disclosure.json",
    }
)
_DRILLDOWN_FILES = frozenset({"holdings.parquet", "fills.parquet"})


class DashboardViewService:
    """向接口层提供不暴露基础设施实现的研究工作台用例。

    入参：
        engine：引擎。
        settings：应用设置。
        repository：已由组合根装配的研究数据查询仓库。
        market_review：只读市场全景查询服务。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``RuntimeError``、``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Return JSON-safe dashboard views without exposing persistence handles.
    """

    def __init__(
        self,
        engine: Engine,
        settings: Settings,
        repository: ResearchDataRepository,
        market_review: MarketReviewService,
        routes: RoutingTable,
    ) -> None:
        self._engine = engine
        self._settings = settings
        self._repository = repository
        self._catalog = MetadataRepository(engine)
        self._experiments = ExperimentQuery(engine)
        self._tasks = TaskQueue(
            engine,
            task_log_root=settings.data_root / "state" / "task-logs",
        )
        self._factor_studies = FactorStudyRepository(engine)
        self._market_review = market_review
        self._routes = routes

    def market_review_dates(self) -> MarketReviewDates:
        """返回当前验证目录支持的市场全景交易日。

        入参：无。
        返回值：不可变的市场全景日期目录 DTO。
        异常：数据门未开放或必要数据集覆盖不完整时传播依赖异常。
        """

        return self._market_review.available_dates()

    def market_review(
        self, trade_date: date | None, *, exclude_st: bool
    ) -> MarketReviewResponse:
        """返回指定交易日和股票口径的完整市场全景。

        入参：
            trade_date：目标交易日；为空时使用最新有效交易日。
            exclude_st：是否统一剔除风险警示股票。
        返回值：不可变的市场全景 DTO。
        异常：日期、数据门、目录身份或 Canonical 内容异常时传播依赖异常。
        """

        return self._market_review.review(trade_date, exclude_st=exclude_st)

    def factor_catalog(self) -> dict[str, object]:
        """返回 MVP 可选股票因子、方向、窗口和收益定义。

        入参：
            无。
        返回值：
            返回数据目录（``dict[str, object]``）。
        异常：
            无。
        """
        labels = {
            "earnings_yield_ttm": "盈利收益率",
            "book_to_price_mrq": "账面市值比",
            "roe_pit": "ROE",
            "momentum_120_20": "120-20动量",
            "volatility_60d": "60日波动率",
            "downside_volatility_60d": "60日下行波动率",
            "max_drawdown_120d": "120日最大回撤",
        }
        negative = {"volatility_60d", "downside_volatility_60d", "max_drawdown_120d"}
        return {
            "items": [
                {
                    "factor_ref": ref,
                    "name": labels[ref],
                    "direction": -1 if ref in negative else 1,
                }
                for ref in STOCK_FACTOR_REFS
            ],
            "horizons": list(HORIZONS),
            "quantiles": 5,
            "return_definition": "T+1开盘至T+h收盘",
            "signal_variants": list(SIGNAL_VARIANTS),
            "industry": {
                "taxonomy": INDUSTRY_TAXONOMY,
                "unclassified_policies": list(INDUSTRY_UNCLASSIFIED_POLICIES),
            },
        }

    def factor_studies(self, page: int, page_size: int) -> dict[str, object]:
        """分页返回独立因子研究列表。

        入参：
            page：页码。
            page_size：页码字节数。
        返回值：
            返回``studies``（``dict[str, object]``）。
        异常：
            无。
        """
        return self._factor_studies.list_studies(page, page_size)

    def factor_study(self, study_id: str) -> dict[str, object]:
        """返回指定研究及其全部不可变运行。

        入参：
            study_id：因子研究定义的 UUID 标识。
        返回值：
            返回因子研究（``dict[str, object]``）。
        异常：
            无。
        """
        return self._factor_studies.get_study(study_id)

    def factor_run(self, run_id: str) -> dict[str, object]:
        """返回运行状态，并在成功时附带已验证摘要。

        入参：
            run_id：一次因子研究运行的 UUID 标识。
        返回值：
            返回运行（``dict[str, object]``）。
        异常：
            无。
        """
        run = self._factor_studies.get_run(run_id)
        if run["status"] != "SUCCEEDED":
            return run
        files, manifest_hash = self._factor_run_files(run_id)
        return {
            **run,
            "manifest_hash": manifest_hash,
            "summary": _ServicesSupport._parquet_rows(files["summary.parquet"]),
        }

    def factor_series(
        self, run_id: str, factor_ref: str, horizon: int, signal_variant: str
    ) -> dict[str, object]:
        """返回指定因子和窗口的已验证时序诊断。

        入参：
            run_id：一次因子研究运行的 UUID 标识。
            factor_ref：因子引用。
            horizon：收益期限。
        返回值：
            返回``series``（``dict[str, object]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if (
            factor_ref not in STOCK_FACTOR_REFS
            or horizon not in HORIZONS
            or signal_variant not in SIGNAL_VARIANTS
        ):
            raise ValueError("unsupported factor, horizon, or signal variant")
        files, manifest_hash = self._factor_run_files(run_id)
        variants = set(
            pl.read_parquet(files["summary.parquet"])["signal_variant"].to_list()
        )
        if signal_variant not in variants:
            raise ValueError("factor run does not contain the requested signal variant")

        def selected(name: str) -> list[dict[str, Any]]:
            return (
                pl.read_parquet(files[name])
                .filter(
                    (pl.col("factor_ref") == factor_ref)
                    & (pl.col("horizon") == horizon)
                    & (pl.col("signal_variant") == signal_variant)
                )
                .to_dicts()
            )

        return {
            "run_id": run_id,
            "manifest_hash": manifest_hash,
            "factor_ref": factor_ref,
            "horizon": horizon,
            "signal_variant": signal_variant,
            "ic": selected("ic.parquet"),
            "quantile_returns": selected("quantile_returns.parquet"),
            "long_short_returns": selected("long_short_returns.parquet"),
            "coverage": (
                pl.read_parquet(files["coverage.parquet"])
                .filter(
                    (pl.col("factor_ref") == factor_ref)
                    & (pl.col("signal_variant") == signal_variant)
                )
                .to_dicts()
            ),
        }

    def factor_correlation(
        self, run_id: str, signal_variant: str
    ) -> dict[str, object]:
        """返回指定成功运行的已验证因子相关矩阵。

        入参：
            run_id：一次因子研究运行的 UUID 标识。
        返回值：
            返回相关性（``dict[str, object]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if signal_variant not in SIGNAL_VARIANTS:
            raise ValueError("unsupported signal variant")
        files, manifest_hash = self._factor_run_files(run_id)
        frame = pl.read_parquet(files["correlation.parquet"])
        if signal_variant not in set(frame["signal_variant"].to_list()):
            raise ValueError("factor run does not contain the requested signal variant")
        return {
            "run_id": run_id,
            "manifest_hash": manifest_hash,
            "signal_variant": signal_variant,
            "data": frame.filter(
                pl.col("signal_variant") == signal_variant
            ).to_dicts(),
        }

    def factor_industry_coverage(self, run_id: str) -> dict[str, object]:
        """返回成功因子运行逐信号日的 PIT 行业覆盖证据。

        入参：
            run_id：成功因子运行的唯一标识。
        返回值：
            返回绑定 Manifest 哈希的逐日行业覆盖记录。
        异常：
            运行未成功、产物缺失或验证失败时抛出 ``ValueError``。
        """
        files, manifest_hash = self._factor_run_files(run_id)
        return {
            "run_id": run_id,
            "manifest_hash": manifest_hash,
            "data": _ServicesSupport._parquet_rows(
                files["industry_coverage.parquet"]
            ),
        }

    def _factor_run_files(self, run_id: str) -> tuple[dict[str, Path], str]:
        run = self._factor_studies.get_run(run_id)
        if run["status"] != "SUCCEEDED":
            raise ValueError("factor run has no published result")
        root = (self._settings.artifact_root / "factor-studies").resolve()
        manifest_path = (
            root / cast(str, run["study_id"]) / run_id / "manifest.json"
        ).resolve()
        if not manifest_path.is_relative_to(root) or not manifest_path.is_file():
            raise ValueError("factor run manifest is outside the trusted root")
        payload = manifest_path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if digest != run["manifest_hash"]:
            raise ValueError("factor run manifest hash mismatch")
        manifest = json.loads(payload)
        canonical_json_bytes(cast(JsonValue, manifest))
        entries = manifest.get("entries")
        if not isinstance(entries, dict):
            raise TypeError("factor run manifest entries are invalid")
        required = {
            "summary.parquet",
            "coverage.parquet",
            "ic.parquet",
            "quantile_returns.parquet",
            "long_short_returns.parquet",
            "correlation.parquet",
            "industry_coverage.parquet",
        }
        files: dict[str, Path] = {}
        for name in required:
            entry = entries.get(name)
            if not isinstance(entry, dict) or entry.get("path") != name:
                raise ValueError("factor run artifact is not registered")
            path = (manifest_path.parent / name).resolve()
            if not path.is_relative_to(manifest_path.parent) or not path.is_file():
                raise ValueError("factor run artifact escaped publication")
            if path.stat().st_size != entry.get("size_bytes"):
                raise ValueError("factor run artifact size mismatch")
            if hashlib.sha256(path.read_bytes()).hexdigest() != entry.get("sha256"):
                raise ValueError("factor run artifact hash mismatch")
            frame = pl.read_parquet(path)
            if frame.height != entry.get("row_count"):
                raise ValueError("factor run artifact row count mismatch")
            if str(frame.schema) != entry.get("schema"):
                raise ValueError("factor run artifact schema mismatch")
            files[name] = path
        _ServicesSupport._validate_factor_artifacts(files, manifest)
        return files, digest

    def health(self) -> dict[str, object]:
        """汇总 Dashboard 健康状态研究工作台。

        入参：
            无。
        返回值：
            返回``health``（``dict[str, object]``）。
        异常：
            无。
        """
        with self._engine.connect() as connection:
            connection.exec_driver_sql("SELECT 1").scalar_one()
        return {"status": "ok", "service": "quant-dashboard", "version": "1"}

    def overview(self) -> dict[str, object]:
        """返回研究工作台的就绪状态、运行证据与近期研究结果。

        入参：
            无。
        返回值：
            返回可由 ``OverviewResponse`` 严格校验的 JSON 安全字典。
        异常：
            无。
        """
        operations = self.data_summary()
        datasets = self._catalog.list_canonical_datasets()
        active_statuses = (
            TaskStatus.QUEUED,
            TaskStatus.RUNNING,
            TaskStatus.CANCEL_REQUESTED,
        )
        active_candidates = [
            item
            for status in active_statuses
            for item in self._tasks.list(status=status, limit=5)
        ]
        active_tasks = tuple(
            sorted(
                active_candidates,
                key=lambda item: (item.updated_at, item.id),
                reverse=True,
            )[:5]
        )
        task_status_counts = {status.value: 0 for status in TaskStatus}
        with Session(self._engine) as session:
            for status, count in session.execute(
                select(TaskORM.status, func.count())
                .select_from(TaskORM)
                .group_by(TaskORM.status)
            ):
                task_status_counts[str(status)] = int(count)
        latest_trade_date = max(
            (item.end_date for item in datasets if item.end_date is not None),
            default=None,
        )
        return {
            "gate": operations["gate"],
            "freshness": operations["freshness"],
            "latest_trade_date": _ServicesSupport._iso(latest_trade_date),
            "dataset_count": len(datasets),
            "gate_quality_run": operations["gate_quality_run"],
            "latest_quality_run": operations["latest_quality_run"],
            "worker": operations["worker"],
            "last_successful_update": operations["last_successful_update"],
            "tasks": {
                "status_counts": task_status_counts,
                "active": tuple(_ServicesSupport._task(item) for item in active_tasks),
            },
        }

    def data_summary(self) -> dict[str, object]:
        """返回数据中心门禁、新鲜度与更新任务汇总。

        入参：
            无。
        返回值：
            返回不包含持久化句柄的 JSON 安全汇总。
        异常：
            依赖查询异常按原契约传播。
        """
        state = self._catalog.catalog_state()
        runs = self._catalog.list_quality_runs()
        latest = runs[0] if runs else None
        gate_run = (
            self._catalog.get_quality_run(state.quality_run_id)
            if state.quality_run_id is not None
            else None
        )
        freshness, latest_complete_session = self._freshness_state()
        data_tasks = self._tasks.list(task_type="DATA_UPDATE", limit=100)
        active = next(
            (
                item
                for item in data_tasks
                if item.status
                in {
                    TaskStatus.QUEUED,
                    TaskStatus.RUNNING,
                    TaskStatus.CANCEL_REQUESTED,
                }
            ),
            None,
        )
        last_success = next(
            (item for item in data_tasks if item.status is TaskStatus.SUCCEEDED), None
        )
        with Session(self._engine) as session:
            active_research = int(
                session.scalar(
                    select(func.count())
                    .select_from(TaskORM)
                    .where(
                        TaskORM.task_type.in_(["BACKTEST", "FACTOR_ANALYSIS"]),
                        TaskORM.status.in_(["QUEUED", "RUNNING", "CANCEL_REQUESTED"]),
                    )
                )
                or 0
            )
            worker_row = session.scalar(
                select(TaskORM)
                .where(TaskORM.heartbeat_at.is_not(None))
                .order_by(desc(TaskORM.heartbeat_at))
                .limit(1)
            )
        counts = {
            status: sum(item.status.value == status for item in freshness)
            for status in ("CURRENT", "STALE", "MISSING", "UNKNOWN")
        }
        overall = (
            "MISSING"
            if counts["MISSING"]
            else "STALE"
            if counts["STALE"]
            else "UNKNOWN"
            if counts["UNKNOWN"]
            else "CURRENT"
        )
        return {
            "gate": {
                "status": "READY" if state.is_validated else "BLOCKED",
                "reason": self._gate_reason(
                    state.is_validated, state.catalog_hash, runs
                ),
                "catalog_hash": state.catalog_hash,
                "validated_catalog_hash": state.validated_catalog_hash,
                "quality_run_id": (
                    None if state.quality_run_id is None else str(state.quality_run_id)
                ),
                "updated_at": _ServicesSupport._iso(state.updated_at),
                "validated_at": _ServicesSupport._iso(state.validated_at),
            },
            "freshness": {
                "status": overall,
                "counts": counts,
                "evaluated_at": (
                    freshness[0].evaluated_at.isoformat() if freshness else None
                ),
                "latest_complete_session": _ServicesSupport._iso(
                    latest_complete_session
                ),
            },
            "gate_quality_run": self._quality_run_summary(gate_run),
            "latest_quality_run": self._quality_run_summary(latest),
            "active_update": None if active is None else _ServicesSupport._task(active),
            "last_successful_update": (
                None if last_success is None else _ServicesSupport._task(last_success)
            ),
            "worker": (
                None
                if worker_row is None
                else {
                    "worker_id": worker_row.worker_id,
                    "task_id": worker_row.id,
                    "task_status": worker_row.status,
                    "heartbeat_at": worker_row.heartbeat_at,
                }
            ),
            "active_research_task_count": active_research,
        }

    def data_datasets(self) -> dict[str, object]:
        """返回全部数据资产及其新鲜度和运营状态。

        入参：无。返回值：返回不可变响应可验证的字典。异常：依赖查询异常按原契约传播。
        """
        """返回全部目录定义、Canonical 汇总与新鲜度状态。"""

        current = {
            item.dataset: item for item in self._catalog.list_canonical_datasets()
        }
        states = {
            item.dataset: item
            for item in self._catalog.list_dataset_operational_states()
        }
        fresh = {item.dataset: item for item in self._freshness_state()[0]}
        latest = self._catalog.latest_quality_run()
        return {
            "items": tuple(
                self._dataset_summary(
                    dataset,
                    current.get(dataset),
                    states.get(dataset),
                    fresh[dataset],
                    latest,
                )
                for dataset in DATASET_CATALOG
            )
        }

    def data_dataset(self, dataset: str) -> dict[str, object]:
        """返回单个数据集契约、分区和运营详情。

        入参：
            dataset：Canonical 数据集名称。
        返回值：
            返回不含受信文件路径的详情。
        异常：
            ValueError：数据集名称不在目录中时抛出。
        """
        definition = DATASET_CATALOG.parse(dataset)
        current = self._catalog.find_canonical_dataset(definition.kind)
        states = {
            item.dataset: item
            for item in self._catalog.list_dataset_operational_states()
        }
        freshness = {item.dataset: item for item in self._freshness_state()[0]}[
            definition.kind
        ]
        summary = self._dataset_summary(
            definition.kind,
            current,
            states.get(definition.kind),
            freshness,
            self._catalog.latest_quality_run(),
        )
        return {
            **summary,
            "contract": {
                "partitioning": definition.partitioning.value,
                "fetch_granularity": definition.fetch_granularity.value,
                "cadence": definition.cadence.value,
                "reuse": definition.reuse.value,
                "overlap_days": definition.overlap_days,
                "primary_key": tuple(definition.schema.primary_key),
                "sort_key": tuple(definition.schema.sort_key),
                "pit_fields": tuple(definition.pit_fields),
                "schema": tuple(
                    {"name": name, "type": str(dtype)}
                    for name, dtype in definition.schema.columns.items()
                ),
                "sources": tuple(
                    {
                        "source": source,
                        "endpoints": tuple(item.endpoint for item in endpoints),
                    }
                    for source, endpoints in definition.source_endpoints.items()
                ),
            },
            "partitions": (
                ()
                if current is None
                else tuple(
                    {
                        "partition_key": item.partition_key,
                        "ordinal": index,
                        "row_count": item.row_count,
                        "content_hash": item.content_hash,
                        "schema_fingerprint": item.schema_fingerprint,
                        "input_hash": item.input_hash,
                    }
                    for index, item in enumerate(current.partitions)
                )
            ),
        }

    def quality_runs(
        self,
        *,
        scope: str | None,
        status: str | None,
        dataset: str | None,
        severity: str | None,
        rule: str | None,
        page: int,
        page_size: int,
    ) -> dict[str, object]:
        """分页筛选质量运行历史。

        入参：范围、状态、数据集、严重度、规则和分页参数。返回值：分页结果。异常：非法参数抛出 ValueError。
        """
        """分页筛选质量运行历史。"""

        runs = list(self._catalog.list_quality_runs())
        if scope:
            runs = [item for item in runs if item.scope.upper() == scope.upper()]
        if status:
            runs = [item for item in runs if item.status.upper() == status.upper()]
        if dataset:
            runs = [item for item in runs if dataset in item.dataset_hashes]
        if severity:
            runs = [
                item
                for item in runs
                if any(
                    issue.severity.value == severity.upper() for issue in item.issues
                )
            ]
        if rule:
            runs = [
                item
                for item in runs
                if any(issue.rule_id == rule for issue in item.issues)
            ]
        total = len(runs)
        start = (page - 1) * page_size
        return {
            "items": tuple(
                self._quality_run_summary(item)
                for item in runs[start : start + page_size]
            ),
            "page": page,
            "page_size": page_size,
            "total": total,
        }

    def quality_run(self, run_id: str) -> dict[str, object]:
        """返回一次质量运行的完整审计证据。

        入参：质量运行标识。返回值：问题字段和数据集哈希。异常：运行不存在或标识非法时按原契约传播。
        """
        """返回一次质量运行的完整审计证据。"""

        run = self._catalog.get_quality_run(QualityRunId.parse(run_id))
        issues = tuple(self._quality_issue_view(issue) for issue in run.issues)
        rule_results = self._quality_rule_results(run, issues)
        counts = {
            status: sum(item["status"] == status for item in rule_results)
            for status in ("PASS", "FAIL", "SKIPPED", "UNKNOWN")
        }
        return {
            **cast(dict[str, object], self._quality_run_summary(run)),
            "dataset_hashes": dict(run.dataset_hashes),
            "results_complete": run.results_complete,
            "result_counts": counts,
            "rule_results": rule_results,
            "issues": issues,
        }

    @staticmethod
    def _quality_issue_view(issue: QualityIssue) -> dict[str, object]:
        """把持久化质量问题转换为 JSON 安全详情。"""
        return {
            "rule_id": issue.rule_id,
            "severity": issue.severity.value,
            "dataset": issue.dataset.value,
            "scope": thaw_json(issue.scope),
            "actual": thaw_json(issue.actual),
            "threshold": thaw_json(issue.threshold),
            "message": issue.message,
            "remediation": issue.remediation,
        }

    @staticmethod
    def _quality_rule_results(
        run: QualityRunRecord,
        issues: tuple[dict[str, object], ...],
    ) -> tuple[dict[str, object], ...]:
        """返回运行快照，或为旧运行显式构造 FAIL/UNKNOWN 证据。"""
        issues_by_key: dict[tuple[str, str], list[dict[str, object]]] = {}
        for issue in issues:
            key = (str(issue["dataset"]), str(issue["rule_id"]))
            issues_by_key.setdefault(key, []).append(issue)
        if run.results_complete:
            return tuple(
                {
                    "rule_id": result.rule_id,
                    "dataset": result.dataset.value,
                    "status": result.status.value,
                    "severity": result.severity.value,
                    "title": result.title,
                    "description": result.description,
                    "pass_criterion": result.pass_criterion,
                    "scope": thaw_json(result.scope),
                    "actual": thaw_json(result.actual),
                    "threshold": thaw_json(result.threshold),
                    "skip_reason": result.skip_reason,
                    "evidence": result.evidence.value,
                    "issues": tuple(
                        issues_by_key.get((result.dataset.value, result.rule_id), ())
                    ),
                }
                for result in run.rule_results
            )

        rows: list[dict[str, object]] = []
        known_keys: set[tuple[str, str]] = set()
        run_datasets = tuple(DatasetKind(item) for item in sorted(run.dataset_hashes))
        for dataset in run_datasets:
            for definition in QUALITY_RULE_CATALOG:
                if dataset not in definition.datasets:
                    continue
                key = (dataset.value, definition.rule_id)
                known_keys.add(key)
                matching = tuple(issues_by_key.get(key, ()))
                legacy_issue = matching[0] if matching else None
                rows.append(
                    {
                        "rule_id": definition.rule_id,
                        "dataset": dataset.value,
                        "status": "FAIL" if legacy_issue is not None else "UNKNOWN",
                        "severity": definition.severity.value,
                        "title": definition.title,
                        "description": definition.description,
                        "pass_criterion": definition.pass_criterion,
                        "scope": {} if legacy_issue is None else legacy_issue["scope"],
                        "actual": None
                        if legacy_issue is None
                        else legacy_issue["actual"],
                        "threshold": None
                        if legacy_issue is None
                        else legacy_issue["threshold"],
                        "skip_reason": (
                            "该历史运行未保存规则执行证据。"
                            if legacy_issue is None
                            else None
                        ),
                        "evidence": "MISSING"
                        if legacy_issue is None
                        else "LEGACY_ISSUE",
                        "issues": matching,
                    }
                )
        for key in sorted(set(issues_by_key) - known_keys):
            matching = tuple(issues_by_key[key])
            issue = matching[0]
            rows.append(
                {
                    "rule_id": key[1],
                    "dataset": key[0],
                    "status": "FAIL",
                    "severity": issue["severity"],
                    "title": key[1],
                    "description": "历史运行保存的质量问题；当前规则目录中已无对应定义。",
                    "pass_criterion": "历史运行未保存通过条件。",
                    "scope": issue["scope"],
                    "actual": issue["actual"],
                    "threshold": issue["threshold"],
                    "skip_reason": None,
                    "evidence": "LEGACY_ISSUE",
                    "issues": matching,
                }
            )
        return tuple(rows)

    def _freshness_state(
        self,
    ) -> tuple[tuple[DatasetFreshness, ...], date | None]:
        evaluated_at = datetime.now(UTC)
        latest_session = self._latest_complete_session(evaluated_at)
        evaluator = FreshnessEvaluator(
            DATASET_CATALOG,
            timezone=self._settings.timezone,
        )
        return (
            evaluator.evaluate(
                canonical=self._catalog.list_canonical_datasets(),
                operational=self._catalog.list_dataset_operational_states(),
                evaluated_at=evaluated_at,
                latest_complete_session=latest_session,
            ),
            latest_session,
        )

    def _latest_complete_session(self, evaluated_at: datetime) -> date | None:
        local = evaluated_at.astimezone(self._settings.timezone)
        cutoff = local.date() if local.hour >= 18 else local.date() - timedelta(days=1)
        calendar = self._catalog.find_canonical_dataset(DatasetKind.TRADE_CALENDAR)
        if calendar is None or calendar.end_date is None or calendar.end_date < cutoff:
            return None
        if not self._catalog.catalog_state().is_validated:
            return None
        try:
            rows = (
                self._repository.trade_calendar(cutoff - timedelta(days=45), cutoff)
                .filter(pl.col("is_trading_day"))
                .select(pl.col("trade_date").max())
                .collect()
            )
        except (QuantError, ValueError, KeyError, OSError):
            return None
        if rows.height == 0:
            return None
        value = rows.item(0, 0)
        return value if isinstance(value, date) else None

    @staticmethod
    def _gate_reason(
        validated: bool,
        catalog_hash: str,
        runs: tuple[QualityRunRecord, ...],
    ) -> str:
        if validated:
            return "VALIDATED"
        latest_global = next(
            (item for item in runs if item.scope.upper() == "ALL"), None
        )
        if latest_global is None:
            return "NEVER_VALIDATED"
        if latest_global.input_hash != catalog_hash:
            return "CATALOG_CHANGED"
        return "VALIDATION_FAILED"

    @staticmethod
    def _quality_run_summary(run: QualityRunRecord | None) -> dict[str, object] | None:
        if run is None:
            return None
        return {
            "run_id": str(run.id),
            "scope": run.scope,
            "input_hash": run.input_hash,
            "status": run.status,
            "started_at": _ServicesSupport._iso(run.started_at),
            "completed_at": _ServicesSupport._iso(run.completed_at),
            "issue_count": len(run.issues),
            "blocking_issue_count": sum(
                issue.severity.value in {"SEVERE", "FATAL"} for issue in run.issues
            ),
        }

    @staticmethod
    def _dataset_summary(
        dataset: DatasetKind,
        current: CanonicalDatasetRecord | None,
        operational: DatasetOperationalStateRecord | None,
        freshness: DatasetFreshness,
        latest_run: QualityRunRecord | None,
    ) -> dict[str, object]:
        definition = DATASET_CATALOG[dataset]
        issues = (
            ()
            if latest_run is None
            else tuple(item for item in latest_run.issues if item.dataset is dataset)
        )
        source = (
            current.source
            if current is not None
            else next(iter(definition.source_endpoints), None)
        )
        return {
            "dataset": dataset.value,
            "source": source,
            "start_date": None
            if current is None
            else _ServicesSupport._iso(current.start_date),
            "end_date": None
            if current is None
            else _ServicesSupport._iso(current.end_date),
            "partition_count": 0 if current is None else len(current.partitions),
            "row_count": 0
            if current is None
            else sum(item.row_count for item in current.partitions),
            "content_hash": None if current is None else current.content_hash,
            "updated_at": None
            if current is None
            else _ServicesSupport._iso(current.updated_at),
            "partitioning": definition.partitioning.value,
            "cadence": definition.cadence.value,
            "fetch_granularity": definition.fetch_granularity.value,
            "reuse": definition.reuse.value,
            "overlap_days": definition.overlap_days,
            "freshness": {
                "status": freshness.status.value,
                "actual_watermark": _ServicesSupport._iso(freshness.actual_watermark),
                "expected_watermark": _ServicesSupport._iso(
                    freshness.expected_watermark
                ),
                "lag_days": freshness.lag_days,
                "evaluated_at": freshness.evaluated_at.isoformat(),
                "reason": freshness.reason,
            },
            "operational": {
                "last_localized_at": None
                if operational is None
                else _ServicesSupport._iso(operational.last_localized_at),
                "localized_through": None
                if operational is None
                else _ServicesSupport._iso(operational.localized_through),
                "last_curated_at": None
                if operational is None
                else _ServicesSupport._iso(operational.last_curated_at),
                "last_validated_at": None
                if operational is None
                else _ServicesSupport._iso(operational.last_validated_at),
            },
            "quality_issue_count": len(issues),
            "blocking_issue_count": sum(
                item.severity.value in {"SEVERE", "FATAL"} for item in issues
            ),
        }

    def data_catalog(self) -> dict[str, object]:
        """处理研究工作台中的数据数据目录。

        入参：
            无。
        返回值：
            返回数据目录（``dict[str, object]``）。
        异常：
            无。
        """
        state = self._catalog.catalog_state()
        current = {
            item.dataset: item for item in self._catalog.list_canonical_datasets()
        }
        datasets: list[dict[str, object]] = []
        for dataset in DATASET_CATALOG:
            item = current.get(dataset)
            routes = self._routes[dataset]
            datasets.append(
                {
                    "dataset": dataset.value,
                    "source": (
                        item.source
                        if item is not None
                        else (routes[0].source if routes else None)
                    ),
                    "start_date": None
                    if item is None
                    else _ServicesSupport._iso(item.start_date),
                    "end_date": None
                    if item is None
                    else _ServicesSupport._iso(item.end_date),
                    "partition_count": (None if item is None else len(item.partitions)),
                    "row_count": (
                        None
                        if item is None
                        else sum(part.row_count for part in item.partitions)
                    ),
                    "content_hash": None if item is None else item.content_hash,
                    "updated_at": None
                    if item is None
                    else _ServicesSupport._iso(item.updated_at),
                }
            )
        return {
            "catalog_hash": state.catalog_hash,
            "validated_catalog_hash": state.validated_catalog_hash,
            "is_validated": state.is_validated,
            "updated_at": _ServicesSupport._iso(state.updated_at),
            "validated_at": _ServicesSupport._iso(state.validated_at),
            "datasets": datasets,
        }

    def quality(self) -> dict[str, object]:
        """处理研究工作台中的质量校验。

        入参：
            无。
        返回值：
            返回质量校验（``dict[str, object]``）。
        异常：
            无。
        """
        run = self._catalog.latest_quality_run()
        if run is None:
            return {"run": None, "issues": []}
        return {
            "run": {
                "id": str(run.id),
                "scope": run.scope,
                "input_hash": run.input_hash,
                "status": run.status,
                "started_at": _ServicesSupport._iso(run.started_at),
                "completed_at": _ServicesSupport._iso(run.completed_at),
            },
            "issues": [
                {
                    "rule_id": issue.rule_id,
                    "severity": issue.severity.value,
                    "dataset": issue.dataset.value,
                    "message": issue.message,
                    "remediation": issue.remediation,
                }
                for issue in run.issues
            ],
        }

    def experiment_list(
        self,
        *,
        status: str | None,
        strategy_id: str | None,
        research_mark: str | None,
        page: int,
        page_size: int,
    ) -> dict[str, object]:
        """处理研究工作台中的实验``list``。

        入参：
            status：当前记录所处的受控生命周期状态。
            strategy_id：用于持久化关联和日志追踪的策略标识。
            research_mark：用户对实验标记的基线、候选或废弃研究结论。
            page：页码。
            page_size：页码字节数。
        返回值：
            返回``list``（``dict[str, object]``）。
        异常：
            无。
        """
        parsed_status = ExperimentStatus(status) if status else None
        parsed_mark = ResearchMark(research_mark) if research_mark else None
        offset = (page - 1) * page_size
        records = self._experiments.list(
            statuses=parsed_status,
            strategy_id=strategy_id,
            research_mark=parsed_mark,
            limit=page_size,
            offset=offset,
        )
        with Session(self._engine) as session:
            statement = select(func.count()).select_from(ExperimentORM)
            if parsed_status is not None:
                statement = statement.where(ExperimentORM.status == parsed_status.value)
            if strategy_id:
                statement = statement.where(ExperimentORM.strategy_id == strategy_id)
            if parsed_mark is not None:
                statement = statement.where(
                    ExperimentORM.research_mark == parsed_mark.value
                )
            total = int(session.scalar(statement) or 0)
        return {
            "items": [self._experiment_summary(item) for item in records],
            "page": page,
            "page_size": page_size,
            "total": total,
        }

    def experiment_detail(self, experiment_id: str) -> dict[str, object]:
        """处理研究工作台中的实验详情。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
        返回值：
            返回详情（``dict[str, object]``）。
        异常：
            无。
        """
        detail = self._experiments.get(experiment_id)
        with Session(self._engine) as session:
            latest_task = session.scalar(
                select(TaskORM)
                .where(TaskORM.experiment_id == experiment_id)
                .order_by(TaskORM.created_at.desc())
                .limit(1)
            )
        return {
            **self._experiment_summary(detail.record),
            "config": detail.record.config,
            "source_tree_hash": detail.record.source_tree_hash,
            "git_commit_hash": detail.record.git_commit_hash,
            "lockfile_hash": detail.record.lockfile_hash,
            "rulebook_hash": detail.record.rulebook_hash,
            "tags": list(detail.tags),
            "note": detail.note,
            "latest_task": (
                None
                if latest_task is None
                else {"id": latest_task.id, "status": latest_task.status}
            ),
            "metrics": [
                {"name": item.name, "value": item.value, "unit": item.unit}
                for item in detail.metrics
            ],
            "artifacts": [
                {
                    "name": item.name,
                    "type": item.artifact_type,
                    "content_hash": item.content_hash,
                    "metadata": item.metadata,
                }
                for item in detail.artifacts
            ],
            "audit": [
                {
                    "event_type": item.event_type,
                    "actor": item.actor,
                    "details": dict(item.details),
                    "created_at": _ServicesSupport._iso(item.created_at),
                }
                for item in detail.audit
            ],
        }

    def compare_experiments(self, experiment_ids: tuple[str, ...]) -> dict[str, object]:
        """比较``experiments``。

        入参：
            experiment_ids：参与本次处理的实验``ids``；调用方不得依赖未声明的顺序。
        返回值：
            返回``experiments``（``dict[str, object]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if len(set(experiment_ids)) != len(experiment_ids):
            raise ValueError("experiment IDs must be unique")
        details = [self._experiments.get(item) for item in experiment_ids]
        if any(
            item.record.status is not ExperimentStatus.SUCCEEDED for item in details
        ):
            raise ValueError("only successful experiments can be compared")
        metric_names = sorted(
            {metric.name for detail in details for metric in detail.metrics}
        )
        return {
            "experiments": [
                {
                    "id": detail.record.id,
                    "strategy_id": detail.record.strategy_id,
                    "data_hash": detail.record.data_hash,
                    "config": detail.record.config,
                    "metrics": {
                        name: next(
                            (
                                metric.value
                                for metric in detail.metrics
                                if metric.name == name
                            ),
                            None,
                        )
                        for name in metric_names
                    },
                }
                for detail in details
            ],
            "metric_names": metric_names,
        }

    def backtest(self, experiment_id: str) -> dict[str, object]:
        """处理研究工作台中的回测。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
        返回值：
            返回回测（``dict[str, object]``）。
        异常：
            无。
        """
        paths, manifest_hash = self._artifact_paths(experiment_id, _BACKTEST_FILES)
        metrics = self._read_json(paths["metrics.json"])
        nav = pl.read_parquet(paths["nav.parquet"])
        first_nav = cast(int, nav["nav_fen"].item(0))
        first_benchmark = cast(float, nav["benchmark_close"].item(0))
        nav_rows = nav.with_columns(
            (pl.col("nav_fen") / first_nav).alias("portfolio_nav"),
            (pl.col("benchmark_close") / first_benchmark).alias("benchmark_nav"),
        ).select("trade_date", "portfolio_nav", "benchmark_nav")
        return {
            "experiment_id": experiment_id,
            "manifest_hash": manifest_hash,
            "metrics": metrics,
            "nav": nav_rows.to_dicts(),
            "drawdown": _ServicesSupport._parquet_rows(paths.get("drawdown.parquet")),
            "monthly_returns": _ServicesSupport._parquet_rows(
                paths.get("monthly_returns.parquet")
            ),
            "exposures": _ServicesSupport._parquet_rows(
                paths.get("exposure_summary.parquet")
            ),
            "attribution": _ServicesSupport._parquet_rows(
                paths.get("attribution.parquet")
            ),
            "execution_summary": _ServicesSupport._parquet_rows(
                paths.get("execution_summary.parquet")
            ),
            "quality": (
                self._read_json(paths["quality_disclosure.json"])
                if "quality_disclosure.json" in paths
                else {}
            ),
        }

    def drilldown(
        self,
        experiment_id: str,
        *,
        kind: str,
        start: date | None,
        end: date | None,
        instrument_id: str | None,
        page: int,
        page_size: int,
    ) -> dict[str, object]:
        """处理研究工作台中的``drilldown``。

        入参：
            experiment_id：目标实验标识，类型为 ``str``。
            kind：``kind``。
            start：处理区间的开始日期，类型为 ``date | None``。
            end：处理区间的结束日期，类型为 ``date | None``。
            instrument_id：目标证券标识，类型为 ``str | None``。
            page：页码。
            page_size：页码字节数。
        返回值：
            返回``drilldown``（``dict[str, object]``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``RuntimeError``、``ValueError``。
        """
        filename = {"holdings": "holdings.parquet", "fills": "fills.parquet"}.get(kind)
        if filename is None:
            raise ValueError("unsupported drilldown kind")
        paths, manifest_hash = self._artifact_paths(experiment_id, {filename})
        clauses: list[str] = []
        parameters: list[object] = [str(paths[filename])]
        if start is not None:
            clauses.append("trade_date >= ?")
            parameters.append(start)
        if end is not None:
            clauses.append("trade_date <= ?")
            parameters.append(end)
        if instrument_id:
            clauses.append("instrument_id = ?")
            parameters.append(instrument_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        connection = duckdb.connect()
        try:
            count_row = connection.execute(
                f"SELECT count(*) FROM read_parquet(?){where}", parameters
            ).fetchone()
            if count_row is None:
                raise RuntimeError("DuckDB count query returned no row")
            total = int(count_row[0])
            query_params = [*parameters, page_size, (page - 1) * page_size]
            cursor = connection.execute(
                f"SELECT * FROM read_parquet(?){where} "
                "ORDER BY trade_date, instrument_id LIMIT ? OFFSET ?",
                query_params,
            )
            columns = [item[0] for item in cursor.description]
            rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        finally:
            connection.close()
        return {
            "items": rows,
            "page": page,
            "page_size": page_size,
            "total": total,
            "manifest_hash": manifest_hash,
        }

    def task_list(
        self,
        *,
        status: str | None,
        task_type: str | None = None,
        page: int,
        page_size: int,
    ) -> dict[str, object]:
        """分页返回任务列表与不受当前筛选影响的全局状态计数。

        入参：
            status：当前记录所处的受控生命周期状态。
            task_type：可选任务类型筛选；为空时返回所有任务类型。
            page：页码。
            page_size：页码字节数。
        返回值：
            返回任务页、分页信息和 ``status_counts``，类型为 ``dict[str, object]``。
        异常：
            无。
        """
        parsed = TaskStatus(status) if status else None
        items = self._tasks.list(
            status=parsed,
            task_type=task_type,
            limit=page_size,
            offset=(page - 1) * page_size,
        )
        with Session(self._engine) as session:
            statement = select(func.count()).select_from(TaskORM)
            if parsed is not None:
                statement = statement.where(TaskORM.status == parsed.value)
            if task_type is not None:
                statement = statement.where(TaskORM.task_type == task_type)
            total = int(session.scalar(statement) or 0)
            status_counts = {item.value: 0 for item in TaskStatus}
            for task_status, count in session.execute(
                select(TaskORM.status, func.count())
                .select_from(TaskORM)
                .group_by(TaskORM.status)
            ):
                status_counts[str(task_status)] = int(count)
        return {
            "items": [_ServicesSupport._task(item) for item in items],
            "page": page,
            "page_size": page_size,
            "total": total,
            "status_counts": status_counts,
        }

    def task_detail(self, task_id: str) -> dict[str, object]:
        """返回任务运行详情与按新到旧排列的有界尝试历史。

        入参：
            task_id：目标任务标识，类型为 ``str``。
        返回值：
            返回任务、只读 ``payload`` 及尝试摘要；日志只暴露 ``has_log``
            而不暴露路径。
        异常：
            无。
        """
        task = self._tasks.get(task_id)
        return {
            **_ServicesSupport._task(task),
            "payload": _ServicesSupport._task_payload(task.payload),
            "attempts": [
                {
                    "id": item.id,
                    "attempt_no": item.attempt_no,
                    "status": item.status.value,
                    "worker_id": item.worker_id,
                    "started_at": _ServicesSupport._iso(item.started_at),
                    "heartbeat_at": _ServicesSupport._iso(item.heartbeat_at),
                    "completed_at": _ServicesSupport._iso(item.completed_at),
                    "progress": item.progress,
                    "error": item.error,
                    "has_log": item.log_path is not None,
                    "result": item.result,
                }
                for item in self._tasks.list_attempts(task_id)
            ],
        }

    def task_log(
        self, task_id: str, attempt_id: str, tail_lines: int
    ) -> dict[str, object]:
        """读取受信任任务日志尾部并提取可操作的结构化失败诊断。

        入参：
            task_id：目标任务标识，类型为 ``str``。
            attempt_id：一次任务执行尝试的 UUID 标识。
            tail_lines：尾部``lines``。
        返回值：
            返回日志可用性、行数、截断状态、尾部文本和 ``diagnostic``。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if not 1 <= tail_lines <= 5000:
            raise ValueError("tail_lines must be from 1 through 5000")
        attempt = next(
            (
                item
                for item in self._tasks.list_attempts(task_id)
                if item.id == attempt_id
            ),
            None,
        )
        if attempt is None:
            raise ValueError("attempt does not belong to task")
        if attempt.log_path is None:
            return _ServicesSupport._task_log_payload(
                task_id=task_id,
                attempt_id=attempt_id,
                available=False,
                all_lines=(),
                tail_lines=tail_lines,
                error=attempt.error,
                progress=attempt.progress,
            )
        root = (self._settings.data_root / "state" / "task-logs").resolve()
        path = Path(attempt.log_path).resolve()
        if not path.is_relative_to(root):
            raise ValueError("registered task log is outside the trusted root")
        if not path.is_file():
            return _ServicesSupport._task_log_payload(
                task_id=task_id,
                attempt_id=attempt_id,
                available=False,
                all_lines=(),
                tail_lines=tail_lines,
                error=attempt.error,
                progress=attempt.progress,
            )
        stat = path.stat()
        if stat.st_size > MAX_TASK_LOG_BYTES:
            raise ValueError("registered task log exceeds the safe size limit")
        text = path.read_text(encoding="utf-8")
        return _ServicesSupport._task_log_payload(
            task_id=task_id,
            attempt_id=attempt_id,
            available=True,
            all_lines=tuple(text.splitlines()),
            tail_lines=tail_lines,
            error=attempt.error,
            progress=attempt.progress,
        )

    def _experiment_summary(self, record: ExperimentRecord) -> dict[str, object]:
        detail = self._experiments.get(record.id)
        return {
            "id": record.id,
            "strategy_id": record.strategy_id,
            "status": record.status.value,
            "research_mark": record.research_mark.value,
            "data_hash": record.data_hash,
            "config_hash": record.config_hash,
            "fingerprint": record.fingerprint,
            "created_at": _ServicesSupport._iso(record.created_at),
            "started_at": _ServicesSupport._iso(record.started_at),
            "completed_at": _ServicesSupport._iso(record.completed_at),
            "tags": tuple(detail.tags),
            "metrics": {item.name: item.value for item in detail.metrics},
        }

    def _artifact_paths(
        self, experiment_id: str, allowed: set[str] | frozenset[str]
    ) -> tuple[dict[str, Path], str]:
        detail = self._experiments.get(experiment_id)
        publication = validate_registered_publication(detail)
        root = self._settings.artifact_root.resolve()
        artifact_dir = publication.artifact_dir.resolve()
        if not artifact_dir.is_relative_to(root):
            raise ValueError("experiment publication is outside the trusted root")
        paths: dict[str, Path] = {}
        for name in allowed:
            entry = publication.entries.get(name)
            if entry is None:
                continue
            path = (artifact_dir / entry.path).resolve()
            if not path.is_relative_to(artifact_dir):
                raise ValueError("artifact path escaped the experiment publication")
            paths[name] = path
        manifest = next(
            item for item in detail.artifacts if item.name == "manifest.json"
        )
        if not paths and allowed:
            raise ValueError("experiment did not publish the requested artifacts")
        return paths, manifest.content_hash

    @staticmethod
    def _read_json(path: Path) -> dict[str, JsonValue]:
        value = json.loads(path.read_bytes())
        canonical_json_bytes(cast(JsonValue, value))
        if not isinstance(value, dict):
            raise TypeError("registered JSON artifact must be an object")
        return cast(dict[str, JsonValue], value)


class _ServicesSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _parquet_rows(path: Path | None) -> list[dict[str, Any]]:
        return [] if path is None else pl.read_parquet(path).to_dicts()

    @staticmethod
    def _validate_factor_artifacts(
        files: dict[str, Path], manifest: dict[str, Any]
    ) -> None:
        """验证新因子研究产物的版本键、排序和行业输入语义。"""
        keys = {
            "summary.parquet": ["signal_variant", "factor_ref", "horizon"],
            "coverage.parquet": [
                "signal_variant",
                "factor_ref",
                "signal_date",
            ],
            "ic.parquet": [
                "signal_variant",
                "factor_ref",
                "horizon",
                "signal_date",
            ],
            "quantile_returns.parquet": [
                "signal_variant",
                "factor_ref",
                "horizon",
                "signal_date",
                "quantile",
            ],
            "long_short_returns.parquet": [
                "signal_variant",
                "factor_ref",
                "horizon",
                "signal_date",
            ],
            "correlation.parquet": ["signal_variant", "factor_x", "factor_y"],
            "industry_coverage.parquet": ["signal_date"],
        }
        for name, primary_key in keys.items():
            frame = pl.read_parquet(files[name])
            if not set(primary_key).issubset(frame.columns):
                raise ValueError("factor run artifact is missing identity columns")
            rows = frame.select(primary_key).rows()
            if len(rows) != len(set(rows)):
                raise ValueError("factor run artifact primary key is not unique")
            if not frame.equals(frame.sort(primary_key)):
                raise ValueError("factor run artifact rows are not canonically sorted")
            if name != "industry_coverage.parquet":
                values = set(frame["signal_variant"].to_list())
                if not values or not values.issubset(SIGNAL_VARIANTS):
                    raise ValueError("factor run artifact signal variant is invalid")
        industry = pl.read_parquet(files["industry_coverage.parquet"])
        expected_schema = pl.Schema(
            {
                "signal_date": pl.Date,
                "taxonomy": pl.String,
                "unclassified_policy": pl.String,
                "eligible_count": pl.Int64,
                "classified_count": pl.Int64,
                "tombstone_count": pl.Int64,
                "missing_state_count": pl.Int64,
                "usable_count": pl.Int64,
                "classified_coverage": pl.Float64,
                "usable_coverage": pl.Float64,
            }
        )
        if industry.schema != expected_schema:
            raise ValueError("industry coverage artifact schema is invalid")
        industry_input = manifest.get("industry_input")
        if industry.is_empty():
            if industry_input is not None:
                raise ValueError("empty industry coverage cannot declare industry input")
            return
        if not isinstance(industry_input, dict) or set(industry_input) != {
            "dataset",
            "taxonomy",
            "unclassified_policy",
            "date_basis",
            "neutralization",
            "availability_source",
            "coverage",
        }:
            raise ValueError("factor run industry input is invalid")
        if (
            industry_input["dataset"] != DatasetKind.INDUSTRY_CLASSIFICATION.value
            or industry_input["taxonomy"] != INDUSTRY_TAXONOMY
            or industry_input["unclassified_policy"]
            not in INDUSTRY_UNCLASSIFIED_POLICIES
            or industry_input["date_basis"] != "SIGNAL_DATE"
            or industry_input["neutralization"] != "EQUAL_WEIGHT_GROUP_DEMEAN"
            or industry_input["availability_source"]
            != "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED"
            or not isinstance(industry_input["coverage"], dict)
        ):
            raise ValueError("factor run industry input values are invalid")
        if (
            set(industry["taxonomy"].to_list()) != {industry_input["taxonomy"]}
            or set(industry["unclassified_policy"].to_list())
            != {industry_input["unclassified_policy"]}
        ):
            raise ValueError("industry coverage does not match manifest semantics")
        for row in industry.iter_rows(named=True):
            eligible_count = row["eligible_count"]
            classified_count = row["classified_count"]
            tombstone_count = row["tombstone_count"]
            missing_count = row["missing_state_count"]
            usable_count = row["usable_count"]
            expected_usable = (
                classified_count
                if industry_input["unclassified_policy"] == "EXCLUDE"
                else eligible_count
            )
            if (
                eligible_count <= 0
                or min(
                    classified_count,
                    tombstone_count,
                    missing_count,
                    usable_count,
                )
                < 0
                or classified_count + tombstone_count + missing_count
                != eligible_count
                or usable_count != expected_usable
                or not math.isclose(
                    row["classified_coverage"],
                    classified_count / eligible_count,
                    abs_tol=1e-15,
                )
                or not math.isclose(
                    row["usable_coverage"],
                    usable_count / eligible_count,
                    abs_tol=1e-15,
                )
            ):
                raise ValueError("industry coverage row is inconsistent")
        coverage = industry_input["coverage"]
        assert isinstance(coverage, dict)
        if set(coverage) != {
            "eligible_observations",
            "classified_observations",
            "tombstone_observations",
            "missing_state_observations",
            "usable_observations",
            "classified_rate",
            "usable_rate",
        }:
            raise ValueError("factor run industry coverage summary is invalid")
        totals = {
            "eligible_observations": int(industry["eligible_count"].sum()),
            "classified_observations": int(industry["classified_count"].sum()),
            "tombstone_observations": int(industry["tombstone_count"].sum()),
            "missing_state_observations": int(industry["missing_state_count"].sum()),
            "usable_observations": int(industry["usable_count"].sum()),
        }
        eligible_total = totals["eligible_observations"]
        if (
            any(coverage[key] != value for key, value in totals.items())
            or not math.isclose(
                cast(float, coverage["classified_rate"]),
                totals["classified_observations"] / eligible_total,
                abs_tol=1e-15,
            )
            or not math.isclose(
                cast(float, coverage["usable_rate"]),
                totals["usable_observations"] / eligible_total,
                abs_tol=1e-15,
            )
        ):
            raise ValueError("industry coverage summary does not match artifact")

    @staticmethod
    def _task(item: TaskRecord) -> dict[str, object]:
        return {
            "id": item.id,
            "subject_kind": item.subject_kind,
            "subject_id": item.subject_id,
            "task_type": item.task_type,
            "status": item.status.value,
            "priority": item.priority,
            "progress": item.progress,
            "created_at": _ServicesSupport._iso(item.created_at),
            "started_at": _ServicesSupport._iso(item.locked_at),
            "updated_at": _ServicesSupport._iso(item.updated_at),
            "heartbeat_at": _ServicesSupport._iso(item.heartbeat_at),
            "completed_at": _ServicesSupport._iso(item.completed_at),
            "worker_id": item.worker_id,
            "error": item.error,
            "result": item.result,
        }

    @staticmethod
    def _factor_run_id(item: TaskRecord) -> str | None:
        """返回因子分析任务绑定的安全运行标识。

        入参：
            item：待序列化的任务记录。
        返回值：
            合法 ``FACTOR_ANALYSIS`` payload 中的非空 ``run_id``；其他情况
            返回 ``None``。
        异常：
            无。
        """
        if item.task_type != "FACTOR_ANALYSIS":
            return None
        run_id = item.payload.get("run_id")
        return run_id if isinstance(run_id, str) and run_id else None

    @staticmethod
    def _task_payload(payload: dict[str, JsonValue]) -> dict[str, JsonValue]:
        return {key: payload[key] for key in sorted(payload)}

    @staticmethod
    def _task_log_payload(
        *,
        task_id: str,
        attempt_id: str,
        available: bool,
        all_lines: tuple[str, ...],
        tail_lines: int,
        error: dict[str, JsonValue] | None,
        progress: dict[str, JsonValue],
    ) -> dict[str, object]:
        returned = all_lines[-tail_lines:]
        return {
            "task_id": task_id,
            "attempt_id": attempt_id,
            "available": available,
            "lines": list(returned),
            "total_lines": len(all_lines),
            "truncated": len(returned) < len(all_lines),
            "diagnostic": _ServicesSupport._task_diagnostic(
                all_lines,
                error=error,
                progress=progress,
            ),
        }

    @staticmethod
    def _task_diagnostic(
        lines: tuple[str, ...],
        *,
        error: dict[str, JsonValue] | None,
        progress: dict[str, JsonValue],
    ) -> dict[str, object] | None:
        stage = _ServicesSupport._text_value(progress.get("stage"))
        fallback = _ServicesSupport._persisted_diagnostic(error, stage=stage)
        for line in reversed(lines):
            try:
                raw_record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if (
                not isinstance(raw_record, dict)
                or raw_record.get("event") != "task.handler_failed"
            ):
                continue
            context = raw_record.get("context")
            values = context if isinstance(context, dict) else {}
            return {
                "code": _ServicesSupport._text_value(raw_record.get("error_code"))
                or (None if fallback is None else fallback["code"]),
                "message": _ServicesSupport._text_value(values.get("exception_message"))
                or (
                    None
                    if fallback is None
                    else _ServicesSupport._text_value(fallback.get("message"))
                ),
                "exception_type": _ServicesSupport._text_value(
                    values.get("exception_type")
                ),
                "stage": _ServicesSupport._text_value(raw_record.get("stage")) or stage,
                "retryable": _ServicesSupport._bool_value(values.get("retryable"))
                if _ServicesSupport._bool_value(values.get("retryable")) is not None
                else (
                    None
                    if fallback is None
                    else _ServicesSupport._bool_value(fallback.get("retryable"))
                ),
                "remediation": _ServicesSupport._text_value(values.get("remediation"))
                or (
                    None
                    if fallback is None
                    else _ServicesSupport._text_value(fallback.get("remediation"))
                )
                or "检查完整 traceback 和本次任务输入后再安全重试。",
                "traceback": _ServicesSupport._text_value(values.get("traceback")),
            }
        return fallback

    @staticmethod
    def _persisted_diagnostic(
        error: dict[str, JsonValue] | None,
        *,
        stage: str | None,
    ) -> dict[str, object] | None:
        if error is None:
            return None
        code = _ServicesSupport._text_value(error.get("code"))
        message = _ServicesSupport._text_value(error.get("message"))
        retryable = _ServicesSupport._bool_value(error.get("retryable"))
        remediation = _ServicesSupport._text_value(error.get("remediation"))
        if code == "TASK_ORPHANED" and remediation is None:
            remediation = "确认 Worker 已停止，检查未发布临时产物后再确认重试。"
        elif code is not None and remediation is None:
            remediation = "查看该次尝试日志，修复原因后再安全重试。"
        return {
            "code": code,
            "message": message,
            "exception_type": None,
            "stage": stage,
            "retryable": retryable,
            "remediation": remediation,
            "traceback": None,
        }

    @staticmethod
    def _text_value(value: object) -> str | None:
        return value if isinstance(value, str) and value else None

    @staticmethod
    def _bool_value(value: object) -> bool | None:
        return value if isinstance(value, bool) else None

    @staticmethod
    def _iso(value: object) -> str | None:
        if value is None:
            return None
        method = getattr(value, "isoformat", None)
        if not callable(method):
            raise TypeError("value must provide isoformat()")
        result = method()
        if not isinstance(result, str):
            raise TypeError("isoformat() must return text")
        return result
