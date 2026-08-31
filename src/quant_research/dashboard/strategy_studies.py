"""提供策略研究 HTTP 路由和可信产物读取。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import cast
from uuid import uuid4

import polars as pl
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ConfigDict, Field

from quant_research.application.strategy_studies import StrategyStudyService
from quant_research.dashboard.models import (
    StrategyAttributionSummary,
    StrategyDrawdownEpisode,
    StrategyExecutionSummary,
    StrategyExposurePoint,
    StrategyPerformancePoint,
    StrategyPeriodReturnPoint,
    StrategyRollingPerformancePoint,
    StrategyStudyQualityDisclosure,
    StrategyStudyReportResponse,
)
from quant_research.data.contracts import JsonValue
from quant_research.strategies.components import StrategyComponentCatalog
from quant_research.strategies.registry import StrategyRegistry
from quant_research.strategy_studies.models import (
    StrategyStudyRecord,
    StrategyStudyStatus,
)

_ARTIFACT_TYPES = frozenset(
    {
        "signals",
        "orders",
        "fills",
        "holdings",
        "costs",
        "nav",
        "dividends",
        "performance",
        "rolling_performance",
        "drawdown_episodes",
        "monthly_returns",
        "annual_returns",
        "execution_summary",
        "exposure_summary",
        "attribution",
        "config",
        "metrics",
        "quality_disclosure",
        "manifest",
    }
)
_ARTIFACT_FILTERS: dict[str, frozenset[str]] = {
    "attribution": frozenset({"dimension"}),
    "exposure_summary": frozenset({"dimension"}),
}


class _StrictBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class YamlBody(_StrictBody):
    """接收非空 YAML。入参：YAML 文本。返回值：严格请求体。异常：空值或额外字段校验失败。"""

    yaml: str = Field(min_length=1)


class StrategyStudyDashboardService:
    """转换研究 HTTP 数据。入参：研究服务、策略目录和产物根。返回值：接口服务。异常：依赖非法时传播。"""

    def __init__(
        self,
        studies: StrategyStudyService,
        strategies: StrategyRegistry,
        artifact_root: Path,
    ) -> None:
        self._studies = studies
        self._strategies = strategies
        self._components = StrategyComponentCatalog()
        self._artifact_root = artifact_root.resolve()

    def strategies(self) -> dict[str, JsonValue]:
        """读取策略目录。入参：无。返回值：策略与五模块 JSON。异常：目录读取失败时传播。"""

        details = self._components.describe()
        return {
            "strategies": [
                {
                    "strategy_id": item.strategy_id,
                    "display_name": item.display_name,
                    "summary": item.summary,
                }
                for item in self._strategies.profiles()
            ],
            "components": {
                key: list(values) for key, values in self._components.list().items()
            },
            "component_schemas": details["components"],
            "capability_rules": details["capability_rules"],
        }

    def strategy(self, strategy_id: str) -> dict[str, JsonValue]:
        """读取单一策略的完整说明。

        入参：已登记策略 ID。返回值：展示名称、摘要和 Markdown。异常：策略未知时抛出值错误。
        """
        item = self._strategies.profile(strategy_id)
        return {
            "strategy_id": item.strategy_id,
            "display_name": item.display_name,
            "summary": item.summary,
            "documentation_markdown": item.documentation_markdown,
        }

    @staticmethod
    def record(value: StrategyStudyRecord) -> dict[str, JsonValue]:
        """序列化研究。入参：冻结记录。返回值：HTTP JSON。异常：序列化失败时传播。"""

        return cast(dict[str, JsonValue], value.model_dump(mode="json"))

    def validate(self, yaml_text: str) -> dict[str, JsonValue]:
        """校验研究 YAML。入参：YAML 文本。返回值：哈希与规范定义。异常：配置非法时传播。"""

        resolved = self._studies.validate(yaml_text)
        return {
            "config_hash": resolved.config_hash,
            "normalized": cast(
                dict[str, JsonValue], resolved.definition.model_dump(mode="json")
            ),
        }

    def submit(self, yaml_text: str, actor: str) -> dict[str, JsonValue]:
        """提交研究。入参：YAML 文本和操作者。返回值：已入队 JSON。异常：门禁或事务失败时传播。"""

        return self.record(self._studies.submit(yaml_text, actor=actor))

    def list(
        self,
        limit: int,
        offset: int,
        status: StrategyStudyStatus | None,
    ) -> dict[str, JsonValue]:
        """分页列出研究。入参：分页和状态。返回值：研究列表 JSON。异常：分页非法时传播。"""

        return {
            "items": [
                self.record(item)
                for item in self._studies.list(
                    limit=limit, offset=offset, status=status
                )
            ]
        }

    def show(self, study_id: str) -> dict[str, JsonValue]:
        """读取研究。入参：研究 ID。返回值：完整 JSON。异常：不存在时抛出键错误。"""

        return self.record(self._studies.show(study_id))

    def delete(self, study_id: str, actor: str) -> dict[str, JsonValue]:
        """删除终态研究。入参：研究 ID 和操作者。返回值：删除结果。异常：活动研究或产物不可信时抛出值错误。"""

        study = self._studies.show(study_id)
        if study.status not in {
            StrategyStudyStatus.SUCCEEDED,
            StrategyStudyStatus.FAILED,
            StrategyStudyStatus.CANCELLED,
        }:
            raise ValueError("active strategy study cannot be deleted")
        staged = self._stage_directory(study)
        try:
            self._studies.delete(study_id, actor=actor)
        except BaseException:
            self._restore_directory(staged)
            raise
        self._purge_directory(staged)
        return {"strategy_study_id": study_id, "status": "DELETED"}

    def report(self, study_id: str) -> StrategyStudyReportResponse:
        """读取完整研究报告。入参：研究 ID。返回值：可信图表 DTO。异常：产物缺失或完整性非法时抛出值错误。"""

        study = self._studies.show(study_id)
        performance = self._require_frame(study, "performance")
        rolling = self._require_frame(study, "rolling_performance")
        monthly = self._require_frame(study, "monthly_returns")
        annual = self._require_frame(study, "annual_returns")
        episodes = self._require_frame(study, "drawdown_episodes")
        exposure = self._require_frame(study, "exposure_summary").filter(
            pl.col("dimension").is_in(["SECURITY", "CASH", "RECEIVABLE"])
        )
        attribution = (
            self._require_frame(study, "attribution")
            .filter(pl.col("dimension") == "SECURITY")
            .group_by("key")
            .agg(
                pl.col("pnl_fen").sum(),
                pl.col("contribution_return").sum(),
            )
            .with_columns(
                pl.col("contribution_return").abs().alias("absolute_contribution")
            )
            .sort(
                ["absolute_contribution", "key"], descending=[True, False]
            )
            .drop("absolute_contribution")
        )
        execution = self._require_frame(study, "execution_summary")
        quality_value = self._read_artifact(study, "quality_disclosure")
        if not isinstance(quality_value, dict):
            raise TypeError("strategy study quality disclosure is not an object")
        quality_payload = dict(quality_value)
        warnings = quality_payload.get("warnings")
        if not isinstance(warnings, list) or not all(
            isinstance(item, str) for item in warnings
        ):
            raise ValueError("strategy study quality warnings are invalid")
        quality_payload["warnings"] = tuple(warnings)
        return StrategyStudyReportResponse(
            performance=tuple(
                StrategyPerformancePoint.model_validate(row)
                for row in performance.to_dicts()
            ),
            rolling_performance=tuple(
                StrategyRollingPerformancePoint.model_validate(row)
                for row in rolling.to_dicts()
            ),
            monthly_returns=tuple(
                StrategyPeriodReturnPoint.model_validate(row)
                for row in monthly.to_dicts()
            ),
            annual_returns=tuple(
                StrategyPeriodReturnPoint.model_validate(row)
                for row in annual.to_dicts()
            ),
            drawdown_episodes=tuple(
                StrategyDrawdownEpisode.model_validate(row)
                for row in episodes.to_dicts()
            ),
            exposure=tuple(
                StrategyExposurePoint.model_validate(row)
                for row in exposure.to_dicts()
            ),
            attribution=tuple(
                StrategyAttributionSummary.model_validate(row)
                for row in attribution.to_dicts()
            ),
            execution=tuple(
                StrategyExecutionSummary.model_validate(row)
                for row in execution.to_dicts()
            ),
            quality=StrategyStudyQualityDisclosure.model_validate(quality_payload),
        )

    def artifact(
        self,
        study_id: str,
        artifact_type: str,
        page: int,
        page_size: int,
        *,
        dimension: str | None = None,
    ) -> dict[str, JsonValue]:
        """读取可信产物。入参：研究、类型、分页和过滤条件。返回值：产物 JSON。异常：完整性或边界非法时抛出值错误。"""

        if artifact_type not in _ARTIFACT_TYPES:
            raise ValueError("unsupported artifact type")
        requested_filters: dict[str, str | int] = {
            name: value
            for name, value in {"dimension": dimension}.items()
            if value is not None
        }
        unsupported = set(requested_filters) - _ARTIFACT_FILTERS.get(
            artifact_type, frozenset()
        )
        if unsupported:
            raise ValueError(
                f"unsupported filter for {artifact_type}: {min(unsupported)}"
            )
        study = self._studies.show(study_id)
        value = self._read_artifact(study, artifact_type)
        if not isinstance(value, pl.DataFrame):
            if artifact_type == "manifest":
                if not isinstance(value, dict):
                    raise ValueError("strategy study Manifest is not an object")
                return cast(dict[str, JsonValue], value)
            return {"value": value}
        frame = value
        for name, value in requested_filters.items():
            if name not in frame.columns:
                raise ValueError(f"artifact filter column is missing: {name}")
            frame = frame.filter(pl.col(name) == value)
        return {
            "items": cast(
                list[JsonValue],
                jsonable_encoder(
                    frame.slice((page - 1) * page_size, page_size).to_dicts()
                ),
            ),
            "page": page,
            "page_size": page_size,
            "total": len(frame),
        }

    def _require_frame(
        self, study: StrategyStudyRecord, artifact_type: str
    ) -> pl.DataFrame:
        value = self._read_artifact(study, artifact_type)
        if not isinstance(value, pl.DataFrame):
            raise TypeError(f"strategy study {artifact_type} is not tabular")
        return value

    def _read_artifact(
        self, study: StrategyStudyRecord, artifact_type: str
    ) -> JsonValue | pl.DataFrame:
        directory, manifest = self._verified_manifest(study)
        if artifact_type == "manifest":
            return cast(JsonValue, manifest)
        entries = cast(list[dict[str, JsonValue]], manifest.get("artifacts"))
        entry = next(
            (
                item
                for item in entries
                if item.get("artifact_type") == artifact_type
            ),
            None,
        )
        if entry is None:
            raise ValueError("strategy study did not publish requested artifact")
        relative_path = entry.get("relative_path")
        if not isinstance(relative_path, str):
            raise TypeError("artifact relative path is invalid")
        path = (directory / relative_path).resolve()
        if not path.is_relative_to(directory):
            raise ValueError("artifact path escaped strategy study directory")
        content = path.read_bytes()
        if (
            len(content) != entry.get("byte_count")
            or hashlib.sha256(content).hexdigest() != entry.get("content_hash")
        ):
            raise ValueError("artifact integrity check failed")
        if path.suffix == ".json":
            return cast(JsonValue, json.loads(content))
        frame = pl.read_parquet(path)
        if entry.get("row_count") != len(frame):
            raise ValueError("artifact row count does not match Manifest")
        actual_schema = {name: str(dtype) for name, dtype in frame.schema.items()}
        if entry.get("schema") != actual_schema:
            raise ValueError("artifact schema does not match Manifest")
        primary_key = tuple(cast(list[str], entry.get("primary_key") or ()))
        sort_key = tuple(cast(list[str], entry.get("sort_key") or ()))
        if not primary_key or not sort_key:
            raise ValueError("Parquet artifact lacks key metadata")
        if frame.select(pl.struct(primary_key).is_duplicated().any()).item():
            raise ValueError("artifact primary key is not unique")
        if not frame.equals(frame.sort(sort_key)):
            raise ValueError("artifact rows are not canonically sorted")
        return frame

    def _verified_manifest(
        self, study: StrategyStudyRecord
    ) -> tuple[Path, dict[str, JsonValue]]:
        if study.artifact_dir is None:
            raise ValueError("strategy study has no published artifacts")
        directory = Path(study.artifact_dir).resolve()
        if not directory.is_relative_to(self._artifact_root):
            raise ValueError("strategy study artifact directory is outside trusted root")
        manifest_bytes = (directory / "manifest.json").read_bytes()
        if study.manifest_hash != hashlib.sha256(manifest_bytes).hexdigest():
            raise ValueError("strategy study manifest hash mismatch")
        manifest = json.loads(manifest_bytes)
        if not isinstance(manifest, dict) or not isinstance(
            manifest.get("artifacts"), list
        ):
            raise TypeError("strategy study Manifest is invalid")
        return directory, cast(dict[str, JsonValue], manifest)

    def _stage_directory(
        self, study: StrategyStudyRecord
    ) -> tuple[Path, Path] | None:
        if study.artifact_dir is None:
            return None
        directory = Path(study.artifact_dir).resolve()
        expected = (self._artifact_root / "strategy-studies" / study.id).resolve()
        if directory != expected or not directory.is_relative_to(self._artifact_root):
            raise ValueError("strategy study artifact directory is not trusted")
        if not directory.exists():
            raise ValueError("strategy study artifact directory is missing")
        tombstone = directory.with_name(f".{directory.name}.deleting-{uuid4().hex}")
        os.replace(directory, tombstone)
        return directory, tombstone

    @staticmethod
    def _restore_directory(staged: tuple[Path, Path] | None) -> None:
        if staged is not None and staged[1].exists():
            os.replace(staged[1], staged[0])

    @staticmethod
    def _purge_directory(staged: tuple[Path, Path] | None) -> None:
        if staged is None:
            return
        shutil.rmtree(staged[1], ignore_errors=True)
        try:
            staged[0].parent.rmdir()
        except OSError:
            pass


class StrategyStudyRoutes:
    """注册策略研究路由。入参：应用和服务。返回值：路由注册器。异常：框架注册失败时传播。"""

    @staticmethod
    def register(app: FastAPI, service: StrategyStudyDashboardService) -> None:
        """注册 HTTP 契约。入参：FastAPI 应用和接口服务。返回值：无。异常：路由冲突时由框架抛出。"""

        @app.get("/api/v1/strategies")
        def strategies() -> dict[str, JsonValue]:
            return service.strategies()

        @app.get("/api/v1/strategies/{strategy_id}")
        def strategy(strategy_id: str) -> dict[str, JsonValue]:
            try:
                return service.strategy(strategy_id)
            except ValueError as error:
                raise HTTPException(status_code=404) from error

        @app.post("/api/v1/strategy-studies/validate")
        def validate(body: YamlBody) -> dict[str, JsonValue]:
            return service.validate(body.yaml)

        @app.post("/api/v1/strategy-studies", status_code=202)
        def submit(body: YamlBody, request: Request) -> dict[str, JsonValue]:
            return service.submit(body.yaml, request.state.request_id)

        @app.get("/api/v1/strategy-studies")
        def list_studies(
            limit: int = Query(default=100, ge=1, le=500),
            offset: int = Query(default=0, ge=0),
            status: StrategyStudyStatus | None = None,
        ) -> dict[str, JsonValue]:
            return service.list(limit, offset, status)

        @app.get("/api/v1/strategy-studies/{study_id}")
        def show(study_id: str) -> dict[str, JsonValue]:
            return service.show(study_id)

        @app.get("/api/v1/strategy-studies/{study_id}/report")
        def report(study_id: str) -> StrategyStudyReportResponse:
            return service.report(study_id)

        @app.delete("/api/v1/strategy-studies/{study_id}")
        def delete(study_id: str, request: Request) -> dict[str, JsonValue]:
            return service.delete(study_id, request.state.request_id)

        @app.get("/api/v1/strategy-studies/{study_id}/artifacts/{artifact_type}")
        def artifact(
            study_id: str,
            artifact_type: str,
            page: int = Query(default=1, ge=1),
            page_size: int = Query(default=100, ge=1, le=1000),
            dimension: str | None = Query(default=None),
        ) -> dict[str, JsonValue]:
            return service.artifact(
                study_id,
                artifact_type,
                page,
                page_size,
                dimension=dimension,
            )


__all__ = [
    "StrategyStudyDashboardService",
    "StrategyStudyRoutes",
    "YamlBody",
]
