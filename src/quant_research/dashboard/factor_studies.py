"""提供独立因子研究 HTTP 路由和可信产物读取。"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from pathlib import Path
from typing import Literal, cast
from uuid import uuid4

import polars as pl
from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ConfigDict, Field

from quant_research.application.factor_studies import FactorStudyService
from quant_research.data.contracts import JsonValue
from quant_research.factor_studies.models import (
    FactorDecisionMark,
    FactorStudyDecisionKey,
    FactorStudyRecord,
    FactorStudyStatus,
)

_ARTIFACT_TYPES = frozenset(
    {
        "summary",
        "coverage",
        "label_quality",
        "industry_coverage",
        "ic",
        "quantile_returns",
        "long_short_returns",
        "monotonicity",
        "turnover",
        "cost_scenarios",
        "correlation",
        "config",
        "metrics",
        "manifest",
    }
)
_FILTERS: dict[str, frozenset[str]] = {
    "summary": frozenset({"signal_variant", "label_kind", "factor_ref", "horizon"}),
    "coverage": frozenset({"signal_variant", "factor_ref"}),
    "label_quality": frozenset({"label_kind", "horizon", "reason"}),
    "industry_coverage": frozenset({"taxonomy", "unclassified_policy"}),
    "ic": frozenset({"signal_variant", "label_kind", "factor_ref", "horizon"}),
    "quantile_returns": frozenset(
        {"signal_variant", "label_kind", "factor_ref", "horizon"}
    ),
    "long_short_returns": frozenset(
        {"signal_variant", "label_kind", "factor_ref", "horizon"}
    ),
    "monotonicity": frozenset(
        {"signal_variant", "label_kind", "factor_ref", "horizon"}
    ),
    "turnover": frozenset({"signal_variant", "factor_ref"}),
    "cost_scenarios": frozenset(
        {"signal_variant", "label_kind", "factor_ref", "horizon", "cost_bps"}
    ),
    "correlation": frozenset({"signal_variant"}),
}


class _StrictBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class FactorStudyYamlBody(_StrictBody):
    """接收研究 YAML。入参：非空文本。返回值：严格请求体。异常：字段非法时返回校验错误。"""

    yaml: str = Field(min_length=1)


class FactorStudyDecisionBody(_StrictBody):
    """接收人工结论。入参：四维键、标记和备注。返回值：严格请求体。异常：字段非法时返回校验错误。"""

    signal_variant: str = Field(min_length=1)
    label_kind: str = Field(min_length=1)
    factor_ref: str = Field(min_length=1)
    horizon: int = Field(gt=0)
    mark: Literal["UNREVIEWED", "CANDIDATE", "DISCARDED"]
    note: str = Field(default="", max_length=4000)


class FactorStudyDashboardService:
    """转换研究 HTTP 数据。入参：研究服务和可信根。返回值：Dashboard 服务。异常：路径或依赖非法时抛出。"""

    def __init__(self, studies: FactorStudyService, artifact_root: Path) -> None:
        self._studies = studies
        self._artifact_root = artifact_root.resolve()

    def catalog(self) -> dict[str, JsonValue]:
        """读取创建目录。入参：无。返回值：因子和枚举 JSON。异常：目录不可用时由依赖抛出。"""
        return {
            "factors": [{"factor_id": item} for item in self._studies.catalog()],
            "universes": ["CN_STOCK_STANDARD"],
            "corrections": ["BONFERRONI", "BH_FDR"],
            "industry_policies": ["EXCLUDE", "UNCLASSIFIED"],
            "label_kinds": [
                "THEORETICAL_FORWARD_RETURN",
                "EXECUTABLE_FORWARD_RETURN",
            ],
        }

    def validate(self, yaml_text: str) -> dict[str, JsonValue]:
        """校验研究配置。入参：YAML 文本。返回值：规范配置和哈希。异常：配置非法时抛出值错误。"""
        resolved = self._studies.validate(yaml_text)
        return {
            "config_hash": resolved.config_hash,
            "normalized": cast(
                dict[str, JsonValue],
                resolved.definition.model_dump(mode="json"),
            ),
        }

    def submit(self, yaml_text: str, actor: str) -> dict[str, JsonValue]:
        """提交研究。入参：YAML 文本和操作者。返回值：已排队快照。异常：门禁或事务失败时抛出。"""
        return self._record(self._studies.submit(yaml_text, actor=actor))

    def list(
        self,
        limit: int,
        offset: int,
        status: FactorStudyStatus | None,
        decision: FactorDecisionMark | None,
    ) -> dict[str, JsonValue]:
        """列出工作台摘要。入参：分页和筛选条件。返回值：分页 JSON。异常：参数非法时抛出。"""
        records = self._studies.list(
            limit=limit, offset=offset, status=status, decision=decision
        )
        return {
            "items": [self._overview(item) for item in records],
            "limit": limit,
            "offset": offset,
        }

    def detail(self, study_id: str) -> dict[str, JsonValue]:
        """读取研究详情。入参：研究 ID。返回值：聚合 JSON。异常：研究不存在时抛出。"""
        return self._record(self._studies.show(study_id))

    def matrix(self, study_id: str) -> dict[str, JsonValue]:
        """构造决策矩阵。入参：研究 ID。返回值：证据与结论 JSON。异常：可信产物非法时抛出。"""
        study = self._studies.show(study_id)
        frame = self._frame(study, "summary")
        metrics = {item.name: item for item in study.metrics}
        decisions = {
            (
                item.signal_variant,
                item.label_kind,
                item.factor_ref,
                item.horizon,
            ): item
            for item in study.decisions
        }
        rows: list[JsonValue] = []
        for row in cast(list[dict[str, JsonValue]], jsonable_encoder(frame.to_dicts())):
            dimensions = "/".join(
                str(row[name])
                for name in ("signal_variant", "label_kind", "factor_ref", "horizon")
            )
            rank_metric = metrics.get(f"rank_ic_mean/{dimensions}")
            decision = decisions.get(
                (
                    str(row["signal_variant"]),
                    str(row["label_kind"]),
                    str(row["factor_ref"]),
                    int(cast(int, row["horizon"])),
                )
            )
            rows.append(
                {
                    "signal_variant": row["signal_variant"],
                    "label_kind": row["label_kind"],
                    "factor_ref": row["factor_ref"],
                    "horizon": row["horizon"],
                    "rank_ic_mean": row.get("rank_ic_mean"),
                    "rank_ic_hac_t_stat": row.get("rank_ic_hac_t_stat"),
                    "rank_ic_adjusted_p_value": (
                        rank_metric.adjusted_p_value if rank_metric else None
                    ),
                    "monotonicity_mean": row.get("monotonicity_mean"),
                    "gross_spread_mean": row.get("long_short_mean"),
                    "break_even_cost_bps": row.get("break_even_cost_bps"),
                    "total_turnover_mean": row.get("total_turnover_mean"),
                    "decision": (
                        decision.model_dump(mode="json") if decision else None
                    ),
                }
            )
        return {"items": rows, "total": len(rows)}

    def decide(
        self, study_id: str, body: FactorStudyDecisionBody, actor: str
    ) -> dict[str, JsonValue]:
        """保存人工结论。入参：研究、请求体和操作者。返回值：最新聚合 JSON。异常：矩阵键或状态非法时抛出。"""
        study = self._studies.show(study_id)
        summary = self._frame(study, "summary")
        matched = summary.filter(
            (pl.col("signal_variant") == body.signal_variant)
            & (pl.col("label_kind") == body.label_kind)
            & (pl.col("factor_ref") == body.factor_ref)
            & (pl.col("horizon") == body.horizon)
        )
        if matched.is_empty():
            raise ValueError("decision key is absent from published summary")
        value = self._studies.decide(
            study_id,
            FactorStudyDecisionKey(
                signal_variant=body.signal_variant,
                label_kind=body.label_kind,
                factor_ref=body.factor_ref,
                horizon=body.horizon,
            ),
            FactorDecisionMark(body.mark),
            body.note,
            actor=actor,
        )
        return self._record(value)

    def artifact(
        self,
        study_id: str,
        artifact_type: str,
        page: int,
        page_size: int,
        filters: dict[str, str | int],
    ) -> dict[str, JsonValue]:
        """读取可信产物。入参：研究、类型、分页和过滤。返回值：复核后的 JSON。异常：路径、哈希或过滤非法时抛出。"""
        study = self._studies.show(study_id)
        if artifact_type == "manifest":
            directory, manifest = self._manifest(study)
            del directory
            return manifest
        if artifact_type in {"config", "metrics"}:
            return {"value": self._json_artifact(study, artifact_type)}
        allowed = _FILTERS.get(artifact_type, frozenset())
        unsupported = set(filters) - allowed
        if unsupported:
            raise ValueError(
                f"unsupported filter for {artifact_type}: {min(unsupported)}"
            )
        frame = self._frame(study, artifact_type)
        for name, value in filters.items():
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

    def delete(self, study_id: str, actor: str) -> dict[str, JsonValue]:
        """删除终态研究。入参：研究 ID 和操作者。返回值：删除结果。异常：状态、路径或事务非法时回滚并抛出。"""
        study = self._studies.show(study_id)
        staged: tuple[Path, Path] | None = None
        if study.artifact_dir is not None:
            directory = Path(study.artifact_dir).resolve()
            expected = (self._artifact_root / "factor-studies" / study.id).resolve()
            if directory != expected:
                raise ValueError("factor study artifact directory identity mismatch")
            if directory.exists():
                tombstone = directory.with_name(
                    f".deleting-{study.id}-{uuid4().hex}"
                )
                os.replace(directory, tombstone)
                staged = (directory, tombstone)
        try:
            self._studies.delete(study_id, actor=actor)
        except BaseException:
            if staged is not None and staged[1].exists():
                os.replace(staged[1], staged[0])
            raise
        if staged is not None:
            shutil.rmtree(staged[1], ignore_errors=True)
        return {"factor_study_id": study_id, "status": "DELETED"}

    def _frame(self, study: FactorStudyRecord, artifact_type: str) -> pl.DataFrame:
        if artifact_type not in _ARTIFACT_TYPES:
            raise ValueError("unsupported artifact type")
        directory, manifest = self._manifest(study)
        entry = next(
            (
                item
                for item in cast(list[dict[str, JsonValue]], manifest["artifacts"])
                if item["artifact_type"] == artifact_type
            ),
            None,
        )
        if entry is None:
            raise ValueError("factor study did not publish requested artifact")
        path = (directory / cast(str, entry["relative_path"])).resolve()
        if not path.is_relative_to(directory):
            raise ValueError("artifact path escaped factor study directory")
        content = path.read_bytes()
        if len(content) != entry["byte_count"] or hashlib.sha256(
            content
        ).hexdigest() != entry["content_hash"]:
            raise ValueError("artifact integrity check failed")
        if path.suffix == ".json":
            raise ValueError("JSON artifact is not tabular")
        frame = pl.read_parquet(path)
        if entry.get("row_count") != len(frame):
            raise ValueError("artifact row count does not match Manifest")
        actual_schema = {name: str(dtype) for name, dtype in frame.schema.items()}
        if entry.get("schema") != actual_schema:
            raise ValueError("artifact schema does not match Manifest")
        keys = tuple(cast(list[str], entry.get("sort_key") or ()))
        primary = tuple(cast(list[str], entry.get("primary_key") or ()))
        if not keys or not primary:
            raise ValueError("Parquet artifact lacks key metadata")
        if frame.select(pl.struct(primary).is_duplicated().any()).item():
            raise ValueError("artifact primary key is not unique")
        if not frame.equals(frame.sort(keys)):
            raise ValueError("artifact rows are not canonically sorted")
        return frame

    def _json_artifact(
        self, study: FactorStudyRecord, artifact_type: str
    ) -> JsonValue:
        directory, manifest = self._manifest(study)
        entry = next(
            (
                item
                for item in cast(list[dict[str, JsonValue]], manifest["artifacts"])
                if item["artifact_type"] == artifact_type
            ),
            None,
        )
        if entry is None:
            raise ValueError("factor study did not publish requested artifact")
        path = (directory / cast(str, entry["relative_path"])).resolve()
        if not path.is_relative_to(directory):
            raise ValueError("artifact path escaped factor study directory")
        content = path.read_bytes()
        if len(content) != entry["byte_count"] or hashlib.sha256(
            content
        ).hexdigest() != entry["content_hash"]:
            raise ValueError("artifact integrity check failed")
        return cast(JsonValue, json.loads(content))

    def _manifest(
        self, study: FactorStudyRecord
    ) -> tuple[Path, dict[str, JsonValue]]:
        if study.artifact_dir is None or study.manifest_hash is None:
            raise ValueError("factor study has no published artifacts")
        directory = Path(study.artifact_dir).resolve()
        expected = (self._artifact_root / "factor-studies" / study.id).resolve()
        if directory != expected:
            raise ValueError("factor study artifact directory is outside trusted root")
        content = (directory / "manifest.json").read_bytes()
        if hashlib.sha256(content).hexdigest() != study.manifest_hash:
            raise ValueError("factor study manifest hash mismatch")
        return directory, cast(dict[str, JsonValue], json.loads(content))

    @staticmethod
    def _record(study: FactorStudyRecord) -> dict[str, JsonValue]:
        return cast(dict[str, JsonValue], study.model_dump(mode="json"))

    @staticmethod
    def _overview(study: FactorStudyRecord) -> dict[str, JsonValue]:
        summary = next(
            (item for item in study.artifacts if item.artifact_type == "summary"), None
        )
        matrix_total = summary.row_count if summary and summary.row_count else 0
        candidate = sum(
            item.mark is FactorDecisionMark.CANDIDATE for item in study.decisions
        )
        discarded = sum(
            item.mark is FactorDecisionMark.DISCARDED for item in study.decisions
        )
        return {
            **cast(dict[str, JsonValue], study.model_dump(mode="json")),
            "matrix_total": matrix_total,
            "candidate_count": candidate,
            "discarded_count": discarded,
            "unreviewed_count": max(0, matrix_total - candidate - discarded),
        }


class FactorStudyRoutes:
    """注册研究接口。入参：应用和服务。返回值：路由集合。异常：挂载冲突时由框架抛出。"""

    @staticmethod
    def _actor(request: Request) -> str:
        """返回审计关联标识；独立挂载路由时使用稳定本机主体。"""
        return str(getattr(request.state, "request_id", "dashboard"))

    @staticmethod
    def mount(app: FastAPI, service: FactorStudyDashboardService) -> None:
        """挂载研究路由。入参：FastAPI 应用和服务。返回值：无。异常：路由冲突时由框架抛出。"""

        @app.get("/api/v1/factor-studies/catalog")
        def catalog() -> dict[str, JsonValue]:
            return service.catalog()

        @app.post("/api/v1/factor-studies/validate")
        def validate(body: FactorStudyYamlBody) -> dict[str, JsonValue]:
            return service.validate(body.yaml)

        @app.post("/api/v1/factor-studies", status_code=202)
        def submit(
            body: FactorStudyYamlBody, request: Request
        ) -> dict[str, JsonValue]:
            return service.submit(body.yaml, FactorStudyRoutes._actor(request))

        @app.get("/api/v1/factor-studies")
        def list_studies(
            limit: int = Query(100, ge=1, le=200),
            offset: int = Query(0, ge=0),
            status: FactorStudyStatus | None = None,
            decision: FactorDecisionMark | None = None,
        ) -> dict[str, JsonValue]:
            return service.list(limit, offset, status, decision)

        @app.get("/api/v1/factor-studies/{study_id}")
        def detail(study_id: str) -> dict[str, JsonValue]:
            return service.detail(study_id)

        @app.delete("/api/v1/factor-studies/{study_id}")
        def delete_study(study_id: str, request: Request) -> dict[str, JsonValue]:
            return service.delete(study_id, FactorStudyRoutes._actor(request))

        @app.get("/api/v1/factor-studies/{study_id}/matrix")
        def matrix(study_id: str) -> dict[str, JsonValue]:
            return service.matrix(study_id)

        @app.put("/api/v1/factor-studies/{study_id}/decisions")
        def decide(
            study_id: str, body: FactorStudyDecisionBody, request: Request
        ) -> dict[str, JsonValue]:
            return service.decide(
                study_id, body, FactorStudyRoutes._actor(request)
            )

        @app.get("/api/v1/factor-studies/{study_id}/artifacts/{artifact_type}")
        def artifact(
            study_id: str,
            artifact_type: str,
            page: int = Query(1, ge=1),
            page_size: int = Query(100, ge=1, le=1000),
            signal_variant: str | None = Query(None, min_length=1),
            label_kind: str | None = Query(None, min_length=1),
            factor_ref: str | None = Query(None, min_length=1),
            horizon: int | None = Query(None, gt=0),
            reason: str | None = Query(None, min_length=1),
            taxonomy: str | None = Query(None, min_length=1),
            unclassified_policy: str | None = Query(None, min_length=1),
            cost_bps: int | None = Query(None, ge=0),
        ) -> dict[str, JsonValue]:
            filters = {
                name: value
                for name, value in {
                    "signal_variant": signal_variant,
                    "label_kind": label_kind,
                    "factor_ref": factor_ref,
                    "horizon": horizon,
                    "reason": reason,
                    "taxonomy": taxonomy,
                    "unclassified_policy": unclassified_policy,
                    "cost_bps": cost_bps,
                }.items()
                if value is not None
            }
            return service.artifact(study_id, artifact_type, page, page_size, filters)


__all__ = ["FactorStudyDashboardService", "FactorStudyRoutes"]
