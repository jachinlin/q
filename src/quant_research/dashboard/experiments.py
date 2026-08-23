"""提供统一实验 HTTP 路由和可信 Run 产物读取。"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

import polars as pl
from fastapi import FastAPI, Query, Request
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, ConfigDict, Field

from quant_research.application.experiments import ExperimentService
from quant_research.data.contracts import JsonValue
from quant_research.experiments.models import ExperimentAggregate, ResearchMark
from quant_research.strategies.components import StrategyComponentCatalog
from quant_research.strategies.registry import StrategyRegistry

_ARTIFACT_TYPES = frozenset(
    {
        "correlation",
        "coverage",
        "ic",
        "signals",
        "orders",
        "fills",
        "holdings",
        "costs",
        "nav",
        "performance",
        "attribution",
        "config",
        "metrics",
        "manifest",
        "long_short_returns",
        "quantile_returns",
        "summary",
    }
)


class _StrictBody(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class YamlBody(_StrictBody):
    """接收由后端作为唯一解析来源的 YAML 文本。

    入参：
        yaml：非空实验定义或 Run 配置文本。
    返回值：
        创建禁止额外字段的请求体模型。
    异常：
        ValidationError：文本为空或包含额外字段时由 Pydantic 抛出。
    """

    yaml: str = Field(min_length=1)


class MarkBody(_StrictBody):
    """接收 Run 研究标记。

    入参：
        mark：UNREVIEWED、BASELINE、CANDIDATE 或 DISCARDED。
    返回值：
        创建严格研究标记请求体。
    异常：
        ValidationError：标记非法或出现额外字段时由 Pydantic 抛出。
    """

    mark: ResearchMark


class CompareBody(_StrictBody):
    """接收两个以上待比较 Run ID。

    入参：
        run_ids：至少两个 Run 标识。
    返回值：
        创建严格比较请求体。
    异常：
        ValidationError：Run 数量不足或出现额外字段时由 Pydantic 抛出。
    """

    run_ids: tuple[str, ...] = Field(min_length=2)


class ExperimentDashboardService:
    """把实验用例转换为 HTTP JSON，并限制产物读取边界。

    入参：
        experiments：统一实验应用服务；strategies：策略注册表；artifact_root：
        唯一可信实验产物根。
    返回值：
        创建供 FastAPI 路由使用的无基础设施依赖服务。
    异常：
        产物根解析失败时传播文件系统异常。
    """

    def __init__(
        self,
        experiments: ExperimentService,
        strategies: StrategyRegistry,
        artifact_root: Path,
    ) -> None:
        self._experiments = experiments
        self._strategies = strategies
        self._components = StrategyComponentCatalog()
        self._artifact_root = artifact_root.resolve()

    def strategies(self) -> dict[str, JsonValue]:
        """返回策略 ID、截面五模块目录和编排器参数 Schema。

        入参：
            无。
        返回值：
            返回稳定排序的策略标识、组件标识、参数 Schema 和能力规则。
        异常：
            注册表包含冲突策略时由组合根提前抛出。
        """
        details = self._components.describe()
        return {
            "strategies": list(self._strategies.strategy_ids()),
            "components": {
                key: list(values) for key, values in self._components.list().items()
            },
            "component_schemas": details["components"],
            "capability_rules": details["capability_rules"],
        }

    @staticmethod
    def aggregate(value: ExperimentAggregate) -> dict[str, JsonValue]:
        """序列化实验聚合，不泄露 ORM。

        入参：
            value：应用层返回的冻结实验聚合。
        返回值：
            返回只包含 JSON 值的实验、Run 和标签映射。
        异常：
            模型中存在不可序列化字段时抛出 Pydantic 序列化异常。
        """
        return cast(
            dict[str, JsonValue],
            {
                "experiment": value.experiment.model_dump(mode="json"),
                "runs": [item.model_dump(mode="json") for item in value.runs],
                "tags": list(value.tags),
            },
        )

    def artifact(
        self, run_id: str, artifact_type: str, page: int, page_size: int
    ) -> dict[str, JsonValue]:
        """只从登记 Run 的可信 Manifest 读取并校验白名单产物。

        入参：
            run_id、artifact_type：Run 和白名单产物类型；page、page_size：
            Parquet 明细分页参数。
        返回值：
            JSON 产物返回 value；Parquet 返回分页 items 和总行数；Manifest 原样返回。
        异常：
            ValueError：类型不受支持、路径越界、产物未发布或完整性校验失败时抛出。
        """
        if artifact_type not in _ARTIFACT_TYPES:
            raise ValueError("unsupported artifact type")
        run = self._experiments.get_run(run_id)
        if run.artifact_dir is None:
            raise ValueError("Run has no published artifacts")
        directory = Path(run.artifact_dir).resolve()
        if not directory.is_relative_to(self._artifact_root):
            raise ValueError("Run artifact directory is outside trusted root")
        manifest_path = directory / "manifest.json"
        manifest_bytes = manifest_path.read_bytes()
        if run.manifest_hash != hashlib.sha256(manifest_bytes).hexdigest():
            raise ValueError("Run manifest hash mismatch")
        manifest = json.loads(manifest_bytes)
        if artifact_type == "manifest":
            return cast(dict[str, JsonValue], manifest)
        entry = next(
            (
                item
                for item in manifest["artifacts"]
                if item["artifact_type"] == artifact_type
            ),
            None,
        )
        if entry is None:
            raise ValueError("Run did not publish requested artifact")
        path = (directory / entry["relative_path"]).resolve()
        if not path.is_relative_to(directory):
            raise ValueError("artifact path escaped Run directory")
        content = path.read_bytes()
        if (
            len(content) != entry["byte_count"]
            or hashlib.sha256(content).hexdigest() != entry["content_hash"]
        ):
            raise ValueError("artifact integrity check failed")
        if path.suffix == ".json":
            return {"value": cast(JsonValue, json.loads(content))}
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
        return {
            "items": self._json_rows(
                frame.slice((page - 1) * page_size, page_size)
            ),
            "page": page,
            "page_size": page_size,
            "total": len(frame),
        }

    @staticmethod
    def _json_rows(frame: pl.DataFrame) -> list[JsonValue]:
        """把 Parquet 分页结果中的日期等原生标量转换为 JSON 值。

        入参：
            frame：已经通过 Manifest 完整性、Schema、主键和排序校验的分页数据帧。
        返回值：
            返回可由 FastAPI 响应模型和 JSON 编码器直接处理的行对象列表；日期和
            时间值使用 ISO 8601 字符串。
        异常：
            ValueError：数据中存在 FastAPI 编码器无法转换的值时传播编码异常。
        """
        return cast(list[JsonValue], jsonable_encoder(frame.to_dicts()))

    def validate(self, yaml_text: str) -> dict[str, JsonValue]:
        """校验并规范化实验 YAML，不产生持久化写入。

        入参：
            yaml_text：实验 YAML 文本。
        返回值：
            返回配置哈希和规范化实验定义。
        异常：
            ValueError：YAML 或领域约束非法时抛出。
        """
        resolved = self._experiments.validate_experiment(yaml_text)
        return {
            "config_hash": resolved.config_hash,
            "normalized": cast(
                dict[str, JsonValue], resolved.definition.model_dump(mode="json")
            ),
        }

    def submit(self, yaml_text: str, actor: str) -> dict[str, JsonValue]:
        """提交实验并序列化首个已入队 Run。

        入参：
            yaml_text：严格实验 YAML；actor：请求审计标识。
        返回值：
            返回新实验聚合。
        异常：
            配置、数据门禁或持久化失败时传播应用层异常。
        """
        return self.aggregate(self._experiments.submit(yaml_text, actor=actor))

    def list(self, limit: int, offset: int) -> dict[str, JsonValue]:
        """分页返回实验摘要。

        入参：
            limit、offset：页大小和零基偏移量。
        返回值：
            返回摘要 items 及分页参数。
        异常：
            持久化读取失败时传播应用层异常。
        """
        records = self._experiments.list(limit=limit, offset=offset)
        items: list[JsonValue] = []
        for record in records:
            aggregate = self._experiments.show(record.id)
            latest = aggregate.runs[-1] if aggregate.runs else None
            items.append(
                {
                    **record.model_dump(mode="json"),
                    "latest_run": latest.model_dump(mode="json") if latest else None,
                    "run_count": len(aggregate.runs),
                    "test_uses": sum(item.uses_test_region for item in aggregate.runs),
                }
            )
        return {
            "items": items,
            "limit": limit,
            "offset": offset,
        }

    def detail(self, experiment_id: str) -> dict[str, JsonValue]:
        """返回实验定义及全部 Run。

        入参：
            experiment_id：实验标识。
        返回值：
            返回实验聚合 JSON。
        异常：
            KeyError：实验不存在时抛出。
        """
        return self.aggregate(self._experiments.show(experiment_id))

    def add_run(
        self, experiment_id: str, yaml_text: str, actor: str
    ) -> dict[str, JsonValue]:
        """为实验追加显式 Run。

        入参：
            experiment_id：实验标识；yaml_text：严格 Run YAML；actor：审计标识。
        返回值：
            返回更新后的实验聚合。
        异常：
            实验不存在或 Run 配置违反协议时传播应用层异常。
        """
        return self.aggregate(
            self._experiments.add_run(experiment_id, yaml_text, actor=actor)
        )

    def rerun(self, run_id: str, actor: str) -> dict[str, JsonValue]:
        """从冻结配置创建不可覆盖的新 Run。

        入参：
            run_id：源 Run 标识；actor：审计标识。
        返回值：
            返回包含新 Run 的实验聚合。
        异常：
            KeyError：源 Run 不存在时抛出。
        """
        return self.aggregate(self._experiments.rerun(run_id, actor=actor))

    def mark(self, run_id: str, mark: ResearchMark, actor: str) -> dict[str, JsonValue]:
        """修改 Run 研究标记并维护 baseline 指针。

        入参：
            run_id、mark：Run 标识和目标标记；actor：审计标识。
        返回值：
            返回更新后的实验聚合。
        异常：
            KeyError：Run 不存在时抛出；baseline 写入失败时传播存储异常。
        """
        return self.aggregate(self._experiments.mark(run_id, mark, actor=actor))

    def compare(self, run_ids: tuple[str, ...]) -> dict[str, JsonValue]:
        """读取多个 Run 的冻结配置和已登记指标摘要。

        入参：
            run_ids：至少两个且互不重复的 Run 标识。
        返回值：
            返回与输入顺序一致的 Run 快照数组。
        异常：
            ValueError：标识重复时抛出；KeyError：任一 Run 不存在时抛出。
        """
        if len(set(run_ids)) != len(run_ids):
            raise ValueError("run_ids must be unique")
        return {
            "runs": [
                cast(JsonValue, self._experiments.get_run(item).model_dump(mode="json"))
                for item in run_ids
            ]
        }


class ExperimentRoutes:
    """注册目标 `/api/v1/experiments`、Run 和策略接口。

    入参：
        mount 接收 FastAPI 应用和实验 Dashboard 服务。
    返回值：
        类仅提供无状态路由挂载入口。
    异常：
        路由重复或 FastAPI 应用无效时由框架抛出。
    """

    @staticmethod
    def mount(app: FastAPI, service: ExperimentDashboardService) -> None:
        """将统一实验与策略 HTTP 端点挂载到应用。

        入参：
            app：FastAPI 应用；service：实验 Dashboard 服务。
        返回值：
            路由注册完成后返回 None。
        异常：
            FastAPI 路由注册失败时传播框架异常。
        """

        @app.get("/api/v1/strategies")
        def strategies() -> dict[str, JsonValue]:
            return service.strategies()

        @app.post("/api/v1/experiments/validate")
        def validate(body: YamlBody) -> dict[str, JsonValue]:
            return service.validate(body.yaml)

        @app.post("/api/v1/experiments", status_code=202)
        def submit(body: YamlBody, request: Request) -> dict[str, JsonValue]:
            return service.submit(body.yaml, request.state.request_id)

        @app.get("/api/v1/experiments")
        def list_experiments(
            limit: int = Query(100, ge=1, le=200), offset: int = Query(0, ge=0)
        ) -> dict[str, JsonValue]:
            return service.list(limit, offset)

        @app.get("/api/v1/experiments/{experiment_id}")
        def detail(experiment_id: str) -> dict[str, JsonValue]:
            return service.detail(experiment_id)

        @app.post("/api/v1/experiments/{experiment_id}/runs", status_code=202)
        def add_run(
            experiment_id: str, body: YamlBody, request: Request
        ) -> dict[str, JsonValue]:
            return service.add_run(experiment_id, body.yaml, request.state.request_id)

        @app.post("/api/v1/runs/{run_id}/rerun", status_code=202)
        def rerun(run_id: str, request: Request) -> dict[str, JsonValue]:
            return service.rerun(run_id, request.state.request_id)

        @app.patch("/api/v1/runs/{run_id}/research")
        def mark(run_id: str, body: MarkBody, request: Request) -> dict[str, JsonValue]:
            return service.mark(run_id, body.mark, request.state.request_id)

        @app.post("/api/v1/experiments/compare")
        def compare(body: CompareBody) -> dict[str, JsonValue]:
            return service.compare(body.run_ids)

        @app.get("/api/v1/runs/{run_id}/artifacts/{artifact_type}")
        def artifact(
            run_id: str,
            artifact_type: str,
            page: int = Query(1, ge=1),
            page_size: int = Query(100, ge=1, le=1000),
        ) -> dict[str, JsonValue]:
            return service.artifact(run_id, artifact_type, page, page_size)


__all__ = ["ExperimentDashboardService", "ExperimentRoutes"]
