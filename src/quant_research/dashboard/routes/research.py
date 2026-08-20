"""注册统一研究中心 HTTP API。"""

from __future__ import annotations

from fastapi import FastAPI, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from quant_research.application.research_platform import ResearchCommandService
from quant_research.dashboard.research_views import ResearchDashboardService
from quant_research.data.contracts import JsonValue


class _RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ResearchYamlRequest(_RequestModel):
    """接收受限大小的内存研究 YAML。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    config_yaml: str = Field(min_length=1, max_length=1_048_576)


class ResearchUpdateRequest(_RequestModel):
    """接收可审计的研究者标记、标签与结论。

入参：
    参数和字段含义由公开签名及类型声明给出。
返回值：
    返回该操作构造、计算或查询得到的领域结果。
异常：
    输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    mark: str
    tags: tuple[str, ...] = Field(default=(), max_length=32)
    note: str | None = Field(default=None, max_length=20_000)


class _ResearchRoutes:
    """集中挂载 `/api/v1/research` 路由。"""

    @staticmethod
    def mount(
        app: FastAPI,
        views: ResearchDashboardService,
        commands: ResearchCommandService,
    ) -> None:
        """挂载组件、模板、解析、提交、详情和重跑 API。"""

        @app.get("/api/v1/research/components")
        def components() -> dict[str, JsonValue]:
            return views.component_catalog()

        @app.get("/api/v1/research/templates")
        def templates() -> dict[str, JsonValue]:
            return views.templates()

        @app.post("/api/v1/research/validate")
        def validate(body: ResearchYamlRequest) -> dict[str, JsonValue]:
            return commands.validate_yaml(body.config_yaml)

        @app.post("/api/v1/research/families", status_code=202)
        def submit(request: Request, body: ResearchYamlRequest) -> dict[str, JsonValue]:
            return commands.submit(body.config_yaml, request_id=request.state.request_id)

        @app.get("/api/v1/research/families")
        def families(
            page: int = Query(default=1, ge=1),
            page_size: int = Query(default=25, ge=1, le=200),
        ) -> dict[str, JsonValue]:
            return views.families(page=page, page_size=page_size)

        @app.get("/api/v1/research/families/{family_id}")
        def family(family_id: str) -> dict[str, JsonValue]:
            return views.family(family_id)

        @app.patch("/api/v1/research/families/{family_id}")
        def update_family(
            family_id: str,
            body: ResearchUpdateRequest,
        ) -> dict[str, JsonValue]:
            return views.update_research(
                family_id,
                mark=body.mark,
                note=body.note,
                tags=body.tags,
            )

        @app.post("/api/v1/research/families/{family_id}/executions", status_code=202)
        def rerun(family_id: str, request: Request) -> dict[str, JsonValue]:
            return commands.rerun(family_id, request_id=request.state.request_id)

        @app.get("/api/v1/research/runs/{run_id}/artifacts/{artifact_type}")
        def artifact(
            run_id: str,
            artifact_type: str,
            page: int = Query(default=1, ge=1),
            page_size: int = Query(default=200, ge=1, le=2000),
        ) -> dict[str, JsonValue]:
            return views.artifact(
                run_id,
                artifact_type,
                page=page,
                page_size=page_size,
            )
