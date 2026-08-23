"""提供研究界面与app相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import FileResponse, JSONResponse
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import Response

from quant_research.application.operations import OperationalCommandService
from quant_research.dashboard.experiments import (
    ExperimentDashboardService,
    ExperimentRoutes,
)
from quant_research.dashboard.notebook import NotebookProbe
from quant_research.dashboard.routes.api import _DashboardRoutes
from quant_research.dashboard.views import DashboardViewService
from quant_research.domain.errors import QuantError


def create_dashboard_app(
    *,
    service: DashboardViewService,
    commands: OperationalCommandService,
    experiment_service: ExperimentDashboardService,
    notebook_probe: NotebookProbe,
    static_dir: Path,
    allowed_hosts: tuple[str, ...] = ("127.0.0.1", "localhost", "[::1]"),
    close_callback: Callable[[], None] | None = None,
) -> FastAPI:
    """创建并返回约定对象；该函数作为稳定公开 API保留在模块级。

    入参：
        service：只读 Dashboard 展示服务。
        commands：受控研究写用例服务。
        notebook_probe：本机 JupyterLab 就绪状态的消费者侧探测器。
        static_dir：已构建 SPA 的静态目录。
        allowed_hosts：参与本次处理的允许``hosts``；调用方不得依赖未声明的顺序。
        close_callback：应用生命周期结束时调用的资源释放函数。
    返回值：
        返回创建``dashboard``应用后的``dashboard``应用（``FastAPI``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``HTTPException``。
    Create one local-only API and optional built SPA host.
    """

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if close_callback is not None:
                close_callback()

    app = FastAPI(
        title="Quant Research Dashboard API",
        version="1.0.0",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.dashboard_service = service
    app.state.dashboard_commands = commands
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=list(allowed_hosts))

    @app.middleware("http")
    async def secure_mutations(
        request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request_id = request.headers.get("X-Request-ID") or str(uuid4())
        request.state.request_id = request_id
        if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            supplied = request.headers.get("X-Request-ID")
            if supplied is None or not supplied.strip() or len(supplied) > 128:
                return _AppSupport._error_response(
                    400,
                    "REQUEST_ID_REQUIRED",
                    "写操作必须提供有效的 X-Request-ID",
                    request_id,
                )
            content_type = request.headers.get("content-type", "")
            if not content_type.lower().startswith("application/json"):
                return _AppSupport._error_response(
                    415,
                    "JSON_REQUIRED",
                    "写操作只接受 application/json",
                    request_id,
                )
            origin = request.headers.get("origin")
            expected = f"{request.url.scheme}://{request.url.netloc}"
            development_origin = os.environ.get("QUANT_DASHBOARD_DEV_ORIGIN")
            if origin not in {expected, development_origin}:
                return _AppSupport._error_response(
                    403,
                    "ORIGIN_REJECTED",
                    "写操作必须来自 Dashboard 同源页面",
                    request_id,
                )
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        return response

    @app.exception_handler(QuantError)
    async def quant_error(request: Request, error: QuantError) -> JSONResponse:
        detail = error.detail
        return JSONResponse(
            status_code=409 if detail.retryable else 422,
            content={
                "error": {
                    "code": detail.code,
                    "message": detail.message,
                    "severity": detail.severity.value,
                    "retryable": detail.retryable,
                    "remediation": detail.remediation,
                    "context": dict(detail.context),
                    "request_id": request.state.request_id,
                }
            },
        )

    @app.exception_handler(ValueError)
    async def value_error(request: Request, error: ValueError) -> JSONResponse:
        return _AppSupport._error_response(
            422,
            "DASHBOARD_INPUT_INVALID",
            str(error),
            request.state.request_id,
            remediation="修改实验配置后重新校验；若校验已通过，请刷新页面以避免提交旧配置。",
        )

    @app.exception_handler(TypeError)
    async def type_error(request: Request, error: TypeError) -> JSONResponse:
        return _AppSupport._error_response(
            422,
            "DASHBOARD_INPUT_TYPE_INVALID",
            str(error),
            request.state.request_id,
            remediation="按字段 Schema 修正类型后重新校验并提交。",
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(
        request: Request, error: RequestValidationError
    ) -> JSONResponse:
        issues = error.errors()
        first = issues[0] if issues else {}
        location = ".".join(str(item) for item in first.get("loc", ()))
        detail = str(first.get("msg", "请求参数不符合接口约束"))
        message = f"{location}: {detail}" if location else detail
        return _AppSupport._error_response(
            422,
            "REQUEST_VALIDATION_FAILED",
            message,
            request.state.request_id,
            remediation="刷新页面后重试；若仍失败，请确认请求体只包含非空 yaml 字段。",
        )

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error: HTTPException) -> JSONResponse:
        code = "RESOURCE_NOT_FOUND" if error.status_code == 404 else "HTTP_ERROR"
        message = "请求的资源不存在" if error.status_code == 404 else "请求无法处理"
        return _AppSupport._error_response(
            error.status_code,
            code,
            message,
            request.state.request_id,
        )

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, _: Exception) -> JSONResponse:
        return _AppSupport._error_response(
            500,
            "DASHBOARD_INTERNAL_ERROR",
            "Dashboard 暂时无法完成请求",
            request.state.request_id,
            retryable=True,
            remediation="稍后重试；若问题持续，请检查本机 Worker 和状态数据库。",
        )

    _DashboardRoutes.mount(app, service, commands, notebook_probe)
    ExperimentRoutes.mount(app, experiment_service)

    built = static_dir.resolve()
    index = built / "index.html"

    @app.get("/")
    def spa_root() -> object:
        if index.is_file():
            return FileResponse(index)
        return {"service": "quant-dashboard", "frontend": "not-built"}

    @app.get("/{spa_path:path}")
    def spa(spa_path: str) -> object:
        if spa_path.startswith("api/"):
            raise HTTPException(status_code=404)
        candidate = (built / spa_path).resolve()
        if candidate.is_relative_to(built) and candidate.is_file():
            return FileResponse(candidate)
        if index.is_file():
            return FileResponse(index)
        raise HTTPException(status_code=404)

    return app


class _AppSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _error_response(
        status_code: int,
        code: str,
        message: str,
        request_id: str,
        *,
        retryable: bool = False,
        remediation: str | None = None,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status_code,
            content={
                "error": {
                    "code": code,
                    "message": message,
                    "severity": "SEVERE",
                    "retryable": retryable,
                    "remediation": remediation,
                    "context": {},
                    "request_id": request_id,
                }
            },
        )
