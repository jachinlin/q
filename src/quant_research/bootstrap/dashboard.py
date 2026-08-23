"""装配本地 Dashboard 的 HTTP、应用服务与持久化依赖。"""

from __future__ import annotations

import urllib.request
from contextlib import suppress
from pathlib import Path

from fastapi import FastAPI

from quant_research.application.experiments import ExperimentService
from quant_research.application.operations import OperationalCommandService
from quant_research.backtest.rulebook import AShareRuleBook
from quant_research.config import Settings
from quant_research.dashboard.app import create_dashboard_app as create_http_app
from quant_research.dashboard.experiments import ExperimentDashboardService
from quant_research.dashboard.market_review import MarketReviewService
from quant_research.dashboard.notebook import NotebookProbe
from quant_research.dashboard.views import DashboardViewService
from quant_research.data.pipeline.publish import DataUpdatePlanner
from quant_research.data.repository import CanonicalResearchRepository
from quant_research.infrastructure.baostock import (
    BAOSTOCK_ROUTES,
    BaoStockCalendarPolicy,
    BaoStockClient,
    BaoStockConfig,
    BaoStockSdkGateway,
)
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.experiment_runs import (
    ExperimentRunRegistry,
)
from quant_research.infrastructure.persistence.repositories import MetadataRepository
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.strategies.registry import StrategyRegistry


class _LocalNotebookProbe:
    """通过固定回环地址探测本机 JupyterLab。

    入参：
        status_url：Jupyter Server 状态端点。
        timeout：单次 HTTP 探测的超时秒数。
    返回值：
        构造并返回可注入 Dashboard 的本机探测器。
    异常：
        构造阶段无主动异常；探测阶段将预期网络异常收敛为未就绪。
    """

    def __init__(
        self,
        status_url: str = "http://127.0.0.1:8009/api/status",
        *,
        timeout: float = 0.25,
    ) -> None:
        self._status_url = status_url
        self._timeout = timeout

    def is_ready(self) -> bool:
        """返回 Jupyter Server 状态端点是否成功响应。

        入参：
            无。
        返回值：
            HTTP 200 时返回 ``True``，连接失败、超时或其他状态返回 ``False``。
        异常：
            ``OSError`` 和无效响应被收敛；其他编程错误继续传播。
        """
        try:
            with urllib.request.urlopen(
                self._status_url,
                timeout=self._timeout,
            ) as response:
                return int(response.status) == 200
        except (OSError, ValueError):
            return False


class DashboardBootstrap:
    """集中创建 Dashboard 所需的配置、仓储和应用服务。

    入参：
        无；通过类方法执行组合。
    返回值：
        构造并返回 ``DashboardBootstrap`` 类型。
    异常：
        组合阶段的配置或依赖异常由类方法传播。
    """

    @classmethod
    def build_app(
        cls,
        *,
        static_dir: Path | None = None,
        allowed_hosts: tuple[str, ...] = ("127.0.0.1", "localhost", "[::1]"),
        notebook_probe: NotebookProbe | None = None,
    ) -> FastAPI:
        """创建具有资源生命周期管理的本地 Dashboard。

        入参：
            static_dir：可选的 SPA 构建目录；默认使用根目录 ``frontend/dist``。
            allowed_hosts：允许访问本地服务的 Host 白名单。
            notebook_probe：可选的 JupyterLab 状态探测器；默认探测固定回环端点。
        返回值：
            返回完整装配的 ``FastAPI`` 应用。
        异常：
            配置、数据库或规则文件无效时传播对应异常。
        """
        source_root = cls._source_root()
        settings = Settings.load()
        upgrade_database(settings.state_db)
        engine = create_sqlite_engine(settings.state_db)
        try:
            config_root = source_root / "configs"
            repository = CanonicalResearchRepository.from_sqlite(
                engine,
                trusted_curated_root=settings.curated_root,
            )
            rulebook = AShareRuleBook.load(config_root / "rules" / "a_share.yaml")
            queue = TaskQueue(
                engine,
                task_log_root=settings.data_root / "state" / "task-logs",
            )
            source_config = BaoStockConfig(
                max_instruments_per_batch=100,
                max_days_per_batch=366,
                max_attempts=5,
                retry_backoff_seconds=(1.0, 2.0, 4.0, 8.0),
                retryable_error_codes=frozenset({"-1", "10002007"}),
            )
            calendar_client = BaoStockClient(BaoStockSdkGateway(), None, source_config)
            service = DashboardViewService(
                engine,
                settings,
                repository,
                MarketReviewService(repository, rulebook),
                BAOSTOCK_ROUTES,
            )
            strategies = StrategyRegistry.builtins(
                commission_bps=rulebook.commission_bps,
                commission_minimum_fen=rulebook.commission_minimum_fen,
            )
            experiments = ExperimentService(
                ExperimentRunRegistry(engine), repository.catalog(), strategies
            )
            commands = OperationalCommandService(
                queue,
                DataUpdatePlanner(
                    calendar=BaoStockCalendarPolicy(calendar_client),
                    repository=MetadataRepository(engine),
                    routes=BAOSTOCK_ROUTES,
                ),
                experiments,
            )
            return create_http_app(
                service=service,
                commands=commands,
                experiment_service=ExperimentDashboardService(
                    experiments, strategies, settings.artifact_root
                ),
                notebook_probe=notebook_probe or _LocalNotebookProbe(),
                static_dir=static_dir or source_root / "frontend" / "dist",
                allowed_hosts=allowed_hosts,
                close_callback=engine.dispose,
            )
        except BaseException:
            with suppress(BaseException):
                engine.dispose()
            raise

    @staticmethod
    def _source_root() -> Path:
        return Path(__file__).resolve().parents[3]


def create_dashboard_app() -> FastAPI:
    """创建默认 Dashboard；该函数是 Uvicorn factory 框架入口。

    入参：
        无。
    返回值：
        返回完整装配的 ``FastAPI`` 应用。
    异常：
        配置或本地运行依赖无效时传播对应异常。
    """
    return DashboardBootstrap.build_app()
