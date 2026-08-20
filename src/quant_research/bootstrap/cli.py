"""装配并启动本地 ``quant`` 命令行应用。"""

from __future__ import annotations

import os
import sys
from contextlib import suppress
from pathlib import Path
from typing import TextIO

from sqlalchemy import Engine

from quant_research.application.data import DataUpdateHandler, DataValidationHandler
from quant_research.application.worker import Worker
from quant_research.backtest.rulebook import AShareRuleBook
from quant_research.bootstrap.research import build_research_platform
from quant_research.cli.app import (
    ApplicationServices,
    LocalTaskCommands,
    LocalWorkerCommands,
    create_app,
    run,
)
from quant_research.cli.research import LocalResearchCommands
from quant_research.config import Settings
from quant_research.dashboard.research_views import ResearchDashboardService
from quant_research.data.partitions import RawPartitionStore
from quant_research.data.pipelines.curate import CuratedPartitionStore
from quant_research.data.pipelines.publish import DataPipeline
from quant_research.data.quality.runner import QualityRunner
from quant_research.data.repository import CanonicalResearchRepository
from quant_research.infrastructure.baostock import (
    BAOSTOCK_ROUTES,
    BaoStockCalendarPolicy,
    BaoStockClient,
    BaoStockConfig,
    BaoStockMapper,
    BaoStockSdkGateway,
)
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.repositories import MetadataRepository
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.logging import (
    LogContext,
    StructuredLogger,
    TeeLogStream,
    sensitive_environment_values,
)


class CliBootstrap:
    """集中装配 CLI 的配置、数据源、持久化与后台 Worker。

    入参：
        无；通过类方法执行组合。
    返回值：
        构造并返回 ``CliBootstrap`` 类型。
    异常：
        组合阶段的配置或依赖异常由类方法传播。
    """

    @classmethod
    def build_services(cls) -> ApplicationServices:
        """为一次 CLI 调用装配数据库、数据流水线、任务队列和应用服务。

        入参：
            无。
        返回值：
            返回已装配且由调用方负责关闭的 ``ApplicationServices``。
        异常：
            配置、数据库或供应商依赖不可用时传播对应异常。
        """
        source_root = cls._source_root()
        settings = Settings.load()
        upgrade_database(settings.state_db)
        engine = create_sqlite_engine(settings.state_db)
        pipeline_logger: StructuredLogger | None = None
        pipeline_stream: TextIO | None = None
        try:
            repository = MetadataRepository(engine)
            source_config = BaoStockConfig(
                max_instruments_per_batch=100,
                max_days_per_batch=366,
                max_attempts=5,
                retry_backoff_seconds=(1.0, 2.0, 4.0, 8.0),
                retryable_error_codes=frozenset({"-1", "10002007"}),
            )
            source = BaoStockClient(BaoStockSdkGateway(), None, source_config)
            calendar_client = BaoStockClient(BaoStockSdkGateway(), None, source_config)
            pipeline_logger, pipeline_stream = cls._pipeline_logger(settings.data_root)
            pipeline = DataPipeline(
                source=source,
                mapper=BaoStockMapper(),
                calendar=BaoStockCalendarPolicy(calendar_client),
                raw_store=RawPartitionStore(settings.raw_root),
                curated_store=CuratedPartitionStore(settings.curated_root),
                repository=repository,
                quality_runner=QualityRunner(),
                routes=BAOSTOCK_ROUTES,
                bootstrap_years=settings.bootstrap_years,
                logger=pipeline_logger,
            )
            queue = TaskQueue(
                engine,
                task_log_root=settings.data_root / "state" / "task-logs",
            )
            research_repository = CanonicalResearchRepository.from_sqlite(
                engine,
                trusted_curated_root=settings.curated_root,
            )
            rulebook = AShareRuleBook.load(
                source_root / "configs" / "rules" / "a_share.yaml"
            )
            research = build_research_platform(
                engine=engine,
                queue=queue,
                repository=research_repository,
                source_root=source_root,
                artifact_root=settings.artifact_root,
                rulebook=rulebook,
            )
            worker = Worker(
                queue,
                worker_id=f"cli-worker-{os.getpid()}",
                handlers=(
                    DataUpdateHandler(pipeline),
                    DataValidationHandler(pipeline),
                    *research.handlers,
                ),
            )
            return ApplicationServices(
                pipeline,
                task_commands=LocalTaskCommands(queue),
                worker_commands=LocalWorkerCommands(
                    worker,
                    queue=queue,
                ),
                research_commands=LocalResearchCommands(
                    research.commands,
                    ResearchDashboardService(
                        research.registry,
                        research.components,
                        source_root / "configs" / "research" / "examples",
                        settings.artifact_root,
                    ),
                    source_root / "configs",
                ),
                close_callback=lambda: cls._close_resources(
                    pipeline_logger, pipeline_stream, engine
                ),
            )
        except BaseException:
            with suppress(BaseException):
                cls._close_resources(pipeline_logger, pipeline_stream, engine)
            raise

    @staticmethod
    def _pipeline_logger(data_root: Path) -> tuple[StructuredLogger, TextIO]:
        log_dir = data_root / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        file_stream = (log_dir / "data_pipeline.log").open(
            "a", encoding="utf-8", newline="\n"
        )
        try:
            logger = StructuredLogger(
                TeeLogStream(file_stream, sys.stderr),
                context=LogContext(stage="PIPELINE"),
                sensitive_values=sensitive_environment_values(os.environ),
            )
        except BaseException:
            with suppress(BaseException):
                file_stream.close()
            raise
        return logger, file_stream

    @staticmethod
    def _close_resources(
        logger: StructuredLogger | None,
        file_stream: TextIO | None,
        engine: Engine,
    ) -> None:
        if logger is not None:
            logger.flush()
        if file_stream is not None:
            with suppress(Exception):
                file_stream.close()
        engine.dispose()

    @staticmethod
    def _source_root() -> Path:
        return Path(__file__).resolve().parents[3]


app = create_app(CliBootstrap.build_services)


def main() -> int:
    """运行安装后的 ``quant`` CLI；该函数是项目脚本框架入口。

    入参：
        无。
    返回值：
        返回命令退出码。
    异常：
        Click 参数错误由 CLI 边界转换为结构化错误。
    """
    return run(app)


if __name__ == "__main__":
    raise SystemExit(main())
