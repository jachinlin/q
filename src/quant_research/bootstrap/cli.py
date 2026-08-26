"""装配并启动本地 ``quant`` 命令行应用。"""

from __future__ import annotations

import os
import sys
from contextlib import suppress
from pathlib import Path
from typing import TextIO

from sqlalchemy import Engine

from quant_research.application.data import (
    DataBootstrapHandler,
    DataUpdateHandler,
    DataValidationHandler,
)
from quant_research.application.experiments import ExperimentService
from quant_research.application.factor_studies import FactorStudyService
from quant_research.backtest.rulebook import AShareRuleBook
from quant_research.bootstrap.worker import build_default_experiment_worker
from quant_research.cli.app import (
    ApplicationServices,
    LocalExperimentCommands,
    LocalFactorStudyCommands,
    LocalStrategyCommands,
    LocalTaskCommands,
    LocalWorkerCommands,
    create_app,
    run,
)
from quant_research.config import Settings
from quant_research.data.pipeline.curate import CuratedPartitionStore
from quant_research.data.pipeline.publish import DataPipeline
from quant_research.data.quality.runner import QualityRunner
from quant_research.data.storage.partitions import RawPartitionStore
from quant_research.factors.builtin import STOCK_FACTOR_REFERENCES
from quant_research.factors.catalog import FactorReferenceCatalog
from quant_research.infrastructure.persistence.database import (
    create_sqlite_engine,
    upgrade_database,
)
from quant_research.infrastructure.persistence.experiment_runs import (
    ExperimentRunRegistry,
)
from quant_research.infrastructure.persistence.factor_studies import (
    FactorStudyRegistry,
)
from quant_research.infrastructure.persistence.repositories import MetadataRepository
from quant_research.infrastructure.persistence.task_queue import TaskQueue
from quant_research.infrastructure.runtime_settings import DataRootEnvSettingsStore
from quant_research.infrastructure.tushare import (
    TUSHARE_ROUTES,
    TushareCalendarPolicy,
    TushareClient,
    TushareConfig,
    TushareMapper,
    TushareSdkGateway,
)
from quant_research.logging import (
    LogContext,
    StructuredLogger,
    TeeLogStream,
    sensitive_environment_values,
)
from quant_research.strategies.registry import StrategyRegistry


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
            runtime_settings = DataRootEnvSettingsStore(settings.data_root)
            source_config = TushareConfig(
                token="",
                benchmark_indexes=settings.tushare.benchmark_indexes,
                max_attempts=settings.tushare.max_attempts,
                retry_backoff_seconds=settings.tushare.retry_backoff_seconds,
                token_provider=lambda: runtime_settings.read_data_source_token().value,
            )
            source = TushareClient(TushareSdkGateway(), source_config)
            calendar_client = TushareClient(TushareSdkGateway(), source_config)
            pipeline_logger, pipeline_stream = cls._pipeline_logger(settings.data_root)
            pipeline = DataPipeline(
                source=source,
                mapper=TushareMapper(),
                calendar=TushareCalendarPolicy(calendar_client),
                raw_store=RawPartitionStore(settings.raw_root),
                curated_store=CuratedPartitionStore(settings.curated_root),
                repository=repository,
                quality_runner=QualityRunner(),
                routes=TUSHARE_ROUTES,
                logger=pipeline_logger,
            )
            queue = TaskQueue(
                engine,
                task_log_root=settings.data_root / "state" / "task-logs",
            )
            rulebook = AShareRuleBook.load(
                source_root / "configs" / "rules" / "a_share.yaml"
            )
            strategies = StrategyRegistry.builtins(
                commission_bps=rulebook.commission_bps,
                commission_minimum_fen=rulebook.commission_minimum_fen,
            )
            experiments = ExperimentService(
                ExperimentRunRegistry(engine), repository, strategies
            )
            factor_studies = FactorStudyService(
                FactorStudyRegistry(engine),
                repository,
                FactorReferenceCatalog(STOCK_FACTOR_REFERENCES),
            )
            worker = build_default_experiment_worker(
                worker_id=f"cli-worker-{os.getpid()}",
                engine=engine,
                extra_handlers=(
                    DataBootstrapHandler(pipeline),
                    DataUpdateHandler(pipeline),
                    DataValidationHandler(pipeline),
                ),
            )
            return ApplicationServices(
                pipeline,
                task_commands=LocalTaskCommands(queue, factor_studies),
                worker_commands=LocalWorkerCommands(
                    worker,
                    queue=queue,
                ),
                experiment_commands=LocalExperimentCommands(
                    experiments, source_root / "configs"
                ),
                factor_study_commands=LocalFactorStudyCommands(
                    factor_studies, source_root / "configs"
                ),
                strategy_commands=LocalStrategyCommands(strategies.strategy_ids()),
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
