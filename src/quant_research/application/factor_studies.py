"""执行 ``FACTOR_ANALYSIS`` 任务并发布不可变研究结果。"""

from __future__ import annotations

import hashlib
import shutil
import tempfile
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import ClassVar, cast

import polars as pl

from quant_research.data.contracts import (
    JsonValue,
    ProviderCapabilities,
    canonical_json_bytes,
)
from quant_research.data.repository import ResearchDataRepository
from quant_research.domain.enums import DatasetKind
from quant_research.domain.identifiers import InstrumentId
from quant_research.factor_studies.analysis import analyze, build_future_returns
from quant_research.factor_studies.contracts import FactorStudyStore
from quant_research.factor_studies.models import (
    DIRECTION_ADJUSTED,
    INDUSTRY_NEUTRALIZED,
    FactorRunStatus,
    FactorStudyConfig,
    FactorStudyIndustryConfig,
)
from quant_research.factors import (
    FactorContext,
    FactorEngine,
    FactorRegistry,
    PartitionedFactorEngine,
)
from quant_research.factors.builtin import register_stock_factors
from quant_research.factors.partitioned import PartitionEngineFactory
from quant_research.factors.transforms import neutralize_industry
from quant_research.tasks.handlers import CancellationToken, ProgressSink
from quant_research.tasks.models import (
    ClaimedTask,
    TaskOutcome,
    TaskProgress,
    TaskStatus,
)
from quant_research.universe.builder import UniverseBuilder
from quant_research.universe.rules import UniverseRules


class FactorAnalysisHandler:
    """在后台 Worker 中执行独立因子研究的完整六阶段流程。

    入参：
        studies：持久化因子研究定义及运行状态的仓储端口。
        repository：提供研究数据及其只读 Canonical 目录的仓储。
        capabilities：当前数据源确实支持的数据集和字段能力。
        artifact_root：不可变实验产物的可信根目录。
        environment：参与本次处理的运行环境；调用方不得依赖未声明的顺序。
        max_partition_size：限制资源使用、数量或等待时间的上限分区字节数。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    task_type = "FACTOR_ANALYSIS"

    def __init__(
        self,
        *,
        studies: FactorStudyStore,
        repository: ResearchDataRepository,
        capabilities: ProviderCapabilities,
        artifact_root: Path,
        environment: Mapping[str, JsonValue],
        max_partition_size: int,
    ) -> None:
        """装配因子任务所需研究仓库、能力信息和发布根目录。

        入参：
            studies：因子研究运行记录仓库。
            repository：提供研究数据及其只读 Canonical 目录的仓储。
            capabilities：当前数据供应商的研究能力集合。
            artifact_root：因子研究产物发布根目录。
            environment：写入产物清单的运行环境身份。
            max_partition_size：单次因子计算分区允许的最大证券数量。
        返回值：
            无。
        异常：
            参数不满足依赖契约时传播对应异常。
        """
        self._studies = studies
        self._repository = repository
        self._catalog = repository.catalog()
        self._capabilities = capabilities
        self._artifact_root = artifact_root
        self._environment = dict(environment)
        self._max_partition_size = max_partition_size

    def run(
        self,
        task: ClaimedTask,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> TaskOutcome:
        """校验任务身份、执行分析并持久化运行终态。

        入参：
            task：Worker 已认领并带所有权围栏的任务快照。
            progress：当前尝试已完成量、总量和阶段说明。
            cancellation：Worker 在阶段边界检查的协作取消端口。
        返回值：
            返回执行应用用例后的运行（``TaskOutcome``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if set(task.payload) != {"run_id", "config_hash"}:
            raise ValueError("FACTOR_ANALYSIS payload fields are invalid")
        run_id = cast(str, task.payload["run_id"])
        run = self._studies.get_run(run_id)
        if (
            run["task_id"] != task.id
            or run["config_hash"] != task.payload["config_hash"]
        ):
            raise ValueError("factor run identity does not match task")
        config = FactorStudyConfig.model_validate(run["config"])
        self._studies.transition(
            run_id, FactorRunStatus.QUEUED, FactorRunStatus.RUNNING
        )
        try:
            result = self._execute(run, config, progress, cancellation)
        except Exception as error:
            self._studies.transition(
                run_id,
                FactorRunStatus.RUNNING,
                FactorRunStatus.FAILED,
                error={
                    "code": "FACTOR_ANALYSIS_FAILED",
                    "error_type": type(error).__name__,
                },
            )
            raise
        if result is None:
            self._studies.transition(
                run_id, FactorRunStatus.RUNNING, FactorRunStatus.CANCELLED
            )
            return TaskOutcome(status=TaskStatus.CANCELLED)
        manifest_path, manifest_hash = result
        self._studies.transition(
            run_id,
            FactorRunStatus.RUNNING,
            FactorRunStatus.SUCCEEDED,
            manifest_path=manifest_path,
            manifest_hash=manifest_hash,
        )
        return TaskOutcome(status=TaskStatus.SUCCEEDED)

    def _execute(
        self,
        run: dict[str, object],
        config: FactorStudyConfig,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> tuple[Path, str] | None:
        stages = ("VALIDATE", "UNIVERSE", "FACTORS", "RETURNS", "ANALYZE", "PUBLISH")
        self._progress(progress, stages, 0)
        state = self._catalog.require_validated_catalog()
        if state.catalog_hash != run["catalog_hash"]:
            raise ValueError("current catalog differs from factor run identity")
        sessions = self._sessions(config.start_date, config.end_date)
        if not sessions:
            raise ValueError("factor study contains no trading sessions")
        if config.industry is not None:
            industry_record = self._catalog.get_canonical_dataset(
                DatasetKind.INDUSTRY_CLASSIFICATION
            )
            if (
                industry_record.start_date is None
                or industry_record.end_date is None
                or config.start_date < industry_record.start_date
                or config.end_date > industry_record.end_date
            ):
                raise ValueError(
                    "factor study date range is outside industry coverage"
                )
        all_calendar = self._repository.trade_calendar(date.min, date.max).collect()
        all_sessions = tuple(
            cast(date, value)
            for value in all_calendar.filter(pl.col("is_trading_day"))[
                "trade_date"
            ].to_list()
        )
        positions = {day: index for index, day in enumerate(all_sessions)}
        end_index = positions.get(sessions[-1])
        if end_index is None or end_index + max(config.horizons) >= len(all_sessions):
            raise ValueError(
                "canonical calendar lacks the complete future-return window"
            )
        future_end = all_sessions[end_index + max(config.horizons)]
        if cancellation.is_cancelled():
            return None

        self._progress(progress, stages, 1)
        universe_rows: list[pl.DataFrame] = []
        builder = UniverseBuilder(self._repository)
        rules = UniverseRules()
        for signal_day in sessions:
            frame = builder.build(signal_day, rules).select(
                pl.col("as_of").alias("signal_date"), "instrument_id", "eligible"
            )
            universe_rows.append(frame)
        eligible = pl.concat(universe_rows).sort("signal_date", "instrument_id")
        instrument_values = (
            eligible.filter(pl.col("eligible"))
            .select("instrument_id")
            .unique()
            .sort("instrument_id")["instrument_id"]
            .to_list()
        )
        if not instrument_values:
            raise ValueError("factor study universe contains no eligible instruments")
        instruments = tuple(
            sorted(
                (InstrumentId.parse(value) for value in instrument_values),
                key=InstrumentId.canonical,
            )
        )
        universe_hash = hashlib.sha256(
            canonical_json_bytes(
                {
                    "config_hash": cast(str, run["config_hash"]),
                    "eligible": cast(
                        JsonValue,
                        eligible.with_columns(
                            pl.col("signal_date").dt.to_string("%Y-%m-%d")
                        ).to_dicts(),
                    ),
                }
            )
        ).hexdigest()
        if cancellation.is_cancelled():
            return None

        self._progress(progress, stages, 2)

        def engine_factory(scope: tuple[InstrumentId, ...]) -> FactorEngine:
            registry = FactorRegistry()
            register_stock_factors(
                registry,
                self._repository,
                self._repository,
                scope,
                price_service=self._repository,
            )
            return FactorEngine(registry, capabilities=self._capabilities)

        factor_result = PartitionedFactorEngine(
            cast(PartitionEngineFactory, engine_factory),
            max_partition_size=self._max_partition_size,
        ).compute(
            config.factor_refs,
            instruments,
            FactorContext(
                run["catalog_hash"], universe_hash, config.start_date, config.end_date
            ),
        )
        directions = {
            node.factor_ref: node.spec.direction
            for node in factor_result.execution_descriptor.plan
        }
        factor_frames = []
        for partition in factor_result.partitions:
            for ref, artifact in partition.artifacts.items():
                factor_frames.append(
                    cast(pl.DataFrame, pl.from_arrow(artifact.table))
                    .rename({"trade_date": "signal_date"})
                    .with_columns((pl.col("value") * directions[ref]).alias("value"))
                    .select(
                        "signal_date", "instrument_id", "factor_id", "value", "is_valid"
                    )
                )
        factors = (
            pl.concat(factor_frames)
            .join(eligible, on=["signal_date", "instrument_id"], how="inner")
            .with_columns((pl.col("is_valid") & pl.col("eligible")).alias("is_valid"))
            .drop("eligible")
        )
        industry_coverage = _FactorIndustrySupport.empty_coverage()
        industry_input: dict[str, JsonValue] | None = None
        baseline = _FactorIndustrySupport.direction_adjusted(factors)
        if config.industry is not None:
            classifications = self._repository.industry_classifications_on_dates(
                instruments, sessions
            ).collect()
            neutralized, industry_coverage, industry_input = (
                _FactorIndustrySupport.build(
                    factors=factors,
                    eligible=eligible,
                    classifications=classifications,
                    config=config.industry,
                )
            )
            factors = pl.concat((baseline, neutralized), how="vertical")
        else:
            factors = baseline
        if cancellation.is_cancelled():
            return None

        self._progress(progress, stages, 3)
        adjusted = self._repository.adjusted_bars(
            instruments,
            config.start_date,
            future_end,
        ).collect()
        tradability = self._repository.security_status_range(
            config.start_date, future_end, instruments
        ).collect()
        returns = build_future_returns(
            adjusted,
            all_sessions,
            eligible,
            config.horizons,
            tradability,
        )
        if cancellation.is_cancelled():
            return None

        self._progress(progress, stages, 4)
        outputs = analyze(
            factors,
            eligible,
            returns,
            quantiles=config.quantiles,
            minimum=config.min_cross_section,
            ic_rolling_window=config.ic_rolling_window,
            ic_rolling_min_valid=config.ic_rolling_min_valid,
            ic_quantile_probabilities=config.ic_quantile_probabilities,
        )
        outputs["industry_coverage"] = industry_coverage
        if (
            self._catalog.require_validated_catalog().catalog_hash
            != run["catalog_hash"]
        ):
            raise ValueError("catalog changed during factor analysis")
        if cancellation.is_cancelled():
            return None

        self._progress(progress, stages, 5)
        published = publish_factor_run(
            artifact_root=self._artifact_root,
            study_id=cast(str, run["study_id"]),
            run_id=cast(str, run["id"]),
            config=config,
            catalog_hash=run["catalog_hash"],
            source_hash=cast(str, run["source_hash"]),
            execution_descriptor=factor_result.execution_descriptor.json_value(),
            environment=self._environment,
            outputs=outputs,
            industry_input=industry_input,
        )
        self._progress(progress, stages, len(stages))
        return published

    @staticmethod
    def _progress(
        progress: ProgressSink, stages: tuple[str, ...], completed: int
    ) -> None:
        stage = stages[min(completed, len(stages) - 1)] if stages else "COMPLETE"
        progress.update(
            TaskProgress(
                stage="COMPLETE" if completed == len(stages) else stage,
                completed=completed,
                total=len(stages),
                message="factor analysis completed"
                if completed == len(stages)
                else f"{stage.lower()} started",
            )
        )

    def _sessions(self, start: date, end: date) -> tuple[date, ...]:
        frame = self._repository.trade_calendar(start, end).collect()
        return tuple(
            cast(date, value)
            for value in frame.filter(pl.col("is_trading_day"))["trade_date"].to_list()
        )


class _FactorIndustrySupport:
    """集中实现因子研究的 PIT 行业对齐、覆盖披露和中性化。"""

    _UNCLASSIFIED_GROUP = "__UNCLASSIFIED__"
    _COVERAGE_SCHEMA: ClassVar[pl.Schema] = pl.Schema({
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
    })

    @classmethod
    def empty_coverage(cls) -> pl.DataFrame:
        """返回未启用行业依赖时使用的固定 Schema 空覆盖表。"""
        return pl.DataFrame(schema=cls._COVERAGE_SCHEMA)

    @staticmethod
    def direction_adjusted(factors: pl.DataFrame) -> pl.DataFrame:
        """为方向调整基线补齐统一审计列和信号版本键。"""
        return factors.with_columns(
            pl.lit(None, dtype=pl.String).alias("invalid_reason"),
            pl.lit(DIRECTION_ADJUSTED).alias("signal_variant"),
        )

    @classmethod
    def build(
        cls,
        *,
        factors: pl.DataFrame,
        eligible: pl.DataFrame,
        classifications: pl.DataFrame,
        config: FactorStudyIndustryConfig,
    ) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, JsonValue]]:
        """构造行业中性化信号、逐日覆盖和 Manifest 输入证据。"""
        required = {
            "query_date",
            "instrument_id",
            "taxonomy",
            "industry_code",
            "is_classified",
        }
        if not required.issubset(classifications.columns):
            raise ValueError("industry classifications are missing required columns")
        states = (
            classifications.filter(pl.col("taxonomy") == config.taxonomy)
            .select(
                pl.col("query_date").alias("signal_date"),
                "instrument_id",
                "industry_code",
                "is_classified",
            )
            .with_columns(pl.lit(True).alias("_state_present"))
            .sort("signal_date", "instrument_id")
        )
        keys = states.select("signal_date", "instrument_id").rows()
        if len(keys) != len(set(keys)):
            raise ValueError("industry classifications contain duplicate state keys")
        malformed = states.filter(
            pl.col("is_classified")
            & (
                pl.col("industry_code").is_null()
                | (pl.col("industry_code").str.len_chars() == 0)
            )
        )
        if not malformed.is_empty():
            raise ValueError("classified industry states must carry an industry code")

        aligned = (
            eligible.filter(pl.col("eligible"))
            .select("signal_date", "instrument_id")
            .join(states, on=["signal_date", "instrument_id"], how="left")
            .with_columns(
                pl.col("_state_present").fill_null(False),
                pl.col("is_classified").fill_null(False),
            )
        )
        classified = pl.col("_state_present") & pl.col("is_classified")
        tombstone = pl.col("_state_present") & ~pl.col("is_classified")
        missing = ~pl.col("_state_present")
        if config.unclassified_policy == "UNCLASSIFIED":
            group = (
                pl.when(classified)
                .then(pl.col("industry_code"))
                .otherwise(pl.lit(cls._UNCLASSIFIED_GROUP))
            )
        else:
            group = (
                pl.when(classified)
                .then(pl.col("industry_code"))
                .otherwise(pl.lit(None, dtype=pl.String))
            )
        aligned = aligned.with_columns(group.alias("industry_group"))
        coverage = (
            aligned.group_by("signal_date")
            .agg(
                pl.len().alias("eligible_count"),
                classified.cast(pl.Int64).sum().alias("classified_count"),
                tombstone.cast(pl.Int64).sum().alias("tombstone_count"),
                missing.cast(pl.Int64).sum().alias("missing_state_count"),
                pl.col("industry_group").is_not_null().sum().alias("usable_count"),
            )
            .with_columns(
                pl.col("eligible_count").cast(pl.Int64),
                pl.col("usable_count").cast(pl.Int64),
                pl.lit(config.taxonomy).alias("taxonomy"),
                pl.lit(config.unclassified_policy).alias("unclassified_policy"),
                (
                    pl.col("classified_count") / pl.col("eligible_count")
                ).alias("classified_coverage"),
                (pl.col("usable_count") / pl.col("eligible_count")).alias(
                    "usable_coverage"
                ),
            )
            .select(list(cls._COVERAGE_SCHEMA))
            .sort("signal_date")
        )
        factor_groups = aligned.select(
            "signal_date", "instrument_id", "industry_group"
        )
        neutralized = neutralize_industry(
            factors.join(
                factor_groups,
                on=["signal_date", "instrument_id"],
                how="left",
            ),
            "value",
            "industry_group",
            ("signal_date", "factor_id"),
        ).drop("industry_group")
        neutralized = neutralized.with_columns(
            pl.lit(INDUSTRY_NEUTRALIZED).alias("signal_variant")
        )
        totals = coverage.select(
            pl.col("eligible_count").sum(),
            pl.col("classified_count").sum(),
            pl.col("tombstone_count").sum(),
            pl.col("missing_state_count").sum(),
            pl.col("usable_count").sum(),
        ).row(0, named=True)
        eligible_total = int(cast(int, totals["eligible_count"]))
        classified_total = int(cast(int, totals["classified_count"]))
        usable_total = int(cast(int, totals["usable_count"]))
        input_evidence: dict[str, JsonValue] = {
            "dataset": DatasetKind.INDUSTRY_CLASSIFICATION.value,
            "taxonomy": config.taxonomy,
            "unclassified_policy": config.unclassified_policy,
            "date_basis": "SIGNAL_DATE",
            "neutralization": "EQUAL_WEIGHT_GROUP_DEMEAN",
            "availability_source": "BAOSTOCK_AS_OF_DATE_RECONSTRUCTED",
            "coverage": {
                "eligible_observations": eligible_total,
                "classified_observations": classified_total,
                "tombstone_observations": int(cast(int, totals["tombstone_count"])),
                "missing_state_observations": int(
                    cast(int, totals["missing_state_count"])
                ),
                "usable_observations": usable_total,
                "classified_rate": classified_total / eligible_total,
                "usable_rate": usable_total / eligible_total,
            },
        }
        return neutralized, coverage, input_evidence


def publish_factor_run(
    *,
    artifact_root: Path,
    study_id: str,
    run_id: str,
    config: FactorStudyConfig,
    catalog_hash: str,
    source_hash: str,
    execution_descriptor: dict[str, JsonValue],
    environment: Mapping[str, JsonValue],
    outputs: Mapping[str, pl.DataFrame],
    industry_input: Mapping[str, JsonValue] | None = None,
) -> tuple[Path, str]:
    """原子发布一个不可变因子运行；该函数作为稳定公开 API保留在模块级。

    入参：
        artifact_root：不可变实验产物的可信根目录。
        study_id：因子研究定义的 UUID 标识。
        run_id：一次因子研究运行的 UUID 标识。
        config：调用所用的配置对象，类型为 ``FactorStudyConfig``。
        catalog_hash：提交时捕获并在运行阶段防漂移校验的 Canonical 数据目录身份。
        source_hash：参与计算的实现源码身份。
        execution_descriptor：本次因子运行完整依赖 DAG 及实现身份的规范 JSON 描述。
        environment：参与本次处理的运行环境；调用方不得依赖未声明的顺序。
        outputs：参与本次处理的输出集合；调用方不得依赖未声明的顺序。
        industry_input：启用行业研究时写入 Manifest 的 PIT 输入与覆盖证据。
    返回值：
        返回校验并原子发布因子运行后的因子运行（``tuple[Path, str]``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    def entry(path: Path, rows: int | None, schema: str | None) -> dict[str, object]:
        return {
            "path": path.name,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "size_bytes": path.stat().st_size,
            "row_count": rows,
            "schema": schema,
        }

    parent = artifact_root / "factor-studies" / study_id
    final = parent / run_id
    parent.mkdir(parents=True, exist_ok=True)
    if final.exists():
        raise ValueError("factor run publication already exists")
    staging = Path(tempfile.mkdtemp(prefix=f".{run_id}-", dir=parent))
    try:
        config_path = staging / "study_config.json"
        config_path.write_bytes(canonical_json_bytes(config.model_dump(mode="json")))
        environment_path = staging / "environment.json"
        environment_path.write_bytes(
            canonical_json_bytes(cast(dict[str, JsonValue], dict(environment)))
        )
        entries: dict[str, dict[str, object]] = {}
        for name, frame in outputs.items():
            path = staging / f"{name}.parquet"
            frame.write_parquet(path, compression="zstd")
            entries[path.name] = entry(path, frame.height, str(frame.schema))
        entries[config_path.name] = entry(config_path, None, None)
        entries[environment_path.name] = entry(environment_path, None, None)
        manifest: dict[str, JsonValue] = {
            "run_id": run_id,
            "study_id": study_id,
            "catalog_hash": catalog_hash,
            "source_hash": source_hash,
            "execution_descriptor": execution_descriptor,
            "entries": cast(JsonValue, entries),
        }
        if industry_input is not None:
            manifest["industry_input"] = cast(JsonValue, dict(industry_input))
        payload = canonical_json_bytes(manifest)
        manifest_path = staging / "manifest.json"
        manifest_path.write_bytes(payload)
        manifest_hash = hashlib.sha256(payload).hexdigest()
        staging.replace(final)
        return final / "manifest.json", manifest_hash
    finally:
        if staging.exists():
            shutil.rmtree(staging)
