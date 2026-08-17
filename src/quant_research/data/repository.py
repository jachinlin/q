"""基于当前已验证 Canonical 数据提供时点安全的研究读取能力。"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Never, Protocol, cast
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from sqlalchemy import Engine

from quant_research.data.adjustments import _PriceAdjustmentEngine
from quant_research.data.safe_files import open_verified_file
from quant_research.data.schemas import CANONICAL_SCHEMAS, CanonicalSchema
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.domain.identifiers import InstrumentId
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
    CanonicalPartitionRecord,
    DataCatalogState,
    MetadataRepository,
)

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_PARTITION_FILE_BYTES = 8 * 1024 * 1024 * 1024
_MAX_DATASET_FILE_BYTES = 64 * 1024 * 1024 * 1024
_MAX_ROW_GROUP_BYTES = 512 * 1024 * 1024


class ResearchDataRepository(Protocol):
    """从当前已通过 ``validate-all`` 的 Canonical 目录读取研究数据。

    入参：
        无。
    返回值：
        构造并返回 ``ResearchDataRepository`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def catalog(self) -> CanonicalCatalog:
        """返回研究读取所绑定的只读 Canonical 目录。

        入参：
            无。
        返回值：
            提供全局质量门禁、数据集身份和覆盖元数据的只读目录接口。
        异常：
            无。
        """
        ...

    def instruments(self) -> pl.LazyFrame:
        """读取全部当前有效的证券主数据。

        入参：
            无。
        返回值：
            返回证券集合（``pl.LazyFrame``）。
        异常：
            无。
        """
        ...

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        """读取指定闭区间内的交易日历。

        入参：
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
        返回值：
            返回交易日历（``pl.LazyFrame``）。
        异常：
            无。
        """
        ...

    def bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取指定证券和日期闭区间内的日行情。

        入参：
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
        返回值：
            返回行情（``pl.LazyFrame``）。
        异常：
            无。
        """
        ...

    def adjusted_bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取以结束日为信息截止日期的前复权日行情。

        入参：
            instruments：待查询的证券标识集合。
            start：查询日期闭区间的开始日期。
            end：查询日期闭区间的结束日期及 PIT 信息截止日期。
        返回值：
            返回包含复权因子和审计列的前复权行情。
        异常：
            日期、目录门禁或 Canonical 分区不满足契约时传播对应异常。
        """
        ...

    def log_returns(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        """返回按交易会话补齐的前复权对数收益。

        入参：
            instruments：待查询的证券标识集合。
            start：结果区间的开始日期。
            end：结果区间的结束日期及 PIT 信息截止日期。
            lookback_sessions：在 ``start`` 前额外读取的交易会话数量。
        返回值：
            返回停牌补零、真实缺失保空的会话级对数收益。
        异常：
            参数、目录门禁或 Canonical 分区不满足契约时传播对应异常。
        """
        ...

    def index_bars(
        self,
        indexes: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取指定指数和日期闭区间内的指数行情。

        入参：
            indexes: 待查询的规范指数标识；空序列返回空结果。
            start: 查询开始日期，包含该日。
            end: 查询结束日期，包含该日且不得早于 ``start``。

        返回值：
            绑定当前已验证 ``index_bar`` 分区的 ``pl.LazyFrame``。

        异常：
            ValueError: 日期范围非法或当前分区完整性校验失败。
        """
        ...

    def daily_basics(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取指定证券和日期闭区间内的每日基础指标。

        入参：
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
        返回值：
            返回``basics``（``pl.LazyFrame``）。
        异常：
            无。
        """
        ...

    def financials_as_of(
        self,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取截至指定日期可见的最新财务观测。

        入参：
            field_ids：参与本次处理的字段``ids``；调用方不得依赖未声明的顺序。
            as_of：PIT 查询和资格判断所依据的观察日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
        返回值：
            返回截至日``of``（``pl.LazyFrame``）。
        异常：
            无。
        """
        ...

    def financial_history(
        self,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取截至指定日期可见的全部财务修订历史。

        入参：
            field_ids：参与本次处理的字段``ids``；调用方不得依赖未声明的顺序。
            as_of：PIT 查询和资格判断所依据的观察日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
        返回值：
            返回``history``（``pl.LazyFrame``）。
        异常：
            无。
        """
        ...

    def industry_classifications_as_of(
        self,
        instruments: Sequence[InstrumentId] | None,
        as_of: date,
    ) -> pl.LazyFrame:
        """读取指定日期时点有效且当时已知的行业分类。

        入参：
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            as_of：PIT 查询和资格判断所依据的观察日。
        返回值：
            返回行业分类截至日``of``（``pl.LazyFrame``）。
        异常：
            无。
        """
        ...

    def industry_classifications_on_dates(
        self,
        instruments: Sequence[InstrumentId] | None,
        dates: Sequence[date],
    ) -> pl.LazyFrame:
        """一次读取并重建多个查询日的供应商 as-of 行业状态。

        入参：
            instruments：证券集合；``None`` 表示全市场。
            dates：消费者实际需要的查询日期集合。
        返回值：
            返回含 ``query_date``、命中事件和全部审计列的惰性数据帧。
        异常：
            日期、目录门禁或 Canonical 分区不满足契约时传播对应异常。
        """
        ...

    def security_status(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取指定交易日的证券状态。

        入参：
            as_of：PIT 查询和资格判断所依据的观察日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
        返回值：
            返回状态（``pl.LazyFrame``）。
        异常：
            无。
        """
        ...

    def security_status_range(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取日期闭区间内可用于时点研究的证券状态。

        入参：
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
        返回值：
            返回状态``range``（``pl.LazyFrame``）。
        异常：
            无。
        """
        ...


class CanonicalCatalog(Protocol):
    """解析当前已验证 Canonical 分区所需的目录查询接口。

    入参：
        无。
    返回值：
        构造并返回 ``CanonicalCatalog`` 实例。
    异常：
        由具体实现按接口契约定义。
    """

    def require_validated_catalog(self) -> DataCatalogState:
        """取得并确认当前目录已经通过全局质量校验。

        入参：
            无。
        返回值：
            返回``validated``数据目录（``DataCatalogState``）。
        异常：
            无。
        """
        ...

    def get_canonical_dataset(self, dataset: DatasetKind) -> CanonicalDatasetRecord:
        """取得指定数据集的当前 Canonical 记录。

        入参：
            dataset：目标数据集，类型为 ``DatasetKind``。
        返回值：
            返回读取Canonical数据集后的Canonical数据集（``CanonicalDatasetRecord``）。
        异常：
            无。
        """
        ...

    def list_canonical_datasets(self) -> tuple[CanonicalDatasetRecord, ...]:
        """列出当前目录登记的全部 Canonical 数据集。

        入参：
            无。
        返回值：
            按数据集标识稳定排序的当前 Canonical 数据集记录。
        异常：
            目录持久化状态损坏或数据库查询失败时传播对应异常。
        """
        ...


class CanonicalDatasetMissing(QuantError):
    """请求的 Canonical 数据集不存在于当前目录。

    入参：
        dataset：目标数据集，类型为 ``DatasetKind``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    """

    def __init__(self, dataset: DatasetKind) -> None:
        """创建 Canonical 数据集缺失错误。

        参数:
            dataset: 当前目录中缺失的数据集类型。

        返回:
            ``None``。
        """
        super().__init__(
            ErrorDetail(
                code="CANONICAL_DATASET_MISSING",
                severity=Severity.FATAL,
                message="current canonical catalog does not contain the dataset",
                context={"dataset": dataset.value},
                remediation="curate the dataset and run validate-all",
                retryable=False,
            )
        )


class CanonicalResearchRepository:
    """通过已验证的当前目录解析所有研究用 Parquet 输入。

    入参：
        catalog：数据目录。
        trusted_curated_root：所有派生路径必须位于其中的可信Curated 数据可信根目录。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``CanonicalDatasetMissing``、``QuantError``、``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    """

    def __init__(
        self, catalog: CanonicalCatalog, *, trusted_curated_root: Path
    ) -> None:
        """创建 Canonical 研究数据仓库。

        参数:
            catalog: 提供全局质量门禁和当前 Canonical 指针的目录接口。
            trusted_curated_root: 可信 Curated 根目录；查询只允许读取其下的分区。

        返回:
            ``None``。
        """
        self._catalog = catalog
        self._partition_verifier = _CanonicalPartitionVerifier(trusted_curated_root)
        self._partition_leases = _CanonicalPartitionLeasePool(self._partition_verifier)
        self._price_adjustments = _PriceAdjustmentEngine(self)

    @classmethod
    def from_sqlite(
        cls,
        engine: Engine,
        *,
        trusted_curated_root: Path,
    ) -> CanonicalResearchRepository:
        """从已初始化的 SQLite Engine 装配 Canonical 研究数据仓库。

        入参：
            engine：由组合根创建并负责释放的 SQLite SQLAlchemy Engine。
            trusted_curated_root：可信 Curated 根目录；查询只允许读取其下的分区。
        返回值：
            内部绑定 ``MetadataRepository`` 的 Canonical 研究数据仓库。
        异常：
            ``engine`` 不是 SQLAlchemy Engine 时抛出 ``TypeError``；不是 SQLite
            Engine 时抛出 ``ValueError``；路径或数据库状态不满足底层契约时传播
            对应异常。

        本方法不执行数据库迁移，也不拥有或关闭传入的 Engine。
        """
        if not isinstance(engine, Engine):
            raise TypeError("engine must be a SQLAlchemy Engine")
        if engine.dialect.name != "sqlite":
            raise ValueError("engine must use the SQLite dialect")
        return cls(
            MetadataRepository(engine),
            trusted_curated_root=trusted_curated_root,
        )

    def catalog(self) -> CanonicalCatalog:
        """返回本仓库绑定的只读 Canonical 目录接口。

        入参：
            无。
        返回值：
            用于目录门禁、身份和覆盖元数据查询的 ``CanonicalCatalog``。
        异常：
            无。
        """
        return self._catalog

    def instruments(self) -> pl.LazyFrame:
        """读取全部当前有效的证券主数据。

        入参：
            无。
        返回值：
            返回证券集合（``pl.LazyFrame``）。
        异常：
            无。
        """
        return self._read(DatasetKind.INSTRUMENT, "TRUE", [])

    def trade_calendar(self, start: date, end: date) -> pl.LazyFrame:
        """读取指定闭区间内的交易日历。

        入参：
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
        返回值：
            返回交易日历（``pl.LazyFrame``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if start > end:
            raise ValueError("start must not follow end")
        return self._read(
            DatasetKind.TRADE_CALENDAR,
            "trade_date >= ? AND trade_date <= ?",
            [start, end],
        )

    def bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取指定证券和日期闭区间内的日行情。

        入参：
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
        返回值：
            返回行情（``pl.LazyFrame``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if start > end:
            raise ValueError("start must not follow end")
        _, leases = self._verify_current_dataset(DatasetKind.DAILY_BAR)
        definition = CANONICAL_SCHEMAS[DatasetKind.DAILY_BAR]
        instrument_ids = [instrument.canonical() for instrument in instruments]
        scope = (
            pl.col("instrument_id").is_in(instrument_ids)
            if instrument_ids
            else pl.lit(False)
        )
        return (
            pl.scan_parquet([lease.path for lease in leases])
            .select(list(definition.columns))
            .filter(
                scope
                & pl.col("trade_date").is_between(start, end, closed="both")
                & pl.col("pit_usable")
                & pl.col("available_at").is_not_null()
                & (pl.col("available_at") <= self._shanghai_day_end_utc(end))
            )
            .cast(definition.columns)
            .sort(list(definition.sort_key))
            .map_batches(self._partition_leases.retain(leases))
        )

    def adjusted_bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取以结束日为信息截止日期的前复权日行情。

        入参：
            instruments：待查询的证券标识集合。
            start：查询日期闭区间的开始日期。
            end：查询日期闭区间的结束日期及 PIT 信息截止日期。
        返回值：
            返回包含复权因子和审计列的前复权行情。
        异常：
            日期、目录门禁或 Canonical 分区不满足契约时传播对应异常。
        """
        return self._price_adjustments.adjusted_bars(instruments, start, end)

    def log_returns(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        """返回按交易会话补齐的前复权对数收益。

        入参：
            instruments：待查询的证券标识集合。
            start：结果区间的开始日期。
            end：结果区间的结束日期及 PIT 信息截止日期。
            lookback_sessions：在 ``start`` 前额外读取的交易会话数量。
        返回值：
            返回停牌补零、真实缺失保空的会话级对数收益。
        异常：
            参数、目录门禁或 Canonical 分区不满足契约时传播对应异常。
        """
        return self._price_adjustments.log_returns(
            instruments,
            start,
            end,
            lookback_sessions=lookback_sessions,
        )

    def index_bars(
        self,
        indexes: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取指定指数和日期闭区间内的指数行情。

        入参：
            indexes: 待查询的规范指数标识；空序列返回空结果。
            start: 查询开始日期，包含该日。
            end: 查询结束日期，包含该日且不得早于 ``start``。

        返回值：
            经过目录、路径和内容身份校验并按 Canonical 主键排序的指数行情。

        异常：
            ValueError: 日期范围非法或当前分区完整性校验失败。
        """
        if start > end:
            raise ValueError("start must not follow end")
        _, leases = self._verify_current_dataset(DatasetKind.INDEX_BAR)
        definition = CANONICAL_SCHEMAS[DatasetKind.INDEX_BAR]
        index_ids = [index.canonical() for index in indexes]
        scope = pl.col("index_id").is_in(index_ids) if index_ids else pl.lit(False)
        return (
            pl.scan_parquet([lease.path for lease in leases])
            .select(list(definition.columns))
            .filter(
                scope
                & pl.col("trade_date").is_between(start, end, closed="both")
                & pl.col("pit_usable")
                & pl.col("available_at").is_not_null()
                & (pl.col("available_at") <= self._shanghai_day_end_utc(end))
            )
            .cast(definition.columns)
            .sort(list(definition.sort_key))
            .map_batches(self._partition_leases.retain(leases))
        )

    def daily_basics(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取指定证券和日期闭区间内的每日基础指标。

        入参：
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
        返回值：
            返回``basics``（``pl.LazyFrame``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if start > end:
            raise ValueError("start must not follow end")
        _, leases = self._verify_current_dataset(DatasetKind.DAILY_BASIC)
        definition = CANONICAL_SCHEMAS[DatasetKind.DAILY_BASIC]
        instrument_ids = [instrument.canonical() for instrument in instruments]
        scope = (
            pl.col("instrument_id").is_in(instrument_ids)
            if instrument_ids
            else pl.lit(False)
        )
        return (
            pl.scan_parquet([lease.path for lease in leases])
            .select(list(definition.columns))
            .filter(
                scope
                & pl.col("trade_date").is_between(start, end, closed="both")
                & pl.col("pit_usable")
                & pl.col("available_at").is_not_null()
                & (pl.col("available_at") <= self._shanghai_day_end_utc(end))
            )
            .cast(definition.columns)
            .sort(list(definition.sort_key))
            .map_batches(self._partition_leases.retain(leases))
        )

    def financials_as_of(
        self,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取截至指定日期上海时区日终已知的最新财务观测。

        入参：
            field_ids：参与本次处理的字段``ids``；调用方不得依赖未声明的顺序。
            as_of：PIT 查询和资格判断所依据的观察日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
        返回值：
            返回截至日``of``（``pl.LazyFrame``）。
        异常：
            无。
        """
        predicates, parameters = self._instrument_predicate(instruments)
        field_predicate, field_parameters = self._value_predicate("metric", field_ids)
        predicates.extend(field_predicate)
        predicates.extend(
            (
                "pit_usable = TRUE",
                "available_at IS NOT NULL",
                "available_at <= ?",
            )
        )
        parameters.extend(field_parameters)
        parameters.append(self._shanghai_day_end_utc(as_of))
        definition = CANONICAL_SCHEMAS[DatasetKind.FINANCIAL_OBSERVATION]
        columns = self._columns(definition)
        order = self._order(definition)
        query = (
            "SELECT "
            + columns
            + " FROM (SELECT "
            + columns
            + ", ROW_NUMBER() OVER (PARTITION BY instrument_id, report_period, metric "
            "ORDER BY available_at DESC, revision DESC) AS _pit_rank FROM data WHERE "
            + " AND ".join(predicates)
            + ") WHERE _pit_rank = 1 ORDER BY "
            + order
        )
        return self._read_query(
            DatasetKind.FINANCIAL_OBSERVATION,
            query,
            parameters,
        )

    def financial_history(
        self,
        field_ids: Sequence[str],
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取截至指定日期上海时区日终已知的全部财务修订历史。

        入参：
            field_ids：参与本次处理的字段``ids``；调用方不得依赖未声明的顺序。
            as_of：PIT 查询和资格判断所依据的观察日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
        返回值：
            返回``history``（``pl.LazyFrame``）。
        异常：
            无。
        """
        predicates, parameters = self._instrument_predicate(instruments)
        field_predicate, field_parameters = self._value_predicate("metric", field_ids)
        predicates.extend(field_predicate)
        predicates.extend(
            (
                "pit_usable = TRUE",
                "available_at IS NOT NULL",
                "available_at <= ?",
            )
        )
        parameters.extend(field_parameters)
        parameters.append(self._shanghai_day_end_utc(as_of))
        return self._read(
            DatasetKind.FINANCIAL_OBSERVATION,
            " AND ".join(predicates),
            parameters,
        )

    def industry_classifications_as_of(
        self,
        instruments: Sequence[InstrumentId] | None,
        as_of: date,
    ) -> pl.LazyFrame:
        """读取指定日期有效且在上海时区日终前已知的行业分类。

        入参：
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            as_of：PIT 查询和资格判断所依据的观察日。
        返回值：
            返回行业分类截至日``of``（``pl.LazyFrame``）。
        异常：
            无。
        """
        definition = CANONICAL_SCHEMAS[DatasetKind.INDUSTRY_CLASSIFICATION]
        return (
            self._industry_classifications_on_dates(instruments, (as_of,))
            .drop("query_date")
            .cast(definition.columns)
        )

    def industry_classifications_on_dates(
        self,
        instruments: Sequence[InstrumentId] | None,
        dates: Sequence[date],
    ) -> pl.LazyFrame:
        """一次读取并重建多个查询日的供应商 as-of 行业状态。

        入参：
            instruments：证券集合；``None`` 表示全市场。
            dates：消费者实际需要的查询日期集合。
        返回值：
            返回含 ``query_date``、命中事件和全部审计列的惰性数据帧。
        异常：
            日期、目录门禁或 Canonical 分区不满足契约时传播对应异常。
        """
        return self._industry_classifications_on_dates(instruments, dates)

    def _industry_classifications_on_dates(
        self,
        instruments: Sequence[InstrumentId] | None,
        dates: Sequence[date],
    ) -> pl.LazyFrame:
        definition = CANONICAL_SCHEMAS[DatasetKind.INDUSTRY_CLASSIFICATION]
        output_schema = pl.Schema(
            [("query_date", pl.Date), *definition.columns.items()]
        )
        requested_dates = tuple(sorted(set(dates)))
        if not requested_dates:
            return pl.DataFrame(schema=output_schema).lazy()
        predicates, instrument_parameters = self._instrument_predicate(instruments)
        predicates.extend(
            (
                "pit_usable = TRUE",
                "available_at IS NOT NULL",
                "as_of_date <= requested.query_date",
                "available_at <= requested.cutoff",
            )
        )
        values = ", ".join("(?, ?)" for _ in requested_dates)
        date_parameters: list[object] = []
        for query_date in requested_dates:
            date_parameters.extend((query_date, self._shanghai_day_end_utc(query_date)))
        columns = self._columns(definition)
        query = (
            "SELECT query_date, "
            + columns
            + " FROM (SELECT requested.query_date, "
            + columns
            + ", ROW_NUMBER() OVER (PARTITION BY requested.query_date, "
            "instrument_id, taxonomy ORDER BY as_of_date DESC, available_at DESC) "
            "AS _pit_rank FROM data CROSS JOIN requested WHERE "
            + " AND ".join(predicates)
            + ") WHERE _pit_rank = 1 ORDER BY query_date, instrument_id, taxonomy"
        )
        _, leases = self._verify_current_dataset(DatasetKind.INDUSTRY_CLASSIFICATION)
        source_query, source_parameters = self._parquet_sources(
            [lease.path for lease in leases]
        )
        connection = duckdb.connect(":memory:")
        try:
            result = connection.execute(
                "WITH data AS ("
                + source_query
                + "), requested(query_date, cutoff) AS (VALUES "
                + values
                + ") "
                + query,
                [
                    *source_parameters,
                    *date_parameters,
                    *instrument_parameters,
                ],
            ).to_arrow_table()
        finally:
            connection.close()
        frame = cast(pl.DataFrame, pl.from_arrow(result))
        return frame.cast(output_schema).lazy()

    def security_status(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取指定交易日的证券状态。

        入参：
            as_of：PIT 查询和资格判断所依据的观察日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
        返回值：
            返回状态（``pl.LazyFrame``）。
        异常：
            无。
        """
        predicates, parameters = self._instrument_predicate(instruments)
        predicates.extend(
            (
                "trade_date = ?",
                "pit_usable = TRUE",
                "available_at IS NOT NULL",
                "available_at <= ?",
            )
        )
        parameters.extend((as_of, self._shanghai_day_end_utc(as_of)))
        return self._read(
            DatasetKind.SECURITY_STATUS,
            " AND ".join(predicates),
            parameters,
        )

    def security_status_range(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取日期闭区间内可用于时点研究的证券状态。

        入参：
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
        返回值：
            返回状态``range``（``pl.LazyFrame``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        if start > end:
            raise ValueError("start must not follow end")
        predicates, parameters = self._instrument_predicate(instruments)
        predicates.extend(
            (
                "trade_date >= ?",
                "trade_date <= ?",
                "pit_usable = TRUE",
                "available_at IS NOT NULL",
                "available_at <= ?",
            )
        )
        parameters.extend((start, end, self._shanghai_day_end_utc(end)))
        return self._read(
            DatasetKind.SECURITY_STATUS,
            " AND ".join(predicates),
            parameters,
        )

    def _read(
        self,
        dataset: DatasetKind,
        predicate: str,
        parameters: Sequence[object],
    ) -> pl.LazyFrame:
        definition = CANONICAL_SCHEMAS[dataset]
        query = (
            "SELECT "
            + self._columns(definition)
            + " FROM data WHERE "
            + predicate
            + " ORDER BY "
            + self._order(definition)
        )
        return self._read_query(dataset, query, parameters)

    def _read_query(
        self,
        dataset: DatasetKind,
        query: str,
        parameters: Sequence[object],
    ) -> pl.LazyFrame:
        _, leases = self._verify_current_dataset(dataset)
        source_query, source_parameters = self._parquet_sources(
            [lease.path for lease in leases]
        )
        connection = duckdb.connect(":memory:")
        try:
            result = connection.execute(
                "WITH data AS (" + source_query + ") " + query,
                [*source_parameters, *parameters],
            ).to_arrow_table()
        finally:
            connection.close()
        frame = cast(pl.DataFrame, pl.from_arrow(result))
        return frame.cast(CANONICAL_SCHEMAS[dataset].columns).lazy()

    def _dataset_record(self, dataset: DatasetKind) -> CanonicalDatasetRecord:
        return self._current_dataset_record(self._catalog, dataset)

    def _verify_current_dataset(
        self, dataset: DatasetKind
    ) -> tuple[CanonicalDatasetRecord, tuple[_CanonicalPartitionLease, ...]]:
        """解析当前数据集，并在首次读取时校验其全部物理分区。"""
        record = self._dataset_record(dataset)
        leases: list[_CanonicalPartitionLease] = []
        verified_bytes = 0
        for partition in record.partitions:
            remaining = _MAX_DATASET_FILE_BYTES - verified_bytes
            if remaining < 0:
                raise ValueError("published dataset exceeds the configured size limit")
            lease = self._partition_leases.acquire(
                partition,
                max_bytes=min(_MAX_PARTITION_FILE_BYTES, remaining),
            )
            leases.append(lease)
            verified_bytes += lease.file_size
        return record, tuple(leases)

    @classmethod
    def _current_dataset_record(
        cls, catalog: CanonicalCatalog, dataset: DatasetKind
    ) -> CanonicalDatasetRecord:
        catalog.require_validated_catalog()
        try:
            record = catalog.get_canonical_dataset(dataset)
        except KeyError as error:
            raise CanonicalDatasetMissing(dataset) from error
        if record.dataset is not dataset:
            cls._raise_catalog_error(dataset, "canonical dataset identity is invalid")
        cls._validate_catalog_partition_identities(dataset, record)
        return record

    @classmethod
    def _instrument_predicate(
        cls,
        instruments: Sequence[InstrumentId] | None,
    ) -> tuple[list[str], list[object]]:
        if instruments is None:
            return [], []
        return cls._value_predicate(
            "instrument_id", [item.canonical() for item in instruments]
        )

    @classmethod
    def _value_predicate(
        cls, column: str, values: Sequence[str]
    ) -> tuple[list[str], list[object]]:
        cls._validate_column(column)
        if not values:
            return ["FALSE"], []
        return [column + " IN (" + ", ".join("?" for _ in values) + ")"], list(values)

    @staticmethod
    def _parquet_sources(paths: Sequence[Path]) -> tuple[str, list[object]]:
        if not paths:
            raise ValueError("canonical dataset must contain at least one partition")
        return (
            " UNION ALL ".join("SELECT * FROM read_parquet(?)" for _ in paths),
            [path.as_posix() for path in paths],
        )

    @classmethod
    def _columns(cls, definition: CanonicalSchema) -> str:
        return ", ".join(cls._quoted(column) for column in definition.columns)

    @classmethod
    def _order(cls, definition: CanonicalSchema) -> str:
        return ", ".join(cls._quoted(column) for column in definition.sort_key)

    @classmethod
    def _quoted(cls, column: str) -> str:
        cls._validate_column(column)
        return f'"{column}"'

    @staticmethod
    def _validate_column(column: str) -> None:
        if not any(
            column in definition.columns for definition in CANONICAL_SCHEMAS.values()
        ):
            raise ValueError("column is not in the canonical schema allowlist")

    @staticmethod
    def _shanghai_day_end_utc(value: date) -> datetime:
        return datetime.combine(value, time.max, tzinfo=_SHANGHAI).astimezone(UTC)

    @classmethod
    def _validate_catalog_partition_identities(
        cls,
        dataset: DatasetKind,
        record: CanonicalDatasetRecord,
    ) -> None:
        paths: set[Path] = set()
        content_hashes: set[str] = set()
        for partition in record.partitions:
            path = partition.path.resolve()
            if path in paths or partition.content_hash in content_hashes:
                cls._raise_catalog_error(
                    dataset,
                    "canonical dataset contains duplicate partition identity",
                )
            paths.add(path)
            content_hashes.add(partition.content_hash)

    @staticmethod
    def _raise_catalog_error(dataset: DatasetKind, message: str) -> Never:
        raise QuantError(
            ErrorDetail(
                code="CANONICAL_CATALOG_INVALID",
                severity=Severity.FATAL,
                message=message,
                context={"dataset": dataset.value},
                remediation="inspect the current canonical catalog and rerun validate-all",
                retryable=False,
            )
        )


class _CanonicalPartitionVerifier:
    """Bind trusted Parquet bytes to current logical catalog metadata."""

    def __init__(self, trusted_curated_root: Path) -> None:
        if not isinstance(trusted_curated_root, Path):
            raise TypeError("trusted_curated_root must be a Path")
        self.trusted_root = trusted_curated_root.absolute()

    def verify(
        self,
        partition: CanonicalPartitionRecord,
        *,
        max_bytes: int,
    ) -> int:
        message = "canonical partition fails catalog integrity checks"
        try:
            with open_verified_file(
                partition.path.absolute(),
                trusted_root=self.trusted_root,
                max_bytes=max_bytes,
            ) as opened:
                parquet = pq.ParquetFile(opened.file)
                metadata = parquet.metadata
                schema = parquet.schema_arrow
                if metadata.num_rows != partition.row_count:
                    raise ValueError(message)
                for index in range(metadata.num_row_groups):
                    if metadata.row_group(index).total_byte_size > _MAX_ROW_GROUP_BYTES:
                        raise ValueError(
                            "canonical partition row group exceeds the configured size limit"
                        )
                content_hash = self._parquet_content_hash(opened.file)
                file_size = opened.size
            schema_fingerprint = hashlib.sha256(
                schema.serialize().to_pybytes()
            ).hexdigest()
        except ValueError as error:
            if "size limit" in str(error) or "outside the trusted root" in str(error):
                raise
            raise ValueError(message) from error
        except Exception as error:
            raise ValueError(message) from error
        if (
            content_hash != partition.content_hash
            or schema_fingerprint != partition.schema_fingerprint
        ):
            raise ValueError(message)
        return file_size

    @staticmethod
    def _parquet_content_hash(source: object) -> str:
        """按 Curate 写入端的整表 Arrow IPC 规则重算内容哈希。"""
        table = pq.read_table(source)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


class _CanonicalPartitionLease:
    """A verified direct pointer into the current canonical store."""

    def __init__(
        self,
        partition: CanonicalPartitionRecord,
        verifier: _CanonicalPartitionVerifier,
        *,
        max_bytes: int,
    ) -> None:
        try:
            self.path = partition.path.absolute()
            self.file_size = verifier.verify(partition, max_bytes=max_bytes)
        except ValueError as error:
            if (
                "link or reparse point" in str(error)
                or "outside the trusted root" in str(error)
                or "size limit" in str(error)
            ):
                raise
            raise ValueError("canonical partition is unavailable") from error


class _CanonicalPartitionLeasePool:
    """Single-flight process cache of verified content-addressed pointers."""

    def __init__(self, verifier: _CanonicalPartitionVerifier) -> None:
        self._lock = threading.Lock()
        self._verifier = verifier
        self._leases: dict[tuple[str, str, int], _CanonicalPartitionLease] = {}

    def acquire(
        self,
        partition: CanonicalPartitionRecord,
        *,
        max_bytes: int,
    ) -> _CanonicalPartitionLease:
        key = (
            partition.content_hash,
            partition.schema_fingerprint,
            partition.row_count,
        )
        with self._lock:
            lease = self._leases.get(key)
            if lease is None:
                lease = _CanonicalPartitionLease(
                    partition,
                    self._verifier,
                    max_bytes=max_bytes,
                )
                self._leases[key] = lease
            elif lease.file_size > max_bytes:
                raise ValueError(
                    "canonical partition exceeds the configured size limit"
                )
            return lease

    @staticmethod
    def retain(
        leases: tuple[_CanonicalPartitionLease, ...],
    ) -> Callable[[pl.DataFrame], pl.DataFrame]:
        """Keep verified mirror pointers bound to every execution of the lazy plan."""

        def retain(frame: pl.DataFrame) -> pl.DataFrame:
            if not leases:
                raise ValueError("daily-bar dataset must contain a partition")
            return frame

        return retain
