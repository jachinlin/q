"""基于当前已验证 Canonical 数据提供时点安全的研究读取能力。"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Sequence
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Never, Protocol
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from sqlalchemy import Engine

from quant_research.data.canonical.adjustments import _PriceAdjustmentEngine
from quant_research.data.canonical.schemas import CANONICAL_SCHEMAS
from quant_research.data.storage.verified_files import open_verified_file
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.domain.identifiers import IndexId, InstrumentId
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
    """定义研究读取端口。入参：查询条件。返回值：Canonical 帧。异常：门禁失败时抛出。"""

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

    def stocks(self) -> pl.LazyFrame:
        """读取股票。入参：无。返回值：股票帧。异常：门禁失败时抛出。"""
        ...

    def funds(self) -> pl.LazyFrame:
        """读取基金。入参：无。返回值：基金帧。异常：门禁失败时抛出。"""
        ...

    def indexes(self) -> pl.LazyFrame:
        """读取指数。入参：无。返回值：指数帧。异常：门禁失败时抛出。"""
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

    def stock_bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取股票行情。入参：股票和范围。返回值：行情帧。异常：范围或门禁非法时抛出。"""
        ...

    def fund_bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取基金行情。入参：基金和范围。返回值：行情帧。异常：范围或门禁非法时抛出。"""
        ...

    def adjusted_stock_bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取股票复权行情。入参：股票和范围。返回值：行情帧。异常：因子缺失时抛出。"""
        ...

    def adjusted_fund_bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取基金复权行情。入参：基金和范围。返回值：行情帧。异常：因子缺失时抛出。"""
        ...

    def stock_log_returns(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        """读取股票收益。入参：股票、范围和回看。返回值：收益帧。异常：参数非法时抛出。"""
        ...

    def fund_log_returns(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        """读取基金收益。入参：基金、范围和回看。返回值：收益帧。异常：参数非法时抛出。"""
        ...

    def index_bars(
        self,
        indexes: Sequence[IndexId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取指数行情。入参：指数和范围。返回值：行情帧。异常：范围非法时抛出。"""
        ...

    def stock_daily_basics(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取股票指标。入参：股票和范围。返回值：指标帧。异常：范围非法时抛出。"""
        ...

    def stock_financial_indicators(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取财务指标。入参：观察日和股票。返回值：指标帧。异常：门禁失败时抛出。"""
        ...

    def stock_income_statements(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取利润表。入参：观察日和股票。返回值：最新可见修订。异常：门禁失败时抛出。"""
        ...

    def stock_balance_sheets(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取资产负债表。入参：观察日和股票。返回值：最新可见修订。异常：门禁失败时抛出。"""
        ...

    def stock_cash_flow_statements(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取现金流量表。入参：观察日和股票。返回值：最新可见修订。异常：门禁失败时抛出。"""
        ...

    def stock_dividends(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取股票分红。入参：观察日和股票。返回值：最新可见修订。异常：门禁失败时抛出。"""
        ...

    def fund_dividends(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取场内基金分红。入参：观察日和基金。返回值：最新可见修订。异常：门禁失败时抛出。"""
        ...

    def industry_catalog(self) -> pl.LazyFrame:
        """读取行业目录。入参：无。返回值：目录帧。异常：门禁失败时抛出。"""
        ...

    def industry_memberships_on_dates(
        self,
        instruments: Sequence[InstrumentId] | None = None,
        dates: Sequence[date] = (),
    ) -> pl.LazyFrame:
        """读取每个查询日、每只股票唯一的 PIT 行业成员状态。

        入参：
            instruments：可选的股票范围；为空时读取全市场。
            dates：需要重建行业状态的查询日期。
        返回值：
            返回按查询日和证券唯一、确定性排序的行业成员帧。重叠的可见关系按最新
            生效日、进入事件可用时间及记录可用时间依次裁决。
        异常：
            目录门禁、可信路径或底层读取失败时传播对应异常。
        """
        ...

    def stock_suspensions(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取停牌。入参：范围和股票。返回值：事件帧。异常：范围非法时抛出。"""
        ...

    def stock_risk_warnings(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取风险警示。入参：范围和股票。返回值：记录帧。异常：范围非法时抛出。"""
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
        self._stock_price_adjustments = _PriceAdjustmentEngine(
            self, self.stock_bars, self._stock_adjustment_factors
        )
        self._fund_price_adjustments = _PriceAdjustmentEngine(
            self, self.fund_bars, self._fund_adjustment_factors
        )

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

    def stocks(self) -> pl.LazyFrame:
        """读取股票。入参：无。返回值：股票帧。异常：门禁失败时抛出。"""
        return self._read(DatasetKind.STOCK_MASTER, pl.lit(True))

    def funds(self) -> pl.LazyFrame:
        """读取基金。入参：无。返回值：基金帧。异常：门禁失败时抛出。"""
        return self._read(DatasetKind.FUND_MASTER, pl.lit(True))

    def indexes(self) -> pl.LazyFrame:
        """读取指数。入参：无。返回值：指数帧。异常：门禁失败时抛出。"""
        return self._read(DatasetKind.INDEX_MASTER, pl.lit(True))

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
            pl.col("trade_date").is_between(start, end, closed="both"),
        )

    def stock_bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取股票行情。入参：股票和范围。返回值：行情帧。异常：范围或门禁非法时抛出。"""
        return self._instrument_bars(
            DatasetKind.STOCK_DAILY_BAR, instruments, start, end
        )

    def fund_bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取基金行情。入参：基金和范围。返回值：行情帧。异常：范围或门禁非法时抛出。"""
        return self._instrument_bars(
            DatasetKind.FUND_DAILY_BAR, instruments, start, end
        )

    def _instrument_bars(
        self,
        dataset: DatasetKind,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        if start > end:
            raise ValueError("start must not follow end")
        definition = CANONICAL_SCHEMAS[dataset]
        instrument_ids = [instrument.canonical() for instrument in instruments]
        scope = (
            pl.col("instrument_id").is_in(instrument_ids)
            if instrument_ids
            else pl.lit(False)
        )
        return (
            self._scan(dataset)
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
        )

    def _stock_adjustment_factors(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        return self._adjustment_factors(
            DatasetKind.STOCK_ADJUSTMENT_FACTOR, instruments, start, end
        )

    def _fund_adjustment_factors(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        return self._adjustment_factors(
            DatasetKind.FUND_ADJUSTMENT_FACTOR, instruments, start, end
        )

    def _adjustment_factors(
        self,
        dataset: DatasetKind,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        if start > end:
            raise ValueError("start must not follow end")
        return self._read(
            dataset,
            self._instrument_scope(instruments)
            & (pl.col("trade_date") <= end)
            & pl.col("pit_usable")
            & pl.col("available_at").is_not_null()
            & (pl.col("available_at") <= self._shanghai_day_end_utc(end)),
        )

    def adjusted_stock_bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取股票复权行情。入参：股票和范围。返回值：行情帧。异常：因子缺失时抛出。"""
        return self._stock_price_adjustments.adjusted_bars(instruments, start, end)

    def adjusted_fund_bars(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
    ) -> pl.LazyFrame:
        """读取基金复权行情。入参：基金和范围。返回值：行情帧。异常：因子缺失时抛出。"""
        return self._fund_price_adjustments.adjusted_bars(instruments, start, end)

    def stock_log_returns(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        """读取股票收益。入参：股票、范围和回看。返回值：收益帧。异常：参数非法时抛出。"""
        return self._stock_price_adjustments.log_returns(
            instruments,
            start,
            end,
            lookback_sessions=lookback_sessions,
        )

    def fund_log_returns(
        self,
        instruments: Sequence[InstrumentId],
        start: date,
        end: date,
        *,
        lookback_sessions: int,
    ) -> pl.LazyFrame:
        """读取基金收益。入参：基金、范围和回看。返回值：收益帧。异常：参数非法时抛出。"""
        return self._fund_price_adjustments.log_returns(
            instruments,
            start,
            end,
            lookback_sessions=lookback_sessions,
        )

    def index_bars(
        self,
        indexes: Sequence[IndexId],
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
        definition = CANONICAL_SCHEMAS[DatasetKind.INDEX_DAILY_BAR]
        index_ids = [index.canonical() for index in indexes]
        scope = pl.col("index_id").is_in(index_ids) if index_ids else pl.lit(False)
        return (
            self._scan(DatasetKind.INDEX_DAILY_BAR)
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
        )

    def stock_daily_basics(
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
        definition = CANONICAL_SCHEMAS[DatasetKind.STOCK_DAILY_BASIC]
        instrument_ids = [instrument.canonical() for instrument in instruments]
        scope = (
            pl.col("instrument_id").is_in(instrument_ids)
            if instrument_ids
            else pl.lit(False)
        )
        return (
            self._scan(DatasetKind.STOCK_DAILY_BASIC)
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
        )

    def stock_financial_indicators(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取财务指标。入参：观察日和股票。返回值：指标帧。异常：门禁失败时抛出。"""
        return self._latest_visible_revisions(
            DatasetKind.STOCK_FINANCIAL_INDICATOR,
            as_of,
            instruments,
            ("instrument_id", "report_period"),
        )

    def stock_income_statements(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取利润表。入参：观察日和股票。返回值：最新可见修订。异常：门禁失败时抛出。"""
        return self._latest_visible_revisions(
            DatasetKind.STOCK_INCOME_STATEMENT,
            as_of,
            instruments,
            ("instrument_id", "report_period", "report_type"),
        )

    def stock_balance_sheets(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取资产负债表。入参：观察日和股票。返回值：最新可见修订。异常：门禁失败时抛出。"""
        return self._latest_visible_revisions(
            DatasetKind.STOCK_BALANCE_SHEET,
            as_of,
            instruments,
            ("instrument_id", "report_period", "report_type"),
        )

    def stock_cash_flow_statements(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取现金流量表。入参：观察日和股票。返回值：最新可见修订。异常：门禁失败时抛出。"""
        return self._latest_visible_revisions(
            DatasetKind.STOCK_CASH_FLOW_STATEMENT,
            as_of,
            instruments,
            ("instrument_id", "report_period", "report_type"),
        )

    def stock_dividends(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取股票分红。入参：观察日和股票。返回值：最新可见修订。异常：门禁失败时抛出。"""
        return self._latest_visible_revisions(
            DatasetKind.STOCK_DIVIDEND,
            as_of,
            instruments,
            ("instrument_id", "report_period", "announcement_date"),
        )

    def fund_dividends(
        self,
        as_of: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取场内基金分红。入参：观察日和基金。返回值：最新可见修订。异常：门禁失败时抛出。"""
        return self._latest_visible_revisions(
            DatasetKind.FUND_DIVIDEND,
            as_of,
            instruments,
            ("instrument_id", "announcement_date", "base_date"),
        )

    def _latest_visible_revisions(
        self,
        dataset: DatasetKind,
        as_of: date,
        instruments: Sequence[InstrumentId] | None,
        business_key: tuple[str, ...],
    ) -> pl.LazyFrame:
        definition = CANONICAL_SCHEMAS[dataset]
        ranking = [*business_key, "available_at", "revision"]
        return (
            self._scan(dataset)
            .select(list(definition.columns))
            .filter(
                self._instrument_scope(instruments)
                & pl.col("pit_usable")
                & pl.col("available_at").is_not_null()
                & (pl.col("available_at") <= self._shanghai_day_end_utc(as_of))
            )
            .sort(ranking)
            .unique(
                subset=list(business_key),
                keep="last",
                maintain_order=True,
            )
            .cast(definition.columns)
            .sort(list(definition.sort_key))
        )

    def industry_catalog(self) -> pl.LazyFrame:
        """读取行业目录。入参：无。返回值：目录帧。异常：门禁失败时抛出。"""
        return self._read(DatasetKind.INDUSTRY_CATALOG, pl.lit(True))

    def industry_memberships_on_dates(
        self,
        instruments: Sequence[InstrumentId] | None = None,
        dates: Sequence[date] = (),
    ) -> pl.LazyFrame:
        """读取行业成员。入参：股票和日期。返回值：成员帧。异常：门禁失败时抛出。"""
        definition = CANONICAL_SCHEMAS[DatasetKind.INDUSTRY_MEMBERSHIP]
        output_schema = pl.Schema([("query_date", pl.Date), *definition.columns.items()])
        requested_dates = tuple(sorted(set(dates)))
        if not requested_dates:
            return pl.DataFrame(schema=output_schema).lazy()
        requested = pl.DataFrame(
            {
                "query_date": requested_dates,
                "cutoff": tuple(
                    self._shanghai_day_end_utc(value) for value in requested_dates
                ),
            },
            schema={
                "query_date": pl.Date,
                "cutoff": pl.Datetime("us", "UTC"),
            },
        )
        ranking = (
            "query_date",
            "instrument_id",
            "in_date",
            "in_available_at",
            "available_at",
            "ingested_at",
            "level1_code",
        )
        return (
            self._scan(DatasetKind.INDUSTRY_MEMBERSHIP)
            .select(list(definition.columns))
            .join(requested.lazy(), how="cross")
            .filter(
                self._instrument_scope(instruments)
                & pl.col("pit_usable")
                & pl.col("available_at").is_not_null()
                & (pl.col("in_date") <= pl.col("query_date"))
                & (pl.col("in_available_at") <= pl.col("cutoff"))
                & (
                    pl.col("out_date").is_null()
                    | (pl.col("out_date") > pl.col("query_date"))
                    | pl.col("out_available_at").is_null()
                    | (pl.col("out_available_at") > pl.col("cutoff"))
                )
            )
            .sort(
                ranking,
                descending=(False, False, False, False, False, False, True),
            )
            .unique(
                subset=["query_date", "instrument_id"],
                keep="last",
                maintain_order=True,
            )
            .select("query_date", *definition.columns.names())
            .cast(output_schema)
            .sort("query_date", "instrument_id", "level1_code")
        )

    def stock_suspensions(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取停牌。入参：范围和股票。返回值：事件帧。异常：范围非法时抛出。"""
        return self._stock_event_range(
            DatasetKind.STOCK_SUSPENSION, start, end, instruments
        )

    def stock_risk_warnings(
        self,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None = None,
    ) -> pl.LazyFrame:
        """读取风险警示。入参：范围和股票。返回值：记录帧。异常：范围非法时抛出。"""
        return self._stock_event_range(
            DatasetKind.STOCK_RISK_WARNING, start, end, instruments
        )

    def _stock_event_range(
        self,
        dataset: DatasetKind,
        start: date,
        end: date,
        instruments: Sequence[InstrumentId] | None,
    ) -> pl.LazyFrame:
        if start > end:
            raise ValueError("start must not follow end")
        return self._read(
            dataset,
            self._instrument_scope(instruments)
            & pl.col("trade_date").is_between(start, end, closed="both")
            & pl.col("pit_usable")
            & pl.col("available_at").is_not_null()
            & (pl.col("available_at") <= self._shanghai_day_end_utc(end)),
        )

    def _read(
        self,
        dataset: DatasetKind,
        predicate: pl.Expr,
    ) -> pl.LazyFrame:
        definition = CANONICAL_SCHEMAS[dataset]
        return (
            self._scan(dataset)
            .select(list(definition.columns))
            .filter(predicate)
            .cast(definition.columns)
            .sort(list(definition.sort_key))
        )

    def _scan(self, dataset: DatasetKind) -> pl.LazyFrame:
        """返回绑定当前已验证内容寻址路径的真正惰性 Parquet 扫描。"""
        _, leases = self._verify_current_dataset(dataset)
        return pl.scan_parquet([lease.path for lease in leases])

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

    @staticmethod
    def _instrument_scope(
        instruments: Sequence[InstrumentId] | None,
    ) -> pl.Expr:
        if instruments is None:
            return pl.lit(True)
        values = [item.canonical() for item in instruments]
        return pl.col("instrument_id").is_in(values) if values else pl.lit(False)

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
        for partition in record.partitions:
            path = partition.path.resolve()
            if path in paths:
                cls._raise_catalog_error(
                    dataset,
                    "canonical dataset contains duplicate partition path",
                )
            paths.add(path)

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
            relative = partition.path.absolute().relative_to(self.trusted_root)
            if not relative.parts or relative.parts[0] != "source=tushare":
                raise ValueError(
                    "canonical partition must use the source=tushare namespace"
                )
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
        self._leases: dict[
            tuple[Path, str, str, int], _CanonicalPartitionLease
        ] = {}

    def acquire(
        self,
        partition: CanonicalPartitionRecord,
        *,
        max_bytes: int,
    ) -> _CanonicalPartitionLease:
        key = (
            partition.path.absolute(),
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
