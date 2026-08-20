"""按数据集编排 LOCALIZE、CURATE 与 VALIDATE 阶段。"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Never, Protocol, cast
from uuid import uuid4
from zoneinfo import ZoneInfo

import polars as pl

from quant_research.data.canonical.mapper import CanonicalMapper
from quant_research.data.catalog import (
    DATASET_CATALOG,
    DatasetCatalog,
    FetchPlan,
    ReuseSemantics,
)
from quant_research.data.contracts import (
    CanonicalBatch,
    JsonValue,
    PublishedPartition,
    RawBatch,
    canonical_json_bytes,
)
from quant_research.data.pipeline.curate import (
    CanonicalPartitionReplacement,
    CuratedPartitionStore,
)
from quant_research.data.pipeline.localize import (
    LocalizePlanContext,
    LocalizePlanExecutor,
)
from quant_research.data.quality.models import (
    QualityIssue,
    QualityRuleResult,
    QualityRunSpec,
    thaw_json,
)
from quant_research.data.quality.runner import QualityRunner
from quant_research.data.sources.financials import FinancialDisclosureSchedule
from quant_research.data.sources.routing import RoutingTable
from quant_research.data.storage.partitions import RawPartitionStore
from quant_research.data.storage.paths import DataRootExecutionLock
from quant_research.domain.enums import DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.domain.identifiers import QualityRunId
from quant_research.infrastructure.persistence.repositories import (
    CanonicalDatasetRecord,
    CanonicalPartitionRecord,
    DataInitializationStateRecord,
    MetadataRepository,
    RawHeadIdentity,
    RawHeadSnapshot,
    RawPartitionRecord,
    RawPartitionSpec,
)
from quant_research.logging import StructuredLogger


class DatasetSource(Protocol):
    """约束 Dataset pipeline 所需的完整数据源能力。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``DatasetSource`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    @property
    def provider(self) -> str:
        """返回当前数据源的稳定供应商标识。

        入参：
            无。
        返回值：
            返回数据供应商（``str``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def login(self) -> None:
        """建立供应商会话；重复调用保持幂等。

        入参：
            无。
        返回值：
            无。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def close(self) -> None:
        """关闭供应商会话；未登录时不执行额外操作。

        入参：
            无。
        返回值：
            无。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def fetch_instruments(self) -> Iterable[RawBatch]:
        """获取完整的供应商原生证券目录。

        入参：
            无。
        返回值：
            返回从供应商获取证券集合后的证券集合（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def trade_calendar_request(self, start: date, end: date) -> Mapping[str, JsonValue]:
        """构造指定闭区间的规范化交易日历请求。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回交易日历请求（``Mapping[str, JsonValue]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def fetch_trade_calendar(self, start: date, end: date) -> Iterable[RawBatch]:
        """获取指定闭区间的供应商原生交易日历批次。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回从供应商获取交易交易日历后的交易交易日历（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def calendar_trading_days(
        self, partition: PublishedPartition, start: date, end: date
    ) -> tuple[date, ...]:
        """从交易日历分区提取指定闭区间内的开市日期。

        入参：
            partition：待读取、校验或映射的分区。
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回交易``days``（``tuple[date, ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def daily_bars_request(self, trade_date: date) -> Mapping[str, JsonValue]:
        """构造单个开市日的全市场日行情请求。

        入参：
            trade_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回行情请求（``Mapping[str, JsonValue]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def fetch_daily_bars(self, trade_date: date) -> Iterable[RawBatch]:
        """获取指定证券或交易日范围的日行情 Raw 批次。

        入参：
            trade_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回从供应商获取日频行情后的日频行情（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def etf_bar_requests(
        self, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """构造配置中 ETF 白名单的区间行情请求。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            每只 ETF 按供应商允许跨度切分后的确定性区间请求。
        异常：
            日期或数据源配置无效时传播 ``ValueError``。
        """
        ...

    def fetch_etf_bars(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取一个 ETF 闭区间的未复权 Raw 行情。

        入参：
            request：包含 ETF、开始和结束日期的规范请求。
        返回值：
            供应商 Raw 批次。
        异常：
            数据源未连接或供应商响应不合法时传播边界异常。
        """
        ...

    def benchmark_bars_request(self, trade_date: date) -> Mapping[str, JsonValue]:
        """构造单个开市日的基准指数行情请求。

        入参：
            trade_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回行情请求（``Mapping[str, JsonValue]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def fetch_benchmark_bars(self, trade_date: date) -> Iterable[RawBatch]:
        """获取单个开市日的基准指数 Raw 批次。

        入参：
            trade_date：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回从供应商获取基准行情后的基准行情（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def index_bar_requests(
        self, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """构造配置中各基准指数的区间请求。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回行情``requests``（``tuple[Mapping[str, JsonValue], ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def fetch_index_bars(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取一个指数闭区间的未复权 Raw 行情。

        入参：
            request：包含完整业务字段的规范化供应商请求。
        返回值：
            返回从供应商获取索引行情后的索引行情（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def financial_requests(
        self, start: date, end: date
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """为报告期末闭区间构造已越过披露截止日的财务请求单元。

        入参：
            start：最早报告期末日。
            end：最晚报告期末日。
        返回值：
            返回``requests``（``tuple[Mapping[str, JsonValue], ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def fetch_financials(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取一个报告单元的供应商原生财务批次。

        入参：
            request：包含完整业务字段的规范化供应商请求。
        返回值：
            返回从供应商获取``financials``后的``financials``（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def industry_requests(
        self, trading_days: Sequence[date]
    ) -> tuple[Mapping[str, JsonValue], ...]:
        """为已完整结束的交易日构造全市场行业分类请求。

        入参：
            trading_days：按升序提供的已完整结束交易日集合。
        返回值：
            返回``requests``（``tuple[Mapping[str, JsonValue], ...]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def fetch_industry(self, request: Mapping[str, JsonValue]) -> Iterable[RawBatch]:
        """获取指定时点的全市场行业分类批次。

        入参：
            request：包含完整业务字段的规范化供应商请求。
        返回值：
            返回从供应商获取行业分类后的行业分类（``Iterable[RawBatch]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...


class CalendarPolicy(Protocol):
    """约束交易日历窗口的计算策略。

    入参：
        按枚举值或实现类契约构造；无额外运行时输入。
    返回值：
        构造并返回 ``CalendarPolicy`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def bootstrap_window(self, years: int) -> tuple[date, date]:
        """计算首次构建所需的交易日历窗口。

        入参：
            years：调用接口所需的同名参数，具体约束见类型标注。
        返回值：
            返回窗口（``tuple[date, date]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def latest_complete_day(self) -> date:
        """返回当前时钟下最近一个已完整结束的交易日。

        入参：
            无。
        返回值：
            返回当前时钟下最近一个数据已经完整落定的交易日。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...

    def explicit_window(self, start: date, end: date) -> tuple[date, date]:
        """校验并返回用户明确指定的日期闭区间。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回窗口（``tuple[date, date]``）。
        异常：
            实现可传播参数校验、供应商访问、目录状态或文件完整性异常。
        """
        ...


@dataclass(frozen=True, slots=True)
class LocalizeResult:
    """汇总一次 LOCALIZE 操作的请求数与新写入数。

    入参：
        dataset：目标 Canonical 数据集标识。
        fetched：实际访问供应商并发布的新 Raw 请求数量。
        skipped：因已有可复用 Raw 当前头而未重新抓取的请求数量。
        raw_partitions：本次窗口解析并确认可用的 Raw 请求总数。
    返回值：
        构造并返回 ``LocalizeResult`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    dataset: DatasetKind
    fetched: int
    skipped: int
    raw_partitions: int


@dataclass(frozen=True, slots=True)
class DatasetCurateResult:
    """汇总单个数据集 CURATE 操作的发布与复用结果。

    入参：
        dataset：目标 Canonical 数据集标识。
        content_hash：发布后整个 Canonical 数据集当前状态的内容哈希。
        partitions：发布后数据集包含的当前物理分区数量。
        rows：发布后全部当前分区的记录数合计。
        rebuilt_partitions：因输入、转换身份或文件状态变化而重新生成的分区数。
        reused_partitions：输入身份未变且通过文件校验而直接复用的分区数。
        raw_inputs_read：为重建目标分区实际读取的唯一 Raw 对象数。
    返回值：
        构造并返回 ``DatasetCurateResult`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    dataset: DatasetKind
    content_hash: str
    partitions: int
    rows: int
    rebuilt_partitions: int
    reused_partitions: int
    raw_inputs_read: int


@dataclass(frozen=True, slots=True)
class PipelineResult:
    """汇总一次完整数据流水线运行的阶段结果。

    入参：
        run_id：本次 LOCALIZE、CURATE、VALIDATE 编排运行的唯一标识。
        quality_run_id：最终全目录质量运行的持久化标识。
        data_hash：质量门通过时对应的 Canonical 全目录身份。
    返回值：
        构造并返回 ``PipelineResult`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    run_id: str
    quality_run_id: QualityRunId
    data_hash: str


class DataUpdateWindowBasis(StrEnum):
    """标识单个数据集更新窗口的生成依据。

    入参：按枚举值构造。返回值：返回窗口依据枚举。异常：非法值抛出 ``ValueError``。
    """

    EXPLICIT = "EXPLICIT"
    BOOTSTRAP = "BOOTSTRAP"
    INCREMENTAL = "INCREMENTAL"
    SNAPSHOT_REFRESH = "SNAPSHOT_REFRESH"
    DISCLOSURE_TRIGGER = "DISCLOSURE_TRIGGER"


@dataclass(frozen=True, slots=True)
class DataUpdateWindow:
    """保存一个数据集在一次更新任务中的确定执行窗口。

    入参：数据集、依据、闭区间、重叠天数、可选当前水位和披露触发日。
    返回值：构造不可变窗口。异常：字段非法时由解析或计划校验抛出。
    """

    dataset: DatasetKind
    basis: DataUpdateWindowBasis
    start: date
    end: date
    overlap_days: int
    current_watermark: date | None = None
    trigger_date: date | None = None

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ValueError("DATA_UPDATE dataset window start must not follow end")
        if self.overlap_days < 0:
            raise ValueError("DATA_UPDATE overlap_days must be non-negative")
        if self.basis is DataUpdateWindowBasis.DISCLOSURE_TRIGGER:
            if (
                self.dataset is not DatasetKind.FINANCIAL_OBSERVATION
                or self.current_watermark is not None
                or self.overlap_days != 0
                or self.trigger_date is None
            ):
                raise ValueError("financial disclosure window fields are inconsistent")
        elif self.basis is DataUpdateWindowBasis.SNAPSHOT_REFRESH:
            if (
                self.dataset is not DatasetKind.INSTRUMENT
                or self.current_watermark is not None
                or self.overlap_days != 0
                or self.trigger_date is not None
                or self.start != self.end
            ):
                raise ValueError("instrument snapshot window fields are inconsistent")
        elif self.trigger_date is not None:
            raise ValueError("only disclosure windows may declare trigger_date")

    def to_payload(self) -> dict[str, JsonValue]:
        """返回可持久化参数；不适用的当前水位字段直接省略。

        入参：无。返回值：JSON 安全的窗口参数。异常：无主动抛出的异常。
        """
        payload: dict[str, JsonValue] = {
            "dataset": self.dataset.value,
            "basis": self.basis.value,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "overlap_days": self.overlap_days,
        }
        if self.current_watermark is not None:
            payload["current_watermark"] = self.current_watermark.isoformat()
        if self.trigger_date is not None:
            payload["trigger_date"] = self.trigger_date.isoformat()
        return payload

    @classmethod
    def from_payload(cls, payload: Mapping[str, JsonValue]) -> DataUpdateWindow:
        """从任务参数恢复窗口。

        入参：任务队列保存的窗口参数。返回值：不可变更新窗口。
        异常：字段、类型、日期或范围非法时抛出 ``TypeError`` 或 ``ValueError``。
        """
        expected = {"dataset", "basis", "start", "end", "overlap_days"}
        optional = {"current_watermark", "trigger_date"}
        if not expected.issubset(payload) or not set(payload).issubset(
            expected | optional
        ):
            raise ValueError("DATA_UPDATE dataset window fields are invalid")
        dataset_value = payload["dataset"]
        basis_value = payload["basis"]
        start_value = payload["start"]
        end_value = payload["end"]
        overlap_days = payload["overlap_days"]
        watermark_value = payload.get("current_watermark")
        trigger_value = payload.get("trigger_date")
        if not all(
            isinstance(value, str)
            for value in (dataset_value, basis_value, start_value, end_value)
        ):
            raise TypeError("DATA_UPDATE window identifiers and dates must be strings")
        if type(overlap_days) is not int or overlap_days < 0:
            raise TypeError("DATA_UPDATE overlap_days must be a non-negative integer")
        if watermark_value is not None and not isinstance(watermark_value, str):
            raise TypeError("DATA_UPDATE current_watermark must be an ISO date string")
        if trigger_value is not None and not isinstance(trigger_value, str):
            raise TypeError("DATA_UPDATE trigger_date must be an ISO date string")
        window = cls(
            dataset=DatasetKind(cast(str, dataset_value)),
            basis=DataUpdateWindowBasis(cast(str, basis_value)),
            start=date.fromisoformat(cast(str, start_value)),
            end=date.fromisoformat(cast(str, end_value)),
            overlap_days=overlap_days,
            current_watermark=(
                None if watermark_value is None else date.fromisoformat(watermark_value)
            ),
            trigger_date=(
                None if trigger_value is None else date.fromisoformat(trigger_value)
            ),
        )
        return window


@dataclass(frozen=True, slots=True)
class DataUpdateSkip:
    """记录自动计划中经业务规则判定为无需执行的数据集。

    入参：
        dataset：未生成执行窗口的数据集。
        reason：稳定的跳过原因代码。
        trigger_date：下一次披露判断所依据的截止日。
    返回值：构造不可变的计划跳过证据。
    异常：仅财务数据集可使用披露截止日跳过原因，否则抛出 ``ValueError``。
    """

    dataset: DatasetKind
    reason: str
    trigger_date: date

    def __post_init__(self) -> None:
        if (
            self.dataset is not DatasetKind.FINANCIAL_OBSERVATION
            or self.reason != "DISCLOSURE_DEADLINE_PENDING"
        ):
            raise ValueError("unsupported DATA_UPDATE skip decision")

    def to_payload(self) -> dict[str, JsonValue]:
        """返回 Dashboard 与任务审计共用的 JSON 跳过证据。

        入参：无。
        返回值：包含数据集、原因代码和披露截止日的 JSON 对象。
        异常：实例已在构造时完成校验，不主动抛出异常。
        """
        return {
            "dataset": self.dataset.value,
            "reason": self.reason,
            "trigger_date": self.trigger_date.isoformat(),
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, JsonValue]) -> DataUpdateSkip:
        """从计划 payload 恢复并校验跳过证据。

        入参：仅包含数据集、原因代码和 ISO 截止日的映射。
        返回值：不可变跳过证据。
        异常：字段集合或字段类型非法时抛出 ``TypeError`` 或 ``ValueError``。
        """
        if set(payload) != {"dataset", "reason", "trigger_date"}:
            raise ValueError("DATA_UPDATE skipped dataset fields are invalid")
        if not all(isinstance(payload[key], str) for key in payload):
            raise TypeError("DATA_UPDATE skipped dataset fields must be strings")
        return cls(
            dataset=DatasetKind(cast(str, payload["dataset"])),
            reason=cast(str, payload["reason"]),
            trigger_date=date.fromisoformat(cast(str, payload["trigger_date"])),
        )


@dataclass(frozen=True, slots=True)
class DataUpdatePlan:
    """表示预览、入库和 Worker 共同使用的不可变数据更新计划。

    入参：模式、生成时间、汇总范围、稳定排序的数据集窗口和可选用户区间。
    返回值：构造不可变计划。异常：模式、排序、范围或窗口不一致时抛出
    ``ValueError``。
    """

    window_mode: str
    planned_at: datetime
    start: date
    end: date
    dataset_windows: tuple[DataUpdateWindow, ...]
    requested_start: date | None = None
    requested_end: date | None = None
    skipped_datasets: tuple[DataUpdateSkip, ...] = ()

    def __post_init__(self) -> None:
        if self.planned_at.tzinfo is None or self.planned_at.utcoffset() is None:
            raise ValueError("DATA_UPDATE planned_at must include a timezone")
        if self.window_mode not in {"AUTO_INCREMENTAL", "EXPLICIT", "BOOTSTRAP"}:
            raise ValueError("DATA_UPDATE window_mode is invalid")
        if not self.dataset_windows and not self.skipped_datasets:
            raise ValueError("DATA_UPDATE plan must contain decisions")
        if (self.requested_start is None) != (self.requested_end is None):
            raise ValueError("DATA_UPDATE requested dates must be supplied together")
        if self.window_mode == "EXPLICIT" and self.requested_start is None:
            raise ValueError("explicit DATA_UPDATE plan requires requested dates")
        if self.window_mode == "AUTO_INCREMENTAL" and self.requested_start is not None:
            raise ValueError("automatic DATA_UPDATE plan must omit requested dates")
        if self.window_mode == "BOOTSTRAP" and self.requested_start is None:
            raise ValueError("bootstrap plan requires its frozen base dates")
        ordered = tuple(
            sorted(self.dataset_windows, key=lambda item: item.dataset.value)
        )
        if ordered != self.dataset_windows:
            raise ValueError("DATA_UPDATE dataset windows must use deterministic order")
        if len({item.dataset for item in ordered}) != len(ordered):
            raise ValueError("DATA_UPDATE dataset windows must be unique")
        skipped = tuple(
            sorted(self.skipped_datasets, key=lambda item: item.dataset.value)
        )
        if skipped != self.skipped_datasets:
            raise ValueError("DATA_UPDATE skipped datasets must use deterministic order")
        if len({item.dataset for item in skipped}) != len(skipped):
            raise ValueError("DATA_UPDATE skipped datasets must be unique")
        if {item.dataset for item in ordered} & {item.dataset for item in skipped}:
            raise ValueError("DATA_UPDATE dataset decisions must not overlap")
        if ordered:
            if self.start != min(item.start for item in ordered):
                raise ValueError("DATA_UPDATE summary start does not match dataset windows")
            if self.end != max(item.end for item in ordered):
                raise ValueError("DATA_UPDATE summary end does not match dataset windows")
        elif self.start != self.end:
            raise ValueError("no-op DATA_UPDATE plan must use a single summary date")

    @property
    def plan_hash(self) -> str:
        """返回排除生成时间后的 SHA-256 确定性计划身份。

        入参：无。返回值：计划内容的 SHA-256。异常：无主动抛出的异常。
        """
        return hashlib.sha256(canonical_json_bytes(self.identity_payload())).hexdigest()

    def identity_payload(self) -> dict[str, JsonValue]:
        """返回用于幂等和预览一致性校验的规范业务参数。

        入参：无。返回值：不含生成时间的 JSON 安全参数。异常：无主动抛出的异常。
        """
        payload: dict[str, JsonValue] = {
            "window_mode": self.window_mode,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "dataset_windows": [item.to_payload() for item in self.dataset_windows],
            "skipped_datasets": [
                item.to_payload() for item in self.skipped_datasets
            ],
        }
        if self.requested_start is not None and self.requested_end is not None:
            payload["requested_start"] = self.requested_start.isoformat()
            payload["requested_end"] = self.requested_end.isoformat()
        return payload

    def to_payload(self) -> dict[str, JsonValue]:
        """返回任务队列持久化的完整只读参数和内容身份。

        入参：无。返回值：包含生成时间和计划哈希的 JSON 参数。异常：无主动抛出的异常。
        """
        payload = self.identity_payload()
        payload["planned_at"] = self.planned_at.astimezone(UTC).isoformat()
        payload["plan_hash"] = self.plan_hash
        return payload

    @classmethod
    def from_payload(cls, payload: Mapping[str, JsonValue]) -> DataUpdatePlan:
        """从任务 payload 恢复计划。

        入参：任务队列保存的完整计划参数。返回值：不可变数据更新计划。
        异常：结构、类型、日期或 hash 不一致时抛出 ``TypeError`` 或 ``ValueError``。
        """
        required = {
            "window_mode",
            "planned_at",
            "start",
            "end",
            "dataset_windows",
            "skipped_datasets",
            "plan_hash",
        }
        optional = {"requested_start", "requested_end"}
        if not required.issubset(payload) or not set(payload).issubset(
            required | optional
        ):
            raise ValueError("DATA_UPDATE task uses an unsupported legacy payload")
        strings = ("window_mode", "planned_at", "start", "end", "plan_hash")
        if not all(isinstance(payload[field], str) for field in strings):
            raise TypeError("DATA_UPDATE plan metadata must be strings")
        raw_windows = payload["dataset_windows"]
        if not isinstance(raw_windows, list):
            raise TypeError("DATA_UPDATE dataset_windows must be a list")
        windows: list[DataUpdateWindow] = []
        for item in raw_windows:
            if not isinstance(item, dict) or not all(
                isinstance(key, str) for key in item
            ):
                raise TypeError("DATA_UPDATE dataset window must be an object")
            windows.append(DataUpdateWindow.from_payload(item))
        raw_skipped = payload["skipped_datasets"]
        if not isinstance(raw_skipped, list):
            raise TypeError("DATA_UPDATE skipped_datasets must be a list")
        skipped: list[DataUpdateSkip] = []
        for item in raw_skipped:
            if not isinstance(item, dict) or not all(
                isinstance(key, str) for key in item
            ):
                raise TypeError("DATA_UPDATE skipped dataset must be an object")
            skipped.append(DataUpdateSkip.from_payload(item))
        requested_start_value = payload.get("requested_start")
        requested_end_value = payload.get("requested_end")
        if requested_start_value is not None and not isinstance(
            requested_start_value, str
        ):
            raise TypeError("DATA_UPDATE requested_start must be an ISO date string")
        if requested_end_value is not None and not isinstance(requested_end_value, str):
            raise TypeError("DATA_UPDATE requested_end must be an ISO date string")
        plan = cls(
            window_mode=cast(str, payload["window_mode"]),
            planned_at=datetime.fromisoformat(cast(str, payload["planned_at"])),
            start=date.fromisoformat(cast(str, payload["start"])),
            end=date.fromisoformat(cast(str, payload["end"])),
            dataset_windows=tuple(windows),
            requested_start=(
                None
                if requested_start_value is None
                else date.fromisoformat(requested_start_value)
            ),
            requested_end=(
                None
                if requested_end_value is None
                else date.fromisoformat(requested_end_value)
            ),
            skipped_datasets=tuple(skipped),
        )
        if payload["plan_hash"] != plan.plan_hash:
            raise ValueError("DATA_UPDATE plan hash does not match its content")
        return plan


class DataUpdatePlanningRepository(Protocol):
    """约束更新计划读取当前 Canonical 水位所需的最小仓储能力。

    入参和返回值由协议方法声明；实现异常按仓储契约传播。
    """

    def find_canonical_dataset(
        self, dataset: DatasetKind
    ) -> CanonicalDatasetRecord | None:
        """读取指定数据集的当前 Canonical 水位。

        入参：目标数据集。返回值：当前记录；尚未发布时返回空值。
        异常：仓储读取异常按消费者侧协议传播。
        """
        ...

    def find_data_initialization(self) -> DataInitializationStateRecord | None:
        """读取首次初始化状态。

        入参：无。返回值：冻结状态；尚未启动时返回空值。
        异常：仓储读取异常保持原语义。
        """
        ...


class DataUpdatePlanner:
    """根据供应商交易日历和 Canonical 水位生成确定性更新计划。

    入参：日历策略、水位仓储、路由、目录和可注入时钟。
    返回值：构造计划器。异常：目录或窗口非法时抛出 ``ValueError``。
    """

    def __init__(
        self,
        *,
        calendar: CalendarPolicy,
        repository: DataUpdatePlanningRepository,
        routes: RoutingTable,
        catalog: DatasetCatalog = DATASET_CATALOG,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        """保存更新计划所需的只读依赖。"""
        self._calendar = calendar
        self._repository = repository
        self._routes = routes
        self._catalog = catalog
        self._clock = clock

    def plan_bootstrap(
        self,
        years: int,
        *,
        frozen_window: tuple[date, date] | None = None,
        frozen_planned_at: datetime | None = None,
    ) -> DataUpdatePlan:
        """按调用方明确给出的年数冻结首次初始化窗口。

        入参：向前覆盖的正整数年数。返回值：全部可执行数据集的不可变窗口。
        异常：年数、供应商日历或目录非法时传播对应异常。
        """
        if type(years) is not int or years <= 0:
            raise ValueError("bootstrap years must be a positive integer")
        planned_at = self._clock() if frozen_planned_at is None else frozen_planned_at
        planning_date = planned_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
        start, end = (
            self._calendar.bootstrap_window(years)
            if frozen_window is None
            else frozen_window
        )
        windows: list[DataUpdateWindow] = []
        for dataset in sorted(
            (item for item in self._catalog if self._routes[item]),
            key=lambda item: item.value,
        ):
            if dataset is DatasetKind.INSTRUMENT:
                actual = (planning_date, planning_date)
                basis = DataUpdateWindowBasis.SNAPSHOT_REFRESH
            else:
                actual = _DatasetPipelineSupport._calendar_horizon(
                    dataset, (start, end)
                )
                basis = DataUpdateWindowBasis.BOOTSTRAP
            windows.append(
                DataUpdateWindow(
                    dataset=dataset,
                    basis=basis,
                    start=actual[0],
                    end=actual[1],
                    overlap_days=0,
                )
            )
        ordered = tuple(windows)
        return DataUpdatePlan(
            window_mode="BOOTSTRAP",
            planned_at=planned_at,
            start=min(item.start for item in ordered),
            end=max(item.end for item in ordered),
            dataset_windows=ordered,
            requested_start=start,
            requested_end=end,
        )

    def plan(
        self,
        *,
        start: date | None,
        end: date | None,
        datasets: Sequence[DatasetKind] | None = None,
    ) -> DataUpdatePlan:
        """解析可直接入库执行的计划。

        入参：日期同时为空表示自动增量，同时非空表示用户指定闭区间；数据集
        为空表示全部可执行数据集，否则必须是非空、无重复的可执行子集。
        返回值：包含所选数据集窗口的 ``DataUpdatePlan``。
        异常：日期、数据集、供应商日历或水位非法时传播对应异常。
        """
        planned_at = self._clock()
        planning_date = planned_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
        if (start is None) != (end is None):
            raise ValueError("start and end must be supplied together")
        executable = tuple(
            sorted(
                (item for item in self._catalog if self._routes[item]),
                key=lambda item: item.value,
            )
        )
        initialization = self._repository.find_data_initialization()
        if initialization is not None and initialization.status != "COMPLETED":
            raise QuantError(
                ErrorDetail(
                    code="DATA_UPDATE_REQUIRES_BOOTSTRAP",
                    severity=Severity.SEVERE,
                    message="data initialization has not completed",
                    context={"frozen_years": initialization.years},
                    remediation=(
                        "retry quant data bootstrap --years "
                        f"{initialization.years}"
                    ),
                    retryable=False,
                )
            )
        if datasets is None:
            selected = executable
        else:
            requested = tuple(datasets)
            if not requested:
                raise ValueError("DATA_UPDATE datasets must not be empty")
            if any(not isinstance(item, DatasetKind) for item in requested):
                raise TypeError("DATA_UPDATE datasets must contain DatasetKind values")
            if len(set(requested)) != len(requested):
                raise ValueError("DATA_UPDATE datasets must be unique")
            unsupported = set(requested) - set(executable)
            if unsupported:
                names = sorted(item.value for item in unsupported)
                raise ValueError(f"DATA_UPDATE datasets are not executable: {names}")
            selected = tuple(sorted(requested, key=lambda item: item.value))
        all_records = {
            dataset: self._repository.find_canonical_dataset(dataset)
            for dataset in executable
        }
        missing_baseline = tuple(
            dataset.value
            for dataset, record in all_records.items()
            if record is None or record.end_date is None
        )
        if missing_baseline:
            raise QuantError(
                ErrorDetail(
                    code="DATA_UPDATE_REQUIRES_BOOTSTRAP",
                    severity=Severity.SEVERE,
                    message="data update requires a complete canonical baseline",
                    context={"missing_datasets": list(missing_baseline)},
                    remediation="run quant data bootstrap --years <years>",
                    retryable=False,
                )
            )
        records = {dataset: all_records[dataset] for dataset in selected}
        windows: list[DataUpdateWindow] = []
        skipped: list[DataUpdateSkip] = []
        if start is not None and end is not None:
            resolved = self._calendar.explicit_window(start, end)
            for dataset in selected:
                record = records[dataset]
                if dataset is DatasetKind.INSTRUMENT:
                    actual = (planning_date, planning_date)
                    basis = DataUpdateWindowBasis.SNAPSHOT_REFRESH
                else:
                    actual = _DatasetPipelineSupport._calendar_horizon(
                        dataset, resolved
                    )
                    basis = DataUpdateWindowBasis.EXPLICIT
                windows.append(
                    DataUpdateWindow(
                        dataset=dataset,
                        basis=basis,
                        start=actual[0],
                        end=actual[1],
                        overlap_days=self._catalog[dataset].overlap_days,
                        current_watermark=(
                            None
                            if record is None
                            or dataset
                            in {
                                DatasetKind.FINANCIAL_OBSERVATION,
                                DatasetKind.INSTRUMENT,
                            }
                            else record.end_date
                        ),
                    )
                )
            mode = "EXPLICIT"
        else:
            latest = self._calendar.latest_complete_day()
            for dataset in selected:
                record = records[dataset]
                if record is None or record.end_date is None:
                    raise RuntimeError("validated baseline record unexpectedly missing")
                overlap = self._catalog[dataset].overlap_days
                if dataset is DatasetKind.INSTRUMENT:
                    actual = (planning_date, planning_date)
                    basis = DataUpdateWindowBasis.SNAPSHOT_REFRESH
                    watermark = None
                    trigger_date = None
                elif dataset is DatasetKind.FINANCIAL_OBSERVATION:
                    batch = FinancialDisclosureSchedule.latest_completed_batch(
                        planning_date
                    )
                    if planning_date <= batch.disclosure_deadline:
                        skipped.append(
                            DataUpdateSkip(
                                dataset=dataset,
                                reason="DISCLOSURE_DEADLINE_PENDING",
                                trigger_date=batch.disclosure_deadline,
                            )
                        )
                        continue
                    actual = (batch.start, batch.end)
                    basis = DataUpdateWindowBasis.DISCLOSURE_TRIGGER
                    watermark = None
                    trigger_date = batch.disclosure_deadline
                else:
                    target_end = latest + (
                        timedelta(days=90)
                        if dataset is DatasetKind.TRADE_CALENDAR
                        else timedelta()
                    )
                    actual = (
                        min(record.end_date, latest) - timedelta(days=overlap),
                        target_end,
                    )
                    basis = DataUpdateWindowBasis.INCREMENTAL
                    watermark = record.end_date
                    trigger_date = None
                windows.append(
                    DataUpdateWindow(
                        dataset=dataset,
                        basis=basis,
                        start=actual[0],
                        end=actual[1],
                        overlap_days=overlap,
                        current_watermark=watermark,
                        trigger_date=trigger_date,
                    )
                )
            mode = "AUTO_INCREMENTAL"
        ordered = tuple(windows)
        summary_start = min((item.start for item in ordered), default=planning_date)
        summary_end = max((item.end for item in ordered), default=planning_date)
        return DataUpdatePlan(
            window_mode=mode,
            planned_at=planned_at,
            start=summary_start,
            end=summary_end,
            dataset_windows=ordered,
            requested_start=start,
            requested_end=end,
            skipped_datasets=tuple(skipped),
        )


class PipelineObserver(Protocol):
    """接收流水线进度并提供协作取消状态。

    入参：由协议方法声明给出。返回值：由实现返回。异常：实现异常按原契约传播。
    """

    def stage_started(self, stage: str, total: int) -> None:
        """通知阶段开始。

        入参：阶段名和数据集总数。返回值：无。异常：实现异常按原契约传播。
        """
        ...

    def dataset_completed(
        self,
        stage: str,
        dataset: DatasetKind,
        completed: int,
        total: int,
        details: Mapping[str, JsonValue],
    ) -> None:
        """通知一个数据集成功完成阶段。

        入参：阶段、数据集、计数和详情。返回值：无。异常：实现异常按原契约传播。
        """
        ...

    def boundary(
        self,
        stage: str,
        dataset: DatasetKind,
        kind: str,
        details: Mapping[str, JsonValue],
    ) -> None:
        """通知一个 Raw 请求或 Canonical 分区已到达安全边界。

        入参：阶段、数据集、边界类型和详情。返回值：无。异常：实现异常按原契约传播。
        """
        ...

    def is_cancelled(self) -> bool:
        """读取协作取消状态。

        入参：无。返回值：已请求取消时为真。异常：实现异常按原契约传播。
        """
        ...


class DataPipelineCancelled(RuntimeError):
    """表示流水线已在安全边界响应取消。

    入参：异常消息。返回值：构造异常。异常：无主动抛出的额外异常。
    """


class _NullPipelineObserver:
    """为 CLI 等同步调用提供无副作用观察器。"""

    def stage_started(self, stage: str, total: int) -> None:
        del stage, total

    def dataset_completed(
        self,
        stage: str,
        dataset: DatasetKind,
        completed: int,
        total: int,
        details: Mapping[str, JsonValue],
    ) -> None:
        del stage, dataset, completed, total, details

    def boundary(
        self,
        stage: str,
        dataset: DatasetKind,
        kind: str,
        details: Mapping[str, JsonValue],
    ) -> None:
        del stage, dataset, kind, details

    def is_cancelled(self) -> bool:
        return False


class DataPipeline:
    """编排可独立恢复的 LOCALIZE、CURATE 与 VALIDATE 阶段。

    入参：
        source：负责供应商会话、请求构造和 Raw 批次抓取的数据源端口。
        mapper：把已发布 Raw 分区规范化为 Canonical 批次的映射端口。
        calendar：解析最新完整交易日、显式窗口和首次建库窗口的日历策略。
        raw_store：原子发布并校验内容寻址 Raw Parquet 的存储服务。
        curated_store：合并、发布并校验 Canonical 当前分区的存储服务。
        repository：登记 Raw、Canonical、质量运行和目录状态的元数据端口。
        quality_runner：对绑定的 Canonical 状态执行数据质量规则的运行器。
        catalog：声明全部数据集 Schema、分区、抓取和复用语义的目录。
        routes：为每个数据集选择已启用供应商的静态路由表。
        clock：生成带时区当前时间的可注入时钟。
        logger：记录各阶段请求、输入身份和发布结果的结构化日志器。
    返回值：
        构造并返回 ``DataPipeline`` 实例。
    异常：
        ValueError：字段、枚举值或跨字段约束不合法时抛出。
    """

    def __init__(
        self,
        *,
        source: DatasetSource,
        mapper: CanonicalMapper,
        calendar: CalendarPolicy,
        raw_store: RawPartitionStore,
        curated_store: CuratedPartitionStore,
        repository: MetadataRepository,
        quality_runner: QualityRunner,
        routes: RoutingTable,
        catalog: DatasetCatalog = DATASET_CATALOG,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        logger: StructuredLogger | None = None,
    ) -> None:
        self._source = source
        self._mapper = mapper
        self._calendar = calendar
        self._raw_store = raw_store
        self._curated_store = curated_store
        self._repository = repository
        self._quality_runner = quality_runner
        self._catalog = catalog
        self._routes = routes
        self._clock = clock
        self._logger = logger
        self._source_session_active = False
        data_root = raw_store.root.parent
        if curated_store.root.parent != data_root:
            raise ValueError("raw and canonical stores must share one data root")
        self._execution_lock = DataRootExecutionLock(data_root)
        self._update_planner = DataUpdatePlanner(
            calendar=calendar,
            repository=repository,
            routes=routes,
            catalog=catalog,
            clock=clock,
        )

    def localize(
        self,
        dataset: DatasetKind,
        *,
        start: date,
        end: date,
        observer: PipelineObserver | None = None,
    ) -> LocalizeResult:
        """执行单个数据集的 LOCALIZE 阶段并记录 Raw 请求结果。

        入参：
            dataset：目标 Canonical 数据集标识。
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回``localize``（``LocalizeResult``）。
        异常：
            QuantError：同一数据根已有流水线运行时抛出
            ``DATA_PIPELINE_ALREADY_RUNNING``；其余数据或供应商错误保持原错误码。
        """
        with self._execution_lock:
            return self._localize(
                dataset,
                start=start,
                end=end,
                observer=observer,
            )

    def _localize(
        self,
        dataset: DatasetKind,
        *,
        start: date,
        end: date,
        observer: PipelineObserver | None,
    ) -> LocalizeResult:
        progress = observer or _NullPipelineObserver()
        spec = self._catalog[dataset]
        is_financial_cell = spec.fetch_plan is FetchPlan.FINANCIAL_CELL
        source_name = self._routes.source_for(dataset)
        if source_name != self._source.provider:
            self._raise(
                "DATA_ROUTE_SOURCE_MISMATCH", f"source {source_name} is unavailable"
            )
        if start > end:
            self._raise(
                "DATA_PIPELINE_ARGUMENT",
                "localize start must not follow end",
            )
        resolved_start, resolved_end = start, end
        command_request: dict[str, object] = {
            "dataset": dataset.value,
            "from": resolved_start.isoformat(),
            "to": resolved_end.isoformat(),
        }
        self._localize_log(
            "localize.started",
            request=command_request,
            dataset=dataset.value,
            source=source_name,
        )
        fetched = 0
        skipped = 0
        visible: list[PublishedPartition] = []

        def validate_batch(
            batch: RawBatch,
            *,
            endpoint: str,
            request: Mapping[str, JsonValue],
        ) -> None:
            if batch.source != source_name or batch.endpoint != endpoint:
                self._raise(
                    "DATA_SOURCE_CONTRACT", "source returned a different endpoint"
                )
            if dict(batch.request) != dict(request):
                self._raise(
                    "DATA_SOURCE_CONTRACT", "source returned a different request"
                )

        def publish_batch(
            batch: RawBatch,
            *,
            endpoint: str,
            request: Mapping[str, JsonValue],
        ) -> PublishedPartition:
            validate_batch(batch, endpoint=endpoint, request=request)
            partition = self._raw_store.publish(batch)
            self._register_raw_partition(partition)
            self._localize_log(
                "localize.raw_completed",
                request=request,
                dataset=dataset.value,
                source=source_name,
                endpoint=endpoint,
                disposition="fetched",
                request_hash=partition.request_hash,
                content_hash=partition.content_hash,
                schema_fingerprint=partition.schema_fingerprint,
                row_count=partition.row_count,
                retrieved_at=partition.retrieved_at.isoformat(),
                data_path=str(partition.data_path),
                manifest_path=str(partition.manifest_path),
            )
            progress.boundary(
                "LOCALIZE",
                dataset,
                "raw_request",
                {
                    "endpoint": endpoint,
                    "request_hash": partition.request_hash,
                    "row_count": partition.row_count,
                    "disposition": "fetched",
                },
            )
            return partition

        def publish_fetched(batch: RawBatch) -> PublishedPartition:
            nonlocal fetched
            partition = publish_batch(
                batch,
                endpoint=batch.endpoint,
                request=batch.request,
            )
            visible.append(partition)
            fetched += 1
            return partition

        def publish_or_reuse(
            endpoint: str,
            request: Mapping[str, JsonValue],
            fetch: Callable[[], Iterable[RawBatch]],
            *,
            force_fetch: bool = False,
        ) -> PublishedPartition | None:
            nonlocal fetched, skipped
            if progress.is_cancelled():
                raise DataPipelineCancelled("data pipeline cancellation requested")
            existing = None
            if not force_fetch:
                existing = self._find_raw_checkpoint(
                    source_name,
                    endpoint,
                    request,
                    reject_filesystem=(
                        None
                        if not is_financial_cell
                        else lambda partition: (
                            partition.row_count == 0
                            or not self._mapper.accepts_raw_schema(
                                endpoint, partition.schema_fingerprint
                            )
                        )
                    ),
                )
            if existing is not None:
                partition, checkpoint = existing
                skipped += 1
                visible.append(partition)
                self._localize_log(
                    "localize.raw_completed",
                    request=request,
                    dataset=dataset.value,
                    source=source_name,
                    endpoint=endpoint,
                    disposition=(
                        "sqlite_reused"
                        if checkpoint == "sqlite"
                        else "manifest_recovered"
                    ),
                    checkpoint=checkpoint,
                    request_hash=partition.request_hash,
                    content_hash=partition.content_hash,
                    schema_fingerprint=partition.schema_fingerprint,
                    row_count=partition.row_count,
                    retrieved_at=partition.retrieved_at.isoformat(),
                    data_path=str(partition.data_path),
                    manifest_path=str(partition.manifest_path),
                )
                progress.boundary(
                    "LOCALIZE",
                    dataset,
                    "raw_request",
                    {
                        "endpoint": endpoint,
                        "request_hash": partition.request_hash,
                        "row_count": partition.row_count,
                        "disposition": "reused",
                    },
                )
                return partition
            result: PublishedPartition | None = None
            try:
                for batch in fetch():
                    if (
                        is_financial_cell and len(batch.rows) == 0
                    ):
                        validate_batch(batch, endpoint=endpoint, request=request)
                        fetched += 1
                        self._localize_log(
                            "localize.raw_completed",
                            request=request,
                            dataset=dataset.value,
                            source=source_name,
                            endpoint=endpoint,
                            disposition="empty_discarded",
                            request_hash=hashlib.sha256(
                                canonical_json_bytes(dict(request))
                            ).hexdigest(),
                            row_count=0,
                        )
                        progress.boundary(
                            "LOCALIZE",
                            dataset,
                            "raw_request",
                            {
                                "endpoint": endpoint,
                                "request_hash": hashlib.sha256(
                                    canonical_json_bytes(dict(request))
                                ).hexdigest(),
                                "row_count": 0,
                                "disposition": "empty_discarded",
                            },
                        )
                        continue
                    result = publish_batch(batch, endpoint=endpoint, request=request)
                    visible.append(result)
                    fetched += 1
            except Exception as error:
                error_code = (
                    error.detail.code if isinstance(error, QuantError) else None
                )
                self._localize_log(
                    "localize.raw_failed",
                    request=request,
                    level="ERROR",
                    dataset=dataset.value,
                    source=source_name,
                    endpoint=endpoint,
                    error_type=type(error).__name__,
                    error_message=str(error),
                    error_code=error_code,
                )
                raise
            return result

        def filter_completed_requests(
            units: Sequence[tuple[str, Mapping[str, JsonValue]]],
            *,
            plan: str,
        ) -> tuple[tuple[str, Mapping[str, JsonValue], bool], ...]:
            """Resolve completed request heads with one query per endpoint."""
            nonlocal skipped
            endpoints = sorted({endpoint for endpoint, _ in units})
            raw_heads = {
                (record.endpoint, record.request_hash): record
                for endpoint in endpoints
                for record in self._repository.list_raw_partitions(
                    source=source_name, endpoint=endpoint
                )
            }
            pending: list[tuple[str, Mapping[str, JsonValue], bool]] = []
            empty_checkpoints_ignored = 0
            incompatible_checkpoints_ignored = 0
            for endpoint, request in units:
                request_hash = hashlib.sha256(
                    canonical_json_bytes(dict(request))
                ).hexdigest()
                record = raw_heads.get((endpoint, request_hash))
                if record is None:
                    pending.append((endpoint, request, False))
                    continue
                if plan == "financial_cell" and record.row_count == 0:
                    pending.append((endpoint, request, True))
                    empty_checkpoints_ignored += 1
                    continue
                if not self._mapper.accepts_raw_schema(
                    endpoint, record.schema_fingerprint
                ):
                    pending.append((endpoint, request, True))
                    incompatible_checkpoints_ignored += 1
                    continue
                partition = self._resolve_raw_record(
                    record,
                    request=request,
                )
                skipped += 1
                visible.append(partition)
            self._localize_log(
                "localize.requests_filtered",
                request=command_request,
                dataset=dataset.value,
                source=source_name,
                plan=plan,
                endpoints=endpoints,
                total_requests=len(units),
                completed_requests=len(units) - len(pending),
                pending_requests=len(pending),
                empty_checkpoints_ignored=empty_checkpoints_ignored,
                incompatible_checkpoints_ignored=incompatible_checkpoints_ignored,
                first_pending_request=(None if not pending else dict(pending[0][1])),
                filter="sqlite_bulk",
            )
            return tuple(pending)

        def ensure_active() -> None:
            if progress.is_cancelled():
                raise DataPipelineCancelled("data pipeline cancellation requested")

        def raise_source_contract(message: str) -> Never:
            self._raise("DATA_SOURCE_CONTRACT", message)

        owns_source_session = not self._source_session_active
        if owns_source_session:
            self._localize_log(
                "localize.source_login_started",
                request=command_request,
                dataset=dataset.value,
                source=source_name,
            )
            self._source.login()
            self._source_session_active = True
            self._localize_log(
                "localize.source_login_completed",
                request=command_request,
                dataset=dataset.value,
                source=source_name,
            )
        try:
            LocalizePlanExecutor.execute(
                spec.fetch_plan,
                LocalizePlanContext(
                    source=self._source,
                    start=resolved_start,
                    end=resolved_end,
                    endpoints=tuple(
                        endpoint.endpoint
                        for endpoint in spec.source_endpoints[source_name]
                    ),
                    publish_batch=publish_fetched,
                    publish_or_reuse=lambda endpoint, request, fetch, force: (
                        publish_or_reuse(
                            endpoint,
                            request,
                            fetch,
                            force_fetch=force,
                        )
                    ),
                    filter_completed=lambda units, plan: filter_completed_requests(
                        units, plan=plan
                    ),
                    ensure_active=ensure_active,
                    raise_contract=raise_source_contract,
                ),
            )
        finally:
            if owns_source_session:
                self._source_session_active = False
                self._localize_log(
                    "localize.source_close_started",
                    request=command_request,
                    dataset=dataset.value,
                    source=source_name,
                )
                try:
                    self._source.close()
                except Exception as error:  # noqa: BLE001 - cleanup cannot mask the request.
                    error_code = (
                        error.detail.code if isinstance(error, QuantError) else None
                    )
                    self._localize_log(
                        "localize.source_close_failed",
                        request=command_request,
                        level="WARNING",
                        dataset=dataset.value,
                        source=source_name,
                        error_type=type(error).__name__,
                        error_message=str(error),
                        error_code=error_code,
                    )
                else:
                    self._localize_log(
                        "localize.source_close_completed",
                        request=command_request,
                        dataset=dataset.value,
                        source=source_name,
                    )
        result = LocalizeResult(dataset, fetched, skipped, len(visible))
        self._localize_log(
            "localize.completed",
            request=command_request,
            dataset=dataset.value,
            source=source_name,
            fetched=fetched,
            skipped=skipped,
            raw_partitions=len(visible),
        )
        localized_through = (
            resolved_end - timedelta(days=90)
            if spec.fetch_plan is FetchPlan.TRADE_CALENDAR_RANGE
            else resolved_end
        )
        self._repository.record_dataset_stage(
            dataset,
            "LOCALIZE",
            completed_at=self._now(),
            localized_through=localized_through,
        )
        return result

    def localize_all(
        self,
        *,
        windows: Sequence[DataUpdateWindow],
        observer: PipelineObserver | None = None,
    ) -> tuple[LocalizeResult, ...]:
        """按目录顺序执行计划所选或全部数据集的 LOCALIZE 阶段。

        入参：
            windows：按数据集稳定排序且日期均已解析完成的执行窗口。
        返回值：
            返回``all``（``tuple[LocalizeResult, ...]``）。
        异常：
            QuantError：同一数据根已有流水线运行时抛出
            ``DATA_PIPELINE_ALREADY_RUNNING``；其余数据或供应商错误保持原错误码。
        """
        with self._execution_lock:
            return self._localize_all(
                windows=windows,
                observer=observer,
            )

    def _localize_all(
        self,
        *,
        windows: Sequence[DataUpdateWindow],
        observer: PipelineObserver | None,
    ) -> tuple[LocalizeResult, ...]:
        executable = tuple(
            dataset for dataset in self._catalog if self._routes[dataset]
        )
        ordered_windows = tuple(windows)
        if not ordered_windows:
            self._raise(
                "DATA_PIPELINE_ARGUMENT",
                "localize-all requires at least one explicit dataset window",
            )
        if ordered_windows != tuple(
            sorted(ordered_windows, key=lambda item: item.dataset.value)
        ) or len({item.dataset for item in ordered_windows}) != len(ordered_windows):
            self._raise(
                "DATA_PIPELINE_ARGUMENT",
                "localize-all windows must be unique and sorted by dataset",
            )
        datasets = tuple(item.dataset for item in ordered_windows)
        if not set(datasets).issubset(set(executable)):
            self._raise(
                "DATA_PIPELINE_ARGUMENT",
                "localize-all windows are not an executable catalog subset",
            )
        window_by_dataset = {item.dataset: item for item in ordered_windows}
        request: dict[str, object] = {
            "windows": [item.to_payload() for item in ordered_windows]
        }
        progress = observer or _NullPipelineObserver()
        progress.stage_started("LOCALIZE", len(datasets))
        self._localize_log(
            "localize_all.started",
            request=request,
            datasets=[dataset.value for dataset in datasets],
        )
        if self._source_session_active:
            raise RuntimeError(
                "localize-all cannot start inside an active source session"
            )
        source_name = self._source.provider
        self._localize_log(
            "localize_all.source_login_started",
            request=request,
            source=source_name,
        )
        self._source.login()
        self._source_session_active = True
        self._localize_log(
            "localize_all.source_login_completed",
            request=request,
            source=source_name,
        )
        try:
            completed_results: list[LocalizeResult] = []
            for index, dataset in enumerate(datasets, start=1):
                if progress.is_cancelled():
                    raise DataPipelineCancelled("data pipeline cancellation requested")
                window = window_by_dataset[dataset]
                result = self.localize(
                    dataset,
                    start=window.start,
                    end=window.end,
                    observer=progress,
                )
                completed_results.append(result)
                progress.dataset_completed(
                    "LOCALIZE",
                    dataset,
                    index,
                    len(datasets),
                    {
                        "fetched": result.fetched,
                        "skipped": result.skipped,
                        "raw_partitions": result.raw_partitions,
                    },
                )
            results = tuple(completed_results)
        finally:
            self._source_session_active = False
            self._localize_log(
                "localize_all.source_close_started",
                request=request,
                source=source_name,
            )
            try:
                self._source.close()
            except Exception as error:  # noqa: BLE001 - cleanup cannot mask localization.
                error_code = (
                    error.detail.code if isinstance(error, QuantError) else None
                )
                self._localize_log(
                    "localize_all.source_close_failed",
                    request=request,
                    level="WARNING",
                    source=source_name,
                    error_type=type(error).__name__,
                    error_message=str(error),
                    error_code=error_code,
                )
            else:
                self._localize_log(
                    "localize_all.source_close_completed",
                    request=request,
                    source=source_name,
                )
        self._localize_log(
            "localize_all.completed",
            request=request,
            datasets=[item.dataset.value for item in results],
            fetched=sum(item.fetched for item in results),
            skipped=sum(item.skipped for item in results),
            raw_partitions=sum(item.raw_partitions for item in results),
        )
        return results

    def _find_raw_checkpoint(
        self,
        source: str,
        endpoint: str,
        request: Mapping[str, JsonValue],
        *,
        reject_filesystem: Callable[[PublishedPartition], bool] | None = None,
    ) -> tuple[PublishedPartition, str] | None:
        request_hash = hashlib.sha256(canonical_json_bytes(dict(request))).hexdigest()
        record = self._repository.find_raw_partition(source, endpoint, request_hash)
        if record is not None:
            return (
                self._resolve_raw_record(
                    record,
                    request=request,
                ),
                "sqlite",
            )

        filesystem_partition = self._raw_store.find_metadata_by_request(
            source, endpoint, request
        )
        if filesystem_partition is None:
            return None
        if reject_filesystem is not None and reject_filesystem(filesystem_partition):
            return None
        self._register_raw_partition(filesystem_partition)
        return filesystem_partition, "filesystem_manifest"

    def _resolve_raw_record(
        self,
        record: RawPartitionRecord,
        *,
        request: Mapping[str, JsonValue],
    ) -> PublishedPartition:
        if dict(record.request) != dict(request):
            self._raise(
                "DATA_RAW_CATALOG_MISMATCH",
                "SQLite raw checkpoint request does not match its hash",
            )
        return self._published_partition(record)

    def _register_raw_partition(self, partition: PublishedPartition) -> None:
        self._repository.register_raw_partition(
            RawPartitionSpec(
                source=partition.source,
                endpoint=partition.endpoint,
                request=partition.request,
                request_hash=partition.request_hash,
                content_hash=partition.content_hash,
                data_path=partition.data_path,
                manifest_path=partition.manifest_path,
                schema_fingerprint=partition.schema_fingerprint,
                row_count=partition.row_count,
                retrieved_at=partition.retrieved_at,
            )
        )

    @staticmethod
    def _published_partition(record: RawPartitionRecord) -> PublishedPartition:
        return PublishedPartition(
            source=record.source,
            endpoint=record.endpoint,
            request=record.request,
            retrieved_at=record.retrieved_at,
            data_path=record.data_path,
            manifest_path=record.manifest_path,
            request_hash=record.request_hash,
            content_hash=record.content_hash,
            schema_fingerprint=record.schema_fingerprint,
            row_count=record.row_count,
        )

    def curate(
        self,
        dataset: DatasetKind,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> DatasetCurateResult:
        """执行单个数据集的增量 CURATE 阶段。

        入参：
            dataset：目标 Canonical 数据集标识。
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回``curate``（``DatasetCurateResult``）。
        异常：
            QuantError：同一数据根已有流水线运行时抛出
            ``DATA_PIPELINE_ALREADY_RUNNING``；其余数据错误保持原错误码。
        """
        with self._execution_lock:
            return self._curate(
                dataset,
                start=start,
                end=end,
            )

    def _curate(
        self,
        dataset: DatasetKind,
        *,
        start: date | None,
        end: date | None,
    ) -> DatasetCurateResult:
        if (start is None) != (end is None):
            self._raise(
                "DATA_PIPELINE_ARGUMENT", "start and end must be supplied together"
            )
        result = self._curate_datasets(
            (dataset,),
            windows={dataset: (start, end)},
            observer=None,
        )[0]
        return result

    def curate_all(
        self,
        *,
        observer: PipelineObserver | None = None,
    ) -> tuple[DatasetCurateResult, ...]:
        """共享 Raw 读取并执行全部数据集的 CURATE 阶段。

        入参：
        返回值：
            返回``all``（``tuple[DatasetCurateResult, ...]``）。
        异常：
            QuantError：同一数据根已有流水线运行时抛出
            ``DATA_PIPELINE_ALREADY_RUNNING``；其余数据错误保持原错误码。
        """
        with self._execution_lock:
            return self._curate_all(observer=observer)

    def _curate_all(
        self,
        *,
        observer: PipelineObserver | None,
    ) -> tuple[DatasetCurateResult, ...]:
        datasets = tuple(dataset for dataset in self._catalog if self._routes[dataset])
        return self._curate_many(datasets, observer=observer)

    def _curate_many(
        self,
        datasets: Sequence[DatasetKind],
        *,
        observer: PipelineObserver | None,
    ) -> tuple[DatasetCurateResult, ...]:
        """共享 Raw 读取并发布指定的非空数据集序列。"""
        if not datasets:
            self._raise(
                "DATA_UPDATE_PLAN_INVALID",
                "curate dataset selection must not be empty",
            )
        progress = observer or _NullPipelineObserver()
        progress.stage_started("CURATE", len(datasets))
        if progress.is_cancelled():
            raise DataPipelineCancelled("data pipeline cancellation requested")
        dataset_names = [dataset.value for dataset in datasets]
        self._curate_log(
            "curate_all.started",
            datasets=dataset_names,
        )
        results = self._curate_datasets(
            datasets,
            windows={dataset: (None, None) for dataset in datasets},
            observer=progress,
        )
        self._curate_log(
            "curate_all.completed",
            datasets=[
                {
                    "dataset": item.dataset.value,
                    "content_hash": item.content_hash,
                    "partition_count": item.partitions,
                    "row_count": item.rows,
                    "rebuilt_partitions": item.rebuilt_partitions,
                    "reused_partitions": item.reused_partitions,
                    "raw_inputs_read": item.raw_inputs_read,
                }
                for item in results
            ],
            partition_count=sum(item.partitions for item in results),
            row_count=sum(item.rows for item in results),
            rebuilt_partitions=sum(item.rebuilt_partitions for item in results),
            reused_partitions=sum(item.reused_partitions for item in results),
            raw_inputs_read=sum(item.raw_inputs_read for item in results),
        )
        return results

    def _curate_datasets(
        self,
        datasets: Sequence[DatasetKind],
        *,
        windows: Mapping[DatasetKind, tuple[date | None, date | None]],
        observer: PipelineObserver | None,
    ) -> tuple[DatasetCurateResult, ...]:
        progress = observer or _NullPipelineObserver()
        run_id = uuid4().hex
        endpoint_records: dict[tuple[str, str], tuple[RawPartitionRecord, ...]] = {}
        records_by_dataset: dict[DatasetKind, tuple[RawPartitionRecord, ...]] = {}
        snapshots: dict[DatasetKind, RawHeadSnapshot] = {}
        previous: dict[DatasetKind, CanonicalDatasetRecord | None] = {}
        groups: dict[DatasetKind, dict[str, list[RawPartitionRecord]]] = defaultdict(
            lambda: defaultdict(list)
        )
        candidates: dict[tuple[DatasetKind, str, str, str], tuple[str, ...]] = {}
        input_hashes: dict[DatasetKind, dict[str, str]] = defaultdict(dict)
        target_keys: dict[DatasetKind, set[str]] = defaultdict(set)
        removed_keys: dict[DatasetKind, set[str]] = defaultdict(set)
        rebuild_reasons: dict[tuple[DatasetKind, str], str] = {}
        skipped_schemas: dict[DatasetKind, int] = defaultdict(int)
        blocked_keys: dict[DatasetKind, set[str]] = defaultdict(set)

        for dataset in datasets:
            source = self._routes.source_for(dataset)
            start, end = windows[dataset]
            self._curate_log(
                "curate.started",
                dataset=dataset.value,
                source=source,
                requested_window={
                    "from": None if start is None else start.isoformat(),
                    "to": None if end is None else end.isoformat(),
                },
            )
            endpoints = tuple(
                sorted(
                    endpoint.endpoint
                    for endpoint in self._catalog[dataset].source_endpoints[source]
                )
            )
            records: list[RawPartitionRecord] = []
            for endpoint in endpoints:
                endpoint_cache_key = source, endpoint
                if endpoint_cache_key not in endpoint_records:
                    endpoint_records[endpoint_cache_key] = (
                        self._repository.list_raw_partitions(
                            source=source, endpoint=endpoint
                        )
                    )
                records.extend(endpoint_records[endpoint_cache_key])
            ordered_records = tuple(
                sorted(records, key=_DatasetPipelineSupport._raw_record_identity_key)
            )
            snapshots[dataset] = RawHeadSnapshot(
                source=source,
                endpoints=endpoints,
                heads=tuple(
                    RawHeadIdentity.from_record(record) for record in ordered_records
                ),
            )
            curate_now = self._now()
            ordered_records = tuple(
                record
                for record in ordered_records
                if self._mapper.raw_head_is_usable(
                    dataset, record.request, curate_now
                )
            )
            records_by_dataset[dataset] = ordered_records
            accepts_raw_schema = getattr(self._mapper, "accepts_raw_schema", None)
            for record in ordered_records:
                partition = self._published_partition(record)
                try:
                    keys = self._mapper.candidate_partition_keys(dataset, partition)
                except (KeyError, TypeError, ValueError) as error:
                    raise QuantError(
                        ErrorDetail(
                            code="DATA_CURATE_PARTITION_SCOPE_MISMATCH",
                            severity=Severity.FATAL,
                            message="Raw request cannot be assigned to a Canonical partition",
                            context={
                                "dataset": dataset.value,
                                "source": record.source,
                                "endpoint": record.endpoint,
                                "request_hash": record.request_hash,
                            },
                            remediation="repair the Raw request or mapper partition contract",
                            retryable=False,
                        )
                    ) from error
                identity = _DatasetPipelineSupport._raw_record_identity_key(record)
                candidates[(dataset, *identity)] = keys
                if callable(accepts_raw_schema) and not accepts_raw_schema(
                    record.endpoint, record.schema_fingerprint
                ):
                    skipped_schemas[dataset] += 1
                    blocked_keys[dataset].update(keys)
                    continue
                for partition_key in keys:
                    groups[dataset][partition_key].append(record)

            spec = self._catalog[dataset]
            transform_hash = self._curated_store.transform_hash(
                dataset,
                mapper_hash=self._mapper.transform_hash(dataset),
                partitioning=spec.partitioning.value,
                reuse=spec.reuse.value,
            )
            transform_digest = hashlib.sha256()
            transform_digest.update(bytes.fromhex(transform_hash))
            transform_digest.update(Path(__file__).read_bytes())
            transform_hash = transform_digest.hexdigest()
            for partition_key, raw_inputs in groups[dataset].items():
                input_hashes[dataset][partition_key] = (
                    _DatasetPipelineSupport._curate_input_hash(
                        dataset, partition_key, transform_hash, raw_inputs
                    )
                )
            current = self._repository.find_canonical_dataset(dataset)
            previous[dataset] = current
            previous_by_key = (
                {item.partition_key: item for item in current.partitions}
                if current is not None
                else {}
            )
            selected = {
                partition_key
                for partition_key in set(previous_by_key) | set(groups[dataset])
                if _DatasetPipelineSupport._partition_selected(
                    partition_key, start, end
                )
            }
            for partition_key in selected:
                old = previous_by_key.get(partition_key)
                if partition_key not in groups[dataset]:
                    if old is not None and partition_key not in blocked_keys[dataset]:
                        removed_keys[dataset].add(partition_key)
                    continue
                reason = None
                if old is None:
                    reason = "new_partition"
                elif not old.path.is_file():
                    reason = "canonical_file_missing"
                elif old.input_hash != input_hashes[dataset][partition_key]:
                    reason = "raw_input_changed"
                if reason is not None:
                    target_keys[dataset].add(partition_key)
                    rebuild_reasons[(dataset, partition_key)] = reason
            if current is None and not groups[dataset]:
                self._raise(
                    "DATA_CURATE_INPUT_MISSING", f"no raw input for {dataset.value}"
                )

        raw_targets: dict[tuple[str, str, str, str], dict[DatasetKind, set[str]]] = (
            defaultdict(lambda: defaultdict(set))
        )
        records_by_identity: dict[tuple[str, str, str, str], RawPartitionRecord] = {}
        raw_history: dict[
            tuple[str, str], dict[str, tuple[RawPartitionRecord, ...]]
        ] = {}
        for dataset in datasets:
            for partition_key in target_keys[dataset]:
                for record in groups[dataset][partition_key]:
                    histories: tuple[RawPartitionRecord, ...] = (record,)
                    if self._mapper.requires_raw_history(dataset):
                        endpoint_key = record.source, record.endpoint
                        if endpoint_key not in raw_history:
                            by_request: dict[str, list[RawPartitionRecord]] = (
                                defaultdict(list)
                            )
                            for item in self._repository.list_raw_objects(
                                source=record.source, endpoint=record.endpoint
                            ):
                                if self._mapper.accepts_raw_schema(
                                    item.endpoint, item.schema_fingerprint
                                ):
                                    by_request[item.request_hash].append(item)
                            raw_history[endpoint_key] = {
                                request_hash: tuple(items)
                                for request_hash, items in by_request.items()
                            }
                        histories = raw_history[endpoint_key].get(
                            record.request_hash, (record,)
                        )
                    for item in histories:
                        object_identity = (
                            _DatasetPipelineSupport._raw_object_identity_key(item)
                        )
                        records_by_identity[object_identity] = item
                        raw_targets[object_identity][dataset].add(partition_key)

        frames: dict[DatasetKind, dict[str, list[pl.DataFrame]]] = defaultdict(
            lambda: defaultdict(list)
        )
        normalized_batches: dict[DatasetKind, list[CanonicalBatch]] = defaultdict(list)
        raw_inputs_read: dict[DatasetKind, set[tuple[str, str, str, str]]] = (
            defaultdict(set)
        )
        ordered_to_read = sorted(
            records_by_identity.values(),
            key=lambda item: (
                item.retrieved_at,
                item.endpoint,
                item.request_hash,
            ),
        )
        for record in ordered_to_read:
            raw_identity = _DatasetPipelineSupport._raw_object_identity_key(record)
            partition = self._published_partition(record)
            self._raw_store.verify_managed_partition(partition)
            outputs = tuple(self._mapper.normalize(partition))
            for dataset in raw_targets[raw_identity]:
                raw_inputs_read[dataset].add(raw_identity)
            for batch in outputs:
                wanted = raw_targets[raw_identity].get(batch.dataset)
                if not wanted or batch.frame.is_empty():
                    continue
                actual = self._curated_store.partition_frame(batch.dataset, batch.frame)
                allowed = set(
                    candidates[
                        (
                            batch.dataset,
                            record.source,
                            record.endpoint,
                            record.request_hash,
                        )
                    ]
                )
                actual_keys = {partition_key for partition_key, _ in actual}
                if not actual_keys.issubset(allowed):
                    raise QuantError(
                        ErrorDetail(
                            code="DATA_CURATE_PARTITION_SCOPE_MISMATCH",
                            severity=Severity.FATAL,
                            message="mapped rows escaped the Raw request partition scope",
                            context={
                                "dataset": batch.dataset.value,
                                "endpoint": record.endpoint,
                                "request_hash": record.request_hash,
                                "candidate_partitions": sorted(allowed),
                                "actual_partitions": sorted(actual_keys),
                            },
                            remediation="repair request-to-partition mapping",
                            retryable=False,
                        )
                    )
                normalized_batches[batch.dataset].append(batch)
                for partition_key, frame in actual:
                    if partition_key in wanted:
                        frames[batch.dataset][partition_key].append(frame)

        results: list[DatasetCurateResult] = []
        for dataset in datasets:
            source = self._routes.source_for(dataset)
            current = previous[dataset]
            if not target_keys[dataset] and not removed_keys[dataset]:
                if current is None:
                    self._raise(
                        "DATA_CURATE_INPUT_MISSING", f"no raw input for {dataset.value}"
                    )
                assert current is not None
                result = DatasetCurateResult(
                    dataset=dataset,
                    content_hash=current.content_hash,
                    partitions=len(current.partitions),
                    rows=sum(item.row_count for item in current.partitions),
                    rebuilt_partitions=0,
                    reused_partitions=len(current.partitions),
                    raw_inputs_read=0,
                )
                self._curate_log(
                    "curate.dataset_reused",
                    dataset=dataset.value,
                    source=source,
                    run_id=run_id,
                    dataset_content_hash=current.content_hash,
                    partition_count=result.partitions,
                    row_count=result.rows,
                )
                results.append(result)
                self._log_curate_completed(result, current, source, run_id)
                self._repository.record_dataset_stage(
                    dataset, "CURATE", completed_at=self._now()
                )
                self._notify_curate_result(
                    progress, result, len(results), len(datasets)
                )
                continue

            replacements: list[CanonicalPartitionReplacement] = []
            for partition_key in sorted(target_keys[dataset]):
                if progress.is_cancelled():
                    raise DataPipelineCancelled("data pipeline cancellation requested")
                pieces = frames[dataset].get(partition_key, [])
                complete = self._mapper.consolidate_partition(dataset, pieces)
                replacements.append(
                    CanonicalPartitionReplacement(
                        partition_key=partition_key,
                        frame=complete,
                        input_hash=input_hashes[dataset][partition_key],
                        raw_input_count=len(groups[dataset][partition_key]),
                        rebuild_reason=rebuild_reasons[(dataset, partition_key)],
                    )
                )
                progress.boundary(
                    "CURATE",
                    dataset,
                    "canonical_partition",
                    {
                        "partition_key": partition_key,
                        "row_count": complete.height,
                        "raw_input_count": len(groups[dataset][partition_key]),
                    },
                )
            start, end = windows[dataset]
            if normalized_batches[dataset]:
                resolved_start, resolved_end = self._batch_window(
                    normalized_batches[dataset], start, end
                )
            elif current is not None and current.start_date and current.end_date:
                resolved_start, resolved_end = current.start_date, current.end_date
            elif start is not None and end is not None:
                resolved_start, resolved_end = start, end
            else:
                today = self._now().date()
                resolved_start = resolved_end = today
            replace_existing_window = (
                self._catalog[dataset].reuse is ReuseSemantics.FULL_REFRESH
            )
            if current is not None and not replace_existing_window:
                if current.start_date is not None:
                    resolved_start = min(resolved_start, current.start_date)
                if current.end_date is not None:
                    resolved_end = max(resolved_end, current.end_date)
            self._curate_log(
                "curate.publish_started",
                dataset=dataset.value,
                source=source,
                run_id=run_id,
                resolved_from=resolved_start.isoformat(),
                resolved_to=resolved_end.isoformat(),
                raw_partitions=len(records_by_dataset[dataset]),
                skipped_raw_partitions=skipped_schemas[dataset],
                rebuilt_partitions=len(replacements),
                removed_partitions=len(removed_keys[dataset]),
                raw_inputs_read=len(raw_inputs_read[dataset]),
                input_rows=sum(item.frame.height for item in replacements),
            )
            published_record = self._curated_store.publish_replacements(
                dataset,
                replacements,
                removed_keys=tuple(sorted(removed_keys[dataset])),
                previous=current,
                run_id=run_id,
                source=source,
                start=resolved_start,
                end=resolved_end,
                repository=self._repository,
                expected_raw_heads=snapshots[dataset],
                logger=self._logger,
            )
            result = DatasetCurateResult(
                dataset=dataset,
                content_hash=published_record.content_hash,
                partitions=len(published_record.partitions),
                rows=sum(item.row_count for item in published_record.partitions),
                rebuilt_partitions=len(replacements),
                reused_partitions=len(published_record.partitions) - len(replacements),
                raw_inputs_read=len(raw_inputs_read[dataset]),
            )
            results.append(result)
            self._log_curate_completed(result, published_record, source, run_id)
            self._repository.record_dataset_stage(
                dataset, "CURATE", completed_at=self._now()
            )
            self._notify_curate_result(progress, result, len(results), len(datasets))
        return tuple(results)

    @staticmethod
    def _notify_curate_result(
        observer: PipelineObserver,
        result: DatasetCurateResult,
        completed: int,
        total: int,
    ) -> None:
        observer.dataset_completed(
            "CURATE",
            result.dataset,
            completed,
            total,
            {
                "content_hash": result.content_hash,
                "partitions": result.partitions,
                "rows": result.rows,
                "rebuilt_partitions": result.rebuilt_partitions,
                "reused_partitions": result.reused_partitions,
                "raw_inputs_read": result.raw_inputs_read,
            },
        )

    def _log_curate_completed(
        self,
        result: DatasetCurateResult,
        record: CanonicalDatasetRecord,
        source: str,
        run_id: str,
    ) -> None:
        self._curate_log(
            "curate.completed",
            dataset=result.dataset.value,
            source=source,
            run_id=run_id,
            dataset_content_hash=record.content_hash,
            partition_count=result.partitions,
            row_count=result.rows,
            rebuilt_partitions=result.rebuilt_partitions,
            reused_partitions=result.reused_partitions,
            raw_inputs_read=result.raw_inputs_read,
            partitions=[
                _DatasetPipelineSupport._canonical_partition_context(partition)
                for partition in record.partitions
            ],
            updated_at=record.updated_at.isoformat(),
        )

    def validate(
        self,
        dataset: DatasetKind | None = None,
        *,
        heartbeat: Callable[[], None] = lambda: None,
    ) -> QualityRunId:
        """诊断指定范围的 Canonical 数据质量并记录运行结果。

        入参：
            dataset：目标 Canonical 数据集标识。
            heartbeat：在数据集和质量规则安全边界执行的心跳或取消检查。
        返回值：
            返回校验Canonical 数据后的``validate``（``QualityRunId``）。
        异常：
            QuantError：同一数据根已有流水线运行时抛出
            ``DATA_PIPELINE_ALREADY_RUNNING``；其余质量或文件错误保持原错误码。
        """
        with self._execution_lock:
            return self._validate(dataset, heartbeat=heartbeat)

    def _validate(
        self,
        dataset: DatasetKind | None,
        *,
        heartbeat: Callable[[], None],
    ) -> QualityRunId:
        scope = "ALL" if dataset is None else "DATASET"
        heartbeat()
        command_request: dict[str, object] = {
            "dataset": None if dataset is None else dataset.value,
            "scope": "all" if dataset is None else "dataset",
        }
        datasets = (
            (dataset,)
            if dataset is not None
            else tuple(item for item in self._catalog if self._routes[item])
        )
        input_hash = self._repository.catalog_state().catalog_hash
        self._validate_log(
            "validate.started",
            scope=scope,
            selected_dataset=command_request["dataset"],
            datasets=[item.value for item in datasets],
            catalog_hash=input_hash,
        )
        records: dict[DatasetKind, CanonicalDatasetRecord] = {}
        for item in datasets:
            heartbeat()
            current = self._repository.find_canonical_dataset(item)
            if current is None:
                self._raise(
                    "DATA_VALIDATE_INPUT_MISSING",
                    f"no canonical data for {item.value}",
                )
            records[item] = current
            self._validate_log(
                "validate.dataset_resolved",
                scope=scope,
                catalog_hash=input_hash,
                dataset=item.value,
                source=current.source,
                dataset_content_hash=current.content_hash,
                partition_count=len(current.partitions),
                row_count=sum(partition.row_count for partition in current.partitions),
                partitions=[
                    _DatasetPipelineSupport._canonical_partition_context(partition)
                    for partition in current.partitions
                ],
            )
        frames = {
            item: self._curated_store.scan_dataset(record)
            for item, record in records.items()
        }
        dataset_hashes = {
            item.value: record.content_hash for item, record in records.items()
        }
        self._validate_log(
            "validate.rules_started",
            scope=scope,
            catalog_hash=input_hash,
            dataset_hashes=dataset_hashes,
        )
        evaluation = self._quality_runner.evaluate(frames, heartbeat=heartbeat)
        heartbeat()
        issues = list(evaluation.issues)
        for result in evaluation.rule_results:
            self._validate_log(
                "validate.rule_evaluated",
                scope=scope,
                catalog_hash=input_hash,
                result=_DatasetPipelineSupport._quality_rule_result_context(result),
            )
        for issue in issues:
            self._validate_log(
                "validate.issue_detected",
                scope=scope,
                catalog_hash=input_hash,
                issue=_DatasetPipelineSupport._quality_issue_context(issue),
            )
        severity_counts = {
            severity.value: sum(issue.severity is severity for issue in issues)
            for severity in Severity
        }
        result_counts = {
            status: sum(item.status.value == status for item in evaluation.rule_results)
            for status in ("PASS", "FAIL", "SKIPPED")
        }
        self._validate_log(
            "validate.rules_completed",
            scope=scope,
            catalog_hash=input_hash,
            dataset_hashes=dataset_hashes,
            issue_count=len(issues),
            severity_counts=severity_counts,
            result_counts=result_counts,
            blocking_issue_count=sum(
                issue.severity in {Severity.SEVERE, Severity.FATAL} for issue in issues
            ),
        )
        now = self._now()
        self._validate_log(
            "validate.quality_run_write_started",
            scope=scope,
            catalog_hash=input_hash,
            dataset_hashes=dataset_hashes,
            issue_count=len(issues),
        )
        heartbeat()
        quality = self._repository.register_quality_run(
            QualityRunSpec(
                dataset_hashes=dataset_hashes,
                input_hash=input_hash,
                scope=scope,
                issues=tuple(issues),
                rule_results=evaluation.rule_results,
                results_complete=True,
                started_at=now,
                completed_at=now,
            )
        )
        self._validate_log(
            "validate.quality_run_registered",
            scope=scope,
            catalog_hash=input_hash,
            quality_run_id=str(quality.id),
            status=quality.status,
            dataset_hashes=dict(quality.dataset_hashes),
            issue_count=len(quality.issues),
        )
        gate_opened = False
        if dataset is None and quality.status == "PASSED":
            state = self._repository.mark_catalog_validated(
                quality.id, validated_at=self._now()
            )
            gate_opened = True
            self._validate_log(
                "validate.gate_opened",
                scope=scope,
                quality_run_id=str(quality.id),
                data_hash=state.catalog_hash,
            )
        self._validate_log(
            "validate.completed",
            scope=scope,
            catalog_hash=input_hash,
            quality_run_id=str(quality.id),
            status=quality.status,
            issue_count=len(quality.issues),
            severity_counts=severity_counts,
            gate_opened=gate_opened,
        )
        validated_at = self._now()
        for item in datasets:
            self._repository.record_dataset_stage(
                item, "VALIDATE", completed_at=validated_at
            )
        if dataset is None and quality.status != "PASSED":
            self._raise(
                "DATA_VALIDATION_FAILED",
                "validate-all found blocking quality issues",
            )
        return quality.id

    def bootstrap(self, *, years: int) -> PipelineResult:
        """从空数据根目录执行首次全量数据流水线。

        入参：
            years：首次基线向前覆盖的正整数年数。
        返回值：
            返回``bootstrap``（``PipelineResult``）。
        异常：
            QuantError：同一数据根已有流水线运行时抛出
            ``DATA_PIPELINE_ALREADY_RUNNING``；其余阶段错误保持原错误码。
        """
        with self._execution_lock:
            return self._bootstrap(years=years)

    def _bootstrap(self, *, years: int) -> PipelineResult:
        if type(years) is not int or years <= 0:
            self._raise(
                "DATA_PIPELINE_ARGUMENT",
                "bootstrap years must be a positive integer",
            )
        executable = {
            dataset for dataset in self._catalog if self._routes[dataset]
        }
        existing = {
            record.dataset for record in self._repository.list_canonical_datasets()
        }
        initialization = self._repository.find_data_initialization()
        if (
            initialization is not None and initialization.status == "COMPLETED"
        ) or (initialization is None and executable.issubset(existing)):
            raise QuantError(
                ErrorDetail(
                    code="DATA_BOOTSTRAP_ALREADY_INITIALIZED",
                    severity=Severity.SEVERE,
                    message=(
                        "bootstrap is only available before the canonical baseline "
                        "is complete"
                    ),
                    context={},
                    remediation="run quant data update for subsequent refreshes",
                    retryable=False,
                )
            )
        if initialization is not None and initialization.years != years:
            raise QuantError(
                ErrorDetail(
                    code="DATA_BOOTSTRAP_YEARS_MISMATCH",
                    severity=Severity.SEVERE,
                    message="bootstrap retry must use the frozen year count",
                    context={
                        "frozen_years": initialization.years,
                        "requested_years": years,
                    },
                    remediation=(
                        f"retry with quant data bootstrap --years {initialization.years}"
                    ),
                    retryable=False,
                )
            )
        frozen = (
            None
            if initialization is None
            else (initialization.start_date, initialization.end_date)
        )
        plan = self._update_planner.plan_bootstrap(
            years,
            frozen_window=frozen,
            frozen_planned_at=(
                None if initialization is None else initialization.started_at
            ),
        )
        if initialization is None:
            if plan.requested_start is None or plan.requested_end is None:
                raise RuntimeError("bootstrap plan did not freeze its base window")
            initialization = self._repository.begin_data_initialization(
                years=years,
                start_date=plan.requested_start,
                end_date=plan.requested_end,
                started_at=plan.planned_at,
            )
        run_id = uuid4().hex
        self.localize_all(windows=plan.dataset_windows)
        self.curate_all()
        quality = self.validate()
        state = self._repository.catalog_state()
        self._repository.complete_data_initialization(
            catalog_hash=state.catalog_hash,
            quality_run_id=quality,
            completed_at=self._now(),
        )
        return PipelineResult(
            run_id, quality, state.catalog_hash
        )

    def update(
        self,
        *,
        start: date | None = None,
        end: date | None = None,
        observer: PipelineObserver | None = None,
    ) -> PipelineResult:
        """按显式窗口执行日常增量数据流水线。

        入参：
            start：日期闭区间的开始日期。
            end：日期闭区间的结束日期。
        返回值：
            返回``update``（``PipelineResult``）。
        异常：
            QuantError：同一数据根已有流水线运行时抛出
            ``DATA_PIPELINE_ALREADY_RUNNING``；其余计划或阶段错误保持原错误码。
        """
        with self._execution_lock:
            return self._update(start=start, end=end, observer=observer)

    def _update(
        self,
        *,
        start: date | None,
        end: date | None,
        observer: PipelineObserver | None,
    ) -> PipelineResult:
        plan = self.plan_update(start=start, end=end)
        return self.execute_update_plan(plan, observer=observer)

    def plan_update(
        self,
        *,
        start: date | None = None,
        end: date | None = None,
        datasets: Sequence[DatasetKind] | None = None,
    ) -> DataUpdatePlan:
        """按当前水位和供应商日历生成确定性计划。

        入参：可选的完整日期闭区间。返回值：不可变更新计划。
        异常：参数、日历或水位解析失败时传播对应异常。
        """
        return self._update_planner.plan(
            start=start,
            end=end,
            datasets=datasets,
        )

    def execute_update_plan(
        self,
        plan: DataUpdatePlan,
        *,
        observer: PipelineObserver | None = None,
    ) -> PipelineResult:
        """严格执行已持久化计划。

        入参：已验证计划和可选进度观察器。返回值：完整流水线结果。
        异常：计划数据集与目录不一致或任一流水线阶段失败时传播对应异常。
            同一数据根已有流水线运行时抛出
            ``DATA_PIPELINE_ALREADY_RUNNING``。
        """
        with self._execution_lock:
            return self._execute_update_plan(plan, observer=observer)

    def _execute_update_plan(
        self,
        plan: DataUpdatePlan,
        *,
        observer: PipelineObserver | None,
    ) -> PipelineResult:
        run_id = uuid4().hex
        progress = observer or _NullPipelineObserver()
        self.localize_all(windows=plan.dataset_windows, observer=progress)
        selected = tuple(item.dataset for item in plan.dataset_windows)
        self._curate_many(selected, observer=progress)
        if progress.is_cancelled():
            raise DataPipelineCancelled("data pipeline cancellation requested")
        datasets = tuple(dataset for dataset in self._catalog if self._routes[dataset])
        progress.stage_started("VALIDATE", len(datasets))
        quality = self.validate()
        for index, dataset in enumerate(datasets, start=1):
            progress.dataset_completed(
                "VALIDATE",
                dataset,
                index,
                len(datasets),
                {"quality_run_id": str(quality)},
            )
        return PipelineResult(
            run_id, quality, self._repository.catalog_state().catalog_hash
        )

    @staticmethod
    def _batch_window(
        batches: Sequence[CanonicalBatch], start: date | None, end: date | None
    ) -> tuple[date, date]:
        if batches and all(
            batch.dataset is DatasetKind.INSTRUMENT for batch in batches
        ):
            snapshot_dates = [
                value.astimezone(ZoneInfo("Asia/Shanghai")).date()
                for batch in batches
                for value in batch.frame["ingested_at"].to_list()
                if isinstance(value, datetime)
            ]
            if snapshot_dates:
                return min(snapshot_dates), max(snapshot_dates)
        if start is not None and end is not None:
            return start, end
        dates: list[date] = []
        for batch in batches:
            frame = batch.frame
            for column in (
                "trade_date",
                "report_period",
                "as_of_date",
                "effective_start",
                "list_date",
            ):
                if column in frame.columns:
                    dates.extend(
                        value for value in frame[column].to_list() if value is not None
                    )
                    break
        today = datetime.now(UTC).date()
        return (min(dates), max(dates)) if dates else (today, today)

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    def _localize_log(
        self,
        event: str,
        *,
        request: Mapping[str, object],
        level: str = "INFO",
        error_code: str | None = None,
        **context: object,
    ) -> None:
        if self._logger is None:
            return
        self._logger.emit(
            level,
            event,
            stage="LOCALIZE",
            context={"request": dict(request), **context},
            error_code=error_code,
        )

    def _curate_log(
        self,
        event: str,
        *,
        level: str = "INFO",
        **business_data: object,
    ) -> None:
        if self._logger is None:
            return
        self._logger.emit(
            level,
            event,
            stage="CURATE",
            context=business_data,
        )

    def _validate_log(
        self,
        event: str,
        *,
        scope: str,
        level: str = "INFO",
        **validation_data: object,
    ) -> None:
        if self._logger is None:
            return
        self._logger.emit(
            level,
            event,
            stage="VALIDATE",
            context={"scope": scope, **validation_data},
        )

    @staticmethod
    def _raise(code: str, message: str) -> Never:
        raise QuantError(
            ErrorDetail(
                code=code,
                severity=Severity.SEVERE,
                message=message,
                context={},
                remediation="inspect the preceding data stage and retry",
                retryable=False,
            )
        )


class _DatasetPipelineSupport:
    """集中承载数据流水线内部的身份、窗口与日志上下文计算逻辑。"""

    @staticmethod
    def _canonical_partition_context(
        partition: CanonicalPartitionRecord,
    ) -> dict[str, object]:
        return {
            "partition_key": partition.partition_key,
            "content_hash": partition.content_hash,
            "schema_fingerprint": partition.schema_fingerprint,
            "row_count": partition.row_count,
            "path": str(partition.path),
        }

    @staticmethod
    def _raw_record_identity_key(record: RawPartitionRecord) -> tuple[str, str, str]:
        return record.source, record.endpoint, record.request_hash

    @staticmethod
    def _raw_object_identity_key(
        record: RawPartitionRecord,
    ) -> tuple[str, str, str, str]:
        return record.source, record.endpoint, record.request_hash, record.content_hash

    @staticmethod
    def _curate_input_hash(
        dataset: DatasetKind,
        partition_key: str,
        transform_hash: str,
        records: Sequence[RawPartitionRecord],
    ) -> str:
        payload = cast(
            JsonValue,
            {
                "dataset": dataset.value,
                "partition_key": partition_key,
                "transform_hash": transform_hash,
                "inputs": [
                    {
                        "source": record.source,
                        "endpoint": record.endpoint,
                        "request_hash": record.request_hash,
                        "content_hash": record.content_hash,
                        "schema_fingerprint": record.schema_fingerprint,
                        "retrieved_at": record.retrieved_at.astimezone(UTC)
                        .isoformat()
                        .replace("+00:00", "Z"),
                    }
                    for record in sorted(
                        records, key=_DatasetPipelineSupport._raw_record_identity_key
                    )
                ],
            },
        )
        return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()

    @staticmethod
    def _partition_selected(
        partition_key: str, start: date | None, end: date | None
    ) -> bool:
        if start is None or end is None or partition_key == "all":
            return True
        try:
            year = int(partition_key.rsplit("=", 1)[1])
        except (IndexError, ValueError):
            return True
        return start.year <= year <= end.year

    @staticmethod
    def _calendar_horizon(
        dataset: DatasetKind,
        window: tuple[date, date],
    ) -> tuple[date, date]:
        if dataset is not DatasetKind.TRADE_CALENDAR:
            return window
        start, end = window
        return start, end + timedelta(days=90)

    @staticmethod
    def _quality_issue_context(issue: QualityIssue) -> dict[str, object]:
        return {
            "rule_id": issue.rule_id,
            "severity": issue.severity.value,
            "dataset": issue.dataset.value,
            "scope": thaw_json(issue.scope),
            "actual": thaw_json(issue.actual),
            "threshold": thaw_json(issue.threshold),
            "message": issue.message,
            "remediation": issue.remediation,
        }

    @staticmethod
    def _quality_rule_result_context(result: QualityRuleResult) -> dict[str, object]:
        return {
            "rule_id": result.rule_id,
            "severity": result.severity.value,
            "dataset": result.dataset.value,
            "status": result.status.value,
            "scope": thaw_json(result.scope),
            "actual": thaw_json(result.actual),
            "threshold": thaw_json(result.threshold),
            "skip_reason": result.skip_reason,
        }
