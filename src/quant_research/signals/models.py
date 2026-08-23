"""定义三种互斥 Schema 的不可变策略信号产物。"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from math import isfinite
from typing import Protocol

from quant_research.domain.identifiers import InstrumentId


class Direction(StrEnum):
    """定义时序方向信号。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    LONG = "LONG"
    FLAT = "FLAT"
    SHORT = "SHORT"


@dataclass(frozen=True, slots=True)
class ArtifactIdentity:
    """绑定信号产物的运行、组件、数据和研究区间身份。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    run_id: str
    component_id: str
    component_hash: str
    catalog_hash: str
    universe_hash: str | None
    start_date: date
    end_date: date

    def __post_init__(self) -> None:
        if not self.run_id or not self.component_id:
            raise ValueError("artifact run and component IDs must not be empty")
        for name, value in (
            ("component_hash", self.component_hash),
            ("catalog_hash", self.catalog_hash),
        ):
            if len(value) != 64:
                raise ValueError(f"{name} must be a SHA-256 digest")
        if self.start_date > self.end_date:
            raise ValueError("artifact start_date must not exceed end_date")


@dataclass(frozen=True, slots=True)
class CrossSectionalScoreRow:
    """表示一个证券在一个决策日的横截面评分。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    signal_date: date
    instrument_id: str
    signal_id: str
    score: float | None
    confidence: float
    available_at: datetime
    is_valid: bool
    invalid_reason: str | None

    def __post_init__(self) -> None:
        InstrumentId.parse(self.instrument_id)
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("signal confidence must be between zero and one")
        if self.is_valid != (self.score is not None):
            raise ValueError("valid cross-sectional signals require a score")
        if self.score is not None and not isfinite(self.score):
            raise ValueError("signal score must be finite")
        if self.is_valid == (self.invalid_reason is not None):
            raise ValueError("invalid_reason must be present only for invalid signals")


@dataclass(frozen=True, slots=True)
class DirectionalSignalRow:
    """表示一个证券在一个决策日的方向和状态变化。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    signal_date: date
    instrument_id: str
    signal_id: str
    direction: Direction | None
    strength: float
    state_changed: bool
    available_at: datetime
    is_valid: bool
    invalid_reason: str | None

    def __post_init__(self) -> None:
        InstrumentId.parse(self.instrument_id)
        if not 0.0 <= self.strength <= 1.0:
            raise ValueError("directional strength must be between zero and one")
        if self.is_valid != (self.direction is not None):
            raise ValueError("valid directional signals require a direction")
        if self.is_valid == (self.invalid_reason is not None):
            raise ValueError("invalid_reason must be present only for invalid signals")


@dataclass(frozen=True, slots=True)
class AllocationSignalRow:
    """表示一个证券在一个决策日的目标暴露意图。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    signal_date: date
    instrument_id: str
    signal_id: str
    desired_exposure: float | None
    available_at: datetime
    is_valid: bool
    invalid_reason: str | None

    def __post_init__(self) -> None:
        InstrumentId.parse(self.instrument_id)
        if self.is_valid != (self.desired_exposure is not None):
            raise ValueError("valid allocation signals require desired_exposure")
        if self.desired_exposure is not None and not isfinite(self.desired_exposure):
            raise ValueError("desired_exposure must be finite")
        if self.is_valid == (self.invalid_reason is not None):
            raise ValueError("invalid_reason must be present only for invalid signals")


@dataclass(frozen=True, slots=True)
class CrossSectionalScoreArtifact:
    """保存按稳定主键排序的横截面评分产物。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    identity: ArtifactIdentity
    rows: tuple[CrossSectionalScoreRow, ...]

    def __post_init__(self) -> None:
        _validate_rows(self.rows)


@dataclass(frozen=True, slots=True)
class DirectionalSignalArtifact:
    """保存按稳定主键排序的方向信号产物。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    identity: ArtifactIdentity
    rows: tuple[DirectionalSignalRow, ...]

    def __post_init__(self) -> None:
        _validate_rows(self.rows)


@dataclass(frozen=True, slots=True)
class AllocationSignalArtifact:
    """保存按稳定主键排序的配置暴露产物。

    入参：
        参数和字段含义由公开签名及类型声明给出。
    返回值：
        返回该操作构造、计算或查询得到的领域结果。
    异常：
        输入违反领域不变量时抛出类型或值错误；依赖失败保持原异常语义。
    """

    identity: ArtifactIdentity
    rows: tuple[AllocationSignalRow, ...]

    def __post_init__(self) -> None:
        _validate_rows(self.rows)


class _SignalKey(Protocol):
    @property
    def signal_date(self) -> date: ...

    @property
    def instrument_id(self) -> str: ...

    @property
    def signal_id(self) -> str: ...


def _validate_rows(rows: Sequence[_SignalKey]) -> None:
    keys = tuple((row.signal_date, row.instrument_id, row.signal_id) for row in rows)
    if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
        raise ValueError(
            "signal rows must have unique, deterministic primary-key order"
        )
