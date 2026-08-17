"""提供因子与基础契约相关的公开模型、协议与处理流程。"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from math import isfinite
from types import MappingProxyType
from typing import Protocol, Self, cast
from zoneinfo import ZoneInfo

import numpy as np
import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.compute as pc  # type: ignore[import-untyped]

from quant_research.data.contracts import JsonScalar, JsonValue, canonical_json_bytes
from quant_research.data.schemas import PolarsDataType
from quant_research.domain.enums import DatasetKind

type FrozenJsonValue = (
    JsonScalar | tuple["FrozenJsonValue", ...] | Mapping[str, "FrozenJsonValue"]
)

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SHANGHAI = ZoneInfo("Asia/Shanghai")

_FACTOR_OUTPUT_COLUMNS: dict[str, PolarsDataType] = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "factor_id": pl.String,
    "value": pl.Float64,
    "available_at": pl.Datetime("us", "UTC"),
    "is_valid": pl.Boolean,
}
FACTOR_OUTPUT_SCHEMA = pl.Schema(_FACTOR_OUTPUT_COLUMNS)


@dataclass(frozen=True, slots=True)
class FactorSpec:
    """定义可持久化并参与身份计算的因子不可变规格。

    入参：
        factor_id：用于持久化关联和日志追踪的因子标识。
        frequency：调仓频率。
        lookback_sessions：回看窗口交易会话集合。
        dependencies：参与本次处理的依赖因子；调用方不得依赖未声明的顺序。
        direction：因子方向。
        parameters：参与本次处理的因子参数；调用方不得依赖未声明的顺序。
        required_datasets：因子显式声明的 Canonical 数据依赖。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Stable logical identity and computation controls for one factor.
    """

    factor_id: str
    frequency: str
    lookback_sessions: int
    dependencies: tuple[str, ...]
    direction: int
    parameters: Mapping[str, JsonValue]
    required_datasets: tuple[DatasetKind, ...] = ()

    def __post_init__(self) -> None:
        _BaseSupport._validate_identifier(self.factor_id, "factor_id")
        _BaseSupport._validate_identifier(self.frequency, "frequency")
        if type(self.lookback_sessions) is not int or self.lookback_sessions < 0:
            raise ValueError("lookback_sessions must be a nonnegative integer")
        if type(self.direction) is not int or self.direction not in {-1, 1}:
            raise ValueError("direction must be -1 or +1")
        if not isinstance(self.dependencies, tuple):
            raise TypeError("dependencies must be a tuple")
        dependencies = tuple(
            canonical_factor_ref(dependency) for dependency in self.dependencies
        )
        if len(set(dependencies)) != len(dependencies):
            raise ValueError("duplicate factor dependency")
        if not isinstance(self.parameters, Mapping):
            raise TypeError("parameters must be a mapping")
        if not isinstance(self.required_datasets, tuple) or any(
            not isinstance(dataset, DatasetKind) for dataset in self.required_datasets
        ):
            raise TypeError("required_datasets must be a tuple of DatasetKind")
        required_datasets = tuple(
            sorted(set(self.required_datasets), key=lambda dataset: dataset.value)
        )
        if len(required_datasets) != len(self.required_datasets):
            raise ValueError("duplicate required dataset")
        canonical_json_bytes(cast(JsonValue, self.parameters))
        frozen = _BaseSupport._freeze_json(self.parameters)
        if not isinstance(frozen, Mapping):
            raise TypeError("parameters must be a mapping")
        object.__setattr__(self, "dependencies", dependencies)
        object.__setattr__(self, "parameters", cast(Mapping[str, JsonValue], frozen))
        object.__setattr__(self, "required_datasets", required_datasets)

    @property
    def canonical_ref(self) -> str:
        """输出规范形式的``ref``。

        入参：
            无。
        返回值：
            返回``ref``（``str``）。
        异常：
            无。
        Return the unique factor identifier.
        """
        return self.factor_id


@dataclass(frozen=True, slots=True)
class FactorContext:
    """表示因子计算流程中的因子运行上下文及其业务不变量。

    入参：
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        universe_hash：本次运行使用的 PIT 股票池内容身份。
        start：处理区间的开始日期，类型为 ``date``。
        end：处理区间的结束日期，类型为 ``date``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Exact immutable point-in-time scope supplied to a factor computation.
    """

    data_hash: str
    universe_hash: str
    start: date
    end: date

    def __post_init__(self) -> None:
        validate_sha256(self.data_hash, "data_hash")
        validate_sha256(self.universe_hash, "universe_hash")
        if type(self.start) is not date:
            raise TypeError("start must be a date")
        if type(self.end) is not date:
            raise TypeError("end must be a date")
        if self.start > self.end:
            raise ValueError("start must not follow end")


@dataclass(frozen=True, slots=True)
class FactorArtifact:
    """绑定单个因子的不可变 Arrow 结果、内容哈希和 PIT 输入身份。

    入参：
        factor_ref：因子引用。
        content_hash：按规范字节计算、用于内容寻址和完整性校验的 SHA-256。
        row_count：产物或分区中经验证的数据行数。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        universe_hash：本次运行使用的 PIT 股票池内容身份。
        start：处理区间的开始日期，类型为 ``date``。
        end：处理区间的结束日期，类型为 ``date``。
        table：``table``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    One verified immutable in-memory factor result and its PIT binding.
    """

    factor_ref: str
    content_hash: str
    row_count: int
    data_hash: str
    universe_hash: str
    start: date
    end: date
    table: pa.Table

    def __post_init__(self) -> None:
        if not isinstance(self.table, pa.Table):
            raise TypeError("artifact table must be a pyarrow Table")
        table, actual_hash = _BaseSupport._owned_read_only_table(self.table)
        self._validate_owned_table(table, actual_hash)
        object.__setattr__(self, "table", table)

    @classmethod
    def _from_unhashed_table(
        cls,
        *,
        factor_ref: str,
        data_hash: str,
        universe_hash: str,
        start: date,
        end: date,
        table: pa.Table,
    ) -> Self:
        """Own and hash newly materialized content in one canonical pass."""
        if not isinstance(table, pa.Table):
            raise TypeError("artifact table must be a pyarrow Table")
        owned, content_hash = _BaseSupport._owned_read_only_table(table)
        artifact = object.__new__(cls)
        object.__setattr__(artifact, "factor_ref", factor_ref)
        object.__setattr__(artifact, "content_hash", content_hash)
        object.__setattr__(artifact, "row_count", owned.num_rows)
        object.__setattr__(artifact, "data_hash", data_hash)
        object.__setattr__(artifact, "universe_hash", universe_hash)
        object.__setattr__(artifact, "start", start)
        object.__setattr__(artifact, "end", end)
        object.__setattr__(artifact, "table", owned)
        artifact._validate_owned_table(owned, content_hash)
        return artifact

    def _validate_owned_table(self, table: pa.Table, actual_hash: str) -> None:
        canonical_factor_ref(self.factor_ref)
        validate_sha256(self.content_hash, "content_hash")
        validate_sha256(self.universe_hash, "universe_hash")
        if type(self.row_count) is not int or self.row_count < 0:
            raise ValueError("row_count must be a nonnegative integer")
        if table.num_rows != self.row_count:
            raise ValueError("artifact table row count does not match metadata")
        frame = cast(pl.DataFrame, pl.from_arrow(table))
        if frame.schema != FACTOR_OUTPUT_SCHEMA:
            raise ValueError("artifact table schema is invalid")
        if actual_hash != self.content_hash:
            raise ValueError("artifact table content hash does not match metadata")
        validate_sha256(self.data_hash, "data_hash")
        if type(self.start) is not date or type(self.end) is not date:
            raise TypeError("artifact start and end must be dates")
        if self.start > self.end:
            raise ValueError("artifact start must not follow end")
        validate_factor_output(
            frame,
            factor_id=self.factor_ref,
            start=self.start,
            end=self.end,
        )

    def lazy_frame(self) -> pl.LazyFrame:
        """处理因子计算中的``lazy``数据表。

        入参：
            无。
        返回值：
            返回``frame``（``pl.LazyFrame``）。
        异常：
            无。
        Return a lazy view over this artifact's owned immutable Arrow content.
        """
        return cast(pl.DataFrame, pl.from_arrow(self.table)).lazy()


class Factor(Protocol):
    """定义 ``Factor`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        由具体实现按接口契约定义。
    A factor implementation with injected data dependencies.
    """

    @property
    def spec(self) -> FactorSpec:
        """处理因子计算中的不可变规格。

        入参：
            无。
        返回值：
            返回不可变规格（``FactorSpec``）。
        异常：
            无。
        Return this implementation's immutable logical contract.
        """

    def compute(self, ctx: FactorContext) -> pl.LazyFrame:
        """计算因子计算。

        入参：
            ctx：本次计算的上下文，类型为 ``FactorContext``。
        返回值：
            返回计算因子计算后的``compute``（``pl.LazyFrame``）。
        异常：
            无。
        Compute exact-schema observations for the supplied PIT scope.
        """


def canonical_factor_ref(value: str) -> str:
    """输出规范形式的因子``ref``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        value：待校验或转换的值，类型为 ``str``。
    返回值：
        返回因子``ref``（``str``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Validate and return a unique factor identifier.
    """
    if not isinstance(value, str):
        raise TypeError("factor reference must be a string")
    if "@" in value:
        raise ValueError("factor reference must be a factor_id")
    _BaseSupport._validate_identifier(value, "factor_id")
    return value


def validate_sha256(value: str, field: str) -> str:
    """校验SHA-256 摘要；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        value：待校验或转换的值，类型为 ``str``。
        field：字段。
    返回值：
        返回校验SHA-256 摘要；该函数作为稳定公开 API 或框架入口保留在模块级后的SHA-256 摘要（``str``）。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Validate a lowercase hexadecimal SHA-256 digest.
    """
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field} must be a 64-character lowercase SHA-256 hash")
    return value


def is_available_on_signal_day(value: datetime | None, signal_day: date) -> bool:
    """判断可见时间``on``信号日期``day``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        value：待校验或转换的值，类型为 ``datetime | None``。
        signal_day：信号日期``day``。
    返回值：
        返回是否可见时间``on``信号日期``day``；该函数作为稳定公开 API 或框架入口保留在模块级。
    异常：
        无。
    Return whether an aware timestamp is visible by Shanghai's signal-day end.
    """
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() is not None
        and value.astimezone(_SHANGHAI).date() <= signal_day
    )


def validate_factor_output_scope(
    frame: pl.DataFrame, *, start: date, end: date
) -> None:
    """校验因子输出范围；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        frame：待处理的数据帧，类型为 ``pl.DataFrame``。
        start：处理区间的开始日期，类型为 ``date``。
        end：处理区间的结束日期，类型为 ``date``。
    返回值：
        无。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Reject output rows outside the complete artifact PIT contract.
    """
    required_non_null = (
        "trade_date",
        "instrument_id",
        "factor_id",
        "is_valid",
    )
    if any(frame.select(required_non_null).null_count().row(0)):
        raise ValueError("factor output identity and audit fields must not be null")
    date_range, future_availability = _BaseSupport._factor_scope_violation_expressions(
        start, end
    )
    invalid_value = (
        pl.col("value").is_null()
        | pl.col("value").is_nan()
        | pl.col("value").is_infinite()
    )
    checks = frame.select(
        date_range,
        future_availability,
        (pl.col("is_valid") & invalid_value).any().alias("valid_value"),
        (pl.col("is_valid") & pl.col("available_at").is_null())
        .any()
        .alias("valid_availability"),
        (
            ~pl.col("is_valid")
            & pl.col("value").is_not_null()
            & pl.col("available_at").is_null()
        )
        .any()
        .alias("invalid_availability"),
    ).row(0, named=True)
    if checks["valid_value"]:
        raise ValueError("valid factor output value must be finite")
    if checks["valid_availability"]:
        raise ValueError("valid factor output available_at must not be null")
    if checks["invalid_availability"]:
        raise ValueError(
            "null available_at is allowed only for a null invalid factor value"
        )
    if checks["date_range"]:
        raise ValueError("factor output trade_date is outside context range")
    if checks["future_availability"]:
        raise ValueError(
            "valid factor output available_at is after the Shanghai signal-day end"
        )


def validate_factor_output(
    frame: pl.DataFrame,
    *,
    factor_id: str,
    start: date,
    end: date,
) -> None:
    """校验因子``output``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        frame：待处理的数据帧，类型为 ``pl.DataFrame``。
        factor_id：用于持久化关联和日志追踪的因子标识。
        start：处理区间的开始日期，类型为 ``date``。
        end：处理区间的结束日期，类型为 ``date``。
    返回值：
        无。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Validate factor identity, primary keys, values, and PIT scope.
    """
    canonical_factor_ref(factor_id)
    validate_factor_output_scope(frame, start=start, end=end)
    if frame.select(
        pl.struct("trade_date", "instrument_id", "factor_id").is_duplicated().any()
    ).item():
        raise ValueError("factor output contains a duplicate primary key")
    if frame.filter(pl.col("factor_id") != factor_id).height:
        raise ValueError("factor output factor_id does not match FactorSpec")


class _BaseSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _factor_scope_violation_expressions(
        start: date, end: date
    ) -> tuple[pl.Expr, pl.Expr]:
        """Build native Polars checks shared by eager and streaming validators."""
        out_of_range = (
            (pl.col("trade_date") < pl.lit(start))
            | (pl.col("trade_date") > pl.lit(end))
        ).any()
        after_shanghai_day = pl.col("available_at").dt.convert_time_zone(
            "Asia/Shanghai"
        ).dt.date() > pl.col("trade_date")
        future_availability = (pl.col("is_valid") & after_shanghai_day).any()
        return (
            out_of_range.alias("date_range"),
            future_availability.alias("future_availability"),
        )

    @staticmethod
    def _factor_table_ipc_bytes(table: pa.Table) -> bytes:
        combined = table.combine_chunks()
        # Arrow does not define bytes beneath null validity bits.  Parquet is free to
        # rewrite those invisible bytes, so replace just those physical values with a
        # fixed Arrow scalar while retaining the original null bitmap.  This stays in
        # Arrow instead of allocating one Python object per research-scale table cell.
        canonical = pa.table(
            [
                _BaseSupport._canonical_null_buffers(
                    combined.column(index).combine_chunks(), field
                )
                for index, field in enumerate(combined.schema)
            ],
            schema=combined.schema,
        )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, canonical.schema) as writer:
            writer.write_table(canonical)
        return cast(bytes, sink.getvalue().to_pybytes())

    @staticmethod
    def _canonical_null_buffers(
        array: pa.Array,
        field: pa.Field,
        parent_validity: pa.Array | None = None,
    ) -> pa.Array:
        """Rebuild logical Arrow values without allocator-dependent null payloads."""
        data_type = field.type
        if pa.types.is_dictionary(data_type):
            decoded = pc.dictionary_decode(array)
            if parent_validity is not None:
                visible = pc.and_(array.is_valid(), parent_validity)
                values = pc.drop_null(pc.filter(decoded, visible))
                if len(values) == 0:
                    return _BaseSupport._empty_dictionary_array(
                        array, data_type, parent_validity
                    )
                decoded = pc.if_else(parent_validity, decoded, values[0])
            canonical = _BaseSupport._canonical_null_buffers(
                decoded, pa.field(field.name, data_type.value_type)
            )
            return pc.cast(canonical, data_type)
        if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
            filled = _BaseSupport._filled_nulls(array, data_type)
            values = _BaseSupport._canonical_null_buffers(
                filled.values, pa.field("item", data_type.value_type)
            )
            return pa.Array.from_buffers(
                data_type,
                len(array),
                [array.buffers()[0], filled.buffers()[1]],
                null_count=array.null_count,
                children=[values],
            )
        if pa.types.is_fixed_size_list(data_type):
            values = array.values.slice(
                array.offset * data_type.list_size, len(array) * data_type.list_size
            )
            own_validity = array.is_valid()
            own_value_validity = pa.array(
                np.repeat(
                    own_validity.to_numpy(zero_copy_only=False),
                    data_type.list_size,
                )
            )
            visible_values = pc.if_else(
                own_value_validity, values, pa.scalar(None, type=data_type.value_type)
            )
            item_parent_validity: pa.Array | None = None
            if parent_validity is not None:
                item_parent_validity = pa.array(
                    np.repeat(
                        parent_validity.to_numpy(zero_copy_only=False),
                        data_type.list_size,
                    )
                )
            values = _BaseSupport._canonical_null_buffers(
                visible_values,
                pa.field("item", data_type.value_type),
                item_parent_validity,
            )
            return pa.Array.from_buffers(
                data_type,
                len(array),
                [array.buffers()[0]],
                null_count=array.null_count,
                children=[values],
            )
        if pa.types.is_map(data_type):
            filled = _BaseSupport._filled_nulls(array, data_type)
            entries = _BaseSupport._canonical_null_buffers(
                filled.values, pa.field("entries", filled.values.type, nullable=False)
            )
            return pa.Array.from_buffers(
                data_type,
                len(array),
                [array.buffers()[0], filled.buffers()[1]],
                null_count=array.null_count,
                children=[entries],
            )
        if pa.types.is_struct(data_type):
            filled = _BaseSupport._filled_nulls(array, data_type)
            visible = array.is_valid()
            if parent_validity is not None:
                visible = pc.and_(visible, parent_validity)
            children = [
                _BaseSupport._canonical_null_buffers(
                    filled.field(index), child, visible
                )
                for index, child in enumerate(data_type)
            ]
            return pa.Array.from_buffers(
                data_type,
                len(array),
                [array.buffers()[0]],
                null_count=array.null_count,
                children=children,
            )
        if array.null_count == 0:
            if parent_validity is None:
                return array
            return pc.if_else(
                parent_validity,
                array,
                _BaseSupport._null_fill_scalar(data_type, array, parent_validity),
            )
        filled = _BaseSupport._filled_nulls(array, data_type)
        if parent_validity is not None:
            filled = pc.if_else(
                parent_validity,
                filled,
                _BaseSupport._null_fill_scalar(data_type, array, parent_validity),
            )
        return pa.Array.from_buffers(
            data_type,
            len(array),
            [array.buffers()[0], *filled.buffers()[1:]],
            null_count=array.null_count,
        )

    @staticmethod
    def _filled_nulls(array: pa.Array, data_type: pa.DataType) -> pa.Array:
        """Use one schema-sized scalar to make invisible null payloads deterministic."""
        return pc.fill_null(array, _BaseSupport._null_fill_scalar(data_type, array))

    @staticmethod
    def _null_fill_scalar(
        data_type: pa.DataType,
        array: pa.Array | None = None,
        parent_validity: pa.Array | None = None,
    ) -> pa.Scalar:
        if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
            value: object = ""
        elif pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
            value = b""
        elif pa.types.is_fixed_size_binary(data_type):
            value = b"\0" * data_type.byte_width
        elif pa.types.is_boolean(data_type):
            value = False
        elif pa.types.is_floating(data_type):
            value = 0.0
        elif pa.types.is_integer(data_type):
            value = 0
        elif pa.types.is_decimal(data_type):
            value = Decimal(0)
        elif pa.types.is_date(data_type):
            value = date(1970, 1, 1)
        elif pa.types.is_timestamp(data_type):
            value = datetime(1970, 1, 1, tzinfo=UTC)
        elif pa.types.is_time(data_type):
            value = time()
        elif pa.types.is_duration(data_type):
            value = timedelta()
        elif pa.types.is_dictionary(data_type):
            value = _BaseSupport._dictionary_fill_value(
                array, data_type.value_type, parent_validity
            )
        elif pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
            value = []
        elif pa.types.is_fixed_size_list(data_type):
            item_array: pa.Array | None = None
            item_validity: pa.Array | None = None
            if isinstance(array, pa.FixedSizeListArray):
                item_array = array.values.slice(
                    array.offset * data_type.list_size,
                    len(array) * data_type.list_size,
                )
                visible = array.is_valid()
                if parent_validity is not None:
                    visible = pc.and_(visible, parent_validity)
                item_validity = pa.array(
                    np.repeat(
                        visible.to_numpy(zero_copy_only=False),
                        data_type.list_size,
                    )
                )
            value = [
                _BaseSupport._null_fill_scalar(
                    data_type.value_type,
                    item_array,
                    item_validity,
                ).as_py()
                for _ in range(data_type.list_size)
            ]
        elif pa.types.is_map(data_type):
            value = []
        elif pa.types.is_struct(data_type):
            visible = array.is_valid() if isinstance(array, pa.StructArray) else None
            if visible is not None and parent_validity is not None:
                visible = pc.and_(visible, parent_validity)
            value = {
                child.name: _BaseSupport._null_fill_scalar(
                    child.type,
                    array.field(index) if isinstance(array, pa.StructArray) else None,
                    visible,
                ).as_py()
                for index, child in enumerate(data_type)
            }
        else:
            raise TypeError(f"unsupported factor hash dtype: {data_type}")
        return pa.scalar(value, type=data_type)

    @staticmethod
    def _dictionary_fill_value(
        array: pa.Array | None,
        value_type: pa.DataType,
        parent_validity: pa.Array | None,
    ) -> object:
        """Reuse an existing dictionary value so hidden struct rows add no category."""
        if isinstance(array, pa.DictionaryArray):
            decoded = pc.dictionary_decode(array)
            if parent_validity is not None:
                decoded = pc.filter(decoded, parent_validity)
            visible = pc.drop_null(decoded)
            if len(visible):
                return visible[0].as_py()
            return None
        return _BaseSupport._null_fill_scalar(value_type).as_py()

    @staticmethod
    def _empty_dictionary_array(
        array: pa.Array,
        data_type: pa.DataType,
        parent_validity: pa.Array,
    ) -> pa.DictionaryArray:
        """Match Arrow's empty dictionary encoding for ancestor-hidden values."""
        validity = pc.or_(pc.invert(parent_validity), array.is_valid())
        valid_count = int(pc.sum(pc.cast(validity, pa.int64())).as_py())
        indices = pc.multiply(
            pc.fill_null(array.indices, pa.scalar(0, type=data_type.index_type)),
            pa.scalar(0, type=data_type.index_type),
        )
        return pa.DictionaryArray.from_buffers(
            data_type,
            len(array),
            [
                None if valid_count == len(array) else validity.buffers()[1],
                indices.buffers()[1],
            ],
            pa.array([], type=data_type.value_type),
            null_count=len(array) - valid_count,
        )

    @staticmethod
    def _owned_read_only_table(table: pa.Table) -> tuple[pa.Table, str]:
        payload = _BaseSupport._factor_table_ipc_bytes(table)
        with pa.ipc.open_stream(pa.py_buffer(payload)) as reader:
            owned = reader.read_all()
        return owned, hashlib.sha256(payload).hexdigest()

    @staticmethod
    def _freeze_json(value: object) -> FrozenJsonValue:
        if isinstance(value, Mapping):
            frozen: dict[str, FrozenJsonValue] = {}
            for key, item in value.items():
                if not isinstance(key, str):
                    raise TypeError("JSON mapping keys must be strings")
                frozen[key] = _BaseSupport._freeze_json(item)
            return MappingProxyType(dict(sorted(frozen.items())))
        if isinstance(value, (list, tuple)):
            return tuple(_BaseSupport._freeze_json(item) for item in value)
        if value is None or isinstance(value, (str, bool)):
            return value
        if type(value) is int:
            return value
        if type(value) is float and isfinite(value):
            return value
        if type(value) is float:
            raise ValueError("value must be JSON serializable")
        raise TypeError(f"unsupported JSON value: {type(value).__name__}")

    @staticmethod
    def _validate_identifier(value: str, field: str) -> None:
        if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
            raise ValueError(f"{field} must be a nonempty identifier")


def factor_table_content_hash(table: pa.Table) -> str:
    """读取因子``table``内容哈希；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        table：``table``。
    返回值：
        返回``table``内容哈希（``str``）。
    异常：
        无。
    Hash factor content after canonical Arrow chunk normalization.
    """
    return hashlib.sha256(_BaseSupport._factor_table_ipc_bytes(table)).hexdigest()


def thaw_json(value: object) -> JsonValue:
    """处理因子计算中的解冻JSON；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        value：待校验或转换的值，类型为 ``object``。
    返回值：
        返回``json``（``JsonValue``）。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Copy immutable factor parameters into canonical JSON containers.
    """
    if isinstance(value, Mapping):
        return {str(key): thaw_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [thaw_json(item) for item in value]
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if type(value) is float and isfinite(value):
        return value
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")
