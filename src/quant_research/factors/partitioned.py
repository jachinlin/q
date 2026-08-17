"""提供因子与分区计算相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from types import MappingProxyType
from typing import Protocol

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import (
    FactorArtifact,
    FactorContext,
    canonical_factor_ref,
    validate_sha256,
)
from quant_research.factors.execution import FactorExecutionDescriptor
from quant_research.factors.registry import FactorEngine


class PartitionEngineFactory(Protocol):
    """定义 ``PartitionEngineFactory`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        由具体实现按接口契约定义。
    Build one factor engine bound to an exact instrument partition.
    """

    def __call__(self, instruments: tuple[InstrumentId, ...]) -> FactorEngine:
        """以可调用对象形式执行公开协议。

        入参：
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
        返回值：
            返回``call``（``FactorEngine``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...


@dataclass(frozen=True, slots=True)
class FactorPartition:
    """绑定一个有界证券分区及其中刚计算完成的因子产物。

    入参：
        index：索引。
        universe_hash：本次运行使用的 PIT 股票池内容身份。
        instrument_ids：参与本次处理的证券``ids``；调用方不得依赖未声明的顺序。
        artifacts：参与本次处理的产物集合；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    One bounded instrument scope and its freshly computed factor results.
    """

    index: int
    universe_hash: str
    instrument_ids: tuple[str, ...]
    artifacts: Mapping[str, FactorArtifact]

    def __post_init__(self) -> None:
        if type(self.index) is not int or self.index < 0:
            raise ValueError("partition index must be nonnegative")
        validate_sha256(self.universe_hash, "partition universe_hash")
        canonical_ids = tuple(
            InstrumentId.parse(value).canonical() for value in self.instrument_ids
        )
        if canonical_ids != tuple(sorted(set(canonical_ids))):
            raise ValueError("partition instruments must be unique and ordered")
        artifacts = dict(self.artifacts)
        if tuple(artifacts) != tuple(sorted(artifacts)):
            raise ValueError("partition factor artifacts must be ordered")
        for reference, artifact in artifacts.items():
            if canonical_factor_ref(reference) != artifact.factor_ref:
                raise ValueError("partition factor artifact identity differs")
            if artifact.universe_hash != self.universe_hash:
                raise ValueError("partition factor artifact universe differs")
        object.__setattr__(self, "artifacts", MappingProxyType(artifacts))

    @property
    def row_count(self) -> int:
        """处理因子计算中的数据行``count``。

        入参：
            无。
        返回值：
            返回``count``（``int``）。
        异常：
            无。
        """
        return sum(artifact.row_count for artifact in self.artifacts.values())


@dataclass(frozen=True, slots=True)
class PartitionedFactorResult:
    """记录一次因子计算操作的结果、业务指标和审计身份。

    入参：
        execution_descriptor：本次因子运行完整依赖 DAG 及实现身份的规范 JSON 描述。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        universe_hash：本次运行使用的 PIT 股票池内容身份。
        start：处理区间的开始日期，类型为 ``date``。
        end：处理区间的结束日期，类型为 ``date``。
        factor_refs：按规范 ``factor_id`` 指定的因子引用集合。
        instrument_ids：参与本次处理的证券``ids``；调用方不得依赖未声明的顺序。
        max_partition_size：限制资源使用、数量或等待时间的上限分区字节数。
        partitions：参与本次处理的分区集合；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Fresh in-memory factor results for one complete experiment scope.
    """

    execution_descriptor: FactorExecutionDescriptor
    data_hash: str
    universe_hash: str
    start: date
    end: date
    factor_refs: tuple[str, ...]
    instrument_ids: tuple[str, ...]
    max_partition_size: int
    partitions: tuple[FactorPartition, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.execution_descriptor, FactorExecutionDescriptor):
            raise TypeError("execution_descriptor must be a FactorExecutionDescriptor")
        validate_sha256(self.data_hash, "data_hash")
        validate_sha256(self.universe_hash, "universe_hash")
        if type(self.start) is not date or type(self.end) is not date:
            raise TypeError("factor result dates must be dates")
        if self.start > self.end:
            raise ValueError("factor result start must not follow end")
        refs = tuple(canonical_factor_ref(value) for value in self.factor_refs)
        if refs != tuple(sorted(set(refs))):
            raise ValueError("factor refs must be unique and ordered")
        ids = tuple(
            InstrumentId.parse(value).canonical() for value in self.instrument_ids
        )
        if ids != tuple(sorted(set(ids))):
            raise ValueError("instrument IDs must be unique and ordered")
        _PartitionedSupport._partition_size(
            self.max_partition_size, self.max_partition_size
        )
        flattened = tuple(
            instrument_id
            for partition in self.partitions
            for instrument_id in partition.instrument_ids
        )
        if flattened != ids:
            raise ValueError("factor partitions do not cover the instrument scope")
        for expected_index, partition in enumerate(self.partitions):
            if partition.index != expected_index:
                raise ValueError("factor partition indices are not contiguous")
            if len(partition.instrument_ids) > self.max_partition_size:
                raise ValueError("factor partition exceeds configured maximum")
            if tuple(partition.artifacts) != refs:
                raise ValueError("factor partition coverage differs")

    @property
    def row_count(self) -> int:
        """处理因子计算中的数据行``count``。

        入参：
            无。
        返回值：
            返回``count``（``int``）。
        异常：
            无。
        """
        return sum(partition.row_count for partition in self.partitions)


class PartitionedFactorEngine:
    """协调因子计算计算所需的输入、规则和输出校验。

    入参：
        engine_factory：引擎工厂。
        max_partition_size：限制资源使用、数量或等待时间的上限分区字节数。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Recompute factors over deterministic bounded instrument partitions.
    """

    def __init__(
        self,
        engine_factory: PartitionEngineFactory,
        *,
        max_partition_size: int,
    ) -> None:
        if not callable(engine_factory):
            raise TypeError("engine_factory must be callable")
        _PartitionedSupport._partition_size(max_partition_size, max_partition_size)
        self._engine_factory = engine_factory
        self._max_partition_size = max_partition_size

    @property
    def max_partition_size(self) -> int:
        """处理因子计算中的上限分区字节数。

        入参：
            无。
        返回值：
            返回分区字节数（``int``）。
        异常：
            无。
        """
        return self._max_partition_size

    def compute(
        self,
        factor_ids: Sequence[str],
        instruments: Sequence[InstrumentId],
        ctx: FactorContext,
        *,
        partition_size: int | None = None,
    ) -> PartitionedFactorResult:
        """计算因子计算。

        入参：
            factor_ids：参与本次处理的因子``ids``；调用方不得依赖未声明的顺序。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            ctx：本次计算的上下文，类型为 ``FactorContext``。
            partition_size：分区字节数。
        返回值：
            返回计算因子计算后的``compute``（``PartitionedFactorResult``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        if not isinstance(ctx, FactorContext):
            raise TypeError("ctx must be a FactorContext")
        refs = _PartitionedSupport._factor_refs(factor_ids)
        canonical_instruments = _PartitionedSupport._instruments(instruments)
        size = self._max_partition_size if partition_size is None else partition_size
        _PartitionedSupport._partition_size(size, self._max_partition_size)
        partitions: list[FactorPartition] = []
        descriptor: FactorExecutionDescriptor | None = None
        for index, offset in enumerate(range(0, len(canonical_instruments), size)):
            scope = canonical_instruments[offset : offset + size]
            engine = self._engine_factory(scope)
            current_descriptor = engine.execution_descriptor(refs)
            if descriptor is None:
                descriptor = current_descriptor
            elif current_descriptor != descriptor:
                raise ValueError("partition execution descriptors differ")
            partition_hash = _PartitionedSupport._partition_universe_hash(
                ctx.universe_hash, scope
            )
            partition_ctx = FactorContext(
                ctx.data_hash,
                partition_hash,
                ctx.start,
                ctx.end,
            )
            artifacts = engine.compute(refs, partition_ctx)
            partitions.append(
                FactorPartition(
                    index=index,
                    universe_hash=partition_hash,
                    instrument_ids=tuple(item.canonical() for item in scope),
                    artifacts=artifacts,
                )
            )
        if descriptor is None:
            descriptor = self._engine_factory(()).execution_descriptor(refs)
        return PartitionedFactorResult(
            execution_descriptor=descriptor,
            data_hash=ctx.data_hash,
            universe_hash=ctx.universe_hash,
            start=ctx.start,
            end=ctx.end,
            factor_refs=refs,
            instrument_ids=tuple(item.canonical() for item in canonical_instruments),
            max_partition_size=self._max_partition_size,
            partitions=tuple(partitions),
        )


class _PartitionedSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _factor_refs(values: Sequence[str]) -> tuple[str, ...]:
        if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
            raise TypeError("factor_ids must be a sequence")
        refs = tuple(canonical_factor_ref(value) for value in values)
        if len(set(refs)) != len(refs):
            raise ValueError("factor request contains duplicate identities")
        return tuple(sorted(refs))

    @staticmethod
    def _instruments(values: Sequence[InstrumentId]) -> tuple[InstrumentId, ...]:
        if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
            raise TypeError("instruments must be a sequence")
        by_id: dict[str, InstrumentId] = {}
        for value in values:
            if not isinstance(value, InstrumentId):
                raise TypeError("instruments must contain InstrumentId values")
            identifier = value.canonical()
            if identifier in by_id:
                raise ValueError("instrument scope contains a duplicate identity")
            by_id[identifier] = value
        return tuple(by_id[key] for key in sorted(by_id))

    @staticmethod
    def _partition_size(value: int, maximum: int) -> int:
        if type(value) is not int or value <= 0:
            raise ValueError("partition size must be a positive integer")
        if type(maximum) is not int or maximum <= 0 or value > maximum:
            raise ValueError("partition size exceeds configured maximum")
        return value

    @staticmethod
    def _partition_universe_hash(
        parent_hash: str,
        instruments: tuple[InstrumentId, ...],
    ) -> str:
        import hashlib

        digest = hashlib.sha256()
        digest.update(parent_hash.encode("ascii"))
        for instrument in instruments:
            digest.update(b"\0")
            digest.update(instrument.canonical().encode("ascii"))
        return digest.hexdigest()


__all__ = [
    "FactorPartition",
    "PartitionEngineFactory",
    "PartitionedFactorEngine",
    "PartitionedFactorResult",
]
