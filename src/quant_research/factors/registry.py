"""提供因子与实验注册相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import cast

import polars as pl

from quant_research.data.contracts import ProviderCapabilities
from quant_research.factors.base import (
    FACTOR_OUTPUT_SCHEMA,
    Factor,
    FactorArtifact,
    FactorContext,
    FactorSpec,
    canonical_factor_ref,
    validate_factor_output,
    validate_sha256,
)
from quant_research.factors.execution import (
    FactorExecutionDescriptor,
    FactorExecutionNode,
)


@dataclass(frozen=True, slots=True)
class _Registration:
    factor: Factor
    spec: FactorSpec
    code_hash: str


class FactorCapabilityUnavailable(ValueError):
    """表示 ``FactorCapabilityUnavailable`` 对应的领域异常。

    入参：
        factor_ref：因子引用。
        missing：参与本次处理的缺失项；调用方不得依赖未声明的顺序。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    A requested factor needs source inputs absent from the runtime profile.
    """

    def __init__(self, factor_ref: str, missing: tuple[str, ...]) -> None:
        self.factor_ref = factor_ref
        self.missing = missing
        super().__init__(
            f"factor {factor_ref} requires unavailable capabilities: {', '.join(missing)}"
        )


class FactorRegistry:
    """登记并按稳定身份查询因子计算定义。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``FactorCapabilityUnavailable``、``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    An in-memory catalog keyed only by canonical factor references.
    """

    def __init__(self) -> None:
        self._registrations: dict[str, _Registration] = {}

    def register(self, factor: Factor, *, code_hash: str) -> None:
        """登记因子计算。

        入参：
            factor：因子。
            code_hash：参与幂等、漂移或完整性校验的代码哈希；使用 SHA-256 十六进制文本。
        返回值：
            无。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        Register one logical factor with an auditable implementation digest.
        """
        if not hasattr(factor, "spec") or not isinstance(factor.spec, FactorSpec):
            raise TypeError("factor must expose a FactorSpec")
        spec = factor.spec
        canonical_ref = spec.canonical_ref
        validate_sha256(code_hash, "code_hash")
        if canonical_ref in self._registrations:
            raise ValueError(f"duplicate factor registration: {canonical_ref}")
        self._registrations[canonical_ref] = _Registration(factor, spec, code_hash)

    def resolve(self, reference: str) -> str:
        """解析并返回确定结果。

        入参：
            reference：规范引用。
        返回值：
            返回解析因子计算后的``resolve``（``str``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        Resolve one unique factor ID.
        """
        factor_id = canonical_factor_ref(reference)
        if factor_id not in self._registrations:
            raise ValueError(f"unknown factor: {factor_id}")
        return factor_id

    def factor(self, reference: str) -> Factor:
        """读取因子因子计算。

        入参：
            reference：规范引用。
        返回值：
            返回因子（``Factor``）。
        异常：
            无。
        Return the implementation for an explicit or unambiguous reference.
        """
        return self._registrations[self.resolve(reference)].factor

    def code_hash(self, reference: str) -> str:
        """处理因子计算中的代码哈希。

        入参：
            reference：规范引用。
        返回值：
            返回哈希（``str``）。
        异常：
            无。
        Return the registered implementation SHA-256 for a factor reference.
        """
        return self._registrations[self.resolve(reference)].code_hash

    def registered_references(self) -> tuple[str, ...]:
        """处理因子计算中的``registered``规范引用集合。

        入参：
            无。
        返回值：
            返回规范引用集合（``tuple[str, ...]``）。
        异常：
            无。
        Return every registered canonical reference in deterministic order.
        """
        return tuple(sorted(self._registrations))

    def spec(self, reference: str) -> FactorSpec:
        """处理因子计算中的不可变规格。

        入参：
            reference：规范引用。
        返回值：
            返回不可变规格（``FactorSpec``）。
        异常：
            无。
        Return the immutable contract captured when a factor was registered.
        """
        return self._registrations[self.resolve(reference)].spec

    def topological_order(self, references: tuple[str, ...]) -> tuple[str, ...]:
        """返回``order``（``tuple[str, ...]``）。

        入参：
            references：参与本次处理的规范引用集合；调用方不得依赖未声明的顺序。
        返回值：
            返回``order``（``tuple[str, ...]``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        Return all transitive dependencies before roots in stable lexical order.
        """
        roots = tuple(sorted({self.resolve(reference) for reference in references}))
        ordered: list[str] = []
        completed: set[str] = set()
        active: list[str] = []

        def visit(canonical_ref: str) -> None:
            if canonical_ref in completed:
                return
            if canonical_ref in active:
                cycle_start = active.index(canonical_ref)
                cycle = (*active[cycle_start:], canonical_ref)
                raise ValueError(f"dependency cycle: {' -> '.join(cycle)}")
            registration = self._registrations.get(canonical_ref)
            if registration is None:
                raise ValueError(f"missing dependency {canonical_ref}")
            active.append(canonical_ref)
            for dependency in sorted(registration.spec.dependencies):
                if dependency not in self._registrations:
                    raise ValueError(
                        f"missing dependency {dependency} required by {canonical_ref}"
                    )
                visit(dependency)
            active.pop()
            completed.add(canonical_ref)
            ordered.append(canonical_ref)

        for root in roots:
            visit(root)
        return tuple(ordered)

    def runnable_references(
        self, capabilities: ProviderCapabilities
    ) -> tuple[str, ...]:
        """处理因子计算中的``runnable``规范引用集合。

        入参：
            capabilities：当前数据源确实支持的数据集和字段能力。
        返回值：
            返回规范引用集合（``tuple[str, ...]``）。
        异常：
            无。
        Return factors whose complete dependency closures are available.
        """
        runnable: list[str] = []
        for canonical_ref in sorted(self._registrations):
            try:
                self.preflight((canonical_ref,), capabilities)
            except FactorCapabilityUnavailable:
                continue
            runnable.append(canonical_ref)
        return tuple(runnable)

    def preflight(
        self, references: Sequence[str], capabilities: ProviderCapabilities
    ) -> tuple[str, ...]:
        """在执行前校验因子计算。

        入参：
            references：参与本次处理的规范引用集合；调用方不得依赖未声明的顺序。
            capabilities：当前数据源确实支持的数据集和字段能力。
        返回值：
            返回``preflight``（``tuple[str, ...]``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``FactorCapabilityUnavailable``。
        Resolve a request and reject unavailable data requirements before compute.
        """
        plan = self.topological_order(tuple(references))
        for canonical_ref in plan:
            missing = capabilities.missing(
                _RegistrySupport._required_capabilities(self.spec(canonical_ref))
            )
            if missing:
                raise FactorCapabilityUnavailable(canonical_ref, missing)
        return plan


class FactorEngine:
    """协调因子计算计算所需的输入、规则和输出校验。

    入参：
        registry：登记并查询不可变业务身份和生命周期状态的登记簿。
        capabilities：当前数据源确实支持的数据集和字段能力。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        ``TypeError``、``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Compute a dependency closure into verified in-memory artifacts.
    """

    def __init__(
        self,
        registry: FactorRegistry,
        *,
        capabilities: ProviderCapabilities,
    ) -> None:
        self._registry = registry
        self._capabilities = capabilities

    def runnable_references(self) -> tuple[str, ...]:
        """处理因子计算中的``runnable``规范引用集合。

        入参：
            无。
        返回值：
            返回规范引用集合（``tuple[str, ...]``）。
        异常：
            无。
        List factors runnable under this engine's explicit capability profile.
        """
        return self._registry.runnable_references(self._capabilities)

    def execution_descriptor(
        self, factor_ids: Sequence[str]
    ) -> FactorExecutionDescriptor:
        """处理因子计算中的成交执行执行描述。

        入参：
            factor_ids：参与本次处理的因子``ids``；调用方不得依赖未声明的顺序。
        返回值：
            返回执行描述（``FactorExecutionDescriptor``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        Describe the complete runnable DAG without computing factor data.
        """
        requested = tuple(self._registry.resolve(reference) for reference in factor_ids)
        if len(set(requested)) != len(requested):
            raise ValueError("factor request contains duplicate logical identities")
        ordered_requested = tuple(sorted(requested))
        plan = self._registry.preflight(ordered_requested, self._capabilities)
        return FactorExecutionDescriptor(
            requested_refs=ordered_requested,
            plan=tuple(
                FactorExecutionNode(
                    spec=self._registry.spec(factor_ref),
                    code_hash=self._registry.code_hash(factor_ref),
                )
                for factor_ref in plan
            ),
        )

    def compute(
        self, factor_ids: Sequence[str], ctx: FactorContext
    ) -> Mapping[str, FactorArtifact]:
        """计算因子计算。

        入参：
            factor_ids：参与本次处理的因子``ids``；调用方不得依赖未声明的顺序。
            ctx：本次计算的上下文，类型为 ``FactorContext``。
        返回值：
            返回计算因子计算后的``compute``（``Mapping[str, FactorArtifact]``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        Return requested canonical artifacts after stable dependency evaluation.
        """
        requested = tuple(self._registry.resolve(reference) for reference in factor_ids)
        if len(set(requested)) != len(requested):
            raise ValueError("factor request contains duplicate logical identities")
        plan = self._registry.preflight(requested, self._capabilities)
        computed: dict[str, FactorArtifact] = {}
        for canonical_ref in plan:
            factor = self._registry.factor(canonical_ref)
            spec = self._registry.spec(canonical_ref)
            output = factor.compute(ctx)
            if not isinstance(output, pl.LazyFrame):
                raise TypeError("factor output must be a polars LazyFrame")
            if output.collect_schema() != FACTOR_OUTPUT_SCHEMA:
                raise ValueError(
                    f"factor output schema must be exactly {FACTOR_OUTPUT_SCHEMA}"
                )
            frame = output.sort(
                ["trade_date", "instrument_id", "factor_id"],
                maintain_order=True,
            ).collect(engine="streaming")
            validate_factor_output(
                frame,
                factor_id=spec.factor_id,
                start=ctx.start,
                end=ctx.end,
            )
            artifact = FactorArtifact._from_unhashed_table(
                factor_ref=spec.canonical_ref,
                data_hash=ctx.data_hash,
                universe_hash=ctx.universe_hash,
                start=ctx.start,
                end=ctx.end,
                table=frame.to_arrow(),
            )
            computed[canonical_ref] = artifact
        return MappingProxyType(
            {canonical_ref: computed[canonical_ref] for canonical_ref in requested}
        )


class _RegistrySupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _required_capabilities(spec: FactorSpec) -> tuple[str, ...]:
        value = spec.parameters.get("required_capabilities", ())
        if not isinstance(value, tuple):
            raise TypeError("required_capabilities must be a list of capability names")
        requirements = cast(tuple[object, ...], value)
        if not all(isinstance(item, str) for item in requirements):
            raise ValueError("required_capabilities must be a list of capability names")
        return cast(tuple[str, ...], requirements)
