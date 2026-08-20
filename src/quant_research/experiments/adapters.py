"""提供实验与实验适配相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from math import isfinite
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, cast
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow as pa  # type: ignore[import-untyped]

from quant_research.backtest.calendar import TradingCalendar
from quant_research.backtest.engine import (
    BacktestEngine,
    BacktestRequest,
    BacktestResult,
    BoundMarketSlice,
    CancellationToken,
    ProgressSink,
    StrategyRef,
)
from quant_research.backtest.models import MarketSlice
from quant_research.backtest.rulebook import MarketRuleBook
from quant_research.data.canonical.schemas import PolarsDataType
from quant_research.data.contracts import ProviderCapabilities
from quant_research.data.repository import ResearchDataRepository
from quant_research.domain.identifiers import InstrumentId
from quant_research.experiments.config import require_provider_capabilities
from quant_research.factors.base import (
    FactorArtifact,
    canonical_factor_ref,
    validate_sha256,
)
from quant_research.factors.partitioned import (
    PartitionedFactorResult,
)
from quant_research.portfolio import PortfolioConstructor, RebalancePlanner
from quant_research.strategies.base import (
    Strategy,
    StrategyContext,
    StrategyData,
    StrategyTargetAdapter,
    validated_factor_values,
    validated_stock_universe,
)
from quant_research.universe.builder import UniverseBuilder
from quant_research.universe.rules import UniverseRules

_STRATEGY_UNIVERSE_COLUMNS: dict[str, PolarsDataType] = {
    "instrument_id": pl.String,
    "as_of": pl.Date,
    "eligible": pl.Boolean,
    "reason_codes": cast(pl.DataType, pl.List(pl.String)),
    "adv_amount": pl.Float64,
}
STRATEGY_UNIVERSE_SCHEMA = pl.Schema(_STRATEGY_UNIVERSE_COLUMNS)
_STRATEGY_FACTOR_COLUMNS: dict[str, PolarsDataType] = {
    "trade_date": pl.Date,
    "instrument_id": pl.String,
    "factor_ref": pl.String,
    "value": pl.Float64,
    "available_at": cast(pl.DataType, pl.Datetime("us", "UTC")),
    "is_valid": pl.Boolean,
}
_STRATEGY_FACTOR_SCHEMA = pl.Schema(_STRATEGY_FACTOR_COLUMNS)
_MARKET_SLICE_SCHEMA = pl.Schema(
    {
        "instrument_id": pl.String,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "preclose": pl.Float64,
        "volume": pl.Int64,
        "is_suspended": pl.Boolean,
        "security_status": pl.String,
        "instrument_type": pl.String,
        "board": pl.String,
    }
)
_MARKET_CAPABILITIES = (
    "daily_bars",
    "trade_calendar",
    "instruments",
    "security_status",
)
_STOCK_CAPABILITIES = (
    "daily_bars",
    "trade_calendar",
    "instruments",
    "security_status",
)
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_ONE_DAY = timedelta(days=1)


class FactorValueSource(Protocol):
    """定义 ``FactorValueSource`` 的依赖端口与实现契约。

    入参：
        无。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        由具体实现按接口契约定义。
    Load one bounded strategy factor slice without joining full artifacts.
    """

    @property
    def universe_hash(self) -> str:
        """处理实验中的股票池哈希。

        入参：
            无。
        返回值：
            返回哈希（``str``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    @property
    def data_hash(self) -> str:
        """处理实验中的数据哈希。

        入参：
            无。
        返回值：
            返回哈希（``str``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def values(
        self,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        factor_refs: tuple[str, ...],
    ) -> pl.DataFrame:
        """读取因子数值实验。

        入参：
            signal_date：只允许使用当日收盘前已知信息的策略信号日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            factor_refs：按规范 ``factor_id`` 指定的因子引用集合。
        返回值：
            返回数值表（``pl.DataFrame``）。
        异常：
            由具体实现按接口契约定义。
        """
        ...

    def close(self) -> None:
        """关闭并释放持有的资源。

        入参：
            无。
        返回值：
            无。
        异常：
            由具体实现按接口契约定义。
        """
        ...


class ArtifactMappingFactorValueSource:
    """表示实验流程中的产物配置映射因子值数据来源及其业务不变量。

    入参：
        artifacts：参与本次处理的产物集合；调用方不得依赖未声明的顺序。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        universe_hash：本次运行使用的 PIT 股票池内容身份。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Factor source for already materialized artifact mappings.
    """

    def __init__(
        self,
        artifacts: Mapping[str, FactorArtifact],
        *,
        data_hash: str,
        universe_hash: str,
    ) -> None:
        if not isinstance(artifacts, Mapping):
            raise TypeError("factor_artifacts must be a mapping")
        verified = dict(artifacts)
        for reference, artifact in verified.items():
            if (
                canonical_factor_ref(reference) != reference
                or not isinstance(artifact, FactorArtifact)
                or artifact.factor_ref != reference
            ):
                raise ValueError("factor artifact mapping has an invalid identity")
        self._artifacts = MappingProxyType(verified)
        self._data_hash = validate_sha256(data_hash, "data_hash")
        self._universe_hash = validate_sha256(universe_hash, "universe_hash")

    @property
    def data_hash(self) -> str:
        """处理实验中的数据哈希。

        入参：
            无。
        返回值：
            返回哈希（``str``）。
        异常：
            无。
        """
        return self._data_hash

    @property
    def universe_hash(self) -> str:
        """处理实验中的股票池哈希。

        入参：
            无。
        返回值：
            返回哈希（``str``）。
        异常：
            无。
        """
        return self._universe_hash

    def values(
        self,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        factor_refs: tuple[str, ...],
    ) -> pl.DataFrame:
        """读取因子数值实验。

        入参：
            signal_date：只允许使用当日收盘前已知信息的策略信号日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            factor_refs：按规范 ``factor_id`` 指定的因子引用集合。
        返回值：
            返回数值表（``pl.DataFrame``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        frames: list[pl.DataFrame] = []
        for reference in factor_refs:
            artifact = self._artifacts.get(reference)
            if artifact is None:
                raise ValueError(f"factor artifact is missing: {reference}")
            _AdaptersSupport._validate_factor_artifact(
                artifact,
                reference=reference,
                data_hash=self._data_hash,
                universe_hash=self._universe_hash,
                signal_date=signal_date,
            )
            identity = cast(pl.DataFrame, pl.from_arrow(artifact.table)).select(
                (pl.col("factor_id") != reference).any().alias("differs")
            )
            if identity.item():
                raise ValueError(
                    "factor artifact table identity does not match its ref"
                )
            frames.append(
                _AdaptersSupport._selected_factor_frame(
                    artifact, reference, signal_date, instruments
                )
            )
        return _AdaptersSupport._combined_factor_frames(frames)

    def close(self) -> None:
        """关闭并释放持有的资源。

        入参：
            无。
        返回值：
            无。
        异常：
            无。
        In-memory artifacts have no external resources to release.
        """


class PartitionedFactorValueSource:
    """从本次运行刚计算的内存分区读取策略所需因子值。

    入参：
        result：结果。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Read freshly computed in-memory partition results for strategy slices.
    """

    def __init__(self, result: PartitionedFactorResult) -> None:
        if not isinstance(result, PartitionedFactorResult):
            raise TypeError("result must be a PartitionedFactorResult")
        self._result = result

    @property
    def result(self) -> PartitionedFactorResult:
        """处理实验中的结果。

        入参：
            无。
        返回值：
            返回结果（``PartitionedFactorResult``）。
        异常：
            无。
        """
        return self._result

    @property
    def data_hash(self) -> str:
        """处理实验中的数据哈希。

        入参：
            无。
        返回值：
            返回哈希（``str``）。
        异常：
            无。
        """
        return self._result.data_hash

    @property
    def universe_hash(self) -> str:
        """处理实验中的股票池哈希。

        入参：
            无。
        返回值：
            返回哈希（``str``）。
        异常：
            无。
        """
        return self._result.universe_hash

    def values(
        self,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        factor_refs: tuple[str, ...],
    ) -> pl.DataFrame:
        """读取因子数值实验。

        入参：
            signal_date：只允许使用当日收盘前已知信息的策略信号日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            factor_refs：按规范 ``factor_id`` 指定的因子引用集合。
        返回值：
            返回数值表（``pl.DataFrame``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        result = self._result
        if signal_date < result.start or signal_date > result.end:
            raise ValueError("factor result does not cover requested date range")
        if not set(factor_refs).issubset(result.factor_refs):
            raise ValueError("factor result is missing a requested factor")
        requested_ids = (
            set(result.instrument_ids)
            if instruments is None
            else {item.canonical() for item in instruments}
        )
        if not requested_ids.issubset(result.instrument_ids):
            raise ValueError("requested instruments exceed factor result scope")

        frames: list[pl.DataFrame] = []
        for partition in result.partitions:
            partition_ids = set(partition.instrument_ids)
            selected_ids = tuple(sorted(requested_ids.intersection(partition_ids)))
            if not selected_ids:
                continue
            if len(partition.instrument_ids) > result.max_partition_size:
                raise ValueError("factor partition exceeds configured maximum")
            selected_instruments = tuple(
                InstrumentId.parse(item) for item in selected_ids
            )
            for reference in factor_refs:
                artifact = partition.artifacts[reference]
                _AdaptersSupport._validate_factor_artifact(
                    artifact,
                    reference=reference,
                    data_hash=result.data_hash,
                    universe_hash=partition.universe_hash,
                    signal_date=signal_date,
                )
                frames.append(
                    _AdaptersSupport._selected_factor_frame(
                        artifact, reference, signal_date, selected_instruments
                    )
                )
        return _AdaptersSupport._combined_factor_frames(frames)

    def close(self) -> None:
        """关闭并释放持有的资源。

        入参：
            无。
        返回值：
            无。
        异常：
            无。
        In-memory partition results have no external resources to release.
        """


class CanonicalBacktestMarketData:
    """把已验证 Canonical 数据适配为回测引擎的交易日与行情端口。

    入参：
        repository：提供持久化访问的仓储，类型为 ``ResearchDataRepository``。
        benchmark：基准。
        capabilities：当前数据源确实支持的数据集和字段能力。
        provider：数据供应商。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Adapt current canonical data to the backtest market-data boundary.
    """

    def __init__(
        self,
        *,
        repository: ResearchDataRepository,
        benchmark: InstrumentId,
        capabilities: ProviderCapabilities,
        provider: str,
    ) -> None:
        if repository is None:
            raise TypeError("repository must be supplied")
        if not isinstance(benchmark, InstrumentId):
            raise TypeError("benchmark must be an InstrumentId")
        if not isinstance(capabilities, ProviderCapabilities):
            raise TypeError("capabilities must be ProviderCapabilities")
        self._repository = repository
        self._benchmark = benchmark
        self._capabilities = capabilities
        self._provider = _AdaptersSupport._nonempty_text(provider, "provider")

    def preflight(self) -> None:
        """在执行前校验实验。

        入参：
            无。
        返回值：
            无。
        异常：
            无。
        Reject an incomplete full-backtest provider without reading or writing.
        """
        self._require_capabilities(_MARKET_CAPABILITIES, stage="VALIDATE")

    def calendar(
        self,
        start: date,
        end: date,
        *,
        include_next_session: bool,
    ) -> TradingCalendar:
        """处理实验中的交易日历。

        入参：
            start：处理区间的开始日期，类型为 ``date``。
            end：处理区间的结束日期，类型为 ``date``。
            include_next_session：控制是否启用包含范围``next``交易会话规则的布尔开关。
        返回值：
            返回交易日历（``TradingCalendar``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        _AdaptersSupport._date_range(start, end)
        if type(include_next_session) is not bool:
            raise TypeError("include_next_session must be a bool")
        self._require_capabilities(("trade_calendar",), stage="VALIDATE")
        loaded_end = end
        if include_next_session:
            if end == date.max:
                raise ValueError("no later trading session can follow date.max")
            later = self._repository.trade_calendar(end + _ONE_DAY, date.max).collect()
            loaded_end = _AdaptersSupport._first_later_session(later, end)
        calendar = TradingCalendar.load(self._repository, start, loaded_end)
        if include_next_session and calendar.next_session(end) != loaded_end:
            raise ValueError("calendar did not load the first later trading session")
        return calendar

    def market_slice(self, trade_date: date) -> BoundMarketSlice:
        """处理实验中的市场数据``slice``。

        入参：
            trade_date：目标交易日期，类型为 ``date``。
        返回值：
            返回``slice``（``BoundMarketSlice``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        _AdaptersSupport._strict_date(trade_date, "trade_date")
        self._require_capabilities(
            ("daily_bars", "instruments", "security_status"),
            stage="BACKTEST",
        )
        instrument_frame = self._repository.instruments().collect()
        identifiers = _AdaptersSupport._instrument_scope(
            instrument_frame, "market slice instruments"
        )
        instrument_rows = _AdaptersSupport._instrument_metadata(instrument_frame)
        benchmark_is_index = (
            instrument_rows[self._benchmark.canonical()]["instrument_type"] == "INDEX"
        )
        bars = self._repository.bars(identifiers, trade_date, trade_date).collect()
        bar_rows = _AdaptersSupport._market_bar_rows(bars, identifiers, trade_date)
        statuses = self._repository.security_status(trade_date, identifiers).collect()
        status_rows = _AdaptersSupport._market_status_rows(
            statuses, set(bar_rows), trade_date
        )
        if set(status_rows) != set(bar_rows):
            raise ValueError("market slice status join is incomplete")
        market_rows = dict(bar_rows)
        if benchmark_is_index:
            benchmark_bars = self._repository.index_bars(
                (self._benchmark,), trade_date, trade_date
            ).collect()
            benchmark_rows = _AdaptersSupport._market_bar_rows(
                benchmark_bars,
                (self._benchmark,),
                trade_date,
                identifier_column="index_id",
            )
            if self._benchmark.canonical() in market_rows:
                raise ValueError("index benchmark also appears in daily bars")
            market_rows.update(benchmark_rows)
        if self._benchmark.canonical() not in market_rows:
            raise ValueError("market slice is missing benchmark")
        output_rows: list[dict[str, object]] = []
        for identifier, row in sorted(market_rows.items()):
            status = status_rows.get(identifier)
            is_index_benchmark = (
                benchmark_is_index and identifier == self._benchmark.canonical()
            )
            if is_index_benchmark:
                is_suspended = False
                security_status = "NORMAL"
            else:
                if status is None:
                    raise ValueError("market slice status join is incomplete")
                is_suspended = cast(bool, status["is_suspended"])
                security_status = "ST" if status["is_st"] is True else "NORMAL"
            volume = row["volume"]
            if volume is None and is_suspended:
                volume = 0
            output_rows.append(
                {
                    "instrument_id": identifier,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "preclose": row["preclose"],
                    "volume": volume,
                    "is_suspended": is_suspended,
                    "security_status": security_status,
                    "instrument_type": instrument_rows[identifier]["instrument_type"],
                    "board": instrument_rows[identifier]["board"],
                }
            )
        output = pl.DataFrame(
            output_rows,
            schema=_MARKET_SLICE_SCHEMA,
            strict=False,
        )
        try:
            market = MarketSlice(trade_date, output)
        except (TypeError, ValueError) as error:
            raise ValueError(
                "market slice contains invalid or nonfinite canonical OHLC values"
            ) from error
        return BoundMarketSlice(market)

    def _require_capabilities(self, required: Sequence[str], *, stage: str) -> None:
        require_provider_capabilities(
            self._capabilities,
            required,
            provider=self._provider,
            stage=stage,
        )


class CanonicalStrategyData:
    """向策略提供当前 Canonical 股票池、行情和已验证因子产物。

    入参：
        repository：提供持久化访问的仓储，类型为 ``ResearchDataRepository``。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        factor_artifacts：参与本次处理的因子产物集合；调用方不得依赖未声明的顺序。
        factor_source：因子数据来源。
        universe_hash：本次运行使用的 PIT 股票池内容身份。
        universe_signal_dates：纳入动态股票池哈希的调仓信号日。
        universe_rules：股票池规则。
        capabilities：当前数据源确实支持的数据集和字段能力。
        provider：数据供应商。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Serve strategy data from current canonical and verified factor artifacts.
    """

    def __init__(
        self,
        *,
        repository: ResearchDataRepository,
        data_hash: str,
        factor_artifacts: Mapping[str, FactorArtifact] | None = None,
        factor_source: FactorValueSource | None = None,
        universe_hash: str,
        universe_signal_dates: tuple[date, ...],
        universe_rules: UniverseRules,
        capabilities: ProviderCapabilities,
        provider: str,
    ) -> None:
        if repository is None:
            raise TypeError("repository must be supplied")
        validate_sha256(data_hash, "data_hash")
        validate_sha256(universe_hash, "universe_hash")
        if not isinstance(universe_signal_dates, tuple) or any(
            not isinstance(value, date) for value in universe_signal_dates
        ):
            raise TypeError("universe_signal_dates must be a tuple of dates")
        if universe_signal_dates != tuple(sorted(universe_signal_dates)) or len(
            set(universe_signal_dates)
        ) != len(universe_signal_dates):
            raise ValueError("universe_signal_dates must be ascending and unique")
        if not isinstance(universe_rules, UniverseRules):
            raise TypeError("universe_rules must be UniverseRules")
        if not isinstance(capabilities, ProviderCapabilities):
            raise TypeError("capabilities must be ProviderCapabilities")
        self._repository = repository
        self._data_hash = data_hash
        if (factor_artifacts is None) == (factor_source is None):
            raise ValueError("supply exactly one factor source or artifact mapping")
        if factor_source is None:
            assert factor_artifacts is not None
            factor_source = ArtifactMappingFactorValueSource(
                factor_artifacts,
                data_hash=data_hash,
                universe_hash=universe_hash,
            )
        if not callable(getattr(factor_source, "values", None)):
            raise TypeError("factor_source must provide values()")
        if getattr(factor_source, "data_hash", None) != data_hash:
            raise ValueError("factor source data hash does not match experiment data")
        if getattr(factor_source, "universe_hash", None) != universe_hash:
            raise ValueError("factor source universe does not match strategy universe")
        self._factor_source = factor_source
        self._universe_hash = universe_hash
        self._universe_signal_dates = frozenset(universe_signal_dates)
        self._universe_rules = universe_rules
        self._capabilities = capabilities
        self._provider = _AdaptersSupport._nonempty_text(provider, "provider")

    def preflight(self, *, require_stock_universe: bool) -> None:
        """在执行前校验实验。

        入参：
            require_stock_universe：控制是否启用``require``股票股票池规则的布尔开关。
        返回值：
            无。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        if type(require_stock_universe) is not bool:
            raise TypeError("require_stock_universe must be a bool")
        if not require_stock_universe:
            return
        require_provider_capabilities(
            self._capabilities,
            _STOCK_CAPABILITIES,
            provider=self._provider,
            stage="VALIDATE",
        )

    def factor_values(
        self,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        factor_refs: tuple[str, ...],
    ) -> pl.DataFrame:
        """读取因子数值表。

        入参：
            signal_date：只允许使用当日收盘前已知信息的策略信号日。
            instruments：本次查询、计算或组合构建涉及的规范证券集合。
            factor_refs：按规范 ``factor_id`` 指定的因子引用集合。
        返回值：
            返回数值表（``pl.DataFrame``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        _AdaptersSupport._strict_date(signal_date, "signal_date")
        requested_instruments = _AdaptersSupport._requested_instruments(instruments)
        if not isinstance(factor_refs, tuple):
            raise TypeError("factor_refs must be a tuple")
        references = tuple(canonical_factor_ref(item) for item in factor_refs)
        if len(set(references)) != len(references):
            raise ValueError("factor_refs must be unique")
        output = self._factor_source.values(
            signal_date, requested_instruments, references
        )
        return validated_factor_values(
            output,
            signal_date=signal_date,
            instruments=requested_instruments,
            factor_refs=references,
        )

    def industry_classifications(
        self,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
        taxonomy: str,
    ) -> pl.DataFrame:
        """读取信号日行业状态，供显式行业依赖策略处理 tombstone。

        入参：
            signal_date：策略信号日。
            instruments：证券集合；``None`` 表示全市场。
            taxonomy：显式选择的分类体系。
        返回值：
            返回指定分类体系的信号日最新状态，保留未分类事件。
        异常：
            日期或 taxonomy 非法时抛出 ``TypeError``、``ValueError``；目录门禁异常传播。
        """
        _AdaptersSupport._strict_date(signal_date, "signal_date")
        requested_instruments = _AdaptersSupport._requested_instruments(instruments)
        if not isinstance(taxonomy, str) or not taxonomy.strip():
            raise ValueError("taxonomy must be a nonempty string")
        return (
            self._repository.industry_classifications_as_of(
                requested_instruments, signal_date
            )
            .filter(pl.col("taxonomy") == taxonomy)
            .collect()
        )

    def stock_universe(self, signal_date: date) -> pl.DataFrame:
        """处理实验中的股票股票池。

        入参：
            signal_date：只允许使用当日收盘前已知信息的策略信号日。
        返回值：
            返回股票池（``pl.DataFrame``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        _AdaptersSupport._strict_date(signal_date, "signal_date")
        if signal_date not in self._universe_signal_dates:
            raise ValueError("stock universe date was not bound into universe_hash")
        self.preflight(require_stock_universe=True)
        universe = UniverseBuilder(self._repository).build(
            signal_date, self._universe_rules
        )
        identifiers = tuple(
            InstrumentId.parse(value) for value in universe["instrument_id"].to_list()
        )
        market_rows = _AdaptersSupport._universe_market_evidence(
            self._repository,
            identifiers,
            signal_date,
        )
        output_rows: list[dict[str, object]] = []
        for row in universe.iter_rows(named=True):
            identifier = cast(str, row["instrument_id"])
            evidence = market_rows.get(identifier)
            eligible = row["eligible"] is True
            if eligible:
                _AdaptersSupport._eligible_market_evidence(identifier, evidence)
            adv_amount: float | None = None
            if evidence is not None:
                adv_amount = evidence["adv_amount"]
            output_rows.append(
                {
                    "instrument_id": identifier,
                    "as_of": row["as_of"],
                    "eligible": row["eligible"],
                    "reason_codes": row["reason_codes"],
                    "adv_amount": adv_amount,
                }
            )
        output = pl.DataFrame(
            output_rows,
            schema=STRATEGY_UNIVERSE_SCHEMA,
            strict=False,
        ).sort("instrument_id")
        return validated_stock_universe(output, signal_date=signal_date)


class CanonicalStrategyContextProvider:
    """只在两个相邻 Canonical 交易日之间构造无前视偏差的策略上下文。

    入参：
        repository：提供持久化访问的仓储，类型为 ``ResearchDataRepository``。
        data：待处理的数据，类型为 ``StrategyData``。
        portfolio_constructor：组合组合构建器。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``。
    Build contexts only for two genuinely adjacent canonical sessions.
    """

    def __init__(
        self,
        *,
        repository: ResearchDataRepository,
        data: StrategyData,
        portfolio_constructor: PortfolioConstructor,
    ) -> None:
        if repository is None or data is None:
            raise TypeError("repository and data must be supplied")
        if not isinstance(portfolio_constructor, PortfolioConstructor):
            raise TypeError("portfolio_constructor must be a PortfolioConstructor")
        self._repository = repository
        self._data = data
        self._portfolio_constructor = portfolio_constructor

    def __call__(self, signal_date: date, execute_date: date) -> StrategyContext:
        """以可调用对象形式执行公开协议。

        入参：
            signal_date：只允许使用当日收盘前已知信息的策略信号日。
            execute_date：使用上一交易日信号生成委托并撮合的交易日。
        返回值：
            返回``call``（``StrategyContext``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        """
        _AdaptersSupport._date_range(signal_date, execute_date)
        if signal_date == execute_date:
            raise ValueError("execute_date must be the next actual session")
        calendar = TradingCalendar.load(
            self._repository,
            signal_date,
            execute_date,
        )
        sessions = calendar.sessions(signal_date, execute_date)
        try:
            adjacent = calendar.next_session(signal_date)
        except ValueError as error:
            raise ValueError("execute_date must be the next actual session") from error
        if adjacent != execute_date or signal_date not in sessions:
            raise ValueError("execute_date must be the next actual session")
        return StrategyContext(
            signal_date,
            execute_date,
            sessions,
            self._data,
            self._portfolio_constructor,
        )


class CanonicalStrategyRunner:
    """按固定阶段顺序编排一次实验运行。

    入参：
        repository：提供持久化访问的仓储，类型为 ``ResearchDataRepository``。
        data_hash：Canonical 数据内容或本次研究输入的数据身份。
        capabilities：当前数据源确实支持的数据集和字段能力。
        provider：数据供应商。
        benchmark：基准。
        factor_artifacts：参与本次处理的因子产物集合；调用方不得依赖未声明的顺序。
        factor_source：因子数据来源。
        universe_hash：本次运行使用的 PIT 股票池内容身份。
        universe_signal_dates：纳入动态股票池哈希的调仓信号日。
        universe_rules：股票池规则。
        strategies：参与本次处理的策略集合；调用方不得依赖未声明的顺序。
        stock_strategy_refs：参与本次处理的股票策略``refs``；调用方不得依赖未声明的顺序。
        rulebook：从 ``configs/rules/a_share.yaml`` 加载的唯一交易规则。
        portfolio_constructor：组合组合构建器。
        rebalance_planner：调仓``planner``。
        artifact_root：不可变实验产物的可信根目录。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    Compose current canonical adapters, strategy targets, and BacktestEngine.
    """

    def __init__(
        self,
        *,
        repository: ResearchDataRepository,
        data_hash: str,
        capabilities: ProviderCapabilities,
        provider: str,
        benchmark: InstrumentId,
        factor_artifacts: Mapping[str, FactorArtifact] | None = None,
        factor_source: FactorValueSource | None = None,
        universe_hash: str,
        universe_signal_dates: tuple[date, ...],
        universe_rules: UniverseRules,
        strategies: Mapping[StrategyRef, Strategy],
        stock_strategy_refs: frozenset[StrategyRef],
        rulebook: MarketRuleBook,
        portfolio_constructor: PortfolioConstructor,
        rebalance_planner: RebalancePlanner,
        artifact_root: Path,
    ) -> None:
        if not isinstance(strategies, Mapping):
            raise TypeError("strategies must be a mapping")
        registered = dict(strategies)
        for reference, strategy in registered.items():
            if (
                not isinstance(reference, StrategyRef)
                or strategy.strategy_id != reference.strategy_id
            ):
                raise ValueError("registry entries must match exact StrategyRef")
        if not isinstance(stock_strategy_refs, frozenset) or not all(
            isinstance(item, StrategyRef) for item in stock_strategy_refs
        ):
            raise TypeError("stock_strategy_refs must be a frozenset of StrategyRef")
        if not stock_strategy_refs.issubset(registered):
            raise ValueError("stock strategy refs must exist in the exact registry")
        if not isinstance(artifact_root, Path):
            raise TypeError("artifact_root must be a Path")
        if not isinstance(rebalance_planner, RebalancePlanner):
            raise TypeError("rebalance_planner must be a RebalancePlanner")
        rulebook_hash = getattr(rulebook, "content_hash", None)
        if not isinstance(rulebook_hash, str):
            raise TypeError("rulebook must provide content_hash")
        validate_sha256(rulebook_hash, "rulebook content_hash")
        self._data_hash = validate_sha256(data_hash, "data_hash")
        self._benchmark = benchmark
        self._strategies = MappingProxyType(registered)
        self._stock_strategy_refs = stock_strategy_refs
        self._rulebook_hash = rulebook_hash
        self._market = CanonicalBacktestMarketData(
            repository=repository,
            benchmark=benchmark,
            capabilities=capabilities,
            provider=provider,
        )
        self._data = CanonicalStrategyData(
            repository=repository,
            data_hash=data_hash,
            factor_artifacts=factor_artifacts,
            factor_source=factor_source,
            universe_hash=universe_hash,
            universe_signal_dates=universe_signal_dates,
            universe_rules=universe_rules,
            capabilities=capabilities,
            provider=provider,
        )
        contexts = CanonicalStrategyContextProvider(
            repository=repository,
            data=self._data,
            portfolio_constructor=portfolio_constructor,
        )
        targets = StrategyTargetAdapter(self._strategies, contexts)
        self._engine = BacktestEngine(
            self._market,
            targets,
            rulebook,
            rebalance_planner,
            artifact_root=artifact_root,
        )

    def run(
        self,
        request: BacktestRequest,
        progress: ProgressSink,
        cancellation: CancellationToken,
    ) -> BacktestResult:
        """执行完整处理流程。

        入参：
            request：请求。
            progress：当前尝试已完成量、总量和阶段说明。
            cancellation：Worker 在阶段边界检查的协作取消端口。
        返回值：
            返回执行实验后的运行（``BacktestResult``）。
        异常：
            输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
        """
        if not isinstance(request, BacktestRequest):
            raise TypeError("request must be a BacktestRequest")
        if request.data_hash != self._data_hash:
            raise ValueError("request data hash does not match bound canonical data")
        if request.benchmark != self._benchmark:
            raise ValueError("request benchmark does not match bound benchmark")
        if request.rulebook_hash != self._rulebook_hash:
            raise ValueError("request rulebook hash does not match bound rulebook")
        if request.strategy not in self._strategies:
            raise ValueError("unknown strategy")
        self._market.preflight()
        self._data.preflight(
            require_stock_universe=request.strategy in self._stock_strategy_refs
        )
        return self._engine.run(request, progress, cancellation)


class _AdaptersSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _first_later_session(frame: pl.DataFrame, after: date) -> date:
        required = {"trade_date", "is_trading_day"}
        if not required.issubset(frame.columns):
            raise ValueError("trade calendar is missing required columns")
        seen: set[date] = set()
        sessions: list[date] = []
        for trade_date, is_open in frame.select(
            "trade_date", "is_trading_day"
        ).iter_rows():
            if type(trade_date) is not date or type(is_open) is not bool:
                raise ValueError("trade calendar has invalid values")
            if trade_date <= after:
                raise ValueError("trade calendar returned an out-of-scope later row")
            if trade_date in seen:
                raise ValueError("trade calendar contains duplicate trade_date")
            seen.add(trade_date)
            if is_open:
                sessions.append(trade_date)
        if not sessions:
            raise ValueError("calendar has no later trading session")
        return min(sessions)

    @staticmethod
    def _instrument_scope(frame: pl.DataFrame, label: str) -> tuple[InstrumentId, ...]:
        if "instrument_id" not in frame.columns:
            raise ValueError(f"{label} is missing instrument_id")
        values = frame["instrument_id"].to_list()
        if len(values) != len(set(values)):
            raise ValueError(f"{label} contains duplicate instrument_id")
        try:
            instruments = tuple(InstrumentId.parse(value) for value in values)
        except (TypeError, ValueError) as error:
            raise ValueError(f"{label} contains a noncanonical instrument") from error
        canonical = [item.canonical() for item in instruments]
        if canonical != sorted(canonical):
            raise ValueError(f"{label} must be canonical-ID sorted")
        return instruments

    @staticmethod
    def _instrument_metadata(frame: pl.DataFrame) -> dict[str, dict[str, str]]:
        required = {"instrument_id", "instrument_type", "board"}
        if not required.issubset(frame.columns):
            raise ValueError("market slice instruments are missing trading metadata")
        rows: dict[str, dict[str, str]] = {}
        for raw in frame.select(*sorted(required)).iter_rows(named=True):
            identifier = raw["instrument_id"]
            instrument_type = raw["instrument_type"]
            board = raw["board"]
            if not all(
                isinstance(value, str) and value
                for value in (identifier, instrument_type, board)
            ):
                raise ValueError("market slice instrument metadata is invalid")
            canonical = InstrumentId.parse(cast(str, identifier)).canonical()
            if canonical != identifier or canonical in rows:
                raise ValueError("market slice instrument metadata identity is invalid")
            rows[canonical] = {
                "instrument_type": cast(str, instrument_type),
                "board": cast(str, board),
            }
        return rows

    @staticmethod
    def _market_bar_rows(
        frame: pl.DataFrame,
        requested: tuple[InstrumentId, ...],
        trade_date: date,
        *,
        identifier_column: str = "instrument_id",
    ) -> dict[str, dict[str, object]]:
        if identifier_column not in {"instrument_id", "index_id"}:
            raise ValueError("unsupported market identifier column")
        columns = {
            identifier_column,
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
        }
        if not columns.issubset(frame.columns):
            raise ValueError("market slice bars are missing required columns")
        allowed = {item.canonical() for item in requested}
        rows: dict[str, dict[str, object]] = {}
        for row in frame.select(*sorted(columns)).iter_rows(named=True):
            identifier = row[identifier_column]
            try:
                canonical = InstrumentId.parse(cast(str, identifier)).canonical()
            except (TypeError, ValueError) as error:
                raise ValueError(
                    "market slice has a noncanonical instrument"
                ) from error
            if canonical != identifier or canonical not in allowed:
                raise ValueError("market slice contains an unexpected instrument")
            if row["trade_date"] != trade_date:
                raise ValueError("market slice bar date does not match request")
            if canonical in rows:
                raise ValueError("market slice contains duplicate bars")
            rows[canonical] = row
        return rows

    @staticmethod
    def _market_status_rows(
        frame: pl.DataFrame,
        expected: set[str],
        trade_date: date,
    ) -> dict[str, dict[str, object]]:
        columns = {
            "instrument_id",
            "trade_date",
            "is_listed",
            "is_suspended",
            "is_st",
        }
        if not columns.issubset(frame.columns):
            raise ValueError("market slice statuses are missing required columns")
        rows: dict[str, dict[str, object]] = {}
        for row in frame.select(*sorted(columns)).iter_rows(named=True):
            identifier = row["instrument_id"]
            if not isinstance(identifier, str):
                raise TypeError("market slice status has an unexpected instrument")
            if identifier not in expected:
                continue
            try:
                canonical = InstrumentId.parse(identifier).canonical()
            except ValueError as error:
                raise ValueError(
                    "market slice status has a noncanonical instrument"
                ) from error
            if canonical != identifier or row["trade_date"] != trade_date:
                raise ValueError("market slice status date or instrument is invalid")
            if identifier in rows:
                raise ValueError("market slice contains duplicate status rows")
            if any(
                type(row[name]) is not bool
                for name in ("is_listed", "is_suspended", "is_st")
            ):
                raise ValueError("market slice status flags must be booleans")
            rows[identifier] = row
        return rows

    @staticmethod
    def _selected_factor_frame(
        artifact: FactorArtifact,
        reference: str,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
    ) -> pl.DataFrame:
        return _AdaptersSupport._selected_factor_table_frame(
            artifact.table, reference, signal_date, instruments
        )

    @staticmethod
    def _selected_factor_table_frame(
        table: object,
        reference: str,
        signal_date: date,
        instruments: tuple[InstrumentId, ...] | None,
    ) -> pl.DataFrame:
        if not isinstance(table, pa.Table):
            raise TypeError("factor slice must be a pyarrow Table")
        instrument_ids = (
            None if instruments is None else [item.canonical() for item in instruments]
        )
        predicate = (pl.col("trade_date") == signal_date) & (
            pl.col("factor_id") == reference
        )
        if instrument_ids is not None:
            predicate &= pl.col("instrument_id").is_in(instrument_ids)
        return (
            cast(pl.DataFrame, pl.from_arrow(table))
            .lazy()
            .filter(predicate)
            .with_columns(pl.lit(reference, dtype=pl.String).alias("factor_ref"))
            .select(_STRATEGY_FACTOR_SCHEMA.names())
            .collect()
        )

    @staticmethod
    def _combined_factor_frames(frames: Sequence[pl.DataFrame]) -> pl.DataFrame:
        return (
            pl.concat(frames)
            .cast(_STRATEGY_FACTOR_SCHEMA)
            .sort("trade_date", "instrument_id", "factor_ref")
            if frames
            else pl.DataFrame(schema=_STRATEGY_FACTOR_SCHEMA)
        )

    @staticmethod
    def _requested_instruments(
        instruments: tuple[InstrumentId, ...] | None,
    ) -> tuple[InstrumentId, ...] | None:
        if instruments is None:
            return None
        if not isinstance(instruments, tuple) or any(
            not isinstance(item, InstrumentId) for item in instruments
        ):
            raise TypeError("instruments must be a tuple of InstrumentId or None")
        canonical = tuple(item.canonical() for item in instruments)
        if len(set(canonical)) != len(canonical):
            raise ValueError("requested instruments must be unique")
        return instruments

    @staticmethod
    def _validate_factor_artifact(
        artifact: FactorArtifact,
        *,
        reference: str,
        data_hash: str,
        universe_hash: str,
        signal_date: date,
    ) -> None:
        if artifact.factor_ref != reference:
            raise ValueError("factor artifact reference does not match request")
        if artifact.data_hash != data_hash:
            raise ValueError("factor artifact data hash does not match experiment data")
        if artifact.universe_hash != universe_hash:
            raise ValueError(
                "factor artifact universe does not match strategy universe"
            )
        if signal_date < artifact.start or signal_date > artifact.end:
            raise ValueError("factor artifact does not cover requested date range")

    @staticmethod
    def _universe_market_evidence(
        repository: ResearchDataRepository,
        instruments: tuple[InstrumentId, ...],
        signal_date: date,
    ) -> dict[str, dict[str, float]]:
        if not instruments:
            return {}
        coverage_start = date.min + _ONE_DAY
        calendar = TradingCalendar.load(repository, coverage_start, signal_date)
        sessions = calendar.sessions(coverage_start, signal_date)[-20:]
        if not sessions or sessions[-1] != signal_date:
            raise ValueError(
                "stock universe signal date is not an actual trading session"
            )
        bars = repository.bars(instruments, sessions[0], signal_date).collect()
        required = {
            "instrument_id",
            "trade_date",
            "amount",
            "available_at",
            "pit_usable",
        }
        if not required.issubset(bars.columns):
            raise ValueError("stock universe market evidence has an invalid schema")
        close_utc = datetime.combine(
            signal_date, time.max, tzinfo=_SHANGHAI
        ).astimezone(UTC)
        visible = bars.filter(
            pl.col("pit_usable")
            & pl.col("available_at").is_not_null()
            & (pl.col("available_at") <= close_utc)
        )
        expected_ids = {item.canonical() for item in instruments}
        expected_dates = set(sessions)
        suspension_statuses = _AdaptersSupport._suspension_statuses(
            repository.security_status_range(
                sessions[0], signal_date, instruments
            ).collect(),
            expected_ids,
            expected_dates,
        )
        seen: set[tuple[str, date]] = set()
        amounts: dict[str, list[float]] = {}
        for row in visible.select(*sorted(required)).iter_rows(named=True):
            identifier = row["instrument_id"]
            trade_date = row["trade_date"]
            if not isinstance(identifier, str) or identifier not in expected_ids:
                raise ValueError("stock universe market evidence has unexpected scope")
            if type(trade_date) is not date or trade_date not in expected_dates:
                raise ValueError("stock universe market evidence has unexpected dates")
            key = (identifier, trade_date)
            if key in seen:
                raise ValueError("stock universe market evidence has duplicate bars")
            seen.add(key)
            amount = row["amount"]
            if amount is None and suspension_statuses.get(key) is True:
                amount = 0.0
            if not isinstance(amount, float) or not isfinite(amount) or amount < 0:
                raise ValueError("stock universe market evidence is nonfinite")
            amounts.setdefault(identifier, []).append(amount)
        result: dict[str, dict[str, float]] = {}
        for identifier, values in amounts.items():
            if values:
                result[identifier] = {
                    "adv_amount": sum(values) / len(values),
                }
        return result

    @staticmethod
    def _suspension_statuses(
        frame: pl.DataFrame,
        expected_ids: set[str],
        expected_dates: set[date],
    ) -> dict[tuple[str, date], bool]:
        required = {"instrument_id", "trade_date", "is_suspended"}
        if not required.issubset(frame.columns):
            raise ValueError("stock universe status evidence has an invalid schema")
        statuses: dict[tuple[str, date], bool] = {}
        for identifier, trade_date, is_suspended in frame.select(
            "instrument_id", "trade_date", "is_suspended"
        ).iter_rows():
            if not isinstance(identifier, str) or identifier not in expected_ids:
                raise ValueError("stock universe status evidence has unexpected scope")
            if type(trade_date) is not date or trade_date not in expected_dates:
                raise ValueError("stock universe status evidence has unexpected dates")
            if type(is_suspended) is not bool:
                raise ValueError("stock universe suspension status is invalid")
            key = (identifier, trade_date)
            if key in statuses:
                raise ValueError("stock universe status evidence has duplicates")
            statuses[key] = is_suspended
        return statuses

    @staticmethod
    def _eligible_market_evidence(
        identifier: str,
        evidence: dict[str, float] | None,
    ) -> None:
        if evidence is None:
            raise ValueError(
                f"stock universe market evidence is missing for {identifier}"
            )

    @staticmethod
    def _date_range(start: object, end: object) -> None:
        _AdaptersSupport._strict_date(start, "start")
        _AdaptersSupport._strict_date(end, "end")
        if cast(date, start) > cast(date, end):
            raise ValueError("start must not follow end")

    @staticmethod
    def _strict_date(value: object, name: str) -> None:
        if type(value) is not date:
            raise TypeError(f"{name} must be a date")

    @staticmethod
    def _nonempty_text(value: object, name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must be a nonempty string")
        return value


__all__ = [
    "STRATEGY_UNIVERSE_SCHEMA",
    "ArtifactMappingFactorValueSource",
    "CanonicalBacktestMarketData",
    "CanonicalStrategyContextProvider",
    "CanonicalStrategyData",
    "CanonicalStrategyRunner",
    "FactorValueSource",
    "PartitionedFactorValueSource",
]
