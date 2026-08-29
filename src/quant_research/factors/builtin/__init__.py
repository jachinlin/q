"""提供因子与内置实现相关的公开模型、协议与处理流程。"""

from __future__ import annotations

from collections.abc import Sequence

from quant_research.domain.identifiers import InstrumentId
from quant_research.factors.base import Factor
from quant_research.factors.builtin._stock_common import BarRepository
from quant_research.factors.builtin.auxiliary import AvgAmount20dFactor
from quant_research.factors.builtin.code_hash import builtin_source_hash
from quant_research.factors.builtin.momentum import (
    AdjustedBarService,
    MarketBarsCache,
    Momentum12020Factor,
    ReturnFactor,
    Trend120dFactor,
)
from quant_research.factors.builtin.quality import (
    FinancialIndicatorsCache,
    FinancialMetricFactor,
    FinancialProvider,
    RoeFactor,
)
from quant_research.factors.builtin.risk import (
    DownsideVolatility60dFactor,
    MaxDrawdown120dFactor,
    Volatility60dFactor,
)
from quant_research.factors.builtin.turnover import Turnover20dFactor
from quant_research.factors.builtin.valuation import (
    BookToPriceFactor,
    DailyBasicsCache,
    DividendYieldFactor,
    EarningsYieldFactor,
    LogTotalMarketCapFactor,
    SalesYieldFactor,
)
from quant_research.factors.registry import FactorRegistry

_RUNTIME_DEPENDENCY_ATTRIBUTES = (
    "_price_service",
    "_repository",
    "_service",
    "_provider",
    "_calendar",
)

STOCK_FACTOR_REFERENCES = (
    "avg_amount_20d",
    "book_to_price_mrq",
    "cash_quality",
    "dividend_yield",
    "downside_volatility_60d",
    "earnings_yield_ttm",
    "gross_margin",
    "leverage",
    "log_total_market_cap",
    "max_drawdown_120d",
    "momentum_120_20",
    "profit_growth",
    "revenue_growth",
    "roa",
    "roe",
    "sales_yield",
    "turnover_20d",
    "volatility_60d",
)


class _InitSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _builtin_runtime_identity(factor: Factor) -> tuple[object, ...]:
        """Identify the concrete providers and instrument domain captured by a factor."""
        identity: list[object] = [type(factor).__module__, type(factor).__qualname__]
        instruments = getattr(factor, "_instruments", ())
        identity.append(
            tuple(
                instrument.canonical()
                if isinstance(instrument, InstrumentId)
                else instrument
                for instrument in instruments
            )
        )
        identity.extend(
            (attribute, id(getattr(factor, attribute)))
            for attribute in _RUNTIME_DEPENDENCY_ATTRIBUTES
            if hasattr(factor, attribute)
        )
        return tuple(identity)

    @staticmethod
    def _market_bars_for(
        registry: FactorRegistry,
        price_service: AdjustedBarService,
        instruments: Sequence[InstrumentId],
    ) -> MarketBarsCache:
        """Reuse the market cache already bound to the exact runtime domain."""
        for reference in registry.registered_references():
            factor = registry.factor(reference)
            existing = getattr(factor, "_market_bars", None)
            if isinstance(existing, MarketBarsCache) and existing.matches(
                price_service,
                instruments,
                max_lookback_sessions=120,
            ):
                return existing
        return MarketBarsCache(price_service, instruments, max_lookback_sessions=120)


def register_builtin(registry: FactorRegistry, factor: Factor) -> None:
    """登记``builtin``；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        registry：登记并查询不可变业务身份和生命周期状态的登记簿。
        factor：因子。
    返回值：
        无。
    异常：
        ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
    Register a bundled factor once, rejecting divergent same-ref code.
    """
    expected_hash = builtin_source_hash(factor.spec)
    try:
        existing_ref = registry.resolve(factor.spec.canonical_ref)
    except ValueError:
        registry.register(factor, code_hash=expected_hash)
        return
    if registry.code_hash(existing_ref) != expected_hash:
        raise ValueError(
            f"conflicting built-in implementation: {factor.spec.canonical_ref}"
        )
    existing = registry.factor(existing_ref)
    if _InitSupport._builtin_runtime_identity(
        existing
    ) != _InitSupport._builtin_runtime_identity(factor):
        raise ValueError(
            f"conflicting built-in runtime dependencies: {factor.spec.canonical_ref}"
        )


def register_etf_factors(
    registry: FactorRegistry,
    price_service: AdjustedBarService,
    instruments: Sequence[InstrumentId],
) -> None:
    """登记``etf``因子集合；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        registry：登记并查询不可变业务身份和生命周期状态的登记簿。
        price_service：价格服务。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
    返回值：
        无。
    异常：
        持久化端口读取、写入或并发状态校验失败时传播其领域异常。
    Register the five exact Task 6 ETF market-factor identities.
    """
    market_bars = _InitSupport._market_bars_for(registry, price_service, instruments)
    factors: tuple[Factor, ...] = (
        ReturnFactor(price_service, instruments, 20, market_bars=market_bars),
        ReturnFactor(price_service, instruments, 60, market_bars=market_bars),
        ReturnFactor(price_service, instruments, 120, market_bars=market_bars),
        Trend120dFactor(price_service, instruments, market_bars=market_bars),
        Volatility60dFactor(price_service, instruments, market_bars=market_bars),
    )
    for factor in factors:
        register_builtin(registry, factor)


def register_stock_factors(
    registry: FactorRegistry,
    bar_repository: BarRepository,
    financial_provider: FinancialProvider,
    instruments: Sequence[InstrumentId],
    *,
    price_service: AdjustedBarService,
) -> None:
    """登记股票因子集合；该函数作为稳定公开 API 或框架入口保留在模块级。

    入参：
        registry：登记并查询不可变业务身份和生命周期状态的登记簿。
        bar_repository：行情研究数据仓储。
        financial_provider：财务数据数据供应商。
        instruments：本次查询、计算或组合构建涉及的规范证券集合。
        price_service：价格服务。
    返回值：
        无。
    异常：
        持久化端口读取、写入或并发状态校验失败时传播其领域异常。
    Register all stock alpha and liquidity auxiliary identities exactly once.
    """
    market_bars = _InitSupport._market_bars_for(registry, price_service, instruments)
    daily_basics = DailyBasicsCache(bar_repository, instruments)
    financial_fields = (
        "debt_to_assets",
        "grossprofit_margin",
        "netprofit_yoy",
        "ocf_to_opincome",
        "roa",
        "roe",
        "tr_yoy",
    )
    financials = FinancialIndicatorsCache(
        financial_provider, instruments, financial_fields
    )
    factors: tuple[Factor, ...] = (
        EarningsYieldFactor(bar_repository, instruments, daily_basics=daily_basics),
        BookToPriceFactor(bar_repository, instruments, daily_basics=daily_basics),
        SalesYieldFactor(bar_repository, instruments, daily_basics=daily_basics),
        DividendYieldFactor(bar_repository, instruments, daily_basics=daily_basics),
        LogTotalMarketCapFactor(
            bar_repository, instruments, daily_basics=daily_basics
        ),
        RoeFactor(financial_provider, instruments, cache=financials),
        FinancialMetricFactor(
            financial_provider,
            instruments,
            factor_id="revenue_growth",
            field="tr_yoy",
            direction=1,
            value_domain="signed_finite",
            measurement="year_over_year",
            cache=financials,
        ),
        FinancialMetricFactor(
            financial_provider,
            instruments,
            factor_id="profit_growth",
            field="netprofit_yoy",
            direction=1,
            value_domain="signed_finite",
            measurement="year_over_year",
            cache=financials,
        ),
        FinancialMetricFactor(
            financial_provider,
            instruments,
            factor_id="roa",
            field="roa",
            direction=1,
            value_domain="signed_finite",
            measurement="point_in_time",
            cache=financials,
        ),
        FinancialMetricFactor(
            financial_provider,
            instruments,
            factor_id="gross_margin",
            field="grossprofit_margin",
            direction=1,
            value_domain="signed_finite",
            measurement="point_in_time",
            cache=financials,
        ),
        FinancialMetricFactor(
            financial_provider,
            instruments,
            factor_id="cash_quality",
            field="ocf_to_opincome",
            direction=1,
            value_domain="signed_finite",
            measurement="point_in_time",
            cache=financials,
        ),
        FinancialMetricFactor(
            financial_provider,
            instruments,
            factor_id="leverage",
            field="debt_to_assets",
            direction=-1,
            value_domain="nonnegative_finite",
            measurement="point_in_time",
            cache=financials,
        ),
        Momentum12020Factor(price_service, instruments, market_bars=market_bars),
        Volatility60dFactor(price_service, instruments, market_bars=market_bars),
        DownsideVolatility60dFactor(
            price_service, instruments, market_bars=market_bars
        ),
        MaxDrawdown120dFactor(price_service, instruments, market_bars=market_bars),
        Turnover20dFactor(bar_repository, instruments),
        AvgAmount20dFactor(bar_repository, instruments),
    )
    for factor in factors:
        register_builtin(registry, factor)


__all__ = [
    "STOCK_FACTOR_REFERENCES",
    "AdjustedBarService",
    "Momentum12020Factor",
    "ReturnFactor",
    "Trend120dFactor",
    "Volatility60dFactor",
    "register_builtin",
    "register_etf_factors",
    "register_stock_factors",
]
