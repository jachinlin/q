"""基于已验证 Canonical 数据生成市场全景。"""

from __future__ import annotations

import math
import threading
from collections import OrderedDict, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Literal, cast

import polars as pl

from quant_research.backtest.rulebook import MarketRuleBook, SecurityStatus
from quant_research.dashboard.models import (
    MarketReviewBreadth,
    MarketReviewBucket,
    MarketReviewDataQuality,
    MarketReviewDates,
    MarketReviewIndex,
    MarketReviewIndustries,
    MarketReviewIndustry,
    MarketReviewLimitEvent,
    MarketReviewLiquidity,
    MarketReviewResponse,
    MarketReviewSentiment,
    MarketReviewSeriesPoint,
    MarketReviewValuation,
    MarketReviewValuationMetric,
)
from quant_research.data.repository import ResearchDataRepository
from quant_research.domain.enums import Board, DatasetKind, Severity
from quant_research.domain.errors import ErrorDetail, QuantError
from quant_research.domain.identifiers import IndexId, InstrumentId
from quant_research.infrastructure.persistence.repositories import DataCatalogState

_INDEXES = (
    ("399317.SZ", "国证A指"),
    ("000016.SH", "上证50"),
    ("000300.SH", "沪深300"),
    ("000905.SH", "中证500"),
    ("000852.SH", "中证1000"),
)
_REQUIRED_DATED_DATASETS = (
    DatasetKind.STOCK_DAILY_BAR,
    DatasetKind.STOCK_DAILY_BASIC,
    DatasetKind.STOCK_SUSPENSION,
    DatasetKind.STOCK_RISK_WARNING,
    DatasetKind.INDEX_DAILY_BAR,
)
_LIMIT_NOTE = (
    "涨跌停按现有 A 股规则簿估算；上市前五个交易日和无法分类证券不纳入。"
    "现有数据无法识别重新上市例外。"
)


class MarketReviewService:
    """生成可追溯、时点安全且口径稳定的市场全景。

    入参：
        repository：提供已验证研究数据及其目录身份、覆盖和质量门禁的仓储。
        rulebook：唯一 A 股交易规则簿，用于涨跌停价格边界。
        cache_size：进程内最多缓存的市场全景响应数量。
    返回值：
        构造并返回 ``MarketReviewService`` 实例。
    异常：
        依赖类型、数据门、目录漂移或 Canonical 内容违反契约时抛出异常。
    """

    def __init__(
        self,
        repository: ResearchDataRepository,
        rulebook: MarketRuleBook,
        *,
        cache_size: int = 64,
    ) -> None:
        """创建市场全景服务。

        入参：
            repository：提供研究数据及其只读 Canonical 目录的仓储。
            rulebook：用于计算涨跌停价格边界的市场规则簿。
            cache_size：按目录身份保留的最大进程内缓存条目数。
        返回值：
            无。
        异常：
            ``cache_size`` 不是正整数时抛出 ``ValueError``。
        """
        if type(cache_size) is not int or cache_size <= 0:
            raise ValueError("cache_size must be a positive integer")
        self._repository = repository
        self._catalog = repository.catalog()
        self._rulebook = rulebook
        self._cache_size = cache_size
        self._lock = threading.RLock()
        self._date_cache: OrderedDict[str, MarketReviewDates] = OrderedDict()
        self._review_cache: OrderedDict[
            tuple[str, date, bool], MarketReviewResponse
        ] = OrderedDict()

    def available_dates(self) -> MarketReviewDates:
        """返回当前验证目录支持的全部市场全景交易日。

        入参：无。
        返回值：目录身份、验证时间、最新日期与升序交易日集合。
        异常：当前目录未验证或必要数据集缺少日期覆盖时抛出异常。
        """

        state = self._catalog.require_validated_catalog()
        return self._dates_for_state(state)

    def review(
        self, trade_date: date | None, *, exclude_st: bool
    ) -> MarketReviewResponse:
        """生成指定交易日的完整市场全景。

        入参：
            trade_date：目标交易日；为空时选择最新有效交易日。
            exclude_st：是否从所有股票横截面统计中剔除 ST。
        返回值：不可变的市场全景 DTO。
        异常：日期不受支持、目录未验证、内容不完整或计算期间目录漂移时抛出。
        """

        if trade_date is not None and not isinstance(trade_date, date):
            raise TypeError("trade_date must be a date or None")
        if type(exclude_st) is not bool:
            raise TypeError("exclude_st must be a bool")

        for _ in range(2):
            state = self._catalog.require_validated_catalog()
            date_catalog = self._dates_for_state(state)
            selected = trade_date or date_catalog.latest_trade_date
            if selected not in date_catalog.dates:
                raise ValueError("trade_date is not an available market review session")
            key = (state.catalog_hash, selected, exclude_st)
            cached = self._cache_get(self._review_cache, key)
            if cached is not None:
                return cached
            result = self._compute_review(state, date_catalog, selected, exclude_st)
            after = self._catalog.require_validated_catalog()
            if after.catalog_hash == state.catalog_hash:
                self._cache_put(self._review_cache, key, result)
                return result

        raise QuantError(
            ErrorDetail(
                code="DATA_CATALOG_DRIFT",
                severity=Severity.FATAL,
                message="canonical catalog changed while market review was computed",
                context={"trade_date": (trade_date or date.min).isoformat()},
                remediation="retry after the current data publication completes",
                retryable=True,
            )
        )

    def _dates_for_state(self, state: DataCatalogState) -> MarketReviewDates:
        cached = self._cache_get(self._date_cache, state.catalog_hash)
        if cached is not None:
            return cached
        if state.validated_at is None:
            raise ValueError("validated catalog has no validation timestamp")
        records = {
            item.dataset: item for item in self._catalog.list_canonical_datasets()
        }
        missing = [
            item.value for item in _REQUIRED_DATED_DATASETS if item not in records
        ]
        if missing:
            raise ValueError(f"market review datasets are missing: {','.join(missing)}")
        required = [records[item] for item in _REQUIRED_DATED_DATASETS]
        starts = [item.start_date for item in required]
        ends = [item.end_date for item in required]
        if any(item is None for item in (*starts, *ends)):
            raise ValueError("market review datasets lack date coverage metadata")
        start = max(cast(date, item) for item in starts)
        end = min(cast(date, item) for item in ends)
        if start > end:
            raise ValueError("market review datasets have no common date coverage")
        calendar = self._repository.trade_calendar(start, end).collect()
        dates = tuple(
            cast(date, value)
            for value in calendar.filter(pl.col("is_trading_day"))[
                "trade_date"
            ].to_list()
        )
        if not dates:
            raise ValueError("market review has no available trading sessions")
        result = MarketReviewDates(
            catalog_hash=state.catalog_hash,
            validated_at=state.validated_at,
            latest_trade_date=dates[-1],
            dates=dates,
        )
        self._cache_put(self._date_cache, state.catalog_hash, result)
        return result

    def _compute_review(
        self,
        state: DataCatalogState,
        date_catalog: MarketReviewDates,
        selected: date,
        exclude_st: bool,
    ) -> MarketReviewResponse:
        if state.validated_at is None:
            raise ValueError("validated catalog has no validation timestamp")
        sessions = tuple(item for item in date_catalog.dates if item <= selected)[-21:]
        if not sessions:
            raise ValueError("market review history is empty")
        history_start = sessions[0]

        raw_stock_rows = self._repository.stocks().collect().rows(named=True)
        stock_rows: list[dict[str, object]] = []
        parsed_stock_ids: list[InstrumentId] = []
        for row in raw_stock_rows:
            try:
                parsed_identifier = InstrumentId.parse(
                    cast(str, row["instrument_id"])
                )
            except (AttributeError, TypeError, ValueError):
                continue
            stock_rows.append(row)
            parsed_stock_ids.append(parsed_identifier)
        stock_ids = tuple(parsed_stock_ids)
        if not stock_ids:
            raise ValueError("market review stock universe is empty")
        metadata = {cast(str, row["instrument_id"]): row for row in stock_rows}

        bars = self._repository.stock_bars(
            stock_ids, history_start, selected
        ).collect()
        suspensions = self._repository.stock_suspensions(
            history_start, selected, stock_ids
        ).collect()
        warnings = self._repository.stock_risk_warnings(
            history_start, selected, stock_ids
        ).collect()
        suspended_keys = {
            (cast(str, row["instrument_id"]), cast(date, row["trade_date"]))
            for row in suspensions.rows(named=True)
        }
        warning_keys = {
            (cast(str, row["instrument_id"]), cast(date, row["trade_date"]))
            for row in warnings.rows(named=True)
        }
        market_rows: dict[date, list[dict[str, object]]] = defaultdict(list)
        for bar in bars.rows(named=True):
            identifier = cast(str, bar["instrument_id"])
            session = cast(date, bar["trade_date"])
            master = metadata[identifier]
            listed = self._is_listed(master, session)
            is_st = (identifier, session) in warning_keys
            if not listed:
                continue
            if exclude_st and is_st:
                continue
            market_rows[session].append(
                {
                    **bar,
                    **master,
                    "is_listed": listed,
                    "is_st": is_st,
                    "is_suspended": (identifier, session) in suspended_keys,
                    "instrument_type": "STOCK",
                }
            )

        current_statuses = [
            {
                **row,
                "trade_date": selected,
                "is_listed": True,
                "is_st": (cast(str, row["instrument_id"]), selected) in warning_keys,
                "is_suspended": (
                    cast(str, row["instrument_id"]), selected
                ) in suspended_keys,
            }
            for row in stock_rows
            if self._is_listed(row, selected)
            and (
                not exclude_st
                or (cast(str, row["instrument_id"]), selected) not in warning_keys
            )
        ]
        current = market_rows[selected]
        priced = [row for row in current if self._is_priced(row)]
        expected_count = len(current_statuses)
        suspended_count = sum(
            row.get("is_suspended") is True for row in current_statuses
        )
        priced_count = len(priced)
        missing_bar_count = max(expected_count - suspended_count - priced_count, 0)
        st_count = sum(row.get("is_st") is True for row in current_statuses)
        denominator = expected_count - suspended_count
        quality = MarketReviewDataQuality(
            expected_count=expected_count,
            priced_count=priced_count,
            suspended_count=suspended_count,
            st_count=st_count,
            missing_bar_count=missing_bar_count,
            coverage_rate=(priced_count / denominator if denominator > 0 else None),
        )

        returns = [cast(float, row["pct_change"]) for row in priced]
        breadth = self._breadth(returns)
        liquidity = self._liquidity(sessions, market_rows)
        index_views = self._indexes(sessions, history_start, selected)
        sentiment, event_by_instrument = self._sentiment(
            priced, date_catalog.dates, selected
        )
        industries = self._industries(
            stock_ids,
            current_statuses,
            current,
            priced,
            event_by_instrument,
            selected,
            expected_count,
        )
        valuation = self._valuation(stock_ids, priced, selected)

        return MarketReviewResponse(
            trade_date=selected,
            catalog_hash=state.catalog_hash,
            validated_at=state.validated_at,
            exclude_st=exclude_st,
            data_quality=quality,
            indexes=index_views,
            liquidity=liquidity,
            breadth=breadth,
            sentiment=sentiment,
            industries=industries,
            valuation=valuation,
        )

    def _indexes(
        self, sessions: tuple[date, ...], start: date, end: date
    ) -> tuple[MarketReviewIndex, ...]:
        identifiers = tuple(IndexId.parse(item[0]) for item in _INDEXES)
        frame = self._repository.index_bars(identifiers, start, end).collect()
        result: list[MarketReviewIndex] = []
        for identifier, name in _INDEXES:
            rows = frame.filter(pl.col("index_id") == identifier).sort("trade_date")
            closes = [self._finite(value) for value in rows["close"].to_list()]
            valid_closes = [value for value in closes if value is not None]
            selected_row = rows.filter(pl.col("trade_date") == end)
            daily_return: float | None = None
            amplitude: float | None = None
            if selected_row.height:
                row = selected_row.row(0, named=True)
                pct = self._finite(row["pct_change"])
                daily_return = pct
                high = self._finite(row["high"])
                low = self._finite(row["low"])
                previous = self._finite(row["preclose"])
                if high is not None and low is not None and previous not in (None, 0.0):
                    amplitude = (high - low) / previous
            return_5d = (
                valid_closes[-1] / valid_closes[-6] - 1.0
                if len(valid_closes) >= 6
                else None
            )
            return_20d = (
                valid_closes[-1] / valid_closes[-21] - 1.0
                if len(valid_closes) >= 21
                else None
            )
            base = valid_closes[0] if valid_closes else None
            series = tuple(
                MarketReviewSeriesPoint(
                    trade_date=cast(date, row["trade_date"]),
                    value=close / base,
                )
                for row, close in zip(rows.rows(named=True), closes, strict=True)
                if close is not None and base not in (None, 0.0)
            )
            result.append(
                MarketReviewIndex(
                    index_id=identifier,
                    name=name,
                    daily_return=daily_return,
                    amplitude=amplitude,
                    return_5d=return_5d,
                    return_20d=return_20d,
                    series=series,
                )
            )
        return tuple(result)

    @classmethod
    def _liquidity(
        cls,
        sessions: tuple[date, ...],
        market_rows: Mapping[date, Sequence[Mapping[str, object]]],
    ) -> MarketReviewLiquidity:
        amounts = [
            sum(
                value
                for row in market_rows.get(session, ())
                if (value := cls._finite(row.get("amount"))) is not None
            )
            for session in sessions
        ]
        current = amounts[-1]
        previous = amounts[-2] if len(amounts) >= 2 else None
        change = (
            current / previous - 1.0
            if previous is not None and previous > 0.0
            else None
        )
        last_20 = amounts[-20:]
        percentile = (
            sum(value <= current for value in last_20) / len(last_20)
            if last_20
            else None
        )
        series = tuple(
            MarketReviewSeriesPoint(
                trade_date=session,
                value=amount,
                auxiliary=cls._mean(amounts[max(0, index - 4) : index + 1]),
            )
            for index, (session, amount) in enumerate(
                zip(sessions, amounts, strict=True)
            )
        )
        return MarketReviewLiquidity(
            amount=current,
            change_vs_previous=change,
            average_5d=cls._mean(amounts[-5:]),
            average_20d=cls._mean(last_20),
            percentile_20d=percentile,
            series=series,
        )

    @classmethod
    def _breadth(cls, returns: Sequence[float]) -> MarketReviewBreadth:
        up = sum(value > 0.0 for value in returns)
        down = sum(value < 0.0 for value in returns)
        flat = len(returns) - up - down
        return MarketReviewBreadth(
            up_count=up,
            down_count=down,
            flat_count=flat,
            advance_rate=(up / len(returns) if returns else None),
            net_advance_count=up - down,
            equal_weight_return=cls._mean(returns),
            median_return=cls._quantile(returns, 0.5),
            p10_return=cls._quantile(returns, 0.1),
            p25_return=cls._quantile(returns, 0.25),
            p75_return=cls._quantile(returns, 0.75),
            p90_return=cls._quantile(returns, 0.9),
            buckets=cls._return_buckets(returns),
        )

    def _sentiment(
        self,
        rows: Sequence[Mapping[str, object]],
        all_dates: tuple[date, ...],
        selected: date,
    ) -> tuple[MarketReviewSentiment, dict[str, str]]:
        events: list[MarketReviewLimitEvent] = []
        event_by_instrument: dict[str, str] = {}
        eligible = 0
        unresolved = 0
        for row in rows:
            listing = row.get("list_date")
            if (
                not isinstance(listing, date)
                or self._listing_sessions(all_dates, listing, selected) <= 5
            ):
                unresolved += 1
                continue
            identifier = cast(str, row["instrument_id"])
            try:
                board = Board(cast(str, row["board"]))
                profile = self._rulebook.trading_profile(
                    InstrumentId.parse(identifier), "STOCK", board, selected
                )
                band = self._rulebook.price_limits(
                    profile,
                    selected,
                    cast(float, row["preclose"]),
                    SecurityStatus.ST
                    if row.get("is_st") is True
                    else SecurityStatus.NORMAL,
                )
            except (TypeError, ValueError):
                unresolved += 1
                continue
            if band is None:
                unresolved += 1
                continue
            eligible += 1
            close = cast(float, row["close"])
            high = cast(float, row["high"])
            low = cast(float, row["low"])
            open_price = cast(float, row["open"])
            event: str | None = None
            if self._same_price(close, band.upper):
                if all(
                    self._same_price(value, band.upper)
                    for value in (open_price, high, low, close)
                ):
                    event = "ONE_PRICE_LIMIT_UP"
                else:
                    event = "LIMIT_UP"
            elif self._same_price(close, band.lower):
                event = "LIMIT_DOWN"
            elif self._same_price(high, band.upper) and close < band.upper:
                event = "BROKEN_LIMIT_UP"
            if event is None:
                continue
            event_by_instrument[identifier] = event
            amount = self._finite(row.get("amount"))
            events.append(
                MarketReviewLimitEvent(
                    instrument_id=identifier,
                    name=cast(str, row["name"]),
                    board=cast(str, row["board"]),
                    is_st=row.get("is_st") is True,
                    pct_change=cast(float, row["pct_change"]),
                    amount=amount,
                    event=cast(
                        Literal[
                            "LIMIT_UP",
                            "LIMIT_DOWN",
                            "BROKEN_LIMIT_UP",
                            "ONE_PRICE_LIMIT_UP",
                        ],
                        event,
                    ),
                )
            )
        events.sort(key=lambda item: (item.event, item.instrument_id))
        total = eligible + unresolved
        return (
            MarketReviewSentiment(
                limit_up_count=sum(
                    item.event in {"LIMIT_UP", "ONE_PRICE_LIMIT_UP"} for item in events
                ),
                limit_down_count=sum(item.event == "LIMIT_DOWN" for item in events),
                broken_limit_up_count=sum(
                    item.event == "BROKEN_LIMIT_UP" for item in events
                ),
                one_price_limit_up_count=sum(
                    item.event == "ONE_PRICE_LIMIT_UP" for item in events
                ),
                eligible_count=eligible,
                unresolved_count=unresolved,
                coverage_rate=(eligible / total if total else None),
                events=tuple(events),
                note=_LIMIT_NOTE,
            ),
            event_by_instrument,
        )

    def _industries(
        self,
        stock_ids: tuple[InstrumentId, ...],
        current_statuses: Sequence[Mapping[str, object]],
        current: Sequence[Mapping[str, object]],
        priced: Sequence[Mapping[str, object]],
        event_by_instrument: Mapping[str, str],
        selected: date,
        expected_count: int,
    ) -> MarketReviewIndustries:
        frame = self._repository.industry_memberships_on_dates(
            None, (selected,)
        ).collect()
        if frame.is_empty():
            return MarketReviewIndustries(
                available=False,
                taxonomy=None,
                coverage_rate=None,
                unavailable_reason="所选日期之前没有可用的供应商重建快照",
                items=(),
            )
        taxonomy = "SW2021"
        selected_ids = [item.canonical() for item in stock_ids]
        frame = frame.filter(
            pl.col("instrument_id").is_in(selected_ids)
        ).with_columns(
            pl.col("level1_code").alias("industry_code"),
            pl.col("level1_name").alias("industry_name"),
        ).filter(
            pl.col("industry_code").is_not_null()
            & pl.col("industry_name").is_not_null()
        )
        classification = {
            cast(str, row["instrument_id"]): row for row in frame.rows(named=True)
        }
        current_by_id = {cast(str, row["instrument_id"]): row for row in current}
        priced_ids = {cast(str, row["instrument_id"]) for row in priced}
        total_amount = sum(
            value
            for row in current
            if (value := self._finite(row.get("amount"))) is not None
        )
        grouped: dict[str, list[str]] = defaultdict(list)
        for status in current_statuses:
            identifier = cast(str, status["instrument_id"])
            industry = classification.get(identifier)
            if industry is not None:
                grouped[cast(str, industry["industry_code"])].append(identifier)
        items: list[MarketReviewIndustry] = []
        for code, identifiers in grouped.items():
            industry = classification[identifiers[0]]
            valid_ids = [item for item in identifiers if item in priced_ids]
            returns = [
                cast(float, current_by_id[item]["pct_change"])
                for item in valid_ids
            ]
            amount = sum(
                value
                for item in identifiers
                if item in current_by_id
                and (value := self._finite(current_by_id[item].get("amount")))
                is not None
            )
            items.append(
                MarketReviewIndustry(
                    industry_code=code,
                    industry_name=cast(str, industry["industry_name"]),
                    equal_weight_return=self._mean(returns),
                    advance_rate=(
                        sum(value > 0.0 for value in returns) / len(returns)
                        if returns
                        else None
                    ),
                    amount_share=(
                        amount / total_amount if total_amount > 0.0 else None
                    ),
                    instrument_count=len(identifiers),
                    priced_count=len(valid_ids),
                    limit_up_count=sum(
                        event_by_instrument.get(item)
                        in {"LIMIT_UP", "ONE_PRICE_LIMIT_UP"}
                        for item in identifiers
                    ),
                    limit_down_count=sum(
                        event_by_instrument.get(item) == "LIMIT_DOWN"
                        for item in identifiers
                    ),
                )
            )
        items.sort(
            key=lambda item: (
                -(
                    item.equal_weight_return
                    if item.equal_weight_return is not None
                    else -math.inf
                ),
                item.industry_code,
            )
        )
        classified_count = sum(len(values) for values in grouped.values())
        return MarketReviewIndustries(
            available=True,
            taxonomy=taxonomy,
            coverage_rate=(
                classified_count / expected_count if expected_count > 0 else None
            ),
            unavailable_reason=None,
            items=tuple(items),
        )

    def _valuation(
        self,
        stock_ids: tuple[InstrumentId, ...],
        priced: Sequence[Mapping[str, object]],
        selected: date,
    ) -> MarketReviewValuation:
        allowed = {cast(str, row["instrument_id"]) for row in priced}
        frame = self._repository.stock_daily_basics(
            stock_ids, selected, selected
        ).collect()
        rows = [
            row
            for row in frame.rows(named=True)
            if cast(str, row["instrument_id"]) in allowed
        ]
        metrics: list[MarketReviewValuationMetric] = []
        for metric in ("pe_ttm", "pb", "ps_ttm"):
            values = [
                value
                for row in rows
                if (value := self._finite(row.get(metric))) is not None and value > 0.0
            ]
            metrics.append(
                MarketReviewValuationMetric(
                    metric=metric,
                    median=self._quantile(values, 0.5),
                    p25=self._quantile(values, 0.25),
                    p75=self._quantile(values, 0.75),
                    valid_count=len(values),
                )
            )
        turnovers = [
            value
            for row in rows
            if (value := self._finite(row.get("turnover_rate"))) is not None
            and value >= 0.0
        ]
        return MarketReviewValuation(
            metrics=tuple(metrics),
            turnover_median=self._quantile(turnovers, 0.5),
            turnover_valid_count=len(turnovers),
        )

    @staticmethod
    def _is_priced(row: Mapping[str, object]) -> bool:
        if row.get("is_suspended") is True:
            return False
        return all(
            MarketReviewService._finite(row.get(field)) is not None
            for field in ("open", "high", "low", "close", "preclose", "pct_change")
        )

    @staticmethod
    def _is_listed(row: Mapping[str, object], session: date) -> bool:
        listing = row.get("list_date")
        delisting = row.get("delist_date")
        return (
            isinstance(listing, date)
            and listing <= session
            and (not isinstance(delisting, date) or delisting > session)
        )

    @staticmethod
    def _finite(value: object) -> float | None:
        if type(value) not in (int, float):
            return None
        parsed = float(cast(int | float, value))
        return parsed if math.isfinite(parsed) else None

    @staticmethod
    def _mean(values: Sequence[float]) -> float | None:
        return sum(values) / len(values) if values else None

    @staticmethod
    def _quantile(values: Sequence[float], probability: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        position = (len(ordered) - 1) * probability
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return ordered[lower]
        weight = position - lower
        return ordered[lower] * (1.0 - weight) + ordered[upper] * weight

    @staticmethod
    def _return_buckets(values: Sequence[float]) -> tuple[MarketReviewBucket, ...]:
        labels = (
            "<-5%",
            "-5%~-3%",
            "-3%~-1%",
            "-1%~0%",
            "0%",
            "0%~1%",
            "1%~3%",
            "3%~5%",
            ">5%",
        )
        counts = [0] * len(labels)
        for value in values:
            if value < -0.05:
                index = 0
            elif value < -0.03:
                index = 1
            elif value < -0.01:
                index = 2
            elif value < 0.0:
                index = 3
            elif value == 0.0:
                index = 4
            elif value <= 0.01:
                index = 5
            elif value <= 0.03:
                index = 6
            elif value <= 0.05:
                index = 7
            else:
                index = 8
            counts[index] += 1
        return tuple(
            MarketReviewBucket(label=label, count=count)
            for label, count in zip(labels, counts, strict=True)
        )

    @staticmethod
    def _listing_sessions(
        sessions: tuple[date, ...], listing: date, selected: date
    ) -> int:
        if listing > selected:
            return 0
        if listing < sessions[0]:
            return 6
        return sum(listing <= session <= selected for session in sessions)

    @staticmethod
    def _same_price(left: float, right: float) -> bool:
        return math.isclose(left, right, rel_tol=0.0, abs_tol=1e-7)

    def _cache_get[K, V](self, cache: OrderedDict[K, V], key: K) -> V | None:
        with self._lock:
            value = cache.get(key)
            if value is not None:
                cache.move_to_end(key)
            return value

    def _cache_put[K, V](self, cache: OrderedDict[K, V], key: K, value: V) -> None:
        with self._lock:
            cache[key] = value
            cache.move_to_end(key)
            while len(cache) > self._cache_size:
                cache.popitem(last=False)
