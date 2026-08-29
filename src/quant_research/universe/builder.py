"""依据观察日可见的 Canonical 数据构建 PIT 股票池资格表。"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from math import isfinite
from typing import cast
from zoneinfo import ZoneInfo

import polars as pl

from quant_research.data.canonical.schemas import PolarsDataType
from quant_research.data.repository import ResearchDataRepository
from quant_research.domain.enums import Board
from quant_research.domain.identifiers import InstrumentId
from quant_research.universe.rules import UniverseRules

_OUTPUT_COLUMNS: dict[str, PolarsDataType] = {
    "instrument_id": pl.String,
    "as_of": pl.Date,
    "eligible": pl.Boolean,
    "reason_codes": cast(pl.DataType, pl.List(pl.String)),
}
_OUTPUT_SCHEMA = pl.Schema(_OUTPUT_COLUMNS)
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_REASON_PRIORITY = (
    "AS_OF_NOT_TRADING_DAY",
    "INSTRUMENT_HISTORY_MISSING",
    "INSTRUMENT_TYPE_NOT_ALLOWED",
    "NOT_LISTED_YET",
    "DELISTED",
    "INSUFFICIENT_LISTING_DAYS",
    "STATUS_MISSING",
    "RISK_WARNING",
    "SUSPENDED",
    "BOARD_NOT_ALLOWED",
    "INSUFFICIENT_LIQUIDITY_HISTORY",
    "MISSING_LIQUIDITY_OBSERVATIONS",
    "LIQUIDITY_AMOUNT_MISSING",
    "INSUFFICIENT_AVERAGE_AMOUNT",
)


class UniverseBuilder:
    """组合证券历史、交易日历、状态和成交额证据判定股票池资格。

    入参：
        repository：只读取已通过全局质量门禁的 Canonical 研究数据仓储。
    返回值：
        返回绑定该研究仓储的股票池构建器。
    异常：
        无；构造阶段不读取数据。
    """

    def __init__(self, repository: ResearchDataRepository) -> None:
        self._repository = repository

    def build(self, as_of: date, rules: UniverseRules) -> pl.DataFrame:
        """生成观察日每只当前证券的准入结论和稳定排序的原因码。

        入参：
            as_of：资格判定日；只使用该日上海时区日终前已可见的数据。
            rules：上市时长、板块、风险警示、停牌和流动性准入规则。
        返回值：
            返回按 ``instrument_id`` 排序的 DataFrame，每行包含 ``as_of``、
            ``eligible`` 以及按固定优先级排列的 ``reason_codes``。
        异常：
            ValueError：仓储返回重复主键、非法证券代码、越界日期或其他不符合
            Canonical 查询契约的数据时抛出；仓储读取异常按原类型传播。
        """
        instruments = self._repository.stocks().collect()
        instruments = _BuilderSupport._validated_instruments(instruments)
        calendar_start = date.min
        calendar = self._repository.trade_calendar(calendar_start, as_of).collect()
        calendar = _BuilderSupport._validated_calendar(calendar, calendar_start, as_of)
        identifiers = _BuilderSupport._instrument_ids(instruments)
        if not _BuilderSupport._is_trading_day(calendar, as_of):
            return _BuilderSupport._all_closed(
                identifiers, as_of, "AS_OF_NOT_TRADING_DAY"
            )

        trading_days = _BuilderSupport._trading_days(calendar, as_of)
        liquidity_window = trading_days[-20:]
        if not identifiers:
            return _BuilderSupport._empty_universe()

        suspensions = self._repository.stock_suspensions(
            as_of, as_of, identifiers
        ).collect()
        warnings = self._repository.stock_risk_warnings(
            as_of, as_of, identifiers
        ).collect()
        _BuilderSupport._validate_events(
            suspensions, {item.canonical() for item in identifiers}, as_of
        )
        _BuilderSupport._validate_events(
            warnings, {item.canonical() for item in identifiers}, as_of
        )
        bar_start = liquidity_window[0] if liquidity_window else as_of
        bars = self._repository.stock_bars(identifiers, bar_start, as_of).collect()
        _BuilderSupport._validate_bars(
            bars, {item.canonical() for item in identifiers}, liquidity_window
        )
        bars = _BuilderSupport._known_by_as_of(bars.lazy(), as_of).collect()
        suspended_ids = set(suspensions["instrument_id"].to_list())
        warning_ids = set(warnings["instrument_id"].to_list())
        status_by_instrument = {
            cast(str, row["instrument_id"]): {
                "is_listed": isinstance(row.get("list_date"), date)
                and row["list_date"] <= as_of
                and (
                    not isinstance(row.get("delist_date"), date)
                    or row["delist_date"] > as_of
                ),
                "is_st": row["instrument_id"] in warning_ids,
                "is_suspended": row["instrument_id"] in suspended_ids,
                "board": row.get("board"),
            }
            for row in instruments.to_dicts()
        }
        amounts_by_instrument = _BuilderSupport._amounts_by_instrument(bars)
        rows = [
            _BuilderSupport._eligibility_row(
                instrument,
                as_of,
                rules,
                trading_days,
                status_by_instrument.get(instrument["instrument_id"]),
                amounts_by_instrument.get(instrument["instrument_id"], {}),
            )
            for instrument in instruments.to_dicts()
        ]
        return pl.DataFrame(rows, schema=_OUTPUT_SCHEMA, strict=False)


class UniverseBatchEvaluator:
    """按同一 ``UniverseRules`` 向量化判定多个交易日的股票池资格。

    入参：
        rules：上市时长、板块、风险警示、停牌和流动性准入规则；批量模式不接受
        日均成交额门槛，因为调用方必须先提供完整的滚动流动性证据。
    返回值：
        返回绑定规则的批量判定器。
    异常：
        ValueError：规则启用了批量输入未提供的成交额门槛时抛出。
    """

    def __init__(self, rules: UniverseRules) -> None:
        if not isinstance(rules, UniverseRules):
            raise TypeError("rules must be UniverseRules")
        if rules.min_avg_amount_20d is not None:
            raise ValueError("batch universe requires explicit liquidity evidence")
        self._rules = rules

    def evaluate(
        self,
        instruments: pl.DataFrame,
        signal_sessions: pl.DataFrame,
        suspended: pl.DataFrame,
        warned: pl.DataFrame,
    ) -> pl.DataFrame:
        """生成按日期和证券稳定排序的批量资格与原因码。

        入参：
            instruments：证券元数据及 ``metadata_present``、``listing_ordinal``。
            signal_sessions：交易日及其全局 ``session_ordinal``。
            suspended、warned：已按每日 PIT 截止过滤的状态主键。
        返回值：
            返回 ``signal_date, instrument_id, eligible, reason_codes``。
        异常：
            ValueError：任一输入缺少必需列时抛出。
        """
        instrument_columns = {
            "instrument_id",
            "list_date",
            "delist_date",
            "board",
            "metadata_present",
            "listing_ordinal",
        }
        session_columns = {"trade_date", "session_ordinal"}
        status_columns = {"signal_date", "instrument_id"}
        if not instrument_columns.issubset(instruments.schema):
            raise ValueError("batch universe instrument schema is invalid")
        if not session_columns.issubset(signal_sessions.schema):
            raise ValueError("batch universe session schema is invalid")
        if not status_columns.issubset(suspended.schema) or not status_columns.issubset(
            warned.schema
        ):
            raise ValueError("batch universe status schema is invalid")
        dates = signal_sessions.rename({"trade_date": "signal_date"})
        status_dates = dates.select("signal_date")
        batch_suspended = suspended.join(
            status_dates, on="signal_date", how="inner"
        )
        batch_warned = warned.join(status_dates, on="signal_date", how="inner")
        status = (
            dates.join(instruments, how="cross")
            .join(
                batch_warned.with_columns(pl.lit(True).alias("_is_warned")),
                on=["signal_date", "instrument_id"],
                how="left",
            )
            .join(
                batch_suspended.with_columns(
                    pl.lit(True).alias("_is_suspended")
                ),
                on=["signal_date", "instrument_id"],
                how="left",
            )
            .with_columns(
                pl.col("_is_warned").fill_null(False),
                pl.col("_is_suspended").fill_null(False),
            )
        )
        metadata_missing = ~pl.col("metadata_present")
        history_missing = metadata_missing | pl.col("list_date").is_null()
        not_listed = (
            pl.col("list_date").is_not_null()
            & (pl.col("list_date") > pl.col("signal_date"))
        )
        delisted = (
            pl.col("delist_date").is_not_null()
            & (pl.col("delist_date") <= pl.col("signal_date"))
        )
        listing_session = (
            pl.col("session_ordinal") - pl.col("listing_ordinal") + 1
        )
        insufficient_listing = (
            pl.col("list_date").is_not_null()
            & (pl.col("list_date") <= pl.col("signal_date"))
            & (listing_session < self._rules.min_listing_days)
        ).fill_null(False)
        warned_out = (
            pl.col("_is_warned")
            if self._rules.exclude_st
            else pl.lit(False)
        )
        suspended_out = (
            pl.col("_is_suspended")
            if self._rules.exclude_suspended
            else pl.lit(False)
        )
        allowed_boards = sorted(board.value for board in self._rules.allowed_boards)
        board_not_allowed = ~pl.col("board").is_in(allowed_boards).fill_null(False)
        conditions = (
            (history_missing, "INSTRUMENT_HISTORY_MISSING"),
            (not_listed, "NOT_LISTED_YET"),
            (delisted, "DELISTED"),
            (insufficient_listing, "INSUFFICIENT_LISTING_DAYS"),
            (warned_out, "RISK_WARNING"),
            (suspended_out, "SUSPENDED"),
            (board_not_allowed, "BOARD_NOT_ALLOWED"),
        )
        reason_codes = pl.concat_list(
            [
                pl.when(condition)
                .then(pl.lit(reason))
                .otherwise(pl.lit(None, dtype=pl.String))
                for condition, reason in conditions
            ]
        ).list.drop_nulls()
        excluded = conditions[0][0]
        for condition, _ in conditions[1:]:
            excluded = excluded | condition
        return status.select(
            "signal_date",
            "instrument_id",
            (~excluded).fill_null(False).alias("eligible"),
            reason_codes.alias("reason_codes"),
        ).sort("signal_date", "instrument_id")


class _BuilderSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _validated_instruments(instruments: pl.DataFrame) -> pl.DataFrame:
        identifiers = instruments["instrument_id"].to_list()
        if len(identifiers) != len(set(identifiers)):
            _BuilderSupport._invalid("duplicate instrument_id")
        try:
            for identifier in identifiers:
                InstrumentId.parse(identifier)
        except (TypeError, ValueError) as error:
            raise ValueError("UNIVERSE_INPUT_INVALID: invalid instrument_id") from error
        return instruments.sort("instrument_id")

    @staticmethod
    def _validated_calendar(
        calendar: pl.DataFrame, calendar_start: date, as_of: date
    ) -> pl.DataFrame:
        required_columns = {"trade_date", "is_trading_day"}
        if not required_columns.issubset(calendar.schema):
            _BuilderSupport._invalid("invalid calendar schema")
        dates = calendar["trade_date"].to_list()
        seen: set[date] = set()
        for value in dates:
            if type(value) is not date:
                _BuilderSupport._invalid("invalid trade_date")
            if value < calendar_start or value > as_of:
                _BuilderSupport._invalid("unexpected trade_date")
            if value in seen:
                _BuilderSupport._invalid("duplicate trade_date")
            seen.add(value)
        return calendar.sort("trade_date")

    @staticmethod
    def _validate_events(
        events: pl.DataFrame, identifiers: set[str], as_of: date
    ) -> None:
        for row in events.select("instrument_id", "trade_date").to_dicts():
            if row["instrument_id"] not in identifiers or row["trade_date"] != as_of:
                _BuilderSupport._invalid("unexpected stock event")

    @staticmethod
    def _validate_bars(
        bars: pl.DataFrame, identifiers: set[str], window: list[date]
    ) -> None:
        keys: set[tuple[object, object]] = set()
        expected_dates = set(window)
        for row in bars.select("instrument_id", "trade_date").to_dicts():
            key = (row["instrument_id"], row["trade_date"])
            if key in keys:
                _BuilderSupport._invalid("duplicate bar primary key")
            keys.add(key)
            if key[0] not in identifiers or key[1] not in expected_dates:
                _BuilderSupport._invalid("unexpected bar observation")

    @staticmethod
    def _invalid(message: str) -> None:
        raise ValueError("UNIVERSE_INPUT_INVALID: " + message)

    @staticmethod
    def _instrument_ids(instruments: pl.DataFrame) -> list[InstrumentId]:
        return [
            InstrumentId.parse(value)
            for value in instruments["instrument_id"].to_list()
        ]

    @staticmethod
    def _is_trading_day(calendar: pl.DataFrame, as_of: date) -> bool:
        return bool(
            calendar.filter(
                (pl.col("trade_date") == as_of) & pl.col("is_trading_day")
            ).height
        )

    @staticmethod
    def _trading_days(calendar: pl.DataFrame, as_of: date) -> list[date]:
        return calendar.filter(
            (pl.col("trade_date") <= as_of) & pl.col("is_trading_day")
        )["trade_date"].to_list()

    @staticmethod
    def _amounts_by_instrument(
        bars: pl.DataFrame,
    ) -> dict[str, dict[date, float | None]]:
        amounts: dict[str, dict[date, float | None]] = {}
        for row in bars.to_dicts():
            amounts.setdefault(row["instrument_id"], {})[row["trade_date"]] = row[
                "amount"
            ]
        return amounts

    @staticmethod
    def _known_by_as_of(frame: pl.LazyFrame, as_of: date) -> pl.LazyFrame:
        return frame.filter(
            pl.col("pit_usable")
            & pl.col("available_at").is_not_null()
            & (pl.col("available_at") <= _BuilderSupport._shanghai_day_end_utc(as_of))
        )

    @staticmethod
    def _shanghai_day_end_utc(value: date) -> datetime:
        return datetime.combine(value, time.max, tzinfo=_SHANGHAI).astimezone(UTC)

    @staticmethod
    def _all_closed(
        identifiers: list[InstrumentId], as_of: date, reason: str
    ) -> pl.DataFrame:
        return pl.DataFrame(
            [
                {
                    "instrument_id": identifier.canonical(),
                    "as_of": as_of,
                    "eligible": False,
                    "reason_codes": [reason],
                }
                for identifier in identifiers
            ],
            schema=_OUTPUT_SCHEMA,
            strict=False,
        )

    @staticmethod
    def _empty_universe() -> pl.DataFrame:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)

    @staticmethod
    def _eligibility_row(
        instrument: dict[str, object],
        as_of: date,
        rules: UniverseRules,
        trading_days: list[date],
        status: dict[str, object] | None,
        amounts: dict[date, float | None],
    ) -> dict[str, object]:
        reasons: set[str] = set()
        list_date = instrument["list_date"]
        delist_date = instrument["delist_date"]
        if not isinstance(list_date, date):
            reasons.add("INSTRUMENT_HISTORY_MISSING")
        elif list_date > as_of:
            reasons.add("NOT_LISTED_YET")
        elif (
            sum(list_date <= day <= as_of for day in trading_days)
            < rules.min_listing_days
        ):
            reasons.add("INSUFFICIENT_LISTING_DAYS")
        if isinstance(delist_date, date) and delist_date <= as_of:
            reasons.add("DELISTED")
        if status is None:
            reasons.add("STATUS_MISSING")
        else:
            _BuilderSupport._add_status_reasons(
                reasons, status, rules, list_date, as_of
            )
        if rules.min_avg_amount_20d is not None:
            _BuilderSupport._add_liquidity_reasons(
                reasons, trading_days, amounts, rules.min_avg_amount_20d
            )
        ordered_reasons = [reason for reason in _REASON_PRIORITY if reason in reasons]
        return {
            "instrument_id": instrument["instrument_id"],
            "as_of": as_of,
            "eligible": not ordered_reasons,
            "reason_codes": ordered_reasons,
        }

    @staticmethod
    def _add_status_reasons(
        reasons: set[str],
        status: dict[str, object],
        rules: UniverseRules,
        list_date: object,
        as_of: date,
    ) -> None:
        if status["is_listed"] is False:
            if isinstance(list_date, date) and list_date > as_of:
                reasons.add("NOT_LISTED_YET")
            elif isinstance(list_date, date):
                reasons.add("DELISTED")
        if rules.exclude_st and status["is_st"] is True:
            reasons.add("RISK_WARNING")
        if rules.exclude_suspended and status["is_suspended"] is True:
            reasons.add("SUSPENDED")
        try:
            board = Board(str(status["board"]))
        except ValueError:
            reasons.add("BOARD_NOT_ALLOWED")
        else:
            if board not in rules.allowed_boards:
                reasons.add("BOARD_NOT_ALLOWED")

    @staticmethod
    def _add_liquidity_reasons(
        reasons: set[str],
        trading_days: list[date],
        amounts: dict[date, float | None],
        minimum: float,
    ) -> None:
        window = trading_days[-20:]
        if len(window) < 20:
            reasons.add("INSUFFICIENT_LIQUIDITY_HISTORY")
            return
        if any(day not in amounts for day in window):
            reasons.add("MISSING_LIQUIDITY_OBSERVATIONS")
            return
        values = [amounts[day] for day in window]
        if any(value is None or not isfinite(value) for value in values):
            reasons.add("LIQUIDITY_AMOUNT_MISSING")
            return
        average = sum(value for value in values if value is not None) / len(values)
        if average < minimum:
            reasons.add("INSUFFICIENT_AVERAGE_AMOUNT")
