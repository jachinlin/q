"""定义回测消费者拥有的分红送转事件与确定性交易日映射。"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from quant_research.backtest.calendar import TradingCalendar
from quant_research.domain.identifiers import InstrumentId


class CorporateActionInstrumentType(StrEnum):
    """标识权益事件适用的证券类型。

    入参：无。返回值：稳定证券类型枚举。异常：无。
    """

    STOCK = "STOCK"
    FUND = "FUND"


class CorporateActionType(StrEnum):
    """标识账户权益事件的唯一经济类型。

    入参：无。返回值：稳定权益事件类型枚举。异常：无。
    """

    CASH_DIVIDEND = "CASH_DIVIDEND"
    STOCK_DISTRIBUTION = "STOCK_DISTRIBUTION"
    FUND_SPLIT = "FUND_SPLIT"


@dataclass(frozen=True, slots=True)
class CorporateAction:
    """表示 Repository 已按登记日 PIT 裁决的一次实施权益事件。

    入参：事件身份、证券、来源 revision、公告与官方事件日期，以及税前现金和
    送转比例。返回值：不可变权益事件。异常：字段缺失、日期倒置或数值非法时
    抛出 ``TypeError`` 或 ``ValueError``。
    """

    event_id: str
    instrument_id: InstrumentId
    instrument_type: CorporateActionInstrumentType
    action_type: CorporateActionType
    source_revision: int
    announcement_date: date | None
    implementation_announcement_date: date | None
    record_date: date | None
    ex_date: date
    pay_date: date | None
    stock_listing_date: date | None
    cash_per_share_or_unit: Decimal
    stock_dividend_per_share: Decimal
    previous_adjustment_factor: Decimal | None = None
    adjustment_factor: Decimal | None = None

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValueError("corporate action event_id must be nonempty")
        if not isinstance(self.instrument_id, InstrumentId):
            raise TypeError("corporate action instrument_id must be InstrumentId")
        if not isinstance(self.instrument_type, CorporateActionInstrumentType):
            raise TypeError("corporate action instrument_type is invalid")
        if not isinstance(self.action_type, CorporateActionType):
            raise TypeError("corporate action action_type is invalid")
        if type(self.source_revision) is not int or self.source_revision < 0:
            raise ValueError("corporate action source_revision must be nonnegative")
        self._require_date(self.ex_date, "ex_date")
        for optional_date, optional_date_name in (
            (self.announcement_date, "announcement_date"),
            (
                self.implementation_announcement_date,
                "implementation_announcement_date",
            ),
            (self.record_date, "record_date"),
            (self.pay_date, "pay_date"),
            (self.stock_listing_date, "stock_listing_date"),
        ):
            if optional_date is not None:
                self._require_date(optional_date, optional_date_name)
        if (
            self.implementation_announcement_date is not None
            and self.record_date is not None
            and self.implementation_announcement_date > self.record_date
        ):
            raise ValueError("implementation announcement must not follow record date")
        if self.record_date is not None and self.record_date >= self.ex_date:
            raise ValueError("record date must precede ex date")
        if self.pay_date is not None and self.pay_date < self.ex_date:
            raise ValueError("pay date must not precede ex date")
        if (
            self.stock_listing_date is not None
            and self.stock_listing_date < self.ex_date
        ):
            raise ValueError("stock listing date must not precede ex date")
        for decimal_value, decimal_name in (
            (self.cash_per_share_or_unit, "cash_per_share_or_unit"),
            (self.stock_dividend_per_share, "stock_dividend_per_share"),
        ):
            if (
                not isinstance(decimal_value, Decimal)
                or not decimal_value.is_finite()
                or decimal_value < 0
            ):
                raise ValueError(
                    f"{decimal_name} must be a finite nonnegative Decimal"
                )
        for optional_decimal, optional_decimal_name in (
            (self.previous_adjustment_factor, "previous_adjustment_factor"),
            (self.adjustment_factor, "adjustment_factor"),
        ):
            if optional_decimal is not None and (
                not isinstance(optional_decimal, Decimal)
                or not optional_decimal.is_finite()
                or optional_decimal <= 0
            ):
                raise ValueError(
                    f"{optional_decimal_name} must be a finite positive Decimal"
                )
        if self.action_type is CorporateActionType.CASH_DIVIDEND:
            if (
                self.cash_per_share_or_unit <= 0
                or self.stock_dividend_per_share != 0
                or self.pay_date is None
                or self.record_date is None
                or self.implementation_announcement_date is None
            ):
                raise ValueError("cash dividend fields are incomplete")
        elif self.action_type is CorporateActionType.STOCK_DISTRIBUTION:
            if (
                self.cash_per_share_or_unit != 0
                or self.stock_dividend_per_share <= 0
                or self.stock_listing_date is None
                or self.record_date is None
                or self.implementation_announcement_date is None
                or self.instrument_type is not CorporateActionInstrumentType.STOCK
            ):
                raise ValueError("stock distribution fields are incomplete")
        elif (
            self.cash_per_share_or_unit != 0
            or self.stock_dividend_per_share <= 1
            or self.instrument_type is not CorporateActionInstrumentType.FUND
            or self.record_date is not None
            or self.pay_date is not None
            or self.stock_listing_date is not None
            or self.previous_adjustment_factor is None
            or self.adjustment_factor is None
        ):
            raise ValueError("fund split fields are incomplete")
        if self.action_type is CorporateActionType.FUND_SPLIT:
            assert self.previous_adjustment_factor is not None
            assert self.adjustment_factor is not None
            derived = self.adjustment_factor / self.previous_adjustment_factor
            relative_error = abs(derived / self.stock_dividend_per_share - 1)
            if (
                self.stock_dividend_per_share
                != self.stock_dividend_per_share.to_integral_value()
                or relative_error > Decimal("0.001")
            ):
                raise ValueError(
                    "fund split requires an unambiguous near-integer factor ratio"
                )
        elif (
            self.previous_adjustment_factor is not None
            or self.adjustment_factor is not None
        ):
            raise ValueError("dividend events cannot carry adjustment factors")

    @staticmethod
    def _require_date(value: object, name: str) -> None:
        if not isinstance(value, date) or isinstance(value, datetime):
            raise TypeError(f"{name} must be a date")


@dataclass(frozen=True, slots=True)
class MappedCorporateAction:
    """记录官方权益日期映射到研究交易会话后的执行事件。

    入参：原始事件、映射登记日、除权日、支付日及送转上市日。返回值：账户可直接
    消费的不可变事件。异常：映射早于官方日期或阶段倒置时抛出 ``ValueError``。
    """

    event: CorporateAction
    mapped_record_date: date | None
    mapped_ex_date: date
    mapped_pay_date: date | None
    mapped_stock_listing_date: date | None

    def __post_init__(self) -> None:
        if not isinstance(self.event, CorporateAction):
            raise TypeError("event must be CorporateAction")
        pairs = ((self.mapped_ex_date, self.event.ex_date, "mapped_ex_date"),)
        for mapped, official, name in pairs:
            CorporateAction._require_date(mapped, name)
            if mapped < official:
                raise ValueError(f"{name} must not precede the official date")
        optional_pairs = (
            (self.mapped_record_date, self.event.record_date, "mapped_record_date"),
            (self.mapped_pay_date, self.event.pay_date, "mapped_pay_date"),
            (
                self.mapped_stock_listing_date,
                self.event.stock_listing_date,
                "mapped_stock_listing_date",
            ),
        )
        for optional_mapped, optional_official, optional_name in optional_pairs:
            if optional_mapped is not None:
                CorporateAction._require_date(optional_mapped, optional_name)
                if (
                    optional_official is None
                    or optional_mapped < optional_official
                ):
                    raise ValueError(
                        f"{optional_name} must not precede the official date"
                    )
        if (
            self.mapped_record_date is not None
            and self.mapped_record_date >= self.mapped_ex_date
        ):
            raise ValueError("mapped record date must not follow mapped ex date")
        if self.mapped_pay_date is not None and self.mapped_pay_date < self.mapped_ex_date:
            raise ValueError("mapped pay date must not precede mapped ex date")
        if (
            self.mapped_stock_listing_date is not None
            and self.mapped_stock_listing_date < self.mapped_ex_date
        ):
            raise ValueError("mapped stock listing date must not precede mapped ex date")


class CorporateActionCalendarMapper:
    """把官方自然日映射到不早于该日的首个已加载交易会话。

    入参：类仅提供无状态映射方法。返回值：映射后的确定性事件。异常：日历或事件
    不满足时点契约时抛出 ``TypeError`` 或 ``ValueError``。
    """

    @staticmethod
    def map(
        actions: tuple[CorporateAction, ...], calendar: TradingCalendar
    ) -> tuple[MappedCorporateAction, ...]:
        """映射并稳定排序权益事件。

        入参：按任意顺序提供的事件及完整研究日历。返回值：仅保留可在已加载日历
        中锁定权益的事件，并按映射日期、证券和事件 ID 排序。异常：重复事件 ID、
        日历或事件类型非法时抛出对应异常。
        """
        if not isinstance(calendar, TradingCalendar):
            raise TypeError("calendar must be TradingCalendar")
        if any(not isinstance(action, CorporateAction) for action in actions):
            raise TypeError("actions must contain CorporateAction")
        event_ids = [action.event_id for action in actions]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("corporate action event_id must be unique")
        sessions = calendar.sessions(calendar.start, calendar.end)
        mapped: list[MappedCorporateAction] = []
        for action in actions:
            record = (
                CorporateActionCalendarMapper._first_session(
                    sessions, action.record_date
                )
                if action.record_date is not None
                else None
            )
            ex_date = CorporateActionCalendarMapper._first_session(
                sessions, action.ex_date
            )
            if (action.record_date is not None and record is None) or ex_date is None:
                continue
            pay = (
                CorporateActionCalendarMapper._first_session(sessions, action.pay_date)
                if action.pay_date is not None
                else None
            )
            listing = (
                CorporateActionCalendarMapper._first_session(
                    sessions, action.stock_listing_date
                )
                if action.stock_listing_date is not None
                else None
            )
            mapped.append(MappedCorporateAction(action, record, ex_date, pay, listing))
        return tuple(
            sorted(
                mapped,
                key=lambda item: (
                    item.mapped_record_date or item.mapped_ex_date,
                    item.mapped_ex_date,
                    item.event.instrument_id.canonical(),
                    item.event.event_id,
                ),
            )
        )

    @staticmethod
    def _first_session(sessions: tuple[date, ...], official: date) -> date | None:
        index = bisect_left(sessions, official)
        return sessions[index] if index < len(sessions) else None


__all__ = [
    "CorporateAction",
    "CorporateActionCalendarMapper",
    "CorporateActionInstrumentType",
    "CorporateActionType",
    "MappedCorporateAction",
]
