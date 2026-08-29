"""提供按证券产品画像解析的历史交易规则。"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import StrEnum
from itertools import pairwise
from math import isfinite
from pathlib import Path
from typing import Any, Protocol

import yaml

from quant_research.domain.enums import Board, Exchange
from quant_research.domain.identifiers import InstrumentId

_RULE_START = date(2005, 1, 24)
_CENT = Decimal(100)


class SecurityStatus(StrEnum):
    """定义影响日涨跌幅的证券风险状态。

    入参：
        无。
    返回值：
        构造并返回证券风险状态枚举值。
    异常：
        ValueError：枚举值不存在时抛出。
    """

    NORMAL = "NORMAL"
    ST = "ST"
    NO_LIMIT = "NO_LIMIT"


class Side(StrEnum):
    """定义模拟成交方向。

    入参：
        无。
    返回值：
        构造并返回买卖方向枚举值。
    异常：
        ValueError：枚举值不存在时抛出。
    """

    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True, slots=True)
class PriceBand:
    """表示以元计价的日涨跌停价格边界。

    入参：
        upper：涨停价格。
        lower：跌停价格。
    返回值：
        构造并返回不可变价格边界。
    异常：
        无主动抛出的异常。
    """

    upper: float
    lower: float


@dataclass(frozen=True, slots=True)
class PriceLimitParameters:
    """表示可供向量化计算使用的精确涨跌停参数。

    入参：
        rate_numerator、rate_denominator：涨跌幅比例的最简整数分数。
        price_scale：把以元计价的价格转换为整数价格单位的十进制倍数。
        tick_units：一个最小报价变动包含的整数价格单位数。
    返回值：
        构造并返回不含二进制浮点近似的不可变参数。
    异常：
        TypeError：字段不是整数时抛出。
        ValueError：分子为负或分母、价格倍数、报价单位数不是正数时抛出。
    """

    rate_numerator: int
    rate_denominator: int
    price_scale: int
    tick_units: int

    def __post_init__(self) -> None:
        for value, name in (
            (self.rate_numerator, "rate_numerator"),
            (self.rate_denominator, "rate_denominator"),
            (self.price_scale, "price_scale"),
            (self.tick_units, "tick_units"),
        ):
            if type(value) is not int:
                raise TypeError(f"{name} must be an integer")
        if self.rate_numerator < 0:
            raise ValueError("rate_numerator must be nonnegative")
        if self.rate_denominator <= 0:
            raise ValueError("rate_denominator must be positive")
        if self.price_scale <= 0:
            raise ValueError("price_scale must be positive")
        if self.tick_units <= 0:
            raise ValueError("tick_units must be positive")


@dataclass(frozen=True, slots=True)
class FeeBreakdown:
    """表示全部以分计价的成交费用分项。

    入参：
        commission_cents：佣金，单位为分。
        stamp_duty_cents：印花税，单位为分。
        transfer_fee_cents：过户费，单位为分。
        total_cents：费用合计，单位为分。
    返回值：
        构造并返回不可变费用分项。
    异常：
        无主动抛出的异常。
    """

    commission_cents: int
    stamp_duty_cents: int
    transfer_fee_cents: int
    total_cents: int

    def as_tuple(self) -> tuple[int, int, int, int]:
        """按固定顺序返回费用分项。

        入参：
            无。
        返回值：
            返回佣金、印花税、过户费和合计组成的元组。
        异常：
            无主动抛出的异常。
        """

        return (
            self.commission_cents,
            self.stamp_duty_cents,
            self.transfer_fee_cents,
            self.total_cents,
        )


@dataclass(frozen=True, slots=True)
class InstrumentTradingProfile:
    """描述一个证券产品在指定交易制度下的不可变交易画像。

    入参：
        profile_id：规则文件中的稳定画像标识。
        instrument_type：Canonical 证券类型。
        price_tick：以元计价的最小价格变动单位。
        buy_minimum、buy_increment：买入最低数量及递增单位。
        sell_minimum、sell_increment：卖出最低数量及递增单位。
        allow_full_odd_lot_sell：是否允许将不满足标准单位的全部余额一次卖出。
        settlement_sessions：买入后需要等待多少个交易日才能卖出。
        price_limit_group：涨跌幅历史规则组。
        fee_group：费用规则组。
    返回值：
        构造并返回经过严格校验的交易画像。
    异常：
        字段无效时抛出 ``TypeError`` 或 ``ValueError``。
    """

    profile_id: str
    instrument_type: str
    price_tick: Decimal
    buy_minimum: int
    buy_increment: int
    sell_minimum: int
    sell_increment: int
    allow_full_odd_lot_sell: bool
    settlement_sessions: int
    price_limit_group: str
    fee_group: str

    def __post_init__(self) -> None:
        for text_value, name in (
            (self.profile_id, "profile_id"),
            (self.instrument_type, "instrument_type"),
            (self.price_limit_group, "price_limit_group"),
            (self.fee_group, "fee_group"),
        ):
            if not isinstance(text_value, str) or not text_value:
                raise ValueError(f"{name} must be a nonempty string")
        if not isinstance(self.price_tick, Decimal) or not self.price_tick.is_finite():
            raise TypeError("price_tick must be a finite Decimal")
        if self.price_tick <= 0:
            raise ValueError("price_tick must be positive")
        for integer_value, name in (
            (self.buy_minimum, "buy_minimum"),
            (self.buy_increment, "buy_increment"),
            (self.sell_minimum, "sell_minimum"),
            (self.sell_increment, "sell_increment"),
        ):
            if type(integer_value) is not int or integer_value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if type(self.allow_full_odd_lot_sell) is not bool:
            raise TypeError("allow_full_odd_lot_sell must be a bool")
        if type(self.settlement_sessions) is not int or self.settlement_sessions < 0:
            raise ValueError("settlement_sessions must be a nonnegative integer")

    def normalize_quantity(
        self,
        side: Side,
        desired: int,
        *,
        position_quantity: int | None = None,
    ) -> int:
        """返回不超过期望数量且满足本画像约束的最大申报数量。

        入参：
            side：买卖方向。
            desired：未经合法化的期望数量。
            position_quantity：卖出时的当前持仓数量。
        返回值：
            返回合法化后的最大数量；不存在合法数量时返回零。
        异常：
            TypeError：买卖方向类型错误时抛出。
            ValueError：期望数量或持仓数量无效时抛出。
        """

        if not isinstance(side, Side):
            raise TypeError("side must be a Side")
        if type(desired) is not int or desired < 0:
            raise ValueError("desired quantity must be a nonnegative integer")
        if position_quantity is not None and (
            type(position_quantity) is not int or position_quantity < 0
        ):
            raise ValueError("position_quantity must be a nonnegative integer or None")
        if (
            side is Side.SELL
            and self.allow_full_odd_lot_sell
            and position_quantity is not None
            and desired >= position_quantity
        ):
            return position_quantity
        minimum = self.buy_minimum if side is Side.BUY else self.sell_minimum
        increment = self.buy_increment if side is Side.BUY else self.sell_increment
        if desired < minimum:
            return 0
        return minimum + (desired - minimum) // increment * increment

    def is_quantity_valid(
        self,
        side: Side,
        quantity: int,
        position_quantity: int | None = None,
    ) -> bool:
        """判断申报数量是否满足画像规则。

        入参：
            side：买卖方向。
            quantity：待校验的申报数量。
            position_quantity：卖出时的当前持仓数量。
        返回值：
            满足最低数量、递增单位或整笔零股卖出规则时返回真。
        异常：
            TypeError：买卖方向类型错误时抛出。
            ValueError：持仓数量无效时抛出。
        """

        if not isinstance(side, Side):
            raise TypeError("side must be a Side")
        if type(quantity) is not int or quantity <= 0:
            return False
        if position_quantity is not None and (
            type(position_quantity) is not int or position_quantity < 0
        ):
            raise ValueError("position_quantity must be a nonnegative integer or None")
        minimum = self.buy_minimum if side is Side.BUY else self.sell_minimum
        increment = self.buy_increment if side is Side.BUY else self.sell_increment
        standard = quantity >= minimum and (quantity - minimum) % increment == 0
        full_odd_lot = (
            side is Side.SELL
            and self.allow_full_odd_lot_sell
            and position_quantity is not None
            and quantity == position_quantity
        )
        return standard or full_odd_lot


@dataclass(frozen=True, slots=True)
class SimulatedFill:
    """封装计算历史成交费用所需的成交输入。

    入参：
        instrument：成交证券标识。
        trade_date：成交日期。
        side：成交方向。
        quantity：成交数量。
        price：成交单价，单位为元。
    返回值：
        构造并返回不可变成交输入。
    异常：
        TypeError：字段类型错误时抛出。
        ValueError：日期、数量或价格无效时抛出。
    """

    instrument: InstrumentId
    trade_date: date
    side: Side
    quantity: int
    price: float

    def __post_init__(self) -> None:
        _RulebookSupport.validate_instrument_and_date(self.instrument, self.trade_date)
        if not isinstance(self.side, Side):
            raise TypeError("side must be a Side")
        if type(self.quantity) is not int or self.quantity <= 0:
            raise ValueError("quantity must be a positive integer")
        _RulebookSupport.decimal_price(self.price)


class MarketRuleBook(Protocol):
    """定义按产品画像提供历史市场约束的依赖端口。

    入参：
        无。
    返回值：
        由具体规则簿实现提供画像、涨跌幅和费用计算。
    异常：
        由具体实现按方法契约抛出。
    """

    @property
    def content_hash(self) -> str:
        """返回规则文件的内容哈希。

        入参：
            无。
        返回值：
            返回小写 SHA-256 十六进制字符串。
        异常：
            由具体实现按契约抛出。
        """

        ...

    def trading_profile(
        self,
        instrument: InstrumentId,
        instrument_type: str,
        board: Board,
        trade_date: date,
    ) -> InstrumentTradingProfile:
        """解析指定证券和日期的交易画像。

        入参：
            instrument：证券标识。
            instrument_type：Canonical 证券类型。
            board：证券板块。
            trade_date：交易日期。
        返回值：
            返回该证券在指定日期生效的不可变交易画像。
        异常：
            TypeError：输入类型错误时抛出。
            ValueError：无法可靠分类或缺少显式映射时抛出。
        """

        ...

    def price_limits(
        self,
        profile: InstrumentTradingProfile,
        trade_date: date,
        prev_close: float,
        status: SecurityStatus,
    ) -> PriceBand | None:
        """根据画像和历史日期返回涨跌停边界。

        入参：
            profile：证券交易画像。
            trade_date：交易日期。
            prev_close：前收盘价，单位为元。
            status：证券风险状态。
        返回值：
            返回价格边界；无涨跌幅限制时返回空值。
        异常：
            TypeError：输入类型错误时抛出。
            ValueError：价格或规则覆盖无效时抛出。
        """

        ...

    def price_limit_parameters(
        self,
        profile: InstrumentTradingProfile,
        trade_date: date,
        status: SecurityStatus,
    ) -> PriceLimitParameters:
        """返回向量化涨跌停判定所需的精确整数参数。

        入参：交易画像、交易日和风险状态。返回值：整数比例和价格单位倍数。
        异常：画像、日期、状态或规则覆盖非法时传播对应异常。
        """

        ...

    def fees(
        self, fill: SimulatedFill, profile: InstrumentTradingProfile
    ) -> FeeBreakdown:
        """根据成交和交易画像计算费用。

        入参：
            fill：模拟成交输入。
            profile：证券交易画像。
        返回值：
            返回全部以分表示的费用分项。
        异常：
            TypeError：输入类型错误时抛出。
            ValueError：费用组或历史规则无效时抛出。
        """

        ...


@dataclass(frozen=True, slots=True)
class _IntervalRule:
    start: date
    end: date | None
    value: Decimal
    basis: str | None = None


class AShareRuleBook:
    """从唯一 YAML 规则文件加载股票和显式 ETF 历史交易制度。

    入参：
        content_hash：规则文件内容哈希。
        profiles：画像标识到不可变交易画像的映射。
        automatic_profiles：股票类型和板块到画像标识的映射。
        instrument_profiles：证券标识到显式画像标识的映射。
        price_limits：各涨跌幅组的历史规则。
        stamp_duty：各买卖方向的印花税历史规则。
        transfer_fee：各交易所的过户费历史规则。
        commission_rate：佣金率。
        commission_minimum_cents：最低佣金，单位为分。
    返回值：
        构造并返回内存中的 A 股规则簿。
    异常：
        规则文件解析入口负责校验构造数据。
    """

    def __init__(
        self,
        content_hash: str,
        profiles: dict[str, InstrumentTradingProfile],
        automatic_profiles: dict[tuple[str, Board], str],
        instrument_profiles: dict[InstrumentId, str],
        price_limits: dict[tuple[str, SecurityStatus], tuple[_IntervalRule, ...]],
        stamp_duty: dict[Side, tuple[_IntervalRule, ...]],
        transfer_fee: dict[Exchange, tuple[_IntervalRule, ...]],
        commission_rate: Decimal,
        commission_minimum_cents: int,
    ) -> None:
        self._content_hash = content_hash
        self._profiles = profiles
        self._automatic_profiles = automatic_profiles
        self._instrument_profiles = instrument_profiles
        self._price_limits = price_limits
        self._stamp_duty = stamp_duty
        self._transfer_fee = transfer_fee
        self._commission_rate = commission_rate
        self._commission_minimum_cents = commission_minimum_cents

    @property
    def content_hash(self) -> str:
        """返回规则文件原始字节的内容哈希。

        入参：
            无。
        返回值：
            返回小写 SHA-256 十六进制字符串。
        异常：
            无主动抛出的异常。
        """

        return self._content_hash

    @property
    def commission_bps(self) -> float:
        """返回事前成本模型复用的佣金比例，单位为基点。

        入参：无。
        返回值：唯一规则文件中的佣金率乘以一万。
        异常：规则簿构造时已校验，本属性不主动抛出异常。
        """
        return float(self._commission_rate * Decimal(10_000))

    @property
    def commission_minimum_fen(self) -> int:
        """返回事前成本模型复用的单笔最低佣金，单位为分。

        入参：无。
        返回值：唯一规则文件中的最低佣金整数分值。
        异常：无。
        """
        return self._commission_minimum_cents

    @classmethod
    def load(cls, config_path: Path) -> AShareRuleBook:
        """读取并严格校验唯一交易规则文件。

        入参：
            config_path：UTF-8 YAML 规则文件路径。
        返回值：
            返回完整覆盖已声明画像的规则簿。
        异常：
            文件不可读、结构无效或规则覆盖不完整时抛出 ``ValueError``。
        """

        if not isinstance(config_path, Path):
            raise TypeError("config_path must be an explicit Path")
        try:
            encoded = config_path.read_bytes()
            loaded = yaml.safe_load(encoded.decode("utf-8"))
        except OSError as error:
            raise ValueError("rulebook config cannot be read") from error
        root = _RulebookSupport.mapping(loaded, "rulebook config")
        commission = _RulebookSupport.mapping(root.get("commission"), "commission")
        minimum = commission.get("minimum_cents")
        if type(minimum) is not int or minimum < 0:
            raise ValueError("commission minimum_cents must be nonnegative")
        profiles = _RulebookSupport.profiles(root.get("trading_profiles"))
        automatic = _RulebookSupport.automatic_profiles(
            root.get("automatic_profiles"), profiles
        )
        instruments = _RulebookSupport.instrument_profiles(
            root.get("instrument_profiles"), profiles
        )
        limits = _RulebookSupport.price_limit_rules(root.get("price_limits"), profiles)
        stamp = _RulebookSupport.fee_rules(root.get("stamp_duty"), Side, "stamp duty")
        transfer = _RulebookSupport.fee_rules(
            root.get("transfer_fee"), Exchange, "transfer fee", require_basis=True
        )
        return cls(
            hashlib.sha256(encoded).hexdigest(),
            profiles,
            automatic,
            instruments,
            limits,
            stamp,
            transfer,
            _RulebookSupport.decimal(commission.get("rate"), "commission rate"),
            minimum,
        )

    def trading_profile(
        self,
        instrument: InstrumentId,
        instrument_type: str,
        board: Board,
        trade_date: date,
    ) -> InstrumentTradingProfile:
        """解析证券交易画像。

        入参：
            instrument：证券标识。
            instrument_type：Canonical 证券类型。
            board：证券板块。
            trade_date：交易日期。
        返回值：
            返回匹配的不可变交易画像。
        异常：
            TypeError：输入类型错误时抛出。
            ValueError：ETF 未显式配置或自动画像缺失、冲突时抛出。
        """

        _RulebookSupport.validate_instrument_and_date(instrument, trade_date)
        if not isinstance(instrument_type, str) or not instrument_type:
            raise TypeError("instrument_type must be a nonempty string")
        if not isinstance(board, Board):
            raise TypeError("board must be a Board")
        profile_id = self._instrument_profiles.get(instrument)
        if profile_id is None:
            if instrument_type == "ETF":
                raise ValueError(
                    f"ETF trading profile is not configured: {instrument.canonical()}"
                )
            try:
                profile_id = self._automatic_profiles[(instrument_type, board)]
            except KeyError as error:
                raise ValueError(
                    f"trading profile is not configured for {instrument_type}:{board.value}"
                ) from error
        profile = self._profiles[profile_id]
        if profile.instrument_type != instrument_type:
            raise ValueError(
                "trading profile instrument_type does not match canonical data"
            )
        return profile

    def price_limits(
        self,
        profile: InstrumentTradingProfile,
        trade_date: date,
        prev_close: float,
        status: SecurityStatus,
    ) -> PriceBand | None:
        """按画像价格步长计算指定日期的涨跌停边界。

        入参：
            profile：证券交易画像。
            trade_date：交易日期。
            prev_close：前收盘价，单位为元。
            status：证券风险状态。
        返回值：
            返回按价格 tick 四舍五入后的涨跌停边界。
        异常：
            TypeError：画像、日期或状态类型错误时抛出。
            ValueError：前收盘价或历史涨跌幅规则无效时抛出。
        """

        _RulebookSupport.validate_profile_and_date(profile, trade_date)
        if not isinstance(status, SecurityStatus):
            raise TypeError("status must be a SecurityStatus")
        if status is SecurityStatus.NO_LIMIT:
            return None
        try:
            rules = self._price_limits[(profile.price_limit_group, status)]
        except KeyError as error:
            raise ValueError(
                "price limit group does not cover security status"
            ) from error
        rate = _RulebookSupport.matching_rate(rules, trade_date)
        close = _RulebookSupport.decimal_price(prev_close)
        upper = (
            (close * (Decimal(1) + rate) / profile.price_tick).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
            * profile.price_tick
        )
        lower = (
            (close * (Decimal(1) - rate) / profile.price_tick).quantize(
                Decimal(1), rounding=ROUND_HALF_UP
            )
            * profile.price_tick
        )
        return PriceBand(float(upper), float(lower))

    def price_limit_parameters(
        self,
        profile: InstrumentTradingProfile,
        trade_date: date,
        status: SecurityStatus,
    ) -> PriceLimitParameters:
        """返回向量化涨跌停判定所需的精确整数参数。

        入参：交易画像、交易日和风险状态。返回值：整数比例和价格单位倍数。
        异常：画像、日期、状态或规则覆盖非法时抛出 ``TypeError`` 或 ``ValueError``。
        """
        _RulebookSupport.validate_profile_and_date(profile, trade_date)
        if not isinstance(status, SecurityStatus):
            raise TypeError("status must be a SecurityStatus")
        try:
            rules = self._price_limits[(profile.price_limit_group, status)]
        except KeyError as error:
            raise ValueError(
                "price limit group does not cover security status"
            ) from error
        rate = _RulebookSupport.matching_rate(rules, trade_date)
        numerator, denominator = rate.as_integer_ratio()
        exponent = profile.price_tick.as_tuple().exponent
        if not isinstance(exponent, int):
            raise TypeError("price tick exponent must be an integer")
        price_scale = 10 ** max(0, -exponent)
        tick_units = int(profile.price_tick * price_scale)
        return PriceLimitParameters(
            numerator, denominator, price_scale, tick_units
        )

    def fees(
        self, fill: SimulatedFill, profile: InstrumentTradingProfile
    ) -> FeeBreakdown:
        """按画像费用组计算佣金、印花税和过户费。

        入参：
            fill：模拟成交输入。
            profile：证券交易画像。
        返回值：
            返回全部以分表示的成交费用分项。
        异常：
            TypeError：成交或画像类型错误时抛出。
            ValueError：费用组或历史费用规则无效时抛出。
        """

        if not isinstance(fill, SimulatedFill):
            raise TypeError("fill must be a SimulatedFill")
        if not isinstance(profile, InstrumentTradingProfile):
            raise TypeError("profile must be an InstrumentTradingProfile")
        if profile.instrument_type not in {"STOCK", "ETF"}:
            raise ValueError("fees support only STOCK and ETF profiles")
        amount = _RulebookSupport.decimal_price(fill.price) * fill.quantity
        commission = max(
            _RulebookSupport.cents(amount * self._commission_rate),
            self._commission_minimum_cents,
        )
        if profile.fee_group == "ETF":
            return FeeBreakdown(commission, 0, 0, commission)
        if profile.fee_group != "STOCK":
            raise ValueError("unsupported fee group")
        stamp = _RulebookSupport.cents(
            amount
            * _RulebookSupport.matching_rate(
                self._stamp_duty[fill.side], fill.trade_date
            )
        )
        transfer_rule = _RulebookSupport.matching_rule(
            self._transfer_fee[fill.instrument.exchange], fill.trade_date
        )
        transfer_base = (
            Decimal(fill.quantity) if transfer_rule.basis == "face_value" else amount
        )
        transfer = _RulebookSupport.cents(transfer_base * transfer_rule.value)
        return FeeBreakdown(commission, stamp, transfer, commission + stamp + transfer)


class _RulebookSupport:
    """集中承载规则文件解析与确定性金额计算。"""

    @staticmethod
    def profiles(value: object) -> dict[str, InstrumentTradingProfile]:
        rows = _RulebookSupport.mapping(value, "trading_profiles")
        profiles: dict[str, InstrumentTradingProfile] = {}
        for profile_id, raw in rows.items():
            row = _RulebookSupport.mapping(raw, f"trading profile {profile_id}")
            profiles[profile_id] = InstrumentTradingProfile(
                profile_id=profile_id,
                instrument_type=_RulebookSupport.text(
                    row.get("instrument_type"), "instrument_type"
                ),
                price_tick=_RulebookSupport.positive_decimal(
                    row.get("price_tick"), "price_tick"
                ),
                buy_minimum=_RulebookSupport.positive_int(
                    row.get("buy_minimum"), "buy_minimum"
                ),
                buy_increment=_RulebookSupport.positive_int(
                    row.get("buy_increment"), "buy_increment"
                ),
                sell_minimum=_RulebookSupport.positive_int(
                    row.get("sell_minimum"), "sell_minimum"
                ),
                sell_increment=_RulebookSupport.positive_int(
                    row.get("sell_increment"), "sell_increment"
                ),
                allow_full_odd_lot_sell=_RulebookSupport.boolean(
                    row.get("allow_full_odd_lot_sell"), "allow_full_odd_lot_sell"
                ),
                settlement_sessions=_RulebookSupport.nonnegative_int(
                    row.get("settlement_sessions"), "settlement_sessions"
                ),
                price_limit_group=_RulebookSupport.text(
                    row.get("price_limit_group"), "price_limit_group"
                ),
                fee_group=_RulebookSupport.text(row.get("fee_group"), "fee_group"),
            )
        if not profiles:
            raise ValueError("trading_profiles must not be empty")
        return profiles

    @staticmethod
    def automatic_profiles(
        value: object, profiles: dict[str, InstrumentTradingProfile]
    ) -> dict[tuple[str, Board], str]:
        rows = _RulebookSupport.mapping(value, "automatic_profiles")
        result: dict[tuple[str, Board], str] = {}
        for raw_key, raw_profile in rows.items():
            parts = raw_key.split(":")
            if len(parts) != 2:
                raise ValueError("automatic profile key must be TYPE:BOARD")
            try:
                board = Board(parts[1])
            except ValueError as error:
                raise ValueError("automatic profile board is invalid") from error
            profile_id = _RulebookSupport.text(raw_profile, "automatic profile")
            profile = _RulebookSupport.require_profile(profiles, profile_id)
            if profile.instrument_type != parts[0]:
                raise ValueError("automatic profile instrument_type mismatch")
            result[(parts[0], board)] = profile_id
        return result

    @staticmethod
    def instrument_profiles(
        value: object, profiles: dict[str, InstrumentTradingProfile]
    ) -> dict[InstrumentId, str]:
        rows = _RulebookSupport.mapping(value, "instrument_profiles")
        result: dict[InstrumentId, str] = {}
        for raw_instrument, raw_profile in rows.items():
            instrument = InstrumentId.parse(raw_instrument)
            profile_id = _RulebookSupport.text(raw_profile, "instrument profile")
            profile = _RulebookSupport.require_profile(profiles, profile_id)
            if profile.instrument_type != "ETF":
                raise ValueError(
                    "explicit instrument profile must reference an ETF profile"
                )
            result[instrument] = profile_id
        return result

    @staticmethod
    def price_limit_rules(
        value: object, profiles: dict[str, InstrumentTradingProfile]
    ) -> dict[tuple[str, SecurityStatus], tuple[_IntervalRule, ...]]:
        grouped: dict[tuple[str, SecurityStatus], list[_IntervalRule]] = {}
        for raw in _RulebookSupport.list_value(value, "price_limits"):
            row = _RulebookSupport.mapping(raw, "price limit rule")
            group = _RulebookSupport.text(row.get("group"), "price limit group")
            status = _RulebookSupport.enum(
                SecurityStatus, row.get("status"), "security status"
            )
            grouped.setdefault((group, status), []).append(
                _RulebookSupport.interval(row, "rate")
            )
        required = {
            (profile.price_limit_group, SecurityStatus.NORMAL)
            for profile in profiles.values()
        }
        required.update(
            (profile.price_limit_group, SecurityStatus.ST)
            for profile in profiles.values()
            if profile.instrument_type == "STOCK"
        )
        if not required.issubset(grouped):
            raise ValueError("price limits do not cover every trading profile")
        return {
            key: _RulebookSupport.validate_intervals(key, rules)
            for key, rules in grouped.items()
        }

    @staticmethod
    def fee_rules(
        value: object,
        enum_type: type[Side | Exchange],
        name: str,
        *,
        require_basis: bool = False,
    ) -> dict[Any, tuple[_IntervalRule, ...]]:
        grouped: dict[Any, list[_IntervalRule]] = {}
        for raw in _RulebookSupport.list_value(value, name):
            row = _RulebookSupport.mapping(raw, f"{name} rule")
            key = _RulebookSupport.enum(
                enum_type,
                row.get("side") if enum_type is Side else row.get("exchange"),
                name,
            )
            rule = _RulebookSupport.interval(row, "rate")
            if require_basis:
                basis = row.get("basis")
                if basis not in {"turnover", "face_value"}:
                    raise ValueError("transfer fee basis is invalid")
                rule = _IntervalRule(rule.start, rule.end, rule.value, str(basis))
            grouped.setdefault(key, []).append(rule)
        if set(grouped) != set(enum_type):
            raise ValueError(f"{name} must cover every dimension")
        return {
            key: _RulebookSupport.validate_intervals(key, rules)
            for key, rules in grouped.items()
        }

    @staticmethod
    def interval(row: dict[str, object], field: str) -> _IntervalRule:
        start = _RulebookSupport.date_value(row.get("start"), "rule start")
        raw_end = row.get("end")
        end = (
            None
            if raw_end is None
            else _RulebookSupport.date_value(raw_end, "rule end")
        )
        if end is not None and end < start:
            raise ValueError("rule date interval must be ordered")
        return _IntervalRule(
            start, end, _RulebookSupport.decimal(row.get(field), field)
        )

    @staticmethod
    def validate_intervals(
        key: object, rules: list[_IntervalRule]
    ) -> tuple[_IntervalRule, ...]:
        for previous, current in pairwise(rules):
            if current.start <= previous.start:
                raise ValueError(f"rule intervals for {key} must increase")
        if not rules or rules[0].start != _RULE_START:
            raise ValueError(f"rule intervals for {key} must start at {_RULE_START}")
        for index, rule in enumerate(rules):
            if rule.end is None:
                if index != len(rules) - 1:
                    raise ValueError(f"rule intervals for {key} overlap")
                continue
            if index == len(rules) - 1:
                raise ValueError(f"rule intervals for {key} need an open end")
            if (rules[index + 1].start - rule.end).days != 1:
                raise ValueError(f"rule intervals for {key} have a gap or overlap")
        return tuple(rules)

    @staticmethod
    def matching_rate(rules: tuple[_IntervalRule, ...], trade_date: date) -> Decimal:
        return _RulebookSupport.matching_rule(rules, trade_date).value

    @staticmethod
    def matching_rule(
        rules: tuple[_IntervalRule, ...], trade_date: date
    ) -> _IntervalRule:
        for rule in rules:
            if rule.start <= trade_date and (
                rule.end is None or trade_date <= rule.end
            ):
                return rule
        raise ValueError("no configured rule matches trade date")

    @staticmethod
    def mapping(value: object, name: str) -> dict[str, object]:
        if not isinstance(value, dict) or not all(
            isinstance(key, str) for key in value
        ):
            raise ValueError(f"{name} must be a string-keyed mapping")
        return value

    @staticmethod
    def list_value(value: object, name: str) -> list[object]:
        if not isinstance(value, list):
            raise TypeError(f"{name} must be a list")
        return value

    @staticmethod
    def enum(enum_type: type[Any], value: object, name: str) -> Any:
        if not isinstance(value, str):
            raise TypeError(f"{name} must be a valid enum value")
        try:
            return enum_type(value)
        except ValueError as error:
            raise ValueError(f"{name} must be a valid enum value") from error

    @staticmethod
    def date_value(value: object, name: str) -> date:
        if not isinstance(value, date) or isinstance(value, datetime):
            raise TypeError(f"{name} must be a date")
        return value

    @staticmethod
    def decimal(value: object, name: str) -> Decimal:
        if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
            raise TypeError(f"{name} must be a finite nonnegative decimal")
        result = Decimal(str(value))
        if not result.is_finite() or result < 0:
            raise ValueError(f"{name} must be a finite nonnegative decimal")
        return result

    @staticmethod
    def positive_decimal(value: object, name: str) -> Decimal:
        result = _RulebookSupport.decimal(value, name)
        if result <= 0:
            raise ValueError(f"{name} must be positive")
        return result

    @staticmethod
    def decimal_price(value: object) -> Decimal:
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            raise TypeError("price must be finite positive")
        if isinstance(value, float) and not isfinite(value):
            raise ValueError("price must be finite positive")
        result = Decimal(str(value))
        if not result.is_finite() or result <= 0:
            raise ValueError("price must be finite positive")
        return result

    @staticmethod
    def cents(yuan: Decimal) -> int:
        return int((yuan * _CENT).quantize(Decimal(1), rounding=ROUND_HALF_UP))

    @staticmethod
    def text(value: object, name: str) -> str:
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must be a nonempty string")
        return value

    @staticmethod
    def positive_int(value: object, name: str) -> int:
        if type(value) is not int or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
        return value

    @staticmethod
    def nonnegative_int(value: object, name: str) -> int:
        if type(value) is not int or value < 0:
            raise ValueError(f"{name} must be a nonnegative integer")
        return value

    @staticmethod
    def boolean(value: object, name: str) -> bool:
        if type(value) is not bool:
            raise TypeError(f"{name} must be a bool")
        return value

    @staticmethod
    def require_profile(
        profiles: dict[str, InstrumentTradingProfile], profile_id: str
    ) -> InstrumentTradingProfile:
        try:
            return profiles[profile_id]
        except KeyError as error:
            raise ValueError(f"unknown trading profile: {profile_id}") from error

    @staticmethod
    def validate_instrument_and_date(
        instrument: InstrumentId, trade_date: date
    ) -> None:
        if not isinstance(instrument, InstrumentId):
            raise TypeError("instrument must be an InstrumentId")
        if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
            raise TypeError("trade_date must be a date")

    @staticmethod
    def validate_profile_and_date(
        profile: InstrumentTradingProfile, trade_date: date
    ) -> None:
        if not isinstance(profile, InstrumentTradingProfile):
            raise TypeError("profile must be an InstrumentTradingProfile")
        if not isinstance(trade_date, date) or isinstance(trade_date, datetime):
            raise TypeError("trade_date must be a date")
