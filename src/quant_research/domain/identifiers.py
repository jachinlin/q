"""提供领域基础与标识符相关的公开模型、协议与处理流程。"""

import re
from dataclasses import dataclass
from typing import Self
from uuid import UUID, uuid4

from quant_research.domain.enums import Exchange


@dataclass(frozen=True, slots=True)
class InstrumentId:
    """表示领域流程中的证券标识及其业务不变量。

    入参：
        exchange：证券挂牌交易所，用于形成规范证券标识。
        symbol：不含交易所后缀的六位证券代码。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        输入、状态或依赖结果违反契约时抛出 ``TypeError``、``ValueError``。
    A canonical exchange-qualified six-digit instrument identifier.
    """

    exchange: Exchange
    symbol: str

    def __post_init__(self) -> None:
        """Enforce identifier invariants for every construction path."""
        if not isinstance(self.exchange, Exchange):
            raise TypeError("exchange must be an Exchange")
        if (
            len(self.symbol) != 6
            or not self.symbol.isascii()
            or not self.symbol.isdecimal()
        ):
            raise ValueError("instrument symbol must be exactly six ASCII digits")

    @classmethod
    def parse(cls, value: str) -> "InstrumentId":
        """解析并校验输入值。

        入参：
            value：待校验或转换的值，类型为 ``str``。
        返回值：
            返回解析并校验领域后的``parse``（``'InstrumentId'``）。
        异常：
            ``ValueError``：输入、状态转换或完整性证据违反上述业务契约时抛出。
        Parse a strict Tushare-compatible ``SYMBOL.EXCHANGE`` identifier.
        """
        symbol, separator, suffix = value.partition(".")
        if separator != "." or "." in suffix:
            raise ValueError("instrument identifier must be SYMBOL.EXCHANGE")
        exchange_by_suffix = {
            "SH": Exchange.SSE,
            "SZ": Exchange.SZSE,
            "BJ": Exchange.BSE,
        }
        try:
            exchange = exchange_by_suffix[suffix]
        except KeyError as error:
            raise ValueError(f"unsupported exchange suffix: {suffix}") from error
        return cls(exchange=exchange, symbol=symbol)

    def canonical(self) -> str:
        """输出规范形式的领域。

        入参：
            无。
        返回值：
            返回Canonical（``str``）。
        异常：
            无。
        Return the identifier in canonical ``SYMBOL.EXCHANGE`` form.
        """
        suffix = {
            Exchange.SSE: "SH",
            Exchange.SZSE: "SZ",
            Exchange.BSE: "BJ",
        }[self.exchange]
        return f"{self.symbol}.{suffix}"


@dataclass(frozen=True, slots=True)
class IndexId:
    """表示不可直接交易的指数标识。

    入参：``value`` 为 Tushare 规范的指数代码。返回值：不可变指数标识。
    异常：代码不满足 ``CODE.SUFFIX`` 约束时抛出 ``ValueError``。
    """

    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, str):
            raise TypeError("index identifier must be a string")
        if re.fullmatch(r"[0-9A-Z]{1,16}\.[A-Z]{2,8}", self.value) is None:
            raise ValueError("index identifier must be an uppercase CODE.SUFFIX")

    @classmethod
    def parse(cls, value: str) -> Self:
        """解析 Tushare 指数代码。

        入参：待解析字符串。返回值：``IndexId``。异常：格式非法时抛出
        ``TypeError`` 或 ``ValueError``。
        """
        return cls(value)

    def canonical(self) -> str:
        """返回规范指数代码。

        入参：无。返回值：``CODE.SUFFIX`` 字符串。异常：无。
        """
        return self.value

    def __str__(self) -> str:
        """返回代码。入参：无。返回值：规范字符串。异常：无。"""
        return self.value


@dataclass(frozen=True, slots=True)
class QualityRunId:
    """表示领域流程中的质量校验运行标识及其业务不变量。

    入参：
        value：待校验或转换的值，类型为 ``UUID``。
    返回值：
        返回完成字段规范化和不变量校验的对象。
    异常：
        无；构造阶段只保存已提供的依赖或值对象。
    UUID identity for one persisted quality evaluation.
    """

    value: UUID

    def __post_init__(self) -> None:
        _IdentifiersSupport._require_uuid(self.value)

    @classmethod
    def parse(cls, value: str) -> Self:
        """解析并校验输入值。

        入参：
            value：待校验或转换的值，类型为 ``str``。
        返回值：
            返回解析并校验领域后的``parse``（``Self``）。
        异常：
            无。
        """
        return cls(_IdentifiersSupport._parse_canonical_uuid(value))

    @classmethod
    def new(cls) -> Self:
        """处理领域中的``new``。

        入参：
            无。
        返回值：
            返回``new``（``Self``）。
        异常：
            无。
        """
        return cls(uuid4())

    def __str__(self) -> str:
        """返回对象的稳定文本表示。

        入参：
            无。
        返回值：
            返回``str``（``str``）。
        异常：
            无。
        """
        return str(self.value)


class _IdentifiersSupport:
    """集中承载本模块的私有实现逻辑。"""

    @staticmethod
    def _parse_canonical_uuid(value: str) -> UUID:
        try:
            parsed = UUID(value)
        except (AttributeError, TypeError, ValueError) as error:
            raise ValueError("identifier must be a canonical UUID") from error
        if str(parsed) != value:
            raise ValueError("identifier must be a canonical UUID")
        return parsed

    @staticmethod
    def _require_uuid(value: object) -> None:
        if not isinstance(value, UUID):
            raise TypeError("identifier value must be a UUID")
