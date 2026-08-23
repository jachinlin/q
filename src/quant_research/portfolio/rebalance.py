"""把目标权重确定性翻译为 P3 整数股数订单。"""

from collections.abc import Callable, Mapping, Sequence
from math import floor, isfinite

from quant_research.domain.identifiers import InstrumentId
from quant_research.strategies.base import (
    AccountView,
    OrderIntent,
    OrderSide,
    TargetWeights,
)


class RebalancePlanner:
    """按账户权益、参考价和证券申报单位生成先卖后买订单。

    入参：
        构造时可注入按证券和方向查询申报单位的函数。
    返回值：
        ``plan`` 返回尚未撮合的整数股数订单序列。
    异常：
        参考价或申报单位无效时抛出 ``ValueError``。
    """

    def __init__(
        self, lot_size: Callable[[InstrumentId, OrderSide], int] | None = None
    ) -> None:
        self._lot_size = lot_size or (lambda _instrument, _side: 100)

    def plan(
        self,
        targets: TargetWeights,
        account: AccountView,
        reference_prices: Mapping[InstrumentId, float],
    ) -> Sequence[OrderIntent]:
        """生成不执行交易的目标差额订单。

        入参：
            targets 为目标权重；account 为信号日账户；reference_prices 为估值价。
        返回值：
            按证券稳定排序且卖单先于买单的 ``OrderIntent`` 元组。
        异常：
            任一涉及证券缺少有效价格，或申报单位非法时抛出 ``ValueError``。
        """
        identifiers = set(account.positions) | set(targets.weights)
        quantities: dict[InstrumentId, int] = {}
        for instrument in identifiers:
            price = reference_prices.get(instrument)
            if price is None or not isfinite(price) or price <= 0:
                raise ValueError(
                    f"missing valid reference price: {instrument.canonical()}"
                )
            desired = floor(
                account.equity_fen
                * targets.weights.get(instrument, 0.0)
                / (price * 100)
            )
            lot = self._lot_size(instrument, OrderSide.BUY)
            if type(lot) is not int or lot <= 0:
                raise ValueError("lot size must be a positive integer")
            quantities[instrument] = desired // lot * lot
        output: list[OrderIntent] = []
        for instrument in sorted(identifiers, key=InstrumentId.canonical):
            current, desired = (
                account.positions.get(instrument, 0),
                quantities[instrument],
            )
            if current > desired:
                amount = min(current - desired, account.sellable.get(instrument, 0))
                if amount > 0:
                    output.append(
                        OrderIntent(
                            instrument, OrderSide.SELL, amount, "TARGET_REBALANCE"
                        )
                    )
        for instrument in sorted(identifiers, key=InstrumentId.canonical):
            current, desired = (
                account.positions.get(instrument, 0),
                quantities[instrument],
            )
            if desired > current:
                output.append(
                    OrderIntent(
                        instrument, OrderSide.BUY, desired - current, "TARGET_REBALANCE"
                    )
                )
        return tuple(output)


__all__ = [
    "AccountView",
    "OrderIntent",
    "OrderSide",
    "RebalancePlanner",
    "TargetWeights",
]
