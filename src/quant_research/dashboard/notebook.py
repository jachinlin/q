"""定义 Dashboard 查询本机 Notebook 就绪状态的消费者侧契约。"""

from __future__ import annotations

from typing import Protocol


class NotebookProbe(Protocol):
    """抽象 Dashboard 所需的本机 JupyterLab 就绪探测。

    入参：
        无；具体实现通过构造参数绑定目标地址和超时。
    返回值：
        构造并返回实现该协议的对象。
    异常：
        协议本身不抛出异常；实现必须把预期的连接失败收敛为未就绪。
    """

    def is_ready(self) -> bool:
        """返回本机 JupyterLab 当前是否可以接受 HTTP 请求。

        入参：
            无。
        返回值：
            服务可用时返回 ``True``，否则返回 ``False``。
        异常：
            实现不得传播连接拒绝、超时或无效响应等预期探测异常。
        """
        ...
