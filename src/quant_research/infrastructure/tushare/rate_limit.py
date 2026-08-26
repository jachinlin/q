"""提供 Tushare 进程内均匀请求限流。"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from quant_research.application.settings import DataSourceRateLimitSetting


class TushareRateLimiter:
    """把一个进程内的 Tushare 请求均匀分配到时间轴。

    入参：动态每分钟请求数 Provider、可注入单调时钟和休眠函数。
    返回值：构造可由多个客户端与线程共享的限流器。
    异常：Provider 返回非法类型或范围时在获取请求时隙前抛出。
    """

    def __init__(
        self,
        requests_per_minute: Callable[[], int],
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._requests_per_minute = requests_per_minute
        self._monotonic = monotonic
        self._sleeper = sleeper
        self._lock = threading.Lock()
        self._last_started_at: float | None = None

    def acquire(self) -> None:
        """等待并占用下一个均匀请求时隙。

        入参：无。
        返回值：获得时隙后返回。
        异常：动态设置非法时在休眠和网络调用前抛出类型或值错误。
        """
        with self._lock:
            rate = DataSourceRateLimitSetting.validated_value(
                self._requests_per_minute()
            )
            now = self._monotonic()
            if self._last_started_at is None:
                self._last_started_at = now
                return
            next_allowed = self._last_started_at + 60.0 / rate
            delay = max(0.0, next_allowed - now)
            if delay > 0.0:
                self._sleeper(delay)
            self._last_started_at = max(self._monotonic(), next_allowed)


__all__ = ["TushareRateLimiter"]
