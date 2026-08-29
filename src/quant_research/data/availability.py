"""定义自然日数据共享的完整日截止策略。"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


class CalendarDataCompletion:
    """解析自然日数据已经完整落定的最近日期。

    入参：由 ``cutoff_date`` 接收评估时点、业务时区和完整小时。
    返回值：返回可安全抓取并用于新鲜度比较的自然日。
    异常：评估时点无时区或完整小时非法时抛出 ``ValueError``。
    """

    DEFAULT_HOUR = 18

    @staticmethod
    def cutoff_date(
        evaluated_at: datetime,
        *,
        timezone: ZoneInfo,
        completion_hour: int = DEFAULT_HOUR,
    ) -> date:
        """返回评估时点对应的最近完整自然日。

        入参：
            evaluated_at：带时区的评估时点。
            timezone：用于解释业务日期和小时的 IANA 时区。
            completion_hour：当天数据视为完整的本地小时。
        返回值：达到完整小时后返回当天，否则返回前一天。
        异常：评估时点无时区或完整小时不在 0 至 23 时抛出 ``ValueError``。
        """
        if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
            raise ValueError("evaluated_at must be timezone-aware")
        if not 0 <= completion_hour <= 23:
            raise ValueError("completion_hour must be from 0 through 23")
        local = evaluated_at.astimezone(timezone)
        return (
            local.date()
            if local.hour >= completion_hour
            else local.date() - timedelta(days=1)
        )
