from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd
import exchange_calendars as ecals
from exchange_calendars.errors import DateOutOfBounds


def _to_session_ts(x: Any, tz: str = "America/New_York") -> pd.Timestamp:
    """
    把各种输入（None/空字符串/“暂定”/日期字符串/date/datetime/Timestamp）
    统一转换成 Timestamp。
    注意：这里先按 tz 规范化，最终传给 exchange_calendars 前会去掉时区。
    """
    def _today() -> pd.Timestamp:
        # 先用带 tz 的“今天”，后面 is_trading_day 会统一去掉 tz
        return pd.Timestamp.now(tz=tz).normalize()

    if x is None:
        return _today()

    if isinstance(x, pd.Timestamp):
        if x.tzinfo is None:
            return x.tz_localize(tz).normalize()
        return x.tz_convert(tz).normalize()

    if isinstance(x, datetime):
        ts = pd.Timestamp(x)
        if ts.tzinfo is None:
            return ts.tz_localize(tz).normalize()
        return ts.tz_convert(tz).normalize()

    if isinstance(x, date):
        return pd.Timestamp(x).tz_localize(tz).normalize()

    if isinstance(x, str):
        s = x.strip()
        if s in ("", "auto", "AUTO", "today", "TODAY", "暂定", "TBD", "tbd"):
            return _today()
        try:
            ts = pd.Timestamp(s)
            if ts.tzinfo is None:
                return ts.tz_localize(tz).normalize()
            return ts.tz_convert(tz).normalize()
        except Exception:
            return _today()

    return _today()


@dataclass
class CalendarUtil:
    cal_name: str = "XNYS"
    tz: str = "America/New_York"

    def __post_init__(self) -> None:
        self.cal = ecals.get_calendar(self.cal_name)

    def is_trading_day(self, when: Any = None) -> bool:
        """
        exchange_calendars 的 is_session 需要“无时区”的 Timestamp。
        我们先把输入归一化，再去掉 tz 再调用 is_session。
        """
        ts = _to_session_ts(when, tz=self.tz)
        ts_naive = ts.tz_localize(None)  # 关键：去掉时区信息

        try:
            return bool(self.cal.is_session(ts_naive))
        except DateOutOfBounds:
            return False


# 兼容旧代码：runner.py 里使用 TradingCalendar 这个名字
TradingCalendar = CalendarUtil
