from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import pandas as pd
import exchange_calendars as ecals
from exchange_calendars.errors import DateOutOfBounds


def _as_naive_day(x: Any) -> pd.Timestamp:
    """
    把输入转成“无时区”的日期（00:00:00）。
    兼容：None/空字符串/auto/today/暂定/date/datetime/Timestamp/日期字符串
    """
    def _today() -> pd.Timestamp:
        return pd.Timestamp.today().normalize()

    if x is None:
        return _today()

    if isinstance(x, pd.Timestamp):
        ts = x
    elif isinstance(x, datetime):
        ts = pd.Timestamp(x)
    elif isinstance(x, date):
        ts = pd.Timestamp(x)
    elif isinstance(x, str):
        s = x.strip()
        if s in ("", "auto", "AUTO", "today", "TODAY", "暂定", "TBD", "tbd"):
            return _today()
        try:
            ts = pd.Timestamp(s)
        except Exception:
            return _today()
    else:
        try:
            ts = pd.Timestamp(x)
        except Exception:
            return _today()

    # 去时区，归一化到日期
    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_convert(None)
    return ts.normalize()


def _third_friday_of_month(d: pd.Timestamp) -> pd.Timestamp:
    """
    返回 d 所在月份的第三个周五（无时区日期）。
    """
    first = d.replace(day=1).normalize()
    # weekday: Mon=0 ... Sun=6；Friday=4
    offset = (4 - first.weekday()) % 7
    first_friday = first + pd.Timedelta(days=offset)
    third_friday = first_friday + pd.Timedelta(days=14)
    return third_friday.normalize()


@dataclass
class CalendarUtil:
    cal_name: str = "XNYS"

    def __post_init__(self) -> None:
        self.cal = ecals.get_calendar(self.cal_name)

    def is_trading_day(self, when: Any = None) -> bool:
        """
        exchange_calendars 的 is_session 需要“无时区”的 Timestamp。
        """
        ts = _as_naive_day(when)
        try:
            return bool(self.cal.is_session(ts))
        except DateOutOfBounds:
            return False

    def third_friday(self, when: Any = None) -> bool:
        """
        判断 when 是否为其所在月份的“第三个周五”（返回 True/False）。
        """
        d = _as_naive_day(when)
        tf = _third_friday_of_month(d)
        return bool(d == tf)

    def trading_days_between(self, start: Any, end: Any) -> int:
        """返回 start 到 end 之间相隔的交易日数量（不含 start，含 end 的位置差）。"""
        s = _as_naive_day(start)
        e = _as_naive_day(end)
        try:
            # sessions_in_range 返回 DatetimeIndex（无时区）
            sessions = self.cal.sessions_in_range(s, e)
            if sessions is None:
                return 999
            # 若 start 本身是交易日，sessions 包含 start；我们要“差值”，所以减 1
            n = int(len(sessions))
            if n <= 0:
                return 0
            if self.is_trading_day(s):
                return max(0, n - 1)
            return n
        except Exception:
            return 999


# 兼容旧代码：runner/strategy 里可能使用 TradingCalendar 这个名字
TradingCalendar = CalendarUtil

