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
    统一转换成交易日历可识别的 Timestamp。
    遇到无效值时，自动回退为“今天（纽约时区）0点”。
    """
    # 兜底：今天
    def _today() -> pd.Timestamp:
        return pd.Timestamp.now(tz=tz).normalize()

    if x is None:
        return _today()

    # pandas Timestamp
    if isinstance(x, pd.Timestamp):
        if x.tzinfo is None:
            return x.tz_localize(tz).normalize()
        return x.tz_convert(tz).normalize()

    # datetime
    if isinstance(x, datetime):
        ts = pd.Timestamp(x)
        if ts.tzinfo is None:
            return ts.tz_localize(tz).normalize()
        return ts.tz_convert(tz).normalize()

    # date
    if isinstance(x, date):
        return pd.Timestamp(x).tz_localize(tz).normalize()

    # str
    if isinstance(x, str):
        s = x.strip()
        # 这些值都当成“自动=今天”
        if s in ("", "auto", "AUTO", "today", "TODAY", "暂定", "TBD", "tbd"):
            return _today()
        try:
            ts = pd.Timestamp(s)
            if ts.tzinfo is None:
                return ts.tz_localize(tz).normalize()
            return ts.tz_convert(tz).normalize()
        except Exception:
            return _today()

    # 其他类型：一律兜底
    return _today()


@dataclass
class CalendarUtil:
    cal_name: str = "XNYS"
    tz: str = "America/New_York"

    def __post_init__(self) -> None:
        self.cal = ecals.get_calendar(self.cal_name)

    def is_trading_day(self, when: Any = None) -> bool:
        ts = _to_session_ts(when, tz=self.tz)
        try:
            return bool(self.cal.is_session(ts))
        except DateOutOfBounds:
            # 日期超出日历库范围时，返回 False，避免脚本直接崩
            return False
