from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Optional

import exchange_calendars as xcals
import pandas as pd


@dataclass(frozen=True)
class TradingCalendar:
    name: str = "XNYS"  # NYSE

    def __post_init__(self):
        # nothing
        pass

    @property
    def cal(self):
        return xcals.get_calendar(self.name)

    def is_trading_day(self, d: dt.date) -> bool:
        # exchange_calendars 提供 is_session，可以直接判断某天是否为交易日
        return bool(self.cal.is_session(pd.Timestamp(d)))
    def trading_day_index(self, d: dt.date) -> Optional[int]:
        # Index trading days by their position since 1970-01-01 (stable)
        # Non-trading day returns None
        if not self.is_trading_day(d):
            return None
        # Use trading sessions list
        sessions = self.cal.sessions_in_range("1970-01-01", d.strftime("%Y-%m-%d"))
        # sessions includes d
        return int(len(sessions))

    def trading_days_between(self, d1: dt.date, d2: dt.date) -> int:
        """Return index(d2)-index(d1). d1 and d2 should be trading days."""
        i1 = self.trading_day_index(d1)
        i2 = self.trading_day_index(d2)
        if i1 is None or i2 is None:
            raise ValueError("d1 and d2 must be trading days")
        return i2 - i1

    def third_friday(self, d: dt.date) -> bool:
        # Third Friday of the month (calendar), and it must be a trading day
        year, month = d.year, d.month
        first = dt.date(year, month, 1)
        # weekday: Monday=0..Sunday=6, we want Friday=4
        offset = (4 - first.weekday()) % 7
        first_friday = first + dt.timedelta(days=offset)
        third_friday = first_friday + dt.timedelta(days=14)
        return d == third_friday and self.is_trading_day(d)
