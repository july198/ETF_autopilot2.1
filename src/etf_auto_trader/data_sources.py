from __future__ import annotations

from typing import Any

import pandas as pd
import yfinance as yf


class MarketData:
    """
    兼容旧代码：允许用 md.close / md.open / md.high / md.low / md.volume
    同时兼容英文列名与中文列名。
    """
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def _pick_col(self, *candidates: str):
        for c in candidates:
            if c in self.df.columns:
                return self.df[c]
        raise KeyError(f"找不到列：{candidates}，当前列名={list(self.df.columns)}")

    @property
    def close(self):
        # 英文优先，其次兼容常见中文
        return self._pick_col("Close", "Adj Close", "收盘", "关闭", "接近")

    @property
    def open(self):
        return self._pick_col("Open", "开盘", "打开")

    @property
    def high(self):
        return self._pick_col("High", "最高", "高")

    @property
    def low(self):
        return self._pick_col("Low", "最低", "低")

    @property
    def volume(self):
        return self._pick_col("Volume", "成交量", "量")


def _today() -> pd.Timestamp:
    return pd.Timestamp.today().normalize()


def _coerce_asof_date(x: Any) -> pd.Timestamp:
    """
    asof_date 统一成“无时区”的日期（00:00:00）。
    遇到 None/空/auto/today/暂定/解析失败 -> 今天
    遇到非常离谱的年份（<1990 或 >明年）-> 今天
    """
    if x is None:
        return _today()

    if isinstance(x, pd.Timestamp):
        ts = x
    else:
        if isinstance(x, str):
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

    # 去时区
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)

    ts = ts.normalize()

    y = ts.year
    if y < 1990 or y > (pd.Timestamp.today().year + 1):
        return _today()

    return ts


def _coerce_start_date(start: Any, asof: pd.Timestamp, lookback_days: int = 450) -> pd.Timestamp:
    """
    start 无效时，自动用 asof - lookback_days。
    """
    if start is None:
        return (asof - pd.Timedelta(days=lookback_days)).normalize()

    try:
        ts = pd.Timestamp(start)
        if ts.tzinfo is not None:
            ts = ts.tz_convert(None)
        ts = ts.normalize()
    except Exception:
        return (asof - pd.Timedelta(days=lookback_days)).normalize()

    if ts.year < 1990 or ts > asof:
        return (asof - pd.Timedelta(days=lookback_days)).normalize()

    return ts


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    把 yfinance 返回的列名尽量统一为：
    Open / High / Low / Close / Adj Close / Volume
    （如果本来就是这套就不动）
    """
    # 有时会出现多层列（少见），简单处理一下
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(x) for x in col if str(x) != ""]).strip() for col in df.columns]

    # title 化
    df.columns = [str(c).strip().title() for c in df.columns]

    # 一些常见变体归一化
    rename_map = {
        "Adjclose": "Adj Close",
        "Adj_close": "Adj Close",
        "Adj. Close": "Adj Close",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    return df


def _download_yf(symbol: str, start: Any, asof_date: Any) -> pd.DataFrame:
    """
    先用 start/end 下载；如果为空，再用 period='2y' 兜底。
    返回的 index 统一成“无时区日期”。
    """
    asof = _coerce_asof_date(asof_date)
    start_ts = _coerce_start_date(start, asof, lookback_days=450)
    end_ts = (asof + pd.Timedelta(days=1)).normalize()  # end 开区间

    df = yf.download(
        symbol,
        start=start_ts.strftime("%Y-%m-%d"),
        end=end_ts.strftime("%Y-%m-%d"),
        interval="1d",
        progress=False,
        auto_adjust=False,
        actions=False,
        threads=False,
    )

    if df is None or df.empty:
        df = yf.Ticker(symbol).history(period="2y", interval="1d", auto_adjust=False)

    if df is None or df.empty:
        raise RuntimeError(f"yfinance 没找到数据：{symbol}")

    df = _normalize_columns(df)

    # 统一索引为无时区日期
    if isinstance(df.index, pd.DatetimeIndex):
        idx = df.index
        if idx.tz is not None:
            idx = idx.tz_convert(None)
        df.index = idx.normalize()

    return df


# ===== 对外 API：必须兼容 runner.py 的调用方式 =====

def fetch_signal_inputs(signal_symbol: str, start: Any = None, asof_date: Any = None) -> MarketData:
    """
    runner.py 以位置参数调用：fetch_signal_inputs(symbol, start, asof_date)
    返回 MarketData，兼容 md.close 这类用法
    """
    df = _download_yf(signal_symbol, start=start, asof_date=asof_date)
    return MarketData(df)


def fetch_prices(symbol: str, start: Any = None, asof_date: Any = None) -> pd.DataFrame:
    """
    runner.py 以位置参数调用：fetch_prices(symbol, start, asof_date)
    """
    return _download_yf(symbol, start=start, asof_date=asof_date)


def fetch_fx_usdcny(asof_date: Any = None) -> float:
    """
    返回：1 USD 兑多少 CNY
    """
    asof = _coerce_asof_date(asof_date)
    start = asof - pd.Timedelta(days=10)

    for sym in ("USDCNY=X", "CNY=X"):
        try:
            df = _download_yf(sym, start=start, asof_date=asof)
            # 汇率一般用 Close
            if "Close" in df.columns and not df["Close"].dropna().empty:
                return float(df["Close"].dropna().iloc[-1])
        except Exception:
            continue

    raise RuntimeError("yfinance 没找到 USD/CNY 汇率数据（USDCNY=X / CNY=X）")
