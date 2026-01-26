from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
import yfinance as yf


def _coerce_asof_date(x: Any) -> pd.Timestamp:
    """
    把 asof_date 统一成“无时区”的日期（00:00:00），并做合理范围校验。
    遇到 None / 空字符串 / auto / today / 暂定 / 解析失败 -> 直接用今天。
    """
    def _today() -> pd.Timestamp:
        return pd.Timestamp.today().normalize()

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

    # 去时区 + 归一化到日期
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    ts = ts.normalize()

    # 合理范围：避免 1970、0001 之类的怪日期导致 yfinance 抽风
    # 年份太小或太大，直接回退到今天
    y = ts.year
    if y < 1990 or y > pd.Timestamp.today().year + 1:
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

    # start 也做合理范围约束
    if ts.year < 1990 or ts > asof:
        return (asof - pd.Timedelta(days=lookback_days)).normalize()

    return ts


def _download_yf(symbol: str, start: Any, asof_date: Any) -> pd.DataFrame:
    """
    统一日期、下载日线 OHLCV。
    先用 start/end 精确下载；如果拿不到数据，再用 period='2y' 兜底。
    """
    asof = _coerce_asof_date(asof_date)
    start_ts = _coerce_start_date(start, asof, lookback_days=450)

    # yfinance 的 end 是“开区间”，用 asof+1 天确保包含 asof 当天
    end_ts = (asof + pd.Timedelta(days=1)).normalize()

    start_s = start_ts.strftime("%Y-%m-%d")
    end_s = end_ts.strftime("%Y-%m-%d")

    df = yf.download(
        symbol,
        start=start_s,
        end=end_s,
        interval="1d",
        progress=False,
        auto_adjust=False,
        actions=False,
        threads=False,
    )

    # 兜底：有时 start/end 下载为空，但用 period 能取到
    if df is None or df.empty:
        t = yf.Ticker(symbol)
        df = t.history(period="2y", interval="1d", auto_adjust=False)

    if df is None or df.empty:
        raise RuntimeError(f"yfinance 没找到数据：{symbol}")

    # 统一列名（有些返回小写/混合）
    df.columns = [str(c).strip().title() for c in df.columns]

    # 统一索引为日期（无时区）
    if isinstance(df.index, pd.DatetimeIndex):
        idx = df.index
        if idx.tz is not None:
            idx = idx.tz_convert(None)
        df.index = idx.normalize()

    return df


# ---- 下面这些接口名，尽量保持和你原项目一致 ----

def fetch_signal_inputs(signal_symbol: str, start: Any, asof_date: Any) -> pd.DataFrame:
    """
    给策略计算用的信号数据（通常是 RSP 的历史日线）。
    """
    return _download_yf(signal_symbol, start=start, asof_date=asof_date)


def fetch_price_history(symbol: str, start: Any, asof_date: Any) -> pd.DataFrame:
    """
    通用价格历史数据（给其他 ETF 用也行）。
    """
    return _download_yf(symbol, start=start, asof_date=asof_date)


def fetch_fx_usdcny(asof_date: Any = None) -> float:
    """
    拉 USD/CNY（优先用 yfinance：USDCNY=X，其次 CNY=X）
    返回：1 USD 兑多少 CNY（float）
    """
    asof = _coerce_asof_date(asof_date)
    # 用近 10 天，避免时区/交易日造成空
    start = asof - pd.Timedelta(days=10)

    for sym in ("USDCNY=X", "CNY=X"):
        try:
            df = _download_yf(sym, start=start, asof_date=asof)
            if "Close" in df.columns and not df["Close"].dropna().empty:
                v = float(df["Close"].dropna().iloc[-1])
                # CNY=X 往往表示 1 USD = ? CNY；如果你发现方向反了，再调整即可
                return v
        except Exception:
            continue

    raise RuntimeError("yfinance 没找到 USD/CNY 汇率数据（USDCNY=X / CNY=X）")
# 兼容旧代码：runner.py 里导入的是 fetch_prices
def fetch_prices(symbol: str, start: Any, asof_date: Any) -> pd.DataFrame:
    return fetch_price_history(symbol=symbol, start=start, asof_date=asof_date)
