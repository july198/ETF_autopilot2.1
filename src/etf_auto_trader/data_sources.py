from __future__ import annotations

from typing import Any

import pandas as pd
import yfinance as yf


class MarketData:
    """
    兼容旧代码：允许用 md.close / md.open / md.high / md.low / md.volume
    兼容列名：Close/Adj Close/Open/High/Low/Volume 及一些中文列名
    兼容 runner.py 里可能出现的：md.prev_close、md.MA200 / md.ma200
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def _pick_col(self, *candidates: str) -> pd.Series:
        for c in candidates:
            if c in self.df.columns:
                return self.df[c]
        raise KeyError(f"找不到列：{candidates}，当前列名={list(self.df.columns)}")

    @property
    def close(self) -> pd.Series:
        # 英文优先，其次兼容常见中文
        return self._pick_col("Close", "Adj Close", "收盘", "关闭", "接近")

    @property
    def open(self) -> pd.Series:
        return self._pick_col("Open", "开盘", "打开")

    @property
    def high(self) -> pd.Series:
        return self._pick_col("High", "最高", "高")

    @property
    def low(self) -> pd.Series:
        return self._pick_col("Low", "最低", "低")

    @property
    def volume(self) -> pd.Series:
        return self._pick_col("Volume", "成交量", "量")

    @property
    def prev_close(self) -> pd.Series:
        # 前一交易日收盘
        return self.close.shift(1)

    @property
    def MA200(self) -> pd.Series:
        # 200 日均线（按 Close 计算）
        return self.close.rolling(window=200, min_periods=200).mean()

    @property
    def ma200(self) -> pd.Series:
        # 兼容 runner.py 若使用小写
        return self.MA200


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


def _normalize_columns(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """
    统一列名为：Open / High / Low / Close / Adj Close / Volume
    并剥掉类似 “Close Rsp / Adj Close Rsp” 的 ticker 后缀。
    """
    sym = str(symbol).strip()
    sym_upper = sym.upper()
    sym_title = sym.title()

    # MultiIndex -> 扁平化
    if isinstance(df.columns, pd.MultiIndex):
        flattened = []
        for col in df.columns:
            parts = [str(x).strip() for x in col if str(x).strip() != ""]
            flattened.append(" ".join(parts).strip())
        df.columns = flattened

    # Title 化
    cols = [str(c).strip() for c in df.columns]
    cols = [c.title() for c in cols]

    # Adj Close 变体归一化
    def fix_adj(c: str) -> str:
        return (
            c.replace("Adjclose", "Adj Close")
            .replace("Adj_close", "Adj Close")
            .replace("Adj. Close", "Adj Close")
        )

    cols = [fix_adj(c) for c in cols]

    # 剥掉 ticker 后缀（例如 "Close Rsp" -> "Close"）
    stripped = []
    for c in cols:
        c_strip = c.strip()
        for suf in (f" {sym_title}", f" {sym_upper}", f" {sym}"):
            if c_strip.endswith(suf):
                c_strip = c_strip[: -len(suf)].strip()
                break
        stripped.append(c_strip)

    df.columns = stripped
    return df


def _download_yf(symbol: str, start: Any, asof_date: Any) -> pd.DataFrame:
    """
    先用 start/end 下载；如果为空，再用 period='2y' 兜底。
    返回 index：无时区日期。
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

    df = _normalize_columns(df, symbol=symbol)

    # 统一索引为无时区日期
    if isinstance(df.index, pd.DatetimeIndex):
        idx = df.index
        if idx.tz is not None:
            idx = idx.tz_convert(None)
        df.index = idx.normalize()

    return df


# ===== 对外 API：必须兼容 runner.py 的调用方式 =====

def fetch_signal_inputs(signal_symbol: str, start: Any = None, asof_date: Any = None) -> MarketData:
    df = _download_yf(signal_symbol, start=start, asof_date=asof_date)
    return MarketData(df)


def fetch_prices(symbol: str, start: Any = None, asof_date: Any = None) -> pd.DataFrame:
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
            if "Close" in df.columns and not df["Close"].dropna().empty:
                return float(df["Close"].dropna().iloc[-1])
        except Exception:
            continue

    raise RuntimeError("yfinance 没找到 USD/CNY 汇率数据（USDCNY=X / CNY=X）")

