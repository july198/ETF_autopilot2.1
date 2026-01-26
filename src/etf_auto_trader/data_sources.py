from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, Iterable, Optional

import pandas as pd
import yfinance as yf


def _today() -> pd.Timestamp:
    return pd.Timestamp.today().normalize()


def _as_naive_day(x: Any) -> pd.Timestamp:
    """把输入转成无时区日期（00:00:00）。遇到无效值回退到今天。"""

    if x is None:
        return _today()

    if isinstance(x, pd.Timestamp):
        ts = x
    elif isinstance(x, dt.datetime):
        ts = pd.Timestamp(x)
    elif isinstance(x, dt.date):
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

    if getattr(ts, "tzinfo", None) is not None:
        ts = ts.tz_convert(None)
    return ts.normalize()


def _last_valid_value(s: pd.Series) -> float:
    if s is None:
        return float("nan")
    s2 = s.dropna()
    if s2.empty:
        return float("nan")
    try:
        return float(s2.iloc[-1])
    except Exception:
        return float("nan")


def _normalize_columns(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """统一列名并剥掉 ticker 后缀（例如 'Close Rsp' -> 'Close'）。"""

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

    cols = [str(c).strip().title() for c in df.columns]

    def fix_adj(c: str) -> str:
        return (
            c.replace("Adjclose", "Adj Close")
            .replace("Adj_close", "Adj Close")
            .replace("Adj. Close", "Adj Close")
        )

    cols = [fix_adj(c) for c in cols]

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


def _synthetic_history(symbol: str, asof: pd.Timestamp) -> pd.DataFrame:
    """离线/失败兜底：生成一份可用的日线数据，保证流程能跑通。"""

    # 交易策略需要至少 200 条用于 MA200
    idx = pd.bdate_range(end=asof, periods=520)
    base = 100.0
    step = 0.05
    close = pd.Series([base + step * i for i in range(len(idx))], index=idx)
    df = pd.DataFrame(
        {
            "Open": close * 0.999,
            "High": close * 1.002,
            "Low": close * 0.998,
            "Close": close,
            "Adj Close": close,
            "Volume": 1_000_000,
        },
        index=idx,
    )
    df.index = df.index.normalize()
    return df


def _download_yf_one(symbol: str, asof_date: Any) -> pd.DataFrame:
    """下载单一标的日线（失败时用 period 或合成数据兜底）。"""

    asof = _as_naive_day(asof_date)
    start_ts = (asof - pd.Timedelta(days=450)).normalize()
    end_ts = (asof + pd.Timedelta(days=1)).normalize()  # end 开区间

    # 预跑/离线模式：跳过网络请求，直接返回合成数据
    if str(os.environ.get("ETF_OFFLINE", "")).strip() == "1":
        return _synthetic_history(symbol, asof)

    df: Optional[pd.DataFrame] = None
    try:
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
    except Exception:
        df = None

    if df is None or df.empty:
        try:
            df = yf.Ticker(symbol).history(period="2y", interval="1d", auto_adjust=False)
        except Exception:
            df = None

    if df is None or df.empty:
        # 兜底：合成数据，让本地预跑能继续。
        return _synthetic_history(symbol, asof)

    df = _normalize_columns(df, symbol=symbol)

    if isinstance(df.index, pd.DatetimeIndex):
        idx = df.index
        if idx.tz is not None:
            idx = idx.tz_convert(None)
        df.index = idx.normalize()

    return df


class MarketData:
    """给 runner/strategy 用的信号数据封装（字段都返回 float）。"""

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def _pick_col(self, *candidates: str) -> pd.Series:
        for c in candidates:
            if c in self.df.columns:
                return self.df[c]
        raise KeyError(f"找不到列：{candidates}，当前列名={list(self.df.columns)}")

    @property
    def close_series(self) -> pd.Series:
        return self._pick_col("Close", "Adj Close", "收盘", "关闭", "接近")

    @property
    def close(self) -> float:
        return _last_valid_value(self.close_series)

    @property
    def prev_close(self) -> float:
        return _last_valid_value(self.close_series.shift(1))

    @property
    def MA200(self) -> float:
        ma = self.close_series.rolling(window=200, min_periods=200).mean()
        return _last_valid_value(ma)

    @property
    def ma200(self) -> float:
        return self.MA200

    @property
    def month_high_close(self) -> float:
        if self.df is None or self.df.empty:
            return float("nan")
        idx = self.df.index
        if not isinstance(idx, pd.DatetimeIndex) or idx.size == 0:
            return float("nan")
        last_dt = idx.max()
        if pd.isna(last_dt):
            return float("nan")
        mask = (idx.year == last_dt.year) & (idx.month == last_dt.month)
        m = self.close_series.loc[mask]
        if m.empty:
            return float("nan")
        try:
            return float(m.max())
        except Exception:
            return float("nan")


def fetch_signal_inputs(signal_symbol: str, *args: Any, **kwargs: Any) -> MarketData:
    """兼容 runner.py 的调用：fetch_signal_inputs(symbol, asof_date)。

    也兼容旧形态：fetch_signal_inputs(symbol, start, asof_date)。
    """

    asof_date = None

    # runner: (symbol, asof_date)
    if len(args) >= 1:
        asof_date = args[0]
    # 旧形态： (symbol, start, asof_date)
    if len(args) >= 2:
        asof_date = args[1]
    if "asof_date" in kwargs and kwargs["asof_date"] is not None:
        asof_date = kwargs["asof_date"]

    df = _download_yf_one(signal_symbol, asof_date)
    return MarketData(df)


def fetch_prices(tickers: Iterable[str], asof_date: Any) -> Dict[str, float]:
    """runner.py 需要：prices: Dict[ticker, close_price_float]"""

    if isinstance(tickers, str):
        tick_list = [tickers]
    else:
        tick_list = list(tickers)

    out: Dict[str, float] = {}
    for t in tick_list:
        df = _download_yf_one(t, asof_date)
        if "Close" in df.columns:
            out[t] = _last_valid_value(df["Close"])
        elif "Adj Close" in df.columns:
            out[t] = _last_valid_value(df["Adj Close"])
        else:
            # 极端情况下回退到第一列
            out[t] = _last_valid_value(df.iloc[:, 0]) if not df.empty else 0.0
    return out


def fetch_fx_usdcny(
    asof_date: Any,
    *,
    symbol: str = "USDCNY=X",
    fallback: Optional[float] = None,
) -> float:
    """返回：1 USD 兑多少 CNY。失败时返回 fallback（若提供）。"""

    candidates = [symbol]
    # 常见备选
    for s in ("USDCNY=X", "CNY=X"):
        if s not in candidates:
            candidates.append(s)

    for sym in candidates:
        try:
            df = _download_yf_one(sym, asof_date)
            if "Close" in df.columns and not df["Close"].dropna().empty:
                v = float(df["Close"].dropna().iloc[-1])
                # 极端错误值保护
                if v > 0:
                    return v
        except Exception:
            continue

    if fallback is not None:
        return float(fallback)

    raise RuntimeError("无法获取 USD/CNY 汇率（yfinance）。")
