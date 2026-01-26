from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd


@dataclass(frozen=True)
class MarketData:
    date: dt.date
    close: float
    prev_close: float
    ma200: float
    month_high_close: float


def _download_yf(symbol: str, start: dt.date, end: dt.date) -> pd.DataFrame:
    # yfinance is imported lazily to keep module import light
    import yfinance as yf  # type: ignore
    df = yf.download(
        symbol,
        start=start.strftime("%Y-%m-%d"),
        end=(end + dt.timedelta(days=1)).strftime("%Y-%m-%d"),
        progress=False,
        auto_adjust=False,
        actions=False,
        threads=True,
    )
    if df is None or df.empty:
        raise RuntimeError(f"yfinance 没拉到数据: {symbol}")
    df = df.reset_index()
    # Normalize columns
    df.columns = [str(c).strip() for c in df.columns]
    if "Date" not in df.columns or "Close" not in df.columns:
        raise RuntimeError(f"yfinance 返回列异常: {df.columns}")
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    return df[["Date", "Close"]].sort_values("Date").reset_index(drop=True)


def fetch_fx_usdcny(asof_date: dt.date, symbol: str = "USDCNY=X", fallback: float | None = None) -> float:
    """
    拉取 USD/CNY 汇率（用于把 CNY 预算换算成 USD 下单金额）。
    优先：Yahoo Finance via yfinance（symbol=USDCNY=X 或 CNY=X）
    兜底：exchangerate.host 的历史汇率
    """
    start = asof_date - dt.timedelta(days=10)

    def _try(sym: str) -> float | None:
        try:
            df = _download_yf(sym, start, asof_date)
            df = df[df["Date"] <= asof_date]
            if df.empty:
                return None
            return float(df.iloc[-1]["Close"])
        except Exception:
            return None

    # 1) primary symbol
    r = _try(symbol)
    if r is not None:
        return r

    # 2) common alternates on Yahoo
    for alt in ["USDCNY=X", "CNY=X"]:
        if alt == symbol:
            continue
        r = _try(alt)
        if r is not None:
            return r

    # 3) fallback provider
    try:
        import requests  # type: ignore
        resp = requests.get(
            f"https://api.exchangerate.host/{asof_date.isoformat()}",
            params={"base": "USD", "symbols": "CNY"},
            timeout=10,
        )
        if resp.ok:
            js = resp.json()
            rate = js.get("rates", {}).get("CNY")
            if rate is not None:
                return float(rate)
    except Exception:
        pass

    if fallback is not None:
        return float(fallback)

    raise RuntimeError("无法获取 USD/CNY 汇率（yfinance 与备用接口都失败）")

def fetch_signal_inputs(signal_symbol: str, asof_date: dt.date) -> MarketData:
    """
    拉取信号用数据：
    - asof_date 的收盘价 close
    - 前一交易日收盘 prev_close
    - 200MA（用收盘价滚动 200 交易日）
    - 本月截至 asof_date 的最高收盘
    """
    # 为了算 MA200，需要至少 260 个自然日缓冲
    start = asof_date - dt.timedelta(days=400)
    df = _download_yf(signal_symbol, start, asof_date)
    if df.empty:
        raise RuntimeError("价格数据为空")

    # 找到 asof_date 这行（若 asof_date 非交易日，外部应先避免）
    df_today = df[df["Date"] <= asof_date].copy()
    if df_today.empty:
        raise RuntimeError("找不到 asof_date 之前的数据")
    # 取最后一行做 close
    close = float(df_today.iloc[-1]["Close"])

    if len(df_today) < 2:
        raise RuntimeError("历史数据不足以获取 prev_close")
    prev_close = float(df_today.iloc[-2]["Close"])

    # MA200：滚动 200（按交易日序列）
    df_today["ma200"] = df_today["Close"].rolling(200).mean()
    ma200 = float(df_today.iloc[-1]["ma200"])
    if pd.isna(ma200):
        # 保险起见：不足 200 时给出 NaN 并报错
        raise RuntimeError("历史数据不足以计算 MA200（需要至少 200 个交易日收盘）")

    # 本月最高收盘
    month_start = asof_date.replace(day=1)
    month_df = df_today[df_today["Date"] >= month_start]
    month_high = float(month_df["Close"].max())

    return MarketData(
        date=asof_date,
        close=close,
        prev_close=prev_close,
        ma200=ma200,
        month_high_close=month_high,
    )


def fetch_prices(symbols: List[str], asof_date: dt.date) -> Dict[str, float]:
    """
    拉取组合里每只 ETF 的 close（用于估算下单股数）。
    直接用 asof_date 的收盘价。
    """
    start = asof_date - dt.timedelta(days=30)
    prices: Dict[str, float] = {}
    for s in symbols:
        df = _download_yf(s, start, asof_date)
        df = df[df["Date"] <= asof_date]
        if df.empty:
            raise RuntimeError(f"拉不到价格: {s}")
        prices[s] = float(df.iloc[-1]["Close"])
    return prices
