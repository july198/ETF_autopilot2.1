from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd


TRADE_LOG_PATH = Path("data/trade_log.csv")
HOLDINGS_PATH = Path("data/holdings.csv")


def ensure_data_dir() -> None:
    Path("data").mkdir(parents=True, exist_ok=True)


def load_holdings() -> pd.DataFrame:
    if not HOLDINGS_PATH.exists():
        raise FileNotFoundError(f"缺少 {HOLDINGS_PATH}. 请先填写 data/holdings.csv")
    df = pd.read_csv(HOLDINGS_PATH)
    df.columns = [c.strip().lower() for c in df.columns]
    if "ticker" not in df.columns or "shares" not in df.columns:
        raise ValueError("holdings.csv 需要列：ticker,shares")
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0.0)
    return df[["ticker", "shares"]]


def load_trade_log() -> pd.DataFrame:
    if not TRADE_LOG_PATH.exists():
        return pd.DataFrame(
            columns=[
                "date",
                "month_key",
                "signal",
                "base_buy_cny",
                "below_ma200",
                "reserve_add_cny",
                "reserve_use_cny",
                "recommended_buy_cny",
                "total_fee_usd",
                "cash_pool_end_cny",
                "rsp_close",
                "month_high_close",
                "monthly_drawdown",
                "third_friday",
                "days_since_last_trade",
                "cooldown_ok",
            ]
        )
    df = pd.read_csv(TRADE_LOG_PATH)
    # Normalize
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    if "month_key" in df.columns:
        df["month_key"] = pd.to_datetime(df["month_key"]).dt.date
    return df


def append_trade_log(row: Dict[str, object]) -> None:
    ensure_data_dir()
    df = load_trade_log()
    df2 = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df2.to_csv(TRADE_LOG_PATH, index=False)


def get_cash_pool_start_cny(trade_log: pd.DataFrame, enabled: bool, source: str, manual_cny: float) -> float:
    if not enabled:
        return 0.0
    if source.upper() == "MANUAL":
        return float(manual_cny)
    # AUTO: last row cash_pool_end_cny
    if trade_log.empty or "cash_pool_end_cny" not in trade_log.columns:
        return 0.0
    last = trade_log["cash_pool_end_cny"].dropna()
    if last.empty:
        return 0.0
    return float(last.iloc[-1])


def get_reserve_balance_cny(trade_log: pd.DataFrame) -> float:
    if trade_log.empty:
        return 0.0
    add = pd.to_numeric(trade_log.get("reserve_add_cny", 0.0), errors="coerce").fillna(0.0).sum()
    use = pd.to_numeric(trade_log.get("reserve_use_cny", 0.0), errors="coerce").fillna(0.0).sum()
    return float(add - use)
