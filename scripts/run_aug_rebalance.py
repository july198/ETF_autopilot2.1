import datetime as dt
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from etf_auto_trader.calendar_utils import TradingCalendar
from etf_auto_trader.config import load_config
from etf_auto_trader.data_sources import fetch_prices
from etf_auto_trader.fees import BuyFees, SellExtraFees
from etf_auto_trader.state import load_holdings


def _round_down(x: float, step: float) -> float:
    if step <= 0:
        return x
    return (x // step) * step


def buy_fee(cfg, shares: float) -> float:
    bf = BuyFees(
        commission_per_share=cfg.fees_buy.commission_per_share,
        commission_min_usd=cfg.fees_buy.commission_min_usd,
        platform_per_share=cfg.fees_buy.platform_per_share,
        platform_min_usd=cfg.fees_buy.platform_min_usd,
        clearing_per_share=cfg.fees_buy.clearing_per_share,
        other_fixed_fee_usd=cfg.execution.other_fixed_fee_usd,
    )
    return bf.fee(abs(shares))


def sell_extra_fee(cfg, shares: float) -> float:
    sf = SellExtraFees(
        activity_per_share=cfg.fees_sell_extra.activity_per_share,
        activity_min_usd=cfg.fees_sell_extra.activity_min_usd,
        activity_max_usd=cfg.fees_sell_extra.activity_max_usd,
        cat_per_share=cfg.fees_sell_extra.cat_per_share,
        sec_fee_usd=cfg.fees_sell_extra.sec_fee_usd,
    )
    return sf.fee(abs(shares))


def affordable_shares_from_usd(cfg, usd_amount: float, price: float, side: str) -> float:
    allow_frac = cfg.execution.allow_fractional_shares
    step = cfg.execution.fractional_step
    if usd_amount <= 0 or price <= 0:
        return 0.0
    raw = usd_amount / price
    if allow_frac:
        sh = _round_down(raw, step)
        sh = float(max(0.0, round(sh, 10)))
        return sh if side == "BUY" else -sh
    sh = int(raw)
    return float(sh) if side == "BUY" else -float(sh)


def main():
    cfg = load_config("config.yaml")
    asof = dt.date.today()

    cal = TradingCalendar()
    if not cal.is_trading_day(asof):
        print("今天不是交易日。建议换成下一个交易日再跑。")
        return

    fx = cfg.params.fx_usd_cny
    prices = fetch_prices(cfg.symbols.portfolio, asof)
    holdings = load_holdings()

    df = holdings.copy()
    df["price"] = df["ticker"].map(prices).astype(float)
    df["value_cny"] = df["shares"].astype(float) * df["price"] * fx
    total_value_cny = float(df["value_cny"].sum())
    target_each = total_value_cny * cfg.params.target_weight_each

    df["target_cny"] = target_each
    df["diff_cny"] = df["target_cny"] - df["value_cny"]

    hold_th = target_each * 0.005

    def action(x: float) -> str:
        if abs(x) < hold_th:
            return "HOLD"
        return "BUY" if x > 0 else "SELL"

    df["action"] = df["diff_cny"].apply(lambda x: action(float(x)))

    spread = cfg.execution.spread_cost_pct
    df["usd_amt"] = (df["diff_cny"].abs() / fx) * (1 - spread)
    df.loc[df["action"] == "HOLD", "usd_amt"] = 0.0

    df["shares_suggest"] = df.apply(
        lambda r: affordable_shares_from_usd(cfg, float(r["usd_amt"]), float(r["price"]), str(r["action"])),
        axis=1,
    )

    def fee_row(r) -> float:
        act = str(r["action"])
        sh = float(r["shares_suggest"])
        if act == "HOLD" or sh == 0:
            return 0.0
        base = buy_fee(cfg, sh)
        if act == "SELL":
            base += sell_extra_fee(cfg, sh)
        return base

    df["fee_usd"] = df.apply(fee_row, axis=1)

    out = {
        "date": asof.isoformat(),
        "total_value_cny": total_value_cny,
        "target_each_cny": target_each,
        "orders": df[["ticker", "action", "shares_suggest", "price", "usd_amt", "fee_usd"]].to_dict(orient="records"),
    }

    Path("data").mkdir(parents=True, exist_ok=True)
    p = Path(f"data/aug_rebalance_{asof.isoformat()}.json")
    p.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"已写入: {p}")


if __name__ == "__main__":
    main()
