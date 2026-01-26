import datetime as dt
import json
import sys
from pathlib import Path

# Allow running without installing the package
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from etf_auto_trader.config import load_config
from etf_auto_trader.calendar_utils import TradingCalendar
from etf_auto_trader.data_sources import fetch_prices, fetch_fx_usdcny
from etf_auto_trader.fees import BuyFees



def resolve_asof_date(cfg, asof_date: dt.date | None = None) -> dt.date:
    """默认按美股收盘日生成建仓清单：收盘前用上一交易日，收盘后用当日。"""
    if asof_date is not None:
        return asof_date
    from zoneinfo import ZoneInfo

    market_now = dt.datetime.now(tz=ZoneInfo("America/New_York"))
    tentative = market_now.date()
    cal = TradingCalendar()

    def last_trading_day(d: dt.date) -> dt.date:
        x = d
        while not cal.is_trading_day(x):
            x -= dt.timedelta(days=1)
        return x

    # 收盘前默认看上一交易日收盘；收盘后用当日（如果是交易日）
    if (market_now.hour, market_now.minute) < (16, 10):
        if cal.is_trading_day(tentative):
            return last_trading_day(tentative - dt.timedelta(days=1))
        return last_trading_day(tentative)

    return last_trading_day(tentative)

if __name__ == "__main__":
    cfg = load_config("config.yaml")
    asof = resolve_asof_date(cfg)

    symbols = list(cfg.symbols.portfolio)
    prices = fetch_prices(symbols, asof)

    fx_rate = cfg.params.fx_usd_cny
    if str(getattr(cfg.params, "fx_mode", "fixed")).lower() == "auto":
        fx_rate = fetch_fx_usdcny(
            asof,
            symbol=getattr(cfg.params, "fx_symbol", "USDCNY=X"),
            fallback=getattr(cfg.params, "fx_fallback_usd_cny", fx_rate),
        )

    invest_cny = float(cfg.bootstrap.initial_invest_cny)
    invest_usd = invest_cny / float(fx_rate)

    n = len(symbols)
    per_usd = invest_usd / n if n else 0.0

    bf = BuyFees(
        commission_per_share=cfg.fees_buy.commission_per_share,
        commission_min_usd=cfg.fees_buy.commission_min_usd,
        platform_per_share=cfg.fees_buy.platform_per_share,
        platform_min_usd=cfg.fees_buy.platform_min_usd,
        clearing_per_share=cfg.fees_buy.clearing_per_share,
        other_fixed_fee_usd=cfg.execution.other_fixed_fee_usd,
    )

    orders = []
    for s in symbols:
        px = float(prices[s])
        raw = per_usd / px if px > 0 else 0.0
        if cfg.execution.allow_fractional_shares:
            step = float(cfg.execution.fractional_step)
            shares = max(0.0, (raw // step) * step)
            # 控制浮点误差
            shares = float(f"{shares:.6f}")
        else:
            shares = float(int(raw))
        est_fee = bf.fee(abs(shares))
        orders.append(
            {
                "ticker": s,
                "side": "BUY",
                "shares": shares,
                "price": px,
                "budget_usd": per_usd,
                "est_fee_usd": est_fee,
            }
        )

    out = {
        "date": asof.isoformat(),
        "fx_usd_cny": float(fx_rate),
        "initial_invest_cny": invest_cny,
        "initial_invest_usd": invest_usd,
        "cash_buffer_usd": float(cfg.bootstrap.cash_buffer_usd),
        "orders": orders,
        "note": "这是等权建仓清单（根据当日收盘价估算）。请在券商端核对后下单。",
    }

    Path("data").mkdir(parents=True, exist_ok=True)
    out_path = Path("data") / f"orders_init_{asof.isoformat()}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False, indent=2))
