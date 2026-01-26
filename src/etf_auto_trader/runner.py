from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict
from pathlib import Path
from typing import Dict

from .calendar_utils import TradingCalendar
from .config import Config, env_or_none
from .data_sources import fetch_prices, fetch_signal_inputs, fetch_fx_usdcny
from .state import (
    append_trade_log,
    get_cash_pool_start_cny,
    load_holdings,
    load_trade_log,
)
from .strategy import allocate_orders, evaluate_signal
from .brokers import PaperBroker, AlpacaBroker
from .notify import send_email


def _fmt_money(x: float, nd: int = 2) -> str:
    try:
        return f"{float(x):,.{nd}f}"
    except Exception:
        return str(x)


def _pct(x: float | None, nd: int = 2) -> str:
    if x is None:
        return ""
    try:
        return f"{float(x) * 100:.{nd}f}%"
    except Exception:
        return str(x)


def _build_email_body(
    *,
    asof_date: dt.date,
    fx_rate: float,
    sig,
    md,
    prices: Dict[str, float],
    orders,
    total_fee_usd: float,
    cash_pool_start_cny: float,
    cash_pool_end_cny: float,
    broker_result: str,
    message: str | None = None,
) -> str:
    """给小白看的邮件正文：一句结论 + 关键数据 + 下单清单。"""
    from zoneinfo import ZoneInfo

    generated_bj = dt.datetime.now(tz=ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
    has_trade = bool(orders) and any(getattr(o, "side", "") == "BUY" and getattr(o, "shares", 0) > 0 for o in orders)

    buy_usd = 0.0
    fee_usd = float(total_fee_usd or 0.0)
    order_lines: list[str] = []
    if has_trade:
        for o in orders:
            if o.side != "BUY" or o.shares <= 0:
                continue
            amt = float(o.shares) * float(o.price)
            buy_usd += amt
            order_lines.append(
                f"- {o.ticker}: 买入 {o.shares} 股；收盘价 {o.price:.2f} USD；预计金额 {_fmt_money(amt,2)} USD；手续费≈{_fmt_money(o.est_fee_usd,2)} USD"
            )
    else:
        order_lines.append("- 今日无下单")

    buy_cny = float(getattr(sig, "recommended_buy_cny", 0.0) or 0.0)
    buy_usd_est = buy_cny / fx_rate if fx_rate else 0.0

    # RSP 关键数据
    below = "是" if bool(getattr(sig, "below_ma200", False)) else "否"
    third_friday = "是" if bool(getattr(sig, "third_friday", False)) else "否"
    cooldown_ok = "是" if bool(getattr(sig, "cooldown_ok", False)) else "否"

    lines = []
    lines.append("ETF 自动交易日报（北京时间）")
    lines.append(f"生成时间：{generated_bj}")
    lines.append(f"对应美股收盘交易日：{asof_date.isoformat()}")
    lines.append("")

    # 1) 结论
    lines.append("1) 今日结论")
    lines.append(f"- 信号：{getattr(sig, 'signal', '')}")
    lines.append(f"- 是否交易：{'交易' if has_trade else '不交易'}")
    if message:
        lines.append(f"- 说明：{message}")
    lines.append(f"- 推荐买入总额：{_fmt_money(buy_cny,2)} CNY（按 FX 约 {_fmt_money(buy_usd_est,2)} USD）")
    lines.append(f"- USD/CNY（当次使用）：{fx_rate:.6f}")
    lines.append("")

    # 2) 关键数据（RSP）
    lines.append("2) 关键数据（用于触发规则）")
    lines.append(f"- RSP 收盘：{_fmt_money(getattr(md, 'close', 0.0),2)}；前收：{_fmt_money(getattr(md, 'prev_close', 0.0),2)}")
    lines.append(f"- MA200：{_fmt_money(getattr(md, 'ma200', 0.0),2)}；收盘在 MA200 下方：{below}")
    lines.append(f"- 月内最高收盘：{_fmt_money(getattr(md, 'month_high_close', 0.0),2)}；月内回撤：{_pct(getattr(sig, 'monthly_drawdown', None),2)}")
    lines.append(f"- 第三个周五兜底：{third_friday}")
    lines.append(f"- 距离上次交易：{int(getattr(sig, 'days_since_last_trade', 0))} 个交易日；冷却期满足：{cooldown_ok}")
    lines.append("")

    # 3) 现金池
    lines.append("3) 现金池（待命现金）")
    lines.append(f"- 起始：{_fmt_money(cash_pool_start_cny,2)} CNY")
    lines.append(f"- 本次增加：{_fmt_money(float(getattr(sig, 'reserve_add_cny', 0.0) or 0.0),2)} CNY")
    lines.append(f"- 本次使用：{_fmt_money(float(getattr(sig, 'reserve_use_cny', 0.0) or 0.0),2)} CNY")
    lines.append(f"- 结束：{_fmt_money(cash_pool_end_cny,2)} CNY")
    lines.append("")

    # 4) 下单清单
    lines.append("4) 今日下单清单（照单下单即可）")
    lines.extend(order_lines)
    lines.append("")
    lines.append("合计（估算）：")
    lines.append(f"- 买入金额：{_fmt_money(buy_usd,2)} USD")
    lines.append(f"- 手续费：{_fmt_money(fee_usd,2)} USD")
    lines.append(f"- 预计占用现金：{_fmt_money(buy_usd + fee_usd,2)} USD")
    lines.append("")

    # 5) 价格回顾
    lines.append("5) 组合 ETF 收盘价（用于下单计算）")
    lines.append(
        "- "
        + ", ".join(
            [
                f"IWY={_fmt_money(prices.get('IWY', 0.0),2)}",
                f"SPMO={_fmt_money(prices.get('SPMO', 0.0),2)}",
                f"RSP={_fmt_money(prices.get('RSP', 0.0),2)}",
                f"PFF={_fmt_money(prices.get('PFF', 0.0),2)}",
                f"VNQ={_fmt_money(prices.get('VNQ', 0.0),2)}",
            ]
        )
    )
    lines.append("")

    # 6) 执行状态
    lines.append("6) 执行状态")
    lines.append(f"- {broker_result}")

    return "\n".join(lines)



def run_daily(cfg: Config, asof_date: dt.date | None = None) -> Dict[str, object]:
    """执行每日流程：拉取数据 -> 评估信号 -> 生成订单 -> 记录日志 -> 邮件通知。"""
    if asof_date is None:
        # 以美东时间判断当天是否已收盘；收盘前运行则默认使用上一个交易日收盘数据
        from zoneinfo import ZoneInfo

        market_now = dt.datetime.now(tz=ZoneInfo("America/New_York"))
        tentative = market_now.date()
        cal_tmp = TradingCalendar()

        def last_trading_day(d: dt.date) -> dt.date:
            x = d
            while not cal_tmp.is_trading_day(x):
                x -= dt.timedelta(days=1)
            return x

        # 收盘后再用当天，否则用上一交易日
        if (market_now.hour, market_now.minute) < (16, 10):
            # 还没收盘：若今天是交易日，用上一交易日；否则取最近交易日
            if cal_tmp.is_trading_day(tentative):
                asof_date = last_trading_day(tentative - dt.timedelta(days=1))
            else:
                asof_date = last_trading_day(tentative)
        else:
            # 已收盘：若今天不是交易日，回退到最近交易日
            asof_date = last_trading_day(tentative)

    cal = TradingCalendar()

    # 信号数据
    md = fetch_signal_inputs(cfg.symbols.signal, asof_date)

    trade_log = load_trade_log()
    sig = evaluate_signal(
        cfg,
        cal,
        asof_date,
        rsp_close=md.close,
        rsp_prev_close=md.prev_close,
        ma200=md.ma200,
        month_high=md.month_high_close,
        trade_log=trade_log,
    )

    cash_pool_start = get_cash_pool_start_cny(
        trade_log,
        enabled=cfg.cash_pool.enabled,
        source=cfg.cash_pool.source,
        manual_cny=cfg.cash_pool.manual_cny,
    )

    # 持仓与价格（即使不交易也拉取，便于邮件里展示）
    holdings = load_holdings()
    prices = fetch_prices(cfg.symbols.portfolio, asof_date)
    fx_rate = cfg.params.fx_usd_cny
    if str(getattr(cfg.params, "fx_mode", "fixed")).lower() == "auto":
        fx_rate = fetch_fx_usdcny(
            asof_date,
            symbol=getattr(cfg.params, "fx_symbol", "USDCNY=X"),
            fallback=getattr(cfg.params, "fx_fallback_usd_cny", fx_rate),
        )

    # 推荐买入=0：不生成订单，也不写入交易日志；依然会发一封“无交易”的日报
    if sig.recommended_buy_cny <= 0:
        broker_result = "BROKER: SKIPPED（无交易）"
        orders = []
        total_fee_usd = 0.0
        cash_pool_end_cny = float(cash_pool_start)

        body = _build_email_body(
            asof_date=asof_date,
            fx_rate=float(fx_rate),
            sig=sig,
            md=md,
            prices=prices,
            orders=orders,
            total_fee_usd=float(total_fee_usd),
            cash_pool_start_cny=float(cash_pool_start),
            cash_pool_end_cny=float(cash_pool_end_cny),
            broker_result=broker_result,
            message="今日无交易信号（推荐买入=0）",
        )

        # 仍然落盘一份 orders/summary，方便你回看
        Path("data").mkdir(parents=True, exist_ok=True)
        Path(f"data/orders_{asof_date.isoformat()}.json").write_text(
            json.dumps(
                {
                    "date": asof_date.isoformat(),
                    "fx_usd_cny": float(fx_rate),
                    "prices_close": prices,
                    "signal": sig.signal,
                    "recommended_buy_cny": float(sig.recommended_buy_cny),
                    "orders": [],
                    "total_fee_usd": 0.0,
                    "message": "no_trade",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        summary = {
            "date": asof_date.isoformat(),
            "signal": sig.signal,
            "recommended_buy_cny": sig.recommended_buy_cny,
            "cash_pool_start_cny": cash_pool_start,
            "cash_pool_end_cny": cash_pool_end_cny,
            "reserve_balance_before_cny": getattr(sig, "reserve_balance_before", None),
            "reserve_use_cny": getattr(sig, "reserve_use_cny", 0.0),
            "reserve_add_cny": getattr(sig, "reserve_add_cny", 0.0),
            "rsp_close": md.close,
            "ma200": md.ma200,
            "month_high_close": md.month_high_close,
            "monthly_drawdown": getattr(sig, "monthly_drawdown", None),
            "orders": [],
            "total_fee_usd": 0.0,
            "broker_result": broker_result,
        }
        Path(f"data/summary_{asof_date.isoformat()}.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        if cfg.email.enabled:
            user = env_or_none(cfg.email.smtp_user_env)
            pwd = env_or_none(cfg.email.smtp_pass_env)
            to = env_or_none(cfg.email.to_env)
            if user and pwd and to:
                send_email(
                    smtp_host=cfg.email.smtp_host,
                    smtp_port=cfg.email.smtp_port,
                    user=user,
                    password=pwd,
                    to_addr=to,
                    subject=f"ETF 自动交易日报 {asof_date.isoformat()} {sig.signal}",
                    body=body,
                )

        return summary

    # 有交易：生成订单并执行
    orders, total_fee_usd, cash_pool_end_cny = allocate_orders(
        cfg,
        fx_usd_cny=float(fx_rate),
        holdings=holdings,
        prices=prices,
        buy_total_cny=sig.recommended_buy_cny,
        cash_pool_cny=cash_pool_start,
    )

    # 选择 broker
    if cfg.broker.mode == "alpaca":
        api_key = env_or_none("ALPACA_API_KEY")
        api_secret = env_or_none("ALPACA_API_SECRET")
        if not api_key or not api_secret:
            raise RuntimeError("broker.mode=alpaca 时需要环境变量 ALPACA_API_KEY / ALPACA_API_SECRET")
        broker = AlpacaBroker(api_key=api_key, api_secret=api_secret, paper=cfg.broker.alpaca_paper)
    else:
        broker = PaperBroker()

    broker_result = broker.place_orders(asof_date, orders)

    # 写 trade log
    row = {
        "date": asof_date.isoformat(),
        "month_key": sig.month_key.isoformat(),
        "signal": sig.signal,
        "base_buy_cny": round(sig.base_buy_cny, 4),
        "below_ma200": bool(sig.below_ma200) if sig.below_ma200 is not None else "",
        "reserve_add_cny": round(sig.reserve_add_cny, 4),
        "reserve_use_cny": round(sig.reserve_use_cny, 4),
        "recommended_buy_cny": round(sig.recommended_buy_cny, 4),
        "total_fee_usd": round(total_fee_usd, 6),
        "fx_usd_cny": round(float(fx_rate), 6),
        "price_IWY": round(float(prices.get("IWY", 0.0)), 6),
        "price_SPMO": round(float(prices.get("SPMO", 0.0)), 6),
        "price_RSP": round(float(prices.get("RSP", 0.0)), 6),
        "price_PFF": round(float(prices.get("PFF", 0.0)), 6),
        "price_VNQ": round(float(prices.get("VNQ", 0.0)), 6),
        "cash_pool_end_cny": round(cash_pool_end_cny, 4),
        "rsp_close": round(md.close, 6),
        "month_high_close": round(md.month_high_close, 6),
        "monthly_drawdown": round(sig.monthly_drawdown, 8) if sig.monthly_drawdown is not None else "",
        "third_friday": bool(sig.third_friday),
        "days_since_last_trade": int(sig.days_since_last_trade),
        "cooldown_ok": bool(sig.cooldown_ok),
    }
    append_trade_log(row)

    body = _build_email_body(
        asof_date=asof_date,
        fx_rate=float(fx_rate),
        sig=sig,
        md=md,
        prices=prices,
        orders=orders,
        total_fee_usd=float(total_fee_usd),
        cash_pool_start_cny=float(cash_pool_start),
        cash_pool_end_cny=float(cash_pool_end_cny),
        broker_result=str(broker_result),
    )

    if cfg.email.enabled:
        user = env_or_none(cfg.email.smtp_user_env)
        pwd = env_or_none(cfg.email.smtp_pass_env)
        to = env_or_none(cfg.email.to_env)
        if user and pwd and to:
            send_email(
                smtp_host=cfg.email.smtp_host,
                smtp_port=cfg.email.smtp_port,
                user=user,
                password=pwd,
                to_addr=to,
                subject=f"ETF 自动交易日报 {asof_date.isoformat()} {sig.signal}",
                body=body,
            )

    summary = {
        "date": asof_date.isoformat(),
        "signal": sig.signal,
        "recommended_buy_cny": sig.recommended_buy_cny,
        "cash_pool_start_cny": cash_pool_start,
        "cash_pool_end_cny": cash_pool_end_cny,
        "reserve_balance_before_cny": sig.reserve_balance_before,
        "reserve_use_cny": sig.reserve_use_cny,
        "reserve_add_cny": sig.reserve_add_cny,
        "rsp_close": md.close,
        "ma200": md.ma200,
        "month_high_close": md.month_high_close,
        "monthly_drawdown": sig.monthly_drawdown,
        "orders": [asdict(o) for o in orders if o.side != "HOLD"],
        "total_fee_usd": total_fee_usd,
        "broker_result": broker_result,
    }

    
    # 保存订单明细（方便你直接在券商端照单下单，也方便回溯）
    orders_out = {
        "date": asof_date.isoformat(),
        "fx_usd_cny": float(fx_rate),
        "prices_close": prices,
        "signal": sig.signal,
        "recommended_buy_cny": float(sig.recommended_buy_cny),
        "orders": [asdict(o) for o in orders],
        "total_fee_usd": float(total_fee_usd),
    }
    Path("data").mkdir(parents=True, exist_ok=True)
    Path(f"data/orders_{asof_date.isoformat()}.json").write_text(
        json.dumps(orders_out, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    Path(f"data/summary_{asof_date.isoformat()}.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    return summary
