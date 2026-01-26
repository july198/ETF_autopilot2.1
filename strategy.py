from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .calendar_utils import TradingCalendar
from .config import Config
from .fees import BuyFees
from .state import get_reserve_balance_cny


@dataclass(frozen=True)
class SignalResult:
    date: dt.date
    is_trading_day: bool
    third_friday: bool
    daily_return: Optional[float]
    monthly_drawdown: Optional[float]
    below_ma200: Optional[bool]
    month_key: dt.date
    trades_this_month: int
    has_first: bool
    has_second: bool
    has_third: bool
    days_since_last_trade: int
    cooldown_ok: bool
    month_limit_ok: bool
    signal: str  # NotTradingDay / None / First / Second / Third / ReserveOnly
    base_buy_cny: float
    reserve_add_cny: float
    reserve_use_cny: float
    recommended_buy_cny: float
    reserve_balance_before: float


def _month_key(d: dt.date) -> dt.date:
    return dt.date(d.year, d.month, 1)


def evaluate_signal(cfg: Config, cal: TradingCalendar, asof: dt.date,
                    rsp_close: float, rsp_prev_close: float, ma200: float, month_high: float,
                    trade_log: pd.DataFrame) -> SignalResult:

    is_trading_day = cal.is_trading_day(asof)
    third_friday = cal.third_friday(asof) if is_trading_day else False

    daily_ret = None
    if rsp_prev_close and rsp_prev_close != 0:
        daily_ret = rsp_close / rsp_prev_close - 1

    monthly_dd = None
    if month_high and month_high != 0:
        monthly_dd = rsp_close / month_high - 1

    below = None
    if ma200 and ma200 != 0:
        below = rsp_close < ma200

    mk = _month_key(asof)
    tlm = trade_log[trade_log.get("month_key", pd.Series([], dtype=object)) == mk] if not trade_log.empty else trade_log.iloc[0:0]
    trades_this_month = int(len(tlm))

    has_first = bool((tlm.get("signal", pd.Series([], dtype=str)) == "First").any()) if trades_this_month else False
    has_second = bool((tlm.get("signal", pd.Series([], dtype=str)) == "Second").any()) if trades_this_month else False
    has_third = bool((tlm.get("signal", pd.Series([], dtype=str)) == "Third").any()) if trades_this_month else False

    # 最近一笔日期（仅当月）
    last_trade_date = None
    if trades_this_month and "date" in tlm.columns:
        last_trade_date = tlm["date"].dropna().max()
        if pd.isna(last_trade_date):
            last_trade_date = None

    if last_trade_date is None or not is_trading_day:
        days_since = 999
    else:
        # 交易日序号差
        try:
            days_since = cal.trading_days_between(last_trade_date, asof)
        except Exception:
            days_since = 999

    cooldown_ok = bool(days_since >= cfg.params.cooldown_trading_days)
    month_limit_ok = bool(trades_this_month < cfg.params.max_trades_per_month)

    reserve_balance = get_reserve_balance_cny(trade_log)

    # Trigger conditions per Excel DailyCheck
    first_trigger = (
        is_trading_day
        and trades_this_month == 0
        and cooldown_ok
        and (
            (daily_ret is not None and daily_ret <= cfg.params.first_daily_drop_threshold)
            or third_friday
        )
    )
    second_trigger = (
        is_trading_day
        and has_first
        and (not has_second)
        and cooldown_ok
        and month_limit_ok
        and (monthly_dd is not None and monthly_dd <= cfg.params.second_drawdown_threshold)
    )
    third_trigger = (
        is_trading_day
        and has_second
        and (not has_third)
        and cooldown_ok
        and month_limit_ok
        and (monthly_dd is not None and monthly_dd <= cfg.params.third_drawdown_threshold)
    )

    use_reserve = (
        is_trading_day
        and reserve_balance > 0
        and cooldown_ok
        and (rsp_close >= ma200 or second_trigger or third_trigger)
    )

    reserve_only = (
        is_trading_day
        and use_reserve
        and (not (first_trigger or second_trigger or third_trigger))
    )

    if not is_trading_day:
        signal = "NotTradingDay"
    elif third_trigger:
        signal = "Third"
    elif second_trigger:
        signal = "Second"
    elif first_trigger:
        signal = "First"
    elif reserve_only:
        signal = "ReserveOnly"
    else:
        signal = "None"

    invest = cfg.params.invest_cny_per_trade

    if signal == "First":
        if below is True:
            base = invest * cfg.params.first_buy_ratio_below_ma200
        else:
            base = invest
    elif signal in ("Second", "Third"):
        base = invest
    else:
        base = 0.0

    reserve_add = 0.0
    if signal == "First" and below is True:
        reserve_add = invest * (1 - cfg.params.first_buy_ratio_below_ma200)

    reserve_use = reserve_balance if use_reserve else 0.0

    recommended_buy = 0.0
    if signal not in ("NotTradingDay", "None"):
        recommended_buy = base + reserve_use

    return SignalResult(
        date=asof,
        is_trading_day=is_trading_day,
        third_friday=third_friday,
        daily_return=daily_ret,
        monthly_drawdown=monthly_dd,
        below_ma200=below,
        month_key=mk,
        trades_this_month=trades_this_month,
        has_first=has_first,
        has_second=has_second,
        has_third=has_third,
        days_since_last_trade=int(days_since),
        cooldown_ok=cooldown_ok,
        month_limit_ok=month_limit_ok,
        signal=signal,
        base_buy_cny=float(base),
        reserve_add_cny=float(reserve_add),
        reserve_use_cny=float(reserve_use),
        recommended_buy_cny=float(recommended_buy),
        reserve_balance_before=float(reserve_balance),
    )


@dataclass
class OrderLine:
    ticker: str
    side: str  # BUY/SELL/HOLD
    shares: float
    price: float
    est_fee_usd: float
    est_gross_usd: float
    note: str = ""


def _round_down(x: float, step: float) -> float:
    if step <= 0:
        return x
    return (x // step) * step


def affordable_buy_shares(
    usd_budget: float,
    price: float,
    allow_fractional: bool,
    step: float,
    buy_fees: BuyFees,
) -> Tuple[float, float]:
    """
    找到最大可买 shares，使 shares*price + fee(shares) <= usd_budget
    返回 (shares, fee)
    """
    if usd_budget <= 0 or price <= 0:
        return 0.0, 0.0

    if allow_fractional:
        raw = usd_budget / price
        shares = _round_down(raw, step)
        # 防止浮点误差
        shares = float(max(0.0, round(shares, 10)))
        for _ in range(200000):
            fee = buy_fees.fee(shares)
            if shares * price + fee <= usd_budget + 1e-10:
                return shares, fee
            shares = float(max(0.0, shares - step))
            if shares <= 0:
                return 0.0, 0.0
        # fallback
        fee = buy_fees.fee(shares)
        return shares, fee
    else:
        shares = int(usd_budget // price)
        for _ in range(100000):
            fee = buy_fees.fee(float(shares))
            if shares * price + fee <= usd_budget + 1e-10:
                return float(shares), fee
            shares -= 1
            if shares <= 0:
                return 0.0, 0.0
        fee = buy_fees.fee(float(shares))
        return float(shares), fee


def allocate_orders(
    cfg: Config,
    holdings: pd.DataFrame,
    prices: Dict[str, float],
    buy_total_cny: float,
    cash_pool_cny: float,
    fx_usd_cny: float | None = None,
) -> Tuple[List[OrderLine], float, float]:
    """
    返回：订单列表、预计总手续费USD、预计交易后零钱池余额(CNY)
    只生成 BUY 订单；卖出留给 8 月再平衡脚本。
    """
    if buy_total_cny <= 0:
        return [], 0.0, cash_pool_cny

    fx = float(fx_usd_cny) if fx_usd_cny is not None else cfg.params.fx_usd_cny
    total_cny = float(buy_total_cny + (cash_pool_cny if cfg.cash_pool.enabled else 0.0))

    # 组合当前市值与权重
    df = holdings.copy()
    df["price"] = df["ticker"].map(prices).astype(float)
    df["value_cny"] = df["shares"].astype(float) * df["price"] * fx
    port_value = float(df["value_cny"].sum())
    # 若没持仓，按等权处理（避免除零）
    if port_value <= 0:
        df["weight"] = 0.0
    else:
        df["weight"] = df["value_cny"] / port_value

    target = cfg.params.target_weight_each
    ceiling = cfg.params.weight_ceiling_guardrail

    def under_score(w: float) -> float:
        if w >= ceiling:
            return 0.0
        return max(0.0, target - w)

    df["underscore"] = df["weight"].apply(under_score)

    sum_us = float(df["underscore"].sum())

    # Suggested buy per ticker (CNY)
    suggested: Dict[str, float] = {}
    if sum_us == 0:
        for t in df["ticker"]:
            suggested[t] = total_cny / len(df)
        top1 = df.iloc[0]["ticker"]
        top2 = ""
    else:
        # top 2 underscore
        df_sorted = df.sort_values("underscore", ascending=False).reset_index(drop=True)
        top1 = str(df_sorted.iloc[0]["ticker"])
        top1s = float(df_sorted.iloc[0]["underscore"])
        top2s = float(df_sorted.iloc[1]["underscore"]) if len(df_sorted) > 1 else 0.0
        top2 = str(df_sorted.iloc[1]["ticker"]) if top2s > 0 else ""
        if top2s == 0:
            for t in df["ticker"]:
                suggested[t] = total_cny if t == top1 else 0.0
        else:
            denom = top1s + top2s
            for t in df["ticker"]:
                if t == top1:
                    suggested[t] = total_cny * top1s / denom
                elif t == top2:
                    suggested[t] = total_cny * top2s / denom
                else:
                    suggested[t] = 0.0

    # Convert to USD budgets, subtract spread cost
    spread = cfg.execution.spread_cost_pct
    allow_frac = cfg.execution.allow_fractional_shares
    step = cfg.execution.fractional_step
    buy_fees = BuyFees(
        commission_per_share=cfg.fees_buy.commission_per_share,
        commission_min_usd=cfg.fees_buy.commission_min_usd,
        platform_per_share=cfg.fees_buy.platform_per_share,
        platform_min_usd=cfg.fees_buy.platform_min_usd,
        clearing_per_share=cfg.fees_buy.clearing_per_share,
        other_fixed_fee_usd=cfg.execution.other_fixed_fee_usd,
    )

    # First pass: base shares per ticker
    orders: Dict[str, OrderLine] = {}
    leftover_usd_pool = 0.0

    for t in df["ticker"]:
        cny = float(suggested.get(t, 0.0))
        if cny <= 0:
            orders[t] = OrderLine(ticker=t, side="HOLD", shares=0.0, price=float(prices[t]),
                                  est_fee_usd=0.0, est_gross_usd=0.0, note="")
            continue
        usd_budget = (cny / fx) * (1 - spread)
        shares, fee = affordable_buy_shares(usd_budget, float(prices[t]), allow_frac, step, buy_fees)
        gross = shares * float(prices[t])
        cost = gross + fee
        leftover = max(0.0, usd_budget - cost)
        leftover_usd_pool += leftover
        note = "OK" if shares > 0 else "整股/费用限制导致0股"
        orders[t] = OrderLine(ticker=t, side="BUY" if shares > 0 else "HOLD", shares=shares, price=float(prices[t]),
                              est_fee_usd=fee, est_gross_usd=gross, note=note)

    # Second allocation: use leftover pool to top1 then top2 (only if base order is BUY)
    def inc_shares(ticker: str, pool: float) -> Tuple[float, float]:
        if pool <= 0:
            return 0.0, pool
        ol = orders[ticker]
        if ol.side != "BUY" or ol.shares <= 0:
            return 0.0, pool

        old_shares = ol.shares
        old_fee = buy_fees.fee(old_shares)
        old_cost = old_shares * ol.price + old_fee

        # upper bound add
        if allow_frac:
            add_est = _round_down(pool / ol.price, step)
            add_est = float(max(0.0, round(add_est, 10)))
            # binary-ish via decrement loop
            add = add_est
            for _ in range(200000):
                new_shares = old_shares + add
                new_fee = buy_fees.fee(new_shares)
                new_cost = new_shares * ol.price + new_fee
                inc = new_cost - old_cost
                if inc <= pool + 1e-10:
                    # accept
                    orders[ticker].shares = float(new_shares)
                    orders[ticker].est_fee_usd = float(new_fee)
                    orders[ticker].est_gross_usd = float(new_shares * ol.price)
                    orders[ticker].note = "OK(含二次分配)"
                    return add, float(pool - inc)
                add = float(max(0.0, add - step))
                if add <= 0:
                    return 0.0, pool
        else:
            add_est = int(pool // ol.price)
            add = add_est
            while add > 0:
                new_shares = old_shares + add
                new_fee = buy_fees.fee(float(new_shares))
                new_cost = float(new_shares) * ol.price + new_fee
                inc = new_cost - old_cost
                if inc <= pool + 1e-10:
                    orders[ticker].shares = float(new_shares)
                    orders[ticker].est_fee_usd = float(new_fee)
                    orders[ticker].est_gross_usd = float(new_shares * ol.price)
                    orders[ticker].note = "OK(含二次分配)"
                    return float(add), float(pool - inc)
                add -= 1
            return 0.0, pool

    # Apply
    if sum_us == 0:
        # equal split时，top1/top2无意义，仍然沿用 underscore 排名
        # 这里用当前 underscore 最大的 1-2 只
        df_sorted = df.sort_values("underscore", ascending=False).reset_index(drop=True)
        top1 = str(df_sorted.iloc[0]["ticker"])
        top2 = str(df_sorted.iloc[1]["ticker"]) if len(df_sorted) > 1 and float(df_sorted.iloc[1]["underscore"]) > 0 else ""
    if top1:
        _, leftover_usd_pool = inc_shares(top1, leftover_usd_pool)
    if top2:
        _, leftover_usd_pool = inc_shares(top2, leftover_usd_pool)

    order_list = []
    total_fee = 0.0
    total_leftover_cny = 0.0

    for t in df["ticker"]:
        ol = orders[t]
        if ol.side == "BUY" and ol.shares > 0:
            total_fee += ol.est_fee_usd

    # 新零钱池：把剩余的 USD（理论上就是 leftover_usd_pool）换回 CNY
    total_leftover_cny = float(leftover_usd_pool * fx)

    return list(orders.values()), float(total_fee), float(total_leftover_cny)


def build_equal_weight_init_orders(
    cfg: Config,
    holdings: pd.DataFrame,
    prices: Dict[str, float],
    invest_cny: float,
) -> Tuple[List[OrderLine], float, float]:
    """一次性等权建仓（只生成 BUY 订单）

    返回：订单列表、预计使用USD、预计手续费USD
    """
    tickers = list(cfg.symbols.portfolio)
    n = len(tickers)
    if n == 0 or invest_cny <= 0:
        return [], 0.0, 0.0

    fx = float(cfg.params.fx_usd_cny)
    spread = float(cfg.execution.spread_cost_pct)
    allow_frac = bool(cfg.execution.allow_fractional_shares)
    step = float(cfg.execution.fractional_step)

    invest_usd = float(invest_cny) / fx
    avail_usd = invest_usd * (1.0 - spread)
    per_usd = avail_usd / n

    buy_fees = BuyFees(
        commission_per_share=cfg.fees_buy.commission_per_share,
        commission_min_usd=cfg.fees_buy.commission_min_usd,
        platform_per_share=cfg.fees_buy.platform_per_share,
        platform_min_usd=cfg.fees_buy.platform_min_usd,
        clearing_per_share=cfg.fees_buy.clearing_per_share,
        other_fixed_fee_usd=cfg.execution.other_fixed_fee_usd,
    )

    orders: List[OrderLine] = []
    used_total = 0.0
    fee_total = 0.0

    # First pass: per-ticker budget
    per_used: Dict[str, float] = {}
    for t in tickers:
        price = float(prices[t])
        if price <= 0:
            orders.append(OrderLine(ticker=t, side="HOLD", shares=0.0, price=price, est_fee_usd=0.0, est_gross_usd=0.0, note="bad price"))
            per_used[t] = 0.0
            continue

        # initial guess
        shares = per_usd / price
        if allow_frac:
            shares = _round_down(shares, step)
        else:
            shares = float(int(shares))

        # adjust down until within budget after fees
        for _ in range(10000):
            fee = buy_fees.fee(shares) if shares > 0 else 0.0
            cost = shares * price + fee
            if cost <= per_usd + 1e-9:
                break
            shares = shares - (step if allow_frac else 1.0)
            if shares <= 0:
                shares = 0.0
                break

        fee = buy_fees.fee(shares) if shares > 0 else 0.0
        gross = shares * price
        cost = gross + fee

        orders.append(OrderLine(
            ticker=t,
            side="BUY" if shares > 0 else "HOLD",
            shares=float(shares),
            price=price,
            est_fee_usd=float(fee),
            est_gross_usd=float(gross),
            note="init equal-weight",
        ))
        per_used[t] = float(cost)
        used_total += float(cost)
        fee_total += float(fee)

    # Second pass: try to spend remaining on the most under-spent ticker (keeps weights closer to equal)
    remaining = float(avail_usd - used_total)
    if remaining > 0 and allow_frac:
        # choose ticker with smallest used relative to per_usd
        t_best = min(tickers, key=lambda x: per_used.get(x, 0.0))
        ol = next(o for o in orders if o.ticker == t_best)
        if ol.side == "BUY" and ol.price > 0:
            old_sh = ol.shares
            old_fee = ol.est_fee_usd
            old_cost = old_sh * ol.price + old_fee

            add_sh = _round_down(remaining / ol.price, step)
            # shrink until feasible after fee increase
            for _ in range(10000):
                new_sh = old_sh + add_sh
                new_fee = buy_fees.fee(new_sh)
                new_cost = new_sh * ol.price + new_fee
                inc = new_cost - old_cost
                if inc <= remaining + 1e-9:
                    ol.shares = float(new_sh)
                    ol.est_fee_usd = float(new_fee)
                    ol.est_gross_usd = float(new_sh * ol.price)
                    ol.note = "init equal-weight + leftover"
                    used_total += float(inc)
                    fee_total += float(new_fee - old_fee)
                    remaining -= float(inc)
                    break
                add_sh = max(0.0, add_sh - step)
                if add_sh <= 0:
                    break

    return orders, float(used_total), float(fee_total)
