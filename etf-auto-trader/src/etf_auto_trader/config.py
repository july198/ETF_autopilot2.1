from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
import os
import yaml


@dataclass(frozen=True)
class Symbols:
    portfolio: List[str]
    signal: str


@dataclass(frozen=True)
class FeesBuy:
    commission_per_share: float
    commission_min_usd: float
    platform_per_share: float
    platform_min_usd: float
    clearing_per_share: float


@dataclass(frozen=True)
class FeesSellExtra:
    activity_per_share: float
    activity_min_usd: float
    activity_max_usd: float
    cat_per_share: float
    sec_fee_usd: float


@dataclass(frozen=True)
class Params:
    fx_usd_cny: float
    fx_mode: str  # auto | fixed
    fx_symbol: str  # e.g. USDCNY=X (Yahoo Finance)
    fx_fallback_usd_cny: float
    invest_cny_per_trade: float
    first_buy_ratio_below_ma200: float
    first_daily_drop_threshold: float
    second_drawdown_threshold: float
    third_drawdown_threshold: float
    cooldown_trading_days: int
    max_trades_per_month: int
    target_weight_each: float
    weight_floor_guardrail: float
    weight_ceiling_guardrail: float


@dataclass(frozen=True)
class Execution:
    allow_fractional_shares: bool
    fractional_step: float
    spread_cost_pct: float
    other_fixed_fee_usd: float


@dataclass(frozen=True)
class CashPool:
    enabled: bool
    source: str  # AUTO / MANUAL
    manual_cny: float


@dataclass(frozen=True)
class Broker:
    mode: str
    alpaca_paper: bool


@dataclass(frozen=True)
class EmailNotify:
    enabled: bool
    smtp_host: str
    smtp_port: int
    smtp_user_env: str
    smtp_pass_env: str
    to_env: str


@dataclass(frozen=True)
class Bootstrap:
    initial_invest_cny: float
    cash_buffer_usd: float
    equal_weight: bool


@dataclass(frozen=True)
class AppCfg:
    timezone: str
    base_currency: str


@dataclass(frozen=True)
class Config:
    app: AppCfg
    symbols: Symbols
    params: Params
    execution: Execution
    cash_pool: CashPool
    fees_buy: FeesBuy
    fees_sell_extra: FeesSellExtra
    broker: Broker
    email: EmailNotify
    bootstrap: Bootstrap


def _req(d: Dict[str, Any], key: str) -> Any:
    if key not in d:
        raise KeyError(f"Missing config key: {key}")
    return d[key]


def load_config(path: str | Path = "config.yaml") -> Config:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"找不到 {p}. 先复制 config.example.yaml 为 config.yaml 再修改。"
        )

    raw = yaml.safe_load(p.read_text(encoding="utf-8"))

    app = raw.get("app", {})
    symbols = raw.get("symbols", {})
    params = raw.get("params", {})
    execution = raw.get("execution", {})
    cash_pool = raw.get("cash_pool", {})
    fees = raw.get("fees", {})
    broker = raw.get("broker", {})
    bootstrap = raw.get("bootstrap", {})
    notify = raw.get("notify", {})
    email = notify.get("email", {})

    cfg = Config(
        app=AppCfg(
            timezone=str(app.get("timezone", "America/New_York")),
            base_currency=str(app.get("base_currency", "CNY")),
        ),
        symbols=Symbols(
            portfolio=list(symbols.get("portfolio", ["IWY", "SPMO", "RSP", "PFF", "VNQ"])),
            signal=str(symbols.get("signal", "RSP")),
        ),
        params=Params(
            fx_usd_cny=float(_req(params, "fx_usd_cny")),
            fx_mode=str(params.get("fx_mode", "fixed")),
            fx_symbol=str(params.get("fx_symbol", "USDCNY=X")),
            fx_fallback_usd_cny=float(params.get("fx_fallback_usd_cny", _req(params, "fx_usd_cny"))),
            invest_cny_per_trade=float(_req(params, "invest_cny_per_trade")),
            first_buy_ratio_below_ma200=float(_req(params, "first_buy_ratio_below_ma200")),
            first_daily_drop_threshold=float(_req(params, "first_daily_drop_threshold")),
            second_drawdown_threshold=float(_req(params, "second_drawdown_threshold")),
            third_drawdown_threshold=float(_req(params, "third_drawdown_threshold")),
            cooldown_trading_days=int(_req(params, "cooldown_trading_days")),
            max_trades_per_month=int(_req(params, "max_trades_per_month")),
            target_weight_each=float(_req(params, "target_weight_each")),
            weight_floor_guardrail=float(_req(params, "weight_floor_guardrail")),
            weight_ceiling_guardrail=float(_req(params, "weight_ceiling_guardrail")),
        ),
        execution=Execution(
            allow_fractional_shares=bool(execution.get("allow_fractional_shares", True)),
            fractional_step=float(execution.get("fractional_step", 0.0001)),
            spread_cost_pct=float(execution.get("spread_cost_pct", 0.001)),
            other_fixed_fee_usd=float(execution.get("other_fixed_fee_usd", 0.0)),
        ),
        cash_pool=CashPool(
            enabled=bool(cash_pool.get("enabled", True)),
            source=str(cash_pool.get("source", "AUTO")).upper(),
            manual_cny=float(cash_pool.get("manual_cny", 0.0)),
        ),
        fees_buy=FeesBuy(
            commission_per_share=float(_req(_req(fees, "buy"), "commission_per_share")),
            commission_min_usd=float(_req(_req(fees, "buy"), "commission_min_usd")),
            platform_per_share=float(_req(_req(fees, "buy"), "platform_per_share")),
            platform_min_usd=float(_req(_req(fees, "buy"), "platform_min_usd")),
            clearing_per_share=float(_req(_req(fees, "buy"), "clearing_per_share")),
        ),
        fees_sell_extra=FeesSellExtra(
            activity_per_share=float(_req(_req(fees, "sell_extra"), "activity_per_share")),
            activity_min_usd=float(_req(_req(fees, "sell_extra"), "activity_min_usd")),
            activity_max_usd=float(_req(_req(fees, "sell_extra"), "activity_max_usd")),
            cat_per_share=float(_req(_req(fees, "sell_extra"), "cat_per_share")),
            sec_fee_usd=float(_req(_req(fees, "sell_extra"), "sec_fee_usd")),
        ),
        broker=Broker(
            mode=str(broker.get("mode", "paper")).lower(),
            alpaca_paper=bool(broker.get("alpaca", {}).get("paper", True)),
        ),
        email=EmailNotify(
            enabled=bool(email.get("enabled", False)),
            smtp_host=str(email.get("smtp_host", "smtp.qq.com")),
            smtp_port=int(email.get("smtp_port", 465)),
            smtp_user_env=str(email.get("smtp_user_env", "SMTP_USER")),
            smtp_pass_env=str(email.get("smtp_pass_env", "SMTP_PASS")),
            to_env=str(email.get("to_env", "SMTP_TO")),
        ),
        bootstrap=Bootstrap(
            initial_invest_cny=float(bootstrap.get("initial_invest_cny", 0.0)),
            cash_buffer_usd=float(bootstrap.get("cash_buffer_usd", 0.0)),
            equal_weight=bool(bootstrap.get("equal_weight", True)),
        ),
    )

    return cfg
def env_or_none(name: str) -> Optional[str]:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return None
    return v.strip()
