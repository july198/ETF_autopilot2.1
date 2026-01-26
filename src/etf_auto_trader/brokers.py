from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from .strategy import OrderLine


class BrokerBase:
    def place_orders(self, date: dt.date, orders: List[OrderLine]) -> str:
        raise NotImplementedError


@dataclass
class PaperBroker(BrokerBase):
    out_dir: Path = Path("data")

    def place_orders(self, date: dt.date, orders: List[OrderLine]) -> str:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "date": date.isoformat(),
            "orders": [
                {
                    "ticker": o.ticker,
                    "side": o.side,
                    "shares": o.shares,
                    "price": o.price,
                    "est_fee_usd": o.est_fee_usd,
                    "est_gross_usd": o.est_gross_usd,
                    "note": o.note,
                }
                for o in orders
                if o.side != "HOLD"
            ],
        }
        p = self.out_dir / f"orders_{date.isoformat()}.json"
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return f"paper: wrote {p}"


@dataclass
class AlpacaBroker(BrokerBase):
    api_key: str
    api_secret: str
    paper: bool = True

    def place_orders(self, date: dt.date, orders: List[OrderLine]) -> str:
        # alpaca-py is required only when you use this broker
        from alpaca.trading.client import TradingClient  # type: ignore
        from alpaca.trading.requests import MarketOrderRequest  # type: ignore
        from alpaca.trading.enums import OrderSide, TimeInForce  # type: ignore

        client = TradingClient(self.api_key, self.api_secret, paper=self.paper)

        placed = 0
        for o in orders:
            if o.side != "BUY" or o.shares <= 0:
                continue
            req = MarketOrderRequest(
                symbol=o.ticker,
                qty=float(o.shares),
                side=OrderSide.BUY,
                time_in_force=TimeInForce.DAY,
            )
            client.submit_order(req)
            placed += 1

        return f"alpaca: submitted {placed} buy orders"
