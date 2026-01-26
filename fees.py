from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BuyFees:
    commission_per_share: float
    commission_min_usd: float
    platform_per_share: float
    platform_min_usd: float
    clearing_per_share: float
    other_fixed_fee_usd: float = 0.0

    def fee(self, shares: float) -> float:
        if shares <= 0:
            return 0.0
        comm = max(self.commission_min_usd, self.commission_per_share * shares)
        plat = max(self.platform_min_usd, self.platform_per_share * shares)
        clearing = self.clearing_per_share * shares
        return comm + plat + clearing + self.other_fixed_fee_usd


@dataclass(frozen=True)
class SellExtraFees:
    activity_per_share: float
    activity_min_usd: float
    activity_max_usd: float
    cat_per_share: float
    sec_fee_usd: float = 0.0

    def fee(self, shares: float) -> float:
        # shares is absolute shares sold
        if shares <= 0:
            return 0.0
        activity = self.activity_per_share * shares
        activity = min(self.activity_max_usd, max(self.activity_min_usd, activity))
        cat = self.cat_per_share * shares
        return activity + cat + self.sec_fee_usd
