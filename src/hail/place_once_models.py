from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class PlaceOnceOrder:
    condition_id: str
    token_id: str
    outcome: str
    side: str
    price: float
    size: float
    reason: str


@dataclass(slots=True)
class MarketFillSummary:
    condition_id: str
    yes_filled_qty: float
    yes_avg_price: float
    no_filled_qty: float
    no_avg_price: float

    @property
    def total_filled_qty(self) -> float:
        return max(self.yes_filled_qty, 0.0) + max(self.no_filled_qty, 0.0)

    @property
    def fill_profile(self) -> str:
        yes_filled = self.yes_filled_qty > 1e-9
        no_filled = self.no_filled_qty > 1e-9
        if yes_filled and no_filled:
            return "BOTH_FILLED"
        if yes_filled or no_filled:
            return "ONE_FILLED"
        return "NONE_FILLED"

