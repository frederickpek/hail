from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass(slots=True)
class MarketDefinition:
    condition_id: str
    slug: str
    question: str
    symbol: str
    window_minutes: int
    strike: float
    end_time: datetime
    yes_token_id: str
    no_token_id: str


@dataclass(slots=True)
class OrderBookTop:
    best_bid: float | None = None
    best_ask: float | None = None
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(slots=True)
class MarketState:
    market: MarketDefinition
    yes_book: OrderBookTop = field(default_factory=OrderBookTop)
    no_book: OrderBookTop = field(default_factory=OrderBookTop)
    fair_yes_prob: float | None = None
    fair_updated_at: datetime | None = None


@dataclass(slots=True)
class PositionState:
    qty_yes: float = 0.0
    avg_yes_price: float = 0.0
    qty_no: float = 0.0
    avg_no_price: float = 0.0
    realized_pnl: float = 0.0


@dataclass(slots=True)
class TradeIntent:
    condition_id: str
    token_id: str
    side: str
    price: float
    size: float
    reason: str
