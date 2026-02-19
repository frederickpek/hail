from __future__ import annotations

import math
from datetime import datetime, timezone

from hail.config import Settings
from hail.models import MarketState, PositionState, TradeIntent


def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


class ProbabilityModel:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def fair_yes_probability(
        self,
        symbol: str,
        spot: float,
        strike: float,
        end_time: datetime,
        annual_vol: float,
    ) -> float:
        now = datetime.now(tz=timezone.utc)
        seconds = max((end_time - now).total_seconds(), 1.0)
        tau = seconds / (365.0 * 24.0 * 3600.0)
        sigma = max(0.05, annual_vol)

        d2 = (math.log(max(spot, 1e-9) / strike) - 0.5 * sigma * sigma * tau) / (
            sigma * math.sqrt(tau)
        )
        p = normal_cdf(d2)
        return max(0.001, min(0.999, p))


class Strategy:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._model = ProbabilityModel(settings)

    def compute_fair_yes(self, market: MarketState, spot: float, annual_vol: float) -> float:
        return self._model.fair_yes_probability(
            symbol=market.market.symbol,
            spot=spot,
            strike=market.market.strike,
            end_time=market.market.end_time,
            annual_vol=annual_vol,
        )

    def generate_intents(self, market: MarketState, position: PositionState) -> list[TradeIntent]:
        intents: list[TradeIntent] = []
        fair_yes = market.fair_yes_prob
        if fair_yes is None:
            return intents
        fair_no = 1.0 - fair_yes

        yes_bid = market.yes_book.best_bid
        yes_ask = market.yes_book.best_ask
        no_bid = market.no_book.best_bid
        no_ask = market.no_book.best_ask

        condition_id = market.market.condition_id
        size = self._settings.default_order_size
        max_pos = self._settings.max_position_per_market

        # Entry logic.
        if yes_ask is not None and (fair_yes - yes_ask) >= self._settings.min_entry_edge:
            if position.qty_yes < max_pos:
                intents.append(
                    TradeIntent(
                        condition_id=condition_id,
                        token_id=market.market.yes_token_id,
                        side="BUY",
                        price=yes_ask,
                        size=min(size, max_pos - position.qty_yes),
                        reason=f"ENTRY_YES edge={fair_yes - yes_ask:.4f}",
                    )
                )

        if no_ask is not None and (fair_no - no_ask) >= self._settings.min_entry_edge:
            if position.qty_no < max_pos:
                intents.append(
                    TradeIntent(
                        condition_id=condition_id,
                        token_id=market.market.no_token_id,
                        side="BUY",
                        price=no_ask,
                        size=min(size, max_pos - position.qty_no),
                        reason=f"ENTRY_NO edge={fair_no - no_ask:.4f}",
                    )
                )

        # Exit logic from profitable inventory.
        if yes_bid is not None and position.qty_yes > 0:
            if yes_bid - position.avg_yes_price >= self._settings.min_exit_edge:
                intents.append(
                    TradeIntent(
                        condition_id=condition_id,
                        token_id=market.market.yes_token_id,
                        side="SELL",
                        price=yes_bid,
                        size=min(size, position.qty_yes),
                        reason=f"EXIT_YES gain={yes_bid - position.avg_yes_price:.4f}",
                    )
                )

        if no_bid is not None and position.qty_no > 0:
            if no_bid - position.avg_no_price >= self._settings.min_exit_edge:
                intents.append(
                    TradeIntent(
                        condition_id=condition_id,
                        token_id=market.market.no_token_id,
                        side="SELL",
                        price=no_bid,
                        size=min(size, position.qty_no),
                        reason=f"EXIT_NO gain={no_bid - position.avg_no_price:.4f}",
                    )
                )

        # Hedge logic: buy opposite side if complete set is sufficiently cheap.
        if position.qty_yes > position.qty_no and no_ask is not None and position.qty_yes > 0:
            if position.avg_yes_price + no_ask <= (1.0 - self._settings.min_hedge_margin):
                hedge_size = min(size, position.qty_yes - position.qty_no, max_pos - position.qty_no)
                if hedge_size > 0:
                    intents.append(
                        TradeIntent(
                            condition_id=condition_id,
                            token_id=market.market.no_token_id,
                            side="BUY",
                            price=no_ask,
                            size=hedge_size,
                            reason=(
                                "HEDGE_BUY_NO "
                                f"combo={position.avg_yes_price + no_ask:.4f}"
                            ),
                        )
                    )

        if position.qty_no > position.qty_yes and yes_ask is not None and position.qty_no > 0:
            if position.avg_no_price + yes_ask <= (1.0 - self._settings.min_hedge_margin):
                hedge_size = min(size, position.qty_no - position.qty_yes, max_pos - position.qty_yes)
                if hedge_size > 0:
                    intents.append(
                        TradeIntent(
                            condition_id=condition_id,
                            token_id=market.market.yes_token_id,
                            side="BUY",
                            price=yes_ask,
                            size=hedge_size,
                            reason=(
                                "HEDGE_BUY_YES "
                                f"combo={position.avg_no_price + yes_ask:.4f}"
                            ),
                        )
                    )

        return intents


def mark_to_market_unrealized(market: MarketState, position: PositionState) -> float:
    pnl = 0.0
    if position.qty_yes > 0 and market.yes_book.best_bid is not None:
        pnl += (market.yes_book.best_bid - position.avg_yes_price) * position.qty_yes
    if position.qty_no > 0 and market.no_book.best_bid is not None:
        pnl += (market.no_book.best_bid - position.avg_no_price) * position.qty_no
    return pnl
