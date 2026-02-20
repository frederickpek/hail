from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from hail.binance_feed import BinanceMidPriceFeed
from hail.config import Settings
from hail.db import BotDatabase
from hail.models import MarketDefinition, MarketState, OrderBookTop, PositionState, TradeIntent
from hail.polymarket_gamma import GammaMarketScanner
from hail.polymarket_trader import PolymarketTrader
from hail.polymarket_ws import PolymarketBookFeed
from hail.strategy import Strategy, mark_to_market_unrealized
from hail.telegram_notifier import TelegramNotifier

class TradingEngine:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._db = BotDatabase(str(settings.db_path))
        self._scanner = GammaMarketScanner(settings)
        self._trader = PolymarketTrader(settings)
        self._binance = BinanceMidPriceFeed(settings)
        self._strategy = Strategy(settings)
        self._notifier = TelegramNotifier(settings)
        self._stop_event = asyncio.Event()

        self._states: dict[str, MarketState] = {}
        self._positions: dict[str, PositionState] = {}
        self._token_to_market: dict[str, tuple[str, str]] = {}
        self._scan_tick = 0
        self._decision_tick = 0

        self._book_feed = PolymarketBookFeed(settings, self._on_book_update)

    async def run(self) -> None:
        self._settings.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._settings.log_path.parent.mkdir(parents=True, exist_ok=True)

        await self._db.init()
        await self._trader.connect()

        tasks = [
            asyncio.create_task(self._binance.start(), name="binance-feed"),
            asyncio.create_task(self._book_feed.start(), name="polymarket-feed"),
            asyncio.create_task(self._scan_loop(), name="scan-loop"),
            asyncio.create_task(self._decision_loop(), name="decision-loop"),
            asyncio.create_task(self._telegram_loop(), name="telegram-loop"),
        ]
        try:
            await self._stop_event.wait()
        finally:
            self._book_feed.stop()
            self._binance.stop()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def stop(self) -> None:
        self._stop_event.set()

    def _on_book_update(self, token_id: str, top: OrderBookTop) -> None:
        marker = self._token_to_market.get(token_id)
        if marker is None:
            return
        condition_id, side = marker
        state = self._states.get(condition_id)
        if state is None:
            return
        if side == "YES":
            state.yes_book = top
        else:
            state.no_book = top

    async def _scan_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._scan_tick += 1
                markets = await self._scanner.scan()
                stats = await self._register_markets(markets)
                logging.info(
                    (
                        "scan-loop tick=%s fetched=%s active=%s new=%s refreshed=%s "
                        "expired_skipped=%s removed=%s subscribed_assets=%s tracked_tokens=%s"
                    ),
                    self._scan_tick,
                    stats["fetched"],
                    stats["active"],
                    stats["new"],
                    stats["refreshed"],
                    stats["expired_skipped"],
                    stats["removed"],
                    stats["subscribed_assets"],
                    stats["tracked_tokens"],
                )
            except Exception as exc:  # noqa: BLE001
                logging.warning("scan loop error: %s", exc)
            await asyncio.sleep(self._settings.scan_interval_seconds)

    async def _register_markets(self, markets: list[MarketDefinition]) -> dict[str, int]:
        token_ids: list[str] = []
        now = datetime.now(tz=timezone.utc)
        active_condition_ids = set()
        new_count = 0
        refreshed_count = 0
        expired_skipped_count = 0
        for market in markets:
            if market.end_time <= now:
                expired_skipped_count += 1
                continue
            active_condition_ids.add(market.condition_id)
            if market.condition_id not in self._states:
                self._states[market.condition_id] = MarketState(market=market)
                new_count += 1
                logging.info(
                    "New target market: %s %s %s",
                    market.symbol,
                    market.window_minutes,
                    market.slug,
                )
            else:
                self._states[market.condition_id].market = market
                refreshed_count += 1

            self._token_to_market[market.yes_token_id] = (market.condition_id, "YES")
            self._token_to_market[market.no_token_id] = (market.condition_id, "NO")
            token_ids.extend([market.yes_token_id, market.no_token_id])
            await self._db.upsert_market(market)

        if token_ids:
            await self._book_feed.add_assets(token_ids)

        # Clean out expired/untracked states.
        to_remove = []
        for condition_id, state in self._states.items():
            if condition_id not in active_condition_ids and state.market.end_time < now:
                to_remove.append(condition_id)
        for condition_id in to_remove:
            self._states.pop(condition_id, None)
            self._positions.pop(condition_id, None)
        return {
            "fetched": len(markets),
            "active": len(active_condition_ids),
            "new": new_count,
            "refreshed": refreshed_count,
            "expired_skipped": expired_skipped_count,
            "removed": len(to_remove),
            "subscribed_assets": len(set(token_ids)),
            "tracked_tokens": len(self._token_to_market),
        }

    async def _decision_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._decision_tick += 1
                stats = await self._evaluate_all_markets()
                if stats["intents_executed"] > 0 or self._decision_tick % 30 == 0:
                    logging.info(
                        (
                            "decision-loop tick=%s states=%s evaluated=%s skipped_expired=%s "
                            "skipped_no_spot=%s intents_created=%s intents_executed=%s"
                        ),
                        self._decision_tick,
                        stats["states"],
                        stats["evaluated"],
                        stats["skipped_expired"],
                        stats["skipped_no_spot"],
                        stats["intents_created"],
                        stats["intents_executed"],
                    )
            except Exception as exc:  # noqa: BLE001
                logging.exception("decision loop error: %s", exc)
            await asyncio.sleep(1)

    async def _evaluate_all_markets(self) -> dict[str, int]:
        now = datetime.now(tz=timezone.utc)
        states_count = len(self._states)
        evaluated_count = 0
        skipped_expired_count = 0
        skipped_no_spot_count = 0
        intents_created_count = 0
        intents_executed_count = 0
        for condition_id, state in list(self._states.items()):
            if state.market.end_time <= now:
                skipped_expired_count += 1
                continue
            spot = self._binance.get_mid(state.market.symbol)
            if spot is None:
                skipped_no_spot_count += 1
                continue
            evaluated_count += 1

            fallback_vol = self._settings.annual_vol_by_symbol.get(state.market.symbol, 0.8)
            annual_vol = self._binance.get_annualized_vol(state.market.symbol, fallback=fallback_vol)
            fair_yes = self._strategy.compute_fair_yes(state, spot=spot, annual_vol=annual_vol)
            state.fair_yes_prob = fair_yes
            state.fair_updated_at = now

            await self._db.record_quote(
                condition_id=condition_id,
                yes_bid=state.yes_book.best_bid,
                yes_ask=state.yes_book.best_ask,
                no_bid=state.no_book.best_bid,
                no_ask=state.no_book.best_ask,
                fair_yes_prob=fair_yes,
            )

            position = await self._get_position(condition_id)
            intents = self._strategy.generate_intents(state, position)
            intents_created_count += len(intents)
            if intents:
                # Prevent overtrading in a single decision pass.
                await self._execute_intent(intents[0], state.market)
                intents_executed_count += 1

            unrealized = mark_to_market_unrealized(state, position)
            await self._db.record_pnl(
                condition_id=condition_id,
                realized_pnl=position.realized_pnl,
                unrealized_pnl=unrealized,
                payload={
                    "symbol": state.market.symbol,
                    "window": state.market.window_minutes,
                    "fair_yes": fair_yes,
                    "spot": spot,
                },
            )
        return {
            "states": states_count,
            "evaluated": evaluated_count,
            "skipped_expired": skipped_expired_count,
            "skipped_no_spot": skipped_no_spot_count,
            "intents_created": intents_created_count,
            "intents_executed": intents_executed_count,
        }

    async def _execute_intent(self, intent: TradeIntent, market: MarketDefinition) -> None:
        await self._db.record_intent(intent, status="PENDING")
        order_id = await self._trader.place_limit_order(intent)
        if not order_id:
            await self._db.record_intent(intent, status="FAILED")
            return

        await self._db.record_intent(intent, status="POSTED", exchange_order_id=order_id)
        pos = await self._get_position(intent.condition_id)
        self._apply_local_fill(pos, intent, market)
        await self._db.upsert_position(intent.condition_id, pos)

    async def _get_position(self, condition_id: str) -> PositionState:
        if condition_id not in self._positions:
            self._positions[condition_id] = await self._db.get_position(condition_id)
        return self._positions[condition_id]

    @staticmethod
    def _apply_local_fill(position: PositionState, intent: TradeIntent, market: MarketDefinition) -> None:
        is_yes = intent.token_id == market.yes_token_id
        qty = intent.size
        px = intent.price
        side = intent.side.upper()

        if is_yes:
            if side == "BUY":
                new_qty = position.qty_yes + qty
                if new_qty > 0:
                    position.avg_yes_price = (
                        (position.avg_yes_price * position.qty_yes) + (px * qty)
                    ) / new_qty
                position.qty_yes = new_qty
            else:
                qty_sold = min(qty, position.qty_yes)
                position.realized_pnl += (px - position.avg_yes_price) * qty_sold
                position.qty_yes -= qty_sold
                if position.qty_yes <= 1e-9:
                    position.qty_yes = 0.0
                    position.avg_yes_price = 0.0
            return

        if side == "BUY":
            new_qty = position.qty_no + qty
            if new_qty > 0:
                position.avg_no_price = (
                    (position.avg_no_price * position.qty_no) + (px * qty)
                ) / new_qty
            position.qty_no = new_qty
        else:
            qty_sold = min(qty, position.qty_no)
            position.realized_pnl += (px - position.avg_no_price) * qty_sold
            position.qty_no -= qty_sold
            if position.qty_no <= 1e-9:
                position.qty_no = 0.0
                position.avg_no_price = 0.0

    async def _telegram_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._send_telegram_snapshot()
            except Exception as exc:  # noqa: BLE001
                logging.warning("telegram loop error: %s", exc)
            await asyncio.sleep(self._settings.telegram_report_interval_seconds)

    async def _send_telegram_snapshot(self) -> None:
        if not self._notifier.enabled:
            return
        if not self._states:
            return

        lines: list[str] = []
        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        for condition_id, state in self._states.items():
            pos = await self._get_position(condition_id)
            unrealized = mark_to_market_unrealized(state, pos)
            total_pnl = pos.realized_pnl + unrealized
            line = (
                f"{state.market.symbol}-{now}-{state.market.window_minutes}min "
                f"pnl=${total_pnl:.2f} "
                f"(r={pos.realized_pnl:.2f}, u={unrealized:.2f})"
            )
            lines.append(line)
        if lines:
            await self._notifier.send("\n".join(lines))
