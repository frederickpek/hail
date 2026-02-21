from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from hail.binance_feed import BinanceMidPriceFeed
from hail.config import Settings
from hail.db import BotDatabase
from hail.models import MarketDefinition, MarketState, OrderBookTop, PositionState, TradeIntent
from hail.polymarket_gamma import GammaMarketScanner
from hail.polymarket_trader import PolymarketTrader
from hail.polymarket_ws import PolymarketBookFeed
from hail.strategy import Strategy, mark_to_market_unrealized
from hail.telegram_notifier import TelegramNotifier

ACCOUNT_SNAPSHOT_INTERVAL_SECONDS = 60
POSITION_RECONCILE_INTERVAL_SECONDS = 45


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
        self._recently_ended: dict[str, tuple[MarketDefinition, PositionState, float]] = {}
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
            asyncio.create_task(self._account_snapshot_loop(), name="account-snapshot-loop"),
            asyncio.create_task(self._position_reconcile_loop(), name="position-reconcile-loop"),
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
                # logging.info(
                #     (
                #         "scan-loop tick=%s fetched=%s active=%s new=%s refreshed=%s "
                #         "expired_skipped=%s removed=%s removed_tokens=%s "
                #         "subscribed_assets=%s tracked_tokens=%s"
                #     ),
                #     self._scan_tick,
                #     stats["fetched"],
                #     stats["active"],
                #     stats["new"],
                #     stats["refreshed"],
                #     stats["expired_skipped"],
                #     stats["removed"],
                #     stats["removed_tokens"],
                #     stats["subscribed_assets"],
                #     stats["tracked_tokens"],
                # )
            except Exception as exc:  # noqa: BLE001
                logging.warning("scan loop error: %s", exc)
            await asyncio.sleep(self._settings.scan_interval_seconds)

    async def _register_markets(self, markets: list[MarketDefinition]) -> dict[str, int]:
        token_ids: list[str] = []
        removed_token_ids: list[str] = []
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
                    "Collected new target market: %s %sm (%s)",
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
            state = self._states.get(condition_id)
            if state is not None:
                pos = await self._get_position(condition_id)
                # Snapshot recently ended markets so Telegram can report shortly after expiry.
                ended_position = PositionState(
                    qty_yes=pos.qty_yes,
                    avg_yes_price=pos.avg_yes_price,
                    qty_no=pos.qty_no,
                    avg_no_price=pos.avg_no_price,
                    realized_pnl=pos.realized_pnl,
                )
                ended_unrealized = mark_to_market_unrealized(state, ended_position)
                self._recently_ended[condition_id] = (
                    state.market,
                    ended_position,
                    ended_unrealized,
                )
            self._states.pop(condition_id, None)
            self._positions.pop(condition_id, None)

        if to_remove:
            # Untrack websocket assets for ended markets and shrink token->market map.
            for token_id, (condition_id, _) in list(self._token_to_market.items()):
                if condition_id in to_remove:
                    removed_token_ids.append(token_id)
                    self._token_to_market.pop(token_id, None)
            if removed_token_ids:
                await self._book_feed.remove_assets(removed_token_ids)
        return {
            "fetched": len(markets),
            "active": len(active_condition_ids),
            "new": new_count,
            "refreshed": refreshed_count,
            "expired_skipped": expired_skipped_count,
            "removed": len(to_remove),
            "removed_tokens": len(removed_token_ids),
            "subscribed_assets": len(set(token_ids)),
            "tracked_tokens": len(self._token_to_market),
        }

    async def _decision_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._decision_tick += 1
                stats = await self._evaluate_all_markets()
                if self._decision_tick % 30 == 0:
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

    async def _account_snapshot_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                collateral = self._trader.get_collateral_balance_usdc()
                open_value, unrealized_pnl, open_markets = self._compute_open_position_metrics()
                if collateral is None:
                    logging.info(
                        (
                            "Portfolio snapshot: collateral=unavailable "
                            "open_position_value=%.6f USDC unrealized_pnl=%.6f open_markets=%s"
                        ),
                        open_value,
                        unrealized_pnl,
                        open_markets,
                    )
                else:
                    total_estimated_value = collateral + open_value
                    logging.info(
                        (
                            "Polymarket collateral balance=%.6f USDC | "
                            "open_position_value=%.6f USDC unrealized_pnl=%.6f "
                            "open_markets=%s total_estimated_value=%.6f USDC"
                        ),
                        collateral,
                        open_value,
                        unrealized_pnl,
                        open_markets,
                        total_estimated_value,
                    )
            except Exception as exc:  # noqa: BLE001
                logging.warning("account snapshot loop error: %s", exc)
            await asyncio.sleep(ACCOUNT_SNAPSHOT_INTERVAL_SECONDS)

    async def _position_reconcile_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                updates = 0
                for condition_id, state in list(self._states.items()):
                    pos = await self._get_position(condition_id)
                    yes_bal = self._trader.get_conditional_balance(state.market.yes_token_id)
                    no_bal = self._trader.get_conditional_balance(state.market.no_token_id)
                    if yes_bal is None and no_bal is None:
                        continue

                    changed = False
                    if yes_bal is not None and abs(pos.qty_yes - yes_bal) > 1e-6:
                        logging.info(
                            "Position reconcile YES: %s %sm local=%.6f chain=%.6f",
                            state.market.symbol,
                            state.market.window_minutes,
                            pos.qty_yes,
                            yes_bal,
                        )
                        pos.qty_yes = max(yes_bal, 0.0)
                        if pos.qty_yes <= 1e-9:
                            pos.qty_yes = 0.0
                            pos.avg_yes_price = 0.0
                        changed = True

                    if no_bal is not None and abs(pos.qty_no - no_bal) > 1e-6:
                        logging.info(
                            "Position reconcile NO: %s %sm local=%.6f chain=%.6f",
                            state.market.symbol,
                            state.market.window_minutes,
                            pos.qty_no,
                            no_bal,
                        )
                        pos.qty_no = max(no_bal, 0.0)
                        if pos.qty_no <= 1e-9:
                            pos.qty_no = 0.0
                            pos.avg_no_price = 0.0
                        changed = True

                    if changed:
                        await self._db.upsert_position(condition_id, pos)
                        updates += 1

                self._log_positions_snapshot()
                if updates > 0:
                    logging.info("Position reconcile updated markets=%s", updates)
            except Exception as exc:  # noqa: BLE001
                logging.warning("position reconcile loop error: %s", exc)
            await asyncio.sleep(POSITION_RECONCILE_INTERVAL_SECONDS)

    def _compute_open_position_metrics(self) -> tuple[float, float, int]:
        open_value = 0.0
        unrealized_pnl = 0.0
        open_markets = 0
        for condition_id, position in self._positions.items():
            qty_yes = max(position.qty_yes, 0.0)
            qty_no = max(position.qty_no, 0.0)
            if qty_yes <= 1e-9 and qty_no <= 1e-9:
                continue
            open_markets += 1
            state = self._states.get(condition_id)
            yes_mark = position.avg_yes_price
            no_mark = position.avg_no_price
            if state is not None:
                if state.yes_book.best_bid is not None:
                    yes_mark = state.yes_book.best_bid
                if state.no_book.best_bid is not None:
                    no_mark = state.no_book.best_bid
            open_value += (qty_yes * yes_mark) + (qty_no * no_mark)
            unrealized_pnl += (yes_mark - position.avg_yes_price) * qty_yes
            unrealized_pnl += (no_mark - position.avg_no_price) * qty_no
        return open_value, unrealized_pnl, open_markets

    def _log_positions_snapshot(self) -> None:
        lines: list[str] = []
        for condition_id, position in self._positions.items():
            if position.qty_yes <= 1e-9 and position.qty_no <= 1e-9:
                continue
            state = self._states.get(condition_id)
            if state is None:
                continue
            lines.append(
                (
                    f"{state.market.symbol} {state.market.window_minutes}m "
                    f"YES={position.qty_yes:.4f} NO={position.qty_no:.4f} "
                    f"avg_yes={position.avg_yes_price:.4f} avg_no={position.avg_no_price:.4f}"
                )
            )
        if not lines:
            logging.info("Current positions: none")
            return
        logging.info("Current positions: %s", " | ".join(lines))

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
            if state.market.strike <= 0:
                market_start = state.market.end_time - timedelta(minutes=state.market.window_minutes)
                resolved_strike, source = await self._binance.resolve_strike_from_start(
                    state.market.symbol,
                    market_start,
                )
                if resolved_strike is not None and resolved_strike > 0:
                    state.market.strike = resolved_strike
                    await self._db.upsert_market(state.market)
                    logging.info(
                        (
                            "Resolved missing strike from start-time reference: "
                            "symbol=%s window=%sm start=%s strike=%.4f source=%s"
                        ),
                        state.market.symbol,
                        state.market.window_minutes,
                        market_start.isoformat(),
                        resolved_strike,
                        source,
                    )
                else:
                    logging.warning(
                        (
                            "Skipping market due to missing strike: symbol=%s window=%sm slug=%s "
                            "start=%s (reference price unresolved from Binance)"
                        ),
                        state.market.symbol,
                        state.market.window_minutes,
                        state.market.slug,
                        market_start.isoformat(),
                    )
                    continue
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
            elif self._decision_tick % 30 == 0:
                self._log_no_intent_diagnostics(state, position)

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

    def _log_no_intent_diagnostics(self, state: MarketState, position: PositionState) -> None:
        fair_yes = state.fair_yes_prob
        if fair_yes is None:
            return

        fair_no = 1.0 - fair_yes
        yes_bid = state.yes_book.best_bid
        yes_ask = state.yes_book.best_ask
        no_bid = state.no_book.best_bid
        no_ask = state.no_book.best_ask

        entry_yes_edge = (fair_yes - yes_ask) if yes_ask is not None else None
        entry_no_edge = (fair_no - no_ask) if no_ask is not None else None
        exit_yes_gain = (yes_bid - position.avg_yes_price) if yes_bid is not None and position.qty_yes > 0 else None
        exit_no_gain = (no_bid - position.avg_no_price) if no_bid is not None and position.qty_no > 0 else None

        blocked_reason = self._classify_no_intent_reason(
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            no_bid=no_bid,
            no_ask=no_ask,
            entry_yes_edge=entry_yes_edge,
            entry_no_edge=entry_no_edge,
            exit_yes_gain=exit_yes_gain,
            exit_no_gain=exit_no_gain,
            position=position,
        )

        # logging.info(
        #     (
        #         "no-intent %s %sm end=%s blocked_reason=%s fair_yes=%.4f fair_no=%.4f "
        #         "book[y_bid=%s y_ask=%s n_bid=%s n_ask=%s] "
        #         "pos[y_qty=%.4f y_avg=%.4f n_qty=%.4f n_avg=%.4f] "
        #         "edges[entry_yes=%s entry_no=%s exit_yes=%s exit_no=%s] "
        #         "thresholds[min_entry=%.4f min_exit=%.4f max_pos=%.4f]"
        #     ),
        #     state.market.symbol,
        #     state.market.window_minutes,
        #     state.market.end_time.isoformat(),
        #     blocked_reason,
        #     fair_yes,
        #     fair_no,
        #     f"{yes_bid:.4f}" if yes_bid is not None else "None",
        #     f"{yes_ask:.4f}" if yes_ask is not None else "None",
        #     f"{no_bid:.4f}" if no_bid is not None else "None",
        #     f"{no_ask:.4f}" if no_ask is not None else "None",
        #     position.qty_yes,
        #     position.avg_yes_price,
        #     position.qty_no,
        #     position.avg_no_price,
        #     f"{entry_yes_edge:.4f}" if entry_yes_edge is not None else "None",
        #     f"{entry_no_edge:.4f}" if entry_no_edge is not None else "None",
        #     f"{exit_yes_gain:.4f}" if exit_yes_gain is not None else "None",
        #     f"{exit_no_gain:.4f}" if exit_no_gain is not None else "None",
        #     self._settings.min_entry_edge,
        #     self._settings.min_exit_edge,
        #     self._settings.max_position_per_market,
        # )

    def _classify_no_intent_reason(
        self,
        *,
        yes_bid: float | None,
        yes_ask: float | None,
        no_bid: float | None,
        no_ask: float | None,
        entry_yes_edge: float | None,
        entry_no_edge: float | None,
        exit_yes_gain: float | None,
        exit_no_gain: float | None,
        position: PositionState,
    ) -> str:
        if yes_ask is None and no_ask is None:
            return "missing_entry_asks"
        if yes_bid is None and no_bid is None:
            return "missing_exit_bids"

        can_enter_yes = (
            entry_yes_edge is not None
            and entry_yes_edge >= self._settings.min_entry_edge
            and position.qty_yes < self._settings.max_position_per_market
        )
        can_enter_no = (
            entry_no_edge is not None
            and entry_no_edge >= self._settings.min_entry_edge
            and position.qty_no < self._settings.max_position_per_market
        )
        can_exit_yes = (
            position.qty_yes > 0
            and exit_yes_gain is not None
            and exit_yes_gain >= self._settings.min_exit_edge
        )
        can_exit_no = (
            position.qty_no > 0
            and exit_no_gain is not None
            and exit_no_gain >= self._settings.min_exit_edge
        )
        if can_enter_yes or can_enter_no or can_exit_yes or can_exit_no:
            # Should be rare because intents would usually be generated.
            return "intent_expected_but_not_generated"

        if position.qty_yes >= self._settings.max_position_per_market and position.qty_no >= self._settings.max_position_per_market:
            return "max_position_reached_both_sides"
        if position.qty_yes <= 0 and position.qty_no <= 0:
            return "entry_edge_below_threshold"
        if position.qty_yes > 0 and exit_yes_gain is not None and exit_yes_gain < self._settings.min_exit_edge:
            return "holding_yes_waiting_exit_edge"
        if position.qty_no > 0 and exit_no_gain is not None and exit_no_gain < self._settings.min_exit_edge:
            return "holding_no_waiting_exit_edge"
        return "edge_below_threshold_or_no_book"

    async def _execute_intent(self, intent: TradeIntent, market: MarketDefinition) -> None:
        state = self._states.get(intent.condition_id)
        position = await self._get_position(intent.condition_id)
        if state is not None:
            micro = self._binance.state.get(market.symbol)
            micro_note = ""
            if micro is not None:
                micro_note = (
                    f" Microstructure: imbalance5={micro.imbalance_5:+.3f}, "
                    f"imbalance20={micro.imbalance_20:+.3f}, "
                    f"top_qty(bid/ask)={micro.best_bid_depth_qty:.3f}/{micro.best_ask_depth_qty:.3f}."
                )
            logging.info(
                (
                    "Order reasoning: market=%s %sm side=%s outcome=%s reason=%s "
                    "fair_yes=%s yes_bid=%s yes_ask=%s no_bid=%s no_ask=%s "
                    "pos_yes=%.4f@%.4f pos_no=%.4f@%.4f%s"
                ),
                market.symbol,
                market.window_minutes,
                intent.side,
                "YES" if intent.token_id == market.yes_token_id else "NO",
                intent.reason,
                (
                    f"{state.fair_yes_prob:.4f}"
                    if state.fair_yes_prob is not None
                    else "None"
                ),
                (
                    f"{state.yes_book.best_bid:.4f}"
                    if state.yes_book.best_bid is not None
                    else "None"
                ),
                (
                    f"{state.yes_book.best_ask:.4f}"
                    if state.yes_book.best_ask is not None
                    else "None"
                ),
                (
                    f"{state.no_book.best_bid:.4f}"
                    if state.no_book.best_bid is not None
                    else "None"
                ),
                (
                    f"{state.no_book.best_ask:.4f}"
                    if state.no_book.best_ask is not None
                    else "None"
                ),
                position.qty_yes,
                position.avg_yes_price,
                position.qty_no,
                position.avg_no_price,
                micro_note,
            )
        else:
            logging.info(
                "Order reasoning: side=%s outcome=%s reason=%s (market state unavailable)",
                intent.side,
                "YES" if intent.token_id == market.yes_token_id else "NO",
                intent.reason,
            )

        await self._db.record_intent(intent, status="PENDING")
        can_attempt, block_reason = self._trader.can_attempt_order(intent.side, intent.token_id)
        if not can_attempt:
            logging.warning(
                (
                    "Skipping order attempt due to proactive balance guard: "
                    "side=%s outcome=%s token=%s reason=%s"
                ),
                intent.side,
                "YES" if intent.token_id == market.yes_token_id else "NO",
                intent.token_id,
                block_reason,
            )
            await self._db.record_intent(
                intent,
                status="SKIPPED_BALANCE_GUARD",
            )
            return

        if intent.side.upper() == "SELL":
            available_qty = self._local_available_qty_for_token(position, intent.token_id, market)
            if intent.size > (available_qty + 1e-9):
                logging.warning(
                    (
                        "Skipping SELL due to insufficient local token balance: "
                        "side=%s outcome=%s requested=%.6f available=%.6f reason=%s"
                    ),
                    intent.side,
                    "YES" if intent.token_id == market.yes_token_id else "NO",
                    intent.size,
                    available_qty,
                    intent.reason,
                )
                await self._db.record_intent(
                    intent,
                    status="SKIPPED_INSUFFICIENT_LOCAL_BALANCE",
                )
                return

        order_id = await self._trader.place_limit_order(intent, market)
        if not order_id:
            await self._db.record_intent(intent, status="FAILED")
            return

        await self._db.record_intent(intent, status="POSTED", exchange_order_id=order_id)
        self._apply_local_fill(position, intent, market)
        await self._db.upsert_position(intent.condition_id, position)
        await self._db.record_intent(intent, status="FILLED_LOCAL", exchange_order_id=order_id)

    async def _get_position(self, condition_id: str) -> PositionState:
        if condition_id not in self._positions:
            self._positions[condition_id] = await self._db.get_position(condition_id)
        return self._positions[condition_id]

    @staticmethod
    def _apply_local_fill(
        position: PositionState,
        intent: TradeIntent,
        market: MarketDefinition,
    ) -> None:
        is_yes = intent.token_id == market.yes_token_id
        qty = max(intent.size, 0.0)
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

    @staticmethod
    def _local_available_qty_for_token(
        position: PositionState,
        token_id: str,
        market: MarketDefinition,
    ) -> float:
        if token_id == market.yes_token_id:
            return max(position.qty_yes, 0.0)
        if token_id == market.no_token_id:
            return max(position.qty_no, 0.0)
        return 0.0

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

        lines: list[str] = []
        announced_ids: list[str] = []
        sg_tz = ZoneInfo("Asia/Singapore")
        now = datetime.now(tz=timezone.utc)
        recent_cutoff = now - timedelta(minutes=30)

        # Drop old ended entries so this cache does not grow unbounded.
        stale_recent = [
            condition_id
        for condition_id, (market, _, _) in self._recently_ended.items()
            if market.end_time < recent_cutoff
        ]
        for condition_id in stale_recent:
            self._recently_ended.pop(condition_id, None)

        # Send only recently ended markets, then remove them so each is announced once.
        for condition_id, (market, pos, ended_unrealized) in self._recently_ended.items():
            if market.end_time > now or market.end_time < recent_cutoff:
                continue
            end_time_sg = market.end_time.astimezone(sg_tz)
            end_time_display = (
                f"{end_time_sg.day} {end_time_sg.strftime('%b')} "
                f"{end_time_sg.strftime('%-I:%M%p').lower()}"
            )
            total_pnl = pos.realized_pnl + ended_unrealized
            lines.append(f"{market.symbol} {market.window_minutes}m, {end_time_display}, pnl=${total_pnl:.2f}")
            announced_ids.append(condition_id)
        if lines:
            await self._notifier.send("\n".join(lines))
            for condition_id in announced_ids:
                self._recently_ended.pop(condition_id, None)
