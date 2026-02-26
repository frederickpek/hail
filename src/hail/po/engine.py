from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from hail.config import Settings
from hail.models import MarketDefinition, MarketState, OrderBookTop, TradeIntent
from hail.po.db import PoDatabase
from hail.po.models import MarketFillSummary, PoOrder
from hail.polymarket_gamma import GammaMarketScanner
from hail.polymarket_trader import PolymarketTrader
from hail.polymarket_ws import PolymarketBookFeed
from hail.telegram_notifier import TelegramNotifier


class PoEngine:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._db = PoDatabase(str(settings.db_path))
        self._scanner = GammaMarketScanner(settings)
        self._trader = PolymarketTrader(settings)
        self._notifier = TelegramNotifier(settings)

        self._states: dict[str, MarketState] = {}
        self._token_to_market: dict[str, tuple[str, str]] = {}
        self._entered_condition_ids: set[str] = set()

        self._stop_event = asyncio.Event()
        self._book_feed = PolymarketBookFeed(settings, self._on_book_update)
        self._daily_lock = asyncio.Lock()

    async def run(self) -> None:
        self._settings.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._settings.log_path.parent.mkdir(parents=True, exist_ok=True)
        await self._db.init()
        self._entered_condition_ids = await self._db.list_entered_condition_ids()
        await self._trader.connect()

        tasks = [
            asyncio.create_task(self._book_feed.start(), name="po-book-feed"),
            asyncio.create_task(self._scan_place_loop(), name="po-scan-place-loop"),
            asyncio.create_task(self._fill_reconcile_loop(), name="po-fill-reconcile-loop"),
            asyncio.create_task(self._resolution_loop(), name="po-resolution-loop"),
            asyncio.create_task(self._stats_loop(), name="po-stats-loop"),
            asyncio.create_task(self._daily_report_loop(), name="po-daily-report-loop"),
            asyncio.create_task(self._result_announce_loop(), name="po-result-announce-loop"),
        ]
        try:
            await self._stop_event.wait()
        finally:
            self._book_feed.stop()
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

    async def _scan_place_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                markets = await self._scanner.scan()
                await self._register_markets(markets)
                await self._place_for_new_markets()
            except Exception as exc:  # noqa: BLE001
                logging.warning("po scan/place loop error: %s", exc)
            await asyncio.sleep(self._settings.po_scan_interval_seconds)

    async def _register_markets(self, markets: list[MarketDefinition]) -> None:
        now = datetime.now(tz=timezone.utc)
        token_ids: list[str] = []
        for market in markets:
            if market.end_time <= now:
                continue
            if market.condition_id not in self._states:
                self._states[market.condition_id] = MarketState(market=market)
            else:
                self._states[market.condition_id].market = market
            self._token_to_market[market.yes_token_id] = (market.condition_id, "YES")
            self._token_to_market[market.no_token_id] = (market.condition_id, "NO")
            token_ids.extend([market.yes_token_id, market.no_token_id])
        if token_ids:
            await self._book_feed.add_assets(token_ids)

    async def _place_for_new_markets(self) -> None:
        now = datetime.now(tz=timezone.utc)
        for condition_id, state in list(self._states.items()):
            if condition_id in self._entered_condition_ids:
                continue
            if state.market.end_time <= now:
                continue

            yes_bid = self._normalize_bid(state.yes_book.best_bid)
            no_bid = self._normalize_bid(state.no_book.best_bid)
            if yes_bid is None or no_bid is None:
                continue

            yes_price = self._clip_price(yes_bid - self._settings.po_price_tick)
            no_price = self._clip_price(no_bid - self._settings.po_price_tick)
            if yes_price is None or no_price is None:
                continue

            inserted = await self._db.register_market_once(
                state.market,
                yes_target_price=yes_price,
                no_target_price=no_price,
            )
            if not inserted:
                self._entered_condition_ids.add(condition_id)
                continue

            self._entered_condition_ids.add(condition_id)
            await self._place_market_pair(state.market, yes_price, no_price)

    async def _place_market_pair(self, market: MarketDefinition, yes_price: float, no_price: float) -> None:
        size = max(self._settings.po_order_size, 0.0)
        yes_order = PoOrder(
            condition_id=market.condition_id,
            token_id=market.yes_token_id,
            outcome="YES",
            side="BUY",
            price=yes_price,
            size=size,
            reason="po_yes_bid_minus_tick",
        )
        no_order = PoOrder(
            condition_id=market.condition_id,
            token_id=market.no_token_id,
            outcome="NO",
            side="BUY",
            price=no_price,
            size=size,
            reason="po_no_bid_minus_tick",
        )
        await self._submit_order(yes_order, market)
        await self._submit_order(no_order, market)
        logging.info(
            "Entered market once: %s %sm condition=%s yes_bid=%.4f no_bid=%.4f size=%.4f dry_run=%s",
            market.symbol,
            market.window_minutes,
            market.condition_id,
            yes_price,
            no_price,
            size,
            self._settings.po_dry_run,
        )

    async def _submit_order(self, order: PoOrder, market: MarketDefinition) -> None:
        row_id = await self._db.record_order_created(order)
        if self._settings.po_dry_run:
            await self._db.mark_order_status(row_id, "DRY_RUN")
            return
        intent = TradeIntent(
            condition_id=order.condition_id,
            token_id=order.token_id,
            side=order.side,
            price=order.price,
            size=order.size,
            reason=order.reason,
        )
        exchange_order_id = await self._trader.place_limit_order(intent, market)
        if not exchange_order_id:
            await self._db.mark_order_status(row_id, "FAILED")
            return
        await self._db.mark_order_posted(row_id, exchange_order_id)

    async def _fill_reconcile_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                orders = await self._db.list_orders_for_reconcile()
                for order in orders:
                    snapshot = self._trader.get_order_fill_snapshot(order["exchange_order_id"])
                    if snapshot is None:
                        continue
                    filled_size, status = snapshot
                    await self._db.upsert_order_snapshot(
                        exchange_order_id=order["exchange_order_id"],
                        new_status=status or "OPEN",
                        new_filled_size=max(filled_size, 0.0),
                    )
            except Exception as exc:  # noqa: BLE001
                logging.warning("po fill reconcile error: %s", exc)
            await asyncio.sleep(self._settings.po_fill_poll_interval_seconds)

    async def _resolution_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                now_iso = datetime.now(tz=timezone.utc).isoformat()
                pending = await self._db.list_markets_pending_resolution(now_iso)
                for row in pending:
                    condition_id = row["condition_id"]
                    resolution = await self._resolve_market(condition_id)
                    if resolution is None:
                        continue
                    fill_summary = await self._db.get_market_fill_summary(condition_id)
                    cost, payout, pnl = self._compute_pnl(fill_summary, resolution["resolved_side"])
                    await self._db.finalize_market_result(
                        condition_id=condition_id,
                        resolved_side=resolution["resolved_side"],
                        resolution_source=resolution["source"],
                        cost=cost,
                        payout=payout,
                        pnl=pnl,
                        fill_summary=fill_summary,
                    )
            except Exception as exc:  # noqa: BLE001
                logging.warning("po resolution loop error: %s", exc)
            await asyncio.sleep(self._settings.po_resolution_poll_interval_seconds)

    async def _result_announce_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if self._notifier.enabled:
                    rows = await self._db.list_unannounced_results(limit=50)
                    for row in rows:
                        message = (
                            f"{row['symbol']} {row['window_minutes']}m resolved={row['resolved_side']} "
                            f"yes_fill={row['yes_filled_qty']:.4f}@{row['yes_avg_fill_price']:.4f} "
                            f"no_fill={row['no_filled_qty']:.4f}@{row['no_avg_fill_price']:.4f} "
                            f"pnl=${row['pnl']:.4f}"
                        )
                        await self._notifier.send(message)
                        await self._db.mark_result_announced(row["condition_id"])
            except Exception as exc:  # noqa: BLE001
                logging.warning("po result announce loop error: %s", exc)
            await asyncio.sleep(30)

    async def _stats_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self._log_stats_snapshot()
            except Exception as exc:  # noqa: BLE001
                logging.warning("po stats loop error: %s", exc)
            await asyncio.sleep(self._settings.po_stats_interval_seconds)

    async def _daily_report_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                async with self._daily_lock:
                    await self._send_daily_report()
            except Exception as exc:  # noqa: BLE001
                logging.warning("po daily report loop error: %s", exc)
            await asyncio.sleep(self._settings.po_daily_report_interval_seconds)

    async def _send_daily_report(self) -> None:
        stats = await self._db.get_stats()
        if not stats:
            return
        previous = await self._db.get_previous_daily_snapshot()

        def delta(key: str) -> float:
            if previous is None:
                return float(stats.get(key, 0))
            return float(stats.get(key, 0)) - float(previous.get(key, 0))

        window_combined_markets = int(delta("combined_markets_total"))
        window_combined_wins = int(delta("combined_wins_total"))
        window_both_markets = int(delta("both_filled_markets_total"))
        window_both_wins = int(delta("both_filled_wins_total"))
        window_one_markets = int(delta("one_filled_markets_total"))
        window_one_wins = int(delta("one_filled_wins_total"))
        window_pnl = delta("lifetime_pnl_total")

        combined_winrate = self._pct(window_combined_wins, window_combined_markets)
        both_winrate = self._pct(window_both_wins, window_both_markets)
        one_winrate = self._pct(window_one_wins, window_one_markets)

        if self._notifier.enabled:
            message = "\n".join(
                [
                    "PO 24h window report",
                    f"pnl=${window_pnl:.4f}",
                    f"winrate_both={both_winrate:.2f}%",
                    f"winrate_one={one_winrate:.2f}%",
                    f"winrate_combined={combined_winrate:.2f}%",
                    f"markets_entered={int(delta('markets_entered_total'))}",
                    f"orders_placed={int(delta('orders_placed_total'))}",
                    f"orders_filled={int(delta('orders_filled_total'))}",
                ]
            )
            await self._notifier.send(message)
        await self._db.create_daily_snapshot(stats)

    async def _log_stats_snapshot(self) -> None:
        stats = await self._db.get_stats()
        if not stats:
            return
        open_markets = await self._db.get_open_market_count()
        both_winrate = self._pct(int(stats["both_filled_wins_total"]), int(stats["both_filled_markets_total"]))
        one_winrate = self._pct(int(stats["one_filled_wins_total"]), int(stats["one_filled_markets_total"]))
        combined_winrate = self._pct(int(stats["combined_wins_total"]), int(stats["combined_markets_total"]))
        logging.info(
            (
                "PO stats | entered=%s open=%s placed=%s filled_orders=%s finalized=%s "
                "pnl=%.4f winrate_both=%.2f%% winrate_one=%.2f%% winrate_combined=%.2f%%"
            ),
            int(stats["markets_entered_total"]),
            open_markets,
            int(stats["orders_placed_total"]),
            int(stats["orders_filled_total"]),
            int(stats["markets_finalized_total"]),
            float(stats["lifetime_pnl_total"]),
            both_winrate,
            one_winrate,
            combined_winrate,
        )

    async def _resolve_market(self, condition_id: str) -> dict[str, str] | None:
        resolved = await self._resolve_from_gamma(condition_id)
        if resolved is not None:
            return resolved
        return None

    async def _resolve_from_gamma(self, condition_id: str) -> dict[str, str] | None:
        timeout = httpx.Timeout(self._settings.request_timeout_seconds)
        urls = [
            f"{self._settings.gamma_api_url}/markets",
            f"{self._settings.gamma_api_url}/events",
        ]
        for url in urls:
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.get(url, params={"condition_ids": condition_id, "limit": "5"})
                    if response.status_code >= 400:
                        continue
                    payload = response.json()
                    resolved = self._extract_resolution_from_payload(payload, condition_id)
                    if resolved is not None:
                        return {"resolved_side": resolved, "source": "gamma_api"}
            except Exception:
                continue
        return None

    def _extract_resolution_from_payload(self, payload: Any, condition_id: str) -> str | None:
        rows: list[dict[str, Any]] = []
        if isinstance(payload, list):
            rows = [item for item in payload if isinstance(item, dict)]
        elif isinstance(payload, dict):
            markets = payload.get("markets")
            if isinstance(markets, list):
                rows = [item for item in markets if isinstance(item, dict)]
            else:
                rows = [payload]

        for row in rows:
            cid = str(row.get("conditionId") or row.get("condition_id") or "")
            if cid and cid != condition_id:
                continue
            for key in ("resolvedOutcome", "resolved_outcome", "winner", "winningOutcome", "result"):
                value = row.get(key)
                parsed = self._normalize_outcome(value)
                if parsed is not None:
                    return parsed
            outcomes = row.get("outcomes")
            payouts = row.get("payouts")
            if isinstance(outcomes, list) and isinstance(payouts, list) and len(outcomes) == len(payouts):
                for outcome, payout in zip(outcomes, payouts):
                    try:
                        payout_float = float(payout)
                    except (TypeError, ValueError):
                        continue
                    if payout_float > 0.5:
                        parsed = self._normalize_outcome(outcome)
                        if parsed is not None:
                            return parsed
        return None

    @staticmethod
    def _normalize_outcome(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip().upper()
        if text in {"YES", "UP", "LONG", "TRUE", "1"}:
            return "YES"
        if text in {"NO", "DOWN", "SHORT", "FALSE", "0"}:
            return "NO"
        return None

    @staticmethod
    def _compute_pnl(fill_summary: MarketFillSummary, resolved_side: str) -> tuple[float, float, float]:
        cost = (fill_summary.yes_filled_qty * fill_summary.yes_avg_price) + (
            fill_summary.no_filled_qty * fill_summary.no_avg_price
        )
        if resolved_side.upper() == "YES":
            payout = fill_summary.yes_filled_qty
        else:
            payout = fill_summary.no_filled_qty
        pnl = payout - cost
        return cost, payout, pnl

    @staticmethod
    def _normalize_bid(value: float | None) -> float | None:
        if value is None:
            return None
        if value > 1.0:
            return value / 100.0
        return value

    @staticmethod
    def _clip_price(value: float) -> float | None:
        clipped = round(value, 4)
        if clipped <= 0:
            return None
        if clipped >= 1:
            return None
        return clipped

    @staticmethod
    def _pct(wins: int, total: int) -> float:
        if total <= 0:
            return 0.0
        return (wins / total) * 100.0

