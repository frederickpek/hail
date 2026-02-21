from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone
from json import JSONDecodeError

import websockets

from hail.config import Settings
from hail.models import OrderBookTop

BookCallback = Callable[[str, OrderBookTop], None]


class PolymarketBookFeed:
    def __init__(self, settings: Settings, on_book_update: BookCallback) -> None:
        self._settings = settings
        self._on_book_update = on_book_update
        self._tracked_assets: set[str] = set()
        self._subscribed_assets: set[str] = set()
        self._initial_subscription_sent = False
        self._tracked_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._ws: websockets.ClientConnection | None = None
        self._last_message_at = datetime.now(tz=timezone.utc)

    async def add_assets(self, token_ids: list[str]) -> None:
        added_count = 0
        async with self._tracked_lock:
            for token in token_ids:
                if token in self._tracked_assets:
                    continue
                self._tracked_assets.add(token)
                added_count += 1
        if added_count == 0:
            return
        logging.info("Added %s new token ids (tracked=%s)", added_count, len(self._tracked_assets))
        # If websocket is already connected, refresh subscription immediately.
        if self._ws is not None:
            try:
                await self._send_subscription(self._ws)
            except Exception as exc:  # noqa: BLE001
                logging.warning("Failed to refresh websocket subscription: %s", exc)

    async def remove_assets(self, token_ids: list[str]) -> None:
        removed_count = 0
        async with self._tracked_lock:
            for token in token_ids:
                if token in self._tracked_assets:
                    self._tracked_assets.remove(token)
                    removed_count += 1
        if removed_count == 0:
            return
        logging.info("Removed %s token ids (tracked=%s)", removed_count, len(self._tracked_assets))
        if self._ws is not None:
            try:
                await self._send_subscription(self._ws)
            except Exception as exc:  # noqa: BLE001
                logging.warning("Failed to refresh websocket subscription after removal: %s", exc)

    def stop(self) -> None:
        self._stop_event.set()

    async def start(self) -> None:
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(self._settings.poly_ws_url, ping_interval=20, ping_timeout=20) as ws:
                    self._ws = ws
                    self._subscribed_assets = set()
                    self._initial_subscription_sent = False
                    self._last_message_at = datetime.now(tz=timezone.utc)
                    logging.info("Connected to Polymarket market websocket")
                    await self._send_subscription(ws)
                    heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))

                    try:
                        while not self._stop_event.is_set():
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=20)
                            except asyncio.TimeoutError:
                                idle_seconds = int(
                                    (datetime.now(tz=timezone.utc) - self._last_message_at).total_seconds()
                                )
                                if idle_seconds >= 60:
                                    logging.warning("Polymarket websocket idle for %ss; syncing subscription.", idle_seconds)
                                    await self._send_subscription(ws)
                                continue
                            await self._process_message(raw, ws)
                    finally:
                        heartbeat_task.cancel()
                        await asyncio.gather(heartbeat_task, return_exceptions=True)
            except Exception as exc:  # noqa: BLE001
                logging.exception("Polymarket websocket disconnected")
                await asyncio.sleep(2)
            finally:
                self._ws = None

    async def _send_subscription(self, ws: websockets.ClientConnection) -> None:
        async with self._tracked_lock:
            tracked = set(self._tracked_assets)

        if not self._initial_subscription_sent:
            payload = {
                "type": "market",
                "assets_ids": sorted(tracked),
                "custom_feature_enabled": True,
            }
            await ws.send(json.dumps(payload))
            self._subscribed_assets = tracked
            self._initial_subscription_sent = True
            logging.info("Subscribed to %s token books", len(tracked))
            return

        to_subscribe = sorted(tracked - self._subscribed_assets)
        to_unsubscribe = sorted(self._subscribed_assets - tracked)
        if not to_subscribe and not to_unsubscribe:
            return

        if to_subscribe:
            payload = {
                "assets_ids": to_subscribe,
                "operation": "subscribe",
                "custom_feature_enabled": True,
            }
            await ws.send(json.dumps(payload))
            logging.info("Subscribed to %s token books", len(to_subscribe))
        if to_unsubscribe:
            payload = {
                "assets_ids": to_unsubscribe,
                "operation": "unsubscribe",
            }
            await ws.send(json.dumps(payload))
            logging.info("Unsubscribed from %s token books", len(to_unsubscribe))

        self._subscribed_assets = tracked

    async def _heartbeat_loop(self, ws: websockets.ClientConnection) -> None:
        while not self._stop_event.is_set() and ws is self._ws:
            await asyncio.sleep(10)
            try:
                await ws.send("PING")
            except Exception as exc:  # noqa: BLE001
                logging.debug("Failed to send websocket heartbeat: %s", exc)
                return

    async def _process_message(self, raw: str, ws: websockets.ClientConnection) -> None:
        if not raw:
            return
        text = raw.strip()
        if not text:
            return
        if text in {"PONG", "PING"}:
            self._last_message_at = datetime.now(tz=timezone.utc)
            return
        try:
            payload = json.loads(text)
        except JSONDecodeError:
            # Ignore heartbeat / non-JSON frames without tearing down the connection.
            logging.debug("Ignoring non-JSON websocket frame: %r", text[:120])
            return
        events = self._flatten_events(payload)
        if not events:
            return
        self._last_message_at = datetime.now(tz=timezone.utc)
        for item in events:
            if not isinstance(item, dict):
                continue
            self._apply_market_event(item)
            event_type = str(item.get("event_type") or item.get("type") or "").lower()
            # In case assets were added after connection established.
            if event_type == "subscribed":
                await self._send_subscription(ws)

    def _flatten_events(self, payload: object) -> list[dict]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            # Some ws payloads wrap events in message/data arrays.
            nested = payload.get("message")
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
            if isinstance(nested, dict):
                return [nested]
            nested = payload.get("data")
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
            if isinstance(nested, dict):
                return [nested]
            return [payload]
        return []

    def _apply_market_event(self, event: dict) -> None:
        event_type = str(event.get("event_type") or event.get("type") or "").lower()
        if event_type == "book":
            asset_id = str(event.get("asset_id") or event.get("assetId") or "")
            if not asset_id:
                return
            bids = event.get("bids") or event.get("buys") or []
            asks = event.get("asks") or event.get("sells") or []
            top = OrderBookTop(
                best_bid=_best_price(bids),
                best_ask=_best_price(asks, ascending=True),
                updated_at=datetime.now(tz=timezone.utc),
            )
            self._on_book_update(asset_id, top)
            return

        if event_type == "price_change":
            for change in event.get("price_changes", []):
                asset_id = str(change.get("asset_id") or change.get("assetId") or "")
                if not asset_id:
                    continue
                bid = _to_float(change.get("best_bid") or change.get("bestBid"))
                ask = _to_float(change.get("best_ask") or change.get("bestAsk"))
                top = OrderBookTop(
                    best_bid=bid,
                    best_ask=ask,
                    updated_at=datetime.now(tz=timezone.utc),
                )
                self._on_book_update(asset_id, top)


def _best_price(levels: list[dict], ascending: bool = False) -> float | None:
    prices = [_to_float(level.get("price")) for level in levels]
    prices = [p for p in prices if p is not None]
    if not prices:
        return None
    return min(prices) if ascending else max(prices)


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
