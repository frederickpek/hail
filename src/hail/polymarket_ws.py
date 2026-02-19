from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from datetime import datetime, timezone

import websockets

from hail.config import Settings
from hail.models import OrderBookTop

BookCallback = Callable[[str, OrderBookTop], None]


class PolymarketBookFeed:
    def __init__(self, settings: Settings, on_book_update: BookCallback) -> None:
        self._settings = settings
        self._on_book_update = on_book_update
        self._tracked_assets: set[str] = set()
        self._tracked_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

    async def add_assets(self, token_ids: list[str]) -> None:
        async with self._tracked_lock:
            for token in token_ids:
                self._tracked_assets.add(token)

    def stop(self) -> None:
        self._stop_event.set()

    async def start(self) -> None:
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(self._settings.poly_ws_url, ping_interval=20, ping_timeout=20) as ws:
                    logging.info("Connected to Polymarket market websocket")
                    await self._send_subscription(ws)

                    async for raw in ws:
                        if self._stop_event.is_set():
                            break
                        await self._process_message(raw, ws)
            except Exception as exc:  # noqa: BLE001
                logging.warning("Polymarket websocket disconnected: %s", exc)
                await asyncio.sleep(2)

    async def _send_subscription(self, ws: websockets.ClientConnection) -> None:
        async with self._tracked_lock:
            assets = sorted(self._tracked_assets)
        payload = {
            "type": "market",
            "assets_ids": assets,
        }
        await ws.send(json.dumps(payload))
        logging.info("Subscribed to %s token books", len(assets))

    async def _process_message(self, raw: str, ws: websockets.ClientConnection) -> None:
        payload = json.loads(raw)
        if isinstance(payload, list):
            for item in payload:
                self._apply_market_event(item)
            return
        self._apply_market_event(payload)

        # In case assets were added after connection established.
        if payload.get("event_type") == "subscribed":
            await self._send_subscription(ws)

    def _apply_market_event(self, event: dict) -> None:
        event_type = event.get("event_type")
        if event_type == "book":
            asset_id = str(event.get("asset_id") or "")
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
                asset_id = str(change.get("asset_id") or "")
                if not asset_id:
                    continue
                bid = _to_float(change.get("best_bid"))
                ask = _to_float(change.get("best_ask"))
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
