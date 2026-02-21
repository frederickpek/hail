from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
import websockets

from hail.config import Settings


@dataclass(slots=True)
class SymbolState:
    symbol: str
    mid: float = 0.0
    last_update_ts: float = 0.0
    returns: deque[float] | None = None
    mid_history: deque[tuple[float, float]] | None = None
    last_history_store_ts: float = 0.0
    bid_liquidity_5: float = 0.0
    ask_liquidity_5: float = 0.0
    bid_liquidity_20: float = 0.0
    ask_liquidity_20: float = 0.0
    imbalance_5: float = 0.0
    imbalance_20: float = 0.0
    best_bid_depth_qty: float = 0.0
    best_ask_depth_qty: float = 0.0
    depth_updates: int = 0
    last_depth_update_ts: float = 0.0

    def __post_init__(self) -> None:
        if self.returns is None:
            self.returns = deque(maxlen=500)
        if self.mid_history is None:
            # 3h of 1-second snapshots.
            self.mid_history = deque(maxlen=3 * 60 * 60)


class BinanceMidPriceFeed:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._state: dict[str, SymbolState] = {
            symbol: SymbolState(symbol=symbol) for symbol in settings.target_symbols
        }
        self._stop_event = asyncio.Event()
        self._reference_price_cache: dict[tuple[str, int], float] = {}

    @property
    def state(self) -> dict[str, SymbolState]:
        return self._state

    async def start(self) -> None:
        channels: list[str] = []
        for symbol in self._settings.target_symbols:
            base = f"{symbol.lower()}usdt"
            channels.append(f"{base}@bookTicker")
            channels.append(f"{base}@depth20@100ms")
        stream = "/".join(channels)
        ws_url = self._build_ws_url(stream)

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20) as ws:
                    logging.info("Connected to Binance stream: %s", ws_url)
                    async for raw in ws:
                        if self._stop_event.is_set():
                            break
                        self._on_message(raw)
            except Exception as exc:  # noqa: BLE001
                logging.warning("Binance websocket disconnected: %s", exc)
                await asyncio.sleep(2)

    def _build_ws_url(self, stream: str) -> str:
        """
        Build a Binance websocket URL for multiplexed streams.
        Supports common base URL forms:
        - wss://stream.binance.com:9443/ws
        - wss://stream.binance.com:9443/stream
        - wss://stream.binance.com:9443
        """
        base = self._settings.binance_ws_url.rstrip("/")
        if base.endswith("/ws"):
            root = base[:-3]
            return f"{root}/stream?streams={stream}"
        if base.endswith("/stream"):
            return f"{base}?streams={stream}"
        if "streams=" in base:
            return base
        return f"{base}/stream?streams={stream}"

    def stop(self) -> None:
        self._stop_event.set()

    def get_mid(self, symbol: str) -> float | None:
        state = self._state.get(symbol)
        if state is None or state.mid <= 0:
            return None
        return state.mid

    def get_annualized_vol(self, symbol: str, fallback: float) -> float:
        state = self._state.get(symbol)
        if state is None or not state.returns:
            return fallback
        if len(state.returns) < 30:
            return fallback
        values = list(state.returns)
        mean = sum(values) / len(values)
        var = sum((x - mean) ** 2 for x in values) / max(1, len(values) - 1)
        std = math.sqrt(var)
        # Approximate from 1-second returns.
        annualized = std * math.sqrt(365 * 24 * 60 * 60)
        return max(0.05, min(annualized, 3.0))

    def get_mid_at_or_before(self, symbol: str, target_ts: float, max_age_seconds: float = 300.0) -> float | None:
        state = self._state.get(symbol)
        if state is None or not state.mid_history:
            return None
        chosen_mid: float | None = None
        chosen_ts = 0.0
        for ts, mid in state.mid_history:
            if ts <= target_ts and ts >= chosen_ts:
                chosen_ts = ts
                chosen_mid = mid
        if chosen_mid is None:
            return None
        if (target_ts - chosen_ts) > max_age_seconds:
            return None
        return chosen_mid

    async def resolve_strike_from_start(self, symbol: str, start_time: datetime) -> tuple[float | None, str]:
        target_ts = start_time.timestamp()
        from_stream = self.get_mid_at_or_before(symbol, target_ts=target_ts, max_age_seconds=300.0)
        if from_stream is not None and from_stream > 0:
            return from_stream, "binance_ws_history"

        minute_ts = int(target_ts // 60) * 60
        cache_key = (symbol, minute_ts)
        cached = self._reference_price_cache.get(cache_key)
        if cached is not None and cached > 0:
            return cached, "binance_rest_cache"

        resolved = await self._fetch_1m_open_price(symbol, minute_ts)
        if resolved is not None and resolved > 0:
            self._reference_price_cache[cache_key] = resolved
            return resolved, "binance_rest_1m_open"
        return None, "unresolved"

    async def _fetch_1m_open_price(self, symbol: str, minute_ts: int) -> float | None:
        # Query a 1-minute kline anchored at market start minute.
        params = {
            "symbol": f"{symbol.upper()}USDT",
            "interval": "1m",
            "startTime": str(minute_ts * 1000),
            "endTime": str((minute_ts + 60) * 1000),
            "limit": "1",
        }
        try:
            timeout = httpx.Timeout(self._settings.request_timeout_seconds)
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.get("https://api.binance.com/api/v3/klines", params=params)
                response.raise_for_status()
                rows = response.json()
            if not rows:
                return None
            first = rows[0]
            # Kline format: [open_time, open, high, low, close, ...]
            return float(first[1])
        except Exception as exc:  # noqa: BLE001
            logging.warning(
                "Failed to fetch Binance 1m kline reference for %s at %s: %s",
                symbol,
                datetime.fromtimestamp(minute_ts, tz=timezone.utc).isoformat(),
                exc,
            )
            return None

    def _on_message(self, raw: str) -> None:
        payload = json.loads(raw)
        data = payload.get("data", payload)
        event_type = str(data.get("e", ""))
        stream_name = str(payload.get("stream", ""))
        symbol_pair = str(data.get("s", "")).upper()
        if symbol_pair.endswith("USDT"):
            symbol = symbol_pair[:-4]
        else:
            symbol = self._symbol_from_stream_name(stream_name)
            if symbol is None:
                return
        if symbol not in self._state:
            return

        state = self._state[symbol]
        # Spot partial depth stream payloads may not include `e`/`s`, and use bids/asks.
        if "bids" in data or "asks" in data:
            bids = data.get("bids") or []
            asks = data.get("asks") or []
            self._apply_depth_snapshot(state, bids, asks)
            return

        if event_type == "depthUpdate":
            bids = data.get("b") or data.get("bids") or []
            asks = data.get("a") or data.get("asks") or []
            self._apply_depth_snapshot(state, bids, asks)
            return

        if event_type == "bookTicker" or ("b" in data and "a" in data):
            bid = float(data.get("b", 0.0))
            ask = float(data.get("a", 0.0))
            if bid > 0 and ask > 0:
                mid = 0.5 * (bid + ask)
                now = time.time()
                if state.mid > 0:
                    r = math.log(mid / state.mid)
                    state.returns.append(r)
                state.mid = mid
                state.last_update_ts = now
                if (now - state.last_history_store_ts) >= 1.0:
                    state.mid_history.append((now, mid))
                    state.last_history_store_ts = now
            return

    @staticmethod
    def _symbol_from_stream_name(stream_name: str) -> str | None:
        name = stream_name.strip().lower()
        if "@" not in name:
            return None
        base = name.split("@", 1)[0]
        if not base.endswith("usdt"):
            return None
        return base[:-4].upper()

    @staticmethod
    def _apply_depth_snapshot(state: SymbolState, bids: list, asks: list) -> None:
        bid_rows: list[tuple[float, float]] = []
        ask_rows: list[tuple[float, float]] = []
        for row in bids[:20]:
            try:
                bid_rows.append((float(row[0]), float(row[1])))
            except (TypeError, ValueError, IndexError):
                continue
        for row in asks[:20]:
            try:
                ask_rows.append((float(row[0]), float(row[1])))
            except (TypeError, ValueError, IndexError):
                continue

        bid_notional_5 = sum(px * qty for px, qty in bid_rows[:5] if px > 0 and qty > 0)
        ask_notional_5 = sum(px * qty for px, qty in ask_rows[:5] if px > 0 and qty > 0)
        bid_notional_20 = sum(px * qty for px, qty in bid_rows if px > 0 and qty > 0)
        ask_notional_20 = sum(px * qty for px, qty in ask_rows if px > 0 and qty > 0)

        state.bid_liquidity_5 = bid_notional_5
        state.ask_liquidity_5 = ask_notional_5
        state.bid_liquidity_20 = bid_notional_20
        state.ask_liquidity_20 = ask_notional_20

        denom_5 = bid_notional_5 + ask_notional_5
        denom_20 = bid_notional_20 + ask_notional_20
        state.imbalance_5 = 0.0 if denom_5 <= 0 else (bid_notional_5 - ask_notional_5) / denom_5
        state.imbalance_20 = 0.0 if denom_20 <= 0 else (bid_notional_20 - ask_notional_20) / denom_20
        state.best_bid_depth_qty = bid_rows[0][1] if bid_rows else 0.0
        state.best_ask_depth_qty = ask_rows[0][1] if ask_rows else 0.0
        state.depth_updates += 1
        state.last_depth_update_ts = time.time()
