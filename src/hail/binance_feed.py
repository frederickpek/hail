from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass

import websockets

from hail.config import Settings


@dataclass(slots=True)
class SymbolState:
    symbol: str
    mid: float = 0.0
    last_update_ts: float = 0.0
    returns: deque[float] | None = None

    def __post_init__(self) -> None:
        if self.returns is None:
            self.returns = deque(maxlen=500)


class BinanceMidPriceFeed:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._state: dict[str, SymbolState] = {
            symbol: SymbolState(symbol=symbol) for symbol in settings.target_symbols
        }
        self._stop_event = asyncio.Event()

    @property
    def state(self) -> dict[str, SymbolState]:
        return self._state

    async def start(self) -> None:
        stream = "/".join(f"{symbol.lower()}usdt@bookTicker" for symbol in self._settings.target_symbols)
        ws_url = f"{self._settings.binance_ws_url}/{stream}"

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

    def _on_message(self, raw: str) -> None:
        payload = json.loads(raw)
        data = payload.get("data", payload)

        symbol_pair = str(data.get("s", "")).upper()
        if not symbol_pair.endswith("USDT"):
            return
        symbol = symbol_pair[:-4]
        if symbol not in self._state:
            return

        bid = float(data.get("b", 0.0))
        ask = float(data.get("a", 0.0))
        if bid <= 0 or ask <= 0:
            return
        mid = 0.5 * (bid + ask)
        now = time.time()

        state = self._state[symbol]
        if state.mid > 0:
            r = math.log(mid / state.mid)
            state.returns.append(r)
        state.mid = mid
        state.last_update_ts = now
