from __future__ import annotations

import logging
from typing import Any

from hail.config import Settings
from hail.models import TradeIntent

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY, SELL


class PolymarketTrader:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: ClobClient | None = None
        self._buy_side: str | None = None
        self._sell_side: str | None = None
        self._order_args_cls: OrderArgs | None = None

    async def connect(self) -> None:
        self._buy_side = BUY
        self._sell_side = SELL
        self._order_args_cls = OrderArgs

        temp_client = ClobClient(
            self._settings.poly_host,
            chain_id=self._settings.poly_chain_id,
            key=self._settings.private_key,
            signature_type=self._settings.poly_signature_type,
            funder=self._settings.funder_address,
        )
        creds = temp_client.create_or_derive_api_creds()
        self._client = ClobClient(
            self._settings.poly_host,
            chain_id=self._settings.poly_chain_id,
            key=self._settings.private_key,
            creds=creds,
            signature_type=self._settings.poly_signature_type,
            funder=self._settings.funder_address,
        )
        logging.info("Polymarket trader connected")

    async def place_limit_order(self, intent: TradeIntent) -> str | None:
        if self._client is None or self._order_args_cls is None:
            raise RuntimeError("Trader client not initialized")

        side_key = intent.side.upper()
        if side_key == "BUY":
            side = self._buy_side
        elif side_key == "SELL":
            side = self._sell_side
        else:
            raise ValueError(f"Unsupported side: {intent.side}")

        args = self._order_args_cls(
            token_id=intent.token_id,
            price=round(intent.price, 4),
            size=round(intent.size, 6),
            side=side,
        )
        try:
            response = await self._client.create_and_post_order(args)
            order_id = response.get("orderID")
            logging.info(
                "Order posted: condition=%s token=%s side=%s size=%.4f price=%.4f order_id=%s",
                intent.condition_id,
                intent.token_id,
                intent.side,
                intent.size,
                intent.price,
                order_id,
            )
            return str(order_id) if order_id else None
        except Exception as exc:  # noqa: BLE001
            logging.exception("Order failed for condition=%s: %s", intent.condition_id, exc)
            return None
