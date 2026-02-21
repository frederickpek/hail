from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation, ROUND_UP
from datetime import datetime
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo

from hail.config import Settings
from hail.models import MarketDefinition, TradeIntent

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams, OrderArgs
from py_clob_client.exceptions import PolyApiException
from py_clob_client.order_builder.constants import BUY, SELL

MIN_MARKETABLE_BUY_NOTIONAL = 1.1
BUY_MIN_NOTIONAL_BUFFER = Decimal("0.0005")
SIZE_DECIMAL_PLACES = 6
MIN_BUY_SIZE_SHARES = Decimal("5")
BALANCE_REJECTION_COOLDOWN_SECONDS = 20.0


class PolymarketTrader:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: ClobClient | None = None
        self._buy_side: str | None = None
        self._sell_side: str | None = None
        self._order_args_cls: OrderArgs | None = None
        self._balance_rejection_cooldown_until: dict[tuple[str, str], float] = {}

    async def connect(self) -> None:
        if not self._settings.private_key:
            raise ValueError(
                "PRIVATE_KEY is required for authenticated trading requests. "
                "Set PRIVATE_KEY in .env."
            )
        self._buy_side = BUY
        self._sell_side = SELL
        self._order_args_cls = OrderArgs

        # Always use signer-derived API creds for this bot flow.
        temp_client = ClobClient(
            self._settings.poly_host,
            chain_id=self._settings.poly_chain_id,
            key=self._settings.private_key,
            signature_type=self._settings.poly_signature_type,
            funder=self._settings.funder_address,
        )
        creds = temp_client.create_or_derive_api_creds()
        logging.info("Derived CLOB API credentials from signer key")
        self._client = ClobClient(
            self._settings.poly_host,
            chain_id=self._settings.poly_chain_id,
            key=self._settings.private_key,
            creds=creds,
            signature_type=self._settings.poly_signature_type,
            funder=self._settings.funder_address,
        )
        logging.info("Polymarket trader connected")
        self._log_collateral_balance()

    async def place_limit_order(self, intent: TradeIntent, market: MarketDefinition) -> str | None:
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
            size=self._normalize_order_size(intent),
            side=side,
        )
        end_time_sg = market.end_time.astimezone(ZoneInfo("Asia/Singapore"))
        end_time_display = (
            f"{end_time_sg.day} {end_time_sg.strftime('%b')} "
            f"{end_time_sg.strftime('%-I:%M%p').lower()}"
        )
        market_display = f"{market.symbol} {market.window_minutes}m, {end_time_display}"
        outcome_side = "YES" if intent.token_id == market.yes_token_id else "NO"
        logging.info(
            (
                "Order attempt: %s %s market=%s requested_size=%.4f "
                "submit_size=%.4f price=%.4f submit_notional=%.4f reason=%s"
            ),
            intent.side,
            outcome_side,
            market_display,
            intent.size,
            args.size,
            args.price,
            args.size * args.price,
            intent.reason,
        )
        try:
            response = self._client.create_and_post_order(args)
            order_id = response.get("orderID")
            logging.info(
                "Order posted: %s %s market=%s size=%.4f price=%.4f order_id=%s",
                intent.side,
                outcome_side,
                market_display,
                args.size,
                intent.price,
                order_id,
            )
            return str(order_id) if order_id else None
        except PolyApiException as exc:
            message = str(exc)
            if "not enough balance / allowance" in message.lower():
                self._mark_balance_rejection(intent.side, intent.token_id)
                self._log_balance_allowance_snapshot(intent.side, intent.token_id)
                logging.error(
                    (
                        "Order rejected (balance/allowance): side=%s outcome=%s "
                        "market=%s size=%.4f price=%.4f condition=%s error=%s"
                    ),
                    intent.side,
                    outcome_side,
                    market_display,
                    args.size,
                    intent.price,
                    intent.condition_id,
                    message,
                )
            else:
                logging.exception("Order failed for condition=%s: %s", intent.condition_id, exc)
            return None
        except Exception as exc:  # noqa: BLE001
            logging.exception("Order failed for condition=%s: %s", intent.condition_id, exc)
            return None

    def get_order_fill_snapshot(self, order_id: str) -> tuple[float, str] | None:
        if self._client is None:
            return None
        try:
            order = self._client.get_order(order_id)
            status_raw = order.get("status", "")
            status = str(status_raw).lower()
            filled_raw = order.get("size_matched")
            if filled_raw is None:
                filled_raw = order.get("sizeMatched", 0)
            filled_size = self._safe_float(filled_raw)
            return filled_size, status
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to fetch order fill snapshot for order_id=%s: %s", order_id, exc)
            return None

    def _log_collateral_balance(self) -> None:
        balance = self.get_collateral_balance_usdc()
        if balance is not None:
            logging.info("Polymarket collateral balance=%.6f USDC", balance)

    def get_collateral_balance_usdc(self) -> float | None:
        snapshot = self.get_collateral_balance_allowance_usdc()
        if snapshot is None:
            return None
        return snapshot[0]

    def get_collateral_balance_allowance_usdc(self) -> tuple[float, float] | None:
        if self._client is None:
            return None
        try:
            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                # For ERC20 collateral checks, API expects empty asset id.
                token_id="",
            )
            info = self._client.get_balance_allowance(params)
            raw_balance = str(info.get("balance", "0"))
            raw_allowance = str(info.get("allowance", "0"))
            human_balance = self._format_units(raw_balance, decimals=6)
            human_allowance = self._format_units(raw_allowance, decimals=6)
            return self._safe_float(human_balance), self._safe_float(human_allowance)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to fetch collateral balance/allowance: %s", exc)
            return None

    def get_conditional_balance_allowance(self, token_id: str) -> tuple[float, float] | None:
        if self._client is None:
            return None
        try:
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL,
                token_id=token_id,
            )
            info = self._client.get_balance_allowance(params)
            balance = self._safe_float(info.get("balance", 0))
            allowance = self._safe_float(info.get("allowance", 0))
            return balance, allowance
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to fetch conditional balance/allowance for token=%s: %s", token_id, exc)
            return None

    def get_conditional_balance(self, token_id: str) -> float | None:
        snapshot = self.get_conditional_balance_allowance(token_id)
        if snapshot is None:
            return None
        return snapshot[0]

    def can_attempt_order(self, side: str, token_id: str) -> tuple[bool, str]:
        key = self._balance_rejection_key(side, token_id)
        now = monotonic()
        until = self._balance_rejection_cooldown_until.get(key, 0.0)
        if now < until:
            remaining = until - now
            return False, f"balance_rejection_cooldown({remaining:.1f}s)"

        side_upper = side.upper()
        if side_upper == "SELL":
            snapshot = self.get_conditional_balance_allowance(token_id)
            if snapshot is not None:
                bal, _allowance = snapshot
                if bal <= 0:
                    return False, f"insufficient_conditional_balance(balance={bal})"
        elif side_upper == "BUY":
            snapshot = self.get_collateral_balance_allowance_usdc()
            if snapshot is not None:
                bal, _allowance = snapshot
                if bal <= 0:
                    return False, f"insufficient_collateral_balance(balance={bal:.6f})"

        return True, "ok"

    def _mark_balance_rejection(self, side: str, token_id: str) -> None:
        key = self._balance_rejection_key(side, token_id)
        self._balance_rejection_cooldown_until[key] = monotonic() + BALANCE_REJECTION_COOLDOWN_SECONDS

    @staticmethod
    def _balance_rejection_key(side: str, token_id: str) -> tuple[str, str]:
        return side.upper(), token_id

    def _log_balance_allowance_snapshot(self, side: str, token_id: str) -> None:
        side_upper = side.upper()
        if side_upper == "BUY":
            snapshot = self.get_collateral_balance_allowance_usdc()
            if snapshot is None:
                return
            balance, allowance = snapshot
            logging.error(
                "Collateral snapshot after rejection: balance=%.6f USDC allowance=%.6f USDC",
                balance,
                allowance,
            )
            return
        if side_upper == "SELL":
            snapshot = self.get_conditional_balance_allowance(token_id)
            if snapshot is None:
                return
            balance, allowance = snapshot
            logging.error(
                "Conditional token snapshot after rejection: token=%s balance=%s allowance=%s",
                token_id,
                balance,
                allowance,
            )

    @staticmethod
    def _format_units(value: str, decimals: int) -> str:
        try:
            as_decimal = Decimal(value)
        except (InvalidOperation, TypeError, ValueError):
            return value
        scaled = as_decimal / (Decimal(10) ** decimals)
        return format(scaled.normalize(), "f")

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _normalize_order_size(self, intent: TradeIntent) -> float:
        size = max(float(intent.size), 0.0)
        price = max(float(intent.price), 0.0)
        if intent.side.upper() != "BUY" or price <= 0:
            return round(size, SIZE_DECIMAL_PLACES)

        size_dec = Decimal(str(size))
        min_size_dec = MIN_BUY_SIZE_SHARES
        notional = size * price
        if notional >= MIN_MARKETABLE_BUY_NOTIONAL and size_dec >= min_size_dec:
            return round(size, SIZE_DECIMAL_PLACES)

        # Round up with a small buffer so post-rounding notional still clears threshold.
        price_dec = Decimal(str(price))
        min_notional = Decimal(str(MIN_MARKETABLE_BUY_NOTIONAL)) + BUY_MIN_NOTIONAL_BUFFER
        required_size_dec = (min_notional / price_dec).quantize(
            Decimal("1." + ("0" * SIZE_DECIMAL_PLACES)),
            rounding=ROUND_UP,
        )
        adjusted = max(size_dec, required_size_dec, min_size_dec)
        adjusted_float = float(adjusted)
        logging.info(
            (
                "Adjusting BUY size to satisfy minimum constraints: "
                "condition=%s token=%s old_size=%.6f new_size=%.6f "
                "price=%.4f notional=%.4f min_size=%s"
            ),
            intent.condition_id,
            intent.token_id,
            size,
            adjusted_float,
            price,
            adjusted_float * price,
            str(min_size_dec),
        )
        return adjusted_float
