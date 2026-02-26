from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from datetime import datetime
from time import monotonic
from typing import Any
from zoneinfo import ZoneInfo
import asyncio

from hail.config import Settings
from hail.models import MarketDefinition, TradeIntent

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams, MarketOrderArgs, OrderArgs, OrderType
from py_clob_client.exceptions import PolyApiException
from py_clob_client.order_builder.constants import BUY, SELL
from web3 import Web3

MIN_MARKETABLE_BUY_NOTIONAL = 1.1
BUY_MIN_NOTIONAL_BUFFER = Decimal("0.0005")
SIZE_DECIMAL_PLACES = 6
MIN_BUY_SIZE_SHARES = Decimal("5")
CONDITIONAL_TOKEN_DECIMALS = 6
SELL_SIZE_FACTOR = Decimal("0.9")
BALANCE_REJECTION_COOLDOWN_SECONDS = 20.0
CTF_CONTRACT_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_COLLATERAL_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"


class PolymarketTrader:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: ClobClient | None = None
        self._buy_side: str | None = None
        self._sell_side: str | None = None
        self._order_args_cls: OrderArgs | None = None
        self._balance_rejection_cooldown_until: dict[tuple[str, str], float] = {}
        self._sell_reduction_tokens: set[str] = set()
        self._web3: Web3 | None = None
        self._signer_address: str | None = None

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
        self._init_claim_client()
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

        end_time_sg = market.end_time.astimezone(ZoneInfo("Asia/Singapore"))
        end_time_display = (
            f"{end_time_sg.day} {end_time_sg.strftime('%b')} "
            f"{end_time_sg.strftime('%-I:%M%p').lower()}"
        )
        market_display = f"{market.symbol} {market.window_minutes}m, {end_time_display}"
        outcome_side = "YES" if intent.token_id == market.yes_token_id else "NO"

        submit_size = self._normalize_order_size(intent)
        if intent.reason.startswith("po_") and abs(submit_size - float(intent.size)) > 1e-9:
            logging.warning(
                (
                    "Rejecting PO order due to size deviation: market=%s outcome=%s "
                    "requested_size=%.6f normalized_size=%.6f price=%.4f condition=%s token=%s"
                ),
                market_display,
                outcome_side,
                float(intent.size),
                submit_size,
                float(intent.price),
                intent.condition_id,
                intent.token_id,
            )
            return None

        args = self._order_args_cls(
            token_id=intent.token_id,
            price=round(intent.price, 4),
            size=submit_size,
            side=side,
        )
        # logging.info(
        #     (
        #         "Order attempt: %s %s market=%s requested_size=%.4f "
        #         "submit_size=%.4f price=%.4f submit_notional=%.4f reason=%s"
        #     ),
        #     intent.side,
        #     outcome_side,
        #     market_display,
        #     intent.size,
        #     args.size,
        #     args.price,
        #     args.size * args.price,
        #     intent.reason,
        # )
        try:
            response = self._client.create_and_post_order(args)
            order_id = response.get("orderID")
            if intent.side.upper() == "SELL" and intent.token_id in self._sell_reduction_tokens:
                self._sell_reduction_tokens.discard(intent.token_id)
                logging.info(
                    "Sell size reduction mode cleared after successful SELL: token=%s",
                    intent.token_id,
                )
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
                if intent.side.upper() == "SELL":
                    self._sell_reduction_tokens.add(intent.token_id)
                    logging.warning(
                        "Sell size reduction mode enabled after balance rejection: token=%s factor=%.2f",
                        intent.token_id,
                        float(SELL_SIZE_FACTOR),
                    )
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
                logging.error(
                    "❌ Order failed: market=%s outcome=%s condition=%s error=%s",
                    market_display,
                    outcome_side,
                    intent.condition_id,
                    exc,
                )
            return None
        except Exception as exc:  # noqa: BLE001
            logging.exception(
                "❌ Order failed: market=%s outcome=%s condition=%s error=%s",
                market_display,
                outcome_side,
                intent.condition_id,
                exc,
            )
            return None

    async def place_market_close_order(self, intent: TradeIntent, market: MarketDefinition) -> str | None:
        if self._client is None:
            raise RuntimeError("Trader client not initialized")
        if intent.side.upper() != "SELL":
            return None

        end_time_sg = market.end_time.astimezone(ZoneInfo("Asia/Singapore"))
        end_time_display = (
            f"{end_time_sg.day} {end_time_sg.strftime('%b')} "
            f"{end_time_sg.strftime('%-I:%M%p').lower()}"
        )
        market_display = f"{market.symbol} {market.window_minutes}m, {end_time_display}"
        outcome_side = "YES" if intent.token_id == market.yes_token_id else "NO"

        try:
            order = self._client.create_market_order(
                MarketOrderArgs(
                    token_id=intent.token_id,
                    amount=max(float(intent.size), 0.0),
                    side=self._sell_side,
                    order_type=OrderType.FAK,
                )
            )
            response = self._client.post_order(order)
            order_id = response.get("orderID")
            logging.info(
                (
                    "Market close posted: %s %s market=%s size=%.6f "
                    "order_id=%s reason=%s"
                ),
                intent.side,
                outcome_side,
                market_display,
                intent.size,
                order_id,
                intent.reason,
            )
            return str(order_id) if order_id else None
        except PolyApiException as exc:
            logging.warning(
                "Market close failed for tiny position: side=%s token=%s size=%.6f error=%s",
                intent.side,
                intent.token_id,
                intent.size,
                exc,
            )
            return None
        except Exception as exc:  # noqa: BLE001
            logging.warning(
                "Market close error for tiny position: side=%s token=%s size=%.6f error=%s",
                intent.side,
                intent.token_id,
                intent.size,
                exc,
            )
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

    def cancel_order(self, order_id: str) -> bool:
        if self._client is None:
            return False
        try:
            response = self._client.cancel(order_id)
            logging.info("Cancel request sent for order_id=%s response=%s", order_id, response)
            return True
        except PolyApiException as exc:
            logging.warning("Cancel failed for order_id=%s: %s", order_id, exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logging.warning("Cancel failed for order_id=%s: %s", order_id, exc)
            return False

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
            raw_balance = str(info.get("balance", "0"))
            raw_allowance = str(info.get("allowance", "0"))
            balance = self._safe_float(
                self._format_units(raw_balance, decimals=CONDITIONAL_TOKEN_DECIMALS)
            )
            allowance = self._safe_float(
                self._format_units(raw_allowance, decimals=CONDITIONAL_TOKEN_DECIMALS)
            )
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
        if intent.side.upper() == "SELL":
            if intent.token_id not in self._sell_reduction_tokens:
                return round(size, SIZE_DECIMAL_PLACES)
            size_dec = Decimal(str(size))
            adjusted = (size_dec * SELL_SIZE_FACTOR).quantize(
                Decimal("1." + ("0" * SIZE_DECIMAL_PLACES)),
                rounding=ROUND_DOWN,
            )
            if adjusted <= 0 and size_dec > 0:
                adjusted = size_dec
            adjusted_float = float(adjusted)
            logging.info(
                (
                    "Adjusting SELL size after prior balance rejection: condition=%s token=%s "
                    "old_size=%.6f new_size=%.6f factor=%.2f"
                ),
                intent.condition_id,
                intent.token_id,
                size,
                adjusted_float,
                float(SELL_SIZE_FACTOR),
            )
            return round(adjusted_float, SIZE_DECIMAL_PLACES)

        if price <= 0:
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

    def _init_claim_client(self) -> None:
        if not self._settings.private_key:
            return
        try:
            self._web3 = Web3(Web3.HTTPProvider(self._settings.polygon_rpc_url))
            account = self._web3.eth.account.from_key(self._settings.private_key)
            self._signer_address = account.address
        except Exception as exc:  # noqa: BLE001
            self._web3 = None
            self._signer_address = None
            logging.warning("Failed to initialize claim client: %s", exc)

    async def claim_positions_if_resolved(self, markets: list[MarketDefinition]) -> int:
        if self._web3 is None or not self._settings.private_key or not markets:
            return 0
        if self._signer_address is None:
            return 0

        claimed = 0
        for market in markets:
            try:
                did_claim = await asyncio.to_thread(self._claim_single_market_sync, market)
                if did_claim:
                    claimed += 1
            except Exception as exc:  # noqa: BLE001
                logging.warning(
                    "Claim attempt failed for condition=%s: %s",
                    market.condition_id,
                    exc,
                )
        return claimed

    def _claim_single_market_sync(self, market: MarketDefinition) -> bool:
        if self._web3 is None or self._signer_address is None:
            return False

        resolved = self._is_condition_resolved_sync(market.condition_id)
        if not resolved:
            return False

        yes_balance = self.get_conditional_balance(market.yes_token_id) or 0.0
        no_balance = self.get_conditional_balance(market.no_token_id) or 0.0
        if yes_balance <= 0 and no_balance <= 0:
            return False

        ctf = self._web3.eth.contract(
            address=Web3.to_checksum_address(CTF_CONTRACT_ADDRESS),
            abi=[
                {
                    "inputs": [{"internalType": "bytes32", "name": "", "type": "bytes32"}],
                    "name": "payoutDenominator",
                    "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                    "stateMutability": "view",
                    "type": "function",
                },
                {
                    "inputs": [
                        {"internalType": "address", "name": "collateralToken", "type": "address"},
                        {"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
                        {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
                        {"internalType": "uint256[]", "name": "indexSets", "type": "uint256[]"},
                    ],
                    "name": "redeemPositions",
                    "outputs": [],
                    "stateMutability": "nonpayable",
                    "type": "function",
                },
            ],
        )
        condition_bytes = self._hex_to_bytes32(market.condition_id)
        if condition_bytes is None:
            return False

        account = self._web3.eth.account.from_key(self._settings.private_key)
        tx = ctf.functions.redeemPositions(
            Web3.to_checksum_address(USDC_COLLATERAL_ADDRESS),
            b"\x00" * 32,
            condition_bytes,
            [1, 2],
        ).build_transaction(
            {
                "from": account.address,
                "nonce": self._web3.eth.get_transaction_count(account.address),
                "chainId": self._settings.poly_chain_id,
                "gasPrice": self._web3.eth.gas_price,
            }
        )
        # add a simple gas cap to reduce underestimation failures
        try:
            tx["gas"] = int(ctf.functions.redeemPositions(
                Web3.to_checksum_address(USDC_COLLATERAL_ADDRESS),
                b"\x00" * 32,
                condition_bytes,
                [1, 2],
            ).estimate_gas({"from": account.address}) * 1.2)
        except Exception:  # noqa: BLE001
            tx["gas"] = 350000

        signed = account.sign_transaction(tx)
        tx_hash = self._web3.eth.send_raw_transaction(signed.raw_transaction)
        logging.info(
            "Claim transaction submitted: condition=%s tx=%s",
            market.condition_id,
            tx_hash.hex(),
        )
        return True

    def _is_condition_resolved_sync(self, condition_id: str) -> bool:
        if self._web3 is None:
            return False
        ctf = self._web3.eth.contract(
            address=Web3.to_checksum_address(CTF_CONTRACT_ADDRESS),
            abi=[
                {
                    "inputs": [{"internalType": "bytes32", "name": "", "type": "bytes32"}],
                    "name": "payoutDenominator",
                    "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
                    "stateMutability": "view",
                    "type": "function",
                }
            ],
        )
        condition_bytes = self._hex_to_bytes32(condition_id)
        if condition_bytes is None:
            return False
        try:
            denominator = int(ctf.functions.payoutDenominator(condition_bytes).call())
            return denominator > 0
        except Exception:  # noqa: BLE001
            return False

    @staticmethod
    def _hex_to_bytes32(value: str) -> bytes | None:
        if not value:
            return None
        normalized = value[2:] if value.startswith("0x") else value
        if len(normalized) != 64:
            return None
        try:
            return bytes.fromhex(normalized)
        except ValueError:
            return None
