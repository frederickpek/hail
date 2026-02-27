from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from py_builder_relayer_client.client import RelayClient
from py_builder_relayer_client.models import OperationType, SafeTransaction
from py_builder_signing_sdk.config import BuilderConfig
from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds
from web3 import Web3

from hail.config import get_settings
from hail.logging_utils import setup_logging
from hail.po.db import PoDatabase
from hail.polymarket_trader import CTF_CONTRACT_ADDRESS, USDC_COLLATERAL_ADDRESS

RELAYER_URL = "https://relayer-v2.polymarket.com"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one-shot PO claim pass via Builder relayer.")
    parser.add_argument(
        "--minutes",
        type=int,
        default=30,
        help="Lookback window in minutes for ended PO markets with fills (default: 30).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Submit relayer transactions. Without this flag, only preflight summary is printed.",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for relayer terminal state (mined/confirmed) before marking claimed.",
    )
    return parser.parse_args()


def _build_redeem_call_data(condition_id: str) -> str:
    normalized = condition_id[2:] if condition_id.startswith("0x") else condition_id
    if len(normalized) != 64:
        raise ValueError(f"invalid condition id: {condition_id}")
    condition_bytes = bytes.fromhex(normalized)
    ctf = Web3().eth.contract(
        address=Web3.to_checksum_address(CTF_CONTRACT_ADDRESS),
        abi=[
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
            }
        ],
    )
    return ctf.encode_abi(
        "redeemPositions",
        args=[
            Web3.to_checksum_address(USDC_COLLATERAL_ADDRESS),
            b"\x00" * 32,
            condition_bytes,
            [1, 2],
        ],
    )


async def _run() -> int:
    args = _parse_args()
    settings = get_settings()
    setup_logging(settings)

    if args.minutes <= 0:
        raise ValueError("--minutes must be > 0")
    if not settings.private_key:
        raise ValueError("PRIVATE_KEY is required in .env for relayer signer address")
    if not settings.poly_api_key or not settings.poly_api_secret or not settings.poly_api_passphrase:
        raise ValueError(
            "Builder credentials missing. Set POLY_API_KEY, POLY_API_SECRET, POLY_API_PASSPHRASE in .env"
        )

    db = PoDatabase(str(settings.db_path))
    await db.init()

    builder_config = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(
            key=settings.poly_api_key,
            secret=settings.poly_api_secret,
            passphrase=settings.poly_api_passphrase,
        )
    )
    relay = RelayClient(
        RELAYER_URL,
        settings.poly_chain_id,
        settings.private_key,
        builder_config,
    )

    now_dt = datetime.now(tz=timezone.utc)
    window_start_dt = now_dt - timedelta(minutes=args.minutes)
    candidates = await db.list_markets_pending_claim(
        window_start_iso=window_start_dt.isoformat(),
        now_iso=now_dt.isoformat(),
    )
    logging.info(
        "PO claim-relay preflight: relayer=%s window_start=%s window_end=%s candidates=%s execute=%s wait=%s",
        RELAYER_URL,
        window_start_dt.isoformat(),
        now_dt.isoformat(),
        len(candidates),
        args.execute,
        args.wait,
    )
    for market in candidates[:20]:
        logging.info(
            "candidate: %s %sm end=%s condition=%s",
            market.symbol,
            market.window_minutes,
            market.end_time.isoformat(),
            market.condition_id,
        )

    if not args.execute or not candidates:
        return 0

    claimed = 0
    for market in candidates:
        try:
            data = _build_redeem_call_data(market.condition_id)
            tx = SafeTransaction(
                to=Web3.to_checksum_address(CTF_CONTRACT_ADDRESS),
                operation=OperationType.Call,
                data=data,
                value="0",
            )
            response = relay.execute([tx], metadata=f"po-redeem-{market.condition_id}")
            logging.info(
                "PO relayer claim submitted: condition=%s transaction_id=%s tx_hash=%s",
                market.condition_id,
                response.transaction_id,
                response.transaction_hash,
            )
            if args.wait:
                terminal = response.wait()
                if terminal is None:
                    logging.warning("PO relayer claim not confirmed: condition=%s", market.condition_id)
                    continue
                logging.info(
                    "PO relayer claim confirmed: condition=%s state=%s tx_hash=%s",
                    market.condition_id,
                    terminal.get("state"),
                    terminal.get("transactionHash"),
                )
            await db.mark_market_claimed(market.condition_id)
            claimed += 1
        except Exception as exc:  # noqa: BLE001
            logging.warning("PO relayer claim failed: condition=%s error=%s", market.condition_id, exc)

    logging.info("PO claim-relay execute: claimed=%s", claimed)
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
