from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from web3 import Web3

from hail.config import Settings, get_settings
from hail.logging_utils import setup_logging
from hail.models import MarketDefinition
from hail.polymarket_trader import CTF_CONTRACT_ADDRESS, PolymarketTrader


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Test Polymarket claim flow for one condition id.")
    parser.add_argument("--condition-id", required=True, help="Polymarket condition id (0x... or raw hex).")
    parser.add_argument("--yes-token-id", default=None, help="YES token id (optional; auto-fetch if omitted).")
    parser.add_argument("--no-token-id", default=None, help="NO token id (optional; auto-fetch if omitted).")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually submit redeemPositions tx. Without this flag, only preflight checks run.",
    )
    return parser.parse_args()


def _normalize_condition_id(value: str) -> str:
    trimmed = value.strip()
    if trimmed.startswith("0x"):
        return trimmed
    return f"0x{trimmed}"


def _parse_json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(v) for v in parsed]
        except json.JSONDecodeError:
            return []
    return []


def _parse_time(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        return datetime.now(tz=timezone.utc)
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return datetime.now(tz=timezone.utc)


def _extract_yes_no_tokens(row: dict[str, Any]) -> tuple[str | None, str | None]:
    token_ids = _parse_json_list(row.get("clobTokenIds"))
    outcomes = _parse_json_list(row.get("outcomes"))
    if len(token_ids) < 2:
        return None, None
    yes_token: str | None = None
    no_token: str | None = None
    for token, outcome in zip(token_ids, outcomes):
        label = str(outcome).strip().upper()
        if label == "YES":
            yes_token = str(token)
        elif label == "NO":
            no_token = str(token)
    if not yes_token or not no_token:
        yes_token = str(token_ids[0])
        no_token = str(token_ids[1])
    return yes_token, no_token


async def _fetch_market_from_gamma(settings: Settings, condition_id: str) -> MarketDefinition | None:
    timeout = httpx.Timeout(settings.request_timeout_seconds)
    urls = [
        f"{settings.gamma_api_url}/markets",
        f"{settings.gamma_api_url}/events",
    ]
    async with httpx.AsyncClient(timeout=timeout) as client:
        for url in urls:
            try:
                response = await client.get(url, params={"condition_ids": condition_id, "limit": "5"})
                if response.status_code >= 400:
                    continue
                payload = response.json()
            except Exception:
                continue

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
                row_condition = str(row.get("conditionId") or row.get("condition_id") or "")
                if row_condition.lower() != condition_id.lower():
                    continue
                yes_token, no_token = _extract_yes_no_tokens(row)
                if not yes_token or not no_token:
                    continue
                return MarketDefinition(
                    condition_id=condition_id,
                    slug=str(row.get("slug") or ""),
                    question=str(row.get("question") or row.get("title") or ""),
                    symbol=str(row.get("ticker") or row.get("symbol") or "UNKNOWN"),
                    window_minutes=0,
                    strike=0.0,
                    end_time=_parse_time(row.get("endDate") or row.get("end_time")),
                    yes_token_id=yes_token,
                    no_token_id=no_token,
                )
    return None


def _is_condition_resolved(settings: Settings, condition_id: str) -> bool:
    web3 = Web3(Web3.HTTPProvider(settings.polygon_rpc_url))
    ctf = web3.eth.contract(
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
    normalized = condition_id[2:] if condition_id.startswith("0x") else condition_id
    if len(normalized) != 64:
        return False
    denominator = int(ctf.functions.payoutDenominator(bytes.fromhex(normalized)).call())
    return denominator > 0


async def _run() -> int:
    args = _parse_args()
    settings = get_settings()
    setup_logging(settings)

    if not settings.private_key:
        raise ValueError("PRIVATE_KEY is required in .env")

    condition_id = _normalize_condition_id(args.condition_id)
    trader = PolymarketTrader(settings)
    await trader.connect()

    signer_address = Web3().eth.account.from_key(settings.private_key).address
    logging.info("Signer address (derived from PRIVATE_KEY): %s", signer_address)

    if args.yes_token_id and args.no_token_id:
        market = MarketDefinition(
            condition_id=condition_id,
            slug="manual-claim-check",
            question="manual-claim-check",
            symbol="UNKNOWN",
            window_minutes=0,
            strike=0.0,
            end_time=datetime.now(tz=timezone.utc),
            yes_token_id=str(args.yes_token_id),
            no_token_id=str(args.no_token_id),
        )
    else:
        market = await _fetch_market_from_gamma(settings, condition_id)
        if market is None:
            logging.error(
                "Unable to fetch YES/NO token ids from Gamma for condition_id=%s. "
                "Pass --yes-token-id and --no-token-id manually.",
                condition_id,
            )
            return 1

    logging.info("Using condition=%s yes_token=%s no_token=%s", market.condition_id, market.yes_token_id, market.no_token_id)
    resolved = _is_condition_resolved(settings, market.condition_id)
    yes_balance = trader.get_conditional_balance(market.yes_token_id) or 0.0
    no_balance = trader.get_conditional_balance(market.no_token_id) or 0.0

    logging.info("Preflight: resolved=%s yes_balance=%.6f no_balance=%.6f", resolved, yes_balance, no_balance)
    if not args.execute:
        logging.info("Preflight only complete. Re-run with --execute to submit redeemPositions.")
        return 0

    claimed = await trader.claim_positions_if_resolved([market])
    if claimed > 0:
        logging.info("Claim transaction submitted successfully for condition=%s", market.condition_id)
        return 0

    logging.warning(
        "Claim not submitted for condition=%s. Check preflight logs (resolved/balances) and wallet gas balance.",
        market.condition_id,
    )
    return 2


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
