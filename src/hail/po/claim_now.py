from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone

from hail.config import get_settings
from hail.logging_utils import setup_logging
from hail.po.db import PoDatabase
from hail.polymarket_trader import PolymarketTrader


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a one-shot PO claim pass.")
    parser.add_argument(
        "--minutes",
        type=int,
        default=30,
        help="Lookback window in minutes for ended PO markets with fills (default: 30).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Submit claim transactions. Without this flag, only preflight summary is printed.",
    )
    return parser.parse_args()


async def _run() -> int:
    args = _parse_args()
    settings = get_settings()
    setup_logging(settings)

    if args.minutes <= 0:
        raise ValueError("--minutes must be > 0")

    db = PoDatabase(str(settings.db_path))
    await db.init()
    trader = PolymarketTrader(settings)
    await trader.connect()

    now_dt = datetime.now(tz=timezone.utc)
    window_start_dt = now_dt - timedelta(minutes=args.minutes)
    candidates = await db.list_markets_pending_claim(
        window_start_iso=window_start_dt.isoformat(),
        now_iso=now_dt.isoformat(),
    )

    logging.info(
        "PO claim-now preflight: window_start=%s window_end=%s candidates=%s execute=%s",
        window_start_dt.isoformat(),
        now_dt.isoformat(),
        len(candidates),
        args.execute,
    )

    if not candidates:
        return 0

    for market in candidates[:20]:
        logging.info(
            "candidate: %s %sm end=%s condition=%s",
            market.symbol,
            market.window_minutes,
            market.end_time.isoformat(),
            market.condition_id,
        )

    if not args.execute:
        return 0

    claimed_condition_ids = await trader.claim_condition_ids_if_resolved(candidates)
    for condition_id in claimed_condition_ids:
        await db.mark_market_claimed(condition_id)
    logging.info("PO claim-now execute: claimed=%s", len(claimed_condition_ids))
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(_run()))


if __name__ == "__main__":
    main()
