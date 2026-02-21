from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

# Allow running directly from project root without installation steps.
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from hail.config import get_settings
from hail.binance_feed import BinanceMidPriceFeed


def _fmt_num(value: float | None, decimals: int = 4) -> str:
    if value is None:
        return "None"
    return f"{value:.{decimals}f}"


async def main(seconds: int, tick_seconds: float) -> None:
    settings = get_settings()
    feed = BinanceMidPriceFeed(settings)

    print("Starting Binance subscription smoke test...")
    print(f"Target symbols from config: {settings.target_symbols}")
    print(f"Binance WS base URL: {settings.binance_ws_url}")
    print(f"Run duration: {seconds}s, print interval: {tick_seconds}s")
    print("-" * 80)

    task = asyncio.create_task(feed.start(), name="binance-feed-test")
    started = time.time()
    try:
        while (time.time() - started) < seconds:
            now = time.time()
            print(f"[t+{int(now - started):>3}s]")
            all_fresh = True
            for symbol in settings.target_symbols:
                state = feed.state.get(symbol)
                if state is None:
                    print(f"  {symbol}: missing state")
                    all_fresh = False
                    continue

                age = (now - state.last_update_ts) if state.last_update_ts > 0 else None
                depth_age = (
                    (now - state.last_depth_update_ts)
                    if state.last_depth_update_ts > 0
                    else None
                )
                fresh = age is not None and age < 5.0
                if not fresh:
                    all_fresh = False

                print(
                    "  "
                    f"{symbol}: "
                    f"mid={_fmt_num(state.mid if state.mid > 0 else None, 2)} "
                    f"age_s={_fmt_num(age, 2)} "
                    f"depth_updates={state.depth_updates} "
                    f"depth_age_s={_fmt_num(depth_age, 2)} "
                    f"imb5={_fmt_num(state.imbalance_5, 3)} "
                    f"imb20={_fmt_num(state.imbalance_20, 3)} "
                    f"top_qty(bid/ask)={_fmt_num(state.best_bid_depth_qty, 3)}/"
                    f"{_fmt_num(state.best_ask_depth_qty, 3)}"
                )
            print(f"  healthy={all_fresh}")
            print("-" * 80)
            await asyncio.sleep(tick_seconds)
    finally:
        feed.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        print("Stopped Binance subscription smoke test.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Smoke test Binance BTC/ETH/SOL subscription with print diagnostics."
    )
    parser.add_argument("--seconds", type=int, default=30, help="How long to run the test")
    parser.add_argument(
        "--tick-seconds",
        type=float,
        default=1.0,
        help="How often to print current subscription state",
    )
    args = parser.parse_args()

    asyncio.run(main(seconds=args.seconds, tick_seconds=args.tick_seconds))
