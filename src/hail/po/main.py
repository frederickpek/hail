from __future__ import annotations

import asyncio
import logging
import signal

from hail.config import get_settings
from hail.logging_utils import setup_logging
from hail.po.engine import PoEngine


async def _run() -> None:
    settings = get_settings()
    setup_logging(settings)

    engine = PoEngine(settings)
    loop = asyncio.get_running_loop()

    def _handle_stop() -> None:
        logging.info("Stop signal received, shutting down PO engine.")
        engine.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_stop)

    await engine.run()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()

