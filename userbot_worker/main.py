from __future__ import annotations

import asyncio
import logging
import signal

from telethon import TelegramClient

from .config import load_settings
from .db import create_pool
from .worker import run_worker


async def amain() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    settings = load_settings()
    pool = await create_pool(settings.database_url)
    client = TelegramClient(settings.telethon_session, settings.telegram_api_id, settings.telegram_api_hash)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await client.start()
    try:
        await run_worker(settings, pool, client, stop_event)
    finally:
        await client.disconnect()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(amain())