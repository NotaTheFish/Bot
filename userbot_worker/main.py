from __future__ import annotations

import asyncio
import logging
import signal

from telethon import TelegramClient
from telethon.sessions import StringSession

from .config import load_settings
from .db import create_pool
from .worker import run_worker


async def amain() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # Не обязательно, но удобно: если python-dotenv установлен и есть .env, подтянем переменные
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass

    settings = load_settings()
    pool = await create_pool(settings.database_url)

    # ✅ КЛЮЧЕВОЕ ИЗМЕНЕНИЕ:
    # TELETHON_SESSION теперь воспринимается как StringSession, а не имя файла .session
    client = TelegramClient(
        StringSession(settings.telethon_session),
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )

    stop_event = asyncio.Event()

    # На Linux (Railway) работает, в некоторых средах может быть NotImplementedError — не критично
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            # fallback: stop_event можно будет поставить через KeyboardInterrupt
            pass

    try:
        await client.start()
    except Exception:
        logging.exception(
            "Telethon client failed to start. "
            "Проверьте TELEGRAM_API_ID/TELEGRAM_API_HASH/TELETHON_SESSION."
        )
        await pool.close()
        raise

    try:
        await run_worker(settings, pool, client, stop_event)
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        try:
            await client.disconnect()
        finally:
            await pool.close()


if __name__ == "__main__":
    asyncio.run(amain())
