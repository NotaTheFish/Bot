from __future__ import annotations

import asyncio
import logging
import signal

from telethon import TelegramClient
from telethon.sessions import StringSession

from .config import load_settings
from .db import acquire_singleton_lock, create_pool, release_singleton_lock
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
    lock_conn = None

    if settings.singleton_lock_enabled:
        try:
            lock_conn = await acquire_singleton_lock(pool, settings.singleton_lock_key)
        except Exception as exc:
            logging.error("Failed to acquire singleton lock due to database error: %s", exc)
            await pool.close()
            return

        if lock_conn is None:
            logging.info(
                "Singleton lock key=%s is already held; another instance is running. Exiting.",
                settings.singleton_lock_key,
            )
            await pool.close()
            return

        logging.info("Acquired singleton lock key=%s", settings.singleton_lock_key)

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
        logging.info("Starting Telethon client…")
        await client.start()
    except Exception:
        logging.exception(
            "Telethon client failed to start. "
            "Проверьте TELEGRAM_API_ID/TELEGRAM_API_HASH/TELETHON_SESSION."
        )
        await pool.close()
        raise

    logging.info("Worker started")

    try:
        await run_worker(settings, pool, client, stop_event)
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        try:
            await client.disconnect()
        finally:
            await release_singleton_lock(pool, lock_conn, settings.singleton_lock_key)
            await pool.close()


if __name__ == "__main__":
    asyncio.run(amain())
