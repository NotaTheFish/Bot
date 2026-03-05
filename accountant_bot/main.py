from __future__ import annotations

import asyncio
import logging
import signal

from aiogram import Bot, Dispatcher
from telethon import TelegramClient
from telethon.sessions import StringSession

from .admin_bot import register_admin_handlers
from .config import load_settings
from .db import acquire_singleton_lock, create_pool, ensure_schema, release_singleton_lock
from .listener import register_listener_handlers
from .reviews import ReviewsService
from .taboo import load_taboo


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    settings = load_settings()
    load_taboo(settings)

    pool = await create_pool(settings.DATABASE_URL)
    await ensure_schema(pool)

    lock_conn = None
    if settings.SINGLETON_LOCK_ENABLED:
        try:
            lock_conn = await acquire_singleton_lock(pool, settings.SINGLETON_LOCK_KEY)
        except Exception as exc:
            logging.error("Failed to acquire singleton lock due to database error: %s", exc)
            await pool.close()
            return

        if lock_conn is None:
            logging.info(
                "Singleton lock key=%s is already held; another instance is running. Exiting.",
                settings.SINGLETON_LOCK_KEY,
            )
            await pool.close()
            return

        logging.info("Acquired singleton lock key=%s", settings.SINGLETON_LOCK_KEY)

    bot = Bot(token=settings.ACCOUNTANT_BOT_TOKEN)
    dispatcher = Dispatcher()

    telethon_client = TelegramClient(
        StringSession(settings.ACCOUNTANT_TG_STRING_SESSION),
        settings.TG_API_ID,
        settings.TG_API_HASH,
    )

    reviews_service = ReviewsService(pool, bot, settings)

    dispatcher["settings"] = settings
    dispatcher["pool"] = pool
    dispatcher["reviews_service"] = reviews_service

    register_listener_handlers(telethon_client, reviews_service, settings)
    register_admin_handlers(dispatcher)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass

    polling_task: asyncio.Task[None] | None = None
    telethon_task: asyncio.Task[None] | None = None
    stop_task: asyncio.Task[None] | None = None

    try:
        logging.info("Starting Telethon client…")
        await telethon_client.start()
        logging.info("Accountant started")

        # Синхронизация истории канала
        await reviews_service.sync_channel_history(telethon_client, settings.REVIEWS_CHANNEL_ID)

        # Обновление описания канала
        await reviews_service.update_channel_about(
            bot,
            settings.REVIEWS_CHANNEL_ID,
            settings.ABOUT_TEMPLATE,
            settings.ABOUT_DATE_FORMAT,
        )

        polling_task = asyncio.create_task(dispatcher.start_polling(bot), name="bot-polling")
        telethon_task = asyncio.create_task(
            telethon_client.run_until_disconnected(),
            name="telethon-runner",
        )
        stop_task = asyncio.create_task(stop_event.wait(), name="stop-waiter")

        done, pending = await asyncio.wait(
            {polling_task, telethon_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )

        if stop_task not in done:
            for task in done:
                task.result()

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    finally:
        try:
            await dispatcher.stop_polling()
        except Exception:
            pass

        if telethon_client.is_connected():
            await telethon_client.disconnect()

        await bot.session.close()
        await release_singleton_lock(pool, lock_conn, settings.SINGLETON_LOCK_KEY)
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())