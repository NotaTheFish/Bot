from __future__ import annotations

import asyncio
import logging
import signal

from aiogram import Bot, Dispatcher
from telethon import TelegramClient
from telethon.sessions import StringSession

from .admin_bot import register_admin_handlers
from .config import load_settings
from .db import create_pool, init_db
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
    await init_db(pool)

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
        await telethon_client.start()

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
        dispatcher.stop_polling()

        if telethon_client.is_connected():
            await telethon_client.disconnect()

        await bot.session.close()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())