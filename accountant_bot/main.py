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


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    settings = load_settings()

    pool = await create_pool(settings.database_url)
    await init_db(pool)

    bot = Bot(token=settings.bot_token)
    dispatcher = Dispatcher()

    telethon_client = TelegramClient(
        StringSession(settings.accountant_tg_string_session),
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )

    register_listener_handlers(dispatcher)
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