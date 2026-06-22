import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from config import Config
from db import Database
from handlers import start, setup, review, inline, client_setup, constructor, my_templates

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    config = Config()
    db = Database(config.DATABASE_URL)
    await db.init()

    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=MemoryStorage())

    dp["db"] = db
    dp["config"] = config

    dp.include_router(start.router)
    dp.include_router(setup.router)
    dp.include_router(review.router)
    dp.include_router(inline.router)
    dp.include_router(client_setup.router)
    dp.include_router(constructor.router)
    dp.include_router(my_templates.router)

    logger.info("ReviewBot starting...")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
