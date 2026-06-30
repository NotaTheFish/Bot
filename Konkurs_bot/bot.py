import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from dotenv import load_dotenv
import os

from database.db import init_db
from database import db
from handlers import admin, chat_trigger
from services.giveaway_service import revalidate_participants

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# How often to re-check participant subscriptions in the background (seconds)
SUBSCRIPTION_CHECK_INTERVAL = 24 * 60 * 60  # once a day


async def background_subscription_checker(bot: Bot):
    """Periodically re-validate subscriptions for all active giveaways."""
    while True:
        await asyncio.sleep(SUBSCRIPTION_CHECK_INTERVAL)
        try:
            active = await db.get_active_giveaways()
            for g in active:
                try:
                    await revalidate_participants(bot, g['id'], ADMIN_ID)
                except Exception as e:
                    logger.error(f"Background subscription check failed for giveaway {g['id']}: {e}")
        except Exception as e:
            logger.error(f"Background subscription checker error: {e}")


async def main():
    bot = Bot(
        token=os.getenv("BOT_TOKEN"),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()

    dp.include_router(admin.router)
    dp.include_router(chat_trigger.router)

    await init_db()
    logger.info("Bot started")

    asyncio.create_task(background_subscription_checker(bot))

    await dp.start_polling(bot, allowed_updates=["message", "callback_query", "chat_member"])


if __name__ == "__main__":
    asyncio.run(main())
