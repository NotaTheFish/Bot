import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.redis import RedisStorage
from redis.asyncio import Redis

from config import BOT_TOKEN, REDIS_URL
from db.database import create_pool
from middlewares.ban_check import BanCheckMiddleware
from middlewares.antiflood import AntiFloodMiddleware
from handlers import common, user, admin

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    # Redis
    redis = Redis.from_url(REDIS_URL, decode_responses=True)

    # FSM storage в Redis
    storage = RedisStorage(redis=redis)

    # Bot & Dispatcher
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher(storage=storage)

    # PostgreSQL pool
    await create_pool()
    logger.info("✅ Database connected")

    # Middleware (порядок важен)
    dp.message.middleware(BanCheckMiddleware())
    dp.message.middleware(AntiFloodMiddleware(redis))

    # Прокидываем redis в хендлеры через workflow_data
    dp["redis"] = redis

    # Роутеры
    dp.include_router(common.router)
    dp.include_router(user.router)
    dp.include_router(admin.router)

    logger.info("🚀 Bot started")
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
