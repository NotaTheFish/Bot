import asyncio
import logging
import sys

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from payment_bot.config import settings
from payment_bot.db.pool import run_migrations, close_pool
from payment_bot.middlewares.invisible import InvisibleBotMiddleware
from payment_bot.handlers import start, deals, payments, admin
from payment_bot.handlers.deals import set_bot_username
from payment_bot.services.rates import rate_updater_loop
from payment_bot.services.expiry import expire_deals_loop
from payment_bot.services.webhook_server import create_webhook_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


async def main():
    bot = Bot(
        token=settings.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher(storage=MemoryStorage())

    # Register invisible bot middleware on ALL updates
    dp.message.middleware(InvisibleBotMiddleware())
    dp.callback_query.middleware(InvisibleBotMiddleware())

    # Register routers
    dp.include_router(start.router)
    dp.include_router(deals.router)
    dp.include_router(payments.router)
    dp.include_router(admin.router)

    # Run DB migrations
    logger.info("Running DB migrations...")
    await run_migrations()

    # Set bot username for invite links
    me = await bot.get_me()
    set_bot_username(me.username)
    logger.info(f"Bot started: @{me.username}")

    # Start background tasks
    loop = asyncio.get_event_loop()
    loop.create_task(rate_updater_loop())
    loop.create_task(expire_deals_loop(bot))

    # Start webhook HTTP server (for aggregator callbacks)
    webhook_app = create_webhook_app(bot)
    runner = web.AppRunner(webhook_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    logger.info("Webhook server running on :8080")

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await runner.cleanup()
        await close_pool()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
