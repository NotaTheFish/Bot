import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

from config import Config
from db import Database
from handlers import start, setup, review, inline, client_setup, constructor, my_templates, channel, admin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    config = Config()
    db = Database(config.DATABASE_URL)
    await db.init()

    # Диагностика шрифтов при старте — чтобы видеть в логах что установлено
    try:
        import subprocess
        result = subprocess.run(["fc-list"], capture_output=True, text=True, timeout=10)
        fonts = result.stdout
        checks = {
            "Noto Sans Coptic": "Noto Sans Coptic" in fonts,
            "Noto Sans CJK": "Noto Sans CJK" in fonts or "NotoSansCJK" in fonts,
            "Noto Color Emoji": "Noto Color Emoji" in fonts or "NotoColorEmoji" in fonts,
            "Symbola": "Symbola" in fonts,
            "Noto Sans (base)": "NotoSans-" in fonts or "Noto Sans:" in fonts,
        }
        logger.info("=== ДИАГНОСТИКА ШРИФТОВ ===")
        total = fonts.count("\n")
        logger.info(f"Всего шрифтов в системе: {total}")
        for name, present in checks.items():
            logger.info(f"  {'✅' if present else '❌'} {name}")
        logger.info("===========================")
    except Exception as e:
        logger.warning(f"Не удалось проверить шрифты: {e}")

    # Увеличенный таймаут сессии — большие карточки (логотип + пруф) грузятся дольше
    session = AiohttpSession(timeout=120)
    bot = Bot(
        token=config.BOT_TOKEN,
        session=session,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher(storage=MemoryStorage())

    dp["db"] = db
    dp["config"] = config

    # Мидлварь блокировки забаненных — на сообщения, колбэки и инлайн
    from middlewares import BanMiddleware, ThrottlingMiddleware
    ban_mw = BanMiddleware(db, config)
    dp.message.middleware(ban_mw)
    dp.callback_query.middleware(ban_mw)
    dp.inline_query.middleware(ban_mw)

    # Троттлинг — ПОСЛЕ бана (сначала отсекаем забаненных, потом лимитируем частоту)
    throttle_mw = ThrottlingMiddleware(db, config)
    dp.message.middleware(throttle_mw)
    dp.callback_query.middleware(throttle_mw)
    dp.inline_query.middleware(throttle_mw)

    dp.include_router(admin.router)
    dp.include_router(start.router)
    dp.include_router(setup.router)
    dp.include_router(review.router)
    dp.include_router(inline.router)
    dp.include_router(client_setup.router)
    dp.include_router(constructor.router)
    dp.include_router(my_templates.router)
    dp.include_router(channel.router)

    logger.info("ReviewBot starting...")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
