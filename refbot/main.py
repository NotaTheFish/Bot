import asyncio
import contextlib
import logging

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

import db
import roulette
from config import (BOT_TOKEN, CONTEST_MIN_MSGS, CONTEST_MSGS_PER_TICKET,
                    CONTEST_TEST_MINUTES, UNLIMITED_SPIN_IDS)
from handlers import admin, chat_events, contest, giveaway, roulette_cmd, skin, user
from services import referrals, settings, ui

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("refbot")

# chat_member ОБЯЗАТЕЛЕН — без него Telegram не пришлёт входы/выходы.
ALLOWED = ["message", "callback_query", "chat_member", "my_chat_member"]


async def hold_worker(bot: Bot):
    """Раз в минуту добиваем созревшие холды. Идемпотентно — можно перезапускать сколько угодно."""
    while True:
        try:
            for p in await referrals.credit_due(bot):
                with contextlib.suppress(Exception):
                    s = await settings.ctx()
                    await ui.send(
                        bot, p["inviter_id"],
                        f"{s['e_paid']} <b>Реферал подтверждён!</b>\n"
                        f"Начислено: <b>{p['amount']:,}</b> {s['e_' + p['currency']]}\n"
                        f"Баланс: <b>{p['balance']:,}</b>".replace(",", " "))
        except Exception:
            log.exception("hold_worker упал, продолжаю")
        await asyncio.sleep(60)


async def main():
    await db.init()

    missing = await db.check_schema()
    if missing:
        log.error("=" * 62)
        log.error("В БАЗЕ НЕТ ТАБЛИЦ: %s", ", ".join(missing))
        log.error("Накати setup.sql, иначе связанные команды будут молча не работать:")
        log.error("  psql -U postgres -d railway -f /setup.sql")
        log.error("Бот запустится и продолжит работать тем, что есть.")
        log.error("=" * 62)
    else:
        log.info("схема БД в порядке: все %d таблиц на месте", len(db.EXPECTED_TABLES))

    await settings.load()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())

    @dp.errors()
    async def on_error(event):
        """
        Ловит ВСЁ, что вылетело из обработчиков, чтобы бот не «зависал».
        TelegramRetryAfter (флуд-контроль) — не ошибка логики, а просьба подождать:
        логируем тихо. Остальное — с трейсбеком, но бот продолжает жить.
        """
        exc = event.exception
        if isinstance(exc, TelegramRetryAfter):
            log.warning("флуд-контроль: Telegram просит подождать %s c", exc.retry_after)
        elif isinstance(exc, TelegramBadRequest):
            log.warning("Telegram отклонил запрос: %s", exc)
        else:
            log.exception("необработанная ошибка в апдейте", exc_info=exc)
        return True  # апдейт «обработан» — бот не падает

    dp.message.outer_middleware(contest.CounterMiddleware())
    dp.include_router(contest.router)
    dp.include_router(chat_events.router)
    dp.include_router(roulette_cmd.router)
    dp.include_router(admin.router)
    dp.include_router(giveaway.router)
    dp.include_router(skin.router)
    dp.include_router(user.router)

    log.info("EV рулетки: %.1f 🍄 / %.0f 🪙 за прокрутку",
             roulette.expected_value("mushrooms"), roulette.expected_value("coins"))
    if CONTEST_MIN_MSGS < CONTEST_MSGS_PER_TICKET:
        log.warning("CONTEST_MIN_MSGS=%d ниже CONTEST_MSGS_PER_TICKET=%d — "
                    "на пороге будет ровно 1 билет. Для дробного теста поставь "
                    "CONTEST_MSGS_PER_TICKET=1", CONTEST_MIN_MSGS, CONTEST_MSGS_PER_TICKET)
    if CONTEST_TEST_MINUTES:
        log.warning("=" * 60)
        log.warning("ТЕСТОВЫЙ РЕЖИМ КОНКУРСА: «неделя» = %d мин", CONTEST_TEST_MINUTES)
        log.warning("Очисти CONTEST_TEST_MINUTES перед боевым запуском.")
        log.warning("=" * 60)
    if UNLIMITED_SPIN_IDS:
        log.warning("=" * 60)
        log.warning("ТЕСТОВЫЙ РЕЖИМ: безлимитная рулетка у %s", UNLIMITED_SPIN_IDS)
        log.warning("Это обход защиты. Очисти UNLIMITED_SPIN_IDS перед боевым запуском.")
        log.warning("=" * 60)

    asyncio.create_task(hold_worker(bot))
    asyncio.create_task(contest.worker(bot))
    asyncio.create_task(giveaway.worker(bot))
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await dp.start_polling(bot, allowed_updates=ALLOWED)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
