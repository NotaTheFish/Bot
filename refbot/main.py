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
from handlers import admin, bank, casino, casino_chat, chat_events, contest, giveaway, inline_grant, offers, promo, roulette_cmd, skin, tokens_admin, tokens_buy, user, whisper
from services import boost, referrals, settings, ui

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("refbot")

# chat_member ОБЯЗАТЕЛЕН — без него Telegram не пришлёт входы/выходы.
ALLOWED = ["message", "callback_query", "chat_member", "my_chat_member", "channel_post",
           "inline_query", "chosen_inline_result"]


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
        # чистим просроченные бонусы (удача/скидка)
        with contextlib.suppress(Exception):
            from services import inventory as _inv
            await _inv.cleanup_expired()
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
            # НЕ exc_info=exc с полным апдейтом — иначе в лог валится всё сообщение
            # с entities на каждую ошибку. Короткая строка: тип, текст и место падения.
            import traceback
            tb = traceback.extract_tb(exc.__traceback__)
            where = f"{tb[-1].filename.split('/')[-1]}:{tb[-1].lineno}" if tb else "?"
            log.error("ошибка в апдейте: %s: %s (в %s)", type(exc).__name__, exc, where)
        return True  # апдейт «обработан» — бот не падает

    dp.message.outer_middleware(contest.CounterMiddleware())
    dp.message.outer_middleware(whisper.UsernameMiddleware())
    from services.menu_owner import MenuOwnerMiddleware
    dp.callback_query.outer_middleware(MenuOwnerMiddleware())
    dp.include_router(whisper.router)
    dp.include_router(contest.router)
    dp.include_router(casino_chat.router)
    dp.include_router(chat_events.router)
    dp.include_router(roulette_cmd.router)
    dp.include_router(admin.router)
    dp.include_router(giveaway.router)
    dp.include_router(casino.router)
    dp.include_router(bank.router)
    dp.include_router(tokens_admin.router)
    dp.include_router(tokens_buy.router)
    from handlers import ttt as ttt_h, ttt_game
    dp.include_router(ttt_h.router)
    dp.include_router(ttt_game.router)
    from handlers import rps as rps_h, rps_game
    dp.include_router(rps_h.router)
    dp.include_router(rps_game.router)
    from handlers import rps_direct
    dp.include_router(rps_direct.router)
    from handlers import lobby_browser
    dp.include_router(lobby_browser.router)
    from handlers import navy as navy_h, navy_place, navy_game, navy_finish
    dp.include_router(navy_h.router)
    dp.include_router(navy_place.router)
    dp.include_router(navy_game.router)
    dp.include_router(navy_finish.router)
    from handlers import quiz as quiz_h, quiz_game
    dp.include_router(quiz_h.router)
    dp.include_router(quiz_game.router)
    from handlers import quiz_finish
    dp.include_router(quiz_finish.router)
    from handlers import quiz_admin
    dp.include_router(quiz_admin.router)
    dp.include_router(inline_grant.router)
    dp.include_router(offers.router)
    dp.include_router(skin.router)
    dp.include_router(user.router)
    from handlers import profile_ui
    dp.include_router(profile_ui.router)
    from handlers import titles_admin
    dp.include_router(titles_admin.router)
    from handlers import achievements_ui
    dp.include_router(achievements_ui.router)
    from handlers import ach_admin
    dp.include_router(ach_admin.router)
    from handlers import shop_admin, shop_ui
    dp.include_router(shop_admin.router)
    dp.include_router(shop_ui.router)
    from handlers import inventory_ui
    dp.include_router(inventory_ui.router)
    dp.include_router(promo.router)

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
    asyncio.create_task(boost.worker(bot))
    from handlers import ttt_game
    asyncio.create_task(ttt_game.timeout_worker(bot))
    from handlers import rps_game
    asyncio.create_task(rps_game.timeout_worker(bot))
    from handlers import navy_game
    asyncio.create_task(navy_game.timeout_worker(bot))
    await bot.delete_webhook(drop_pending_updates=True)
    # регистрируем /secret как ЭФЕМЕРНУЮ команду (невидимый ввод в группах).
    # Фича свежая — если Telegram/версия не примут is_ephemeral, команда всё равно
    # зарегистрируется как обычная (шёпот дойдёт, но ввод будет виден). Не критично.
    with contextlib.suppress(Exception):
        from aiogram.types import BotCommand
        await bot.set_my_commands([
            BotCommand(command="secret", description="Шепнуть приватно: /secret @user текст",
                       is_ephemeral=True),
        ])
        log.info("команда /secret зарегистрирована как эфемерная")
    try:
        await dp.start_polling(bot, allowed_updates=ALLOWED)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
