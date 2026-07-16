import asyncio
import contextlib
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage

import db
import roulette
from config import BOT_TOKEN, CURRENCY_EMOJI
from handlers import admin, chat_events, roulette_cmd, skin, user
from services import referrals, settings

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
                    await bot.send_message(
                        p["inviter_id"],
                        f"✅ <b>Реферал подтверждён!</b>\n"
                        f"Начислено: <b>{p['amount']:,}</b> {CURRENCY_EMOJI[p['currency']]}\n"
                        f"Баланс: <b>{p['balance']:,}</b>".replace(",", " "))
        except Exception:
            log.exception("hold_worker упал, продолжаю")
        await asyncio.sleep(60)


async def main():
    await db.init()
    await settings.load()
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(chat_events.router)
    dp.include_router(roulette_cmd.router)
    dp.include_router(admin.router)
    dp.include_router(skin.router)
    dp.include_router(user.router)

    log.info("EV рулетки: %.1f 🍄 / %.0f 🪙 за прокрутку",
             roulette.expected_value("mushrooms"), roulette.expected_value("coins"))

    asyncio.create_task(hold_worker(bot))
    await bot.delete_webhook(drop_pending_updates=True)
    try:
        await dp.start_polling(bot, allowed_updates=ALLOWED)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
