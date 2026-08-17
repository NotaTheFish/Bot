"""
Акция «x5 шансы» на ежедневную рулетку.

Раз в сутки в СЛУЧАЙНОЕ время (МСК) на BOOST_DURATION_MIN минут бустим шансы !шайн:
крупные полосы и джекпоты ×5, мелочь просажена (см. roulette.boosted_bands / roll).

- В чатах с активной рулеткой — громкое объявление старта и конца (БЕЗ упоминания
  игроков: иначе читерно, все набегут ровно в эти 10 минут зная точно).
- Админу (SUPER_ADMINS) — тихое уведомление в личку о старте (для контроля).

Планировщик: каждый день выбираем случайную секунду суток по МСК. Ждём до неё,
запускаем акцию, ждём длительность, гасим. Затем ждём следующего дня.
"""
import asyncio
import contextlib
import logging
import random
from datetime import datetime, timedelta

from zoneinfo import ZoneInfo

import db
import roulette
from config import BOOST_DURATION_MIN, ROULETTE_TZ, SUPER_ADMINS
from services import ui

log = logging.getLogger("boost")
_TZ = ZoneInfo(ROULETTE_TZ)


async def _announce_start(bot):
    dur = BOOST_DURATION_MIN
    text = (f"⚡️ <b>ВНИМАНИЕ! Акция ×5 запущена!</b> ⚡️\n\n"
            f"Следующие <b>{dur} минут</b> шансы в ежедневной рулетке "
            f"<b>сильно повышены</b>: крупные выигрыши и джекпоты выпадают куда чаще!\n\n"
            f"Успей крутить <b>!шайн</b> — второго такого шанса сегодня не будет 🍄")
    chats = await db.active_roulette_chats()
    for ch in chats:
        with contextlib.suppress(Exception):
            await ui.send(bot, ch["chat_id"], text)
    # тихое уведомление админам в личку
    for admin_id in SUPER_ADMINS:
        with contextlib.suppress(Exception):
            await ui.send(bot, admin_id,
                f"🔔 Акция ×5 стартовала на {dur} мин (в {len(chats)} чатах). "
                f"Это служебное уведомление, в чатах объявлено громко.")


async def _announce_end(bot):
    text = ("🔕 <b>Акция ×5 завершена.</b>\n\n"
            "Шансы вернулись к обычным. Спасибо, что играли — "
            "лови следующую волну завтра в случайное время!")
    chats = await db.active_roulette_chats()
    for ch in chats:
        with contextlib.suppress(Exception):
            await ui.send(bot, ch["chat_id"], text)


async def _run_one_event(bot):
    """Запустить акцию сейчас: включить буст, объявить, ждать, выключить, объявить."""
    dur_sec = BOOST_DURATION_MIN * 60
    roulette.boost_start(dur_sec)
    log.info("АКЦИЯ x5 старт на %s мин", BOOST_DURATION_MIN)
    await _announce_start(bot)
    await asyncio.sleep(dur_sec)
    roulette.boost_stop()
    log.info("АКЦИЯ x5 конец")
    await _announce_end(bot)


def _seconds_until_random_time_today_or_tomorrow(now: datetime) -> float:
    """
    Секунды до случайного момента. Выбираем случайную секунду В ПРЕДЕЛАХ ТЕКУЩИХ
    суток по МСК; если она уже прошла — берём случайную секунду завтрашних суток.
    Так каждый день ровно одна акция в непредсказуемое время.
    """
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # случайная секунда в сутках, оставляя запас на длительность акции в конце дня
    max_sec = 24 * 3600 - BOOST_DURATION_MIN * 60 - 1
    rnd = random.randint(0, max_sec)
    target = day_start + timedelta(seconds=rnd)
    if target <= now:
        target = target + timedelta(days=1)
    return (target - now).total_seconds()


async def worker(bot):
    """Вечный воркер: раз в сутки запускает акцию в случайное время МСК."""
    # небольшой стартовый разброс, чтобы не совпасть с рестартами
    await asyncio.sleep(random.uniform(5, 30))
    while True:
        try:
            now = datetime.now(_TZ)
            wait = _seconds_until_random_time_today_or_tomorrow(now)
            log.info("следующая акция x5 через %.0f мин", wait / 60)
            await asyncio.sleep(wait)
            await _run_one_event(bot)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("ошибка в акции x5, продолжаю через час")
            await asyncio.sleep(3600)
