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


# ---------- хранение расписания в БД (переживает рестарты) ----------
# rb_settings: boost.day='YYYY-MM-DD', boost.at='HH:MM' (МСК), boost.done='0'|'1'
async def _today_str() -> str:
    return datetime.now(_TZ).strftime("%Y-%m-%d")


async def _plan_today(now: datetime) -> datetime | None:
    """
    Назначить время акции на СЕГОДНЯ — случайное, но ТОЛЬКО из оставшейся части дня
    (позже now, с запасом на длительность). Записать в БД. Вернуть datetime старта
    или None, если день уже кончился (окна на 10-минутную акцию не осталось).
    """
    from services import settings
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    max_sec = 24 * 3600 - BOOST_DURATION_MIN * 60 - 1
    sec_now = int((now - day_start).total_seconds()) + 60  # не раньше чем через минуту
    if sec_now > max_sec:
        return None  # день кончился, окна нет
    rnd = random.randint(sec_now, max_sec)
    target = day_start + timedelta(seconds=rnd)
    await settings.set("boost.day", now.strftime("%Y-%m-%d"))
    await settings.set("boost.at", target.strftime("%H:%M"))
    await settings.set("boost.done", "0")
    return target


async def _notify_admins_schedule(bot, target: datetime):
    """Утреннее уведомление админам: во сколько сегодня акция."""
    hhmm = target.strftime("%H:%M")
    for admin_id in SUPER_ADMINS:
        with contextlib.suppress(Exception):
            await ui.send(bot, admin_id,
                f"📅 Сегодня акция ×5 будет в <b>{hhmm}</b> МСК "
                f"(длительность {BOOST_DURATION_MIN} мин).\n"
                f"Это только для тебя — в чатах объявится громко при старте.")


async def worker(bot):
    """
    Вечный воркер акции ×5. Раз в сутки:
      - в 00:01 МСК назначает случайное время акции, пишет админу, ждёт и проводит;
      - переживает рестарты: расписание в БД. При старте бота — восстанавливает.
    """
    from services import settings
    await asyncio.sleep(random.uniform(5, 20))
    while True:
        try:
            now = datetime.now(_TZ)
            today = now.strftime("%Y-%m-%d")
            plan_day = await settings.get("boost.day", "")
            plan_at = await settings.get("boost.at", "")
            done = await settings.get("boost.done", "0")

            if plan_day == today and plan_at:
                # расписание на сегодня уже есть
                if done == "1":
                    await _sleep_until_next_plan(now)
                    continue
                hh, mm = map(int, plan_at.split(":"))
                target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
                if now < target:
                    wait = (target - now).total_seconds()
                    log.info("акция x5 сегодня в %s МСК, через %.0f мин", plan_at, wait / 60)
                    await asyncio.sleep(wait)
                    await _run_one_event(bot)
                else:
                    # время прошло, а акция не проведена (бот падал) — догоняем ОДИН раз
                    log.info("акция x5 пропущена (рестарт), провожу немедленно")
                    await _run_one_event(bot)
                await settings.set("boost.done", "1")
                await _sleep_until_next_plan(now)  # до конца суток больше не планируем
                continue

            # расписания на сегодня нет — назначаем на ОСТАТОК дня
            target = await _plan_today(now)
            if target is None:
                # окна нет (поздний вечер) — ждём завтрашней полуночи, тихо
                log.info("на сегодня окна для акции нет, жду завтра")
                await _sleep_until_next_plan(now)
                continue
            # уведомляем админа ТОЛЬКО про реальную будущую акцию
            await _notify_admins_schedule(bot, target)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("ошибка в акции x5, продолжаю через 10 мин")
            await asyncio.sleep(600)


async def _sleep_until_next_plan(now: datetime):
    """Спать до 00:01 следующего дня (когда назначим новую акцию)."""
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=1, second=0, microsecond=0)
    await asyncio.sleep(max(30, (tomorrow - now).total_seconds()))
