"""
Публикация розыгрыша: объявление в чаты/каналы, закреп, результаты.

Чат — постим от имени бота (премиум-эмодзи работают через ui/render).
Канал — пост уходит от имени канала автоматически; премиум там владелец добавит
вручную, поэтому в канал шлём html как есть (Telegram отрендерит обычные emoji,
кастомные в канале от бота не пройдут — это ограничение).

Кнопка «Участвовать» — deep-link в ЛС бота: t.me/<bot>?start=gw_<id>.
"""
import contextlib
import logging

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

import db
from services import gw_invites, render, settings, ui

log = logging.getLogger("giveaway")


async def _join_kb(bot, gid: int) -> InlineKeyboardMarkup:
    me = await bot.get_me()
    url = f"https://t.me/{me.username}?start=gw_{gid}"
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🎉 Участвовать", url=url)]])


async def publish(bot, gid: int) -> tuple[int, int, list[str]]:
    """
    Опубликовать объявление во все привязанные чаты/каналы и закрепить.
    Возвращает (успешно, всего, список_ошибок).
    """
    gw = await db.gw_get(gid)
    chats = await db.gw_chats(gid)
    kbd = await _join_kb(bot, gid)

    ok_count = 0
    errors = []
    for ch in chats:
        try:
            if ch["kind"] == "channel":
                # в канал — обычная отправка (от имени канала), premium вручную
                m = await bot.send_message(ch["chat_id"], gw["announce_text"],  # noqa: ui
                                           reply_markup=kbd)
            else:
                # в чат — через render, премиум-эмодзи работают
                m = await ui.send(bot, ch["chat_id"], gw["announce_text"],
                                  reply_markup=kbd)
            await db.gw_save_announce_msg(gid, ch["chat_id"], m.message_id)
            # закрепляем
            with contextlib.suppress(Exception):
                await bot.pin_chat_message(ch["chat_id"], m.message_id,
                                           disable_notification=True)
            ok_count += 1
        except Exception as e:
            log.warning("публикация в %s не удалась: %s", ch["chat_id"], e)
            errors.append(f"{ch['title']}: {e}")

    if ok_count:
        await db.gw_set_status(gid, "running")
    return ok_count, len(chats), errors


async def publish_results(bot, gid: int, winners_text: str) -> None:
    """
    Опубликовать результаты: открепить объявление, попытаться удалить его,
    запостить результаты и закрепить их.
    """
    gw = await db.gw_get(gid)
    chats = await db.gw_chats(gid)
    full = gw["finish_text"] + "\n\n" + winners_text

    for ch in chats:
        # открепить + удалить объявление
        if ch.get("announce_msg"):
            with contextlib.suppress(Exception):
                await bot.unpin_chat_message(ch["chat_id"], ch["announce_msg"])
            with contextlib.suppress(Exception):
                await bot.delete_message(ch["chat_id"], ch["announce_msg"])
        # опубликовать результаты
        try:
            if ch["kind"] == "channel":
                m = await bot.send_message(ch["chat_id"], full)  # noqa: ui
            else:
                m = await ui.send(bot, ch["chat_id"], full)
            await db.gw_save_result_msg(gid, ch["chat_id"], m.message_id)
            with contextlib.suppress(Exception):
                await bot.pin_chat_message(ch["chat_id"], m.message_id,
                                           disable_notification=True)
        except Exception as e:
            log.warning("результаты в %s не опубликованы: %s", ch["chat_id"], e)
