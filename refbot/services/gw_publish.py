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


async def _join_url(bot, gid: int) -> str:
    me = await bot.get_me()
    return f"https://t.me/{me.username}?start=gw_{gid}"


async def _publish_to_channel(bot, channel_id: int, gw: dict, photo, join_url: str):
    """
    Публикация в КАНАЛ с сохранением премиум-эмодзи через чат-посредник.

    В канал бот не может слать премиум напрямую. Обход (схема Notarian):
      1. постим объявление в лог-чат (группа) — там премиум работает;
      2. добавляем внизу текст-ссылку «⭢ Участвовать ⭠» (кнопку в канал не
         прокинуть при пересылке, поэтому ссылкой в тексте);
      3. forward из лог-чата в канал — пересылка СОХРАНЯЕТ премиум-эмодзи;
      4. удаляем временный пост в лог-чате.

    Плашка «Переслано от <бот>» в канале остаётся — это ограничение Telegram,
    убрать нельзя. Если GW_LOG_CHAT_ID не задан — шлём в канал напрямую (без премиума).

    Возвращает message_id пересланного поста в канале (для закрепа) или None.
    """
    from config import GW_LOG_CHAT_ID
    # текст с приглашением-ссылкой (жирным) внизу
    join_line = f'\n\n<b><a href="{join_url}">⭢ Участвовать ⭠</a></b>'
    body = gw["announce_text"] + join_line

    if not GW_LOG_CHAT_ID:
        # посредника нет — прямая отправка в канал (премиум не сохранится)
        if photo:
            m = await bot.send_photo(channel_id, photo, caption=body)  # noqa: ui
        else:
            m = await bot.send_message(channel_id, body)  # noqa: ui
        return m

    # 1. постим в лог-чат (премиум работает, идём через ui/render)
    if photo and len(body) <= CAPTION_LIMIT:
        tmp = await ui.send_photo(bot, GW_LOG_CHAT_ID, photo, body, apply_free=False)
    elif photo:
        with contextlib.suppress(Exception):
            await bot.send_photo(GW_LOG_CHAT_ID, photo)  # noqa: ui
        tmp = await ui.send(bot, GW_LOG_CHAT_ID, body, apply_free=False)
    else:
        tmp = await ui.send(bot, GW_LOG_CHAT_ID, body, apply_free=False)

    # 2. forward в канал — премиум сохраняется
    try:
        fwd = await bot.forward_message(channel_id, GW_LOG_CHAT_ID, tmp.message_id)
    except Exception as e:
        log.warning("forward в канал %s не удался: %s", channel_id, e)
        fwd = None
    # 3. удаляем временный пост в лог-чате (успех пересылки не обязателен для чистки)
    with contextlib.suppress(Exception):
        await bot.delete_message(GW_LOG_CHAT_ID, tmp.message_id)
    return fwd


# Лимит подписи к фото у Telegram — 1024 символа. Длиннее — шлём фото отдельно,
# текст обычным сообщением (у него лимит 4096).
CAPTION_LIMIT = 1000


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
    photo = gw.get("announce_photo")
    join_url = await _join_url(bot, gid)
    for ch in chats:
        try:
            if ch["kind"] == "channel":
                m = await _publish_to_channel(bot, ch["chat_id"], gw, photo, join_url)
                if m is None:
                    errors.append(f"{ch['title']}: не удалось опубликовать")
                    continue
            else:
                # в чат — через render, премиум-эмодзи работают
                if photo and len(gw["announce_text"]) <= CAPTION_LIMIT:
                    m = await ui.send_photo(bot, ch["chat_id"], photo,
                                            gw["announce_text"], apply_free=False, reply_markup=kbd)
                elif photo:
                    # текст длиннее лимита подписи — фото отдельно, текст с кнопкой
                    with contextlib.suppress(Exception):
                        await bot.send_photo(ch["chat_id"], photo)  # noqa: ui
                    m = await ui.send(bot, ch["chat_id"], gw["announce_text"],
                                      apply_free=False, reply_markup=kbd)
                else:
                    m = await ui.send(bot, ch["chat_id"], gw["announce_text"],
                                      apply_free=False, reply_markup=kbd)
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
    photo = gw.get("finish_photo")

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
                if photo:
                    m = await bot.send_photo(ch["chat_id"], photo, caption=full)  # noqa: ui
                else:
                    m = await bot.send_message(ch["chat_id"], full)  # noqa: ui
            else:
                if photo:
                    m = await ui.send_photo(bot, ch["chat_id"], photo, full, apply_free=False)
                else:
                    m = await ui.send(bot, ch["chat_id"], full, apply_free=False)
            await db.gw_save_result_msg(gid, ch["chat_id"], m.message_id)
            with contextlib.suppress(Exception):
                await bot.pin_chat_message(ch["chat_id"], m.message_id,
                                           disable_notification=True)
        except Exception as e:
            log.warning("результаты в %s не опубликованы: %s", ch["chat_id"], e)
