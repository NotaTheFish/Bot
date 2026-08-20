"""
Единая точка отправки сообщений.

Раньше премиум-эмодзи надо было тащить руками в каждый вызов — половину мест
я и забыл. Здесь emoji_map подтягивается сам из настроек (кэш в памяти, дёшево),
поэтому любое сообщение, отправленное через ui.*, автоматически с премиумом.

Правило простое: в хендлерах НЕ вызываем message.edit_text / answer / send_message
напрямую. Только ui.edit / ui.answer / ui.send / ui.reply.
"""
from services import render, settings


async def _em():
    return await settings.emoji_map()


async def edit(message, html_text: str, **kw):
    return await render.edit(message, html_text, await _em(), **kw)


async def reply(message, html_text: str, **kw):
    return await render.reply(message, html_text, await _em(), **kw)


async def answer(message, html_text: str, **kw):
    """message.answer — ответ в тот же чат без реплая."""
    em = await _em()
    text, ents = render.render(html_text, em)
    if ents is None:
        return await message.answer(html_text, **kw)
    try:
        return await message.answer(text, entities=ents, parse_mode=None, **kw)
    except Exception:
        return await message.answer(html_text, **kw)


async def send(bot, chat_id: int, html_text: str, apply_free=True, **kw):
    return await render.send(bot, chat_id, html_text, await _em(), apply_free, **kw)


async def send_ephemeral(bot, chat_id: int, receiver_user_id: int, html_text: str, **kw):
    """
    Эфемерное сообщение в групповой чат — видит только receiver_user_id и бот.
    Возвращает Message (с ephemeral_message_id) или None при ошибке.

    ДИАГНОСТИКА: логируем реальный ответ/ошибку Telegram, чтобы видеть, почему
    эфемерка не срабатывает. Также проверяем, что Telegram действительно пометил
    сообщение эфемерным (ephemeral_message_id != None / message_id == 0) — если нет,
    значит receiver_user_id проигнорирован и это НЕ приватное сообщение.
    """
    import logging as _log
    lg = _log.getLogger("ephemeral")
    em = await _em()
    text, ents = render.render(html_text, em)
    try:
        if ents is not None:
            msg = await bot.send_message(chat_id, text, receiver_user_id=receiver_user_id,
                                         entities=ents, parse_mode=None, **kw)
        else:
            msg = await bot.send_message(chat_id, html_text,
                                         receiver_user_id=receiver_user_id, **kw)
    except Exception as e:
        lg.warning("эфемерка НЕ отправлена (ошибка API): %r", e)
        return None

    # проверяем, действительно ли сообщение эфемерное
    eph_id = getattr(msg, "ephemeral_message_id", None)
    mid = getattr(msg, "message_id", None)
    recv = getattr(msg, "receiver_user", None)
    lg.info("эфемерка отправлена: message_id=%s ephemeral_message_id=%s receiver_user=%s",
            mid, eph_id, recv)
    if eph_id is None and mid not in (0, None):
        # Telegram проигнорировал receiver_user_id и отправил ОБЫЧНОЕ (публичное).
        lg.warning("эфемерка НЕ сработала: пришло обычное сообщение (mid=%s). "
                   "Приватность НЕ обеспечена.", mid)
        # возвращаем спец-маркер, чтобы вызывающий знал: это публичное, надо удалить
        return _NotEphemeral(msg)
    return msg


class _NotEphemeral:
    """Маркер: Telegram отправил обычное сообщение вместо эфемерного."""
    def __init__(self, msg):
        self.msg = msg


async def edit_ephemeral(bot, chat_id: int, receiver_user_id: int,
                         ephemeral_message_id: int, html_text: str, **kw):
    """Редактировать эфемерное сообщение (для анимации). None при ошибке."""
    em = await _em()
    text, ents = render.render(html_text, em)
    try:
        if ents is not None:
            return await bot.edit_ephemeral_message_text(
                chat_id=chat_id, receiver_user_id=receiver_user_id,
                ephemeral_message_id=ephemeral_message_id,
                text=text, entities=ents, parse_mode=None, **kw)
        return await bot.edit_ephemeral_message_text(
            chat_id=chat_id, receiver_user_id=receiver_user_id,
            ephemeral_message_id=ephemeral_message_id, text=html_text, **kw)
    except Exception:
        return None


async def send_photo(bot, chat_id: int, photo: str, caption: str, apply_free=True, **kw):
    """Фото с премиум-подписью через render."""
    return await render.send_photo(bot, chat_id, photo, caption, await _em(), apply_free, **kw)


async def panel(bot, chat_id: int, html_text: str, old_msg_id: int = None, **kw):
    """
    «Догоняющая» инлайн-панель: показать её свежим сообщением ВНИЗУ чата.

    Порядок против мерцания: сначала ОТПРАВЛЯЕМ новое (появляется внизу), потом
    удаляем старое (оно выше, исчезновение глазу незаметно). Никогда не наоборот —
    иначе будет миг без панели.

    old_msg_id — id прежней панели, которую надо убрать (или None, если первая).
    Возвращает message_id новой панели — вызывающий сохраняет его для следующего раза.
    """
    import contextlib
    m = await send(bot, chat_id, html_text, **kw)
    if old_msg_id:
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id, old_msg_id)
    return m.message_id


async def repanel(bot, chat_id: int, html_text: str, **kw):
    """
    Догоняющая панель с автоведением id: берёт прежний id из panel_state, показывает
    новую панель внизу, удаляет старую, сохраняет новый id. Это то, что зовут
    «точечные» места (после осмысленного ответа бота), чтобы панель была внизу.
    """
    from services import panel_state
    old = panel_state.get(chat_id)
    new_id = await panel(bot, chat_id, html_text, old_msg_id=old, **kw)
    panel_state.set(chat_id, new_id)
    return new_id


# ---------- кнопки ----------
# Bot API 9.4 добавил icon_custom_emoji_id для InlineKeyboardButton.
# Иконка рисуется ОТДЕЛЬНО от текста, поэтому если премиум есть — эмодзи из
# текста убираем, иначе получим иконку и эмодзи подряд.
async def btn(kb, text: str, callback_data: str, slot: str | None = None, **kw):
    """
    kb — InlineKeyboardBuilder.
    slot — ключ из settings.EMOJI_SLOTS: эмодзи слота подставится в начало текста.
    Без слота просто пиши эмодзи в тексте: "📊 Сводка".

    Дальше — автоматика: если первый символ текста замаплен на премиум (через слот
    или через свободную замену), он превращается в icon_custom_emoji_id и убирается
    из текста. Ничего прописывать руками не надо.
    """
    if slot:
        text = f"{await settings.emoji(slot)} {text}"
    em = await settings.emoji_map()
    # длинные символы вперёд: ZWJ-эмодзи не должно перебиться коротким префиксом
    for ch in sorted(em, key=len, reverse=True):
        if text.startswith(ch):
            rest = text[len(ch):].lstrip()
            # если после выноса эмодзи текста не осталось (кнопка была ТОЛЬКО эмодзи),
            # ставим невидимый символ — иначе `rest or text` вернул бы эмодзи обратно
            # в текст, и он задвоился бы с premium-иконкой (баг двойного 💎).
            label = rest if rest else "\u2063"
            return kb.button(text=label, callback_data=callback_data,
                             icon_custom_emoji_id=em[ch], **kw)
    return kb.button(text=text, callback_data=callback_data, **kw)
