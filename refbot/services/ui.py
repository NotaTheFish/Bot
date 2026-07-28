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


async def send(bot, chat_id: int, html_text: str, **kw):
    return await render.send(bot, chat_id, html_text, await _em(), **kw)


async def send_photo(bot, chat_id: int, photo: str, caption: str, **kw):
    """Фото с премиум-подписью через render."""
    return await render.send_photo(bot, chat_id, photo, caption, await _em(), **kw)


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
            return kb.button(text=rest or text, callback_data=callback_data,
                             icon_custom_emoji_id=em[ch], **kw)
    return kb.button(text=text, callback_data=callback_data, **kw)
