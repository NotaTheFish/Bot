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
