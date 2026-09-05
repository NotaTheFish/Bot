"""
Обёртка над Rich Messages (Bot API 10.3): «кнопки внутри сообщений».
Позволяет слать сообщение, где под строками текста стоят кнопки (RichMessageButton),
с автоматическим откатом на обычные инлайн-кнопки, если rich не сработает
(старый клиент, ошибка API). Так фича новая, но бот не ломается.

Использование:
  rows = [ (html_текста_строки, [(label, callback_data), ...]), ... ]
  await send_rich(bot, chat_id, header_html, rows, fallback_kb)
"""
import logging

log = logging.getLogger("refbot")


async def _premiumize(html_text: str) -> str:
    """Обернуть премиум-символы в <tg-emoji> теги, чтобы rich-сообщение показало
    их премиум-версии (Telegram HTML поддерживает <tg-emoji emoji-id=...>)."""
    from services import settings
    em = await settings.emoji_map()
    if not em:
        return html_text
    for sym, cid in em.items():
        if sym and sym in html_text:
            html_text = html_text.replace(
                sym, f'<tg-emoji emoji-id="{cid}">{sym}</tg-emoji>')
    return html_text


async def _rich_text_parts(text: str):
    """Разбить строку на массив RichText: обычный текст + премиум-эмодзи
    (RichTextCustomEmoji). Понимает и готовые <tg-emoji emoji-id="..."> теги
    (с их конкретным id), и символы из общего emoji_map."""
    from aiogram.types import RichTextCustomEmoji
    from services import settings
    import re
    em = await settings.emoji_map()

    # 1) сначала вытащим готовые <tg-emoji emoji-id="ID">СИМВОЛ</tg-emoji> теги —
    #    их id конкретны и не должны подменяться общим маппингом.
    parts = []
    pos = 0
    tag_re = re.compile(r'<tg-emoji emoji-id="(\d+)">([^<]+)</tg-emoji>')
    for m in tag_re.finditer(text):
        # текст до тега (с вырезанием прочих html) — разберём по emoji_map
        before = re.sub(r"<[^>]+>", "", text[pos:m.start()])
        parts.extend(await _split_by_map(before, em))
        # сам премиум-тег с конкретным id
        parts.append(RichTextCustomEmoji(custom_emoji_id=m.group(1),
                                         alternative_text=m.group(2)))
        pos = m.end()
    # хвост после последнего тега
    tail = re.sub(r"<[^>]+>", "", text[pos:])
    parts.extend(await _split_by_map(tail, em))

    if not parts:
        return re.sub(r"<[^>]+>", "", text)
    if len(parts) == 1 and isinstance(parts[0], str):
        return parts[0]
    return parts


async def _split_by_map(clean: str, em: dict):
    """Разбить чистый текст на [str, RichTextCustomEmoji, ...] по символам emoji_map."""
    from aiogram.types import RichTextCustomEmoji
    if not clean:
        return []
    if not em:
        return [clean]
    out = []
    buf = ""
    i = 0
    while i < len(clean):
        matched = None
        for sym in em:
            if sym and clean.startswith(sym, i):
                matched = sym
                break
        if matched:
            if buf:
                out.append(buf); buf = ""
            out.append(RichTextCustomEmoji(custom_emoji_id=em[matched], alternative_text=matched))
            i += len(matched)
        else:
            buf += clean[i]; i += 1
    if buf:
        out.append(buf)
    return out


async def _build_rich(header_html: str, rows: list[tuple[str, list[tuple[str, str]]]]):
    """Собрать InputRichMessage: заголовок + для каждой строки параграф-текст (с
    премиум-эмодзи прямо в тексте через RichTextCustomEmoji) и блок кнопок под ним."""
    from aiogram.types import (InputRichMessage, InputRichBlockButtons,
                               InputRichBlockParagraph, RichMessageButton)
    blocks = []
    for text_html, btns in rows:
        # текст строки с премиум-эмодзи в самом тексте (массив RichText)
        parts = await _rich_text_parts(text_html)
        blocks.append(InputRichBlockParagraph(text=parts))
        if btns:
            rbtns = [RichMessageButton(text=lbl, callback_data=cb) for lbl, cb in btns]
            blocks.append(InputRichBlockButtons(buttons=rbtns))
    # заголовок тоже с премиум в тексте
    header_parts = await _rich_text_parts(header_html)
    if isinstance(header_parts, str):
        return InputRichMessage(html=header_html, blocks=blocks)
    # если в заголовке есть премиум — вынесем его первым параграфом
    blocks.insert(0, InputRichBlockParagraph(text=header_parts))
    return InputRichMessage(blocks=blocks)


async def send_rich(bot, chat_id: int, header_html: str,
                    rows: list[tuple[str, list[tuple[str, str]]]],
                    fallback_kb=None, reply_markup=None, **kw):
    """
    Отправить rich-сообщение с кнопками в тексте. При ошибке — обычное сообщение
    с fallback_kb (InlineKeyboardMarkup). Возвращает (message, is_rich).
    reply_markup — доп. обычная клавиатура снизу (пагинация и т.п.) для rich-режима.
    """
    try:
        rm = await _build_rich(header_html, rows)
        msg = await bot.send_rich_message(chat_id=chat_id, rich_message=rm,
                                          reply_markup=reply_markup, **kw)
        return msg, True
    except Exception as e:
        log.warning("rich message не удался (%s), откат на инлайн-кнопки", type(e).__name__)
        from services import ui
        full = header_html
        if rows:
            full += "\n\n" + "\n".join(t for t, _ in rows)
        msg = await ui.send(bot, chat_id, full, reply_markup=fallback_kb)
        return msg, False


async def edit_rich(bot, chat_id: int, message_id: int, header_html: str,
                    rows: list[tuple[str, list[tuple[str, str]]]],
                    fallback_kb=None, is_rich: bool = True, reply_markup=None, **kw):
    """
    Отредактировать rich-сообщение. Если было rich — пробуем edit_message_text с
    rich_message; иначе (или при ошибке) — обычное редактирование с fallback_kb.
    """
    if is_rich:
        try:
            rm = await _build_rich(header_html, rows)
            return await bot.edit_message_text(
                chat_id=chat_id, message_id=message_id, rich_message=rm,
                reply_markup=reply_markup, **kw)
        except Exception as e:
            log.warning("rich edit не удался (%s), откат", type(e).__name__)
    from services import render, settings
    em = await settings.emoji_map()
    full = header_html
    if rows:
        full += "\n\n" + "\n".join(t for t, _ in rows)
    return await render.edit_by_id(bot, chat_id, message_id, full, em, reply_markup=fallback_kb)


def supports_rich() -> bool:
    """Есть ли метод send_rich_message в текущем aiogram."""
    try:
        from aiogram import Bot
        return hasattr(Bot, "send_rich_message")
    except Exception:
        return False
