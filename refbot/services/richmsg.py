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


def _build_rich(header_html: str, rows: list[tuple[str, list[tuple[str, str]]]]):
    """Собрать InputRichMessage: заголовок + для каждой строки параграф-текст и блок
    кнопок сразу под ним (текст игры оказывается прямо над её кнопкой «Вступить»)."""
    from aiogram.types import (InputRichMessage, InputRichBlockButtons,
                               InputRichBlockParagraph, RichMessageButton)
    import re
    blocks = []
    for text_html, btns in rows:
        # RichText — свой формат, HTML-теги не парсит: убираем их, оставляем чистый текст
        clean = re.sub(r"<[^>]+>", "", text_html)
        blocks.append(InputRichBlockParagraph(text=clean))
        if btns:
            rbtns = [RichMessageButton(text=lbl, callback_data=cb) for lbl, cb in btns]
            blocks.append(InputRichBlockButtons(buttons=rbtns))
    return InputRichMessage(html=header_html, blocks=blocks)


async def send_rich(bot, chat_id: int, header_html: str,
                    rows: list[tuple[str, list[tuple[str, str]]]],
                    fallback_kb=None, reply_markup=None, **kw):
    """
    Отправить rich-сообщение с кнопками в тексте. При ошибке — обычное сообщение
    с fallback_kb (InlineKeyboardMarkup). Возвращает (message, is_rich).
    reply_markup — доп. обычная клавиатура снизу (пагинация и т.п.) для rich-режима.
    """
    try:
        header_p = await _premiumize(header_html)
        rm = _build_rich(header_p, rows)
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
            header_p = await _premiumize(header_html)
            rm = _build_rich(header_p, rows)
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
