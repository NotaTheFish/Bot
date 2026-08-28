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


def _build_rich(header_html: str, rows: list[tuple[str, list[tuple[str, str]]]]):
    """Собрать InputRichMessage: header + для каждой строки текст-параграф и блок кнопок."""
    from aiogram.types import (InputRichMessage, InputRichBlockButtons,
                               RichMessageButton)
    # текст целиком в html; кнопки — блоками (по строке на игру)
    # для простоты: весь текст в html, а кнопки одним блоком снизу не годится —
    # нам нужна кнопка НАПРОТИВ каждой игры. Поэтому чередуем: параграф-текст + блок-кнопка.
    blocks = []
    for text_html, btns in rows:
        # блок кнопок этой строки
        rbtns = [RichMessageButton(text=lbl, callback_data=cb) for lbl, cb in btns]
        blocks.append(InputRichBlockButtons(buttons=rbtns))
    # заголовок и тексты строк — общим html; кнопки-блоки идут следом.
    # Telegram размещает блоки-кнопки в порядке; для «напротив каждой» надо перемежать
    # текстовыми блоками. Используем html как единый текст + кнопки под ним.
    full_html = header_html
    if rows:
        full_html += "\n\n" + "\n".join(t for t, _ in rows)
    return InputRichMessage(html=full_html, blocks=blocks)


async def send_rich(bot, chat_id: int, header_html: str,
                    rows: list[tuple[str, list[tuple[str, str]]]],
                    fallback_kb=None, **kw):
    """
    Отправить rich-сообщение с кнопками в тексте. При ошибке — обычное сообщение
    с fallback_kb (InlineKeyboardMarkup). Возвращает (message, is_rich).
    """
    try:
        rm = _build_rich(header_html, rows)
        msg = await bot.send_rich_message(chat_id=chat_id, rich_message=rm, **kw)
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
                    fallback_kb=None, is_rich: bool = True, **kw):
    """
    Отредактировать rich-сообщение. Если было rich — пробуем edit_message_text с
    rich_message; иначе (или при ошибке) — обычное редактирование с fallback_kb.
    """
    if is_rich:
        try:
            rm = _build_rich(header_html, rows)
            return await bot.edit_message_text(
                chat_id=chat_id, message_id=message_id, rich_message=rm, **kw)
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
