"""
Рендер сообщений с премиум-эмодзи.

Проблема: Telegram не даёт использовать parse_mode и entities одновременно.
Как только нам нужны custom_emoji (а это entity), HTML-разметка перестаёт работать —
и весь <b> превращается в текст. Поэтому HTML парсим сами и отдаём готовый
список entities: и жирный, и премиум-эмодзи в одном списке.

Все offset считаются в UTF-16 code units, как требует Bot API. Эмодзи вне BMP
занимают 2 единицы, а не 1 — на этом обычно всё и ломается.
"""
import logging
from html.parser import HTMLParser

from aiogram.types import MessageEntity

log = logging.getLogger(__name__)

TAGS = {
    "b": "bold", "strong": "bold",
    "i": "italic", "em": "italic",
    "u": "underline", "ins": "underline",
    "s": "strikethrough", "del": "strikethrough", "strike": "strikethrough",
    "code": "code", "pre": "pre",
    "tg-spoiler": "spoiler", "blockquote": "blockquote",
}


def u16(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


class _Parser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.text = ""
        self.entities: list[MessageEntity] = []
        self._stack: list[tuple[str, int, dict]] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self._stack.append((tag, u16(self.text), {"url": dict(attrs).get("href")}))
        elif tag in TAGS:
            self._stack.append((tag, u16(self.text), {}))

    def handle_endtag(self, tag):
        for i in range(len(self._stack) - 1, -1, -1):
            if self._stack[i][0] == tag:
                t, start, extra = self._stack.pop(i)
                length = u16(self.text) - start
                if length > 0:
                    typ = "text_link" if t == "a" else TAGS[t]
                    self.entities.append(
                        MessageEntity(type=typ, offset=start, length=length, **extra))
                break

    def handle_data(self, data):
        self.text += data


def _emoji_entities(text: str, emoji_map: dict[str, str]) -> list[MessageEntity]:
    """emoji_map: {символ: custom_emoji_id}. Ищем каждый символ во всех вхождениях."""
    out = []
    for ch, cid in emoji_map.items():
        if not ch or not cid:
            continue
        start = 0
        while True:
            i = text.find(ch, start)
            if i == -1:
                break
            out.append(MessageEntity(type="custom_emoji", offset=u16(text[:i]),
                                     length=u16(ch), custom_emoji_id=str(cid)))
            start = i + len(ch)
    return out


def parse_free_pair(text: str, entities) -> tuple[str, str, str]:
    """
    Разбирает сообщение вида "<обычное эмодзи><премиум-эмодзи>".
    -> (обычный_символ, custom_emoji_id, ошибка)

    Границу берём из entity.offset, а НЕ поиском фолбэк-символа в тексте:
    если у премиума фолбэк совпадает с обычным эмодзи (🚩 + премиум-🚩),
    поиск найдёт первое вхождение и решит, что обычного эмодзи нет.
    Берём ПОСЛЕДНИЙ custom_emoji — тогда работает и когда оба эмодзи премиумные.
    """
    ces = [e for e in (entities or []) if e.type == "custom_emoji"]
    if not ces:
        return "", "", "no_premium"
    ce = ces[-1]
    b = text.encode("utf-16-le")
    plain = b[:ce.offset * 2].decode("utf-16-le").strip()
    if not plain:
        return "", "", "no_plain"
    if len(plain) > 8:
        return "", "", "too_long"
    return plain, ce.custom_emoji_id, ""


def render(html_text: str, emoji_map: dict[str, str] | None = None):
    """
    -> (text, entities | None)
    entities=None означает «премиум не нужен» — вызывающий код просто шлёт с parse_mode=HTML.
    """
    emoji_map = {k: v for k, v in (emoji_map or {}).items() if k and v}
    if not emoji_map or not any(ch in html_text for ch in emoji_map):
        return html_text, None
    p = _Parser()
    p.feed(html_text)
    p.close()
    ents = p.entities + _emoji_entities(p.text, emoji_map)
    ents.sort(key=lambda e: (e.offset, e.length))
    return p.text, ents


async def send(bot, chat_id: int, html_text: str, emoji_map=None, **kw):
    """Шлём с премиум-эмодзи; если Telegram их не принял — падаем на обычный HTML.

    Премиум доступен, если у владельца бота есть Telegram Premium (Bot API 9.4,
    09.02.2026) либо боту куплен юзернейм на Fragment. Фолбэк — на случай, если
    ни то ни другое, или эмодзи из недоступного набора.
    """
    text, ents = render(html_text, emoji_map)
    if ents is None:
        return await bot.send_message(chat_id, html_text, **kw)
    try:
        return await bot.send_message(chat_id, text, entities=ents, parse_mode=None, **kw)
    except Exception as e:
        log.warning("премиум-эмодзи отклонены (%s), шлю обычным HTML", e)
        return await bot.send_message(chat_id, html_text, **kw)


async def edit(message, html_text: str, emoji_map=None, **kw):
    text, ents = render(html_text, emoji_map)
    if ents is None:
        return await message.edit_text(html_text, **kw)
    try:
        return await message.edit_text(text, entities=ents, parse_mode=None, **kw)
    except Exception as e:
        log.warning("премиум-эмодзи отклонены (%s), редактирую обычным HTML", e)
        return await message.edit_text(html_text, **kw)


async def reply(message, html_text: str, emoji_map=None, **kw):
    text, ents = render(html_text, emoji_map)
    if ents is None:
        return await message.reply(html_text, **kw)
    try:
        return await message.reply(text, entities=ents, parse_mode=None, **kw)
    except Exception as e:
        log.warning("премиум-эмодзи отклонены (%s), отвечаю обычным HTML", e)
        return await message.reply(html_text, **kw)
