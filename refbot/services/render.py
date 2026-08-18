"""
Рендер сообщений с премиум-эмодзи.

Проблема: Telegram не даёт использовать parse_mode и entities одновременно.
Как только нам нужны custom_emoji (а это entity), HTML-разметка перестаёт работать —
и весь <b> превращается в текст. Поэтому HTML парсим сами и отдаём готовый
список entities: и жирный, и премиум-эмодзи в одном списке.

Все offset считаются в UTF-16 code units, как требует Bot API. Эмодзи вне BMP
занимают 2 единицы, а не 1 — на этом обычно всё и ломается.
"""
import asyncio
import logging
from html.parser import HTMLParser

from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
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
        elif tag == "tg-emoji":
            # премиум-эмодзи, вставленный самим пользователем в текст.
            # <tg-emoji emoji-id="123">🎁</tg-emoji> -> custom_emoji entity.
            eid = dict(attrs).get("emoji-id")
            self._stack.append((tag, u16(self.text), {"custom_emoji_id": eid}))
        elif tag in TAGS:
            self._stack.append((tag, u16(self.text), {}))

    def handle_endtag(self, tag):
        for i in range(len(self._stack) - 1, -1, -1):
            if self._stack[i][0] == tag:
                t, start, extra = self._stack.pop(i)
                length = u16(self.text) - start
                if length > 0:
                    if t == "a":
                        typ = "text_link"
                    elif t == "tg-emoji":
                        typ = "custom_emoji"
                    else:
                        typ = TAGS[t]
                    self.entities.append(
                        MessageEntity(type=typ, offset=start, length=length, **extra))
                break

    def handle_data(self, data):
        self.text += data


def _emoji_entities(text: str, emoji_map: dict[str, str],
                    protected: list = None) -> list[MessageEntity]:
    """
    emoji_map: {символ: custom_emoji_id}. Ищем каждый символ во всех вхождениях.
    protected: список (start, end) в UTF-16 — диапазоны пользовательских премиум-эмодзи,
    куда замену НЕ ставим (fallback-символ внутри чужого премиума не трогаем).
    """
    protected = protected or []
    out = []
    for ch, cid in emoji_map.items():
        if not ch or not cid:
            continue
        start = 0
        while True:
            i = text.find(ch, start)
            if i == -1:
                break
            off = u16(text[:i])
            end = off + u16(ch)
            # пропускаем, если совпадение попадает в защищённый премиум-диапазон
            inside = any(off < p_end and end > p_start for p_start, p_end in protected)
            if not inside:
                out.append(MessageEntity(type="custom_emoji", offset=off,
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


def render(html_text: str, emoji_map: dict[str, str] | None = None,
           apply_free: bool = True):
    """
    -> (text, entities | None)
    entities=None означает «премиум не нужен» — вызывающий код просто шлёт с parse_mode=HTML.

    apply_free — применять ли свободные замены (emoji_map). Для текстов, куда
    пользователь САМ вставил премиум (название/объявление/завершение розыгрыша),
    можно явно False. Но и при True замены НЕ трогают символы внутри пользовательских
    <tg-emoji> — их fallback-символы защищены (см. ниже), иначе чужой премиум подменял бы
    твой. Пользовательский премиум всегда важнее свободных замен.
    """
    emoji_map = {k: v for k, v in (emoji_map or {}).items() if k and v} if apply_free else {}
    # парсим, если есть свободные замены ИЛИ пользователь вставил премиум сам (<tg-emoji>)
    has_user_premium = "<tg-emoji" in html_text
    if not has_user_premium and (not emoji_map or not any(ch in html_text for ch in emoji_map)):
        return html_text, None
    p = _Parser()
    p.feed(html_text)
    p.close()
    # диапазоны, занятые пользовательскими премиум-эмодзи — сюда свободные замены НЕ лезут,
    # иначе fallback-символ внутри твоего премиума подменится чужим премиумом (баг).
    protected = [(e.offset, e.offset + e.length) for e in p.entities
                 if e.type == "custom_emoji"]
    ents = p.entities + _emoji_entities(p.text, emoji_map, protected)
    ents.sort(key=lambda e: (e.offset, e.length))
    return p.text, ents


async def _safe(primary, fallback, retries: int = 2):
    """
    Один вызов Telegram, переживающий флуд-контроль и умеющий откатиться на HTML.

    primary   — корутина-фабрика с премиум-entities (её можно вызвать несколько раз);
    fallback  — корутина-фабрика с обычным HTML.

    TelegramRetryAfter — это НЕ «бот сломался», это Telegram просит подождать N секунд.
    Раньше это исключение ловил общий except и пытался переслать заново -> падало
    наружу и роняло обработчик (та самая «зависшая» рулетка). Теперь ждём и повторяем,
    а на HTML откатываемся только если Telegram реально не принял entities.
    """
    for attempt in range(retries + 1):
        try:
            return await primary()
        except TelegramRetryAfter as e:
            if attempt < retries:
                await asyncio.sleep(e.retry_after + 1)
                continue
            # исчерпали попытки — пусть флуд всплывёт, его словит глобальный обработчик
            raise
        except TelegramBadRequest as e:
            msg = str(e).lower()
            # «message is not modified» — контент идентичен предыдущему (повторное
            # нажатие той же кнопки). Это НЕ отклонение премиум-entities! Фолбэк на
            # обычный HTML здесь сбрасывал бы премиум-эмодзи. Просто выходим тихо.
            if "not modified" in msg:
                return None
            # entities не приняты (нет премиума / чужой набор) — честный фолбэк
            log.warning("премиум-эмодзи отклонены (%s), шлю обычным HTML", e)
            try:
                return await fallback()
            except TelegramRetryAfter as e2:
                await asyncio.sleep(e2.retry_after + 1)
                return await fallback()
            except TelegramBadRequest as e3:
                # фолбэк тоже упал на «not modified» — тоже тихо выходим
                if "not modified" in str(e3).lower():
                    return None
                raise


async def send(bot, chat_id: int, html_text: str, emoji_map=None, apply_free=True, **kw):
    """Шлём с премиум-эмодзи; Telegram их не принял — падаем на обычный HTML.

    Премиум доступен, если у владельца бота есть Telegram Premium (Bot API 9.4,
    09.02.2026) либо боту куплен юзернейм на Fragment.
    """
    text, ents = render(html_text, emoji_map, apply_free)
    if ents is None:
        return await _safe(lambda: bot.send_message(chat_id, html_text, **kw),
                           lambda: bot.send_message(chat_id, html_text, **kw))
    return await _safe(
        lambda: bot.send_message(chat_id, text, entities=ents, parse_mode=None, **kw),
        lambda: bot.send_message(chat_id, html_text, **kw))


async def send_photo(bot, chat_id: int, photo: str, caption: str, emoji_map=None, apply_free=True, **kw):
    """
    Фото с подписью. Подпись рендерится с премиум-эмодзи (caption_entities), при
    отказе Telegram — обычный HTML-caption. photo — file_id.
    """
    text, ents = render(caption, emoji_map, apply_free)
    if ents is None:
        return await _safe(
            lambda: bot.send_photo(chat_id, photo, caption=caption, **kw),
            lambda: bot.send_photo(chat_id, photo, caption=caption, **kw))
    return await _safe(
        lambda: bot.send_photo(chat_id, photo, caption=text,
                               caption_entities=ents, parse_mode=None, **kw),
        lambda: bot.send_photo(chat_id, photo, caption=caption, **kw))


async def edit(message, html_text: str, emoji_map=None, **kw):
    text, ents = render(html_text, emoji_map)
    if ents is None:
        return await _safe(lambda: message.edit_text(html_text, **kw),
                           lambda: message.edit_text(html_text, **kw))
    return await _safe(
        lambda: message.edit_text(text, entities=ents, parse_mode=None, **kw),
        lambda: message.edit_text(html_text, **kw))


async def reply(message, html_text: str, emoji_map=None, **kw):
    text, ents = render(html_text, emoji_map)
    if ents is None:
        return await _safe(lambda: message.reply(html_text, **kw),
                           lambda: message.reply(html_text, **kw))
    return await _safe(
        lambda: message.reply(text, entities=ents, parse_mode=None, **kw),
        lambda: message.reply(html_text, **kw))
