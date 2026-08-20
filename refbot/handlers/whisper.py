"""
Шёпот в чате: приватно отправить сообщение конкретному человеку.

Два пути:
  1) /secret <@username или ID> текст   — латиница, регистрируется как ЭФЕМЕРНАЯ
     команда (ввод невидимый для чата). Получатель — из текста.
  2) !секрет текст  (в ответ на сообщение человека) — кириллица, обычная команда;
     получатель из reply; исходную команду бот удаляет.

Получатель получает эфемерное сообщение «💬 [автор] шепнул тебе: текст» — видит
только он. Бот-админ имеет право слать эфемерное любому участнику.

Резолв @username -> tg_id через таблицу rb_usernames (кого бот видел в чате).
Её наполняет UsernameMiddleware на каждом сообщении.
"""
import contextlib
import logging

import time as _time
import uuid

from aiogram import BaseMiddleware, F, Router
from aiogram.filters import Command
from aiogram.types import (CallbackQuery, InlineQuery,
                           InlineKeyboardButton, InlineKeyboardMarkup,
                           InlineQueryResultArticle, InputTextMessageContent, Message)

import db
from services import ui

router = Router()
log = logging.getLogger("whisper")

# inline-шёпоты: token -> данные. Секрет живёт в памяти, читается по кнопке.
# TTL — час, чистим лениво при обращении (чтобы не копить).
_WHISPER_TTL = 3600
_whispers: dict[str, dict] = {}


def _gc_whispers():
    now = _time.time()
    dead = [t for t, w in _whispers.items() if now - w["created"] > _WHISPER_TTL]
    for t in dead:
        _whispers.pop(t, None)


class UsernameMiddleware(BaseMiddleware):
    """Запоминает username -> tg_id на каждом сообщении в группе (для резолва в шёпотах)."""
    async def __call__(self, handler, event: Message, data: dict):
        with contextlib.suppress(Exception):
            u = getattr(event, "from_user", None)
            if u and not u.is_bot and u.username and \
                    getattr(event, "chat", None) and event.chat.type in ("group", "supergroup"):
                await db.remember_username(u.username, u.id)
        return await handler(event, data)


def _is_group(msg) -> bool:
    return msg.chat.type in ("group", "supergroup")


async def _send_whisper(msg: Message, receiver_id: int, receiver_label: str, text: str):
    """Отправить шёпот получателю эфемерно. Возвращает True при успехе."""
    author = msg.from_user
    author_name = f"@{author.username}" if author.username else author.first_name
    body = f"💬 <b>{author_name}</b> шепнул тебе:\n\n{text}"
    sent = await ui.send_ephemeral(msg.bot, msg.chat.id, receiver_id, body)
    if sent is None or isinstance(sent, ui._NotEphemeral):
        # эфемерка не сработала — удалим публичный дубль, если был
        if isinstance(sent, ui._NotEphemeral):
            with contextlib.suppress(Exception):
                await sent.msg.delete()
        return False
    return True


async def _fail_to_author(msg: Message, text: str):
    """Сообщить автору об ошибке эфемерно (в группе) или обычно."""
    if _is_group(msg):
        s = await ui.send_ephemeral(msg.bot, msg.chat.id, msg.from_user.id, text)
        if s is not None and not isinstance(s, ui._NotEphemeral):
            return
        if isinstance(s, ui._NotEphemeral):
            with contextlib.suppress(Exception):
                await s.msg.delete()
    with contextlib.suppress(Exception):
        await ui.reply(msg, text)


# ---------------- ПУТЬ 1: /secret <получатель> текст (эфемерная команда) ----------------
@router.message(Command("secret"), F.chat.type.in_({"group", "supergroup"}))
async def cmd_secret(msg: Message):
    # текст после /secret
    parts = (msg.text or "").split(maxsplit=2)
    # parts[0]=/secret, parts[1]=получатель, parts[2]=текст
    if len(parts) < 3:
        return await _fail_to_author(msg,
            "Формат: <code>/secret @username текст</code> или <code>/secret ID текст</code>")
    target_raw, whisper_text = parts[1], parts[2]

    receiver_id = None
    if target_raw.startswith("@"):
        receiver_id = await db.resolve_username(target_raw)
        if receiver_id is None:
            return await _fail_to_author(msg,
                f"Не знаю {target_raw}. Он должен хоть раз написать в чате, или укажи числовой ID.")
    else:
        try:
            receiver_id = int(target_raw)
        except ValueError:
            return await _fail_to_author(msg,
                "Получатель — @username или числовой ID.")

    ok = await _send_whisper(msg, receiver_id, target_raw, whisper_text)
    # /secret эфемерная — исходное сообщение и так невидимо; на всякий случай пробуем удалить
    with contextlib.suppress(Exception):
        await msg.delete()
    if not ok:
        await _fail_to_author(msg,
            "Не удалось доставить шёпот (получатель офлайн или недоступен).")


# ---------------- ПУТЬ 2: !секрет текст (reply, кириллица) ----------------
@router.message(F.text.regexp(r"(?i)^!секрет(\s|$)"), F.chat.type.in_({"group", "supergroup"}))
async def cmd_secret_reply(msg: Message):
    # получатель — из reply
    if not msg.reply_to_message or not msg.reply_to_message.from_user:
        return await _fail_to_author(msg,
            "Ответь на сообщение человека, которому хочешь шепнуть.")
    receiver = msg.reply_to_message.from_user
    if receiver.is_bot:
        return await _fail_to_author(msg, "Боту шептать нельзя.")
    # текст после !секрет
    text = (msg.text or "")[len("!секрет"):].strip()
    if not text:
        with contextlib.suppress(Exception):
            await msg.delete()
        return await _fail_to_author(msg, "После !секрет напиши сам текст шёпота.")

    label = f"@{receiver.username}" if receiver.username else receiver.first_name
    ok = await _send_whisper(msg, receiver.id, label, text)
    # удаляем исходную команду, чтобы секрет не остался в чате
    with contextlib.suppress(Exception):
        await msg.delete()
    if not ok:
        await _fail_to_author(msg,
            "Не удалось доставить шёпот (получатель офлайн или недоступен).")


# ==================== INLINE-ШЁПОТ + КНОПКА «ПРОЧИТАТЬ» ====================
# @cosdacasino_bot секрет @username текст  -> карточка -> в чат «шёпот для X» +
# кнопка «Прочитать». Жмёт получатель — видит секрет всплывашкой (только ему).
# Почему кнопка, а не сразу эфемерно: inline-выбор (chosen_inline_result) НЕ даёт
# боту chat_id, поэтому напрямую эфемерное получателю не отправить. Кнопка решает:
# callback от нажатия содержит и chat_id, и адресата.

def _parse_whisper_query(query: str):
    """
    'секрет @username текст' или 'секрет 12345 текст' -> (target_raw, text) или None.
    Первое слово 'секрет' уже гарантировано фильтром, но проверим ещё раз.
    """
    q = (query or "").strip()
    # убираем ведущее «секрет»
    low = q.lower()
    if not low.startswith("секрет"):
        return None
    rest = q[len("секрет"):].strip()
    parts = rest.split(maxsplit=1)
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


@router.inline_query(F.query.regexp(r"(?i)^\s*секрет\b"))
async def on_inline_whisper(q: InlineQuery):
    parsed = _parse_whisper_query(q.query)
    if not parsed:
        art = InlineQueryResultArticle(
            id="whisper_help",
            title="🔒 Шёпот: секрет @username текст",
            description="Пример: секрет @vasya привет — увидит только он",
            input_message_content=InputTextMessageContent(
                message_text="Формат шёпота: <code>секрет @username текст</code>",
                parse_mode="HTML"))
        return await q.answer([art], cache_time=1, is_personal=True)

    target_raw, text = parsed
    # резолвим получателя (для превью — только имя; фактическую проверку сделаем при нажатии)
    receiver_id = None
    if target_raw.startswith("@"):
        receiver_id = await db.resolve_username(target_raw)
    else:
        try:
            receiver_id = int(target_raw)
        except ValueError:
            receiver_id = None

    token = uuid.uuid4().hex[:12]
    _gc_whispers()
    _whispers[token] = {
        "author_id": q.from_user.id,
        "author_name": f"@{q.from_user.username}" if q.from_user.username else q.from_user.first_name,
        "receiver_id": receiver_id,      # может быть None, если @username не резолвится
        "receiver_raw": target_raw,
        "text": text,
        "created": _time.time(),
    }

    author_name = _whispers[token]["author_name"]
    card_text = (f"🔒 <b>{author_name}</b> шепнул для <b>{target_raw}</b>\n"
                 f"<i>Прочитать может только получатель.</i>")
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💬 Прочитать", callback_data=f"wread:{token}")
    ]])
    result = InlineQueryResultArticle(
        id=token,
        title=f"🔒 Шепнуть {target_raw}",
        description=(text[:60] + "…") if len(text) > 60 else text,
        input_message_content=InputTextMessageContent(
            message_text=card_text, parse_mode="HTML"),
        reply_markup=kb)
    await q.answer([result], cache_time=0, is_personal=True)


@router.callback_query(F.data.startswith("wread:"))
async def cb_whisper_read(c: CallbackQuery):
    token = c.data.split(":", 1)[1]
    w = _whispers.get(token)
    if not w:
        return await c.answer("Шёпот истёк или уже недоступен.", show_alert=True)

    uid = c.from_user.id
    # получатель определён по id (если резолвился) — только он может прочитать.
    # если резолва не было (receiver_id None) — сверяем по username из callback.
    allowed = False
    if w["receiver_id"] is not None:
        allowed = uid == w["receiver_id"]
    else:
        # не было id: разрешаем, если username совпадает с адресатом
        raw = w["receiver_raw"].lstrip("@").lower()
        uname = (c.from_user.username or "").lower()
        allowed = bool(uname) and uname == raw
        if allowed:
            # запомним id на будущее
            with contextlib.suppress(Exception):
                await db.remember_username(uname, uid)

    # автор тоже может ткнуть — ему скажем, что это не для него (не палим текст)
    if uid == w["author_id"] and not allowed:
        return await c.answer("Это твой шёпот — его прочитает получатель.", show_alert=True)

    if not allowed:
        return await c.answer("Этот шёпот не для тебя 🤫", show_alert=True)

    # показываем секрет всплывашкой — видит только нажавший
    await c.answer(f"💬 {w['author_name']} шепнул:\n\n{w['text']}", show_alert=True)


# Примечание: chosen_inline_result по нашему токену обрабатывать не нужно —
# карточка уже отправлена с кнопкой. Общий обработчик inline_grant.on_chosen
# безопасно игнорирует наши токены (их нет в его _grants), так что отдельный
# хендлер здесь только создал бы конфликт (токены одинаковой формы 12-hex).
