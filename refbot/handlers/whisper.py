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

from aiogram import BaseMiddleware, F, Router
from aiogram.filters import Command
from aiogram.types import Message

import db
from services import ui

router = Router()
log = logging.getLogger("whisper")


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
