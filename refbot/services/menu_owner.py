"""
Владение меню-сообщениями в группах: чтобы чужой не мог нажать кнопки чужого меню.

Когда меню открывается в группе, регистрируем (chat_id, message_id) -> owner_id.
Callback-middleware проверяет: если нажали помеченное сообщение и это не владелец —
блокируем со всплывашкой. Личка не проверяется (там один собеседник).

Хранилище — в памяти (dict). При рестарте очищается: старые меню перестанут
проверяться (можно пересоздать /start). Чтобы не расти бесконечно, держим預ел
записей и вычищаем самые старые.
"""
import time

# (chat_id, message_id) -> (owner_id, ts)
_owners: dict[tuple[int, int], tuple[int, float]] = {}
_MAX = 5000            # максимум записей в памяти
_TTL = 24 * 3600       # запись живёт сутки


def register(chat_id: int, message_id: int, owner_id: int) -> None:
    """Пометить сообщение как меню, принадлежащее owner_id."""
    _gc()
    _owners[(chat_id, message_id)] = (owner_id, time.time())


def owner_of(chat_id: int, message_id: int) -> int | None:
    """Владелец меню-сообщения или None, если не помечено/просрочено."""
    rec = _owners.get((chat_id, message_id))
    if not rec:
        return None
    owner, ts = rec
    if time.time() - ts > _TTL:
        _owners.pop((chat_id, message_id), None)
        return None
    return owner


def _gc() -> None:
    """Чистка: убрать просроченные, при переполнении — самые старые."""
    now = time.time()
    if len(_owners) < _MAX:
        # дешёвая чистка просроченных только когда карта подросла
        if len(_owners) > _MAX // 2:
            for k in [k for k, (_, ts) in _owners.items() if now - ts > _TTL]:
                _owners.pop(k, None)
        return
    # переполнение — выкидываем старые по времени
    for k, _ in sorted(_owners.items(), key=lambda kv: kv[1][1])[:_MAX // 5]:
        _owners.pop(k, None)


# ---------------- middleware ----------------
from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery


class MenuOwnerMiddleware(BaseMiddleware):
    """
    Блокирует нажатие на кнопки чужого меню в группах.
    Проверяем только сообщения, помеченные register() (меню в группе). Всё
    остальное (личка, непомеченные сообщения) пропускаем без изменений.
    """
    async def __call__(self, handler, event, data):
        cb = event if isinstance(event, CallbackQuery) else None
        if cb is not None and cb.message is not None:
            chat = cb.message.chat
            if chat.type in ("group", "supergroup"):
                owner = owner_of(chat.id, cb.message.message_id)
                if owner is not None and owner != cb.from_user.id:
                    await cb.answer("Это не твоё меню. Открой своё через /start.",
                                    show_alert=True)
                    return  # не пускаем дальше
        return await handler(event, data)
