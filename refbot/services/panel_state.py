"""
Хранилище id последней «догоняющей» панели на чат (в ЛС — chat_id == user_id).

В памяти процесса. При перезапуске теряется — не критично: в худшем случае старая
панель не удалится (останется висеть выше), новая всё равно появится внизу. UX от
этого не ломается, поэтому БД не задействуем.
"""
_last: dict[int, int] = {}


def get(chat_id: int) -> int | None:
    return _last.get(chat_id)


def set(chat_id: int, msg_id: int) -> None:
    _last[chat_id] = msg_id


def clear(chat_id: int) -> None:
    _last.pop(chat_id, None)
