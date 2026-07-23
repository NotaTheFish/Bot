"""
Очередь прокруток на чат.

Проблема: одна прокрутка рулетки — это 6-7 запросов к Telegram (кадры анимации).
Если в чате несколько человек крутят почти одновременно, залп запросов упирается
в флуд-лимит Telegram, и бот «зависает». Очередь сглаживает залп: в каждом чате
прокрутки идут строго по одной, следующая стартует, как только докрутила предыдущая.

Живёт в памяти процесса. Перезапуск бота очередь теряет — это не критично:
незавершённые прокрутки не начислены (баланс пишется в транзакции ДО анимации
только когда прокрутка реально пошла), человек просто крутит заново.

ВАЖНО: дешёвые проверки (бан, одобрен ли чат, крутил ли уже сегодня, бюджет)
делаются ДО постановки в очередь — в вызывающем коде. Сюда попадает только
прокрутка, которая реально состоится, чтобы не занимать место зря.
"""
import asyncio
import logging
from collections import defaultdict

log = logging.getLogger(__name__)

# на каждый чат — свой замок и счётчик ожидающих
_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
_waiting: dict[int, int] = defaultdict(int)

# пауза между прокрутками в одном чате: даём Telegram выдохнуть, но <1с,
# чтобы очередь двигалась живо. Следующего бот берёт сам, не ждёт нового !шайн.
GAP_BETWEEN_SPINS = 0.5


def waiting_count(chat_id: int) -> int:
    """Сколько человек СЕЙЧАС ждут своей очереди в этом чате (не считая текущего)."""
    return _waiting.get(chat_id, 0)


def position(chat_id: int) -> int:
    """Позиция, которую займёт новоприбывший: 1 = крутится сразу, 2+ = ждёт."""
    # .get, а не [], чтобы чтение не создавало пустых записей в defaultdict
    lock = _locks.get(chat_id)
    busy = 1 if (lock is not None and lock.locked()) else 0
    return busy + _waiting.get(chat_id, 0) + 1


class QueueSlot:
    """
    async-контекст: вход = занять очередь чата, выход = освободить + пауза.

    Пока замок занят одной прокруткой, остальные ждут на acquire(). Это и есть
    «очередь»: asyncio.Lock честный (FIFO), кто первым пришёл — первым и крутит.
    """
    def __init__(self, chat_id: int):
        self.chat_id = chat_id
        self._was_waiting = False

    async def __aenter__(self):
        lock = _locks[self.chat_id]
        if lock.locked():
            # кто-то уже крутится — встаём в ожидание
            _waiting[self.chat_id] += 1
            self._was_waiting = True
        await lock.acquire()
        if self._was_waiting:
            _waiting[self.chat_id] -= 1
        return self

    async def __aexit__(self, *exc):
        # пауза перед тем, как отпустить замок и пустить следующего —
        # даём Telegram передышку между сериями сообщений
        try:
            await asyncio.sleep(GAP_BETWEEN_SPINS)
        finally:
            _locks[self.chat_id].release()
            # подчистка, чтобы словари не пухли от разовых чатов
            if not _locks[self.chat_id].locked() and _waiting[self.chat_id] == 0:
                _waiting.pop(self.chat_id, None)
