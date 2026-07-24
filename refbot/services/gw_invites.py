"""
Инвайт-ссылки для розыгрышей.

Бот сам создаёт пригласительные ссылки (он админ). Если админ-человек удалит
ссылку, созданную ботом, бот создаст новую при следующей проверке/публикации.

Проверка подписки: bot.get_chat_member — если человек в чате/канале, считается
подписанным. Работает и для каналов, и для чатов, если бот там админ.
"""
import contextlib
import logging

log = logging.getLogger("giveaway")

# статусы участника, которые считаем «подписан»
_MEMBER = frozenset({"member", "administrator", "creator", "restricted"})


async def ensure_invite(bot, chat_id: int, existing: str = None) -> str | None:
    """
    Вернуть рабочую инвайт-ссылку. Если existing задана — проверять не будем
    (Telegram не даёт «проверить ссылку», только пересоздать). Если None —
    создаём новую. Бот должен быть админом с правом приглашать.
    """
    if existing:
        return existing
    try:
        link = await bot.create_chat_invite_link(chat_id, name="Giveaway")
        return link.invite_link
    except Exception as e:
        log.warning("не смог создать инвайт для %s: %s", chat_id, e)
        return None


async def recreate_invite(bot, chat_id: int) -> str | None:
    """Пересоздать ссылку (если старую удалили). Всегда делает новую."""
    try:
        link = await bot.create_chat_invite_link(chat_id, name="Giveaway")
        return link.invite_link
    except Exception as e:
        log.warning("не смог пересоздать инвайт для %s: %s", chat_id, e)
        return None


async def is_subscribed(bot, chat_id: int, tg_id: int) -> bool:
    """Подписан ли человек на чат/канал. Ошибку трактуем как «не подписан»."""
    try:
        m = await bot.get_chat_member(chat_id, tg_id)
        return m.status in _MEMBER
    except Exception as e:
        log.debug("get_chat_member(%s,%s) ошибка: %s", chat_id, tg_id, e)
        return False


async def check_all(bot, chats: list[dict], tg_id: int) -> list[dict]:
    """
    Проверить подписку на ВСЕ чаты розыгрыша. Возвращает список тех, где НЕ
    подписан (пустой список = подписан везде).
    """
    missing = []
    for ch in chats:
        if not await is_subscribed(bot, ch["chat_id"], tg_id):
            missing.append(ch)
    return missing
