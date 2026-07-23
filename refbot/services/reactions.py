"""
Реакция ожидания на сообщение !шайн, пока человек стоит в очереди.

Telegram ограничивает боту кастомные (премиум) реакции: поставить можно только те,
что админ чата явно разрешил в настройках чата. Premium владельца бота (который даёт
премиум-эмодзи в ТЕКСТЕ) на реакции НЕ распространяется — это отдельное правило.

Поэтому:
  - если в «🎨 Кастомизация → ⏳ Реакция ожидания» задан премиум-эмодзи, пробуем его;
  - Telegram отказал (в чате не разрешён) — падаем на обычные ⏳;
  - премиум не задан — сразу обычные ⏳, без лишней попытки.

Проверить заранее, разрешена ли реакция, API не даёт — только попытка по факту.
Тот же приём, что с премиум-эмодзи в тексте: пробуем -> не вышло -> запасной вариант.
"""
import logging

from aiogram.types import ReactionTypeCustomEmoji, ReactionTypeEmoji

from services import settings

log = logging.getLogger(__name__)

WAIT_SLOT = "wait_reaction"          # ключ настройки
DEFAULT_WAIT_EMOJI = "⌛"            # запасная обычная реакция (Telegram-разрешённая)


async def set_wait(chat_id: int, message_id: int, bot) -> bool:
    """
    Поставить реакцию ожидания. True — поставлена (любая), False — не удалось.
    Не бросает: реакция необязательна, её отсутствие не должно ломать прокрутку.
    """
    premium_id = await settings.get(f"premium.{WAIT_SLOT}")
    # запасной обычный эмодзи — из настроек или дефолт
    plain = await settings.get(f"emoji.{WAIT_SLOT}") or DEFAULT_WAIT_EMOJI

    if premium_id:
        try:
            await bot.set_message_reaction(
                chat_id, message_id,
                reaction=[ReactionTypeCustomEmoji(custom_emoji_id=premium_id)])
            log.info("реакция: премиум ПОСТАВЛЕНА chat=%s msg=%s id=%s",
                     chat_id, message_id, premium_id)
            return True
        except Exception as e:
            # чат не разрешил кастомную реакцию — падаем на обычную
            log.info("реакция: премиум ОТКЛОНЁН (%s), ставлю обычную %r", e, plain)

    try:
        await bot.set_message_reaction(
            chat_id, message_id, reaction=[ReactionTypeEmoji(emoji=plain)])
        log.info("реакция: обычная ПОСТАВЛЕНА chat=%s msg=%s emoji=%r",
                 chat_id, message_id, plain)
        return True
    except Exception as e:
        log.warning("реакция: НЕ смог поставить даже обычную chat=%s msg=%s: %s",
                    chat_id, message_id, e)
        return False


async def clear(chat_id: int, message_id: int, bot) -> None:
    """Снять реакцию (пустой список). Вызывается, когда очередь дошла до игрока."""
    try:
        await bot.set_message_reaction(chat_id, message_id, reaction=[])
    except Exception as e:
        log.debug("не смог снять реакцию: %s", e)
