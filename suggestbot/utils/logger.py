from aiogram import Bot
from config import LOG_CHAT_ID


async def log(bot: Bot, text: str) -> None:
    """Отправляет лог-сообщение в LOG_CHAT_ID. Тихо глотает ошибки."""
    if not LOG_CHAT_ID:
        return
    try:
        await bot.send_message(LOG_CHAT_ID, text, parse_mode="HTML")
    except Exception:
        pass
