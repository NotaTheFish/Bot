from typing import Any, Awaitable, Callable
from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject
from config import ADMIN_IDS
from db.queries import get_user, upsert_user


class BanCheckMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        if not isinstance(event, Message):
            return await handler(event, data)

        user = event.from_user
        if not user:
            return await handler(event, data)

        # Сохраняем/обновляем юзера в БД
        await upsert_user(user.id, user.username, user.first_name)

        # Не проверяем банлист для админов
        if user.id in ADMIN_IDS:
            return await handler(event, data)

        db_user = await get_user(user.id)
        if db_user and db_user["is_banned"]:
            reason = db_user["ban_reason"] or "причина не указана"
            await event.answer(f"🚫 Вы заблокированы.\nПричина: {reason}")
            return

        return await handler(event, data)
