import asyncio
from typing import Any, Awaitable, Callable
from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject
from redis.asyncio import Redis
from config import FLOOD_LIMIT_SECONDS, ADMIN_IDS
from utils.helpers import format_time

REDIS_KEY = "sgb:flood:{user_id}"


class AntiFloodMiddleware(BaseMiddleware):
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        # Антифлуд только для сообщений в состоянии waiting_content
        if not isinstance(event, Message):
            return await handler(event, data)

        user = event.from_user
        if not user or user.id in ADMIN_IDS:
            return await handler(event, data)

        state = data.get("state")
        if state is None:
            return await handler(event, data)

        current_state = await state.get_state()
        if current_state != "UserSuggest:waiting_content":
            return await handler(event, data)

        key = REDIS_KEY.format(user_id=user.id)
        ttl = await self.redis.ttl(key)

        if ttl > 0:
            # Показываем обратный отсчёт
            msg = await event.answer(
                f"⏳ Подождите <b>{format_time(ttl)}</b>",
                parse_mode="HTML"
            )
            # Обновляем каждую секунду и удаляем
            asyncio.create_task(self._countdown(msg, key))
            return  # не передаём дальше

        return await handler(event, data)

    async def _countdown(self, msg: Message, key: str) -> None:
        """Редактирует сообщение с обратным отсчётом, потом плавно удаляет."""
        try:
            for _ in range(30):  # максимум 30 секунд тикаем
                await asyncio.sleep(1)
                ttl = await self.redis.ttl(key)
                if ttl <= 0:
                    break
                try:
                    await msg.edit_text(
                        f"⏳ Подождите <b>{format_time(ttl)}</b>",
                        parse_mode="HTML"
                    )
                except Exception:
                    break
            await asyncio.sleep(1)
            await msg.delete()
        except Exception:
            pass
