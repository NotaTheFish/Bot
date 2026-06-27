import asyncio
import json
from typing import Callable, Awaitable
from redis.asyncio import Redis

BUFFER_TTL = 2  # секунды ожидания перед финальной обработкой
REDIS_PREFIX = "sgb:mg:"


async def add_to_media_group(
    redis: Redis,
    media_group_id: str,
    item: dict,
) -> list[dict] | None:
    """
    Добавляет item в буфер медиагруппы.
    Возвращает полный список, когда все фото собраны (TTL истёк),
    иначе None — значит ещё ждём.
    """
    key = f"{REDIS_PREFIX}{media_group_id}"
    lock_key = f"{REDIS_PREFIX}{media_group_id}:lock"

    # Добавляем элемент в список
    await redis.rpush(key, json.dumps(item))
    await redis.expire(key, 10)  # запас на случай задержки

    # Ставим/обновляем «дедлайн» обработки
    await redis.set(lock_key, "1", px=int(BUFFER_TTL * 1000), nx=True)

    # Ждём дедлайн
    await asyncio.sleep(BUFFER_TTL)

    # Пробуем стать «финализатором» — удаляем lock
    deleted = await redis.delete(lock_key)
    if deleted == 0:
        # Другой обработчик уже финализировал
        return None

    # Мы финализатор — забираем все элементы
    raw_items = await redis.lrange(key, 0, -1)
    await redis.delete(key)

    if not raw_items:
        return None

    items = [json.loads(r) for r in raw_items]
    items.sort(key=lambda x: x.get("sort_order", 0))
    return items


async def get_media_group(redis: Redis, media_group_id: str) -> list[dict]:
    key = f"{REDIS_PREFIX}{media_group_id}"
    raw_items = await redis.lrange(key, 0, -1)
    return [json.loads(r) for r in raw_items]
