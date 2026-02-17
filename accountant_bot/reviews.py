from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Optional, Sequence

import asyncpg
from aiogram import Bot

from .config import Settings
from .db import fetch_review_by_key, fetch_reviews_by_message_id, insert_review


class ReviewsService:
    def __init__(self, pool: asyncpg.Pool, bot: Bot, settings: Settings) -> None:
        self._pool = pool
        self._bot = bot
        self._settings = settings
        self._about_update_task: Optional[asyncio.Task[None]] = None

    @staticmethod
    def build_review_key(root_message_id: int, media_group_id: Optional[str]) -> str:
        media_key = media_group_id or "single"
        return f"{int(root_message_id)}:{media_key}"

    async def add_review(
        self,
        *,
        channel_id: int,
        review_key: str,
        message_ids: Sequence[int],
        source_chat_id: Optional[int] = None,
        source_message_id: Optional[int] = None,
        review_text: Optional[str] = None,
    ) -> Optional[asyncpg.Record]:
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(
                """
                INSERT INTO reviews (
                    channel_id,
                    review_key,
                    source_chat_id,
                    source_message_id,
                    message_ids,
                    review_text
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (channel_id, review_key) DO NOTHING
                RETURNING *
                """,
                int(channel_id),
                str(review_key),
                source_chat_id,
                source_message_id,
                [int(message_id) for message_id in message_ids],
                review_text,
            )

    async def mark_deleted_by_message_id(self, *, channel_id: int, message_id: int) -> list[asyncpg.Record]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                UPDATE reviews
                SET deleted_at = NOW(),
                    updated_at = NOW()
                WHERE channel_id = $1
                  AND deleted_at IS NULL
                  AND $2 = ANY(message_ids)
                RETURNING *
                """,
                int(channel_id),
                int(message_id),
            )
        return list(rows)

    async def count_active(self, channel_id: int) -> int:
        async with self._pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM reviews
                    WHERE channel_id = $1
                      AND deleted_at IS NULL
                    """,
                    int(channel_id),
                )
                or 0
            )

    async def get_stats_reviews(self, period: str) -> int:
        interval_by_period = {
            "day": "1 day",
            "week": "7 days",
            "month": "1 month",
        }
        if period not in interval_by_period:
            raise ValueError("period must be one of: day, week, month")

        async with self._pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM reviews
                    WHERE channel_id = $1
                      AND deleted_at IS NULL
                      AND created_at >= NOW() - $2::interval
                    """,
                    int(self._settings.REVIEWS_CHANNEL_ID),
                    interval_by_period[period],
                )
                or 0
            )

    def format_about_line(self, count: int) -> str:
        return self._settings.ABOUT_TEMPLATE.format(
            count=int(count),
            date=datetime.now().strftime(self._settings.ABOUT_DATE_FORMAT),
        )

    @staticmethod
    def replace_or_append_about(description: Optional[str], new_line: str) -> str:
        base = description or ""
        lines = base.splitlines()
        replaced = False

        for idx, line in enumerate(lines):
            if line.strip().startswith("Отзывов:"):
                lines[idx] = new_line
                replaced = True
                break

        if not replaced:
            if lines:
                lines.append(new_line)
            else:
                lines = [new_line]

        return "\n".join(lines)

    def schedule_about_update(self) -> None:
        if self._about_update_task is not None and not self._about_update_task.done():
            self._about_update_task.cancel()
        self._about_update_task = asyncio.create_task(self._debounced_about_update())

    async def _debounced_about_update(self) -> None:
        try:
            await asyncio.sleep(self._settings.ABOUT_UPDATE_DEBOUNCE_SECONDS)
            count = await self.count_active(self._settings.REVIEWS_CHANNEL_ID)
            new_line = self.format_about_line(count)

            chat = await self._bot.get_chat(self._settings.REVIEWS_CHANNEL_ID)
            current_description = getattr(chat, "description", None)
            next_description = self.replace_or_append_about(current_description, new_line)

            if next_description != (current_description or ""):
                await self._bot.set_chat_description(
                    chat_id=self._settings.REVIEWS_CHANNEL_ID,
                    description=next_description,
                )
        except asyncio.CancelledError:
            raise


async def save_review(
    pool: asyncpg.Pool,
    *,
    channel_id: int,
    review_key: str,
    message_ids: Sequence[int],
    source_chat_id: Optional[int] = None,
    source_message_id: Optional[int] = None,
    review_text: Optional[str] = None,
) -> asyncpg.Record:
    return await insert_review(
        pool,
        channel_id=channel_id,
        review_key=review_key,
        message_ids=message_ids,
        source_chat_id=source_chat_id,
        source_message_id=source_message_id,
        review_text=review_text,
    )


async def get_review(
    pool: asyncpg.Pool,
    *,
    channel_id: int,
    review_key: str,
    include_deleted: bool = False,
) -> Optional[asyncpg.Record]:
    return await fetch_review_by_key(
        pool,
        channel_id=channel_id,
        review_key=review_key,
        include_deleted=include_deleted,
    )


async def find_reviews_by_message(
    pool: asyncpg.Pool,
    *,
    channel_id: int,
    message_id: int,
    include_deleted: bool = False,
) -> list[asyncpg.Record]:
    return await fetch_reviews_by_message_id(
        pool,
        channel_id=channel_id,
        message_id=message_id,
        include_deleted=include_deleted,
    )