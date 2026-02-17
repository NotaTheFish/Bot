from __future__ import annotations

from typing import Optional, Sequence

import asyncpg

from .db import fetch_review_by_key, fetch_reviews_by_message_id, insert_review


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