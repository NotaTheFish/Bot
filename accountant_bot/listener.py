from __future__ import annotations

import asyncio

from telethon import TelegramClient, events

from .config import Settings
from .reviews import ReviewsService


def register_listener_handlers(
    telethon_client: TelegramClient,
    reviews_service: ReviewsService,
    settings: Settings,
) -> None:
    media_group_buffers: dict[int, set[int]] = {}
    media_group_tasks: dict[int, asyncio.Task[None]] = {}

    async def flush_media_group(grouped_id: int) -> None:
        message_ids_set = media_group_buffers.pop(grouped_id, None)
        media_group_tasks.pop(grouped_id, None)
        if not message_ids_set:
            return

        message_ids = sorted(message_ids_set)
        root_message_id = min(message_ids)
        media_group_id = str(grouped_id)

        await reviews_service.add_review(
            channel_id=settings.REVIEWS_CHANNEL_ID,
            review_key=reviews_service.build_review_key(root_message_id, media_group_id),
            message_ids=message_ids,
            source_chat_id=settings.REVIEWS_CHANNEL_ID,
            source_message_id=root_message_id,
        )

    def restart_media_group_flush_timer(grouped_id: int) -> None:
        existing_task = media_group_tasks.get(grouped_id)
        if existing_task is not None:
            existing_task.cancel()

        async def delayed_flush() -> None:
            try:
                await asyncio.sleep(settings.MEDIA_GROUP_BUFFER_SECONDS)
                await flush_media_group(grouped_id)
            except asyncio.CancelledError:
                raise

        media_group_tasks[grouped_id] = asyncio.create_task(delayed_flush())

    @telethon_client.on(events.NewMessage(chats=settings.REVIEWS_CHANNEL_ID))
    async def handle_new_message(event: events.NewMessage.Event) -> None:
        message = event.message
        if message is None:
            return

        message_id = int(message.id)
        grouped_id = getattr(message, "grouped_id", None)

        if grouped_id is not None:
            group_key = int(grouped_id)
            media_group_buffers.setdefault(group_key, set()).add(message_id)
            restart_media_group_flush_timer(group_key)
            return

        await reviews_service.add_review(
            channel_id=settings.REVIEWS_CHANNEL_ID,
            review_key=reviews_service.build_review_key(message_id, None),
            message_ids=[message_id],
            source_chat_id=settings.REVIEWS_CHANNEL_ID,
            source_message_id=message_id,
            review_text=message.message,
        )

    @telethon_client.on(events.MessageDeleted(chats=settings.REVIEWS_CHANNEL_ID))
    async def handle_message_deleted(event: events.MessageDeleted.Event) -> None:
        for message_id in event.deleted_ids:
            deleted_rows = await reviews_service.mark_deleted_by_message_id(
                channel_id=settings.REVIEWS_CHANNEL_ID,
                message_id=int(message_id),
            )
            if deleted_rows:
                reviews_service.schedule_about_update()