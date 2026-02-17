from __future__ import annotations

import asyncio
import logging

from telethon import TelegramClient, events

from .config import Settings
from .reviews import ReviewsService

logger = logging.getLogger("reviews_listener")


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

        row = await reviews_service.add_review(
            channel_id=settings.REVIEWS_CHANNEL_ID,
            review_key=reviews_service.build_review_key(root_message_id, media_group_id),
            message_ids=message_ids,
            source_chat_id=settings.REVIEWS_CHANNEL_ID,
            source_message_id=root_message_id,
        )
        if row is not None:
            reviews_service.schedule_about_update()
        else:
            logger.info("duplicate ignored review_key=%s", reviews_service.build_review_key(root_message_id, media_group_id))

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

    @telethon_client.on(events.NewMessage())
    async def handle_new_message(event: events.NewMessage.Event) -> None:
        message = event.message
        if message is None:
            return

        message_id = int(message.id)
        grouped_id = getattr(message, "grouped_id", None)
        sender_id = getattr(message, "sender_id", None)
        message_text = (getattr(message, "message", "") or "").replace("\n", " ")[:30]
        chat_id = int(event.chat_id) if event.chat_id is not None else None

        logger.info(
            "NewMessage chat=%s msg=%s grouped=%s sender=%s text=%r",
            chat_id,
            message_id,
            grouped_id,
            sender_id,
            message_text,
        )

        if chat_id != int(settings.REVIEWS_CHANNEL_ID):
            logger.debug("ignored chat_id=%s expected=%s", chat_id, settings.REVIEWS_CHANNEL_ID)
            return

        if grouped_id is not None:
            group_key = int(grouped_id)
            media_group_buffers.setdefault(group_key, set()).add(message_id)
            restart_media_group_flush_timer(group_key)
            return

        row = await reviews_service.add_review(
            channel_id=settings.REVIEWS_CHANNEL_ID,
            review_key=reviews_service.build_review_key(message_id, None),
            message_ids=[message_id],
            source_chat_id=settings.REVIEWS_CHANNEL_ID,
            source_message_id=message_id,
            review_text=message.message,
        )
        if row is not None:
            reviews_service.schedule_about_update()
        else:
            logger.info("duplicate ignored review_key=%s", reviews_service.build_review_key(message_id, None))

    @telethon_client.on(events.MessageDeleted())
    async def handle_message_deleted(event: events.MessageDeleted.Event) -> None:
        chat_id = int(event.chat_id) if event.chat_id is not None else None
        deleted_ids = [int(message_id) for message_id in event.deleted_ids]
        logger.info("MessageDeleted chat=%s ids=%s", chat_id, deleted_ids)

        if chat_id != int(settings.REVIEWS_CHANNEL_ID):
            logger.debug("ignored chat_id=%s expected=%s", chat_id, settings.REVIEWS_CHANNEL_ID)
            return

        for message_id in deleted_ids:
            deleted_rows = await reviews_service.mark_deleted_by_message_id(
                channel_id=settings.REVIEWS_CHANNEL_ID,
                message_id=message_id,
            )
            if deleted_rows:
                reviews_service.schedule_about_update()