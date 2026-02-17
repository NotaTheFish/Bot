import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from telethon import events

from accountant_bot.config import Settings
from accountant_bot.listener import register_listener_handlers


class _FakeTelethonClient:
    def __init__(self):
        self.handlers = []

    def on(self, event_builder):
        def decorator(func):
            self.handlers.append((event_builder, func))
            return func

        return decorator


class AccountantBotListenerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.settings = Settings(
            ACCOUNTANT_BOT_TOKEN="token",
            ACCOUNTANT_ADMIN_IDS=[1],
            DATABASE_URL="postgresql://localhost/test",
            REVIEWS_CHANNEL_ID=777,
            TG_API_ID=123,
            TG_API_HASH="hash",
            ACCOUNTANT_TG_STRING_SESSION="session",
            MEDIA_GROUP_BUFFER_SECONDS=0.01,
        )
        self.client = _FakeTelethonClient()
        self.reviews_service = MagicMock()
        self.reviews_service.add_review = AsyncMock()
        self.reviews_service.mark_deleted_by_message_id = AsyncMock(return_value=[])
        self.reviews_service.schedule_about_update = MagicMock()
        self.reviews_service.build_review_key = MagicMock(side_effect=lambda root_id, group_id: f"{root_id}:{group_id or 'single'}")

        register_listener_handlers(self.client, self.reviews_service, self.settings)

    def _get_handler(self, event_type):
        for builder, handler in self.client.handlers:
            if isinstance(builder, event_type):
                return builder, handler
        raise AssertionError(f"Handler for {event_type} not found")

    def test_registers_handlers_for_reviews_channel_only(self):
        new_builder, _ = self._get_handler(events.NewMessage)
        deleted_builder, _ = self._get_handler(events.MessageDeleted)

        self.assertEqual(new_builder.chats, 777)
        self.assertEqual(deleted_builder.chats, 777)

    async def test_single_message_adds_review_immediately(self):
        _, handler = self._get_handler(events.NewMessage)

        event = SimpleNamespace(message=SimpleNamespace(id=10, grouped_id=None, message="hello"))
        await handler(event)

        self.reviews_service.add_review.assert_awaited_once_with(
            channel_id=777,
            review_key="10:single",
            message_ids=[10],
            source_chat_id=777,
            source_message_id=10,
            review_text="hello",
        )

    async def test_grouped_media_is_buffered_and_flushed(self):
        _, handler = self._get_handler(events.NewMessage)

        await handler(SimpleNamespace(message=SimpleNamespace(id=15, grouped_id=888, message="a")))
        await asyncio.sleep(0.005)
        await handler(SimpleNamespace(message=SimpleNamespace(id=14, grouped_id=888, message="b")))
        await asyncio.sleep(0.03)

        self.reviews_service.add_review.assert_awaited_once_with(
            channel_id=777,
            review_key="14:888",
            message_ids=[14, 15],
            source_chat_id=777,
            source_message_id=14,
        )

    async def test_deleted_messages_mark_and_schedule_update(self):
        _, handler = self._get_handler(events.MessageDeleted)
        self.reviews_service.mark_deleted_by_message_id = AsyncMock(side_effect=[[], [object()]])

        await handler(SimpleNamespace(deleted_ids=[1, 2]))

        self.assertEqual(self.reviews_service.mark_deleted_by_message_id.await_count, 2)
        self.reviews_service.schedule_about_update.assert_called_once()


if __name__ == "__main__":
    unittest.main()