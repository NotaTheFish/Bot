import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from telethon import events

from userbot_worker.config import Settings
from userbot_worker.worker import AUTOREPLY_SENT, run_worker


class _FakeClient:
    def __init__(self) -> None:
        self.get_me = AsyncMock(return_value=SimpleNamespace(id=123))
        self.handlers = []
        self.forward_messages = AsyncMock()

    def on(self, event_builder):
        def decorator(func):
            self.handlers.append((event_builder, func))
            return func

        return decorator


class _FakeEvent:
    def __init__(self, sender_id: int = 555) -> None:
        self.is_private = True
        self.sender_id = sender_id
        self.get_sender = AsyncMock(return_value=SimpleNamespace(bot=False))
        self.respond = AsyncMock()


def _settings() -> Settings:
    return Settings(
        database_url="postgresql://localhost/test",
        telegram_api_id=1,
        telegram_api_hash="hash",
        telethon_session="userbot.session",
        min_seconds_between_chats=0,
        max_seconds_between_chats=0,
        worker_poll_seconds=0,
        admin_notify_bot_token="",
        admin_id=0,
        storage_chat_id=999,
        blacklist_chat_ids=set(),
        max_task_attempts=2,
        target_chat_ids=[],
        targets_sync_seconds=999999,
        targets_stale_grace_days=7,
        targets_disable_on_entity_error=True,
        auto_targets_mode="groups_only",
        cooldown_minutes=0,
        activity_gate_min_messages=0,
        anti_dup_minutes=0,
        controller_bot_username="controller_bot",
        authkey_duplicated_cooldown_seconds=120,
    )


class PrivateAutoreplyCooldownTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        AUTOREPLY_SENT.clear()

    async def _get_private_handler(self):
        client = _FakeClient()
        stop_event = asyncio.Event()
        stop_event.set()

        with (
            patch("userbot_worker.worker.ensure_schema", AsyncMock()),
            patch("userbot_worker.worker.sync_userbot_targets", AsyncMock()),
        ):
            await run_worker(_settings(), pool=object(), client=client, stop_event=stop_event)

        for event_builder, handler in client.handlers:
            if isinstance(event_builder, events.NewMessage) and bool(getattr(event_builder, "incoming", False)):
                return handler
        raise AssertionError("Incoming private handler not registered")

    async def _get_outgoing_handler(self):
        client = _FakeClient()
        stop_event = asyncio.Event()
        stop_event.set()

        with (
            patch("userbot_worker.worker.ensure_schema", AsyncMock()),
            patch("userbot_worker.worker.sync_userbot_targets", AsyncMock()),
        ):
            await run_worker(_settings(), pool=object(), client=client, stop_event=stop_event)

        for event_builder, handler in client.handlers:
            if isinstance(event_builder, events.NewMessage) and bool(getattr(event_builder, "outgoing", False)):
                return handler
        raise AssertionError("Outgoing private handler not registered")

    async def test_first_incoming_replies(self):
        handler = await self._get_private_handler()
        event = _FakeEvent()
        settings_row = SimpleNamespace(
            enabled=True,
            cooldown_seconds=60,
            trigger_mode="first_message_only",
            reply_text="hello",
            offline_threshold_minutes=10,
        )

        with (
            patch("userbot_worker.worker.get_worker_autoreply_settings", AsyncMock(return_value=settings_row)),
            patch("userbot_worker.worker.try_mark_autoreply", AsyncMock(return_value=True)) as try_mark_mock,
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()) as upsert_contact_mock,
            patch("userbot_worker.worker.touch_worker_last_manual_outgoing_at", AsyncMock()) as touch_mock,
        ):
            await handler(event)

        event.respond.assert_awaited_once_with("hello")
        try_mark_mock.assert_awaited_once_with(unittest.mock.ANY, user_id=555, cooldown_seconds=60)
        upsert_contact_mock.assert_awaited_once_with(unittest.mock.ANY, user_id=555, replied=False)
        touch_mock.assert_not_awaited()

    async def test_repeat_before_cooldown_does_not_reply(self):
        handler = await self._get_private_handler()
        event = _FakeEvent()
        settings_row = SimpleNamespace(
            enabled=True,
            cooldown_seconds=60,
            trigger_mode="first_message_only",
            reply_text="hello",
            offline_threshold_minutes=10,
        )

        with (
            patch("userbot_worker.worker.get_worker_autoreply_settings", AsyncMock(return_value=settings_row)),
            patch("userbot_worker.worker.try_mark_autoreply", AsyncMock(return_value=False)) as try_mark_mock,
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()) as upsert_contact_mock,
            patch("userbot_worker.worker.touch_worker_last_manual_outgoing_at", AsyncMock()) as touch_mock,
        ):
            await handler(event)

        event.respond.assert_not_awaited()
        try_mark_mock.assert_awaited_once_with(unittest.mock.ANY, user_id=555, cooldown_seconds=60)
        upsert_contact_mock.assert_awaited_once_with(unittest.mock.ANY, user_id=555, replied=False)
        touch_mock.assert_not_awaited()

    async def test_storage_forward_template_sends_forward(self):
        client = _FakeClient()
        stop_event = asyncio.Event()
        stop_event.set()

        with (
            patch("userbot_worker.worker.ensure_schema", AsyncMock()),
            patch("userbot_worker.worker.sync_userbot_targets", AsyncMock()),
        ):
            await run_worker(_settings(), pool=object(), client=client, stop_event=stop_event)

        incoming_handler = None
        for event_builder, handler in client.handlers:
            if isinstance(event_builder, events.NewMessage) and bool(getattr(event_builder, "incoming", False)):
                incoming_handler = handler
                break
        if incoming_handler is None:
            raise AssertionError("Incoming private handler not registered")

        event = _FakeEvent()
        event.chat_id = 999
        client.forward_messages.return_value = [SimpleNamespace(chat_id=999, id=77)]
        settings_row = SimpleNamespace(
            enabled=True,
            cooldown_seconds=60,
            trigger_mode="first_message_only",
            reply_text="hello",
            template_source="storage_forward",
            template_storage_chat_id=-100123,
            template_storage_message_ids=[10],
            offline_threshold_minutes=10,
        )

        with (
            patch("userbot_worker.worker.get_worker_autoreply_settings", AsyncMock(return_value=settings_row)),
            patch("userbot_worker.worker.try_mark_autoreply", AsyncMock(return_value=True)),
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()),
        ):
            await incoming_handler(event)

        client.forward_messages.assert_awaited_once_with(999, [10], from_peer=-100123)
        event.respond.assert_not_awaited()

    async def test_offline_threshold_zero_with_recent_activity_does_not_reply(self):
        handler = await self._get_private_handler()
        event = _FakeEvent()
        settings_row = SimpleNamespace(
            enabled=True,
            cooldown_seconds=60,
            trigger_mode="offline_over_minutes",
            reply_text="hello",
            offline_threshold_minutes=0,
        )

        with (
            patch("userbot_worker.worker.get_worker_autoreply_settings", AsyncMock(return_value=settings_row)),
            patch(
                "userbot_worker.worker.get_worker_state_last_manual_outgoing_at",
                AsyncMock(return_value=datetime.now(timezone.utc) - timedelta(seconds=30)),
            ),
            patch("userbot_worker.worker.try_mark_autoreply", AsyncMock()) as try_mark_mock,
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()) as upsert_contact_mock,
            patch("userbot_worker.worker.touch_worker_last_manual_outgoing_at", AsyncMock()) as touch_mock,
        ):
            await handler(event)

        event.respond.assert_not_awaited()
        try_mark_mock.assert_not_awaited()
        upsert_contact_mock.assert_awaited_once_with(unittest.mock.ANY, user_id=555, replied=False)
        touch_mock.assert_not_awaited()

    async def test_both_mode_requires_offline_and_first_or_cooldown(self):
        handler = await self._get_private_handler()
        event = _FakeEvent()
        settings_row = SimpleNamespace(
            enabled=True,
            cooldown_seconds=60,
            trigger_mode="both",
            reply_text="hello",
            offline_threshold_minutes=10,
        )

        with (
            patch("userbot_worker.worker.get_worker_autoreply_settings", AsyncMock(return_value=settings_row)),
            patch(
                "userbot_worker.worker.get_worker_state_last_manual_outgoing_at",
                AsyncMock(return_value=datetime.now(timezone.utc) - timedelta(minutes=5)),
            ),
            patch("userbot_worker.worker.try_mark_autoreply", AsyncMock()) as try_mark_mock,
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()) as upsert_contact_mock,
            patch("userbot_worker.worker.touch_worker_last_manual_outgoing_at", AsyncMock()) as touch_mock,
        ):
            await handler(event)

        event.respond.assert_not_awaited()
        try_mark_mock.assert_not_awaited()
        upsert_contact_mock.assert_awaited_once_with(unittest.mock.ANY, user_id=555, replied=False)
        touch_mock.assert_not_awaited()

    async def test_two_quick_messages_only_one_reply(self):
        handler = await self._get_private_handler()
        first_event = _FakeEvent()
        second_event = _FakeEvent()
        settings_row = SimpleNamespace(
            enabled=True,
            cooldown_seconds=60,
            trigger_mode="first_message_only",
            reply_text="hello",
            offline_threshold_minutes=10,
        )

        with (
            patch("userbot_worker.worker.get_worker_autoreply_settings", AsyncMock(return_value=settings_row)),
            patch("userbot_worker.worker.try_mark_autoreply", AsyncMock(side_effect=[True, False])) as try_mark_mock,
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()) as upsert_contact_mock,
            patch("userbot_worker.worker.touch_worker_last_manual_outgoing_at", AsyncMock()) as touch_mock,
        ):
            await handler(first_event)
            await handler(second_event)

        first_event.respond.assert_awaited_once_with("hello")
        second_event.respond.assert_not_awaited()
        self.assertEqual(try_mark_mock.await_count, 2)
        upsert_contact_mock.assert_has_awaits(
            [
                unittest.mock.call(unittest.mock.ANY, user_id=555, replied=False),
                unittest.mock.call(unittest.mock.ANY, user_id=555, replied=False),
            ]
        )
        touch_mock.assert_not_awaited()

    async def test_manual_outgoing_skips_autoreply_message(self):
        handler = await self._get_outgoing_handler()
        AUTOREPLY_SENT[(777, 101)] = datetime.now(timezone.utc)
        event = SimpleNamespace(
            is_private=True,
            via_bot_id=None,
            chat_id=777,
            message=SimpleNamespace(id=101),
        )

        with patch("userbot_worker.worker.touch_worker_last_manual_outgoing_at", AsyncMock()) as touch_mock:
            await handler(event)

        touch_mock.assert_not_awaited()

    async def test_manual_outgoing_updates_timestamp_for_real_manual_message(self):
        handler = await self._get_outgoing_handler()
        event = SimpleNamespace(
            is_private=True,
            via_bot_id=None,
            chat_id=888,
            message=SimpleNamespace(id=202),
        )

        with patch("userbot_worker.worker.touch_worker_last_manual_outgoing_at", AsyncMock()) as touch_mock:
            await handler(event)

        touch_mock.assert_awaited_once_with(unittest.mock.ANY)


if __name__ == "__main__":
    unittest.main()