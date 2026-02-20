import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from telethon import events

from userbot_worker.config import Settings
from userbot_worker.worker import run_worker


class _FakeClient:
    def __init__(self) -> None:
        self.get_me = AsyncMock(return_value=SimpleNamespace(id=123))
        self.handlers = {}

    def on(self, event_builder):
        def decorator(func):
            self.handlers[type(event_builder)] = func
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
    async def _get_private_handler(self):
        client = _FakeClient()
        stop_event = asyncio.Event()
        stop_event.set()

        with (
            patch("userbot_worker.worker.ensure_schema", AsyncMock()),
            patch("userbot_worker.worker.sync_userbot_targets", AsyncMock()),
        ):
            await run_worker(_settings(), pool=object(), client=client, stop_event=stop_event)

        return client.handlers[events.NewMessage]

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
            patch("userbot_worker.worker.get_worker_autoreply_remaining", AsyncMock(return_value=0)),
            patch("userbot_worker.worker.get_worker_autoreply_contact", AsyncMock(return_value={"last_replied_at": None})),
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()) as upsert_contact_mock,
            patch("userbot_worker.worker.touch_worker_last_outgoing_at", AsyncMock()) as touch_mock,
        ):
            await handler(event)

        event.respond.assert_awaited_once_with("hello")
        upsert_contact_mock.assert_any_await(unittest.mock.ANY, user_id=555, replied=True)
        touch_mock.assert_awaited_once()

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
            patch("userbot_worker.worker.get_worker_autoreply_remaining", AsyncMock(return_value=30)),
            patch(
                "userbot_worker.worker.get_worker_autoreply_contact",
                AsyncMock(return_value={"last_replied_at": datetime.now(timezone.utc)}),
            ),
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()) as upsert_contact_mock,
            patch("userbot_worker.worker.touch_worker_last_outgoing_at", AsyncMock()) as touch_mock,
        ):
            await handler(event)

        event.respond.assert_not_awaited()
        upsert_contact_mock.assert_awaited_once_with(unittest.mock.ANY, user_id=555, replied=False)
        touch_mock.assert_not_awaited()

    async def test_repeat_after_cooldown_replies_again(self):
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
            patch("userbot_worker.worker.get_worker_autoreply_remaining", AsyncMock(return_value=0)),
            patch(
                "userbot_worker.worker.get_worker_autoreply_contact",
                AsyncMock(return_value={"last_replied_at": datetime.now(timezone.utc)}),
            ),
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()) as upsert_contact_mock,
            patch("userbot_worker.worker.touch_worker_last_outgoing_at", AsyncMock()) as touch_mock,
        ):
            await handler(event)

        event.respond.assert_awaited_once_with("hello")
        upsert_contact_mock.assert_has_awaits(
            [
                unittest.mock.call(unittest.mock.ANY, user_id=555, replied=False),
                unittest.mock.call(unittest.mock.ANY, user_id=555, replied=True),
            ]
        )
        touch_mock.assert_awaited_once()

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
            patch("userbot_worker.worker.get_worker_autoreply_remaining", AsyncMock(return_value=0)),
            patch("userbot_worker.worker.get_worker_autoreply_contact", AsyncMock(return_value={"last_replied_at": None})),
            patch(
                "userbot_worker.worker.get_worker_state_last_outgoing_at",
                AsyncMock(return_value=datetime.now(timezone.utc) - timedelta(seconds=30)),
            ),
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()) as upsert_contact_mock,
            patch("userbot_worker.worker.touch_worker_last_outgoing_at", AsyncMock()) as touch_mock,
        ):
            await handler(event)

        event.respond.assert_not_awaited()
        upsert_contact_mock.assert_awaited_once_with(unittest.mock.ANY, user_id=555, replied=False)
        touch_mock.assert_not_awaited()

    async def test_offline_threshold_positive_with_elapsed_time_replies(self):
        handler = await self._get_private_handler()
        event = _FakeEvent()
        settings_row = SimpleNamespace(
            enabled=True,
            cooldown_seconds=60,
            trigger_mode="offline_over_minutes",
            reply_text="hello",
            offline_threshold_minutes=10,
        )

        with (
            patch("userbot_worker.worker.get_worker_autoreply_settings", AsyncMock(return_value=settings_row)),
            patch("userbot_worker.worker.get_worker_autoreply_remaining", AsyncMock(return_value=0)),
            patch("userbot_worker.worker.get_worker_autoreply_contact", AsyncMock(return_value={"last_replied_at": None})),
            patch(
                "userbot_worker.worker.get_worker_state_last_outgoing_at",
                AsyncMock(return_value=datetime.now(timezone.utc) - timedelta(minutes=11)),
            ),
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()) as upsert_contact_mock,
            patch("userbot_worker.worker.touch_worker_last_outgoing_at", AsyncMock()) as touch_mock,
        ):
            await handler(event)

        event.respond.assert_awaited_once_with("hello")
        upsert_contact_mock.assert_has_awaits(
            [
                unittest.mock.call(unittest.mock.ANY, user_id=555, replied=False),
                unittest.mock.call(unittest.mock.ANY, user_id=555, replied=True),
            ]
        )
        touch_mock.assert_awaited_once()

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
            patch("userbot_worker.worker.get_worker_autoreply_remaining", AsyncMock(return_value=0)),
            patch(
                "userbot_worker.worker.get_worker_autoreply_contact",
                AsyncMock(return_value={"last_replied_at": datetime.now(timezone.utc)}),
            ),
            patch(
                "userbot_worker.worker.get_worker_state_last_outgoing_at",
                AsyncMock(return_value=datetime.now(timezone.utc) - timedelta(minutes=5)),
            ),
            patch("userbot_worker.worker.upsert_worker_autoreply_contact", AsyncMock()) as upsert_contact_mock,
            patch("userbot_worker.worker.touch_worker_last_outgoing_at", AsyncMock()) as touch_mock,
        ):
            await handler(event)

        event.respond.assert_not_awaited()
        upsert_contact_mock.assert_awaited_once_with(unittest.mock.ANY, user_id=555, replied=False)
        touch_mock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()