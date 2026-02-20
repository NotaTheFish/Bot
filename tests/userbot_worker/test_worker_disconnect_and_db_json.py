import asyncio
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from userbot_worker.config import Settings
from userbot_worker.db import TaskRow, _row_to_task, mark_task_done, update_task_progress
from userbot_worker.worker import run_worker


class _FakeClient:
    def __init__(self, *, connected: bool) -> None:
        self._connected = connected
        self.connect = AsyncMock()
        self.get_me = AsyncMock(return_value=SimpleNamespace(id=123))

    def is_connected(self):
        return self._connected

    def on(self, _event):
        def decorator(func):
            return func

        return decorator


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


def _task() -> TaskRow:
    return TaskRow(
        id=7,
        status="pending",
        run_at=datetime.now(timezone.utc),
        target_chat_ids=[101, 102],
        storage_chat_id=999,
        storage_message_ids=[1],
        attempts=1,
        last_error=None,
        sent_count=0,
        error_count=0,
        dedupe_key=None,
        skipped_breakdown={},
    )


class WorkerDisconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_preflight_disconnect_marks_error_and_skips_chat_loop(self):
        stop_event = asyncio.Event()
        client = _FakeClient(connected=False)

        async def _sleep_and_stop(*_args, **_kwargs):
            stop_event.set()

        with (
            patch("userbot_worker.worker.ensure_schema", AsyncMock()),
            patch("userbot_worker.worker.sync_userbot_targets", AsyncMock()),
            patch("userbot_worker.worker.claim_pending_tasks", AsyncMock(return_value=[_task()])),
            patch("userbot_worker.worker.load_userbot_targets_by_chat_ids", AsyncMock(return_value=[SimpleNamespace(chat_id=101), SimpleNamespace(chat_id=102)])),
            patch("userbot_worker.worker.mark_task_error", AsyncMock()) as mark_task_error_mock,
            patch("userbot_worker.worker.update_task_progress", AsyncMock()) as update_progress_mock,
            patch("userbot_worker.worker.send_post_to_chat", AsyncMock()) as send_post_mock,
            patch("userbot_worker.worker.asyncio.sleep", AsyncMock(side_effect=_sleep_and_stop)),
        ):
            await run_worker(_settings(), pool=object(), client=client, stop_event=stop_event)

        send_post_mock.assert_not_awaited()
        update_progress_mock.assert_any_await(
            unittest.mock.ANY,
            task_id=7,
            sent_count=0,
            error_count=0,
            last_error="disconnected",
        )
        mark_task_error_mock.assert_any_await(
            unittest.mock.ANY,
            task_id=7,
            attempts=1,
            max_attempts=2,
            error="disconnected",
            sent_count=0,
            error_count=0,
        )

    async def test_mid_task_disconnect_aborts_early_without_many_errors(self):
        stop_event = asyncio.Event()
        client = _FakeClient(connected=True)
        target = SimpleNamespace(chat_id=101, peer_type="chat", peer_id=101, access_hash=None)

        async def _sleep_and_stop(*_args, **_kwargs):
            stop_event.set()

        with (
            patch("userbot_worker.worker.ensure_schema", AsyncMock()),
            patch("userbot_worker.worker.sync_userbot_targets", AsyncMock()),
            patch("userbot_worker.worker.claim_pending_tasks", AsyncMock(return_value=[_task()])),
            patch("userbot_worker.worker.load_userbot_targets_by_chat_ids", AsyncMock(return_value=[target])),
            patch("userbot_worker.worker.get_task_status", AsyncMock(return_value="processing")),
            patch("userbot_worker.worker.get_userbot_target_state", AsyncMock(return_value=None)),
            patch("userbot_worker.worker.send_post_to_chat", AsyncMock(side_effect=ConnectionError("Cannot send requests while disconnected"))) as send_post_mock,
            patch("userbot_worker.worker.mark_task_error", AsyncMock()) as mark_task_error_mock,
            patch("userbot_worker.worker.update_task_progress", AsyncMock()) as update_progress_mock,
            patch("userbot_worker.worker.mark_task_done", AsyncMock()),
            patch("userbot_worker.worker.asyncio.sleep", AsyncMock(side_effect=_sleep_and_stop)),
        ):
            await run_worker(_settings(), pool=object(), client=client, stop_event=stop_event)

        self.assertEqual(send_post_mock.await_count, 1)
        update_progress_mock.assert_any_await(
            unittest.mock.ANY,
            task_id=7,
            sent_count=0,
            error_count=1,
            last_error="disconnected_mid_task",
        )
        mark_task_error_mock.assert_any_await(
            unittest.mock.ANY,
            task_id=7,
            attempts=1,
            max_attempts=2,
            error="disconnected_mid_task",
            sent_count=0,
            error_count=1,
        )


class DbSkippedBreakdownTests(unittest.IsolatedAsyncioTestCase):
    def test_row_to_task_parses_skipped_breakdown_from_json_string(self):
        row = {
            "id": 1,
            "status": "done",
            "run_at": datetime.now(timezone.utc),
            "target_chat_ids": [1],
            "storage_chat_id": 10,
            "storage_message_ids": [2],
            "attempts": 0,
            "last_error": None,
            "sent_count": 0,
            "error_count": 0,
            "dedupe_key": None,
            "skipped_breakdown": '{"skipped_cooldown": 3}',
        }

        task = _row_to_task(row)

        self.assertEqual(task.skipped_breakdown, {"skipped_cooldown": 3})

    def test_row_to_task_accepts_jsonb_dict_without_type_errors(self):
        row = {
            "id": 1,
            "status": "done",
            "run_at": datetime.now(timezone.utc),
            "target_chat_ids": [1],
            "storage_chat_id": 10,
            "storage_message_ids": [2],
            "attempts": 0,
            "last_error": None,
            "sent_count": 0,
            "error_count": 0,
            "dedupe_key": None,
            "skipped_breakdown": {"skipped_cooldown": 4},
        }

        task = _row_to_task(row)

        self.assertEqual(task.skipped_breakdown, {"skipped_cooldown": 4})

    async def test_update_task_progress_saves_empty_dict_as_json_not_null(self):
        conn = MagicMock()
        conn.execute = AsyncMock()

        with (
            patch("userbot_worker.db._acquire_conn", AsyncMock(return_value=conn)),
            patch("userbot_worker.db._release_conn", AsyncMock()),
        ):
            await update_task_progress(
                object(),
                task_id=1,
                sent_count=0,
                error_count=0,
                last_error=None,
                skipped_breakdown={},
            )

        _, *params = conn.execute.await_args.args
        self.assertEqual(params[-1], "{}")

    async def test_mark_task_done_saves_empty_dict_as_json_not_null(self):
        conn = MagicMock()
        conn.execute = AsyncMock()

        with (
            patch("userbot_worker.db._acquire_conn", AsyncMock(return_value=conn)),
            patch("userbot_worker.db._release_conn", AsyncMock()),
        ):
            await mark_task_done(
                object(),
                task_id=1,
                sent_count=1,
                error_count=0,
                skipped_breakdown={},
            )

        _, *params = conn.execute.await_args.args
        self.assertEqual(params[-1], "{}")


if __name__ == "__main__":
    unittest.main()