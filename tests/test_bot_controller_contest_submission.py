import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("BOT_TOKEN", "123:abc")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("DATABASE_URL", "postgres://localhost/db")
os.environ.setdefault("STORAGE_CHAT_ID", "-100123")

from bot_controller import main_core


class ContestSubmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_contest_keyboard_contains_required_buttons(self):
        keyboard = main_core.contest_user_keyboard()
        flat = [button.text for row in keyboard.keyboard for button in row]
        self.assertEqual(
            flat,
            [
                "🎨 Предложить рисунок",
                "🗳 Голосовать за рисунок",
                "📜 Правила",
                "⬅️ Назад",
            ],
        )

    async def test_submit_start_without_pending_entry_sets_waiting_media_state(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private"),
            from_user=SimpleNamespace(id=77),
            answer=AsyncMock(),
        )
        state = AsyncMock()

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "_get_pending_contest_entry", AsyncMock(return_value=None)),
        ):
            await main_core.contest_submit_start(message, state)

        state.set_state.assert_awaited_once_with(main_core.ContestStates.waiting_submission_media)
        state.update_data.assert_awaited_once_with(contest_pending_entry_id=None)

    async def test_submit_start_with_pending_entry_sets_replace_state(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private"),
            from_user=SimpleNamespace(id=77),
            answer=AsyncMock(),
        )
        state = AsyncMock()

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "_get_pending_contest_entry", AsyncMock(return_value={"id": 15})),
        ):
            await main_core.contest_submit_start(message, state)

        state.set_state.assert_awaited_once_with(main_core.ContestStates.waiting_replace_confirmation)
        state.update_data.assert_awaited_once_with(contest_pending_entry_id=15)

    async def test_submission_media_rejects_non_image_message(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private"),
            from_user=SimpleNamespace(id=88),
            text="hello",
            media_group_id=None,
            photo=None,
            document=None,
            answer=AsyncMock(),
        )
        state = AsyncMock()

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "_upsert_contest_user_start", AsyncMock()),
        ):
            await main_core.contest_submission_media(message, state)

        message.answer.assert_awaited_once()

    async def test_submission_media_creates_pending_entry(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private", id=999),
            from_user=SimpleNamespace(id=88),
            text=None,
            media_group_id=None,
            photo=[SimpleNamespace(file_id="a")],
            document=None,
            message_id=501,
            answer=AsyncMock(),
        )
        state = AsyncMock()
        state.get_data = AsyncMock(return_value={"contest_pending_entry_id": None})

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "_create_pending_contest_entry", AsyncMock()) as create_entry,
            patch.object(main_core, "_get_pending_contest_entry", AsyncMock(return_value={"id": 33})),
            patch.object(main_core, "_notify_admins_about_contest_entry", AsyncMock()) as notify_admins,
            patch.object(main_core, "_upsert_contest_user_start", AsyncMock()),
            patch.object(main_core.bot, "copy_message", AsyncMock(return_value=SimpleNamespace(message_id=701))),
        ):
            await main_core.contest_submission_media(message, state)

        create_entry.assert_awaited_once_with(88, -100123, 701)
        notify_admins.assert_awaited_once_with(33)
        state.clear.assert_awaited_once()

    async def test_submission_media_replaces_pending_entry(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private", id=999),
            from_user=SimpleNamespace(id=88),
            text=None,
            media_group_id=None,
            photo=None,
            document=SimpleNamespace(mime_type="image/png"),
            message_id=502,
            answer=AsyncMock(),
        )
        state = AsyncMock()
        state.get_data = AsyncMock(return_value={"contest_pending_entry_id": 22})

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "_replace_pending_contest_entry", AsyncMock()) as replace_entry,
            patch.object(main_core, "_notify_admins_about_contest_entry", AsyncMock()) as notify_admins,
            patch.object(main_core, "_upsert_contest_user_start", AsyncMock()),
            patch.object(main_core.bot, "copy_message", AsyncMock(return_value=SimpleNamespace(message_id=702))),
        ):
            await main_core.contest_submission_media(message, state)

        replace_entry.assert_awaited_once_with(22, -100123, 702)
        notify_admins.assert_awaited_once_with(22)
        state.clear.assert_awaited_once()

    async def test_contest_admin_entry_keyboard_contains_required_actions(self):
        keyboard = main_core.contest_admin_entry_keyboard(11, 77)
        rows = keyboard.inline_keyboard
        self.assertEqual(rows[0][0].text, "✅ Принять рисунок")
        self.assertEqual(rows[0][0].callback_data, "contest:approve:11")
        self.assertEqual(rows[1][0].text, "❌ Отклонить")
        self.assertEqual(rows[1][0].callback_data, "contest:reject:11")
        self.assertEqual(rows[2][0].text, "💬 Связаться с участником")
        self.assertEqual(rows[2][0].url, "tg://user?id=77")


if __name__ == "__main__":
    unittest.main()