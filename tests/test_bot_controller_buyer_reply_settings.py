import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("BOT_TOKEN", "123:abc")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("DATABASE_URL", "postgres://localhost/db")
os.environ.setdefault("STORAGE_CHAT_ID", "-100123")
os.environ.setdefault("SELLER_USERNAME", "seller_login")

from bot_controller import main_core


class BuyerReplySettingsHandlersTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_shows_variant_keyboard_and_sets_choice_state(self):
        state = AsyncMock()
        message = AsyncMock()
        message.from_user = SimpleNamespace(id=1)

        await main_core.buyer_reply_settings_start(message, state)

        state.set_state.assert_awaited_once_with(main_core.AdminStates.choosing_buyer_reply_variant)
        message.answer.assert_awaited_once()
        kwargs = message.answer.await_args.kwargs
        self.assertIn("reply_markup", kwargs)

    async def test_choose_pre_sets_waiting_pre_state(self):
        state = AsyncMock()
        callback = AsyncMock()
        callback.from_user = SimpleNamespace(id=1)
        callback.message = AsyncMock()

        await main_core.buyer_reply_choose_pre(callback, state)

        state.set_state.assert_awaited_once_with(main_core.AdminStates.waiting_buyer_reply_pre_text)
        callback.message.answer.assert_awaited_once()
        callback.answer.assert_awaited_once()

    async def test_save_pre_updates_db_and_sends_preview(self):
        state = AsyncMock()
        message = AsyncMock()
        message.from_user = SimpleNamespace(id=1)
        message.text = "Новый префикс"
        message.caption = None

        with (
            patch.object(main_core, "save_buyer_reply_pre_text", AsyncMock()) as save_pre,
            patch.object(main_core, "_send_buyer_reply_settings_preview", AsyncMock()) as send_preview,
        ):
            await main_core.buyer_reply_settings_save_pre(message, state)

        save_pre.assert_awaited_once_with("Новый префикс")
        state.clear.assert_awaited_once()
        send_preview.assert_awaited_once_with(message)

    async def test_save_post_updates_db_and_sends_preview(self):
        state = AsyncMock()
        message = AsyncMock()
        message.from_user = SimpleNamespace(id=1)
        message.text = "Новый постфикс"
        message.caption = None

        with (
            patch.object(main_core, "save_buyer_reply_post_text", AsyncMock()) as save_post,
            patch.object(main_core, "_send_buyer_reply_settings_preview", AsyncMock()) as send_preview,
        ):
            await main_core.buyer_reply_settings_save_post(message, state)

        save_post.assert_awaited_once_with("Новый постфикс")
        state.clear.assert_awaited_once()
        send_preview.assert_awaited_once_with(message)


if __name__ == "__main__":
    unittest.main()