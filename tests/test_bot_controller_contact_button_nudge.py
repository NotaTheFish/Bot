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


class ContactButtonNudgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_in_private_sends_pre_reply_and_sets_awaiting_flag(self):
        message = SimpleNamespace(
            text="/start",
            chat=SimpleNamespace(type="private", id=111),
            from_user=SimpleNamespace(id=42),
            answer=AsyncMock(),
        )
        state = AsyncMock()
        pool = AsyncMock()

        with (
            patch.object(main_core, "send_buyer_pre_reply", AsyncMock()) as send_pre_reply,
            patch.object(main_core, "get_db_pool", AsyncMock(return_value=pool)),
            patch.object(main_core, "is_admin_user", return_value=False),
        ):
            await main_core.on_start(message, state)

        send_pre_reply.assert_awaited_once_with(111)
        pool.execute.assert_awaited_once()
        sql = pool.execute.await_args.args[0]
        self.assertIn("awaiting_contact_button", sql)
        self.assertIn("TRUE", sql)
        self.assertEqual(pool.execute.await_args.args[1], 42)

    async def test_private_message_before_button_sends_nudge_instead_of_pre_reply(self):
        message = SimpleNamespace(
            text="Привет",
            chat=SimpleNamespace(type="private", id=222),
            from_user=SimpleNamespace(id=43),
            answer=AsyncMock(),
        )
        state = AsyncMock()
        state.get_state = AsyncMock(return_value=None)

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_buyer_state", AsyncMock(return_value={"awaiting_contact_button": True})),
            patch.object(main_core, "try_touch_buyer_button_nudge_cooldown", AsyncMock(return_value=True)),
            patch.object(main_core, "send_buyer_pre_reply", AsyncMock()) as send_pre_reply,
        ):
            await main_core.private_message_fallback(message, state)

        send_pre_reply.assert_not_called()
        message.answer.assert_awaited_once()
        answer_text = message.answer.await_args.args[0]
        self.assertIn("Секундочку", answer_text)
        self.assertEqual(
            message.answer.await_args.kwargs["reply_markup"],
            main_core.buyer_contact_inline_keyboard(),
        )

    async def test_antispam_two_quick_messages_send_nudge_only_once_per_30_seconds(self):
        state = AsyncMock()
        state.get_state = AsyncMock(return_value=None)

        first_message = SimpleNamespace(
            text="Первое",
            chat=SimpleNamespace(type="private", id=333),
            from_user=SimpleNamespace(id=44),
            answer=AsyncMock(),
        )
        second_message = SimpleNamespace(
            text="Второе",
            chat=SimpleNamespace(type="private", id=333),
            from_user=SimpleNamespace(id=44),
            answer=AsyncMock(),
        )

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_buyer_state", AsyncMock(return_value={"awaiting_contact_button": True})),
            patch.object(
                main_core,
                "try_touch_buyer_button_nudge_cooldown",
                AsyncMock(side_effect=[True, False]),
            ) as touch_cooldown,
            patch.object(main_core, "send_buyer_pre_reply", AsyncMock()) as send_pre_reply,
        ):
            await main_core.private_message_fallback(first_message, state)
            await main_core.private_message_fallback(second_message, state)

        self.assertEqual(touch_cooldown.await_count, 2)
        first_message.answer.assert_awaited_once()
        second_message.answer.assert_not_called()
        send_pre_reply.assert_not_called()

    async def test_pressing_reply_and_inline_buttons_resets_awaiting_flag(self):
        message = SimpleNamespace(
            text="✉️ Связаться с продавцом",
            chat=SimpleNamespace(type="private", id=444),
            from_user=SimpleNamespace(id=45),
            answer=AsyncMock(),
        )
        callback_message = SimpleNamespace(
            chat=SimpleNamespace(type="private", id=444),
            answer=AsyncMock(),
            edit_reply_markup=AsyncMock(),
        )
        callback = SimpleNamespace(
            data="contact:open",
            from_user=SimpleNamespace(id=45),
            message=callback_message,
            answer=AsyncMock(),
        )

        reply_state = AsyncMock()
        reply_state.get_state = AsyncMock(return_value=None)

        inline_state = AsyncMock()
        inline_state.get_state = AsyncMock(return_value=None)
        inline_state.get_data = AsyncMock(return_value={})

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_cooldown_remaining", AsyncMock(return_value=0)),
            patch.object(main_core, "set_next_allowed", AsyncMock()),
            patch.object(main_core, "set_buyer_awaiting_contact_button", AsyncMock()) as set_awaiting,
        ):
            await main_core.buyer_contact_start(message, reply_state)
            await main_core.buyer_contact_start_inline(callback, inline_state)

        set_awaiting.assert_any_await(45, False)
        self.assertEqual(set_awaiting.await_count, 2)

    async def test_after_pressing_button_contact_flow_still_forwards_message(self):
        message = SimpleNamespace(
            text="Нужна помощь",
            chat=SimpleNamespace(type="private", id=555),
            from_user=SimpleNamespace(id=46),
            answer=AsyncMock(),
        )
        state = AsyncMock()
        state.get_data = AsyncMock(return_value={"contact_token": "support"})

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "_notify_admin_about_buyer_message", AsyncMock()) as notify_admin,
            patch.object(main_core, "_send_buyer_reply", AsyncMock()) as send_post_reply,
        ):
            await main_core.contact_message_forward(message, state)

        notify_admin.assert_awaited_once_with(message, "support")
        send_post_reply.assert_awaited_once_with(555)
        state.clear.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()