import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiogram.exceptions import TelegramForbiddenError
from aiogram.types import MessageEntity

os.environ.setdefault("BOT_TOKEN", "123:abc")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("DATABASE_URL", "postgres://localhost/db")
os.environ.setdefault("STORAGE_CHAT_ID", "-100123")

from bot_controller import main_core


class ParseUserIdsCsvTests(unittest.TestCase):
    def test_parse_user_ids_csv_empty_string_returns_empty_list(self):
        self.assertEqual(main_core.parse_user_ids_csv(""), [])

    def test_parse_user_ids_csv_zero_string_returns_empty_list(self):
        self.assertEqual(main_core.parse_user_ids_csv("0"), [])

    def test_parse_user_ids_csv_valid_csv_returns_int_list(self):
        self.assertEqual(main_core.parse_user_ids_csv("123,456"), [123, 456])

    def test_parse_user_ids_csv_ignores_invalid_and_non_positive_tokens(self):
        self.assertEqual(main_core.parse_user_ids_csv("123,abc,, -5, 0"), [123])



class ContestSubmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_admin_menu_contains_contest_button(self):
        keyboard = main_core.admin_menu_keyboard()
        flat = [button.text for row in keyboard.keyboard for button in row]
        self.assertIn("🏆 Конкурс", flat)

    async def test_buyer_keyboard_contains_contact_and_contest_buttons(self):
        keyboard = main_core.buyer_contact_keyboard()
        flat = [button.text for row in keyboard.keyboard for button in row]
        self.assertEqual(flat, ["✉️ Связаться с продавцом", "🏆 Конкурс талантов"])

    async def test_contest_announcement_and_rules_store_entities_in_fsm(self):
        announcement_entities = [MessageEntity(type="bold", offset=0, length=8)]
        rules_entities = [MessageEntity(type="italic", offset=0, length=6)]
        state = AsyncMock()

        announcement_message = SimpleNamespace(
            chat=SimpleNamespace(id=10),
            text="🏆 Анонс",
            caption=None,
            entities=announcement_entities,
            caption_entities=None,
            answer=AsyncMock(),
        )
        rules_message = SimpleNamespace(
            chat=SimpleNamespace(id=10),
            text="Правила",
            caption=None,
            entities=rules_entities,
            caption_entities=None,
            answer=AsyncMock(),
        )

        with (
            patch.object(main_core, "ensure_admin", return_value=True),
            patch.object(main_core, "_send_contest_editor_summary", AsyncMock()) as send_summary,
        ):
            await main_core.contest_announcement_received(announcement_message, state)
            await main_core.contest_rules_received(rules_message, state)

        expected_announcement_entities = main_core._serialize_entities(announcement_entities)
        expected_rules_entities = main_core._serialize_entities(rules_entities)
        state.update_data.assert_any_await(
            contest_announcement_text="🏆 Анонс",
            contest_announcement_entities_json=expected_announcement_entities,
        )
        state.update_data.assert_any_await(
            contest_rules_text="Правила",
            contest_rules_entities_json=expected_rules_entities,
        )
        self.assertEqual(send_summary.await_count, 2)

    async def test_contest_announcement_ignores_system_buttons_without_overwriting_state(self):
        state = AsyncMock()
        message = SimpleNamespace(
            chat=SimpleNamespace(id=10),
            text="👥 Режим: Участники",
            caption=None,
            entities=[MessageEntity(type="bold", offset=0, length=5)],
            caption_entities=None,
            answer=AsyncMock(),
        )

        with (
            patch.object(main_core, "ensure_admin", return_value=True),
            patch.object(main_core, "_send_contest_editor_summary", AsyncMock()) as send_summary,
        ):
            await main_core.contest_announcement_received(message, state)

        state.update_data.assert_not_awaited()
        send_summary.assert_not_awaited()
        message.answer.assert_awaited_once_with("Сначала завершите редактирование или нажмите Сохранить/Отмена")

    async def test_contest_rules_ignores_system_buttons_without_overwriting_state(self):
        state = AsyncMock()
        message = SimpleNamespace(
            chat=SimpleNamespace(id=10),
            text="👥 Режим: Участники",
            caption=None,
            entities=[MessageEntity(type="italic", offset=0, length=5)],
            caption_entities=None,
            answer=AsyncMock(),
        )

        with (
            patch.object(main_core, "ensure_admin", return_value=True),
            patch.object(main_core, "_send_contest_editor_summary", AsyncMock()) as send_summary,
        ):
            await main_core.contest_rules_received(message, state)

        state.update_data.assert_not_awaited()
        send_summary.assert_not_awaited()
        message.answer.assert_awaited_once_with("Сначала завершите редактирование или нажмите Сохранить/Отмена")

    async def test_on_start_sends_pre_reply_before_contest_announcement(self):
        order: list[str] = []

        async def pre_reply(_: int):
            order.append("pre_reply")

        async def send_announcement(*args, **kwargs):
            del args, kwargs
            order.append("announcement")

        message = SimpleNamespace(
            text="/start",
            chat=SimpleNamespace(type="private", id=999),
            from_user=SimpleNamespace(id=555),
            answer=AsyncMock(),
        )
        state = AsyncMock()
        pool = AsyncMock()
        pool.execute = AsyncMock()

        with (
            patch.object(main_core, "_upsert_contest_user_start", AsyncMock()),
            patch.object(main_core, "send_buyer_pre_reply", AsyncMock(side_effect=pre_reply)),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "visibility_mode": "participants", "announcement_text": "Анонс", "announcement_entities_json": None})),
            patch.object(main_core, "get_db_pool", AsyncMock(return_value=pool)),
            patch.object(main_core.bot, "send_message", AsyncMock(side_effect=send_announcement)),
        ):
            await main_core.on_start(message, state)

        self.assertGreaterEqual(len(order), 2)
        self.assertEqual(order[0], "pre_reply")
        self.assertEqual(order[1], "announcement")

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


    async def test_contest_admin_keyboard_has_home_button(self):
        with patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"visibility_mode": "admin"})):
            keyboard = await main_core.contest_admin_menu_keyboard()
        rows = [[button.text for button in row] for row in keyboard.keyboard]
        self.assertEqual(rows[2], ["🖼 Заявки конкурса", "🧪 Тест Mini App"])
        self.assertEqual(rows[3], ["🏠 Главное меню"])


    async def test_admin_test_mini_app_without_url(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private"),
            answer=AsyncMock(),
        )
        with (
            patch.object(main_core, "ensure_admin", return_value=True),
            patch.object(main_core, "CONTEST_WEBAPP_URL", ""),
        ):
            await main_core.contest_admin_test_mini_app(message)

        message.answer.assert_awaited_once_with("Mini App ещё не настроен")

    async def test_admin_test_mini_app_sends_webapp_button(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private"),
            answer=AsyncMock(),
        )
        with (
            patch.object(main_core, "ensure_admin", return_value=True),
            patch.object(main_core, "CONTEST_WEBAPP_URL", "https://example.com/contest"),
        ):
            await main_core.contest_admin_test_mini_app(message)

        message.answer.assert_awaited_once()
        self.assertEqual(message.answer.await_args.args[0], "Откройте приложение для теста голосования:")
        reply_markup = message.answer.await_args.kwargs["reply_markup"]
        button = reply_markup.inline_keyboard[0][0]
        self.assertEqual(button.text, "🗳 Открыть голосование")
        self.assertEqual(button.web_app.url, "https://example.com/contest")

    async def test_vote_button_uses_webapp_when_configured(self):
        message = SimpleNamespace(answer=AsyncMock(), from_user=SimpleNamespace(id=77))
        with (
            patch.object(main_core, "CONTEST_WEBAPP_URL", "https://example.com/contest"),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "visibility_mode": "participants"})),
            patch.object(main_core, "ensure_admin", return_value=False),
        ):
            await main_core.contest_vote_placeholder(message)
        message.answer.assert_awaited_once()
        self.assertEqual(message.answer.await_args.args[0], "Откройте приложение для голосования:")
        reply_markup = message.answer.await_args.kwargs["reply_markup"]
        button = reply_markup.inline_keyboard[0][0]
        self.assertEqual(button.text, "🗳 Открыть голосование")
        self.assertEqual(button.web_app.url, "https://example.com/contest")

    async def test_vote_button_shows_fallback_without_webapp_url(self):
        message = SimpleNamespace(answer=AsyncMock(), from_user=SimpleNamespace(id=77))
        with (
            patch.object(main_core, "CONTEST_WEBAPP_URL", ""),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "visibility_mode": "participants"})),
            patch.object(main_core, "ensure_admin", return_value=False),
        ):
            await main_core.contest_vote_placeholder(message)
        message.answer.assert_awaited_once()
        self.assertEqual(message.answer.await_args.args[0], "Mini App ещё не настроен. Обратитесь к администратору.")

    async def test_submit_start_without_pending_entry_sets_waiting_media_state(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private"),
            from_user=SimpleNamespace(id=77),
            answer=AsyncMock(),
        )
        state = AsyncMock()

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "participants"})),
            patch.object(main_core, "_get_contest_submission_block_reason", AsyncMock(return_value=None)),
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
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "participants"})),
            patch.object(main_core, "_get_contest_submission_block_reason", AsyncMock(return_value=None)),
            patch.object(main_core, "_get_pending_contest_entry", AsyncMock(return_value={"id": 15})),
        ):
            await main_core.contest_submit_start(message, state)

        state.set_state.assert_awaited_once_with(main_core.ContestStates.waiting_replace_confirmation)
        state.update_data.assert_awaited_once_with(contest_pending_entry_id=15)

    async def test_second_submission_requires_replace_or_cancel_choice(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private"),
            from_user=SimpleNamespace(id=77),
            answer=AsyncMock(),
        )
        state = AsyncMock()

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "participants"})),
            patch.object(main_core, "_get_contest_submission_block_reason", AsyncMock(return_value=None)),
            patch.object(main_core, "_get_pending_contest_entry", AsyncMock(return_value={"id": 15})),
        ):
            await main_core.contest_submit_start(message, state)

        message.answer.assert_awaited_once()
        self.assertIn("У вас уже есть активная заявка", message.answer.await_args.args[0])

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
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "participants"})),
            patch.object(main_core, "_get_contest_submission_block_reason", AsyncMock(return_value=None)),
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
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "participants"})),
            patch.object(main_core, "_get_contest_submission_block_reason", AsyncMock(return_value=None)),
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
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "participants"})),
            patch.object(main_core, "_get_contest_submission_block_reason", AsyncMock(return_value=None)),
            patch.object(main_core, "_replace_pending_contest_entry", AsyncMock()) as replace_entry,
            patch.object(main_core, "_notify_admins_about_contest_entry", AsyncMock()) as notify_admins,
            patch.object(main_core, "_upsert_contest_user_start", AsyncMock()),
            patch.object(main_core.bot, "copy_message", AsyncMock(return_value=SimpleNamespace(message_id=702))),
        ):
            await main_core.contest_submission_media(message, state)

        replace_entry.assert_awaited_once_with(22, -100123, 702)
        notify_admins.assert_awaited_once_with(22)
        state.clear.assert_awaited_once()

    async def test_replace_flow_updates_existing_entry_without_duplicate_creation(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private", id=999),
            from_user=SimpleNamespace(id=88),
            text=None,
            media_group_id=None,
            photo=[SimpleNamespace(file_id="b")],
            document=None,
            message_id=503,
            answer=AsyncMock(),
        )
        state = AsyncMock()
        state.get_data = AsyncMock(return_value={"contest_pending_entry_id": 22})

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "participants"})),
            patch.object(main_core, "_get_contest_submission_block_reason", AsyncMock(return_value=None)),
            patch.object(main_core, "_create_pending_contest_entry", AsyncMock()) as create_entry,
            patch.object(main_core, "_replace_pending_contest_entry", AsyncMock()) as replace_entry,
            patch.object(main_core, "_notify_admins_about_contest_entry", AsyncMock()),
            patch.object(main_core, "_upsert_contest_user_start", AsyncMock()),
            patch.object(main_core.bot, "copy_message", AsyncMock(return_value=SimpleNamespace(message_id=703))),
        ):
            await main_core.contest_submission_media(message, state)

        replace_entry.assert_awaited_once_with(22, -100123, 703)
        create_entry.assert_not_called()

    async def test_submit_start_blocks_with_disabled_contest(self):
        message = SimpleNamespace(chat=SimpleNamespace(type="private"), from_user=SimpleNamespace(id=77), answer=AsyncMock())
        state = AsyncMock()

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": False, "submission_open": True, "visibility_mode": "participants"})),
        ):
            await main_core.contest_submit_start(message, state)

        state.clear.assert_awaited_once()
        self.assertEqual(message.answer.await_args.args[0], "Конкурс сейчас отключён. Подача заявок недоступна.")

    async def test_submit_start_blocks_with_closed_submission(self):
        message = SimpleNamespace(chat=SimpleNamespace(type="private"), from_user=SimpleNamespace(id=77), answer=AsyncMock())
        state = AsyncMock()

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": False, "visibility_mode": "participants"})),
        ):
            await main_core.contest_submit_start(message, state)

        state.clear.assert_awaited_once()
        self.assertEqual(message.answer.await_args.args[0], "Приём заявок сейчас закрыт.")


    async def test_tester_receives_announcement_on_start_in_admin_only_mode(self):
        message = SimpleNamespace(
            text="/start",
            chat=SimpleNamespace(type="private", id=999),
            from_user=SimpleNamespace(id=555),
            answer=AsyncMock(),
        )
        state = AsyncMock()
        pool = AsyncMock()
        pool.execute = AsyncMock()

        with (
            patch.object(main_core, "_upsert_contest_user_start", AsyncMock()),
            patch.object(main_core, "send_buyer_pre_reply", AsyncMock()),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "visibility_mode": "admin_only", "announcement_text": "Анонс", "announcement_entities_json": None})),
            patch.object(main_core, "get_db_pool", AsyncMock(return_value=pool)),
            patch.object(main_core, "is_admin_user", return_value=False),
            patch.object(main_core, "CONTEST_TESTER_IDS_LIST", [555]),
            patch.object(main_core.bot, "send_message", AsyncMock()) as send_message,
        ):
            await main_core.on_start(message, state)

        send_message.assert_awaited_once()
        self.assertEqual(send_message.await_args.args[0], 999)
        self.assertEqual(send_message.await_args.kwargs["text"], "Анонс")

    async def test_tester_can_open_contest_menu_in_admin_only_mode(self):
        message = SimpleNamespace(from_user=SimpleNamespace(id=555), answer=AsyncMock())
        state = AsyncMock()

        with (
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "visibility_mode": "admin_only"})),
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "is_admin_user", return_value=False),
            patch.object(main_core, "CONTEST_TESTER_IDS_LIST", [555]),
        ):
            await main_core.buyer_contest_open(message, state)

        state.clear.assert_awaited_once()
        message.answer.assert_awaited_once()
        self.assertEqual(message.answer.await_args.args[0], "🏆 Меню конкурса:")

    async def test_non_admin_tester_gets_user_contest_flow_in_admin_only_mode(self):
        message = SimpleNamespace(from_user=SimpleNamespace(id=555), answer=AsyncMock())
        state = AsyncMock()

        with (
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "visibility_mode": "admin_only"})),
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "is_admin_user", return_value=False),
            patch.object(main_core, "CONTEST_TESTER_IDS_LIST", [555]),
        ):
            await main_core.buyer_contest_open(message, state)

        message.answer.assert_awaited_once_with("🏆 Меню конкурса:", reply_markup=main_core.contest_user_keyboard())

    async def test_non_admin_non_tester_denied_in_admin_only_mode(self):
        message = SimpleNamespace(from_user=SimpleNamespace(id=777), answer=AsyncMock())
        state = AsyncMock()

        with (
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "visibility_mode": "admin_only"})),
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "is_admin_user", return_value=False),
            patch.object(main_core, "CONTEST_TESTER_IDS_LIST", [555]),
        ):
            await main_core.buyer_contest_open(message, state)

        state.clear.assert_not_awaited()
        message.answer.assert_awaited_once_with(
            "Конкурс сейчас недоступен для вашего профиля.",
            reply_markup=main_core.buyer_contact_keyboard(),
        )

    async def test_tester_submit_start_allowed_in_admin_only_mode(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private"),
            from_user=SimpleNamespace(id=555),
            answer=AsyncMock(),
        )
        state = AsyncMock()

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "admin_only"})),
            patch.object(main_core, "is_admin_user", return_value=False),
            patch.object(main_core, "CONTEST_TESTER_IDS_LIST", [555]),
            patch.object(main_core, "_ensure_contest_channel_subscription", AsyncMock(return_value=(True, None))),
            patch.object(main_core, "_get_pending_contest_entry", AsyncMock(return_value=None)),
        ):
            await main_core.contest_submit_start(message, state)

        state.set_state.assert_awaited_once_with(main_core.ContestStates.waiting_submission_media)
        state.update_data.assert_awaited_once_with(contest_pending_entry_id=None)

    async def test_submit_start_blocks_with_restricted_visibility_mode(self):
        message = SimpleNamespace(chat=SimpleNamespace(type="private"), from_user=SimpleNamespace(id=77), answer=AsyncMock())
        state = AsyncMock()

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "admin_only"})),
            patch.object(main_core, "is_admin_user", return_value=False),
        ):
            await main_core.contest_submit_start(message, state)

        state.clear.assert_awaited_once()
        self.assertEqual(message.answer.await_args.args[0], "Подача заявок недоступна для вашего режима конкурса.")

    async def test_admin_can_open_user_contest_menu_in_test_mode(self):
        message = SimpleNamespace(from_user=SimpleNamespace(id=1), answer=AsyncMock())
        state = AsyncMock()

        with (
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "visibility_mode": "admin_only"})),
            patch.object(main_core, "ensure_admin", return_value=True),
        ):
            await main_core.buyer_contest_open(message, state)

        state.clear.assert_awaited_once()
        message.answer.assert_awaited_once()
        self.assertEqual(message.answer.await_args.args[0], "🏆 Меню конкурса:")

    async def test_admin_vote_button_available_in_test_mode(self):
        message = SimpleNamespace(from_user=SimpleNamespace(id=1), answer=AsyncMock())

        with (
            patch.object(main_core, "CONTEST_WEBAPP_URL", "https://example.com/contest"),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "visibility_mode": "admin_only"})),
            patch.object(main_core, "ensure_admin", return_value=True),
        ):
            await main_core.contest_vote_placeholder(message)

        message.answer.assert_awaited_once()
        self.assertEqual(message.answer.await_args.args[0], "Откройте приложение для голосования:")

    async def test_admin_submit_start_allowed_in_test_mode(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private"),
            from_user=SimpleNamespace(id=1),
            answer=AsyncMock(),
        )
        state = AsyncMock()

        with (
            patch.object(main_core, "ensure_admin", return_value=True),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "admin_only"})),
            patch.object(main_core, "_ensure_contest_channel_subscription", AsyncMock(return_value=(True, None))),
            patch.object(main_core, "_get_pending_contest_entry", AsyncMock(return_value=None)),
        ):
            await main_core.contest_submit_start(message, state)

        state.set_state.assert_awaited_once_with(main_core.ContestStates.waiting_submission_media)
        state.update_data.assert_awaited_once_with(contest_pending_entry_id=None)

    async def test_admin_submission_media_allowed_in_test_mode(self):
        message = SimpleNamespace(
            chat=SimpleNamespace(type="private", id=999),
            from_user=SimpleNamespace(id=1),
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
            patch.object(main_core, "ensure_admin", return_value=True),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "admin_only"})),
            patch.object(main_core, "_ensure_contest_channel_subscription", AsyncMock(return_value=(True, None))),
            patch.object(main_core, "_create_pending_contest_entry", AsyncMock()) as create_entry,
            patch.object(main_core, "_get_pending_contest_entry", AsyncMock(return_value={"id": 33})),
            patch.object(main_core, "_notify_admins_about_contest_entry", AsyncMock()),
            patch.object(main_core, "_upsert_contest_user_start", AsyncMock()),
            patch.object(main_core.bot, "copy_message", AsyncMock(return_value=SimpleNamespace(message_id=701))),
        ):
            await main_core.contest_submission_media(message, state)

        create_entry.assert_awaited_once_with(1, -100123, 701)
        state.clear.assert_awaited_once()

    async def test_submit_start_blocks_with_missing_channel_subscription(self):
        message = SimpleNamespace(chat=SimpleNamespace(type="private"), from_user=SimpleNamespace(id=77), answer=AsyncMock())
        state = AsyncMock()

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": True, "submission_open": True, "visibility_mode": "participants"})),
            patch.object(main_core, "_ensure_contest_channel_subscription", AsyncMock(return_value=(False, "Чтобы подать рисунок, подпишитесь на канал конкурса и попробуйте снова."))),
        ):
            await main_core.contest_submit_start(message, state)

        state.clear.assert_awaited_once()
        self.assertEqual(message.answer.await_args.args[0], "Чтобы подать рисунок, подпишитесь на канал конкурса и попробуйте снова.")

    async def test_submission_media_defensively_blocks_when_contest_disabled(self):
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

        with (
            patch.object(main_core, "ensure_admin", return_value=False),
            patch.object(main_core, "get_contest_settings", AsyncMock(return_value={"enabled": False, "submission_open": True, "visibility_mode": "participants"})),
            patch.object(main_core, "_upsert_contest_user_start", AsyncMock()) as upsert_user,
        ):
            await main_core.contest_submission_media(message, state)

        state.clear.assert_awaited_once()
        upsert_user.assert_not_called()
        self.assertEqual(message.answer.await_args.args[0], "Конкурс сейчас отключён. Подача заявок недоступна.")

    async def test_approve_changes_status_and_notifies_participant(self):
        callback = SimpleNamespace(
            data="contest:approve:11",
            from_user=SimpleNamespace(id=1),
            answer=AsyncMock(),
            message=SimpleNamespace(answer=AsyncMock(), edit_reply_markup=AsyncMock()),
        )

        with (
            patch.object(main_core, "is_admin_user", return_value=True),
            patch.object(main_core, "_set_contest_entry_approved", AsyncMock(return_value=88)) as approve_entry,
            patch.object(main_core.bot, "send_message", AsyncMock()) as send_message,
        ):
            await main_core.contest_entry_approve(callback)

        approve_entry.assert_awaited_once_with(11, 1)
        send_message.assert_awaited_once_with(88, "Ваш рисунок принят ✅")

    async def test_reject_requests_reason_and_persists_it(self):
        callback = SimpleNamespace(
            data="contest:reject:12",
            from_user=SimpleNamespace(id=1),
            answer=AsyncMock(),
            message=SimpleNamespace(answer=AsyncMock()),
        )
        state = AsyncMock()

        with patch.object(main_core, "is_admin_user", return_value=True):
            await main_core.contest_entry_reject_start(callback, state)

        state.set_state.assert_awaited_once_with(main_core.AdminStates.waiting_contest_reject_reason)
        state.update_data.assert_awaited_once_with(contest_reject_entry_id=12)

        reject_message = SimpleNamespace(
            text="Есть проблема с оформлением",
            from_user=SimpleNamespace(id=1),
            chat=SimpleNamespace(type="private"),
            answer=AsyncMock(),
        )
        reject_state = AsyncMock()
        reject_state.get_data = AsyncMock(return_value={"contest_reject_entry_id": 12})

        with (
            patch.object(main_core, "ensure_admin", return_value=True),
            patch.object(main_core, "_set_contest_entry_rejected", AsyncMock(return_value=88)) as reject_entry,
            patch.object(main_core.bot, "send_message", AsyncMock()) as send_message,
        ):
            await main_core.contest_entry_reject_reason(reject_message, reject_state)

        reject_entry.assert_awaited_once_with(12, 1, "Есть проблема с оформлением")
        send_message.assert_awaited_once_with(88, "Ваш рисунок отклонён ❌\nПричина: Есть проблема с оформлением")

    async def test_delete_marks_entry_deleted_and_notifies_participant(self):
        callback = SimpleNamespace(
            data="contest:delete:15",
            from_user=SimpleNamespace(id=1),
            answer=AsyncMock(),
            message=SimpleNamespace(answer=AsyncMock(), edit_reply_markup=AsyncMock()),
        )

        with (
            patch.object(main_core, "is_admin_user", return_value=True),
            patch.object(main_core, "_mark_contest_entry_deleted", AsyncMock(return_value=88)) as delete_entry,
            patch.object(main_core.bot, "send_message", AsyncMock()) as send_message,
        ):
            await main_core.contest_entry_delete(callback)

        delete_entry.assert_awaited_once_with(15, 1)
        send_message.assert_awaited_once_with(88, "Ваш рисунок был удалён организатором.")
        callback.answer.assert_awaited_once_with("Рисунок удалён")
        callback.message.answer.assert_awaited_once_with("Рисунок в заявке #15 удалён 🗑")

    async def test_mark_contest_entry_deleted_uses_soft_delete_sql_fields(self):
        pool = AsyncMock()
        pool.fetchrow = AsyncMock(return_value={"owner_user_id": 88})

        with patch.object(main_core, "get_db_pool", AsyncMock(return_value=pool)):
            participant_id = await main_core._mark_contest_entry_deleted(15, 1)

        self.assertEqual(participant_id, 88)
        query = pool.fetchrow.await_args.args[0]
        self.assertIn("SET is_deleted = TRUE", query)
        self.assertIn("deleted_at = NOW()", query)
        self.assertIn("deleted_by_admin_id = $2", query)

    async def test_delete_handler_does_not_crash_when_participant_unreachable(self):
        callback = SimpleNamespace(
            data="contest:delete:15",
            from_user=SimpleNamespace(id=1),
            answer=AsyncMock(),
            message=SimpleNamespace(answer=AsyncMock(), edit_reply_markup=AsyncMock()),
        )

        with (
            patch.object(main_core, "is_admin_user", return_value=True),
            patch.object(main_core, "_mark_contest_entry_deleted", AsyncMock(return_value=88)),
            patch.object(main_core.bot, "send_message", AsyncMock(side_effect=TelegramForbiddenError(method="sendMessage", message="blocked"))),
        ):
            await main_core.contest_entry_delete(callback)

        callback.answer.assert_awaited_once_with("Рисунок удалён")
        callback.message.answer.assert_awaited_once_with("Рисунок в заявке #15 удалён 🗑")


    async def test_reject_reason_ignores_contest_system_buttons(self):
        reject_message = SimpleNamespace(
            text="⬅️ Назад",
            from_user=SimpleNamespace(id=1),
            chat=SimpleNamespace(type="private"),
            answer=AsyncMock(),
        )
        reject_state = AsyncMock()
        reject_state.get_data = AsyncMock(return_value={"contest_reject_entry_id": 12})

        with (
            patch.object(main_core, "ensure_admin", return_value=True),
            patch.object(main_core, "_set_contest_entry_rejected", AsyncMock()) as reject_entry,
        ):
            await main_core.contest_entry_reject_reason(reject_message, reject_state)

        reject_entry.assert_not_awaited()
        reject_state.clear.assert_not_awaited()
        reject_message.answer.assert_awaited_once_with(
            "Сначала завершите отклонение или нажмите Сохранить/Главное меню"
        )


    async def test_notify_admins_about_contest_entry_sends_only_card_without_second_message(self):
        pool = AsyncMock()
        pool.fetchrow = AsyncMock(
            return_value={
                "id": 42,
                "owner_user_id": 77,
                "storage_chat_id": -100500,
                "storage_message_id": 901,
                "status": "pending",
                "username": "artist77",
                "first_name": "Иван",
                "last_name": "Петров",
            }
        )

        with (
            patch.object(main_core, "get_db_pool", AsyncMock(return_value=pool)),
            patch.object(main_core, "ADMIN_IDS_LIST", [1001, 1002]),
            patch.object(main_core.bot, "copy_message", AsyncMock()) as copy_message,
            patch.object(main_core.bot, "send_message", AsyncMock()) as send_message,
        ):
            await main_core._notify_admins_about_contest_entry(42)

        self.assertEqual(copy_message.await_count, 2)
        send_message.assert_not_awaited()

    async def test_notify_admins_about_contest_entry_caption_contains_user_id_and_contact_link_uses_owner_id(self):
        pool = AsyncMock()
        pool.fetchrow = AsyncMock(
            return_value={
                "id": 42,
                "owner_user_id": 77,
                "storage_chat_id": -100500,
                "storage_message_id": 901,
                "status": "pending",
                "username": "nickname",
                "first_name": "Иван",
                "last_name": "Петров",
            }
        )

        with (
            patch.object(main_core, "get_db_pool", AsyncMock(return_value=pool)),
            patch.object(main_core, "ADMIN_IDS_LIST", [1001]),
            patch.object(main_core.bot, "copy_message", AsyncMock()) as copy_message,
        ):
            await main_core._notify_admins_about_contest_entry(42)

        copy_message.assert_awaited_once()
        kwargs = copy_message.await_args.kwargs
        self.assertIn("user_id: <code>77</code>", kwargs["caption"])
        self.assertEqual(kwargs["reply_markup"].inline_keyboard[3][0].url, "tg://user?id=77")

    async def test_contest_admin_entry_keyboard_contains_required_actions(self):
        keyboard = main_core.contest_admin_entry_keyboard(11, 77)
        rows = keyboard.inline_keyboard
        self.assertEqual(rows[0][0].text, "✅ Принять рисунок")
        self.assertEqual(rows[0][0].callback_data, "contest:approve:11")
        self.assertEqual(rows[1][0].text, "❌ Отклонить")
        self.assertEqual(rows[1][0].callback_data, "contest:reject:11")
        self.assertEqual(rows[2][0].text, "🗑 Удалить рисунок")
        self.assertEqual(rows[2][0].callback_data, "contest:delete:11")
        self.assertEqual(rows[3][0].text, "💬 Связаться с участником")
        self.assertEqual(rows[3][0].url, "tg://user?id=77")

    async def test_contest_schema_migration_contains_owner_fields_and_coalesce(self):
        migration_sql = "\n".join(main_core.CONTEST_MIGRATIONS_SQL)
        self.assertIn("owner_user_id", migration_sql)
        self.assertIn("COALESCE(owner_user_id, user_id)", migration_sql)
        self.assertIn("idx_contest_entries_owner_pending_unique", migration_sql)
        self.assertIn("is_deleted BOOLEAN NOT NULL DEFAULT FALSE", migration_sql)
        self.assertIn("deleted_at TIMESTAMPTZ", migration_sql)
        self.assertIn("deleted_by_admin_id", migration_sql)

    async def test_contest_entries_table_contains_soft_delete_fields(self):
        tables_sql = "\n".join(main_core.CONTEST_TABLES_SQL)
        self.assertIn("is_deleted BOOLEAN NOT NULL DEFAULT FALSE", tables_sql)
        self.assertIn("deleted_at TIMESTAMPTZ", tables_sql)
        self.assertIn("deleted_by_admin_id BIGINT", tables_sql)

    async def test_contest_settings_table_uses_jsonb_entities(self):
        tables_sql = "\n".join(main_core.CONTEST_TABLES_SQL)
        self.assertIn("announcement_entities_json JSONB", tables_sql)
        self.assertIn("rules_entities_json JSONB", tables_sql)
        self.assertIn("visibility_mode IN ('admin_only', 'participants')", tables_sql)


if __name__ == "__main__":
    unittest.main()