import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from aiogram.fsm.storage.base import StorageKey

os.environ.setdefault("BOT_TOKEN", "123:abc")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("DATABASE_URL", "postgres://localhost/db")
os.environ.setdefault("STORAGE_CHAT_ID", "-100123")

from bot_controller import main_core


class StoragePostFlowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        main_core.media_group_buffers.clear()

    def test_dispatcher_uses_memory_storage(self):
        self.assertEqual(main_core.dp.storage.__class__.__name__, "ThreadAgnosticMemoryStorage")

    async def test_fsm_state_is_shared_between_topics_for_same_chat_and_user(self):
        storage = main_core.dp.storage
        key_topic_1 = StorageKey(bot_id=1, chat_id=-100123, user_id=1, thread_id=100, business_connection_id=None, destiny="default")
        key_topic_2 = StorageKey(bot_id=1, chat_id=-100123, user_id=1, thread_id=200, business_connection_id=None, destiny="default")

        await storage.set_state(key_topic_1, main_core.AdminStates.waiting_storage_post)
        state_in_another_topic = await storage.get_state(key_topic_2)

        self.assertEqual(state_in_another_topic, main_core.AdminStates.waiting_storage_post.state)

    async def test_create_post_command_outside_storage_for_admin(self):
        state = AsyncMock()
        message = SimpleNamespace(
            chat=SimpleNamespace(id=999),
            from_user=SimpleNamespace(id=1),
            text="/create_post",
            message_thread_id=None,
            answer=AsyncMock(),
        )

        await main_core.create_post_in_storage(message, state)

        message.answer.assert_awaited_once_with("используйте команду в Storage")
        state.set_state.assert_not_called()

    async def test_create_post_command_ignores_non_admin(self):
        state = AsyncMock()
        message = SimpleNamespace(
            chat=SimpleNamespace(id=-100123),
            from_user=SimpleNamespace(id=777),
            text="/create_post",
            message_thread_id=None,
            answer=AsyncMock(),
        )

        await main_core.create_post_in_storage(message, state)

        message.answer.assert_awaited_once_with("Недостаточно прав")
        state.set_state.assert_not_called()

    async def test_create_post_command_for_storage_admin_sets_state_and_answers(self):
        state = AsyncMock()
        state.get_state = AsyncMock(return_value=main_core.AdminStates.waiting_storage_post.state)
        message = SimpleNamespace(
            chat=SimpleNamespace(id=-100123),
            from_user=SimpleNamespace(id=1),
            text="/create_post",
            message_thread_id=None,
            answer=AsyncMock(),
        )

        await main_core.create_post_in_storage(message, state)

        state.set_state.assert_awaited_once_with(main_core.AdminStates.waiting_storage_post)
        message.answer.assert_awaited_once_with("Пришлите пост одним сообщением или альбомом. /cancel чтобы отменить.")

    async def test_single_storage_message_saves_storage_message_ids(self):
        state = AsyncMock()
        message = SimpleNamespace(
            chat=SimpleNamespace(id=-100123),
            from_user=SimpleNamespace(id=1),
            message_id=55,
            media_group_id=None,
            text="hello",
            caption=None,
            photo=None,
            video=None,
            animation=None,
            document=None,
            audio=None,
            voice=None,
            sticker=None,
            content_type="text",
            message_thread_id=None,
            answer=AsyncMock(),
        )

        with (
            patch.object(main_core, "get_storage_chat_id", AsyncMock(return_value=-100123)),
            patch.object(main_core, "get_post", AsyncMock(return_value={"storage_chat_id": -100123, "storage_message_ids": []})),
            patch.object(main_core, "_delete_previous_storage_post", AsyncMock(return_value=[])),
            patch.object(main_core, "set_last_storage_messages", AsyncMock()) as set_last,
            patch.object(main_core, "save_post", AsyncMock()) as save_post,
            patch.object(main_core, "_pin_storage_post", AsyncMock(return_value=None)),
            patch.object(main_core, "send_post_actions", AsyncMock()),
        ):
            await main_core.handle_storage_post(message, state)

        set_last.assert_awaited_once_with(-100123, [55])
        save_post.assert_awaited_once()
        self.assertEqual(save_post.await_args.kwargs["storage_message_ids"], [55])

    async def test_single_storage_message_answers_with_send_hint(self):
        state = AsyncMock()
        message = SimpleNamespace(
            chat=SimpleNamespace(id=-100123),
            from_user=SimpleNamespace(id=1),
            message_id=88,
            media_group_id=None,
            text="hello",
            caption=None,
            photo=None,
            video=None,
            animation=None,
            document=None,
            audio=None,
            voice=None,
            sticker=None,
            content_type="text",
            message_thread_id=None,
            answer=AsyncMock(),
        )

        with (
            patch.object(main_core, "get_storage_chat_id", AsyncMock(return_value=-100123)),
            patch.object(main_core, "get_post", AsyncMock(return_value={"storage_chat_id": -100123, "storage_message_ids": []})),
            patch.object(main_core, "_delete_previous_storage_post", AsyncMock(return_value=[])),
            patch.object(main_core, "set_last_storage_messages", AsyncMock()),
            patch.object(main_core, "save_post", AsyncMock()),
            patch.object(main_core, "_pin_storage_post", AsyncMock(return_value=None)),
            patch.object(main_core, "send_post_actions", AsyncMock()),
        ):
            await main_core.handle_storage_post(message, state)

        message.answer.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("✅ Сохранено и закреплено", reply_text)
        self.assertIn("Для отправки ипользуйте: ✅ Запустить рассылку", reply_text)

    async def test_single_storage_message_auto_queues_userbot_when_flag_enabled(self):
        state = AsyncMock()
        message = SimpleNamespace(
            chat=SimpleNamespace(id=-100123),
            from_user=SimpleNamespace(id=1),
            message_id=89,
            media_group_id=None,
            text="hello",
            caption=None,
            photo=None,
            video=None,
            animation=None,
            document=None,
            audio=None,
            voice=None,
            sticker=None,
            content_type="text",
            message_thread_id=None,
            answer=AsyncMock(),
        )

        with (
            patch.object(main_core, "CREATE_POST_AUTO_QUEUE_USERBOT", True),
            patch.object(main_core, "DELIVERY_MODE", "userbot"),
            patch.object(main_core, "TARGET_CHAT_IDS", [42, 24]),
            patch.object(main_core, "get_storage_chat_id", AsyncMock(return_value=-100123)),
            patch.object(main_core, "get_post", AsyncMock(return_value={"storage_chat_id": -100123, "storage_message_ids": []})),
            patch.object(main_core, "_delete_previous_storage_post", AsyncMock(return_value=[])),
            patch.object(main_core, "set_last_storage_messages", AsyncMock()),
            patch.object(main_core, "save_post", AsyncMock()),
            patch.object(main_core, "_pin_storage_post", AsyncMock(return_value=None)),
            patch.object(main_core, "send_post_actions", AsyncMock()),
            patch.object(main_core, "create_userbot_task", AsyncMock(return_value=777)) as create_task,
        ):
            await main_core.handle_storage_post(message, state)

        create_task.assert_awaited_once()
        reply_text = message.answer.await_args.args[0]
        self.assertIn("Userbot-задача автоматически поставлена в очередь.", reply_text)

    async def test_media_group_message_is_buffered(self):
        state = AsyncMock()
        message = SimpleNamespace(
            chat=SimpleNamespace(id=-100123),
            from_user=SimpleNamespace(id=1),
            message_id=101,
            media_group_id="group-1",
            text=None,
            caption="album",
            photo=[object()],
            video=None,
            document=None,
            animation=None,
            audio=None,
            voice=None,
            sticker=None,
            content_type="photo",
            message_thread_id=None,
            answer=AsyncMock(),
        )

        def _fake_create_task(coro):
            coro.close()
            return object()

        with patch.object(main_core.asyncio, "create_task", side_effect=_fake_create_task):
            await main_core.handle_storage_post(message, state)

        self.assertIn("group-1", main_core.media_group_buffers)
        self.assertEqual(main_core.media_group_buffers["group-1"]["messages"], [message])


    async def test_handle_storage_post_rejects_empty_service_message(self):
        state = AsyncMock()
        message = SimpleNamespace(
            chat=SimpleNamespace(id=-100123),
            from_user=SimpleNamespace(id=1),
            message_id=222,
            media_group_id=None,
            text=None,
            caption=None,
            photo=None,
            video=None,
            document=None,
            animation=None,
            audio=None,
            voice=None,
            sticker=None,
            content_type="service",
            message_thread_id=None,
            answer=AsyncMock(),
        )

        with patch.object(main_core, "_publish_post_messages", AsyncMock()) as publish:
            await main_core.handle_storage_post(message, state)

        publish.assert_not_called()
        message.answer.assert_awaited_once_with("Этот тип сообщения не поддерживается, пришлите текст/медиа/альбом.")


    async def test_album_flow_flushes_once_and_confirms(self):
        state = AsyncMock()

        first = SimpleNamespace(
            chat=SimpleNamespace(id=-100123),
            from_user=SimpleNamespace(id=1),
            message_id=301,
            media_group_id="album-42",
            text=None,
            caption="photo 1",
            photo=[object()],
            video=None,
            document=None,
            animation=None,
            audio=None,
            voice=None,
            sticker=None,
            content_type="photo",
            message_thread_id=None,
            answer=AsyncMock(),
        )
        second = SimpleNamespace(
            chat=SimpleNamespace(id=-100123),
            from_user=SimpleNamespace(id=1),
            message_id=302,
            media_group_id="album-42",
            text=None,
            caption="photo 2",
            photo=[object()],
            video=None,
            document=None,
            animation=None,
            audio=None,
            voice=None,
            sticker=None,
            content_type="photo",
            message_thread_id=None,
            answer=AsyncMock(),
        )

        created_coroutines = []

        def _fake_create_task(coro):
            created_coroutines.append(coro)
            return object()

        with patch.object(main_core.asyncio, "create_task", side_effect=_fake_create_task):
            await main_core.handle_storage_post(first, state)
            await main_core.handle_storage_post(second, state)

        self.assertEqual(len(created_coroutines), 1)
        for coro in created_coroutines:
            coro.close()

        with patch.object(main_core, "_publish_post_messages", AsyncMock()) as publish:
            await main_core._flush_media_group("album-42", state)
            await main_core._flush_media_group("album-42", state)

        publish.assert_awaited_once_with([first, second], state, media_group_id="album-42")

    async def test_send_post_preview_uses_storage_ids(self):
        message = SimpleNamespace(chat=SimpleNamespace(id=1), answer=AsyncMock())

        with (
            patch.object(
                main_core,
                "get_post",
                AsyncMock(return_value={"storage_chat_id": -100500, "source_chat_id": None, "storage_message_ids": [12, 13]}),
            ),
            patch.object(main_core.bot, "copy_message", AsyncMock()) as copy_message,
        ):
            await main_core.send_post_preview(message)

        self.assertEqual(copy_message.await_count, 2)
        self.assertEqual(copy_message.await_args_list[0].kwargs, {"chat_id": 1, "from_chat_id": -100500, "message_id": 12})
        self.assertEqual(copy_message.await_args_list[1].kwargs, {"chat_id": 1, "from_chat_id": -100500, "message_id": 13})


if __name__ == "__main__":
    unittest.main()