import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("BOT_TOKEN", "123:abc")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("DATABASE_URL", "postgres://localhost/db")
os.environ.setdefault("STORAGE_CHAT_ID", "-100123")

from bot_controller import main_core


class StoragePostFlowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        main_core.media_group_buffers.clear()

    def test_dispatcher_uses_memory_storage(self):
        self.assertEqual(main_core.dp.storage.__class__.__name__, "MemoryStorage")

    async def test_create_post_command_outside_storage_for_admin(self):
        state = AsyncMock()
        message = SimpleNamespace(
            chat=SimpleNamespace(id=999),
            from_user=SimpleNamespace(id=1),
            text="/create_post",
            answer=AsyncMock(),
        )

        await main_core.create_post_in_storage(message, state)

        message.answer.assert_not_called()
        state.set_state.assert_not_called()

    async def test_create_post_command_ignores_non_admin(self):
        state = AsyncMock()
        message = SimpleNamespace(
            chat=SimpleNamespace(id=-100123),
            from_user=SimpleNamespace(id=777),
            text="/create_post",
            answer=AsyncMock(),
        )

        await main_core.create_post_in_storage(message, state)

        message.answer.assert_not_called()
        state.set_state.assert_not_called()

    async def test_create_post_command_for_storage_admin_sets_state_and_answers(self):
        state = AsyncMock()
        state.get_state = AsyncMock(return_value=main_core.AdminStates.waiting_storage_post.state)
        message = SimpleNamespace(
            chat=SimpleNamespace(id=-100123),
            from_user=SimpleNamespace(id=1),
            text="/create_post",
            answer=AsyncMock(),
        )

        await main_core.create_post_in_storage(message, state)

        state.set_state.assert_awaited_once_with(main_core.AdminStates.waiting_storage_post)
        message.answer.assert_awaited_once_with("Ок, отправьте следующим сообщением пост/альбом…")

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
            await main_core.receive_storage_post(message, state)

        set_last.assert_awaited_once_with(-100123, [55])
        save_post.assert_awaited_once()
        self.assertEqual(save_post.await_args.kwargs["storage_message_ids"], [55])

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
            answer=AsyncMock(),
        )

        def _fake_create_task(coro):
            coro.close()
            return object()

        with patch.object(main_core.asyncio, "create_task", side_effect=_fake_create_task):
            await main_core.receive_storage_post(message, state)

        self.assertIn("group-1", main_core.media_group_buffers)
        self.assertEqual(main_core.media_group_buffers["group-1"]["messages"], [message])


    async def test_receive_storage_post_rejects_empty_service_message(self):
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
            answer=AsyncMock(),
        )

        with patch.object(main_core, "_publish_post_messages", AsyncMock()) as publish:
            await main_core.receive_storage_post(message, state)

        publish.assert_not_called()
        message.answer.assert_awaited_once_with("пришлите текст или медиа")

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