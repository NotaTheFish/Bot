import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("BOT_TOKEN", "123:abc")
os.environ.setdefault("ADMIN_ID", "1")
os.environ.setdefault("DATABASE_URL", "postgres://localhost/db")
os.environ.setdefault("STORAGE_CHAT_ID", "-100123")
os.environ.setdefault("SELLER_USERNAME", "seller_login")

from aiogram.types import MessageEntity

from bot_controller import main_core
from userbot_worker import sender as worker_sender


class ContactCtaBuilderTests(unittest.TestCase):
    def test_basic_text_link_cta(self):
        text, entities, used_fallback = main_core.build_contact_cta(
            "Исходный текст",
            [],
            "abc123token",
            bot_username="my_shop_bot",
            seller_username="seller",
            mode="text_link",
        )

        self.assertFalse(used_fallback)
        self.assertIn(main_core.CONTACT_CTA_TEXT, text)
        self.assertNotIn("https://", text)
        self.assertNotIn("t.me/", text)
        self.assertEqual(len(entities), 1)
        self.assertEqual(entities[0].type, "text_link")
        self.assertEqual(entities[0].url, "https://t.me/my_shop_bot?start=contact_abc123token")

    def test_fallback_without_url_when_no_bot_username(self):
        text, entities, used_fallback = main_core.build_contact_cta(
            "Исходный текст",
            [],
            "abc123token",
            bot_username="",
            seller_username="seller_login",
            mode="fallback",
        )

        self.assertTrue(used_fallback)
        self.assertIn("@seller_login", text)
        self.assertNotIn("https://", text)
        self.assertNotIn("t.me/", text)
        self.assertEqual([e for e in entities if e.type == "text_link"], [])

    def test_fallback_without_url_when_text_link_is_sanitized(self):
        unsafe_text = "Проверка https://t.me/bot?start=contact_abc\n\n✉️ Написать продавцу"
        unsafe_entities = [
            MessageEntity(
                type="text_link",
                offset=10,
                length=18,
                url="https://t.me/bot?start=contact_abc",
            )
        ]
        fallback_text = "Проверка\n\n✉️ Написать продавцу: @seller_login"

        safe_text, safe_entities = main_core._ensure_safe_publish_text(
            unsafe_text,
            unsafe_entities,
            fallback_text=fallback_text,
            fallback_entities=unsafe_entities,
            context="test",
        )

        self.assertEqual(safe_text, fallback_text)
        self.assertNotIn("https://", safe_text)
        self.assertNotIn("t.me/", safe_text)
        self.assertFalse(any(entity.type == "text_link" for entity in safe_entities))

    def test_media_caption_length_utf16_offsets_and_entities_merge(self):
        base_caption = "До 😀"
        original_entities = [
            MessageEntity(type="bold", offset=0, length=2),
            MessageEntity(type="italic", offset=3, length=2),
            MessageEntity(type="text_link", offset=0, length=2, url="https://example.com"),
        ]

        caption, entities, used_fallback = main_core.build_contact_cta(
            base_caption,
            original_entities,
            "tok123",
            bot_username="my_shop_bot",
            seller_username="seller_login",
            mode="text_link",
        )

        self.assertFalse(used_fallback)
        self.assertLessEqual(len(caption), 1024)
        self.assertEqual(len(entities), 4)

        cta_entity = [entity for entity in entities if entity.type == "text_link" and entity.url and "start=contact_" in entity.url][0]
        prefix = f"{base_caption}\n\n"
        expected_offset = len(prefix.encode("utf-16-le")) // 2
        expected_length = len(main_core.CONTACT_CTA_TEXT.encode("utf-16-le")) // 2
        self.assertEqual(cta_entity.offset, expected_offset)
        self.assertEqual(cta_entity.length, expected_length)

        entity_types = [entity.type for entity in entities]
        self.assertIn("bold", entity_types)
        self.assertIn("italic", entity_types)
        self.assertEqual(len([entity for entity in entities if entity.type == "text_link"]), 2)


class WorkerPathRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_userbot_worker_send_path_keeps_original_message_ids(self):
        client = object()
        with patch.object(worker_sender, "forward_post", AsyncMock(return_value=77)) as forward_post:
            result = await worker_sender.send_post_to_chat(
                client,
                source_chat_id=100,
                source_message_ids=[11, 22],
                target=SimpleNamespace(id=999),
                min_delay=1,
                max_delay=3,
            )

        self.assertEqual(result, 77)
        forward_post.assert_awaited_once()
        kwargs = forward_post.await_args.kwargs
        self.assertEqual(kwargs["source_message_id"], [11, 22])
        self.assertEqual(kwargs["source_chat_id"], 100)


if __name__ == "__main__":
    unittest.main()