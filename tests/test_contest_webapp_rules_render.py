import json
import time
import unittest
from unittest.mock import AsyncMock, patch

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "contest_webapp"))
import contest_api


class ContestWebappRulesRenderTests(unittest.IsolatedAsyncioTestCase):
    async def test_render_without_entities_escapes_text(self):
        rendered = await contest_api.render_telegram_text_with_custom_emoji("<b>x</b>", None)
        self.assertEqual(rendered, "&lt;b&gt;x&lt;/b&gt;")

    async def test_render_single_custom_emoji(self):
        text = "Hello 😀 world"
        entities = [{"type": "custom_emoji", "offset": 6, "length": 2, "custom_emoji_id": "ce1"}]

        with patch.object(contest_api, "_custom_emoji_image_url", AsyncMock(return_value="https://cdn/emoji1.webp")):
            rendered = await contest_api.render_telegram_text_with_custom_emoji(text, entities)

        self.assertEqual(
            rendered,
            'Hello <img class="tg-custom-emoji" src="https://cdn/emoji1.webp" alt="emoji" /> world',
        )

    async def test_render_multiple_custom_emoji(self):
        text = "😀😀"
        entities = [
            {"type": "custom_emoji", "offset": 0, "length": 2, "custom_emoji_id": "ce1"},
            {"type": "custom_emoji", "offset": 2, "length": 2, "custom_emoji_id": "ce2"},
        ]

        with patch.object(
            contest_api,
            "_custom_emoji_image_url",
            AsyncMock(side_effect=["https://cdn/emoji1.webp", "https://cdn/emoji2.webp"]),
        ):
            rendered = await contest_api.render_telegram_text_with_custom_emoji(text, entities)

        self.assertEqual(
            rendered,
            '<img class="tg-custom-emoji" src="https://cdn/emoji1.webp" alt="emoji" />'
            '<img class="tg-custom-emoji" src="https://cdn/emoji2.webp" alt="emoji" />',
        )

    async def test_render_mixed_text_and_emoji_entities_json(self):
        text = "A😀<script>"
        entities_json = json.dumps([
            {"type": "custom_emoji", "offset": 1, "length": 2, "custom_emoji_id": "ce1"}
        ])

        with patch.object(contest_api, "_custom_emoji_image_url", AsyncMock(return_value="https://cdn/emoji1.webp?x=1&y=2")):
            rendered = await contest_api.render_telegram_text_with_custom_emoji(text, entities_json)

        self.assertEqual(
            rendered,
            'A<img class="tg-custom-emoji" src="https://cdn/emoji1.webp?x=1&amp;y=2" alt="emoji" />&lt;script&gt;',
        )

    async def test_render_invalid_offsets_length_graceful_fallback(self):
        text = "abc😀def"
        entities = [
            {"type": "custom_emoji", "offset": -1, "length": 2, "custom_emoji_id": "bad1"},
            {"type": "custom_emoji", "offset": 999, "length": 1, "custom_emoji_id": "bad2"},
            {"type": "custom_emoji", "offset": 3, "length": 0, "custom_emoji_id": "bad3"},
            {"type": "custom_emoji", "offset": "oops", "length": 2, "custom_emoji_id": "bad4"},
        ]

        with patch.object(contest_api, "_custom_emoji_image_url", AsyncMock(return_value="https://unused")):
            rendered = await contest_api.render_telegram_text_with_custom_emoji(text, entities)

        self.assertEqual(rendered, "abc😀def")


if __name__ == "__main__":
    unittest.main()

class ContestWebappRulesEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_contest_rules_returns_rules_html(self):
        pool = AsyncMock()
        pool.fetchrow.return_value = {"rules_text": "Hi 😀", "rules_entities_json": [{"type": "custom_emoji", "offset": 3, "length": 2, "custom_emoji_id": "ce1"}]}

        with (
            patch.object(contest_api, "_extract_auth", return_value={"user_id": 1}),
            patch.object(contest_api, "get_pool", return_value=pool),
            patch.object(contest_api, "render_telegram_text_with_custom_emoji", AsyncMock(return_value="Hi <img />")),
        ):
            response = await contest_api.contest_rules("init")

        payload = json.loads(response.body)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["rules_text"], "Hi 😀")
        self.assertIn("rules_entities_json", payload)
        self.assertEqual(payload["rules_html"], "Hi <img />")

class ContestWebappCustomEmojiResolverTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        contest_api.custom_emoji_cache.clear()

    async def asyncTearDown(self):
        contest_api.custom_emoji_cache.clear()

    async def test_resolve_custom_emoji_url_cache_hit(self):
        contest_api.custom_emoji_cache["ce1"] = (time.time() + 60, "https://cdn/cached.webp")

        with patch.object(contest_api, "_telegram_api_call", AsyncMock()) as tg_call:
            resolved = await contest_api._resolve_custom_emoji_url("ce1")

        self.assertEqual(resolved, "https://cdn/cached.webp")
        tg_call.assert_not_awaited()

    async def test_resolve_custom_emoji_url_cache_miss_fetches_and_caches(self):
        with patch.object(
            contest_api,
            "_telegram_api_call",
            AsyncMock(
                side_effect=[
                    {"ok": True, "result": [{"custom_emoji_id": "ce1", "file_id": "f1"}]},
                    {"ok": True, "result": {"file_path": "stickers/emoji1.webp"}},
                ]
            ),
        ) as tg_call, patch.object(contest_api, "BOT_TOKEN", "123:abc"):
            resolved = await contest_api._resolve_custom_emoji_url("ce1")

        self.assertEqual(resolved, "https://api.telegram.org/file/bot123:abc/stickers/emoji1.webp")
        self.assertEqual(tg_call.await_count, 2)
        self.assertEqual(contest_api._cache_get_custom_emoji_url("ce1"), resolved)

    async def test_resolve_custom_emoji_url_telegram_api_error(self):
        with patch.object(
            contest_api,
            "_telegram_api_call",
            AsyncMock(side_effect=RuntimeError("telegram unavailable")),
        ):
            with self.assertRaises(RuntimeError):
                await contest_api._resolve_custom_emoji_url("ce1")

        self.assertIsNone(contest_api._cache_get_custom_emoji_url("ce1"))