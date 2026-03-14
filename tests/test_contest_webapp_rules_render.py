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
        rendered = await contest_api.render_telegram_text_with_entities_to_html("<b>x</b>", None)
        self.assertEqual(rendered, "&lt;b&gt;x&lt;/b&gt;")

    async def test_render_static_custom_emoji_as_image(self):
        text = "Hello 😀 world"
        entities = [{"type": "custom_emoji", "offset": 6, "length": 2, "custom_emoji_id": "ce1"}]
        meta = contest_api.CustomEmojiRenderInfo(
            custom_emoji_id="ce1",
            file_id="file1",
            file_unique_id="u1",
            is_animated=False,
            is_video=False,
            emoji="😀",
            set_name="set",
            source_format="webp",
            file_path="stickers/a.webp",
            thumbnail_file_id=None,
            render_mode="image",
            mime_type="image/webp",
            width=100,
            height=100,
        )

        with patch.object(contest_api, "_resolve_custom_emoji_info", AsyncMock(return_value=meta)):
            rendered = await contest_api.render_telegram_text_with_entities_to_html(text, entities)

        self.assertIn('data-render-mode="image"', rendered)
        self.assertIn('/api/contest/custom-emoji/ce1/asset', rendered)

    async def test_render_lottie_custom_emoji_placeholder(self):
        text = "A😀B"
        entities = [{"type": "custom_emoji", "offset": 1, "length": 2, "custom_emoji_id": "ce1"}]
        meta = contest_api.CustomEmojiRenderInfo(
            custom_emoji_id="ce1",
            file_id="file1",
            file_unique_id="u1",
            is_animated=True,
            is_video=False,
            emoji="😀",
            set_name=None,
            source_format="tgs",
            file_path="stickers/a.tgs",
            thumbnail_file_id=None,
            render_mode="lottie",
            mime_type="application/x-tgsticker",
            width=100,
            height=100,
        )

        with patch.object(contest_api, "_resolve_custom_emoji_info", AsyncMock(return_value=meta)):
            rendered = await contest_api.render_telegram_text_with_entities_to_html(text, entities)

        self.assertIn("tg-inline-emoji--lottie", rendered)
        self.assertIn('data-asset-url="/api/contest/custom-emoji/ce1/asset"', rendered)

    async def test_render_video_custom_emoji(self):
        text = "A😀B"
        entities = [{"type": "custom_emoji", "offset": 1, "length": 2, "custom_emoji_id": "ce1"}]
        meta = contest_api.CustomEmojiRenderInfo(
            custom_emoji_id="ce1",
            file_id="file1",
            file_unique_id="u1",
            is_animated=False,
            is_video=True,
            emoji="😀",
            set_name=None,
            source_format="webm",
            file_path="stickers/a.webm",
            thumbnail_file_id=None,
            render_mode="video",
            mime_type="video/webm",
            width=100,
            height=100,
        )

        with patch.object(contest_api, "_resolve_custom_emoji_info", AsyncMock(return_value=meta)):
            rendered = await contest_api.render_telegram_text_with_entities_to_html(text, entities)

        self.assertIn("tg-custom-emoji-video", rendered)
        self.assertIn("autoplay loop muted playsinline", rendered)

    async def test_render_uses_safe_fallback_when_resolve_fails(self):
        text = "abc😀def"
        entities = [{"type": "custom_emoji", "offset": 3, "length": 2, "custom_emoji_id": "bad"}]

        with patch.object(contest_api, "_resolve_custom_emoji_info", AsyncMock(return_value=None)):
            rendered = await contest_api.render_telegram_text_with_entities_to_html(text, entities)

        self.assertIn("tg-inline-emoji--fallback", rendered)
        self.assertIn("😀", rendered)


class ContestWebappRulesEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_contest_rules_returns_rules_html(self):
        pool = AsyncMock()
        pool.fetchrow.return_value = {
            "rules_text": "Hi 😀",
            "rules_entities_json": [{"type": "custom_emoji", "offset": 3, "length": 2, "custom_emoji_id": "ce1"}],
        }

        with (
            patch.object(contest_api, "_extract_auth", return_value={"user_id": 1}),
            patch.object(contest_api, "get_pool", return_value=pool),
            patch.object(contest_api, "render_telegram_text_with_entities_to_html", AsyncMock(return_value="Hi <img />")),
        ):
            response = await contest_api.contest_rules("init")

        payload = json.loads(response.body)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["rules_html"], "Hi <img />")


class ContestWebappCustomEmojiResolverTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        contest_api.custom_emoji_cache.clear()
        contest_api._custom_emoji_meta_cache.clear()

    async def asyncTearDown(self):
        contest_api.custom_emoji_cache.clear()
        contest_api._custom_emoji_meta_cache.clear()

    async def test_resolve_custom_emoji_meta_cache_hit(self):
        meta = contest_api.CustomEmojiRenderInfo(
            custom_emoji_id="ce1",
            file_id="f1",
            file_unique_id="u1",
            is_animated=False,
            is_video=False,
            emoji="😀",
            set_name=None,
            source_format="webp",
            file_path="stickers/emoji1.webp",
            thumbnail_file_id=None,
            render_mode="image",
            mime_type="image/webp",
            width=100,
            height=100,
        )
        contest_api._custom_emoji_meta_cache["ce1"] = (time.time() + 60, meta)

        with patch.object(contest_api, "_telegram_api_call", AsyncMock()) as tg_call:
            resolved = await contest_api._resolve_custom_emoji_info("ce1")

        self.assertIsNotNone(resolved)
        self.assertEqual(resolved.custom_emoji_id, "ce1")
        tg_call.assert_not_awaited()

    async def test_resolve_custom_emoji_meta_cache_miss_fetches_and_caches(self):
        with patch.object(
            contest_api,
            "_telegram_api_call",
            AsyncMock(
                side_effect=[
                    {
                        "ok": True,
                        "result": [
                            {
                                "custom_emoji_id": "ce1",
                                "file_id": "f1",
                                "file_unique_id": "u1",
                                "is_animated": False,
                                "is_video": False,
                                "emoji": "😀",
                                "width": 100,
                                "height": 100,
                            }
                        ],
                    },
                    {"ok": True, "result": {"file_path": "stickers/emoji1.webp"}},
                ]
            ),
        ) as tg_call:
            resolved = await contest_api._resolve_custom_emoji_info("ce1")

        self.assertIsNotNone(resolved)
        self.assertEqual(resolved.render_mode, "image")
        self.assertEqual(tg_call.await_count, 2)


if __name__ == "__main__":
    unittest.main()