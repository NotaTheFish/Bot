import logging
import os
from aiohttp import web
from aiogram import Bot
from payment_bot.handlers.payments import process_webhook_payment

logger = logging.getLogger(__name__)

# Directory where domain-verification files (lava-verify_*.html etc.) are stored
VERIFY_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "verify")


def create_webhook_app(bot: Bot) -> web.Application:
    app = web.Application()

    async def lava_webhook(request: web.Request) -> web.Response:
        try:
            payload = await request.json()
            signature = request.headers.get("Signature", "")
            ok = await process_webhook_payment(bot, "lava", payload, signature)
            return web.Response(text="OK" if ok else "ERROR", status=200 if ok else 400)
        except Exception as e:
            logger.error(f"Lava webhook error: {e}")
            return web.Response(text="ERROR", status=500)

    async def payok_webhook(request: web.Request) -> web.Response:
        try:
            payload = dict(await request.post())
            ok = await process_webhook_payment(bot, "payok", payload)
            return web.Response(text="YES" if ok else "ERROR")
        except Exception as e:
            logger.error(f"Payok webhook error: {e}")
            return web.Response(text="ERROR", status=500)

    async def freekassa_webhook(request: web.Request) -> web.Response:
        try:
            payload = dict(await request.post())
            ok = await process_webhook_payment(bot, "freekassa", payload)
            return web.Response(text="YES" if ok else "ERROR")
        except Exception as e:
            logger.error(f"Freekassa webhook error: {e}")
            return web.Response(text="ERROR", status=500)

    async def health(request: web.Request) -> web.Response:
        return web.Response(text="ok")

    async def serve_verify_file(request: web.Request) -> web.Response:
        """
        Serves domain-verification HTML files placed in payment_bot/verify/.
        Used by Lava / Payok / Freekassa to confirm domain ownership.
        Example: GET /lava-verify_b7da374ad60c5915.html
        """
        filename = request.match_info.get("filename", "")
        # Basic safety: only allow simple filenames, no path traversal
        if "/" in filename or "\\" in filename or ".." in filename:
            return web.Response(text="Not found", status=404)
        if not filename.endswith(".html") and not filename.endswith(".txt"):
            return web.Response(text="Not found", status=404)

        filepath = os.path.join(VERIFY_DIR, filename)
        if not os.path.isfile(filepath):
            logger.warning(f"Verify file not found: {filename}")
            return web.Response(text="Not found", status=404)

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        logger.info(f"Served verify file: {filename}")
        return web.Response(text=content, content_type="text/html")

    app.router.add_post("/webhook/lava", lava_webhook)
    app.router.add_post("/webhook/payok", payok_webhook)
    app.router.add_post("/webhook/freekassa", freekassa_webhook)
    app.router.add_get("/health", health)

    # Domain verification files (must be at root, e.g. /lava-verify_xxx.html)
    app.router.add_get("/{filename}", serve_verify_file)

    return app
