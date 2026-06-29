import logging
from aiohttp import web
from aiogram import Bot
from payment_bot.handlers.payments import process_webhook_payment

logger = logging.getLogger(__name__)


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

    app.router.add_post("/webhook/lava", lava_webhook)
    app.router.add_post("/webhook/payok", payok_webhook)
    app.router.add_post("/webhook/freekassa", freekassa_webhook)
    app.router.add_get("/health", health)

    return app
