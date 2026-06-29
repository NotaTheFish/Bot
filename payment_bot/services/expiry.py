import asyncio
import logging
from aiogram import Bot
from payment_bot.config import settings
from payment_bot.db.queries import expire_overdue_deals, log_event
from payment_bot.db.queries_users import get_user_by_telegram_id

logger = logging.getLogger(__name__)


async def expire_deals_loop(bot: Bot):
    while True:
        await asyncio.sleep(settings.EXPIRED_CHECK_INTERVAL)
        try:
            expired = await expire_overdue_deals()
            for deal in expired:
                await log_event(
                    "deal_expired",
                    {"deal_id": deal["id"], "was_status": deal["status"]},
                    deal_id=deal["id"],
                    admin_id=deal["admin_id"],
                )
                # Notify client if they were waiting
                if deal["client_id"]:
                    try:
                        # We need telegram_id from user id
                        from payment_bot.db.pool import get_pool
                        pool = await get_pool()
                        async with pool.acquire() as conn:
                            user = await conn.fetchrow(
                                "SELECT telegram_id FROM pt_users WHERE id = $1",
                                deal["client_id"]
                            )
                        if user:
                            await bot.send_message(
                                user["telegram_id"],
                                "⏰ Время сделки истекло. Обратитесь к продавцу за новой ссылкой."
                            )
                    except Exception:
                        pass
            if expired:
                logger.info(f"Expired {len(expired)} deals")
        except Exception as e:
            logger.error(f"Expiry loop error: {e}")
