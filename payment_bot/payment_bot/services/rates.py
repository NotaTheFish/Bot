import asyncio
import aiohttp
import logging
from payment_bot.config import settings
from payment_bot.db.queries import update_rates

logger = logging.getLogger(__name__)

SUPPORTED = ["RUB", "UAH", "EUR", "KZT", "BYN", "MDL"]


async def fetch_rates() -> dict:
    """Fetch USD-based rates from exchangerate-api or fallback."""
    url = f"https://api.exchangerate-api.com/v4/latest/USD"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                rates = data.get("rates", {})
                result = {"USD": 1.0}
                for cur in SUPPORTED:
                    if cur in rates:
                        result[cur] = float(rates[cur])
                logger.info(f"Rates updated: {result}")
                return result
    except Exception as e:
        logger.warning(f"Rate fetch failed: {e}")
        return {}


async def update_currency_rates():
    rates = await fetch_rates()
    if rates:
        await update_rates(rates)


async def rate_updater_loop():
    while True:
        try:
            await update_currency_rates()
        except Exception as e:
            logger.error(f"Rate updater error: {e}")
        await asyncio.sleep(settings.RATE_UPDATE_INTERVAL)
