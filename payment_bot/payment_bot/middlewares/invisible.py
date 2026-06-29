from typing import Callable, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject, Message, CallbackQuery
from payment_bot.config import settings
from payment_bot.db.queries_users import get_user_by_telegram_id
from payment_bot.db.queries import get_active_deal_for_client


class InvisibleBotMiddleware(BaseMiddleware):
    """
    Core middleware: bot is invisible to everyone except:
    - Main admin (MAIN_ADMIN_ID)
    - Sub-admins (role = sub_admin)
    - Clients with an active WAITING_PAYMENT deal

    Everyone else gets silently ignored.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict], Awaitable[Any]],
        event: TelegramObject,
        data: dict,
    ) -> Any:
        # Extract telegram_id from event
        telegram_id = self._get_telegram_id(event)
        if telegram_id is None:
            return  # ignore unknown event types

        # Always allow main admin
        if telegram_id == settings.MAIN_ADMIN_ID:
            return await handler(event, data)

        # Check DB for user role
        user = await get_user_by_telegram_id(telegram_id)

        if user is None:
            # Unknown user — check if this is a /start with a token
            # We allow it through so the start handler can process the token
            if self._is_start_command(event):
                return await handler(event, data)
            return  # silent ignore

        if user["role"] in ("main_admin", "sub_admin"):
            data["db_user"] = user
            return await handler(event, data)

        if user["role"] == "client":
            # Check for active deal
            active_deal = await get_active_deal_for_client(user["id"])
            if active_deal:
                data["db_user"] = user
                data["active_deal"] = active_deal
                return await handler(event, data)
            # No active deal — check if they're starting with a token
            if self._is_start_command(event):
                data["db_user"] = user
                return await handler(event, data)
            return  # silent ignore

        return  # any other case — silent ignore

    @staticmethod
    def _get_telegram_id(event: TelegramObject) -> int | None:
        if isinstance(event, Message):
            return event.from_user.id if event.from_user else None
        if isinstance(event, CallbackQuery):
            return event.from_user.id if event.from_user else None
        return None

    @staticmethod
    def _is_start_command(event: TelegramObject) -> bool:
        if isinstance(event, Message):
            text = event.text or ""
            return text.startswith("/start")
        return False
