"""Middleware: блокировка забаненных пользователей на уровне всех апдейтов."""
import logging
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery, InlineQuery, ReplyKeyboardRemove

logger = logging.getLogger(__name__)


class BanMiddleware(BaseMiddleware):
    """Перехватывает любые действия забаненных пользователей.
    Отвечает один раз «вы забанены», дальше молча игнорирует."""

    def __init__(self, db, config):
        self.db = db
        self.config = config
        super().__init__()

    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user is None:
            return await handler(event, data)

        # Админа никогда не баним
        if self.config.ADMIN_TG_ID and user.id == self.config.ADMIN_TG_ID:
            return await handler(event, data)

        try:
            banned = await self.db.is_banned(user.id)
        except Exception as e:
            logger.error(f"Ошибка проверки бана: {e}")
            return await handler(event, data)

        if not banned:
            return await handler(event, data)

        # Пользователь забанен — НЕ передаём апдейт дальше.
        # Отвечаем один раз, потом молчим.
        try:
            already = await self.db.was_ban_notified(user.id)
        except Exception:
            already = True

        admin_un = self.config.ADMIN_USERNAME
        contact = f"@{admin_un}" if admin_un else "администратору"
        ban_text = f"🚫 Вы забанены. Для разбана напишите {contact}."

        if not already:
            try:
                if isinstance(event, Message):
                    await event.answer(ban_text, reply_markup=ReplyKeyboardRemove())
                elif isinstance(event, CallbackQuery):
                    await event.answer(ban_text, show_alert=True)
                    # Также убираем reply-клавиатуру отдельным сообщением
                    try:
                        await event.message.answer(ban_text, reply_markup=ReplyKeyboardRemove())
                    except Exception:
                        pass
                elif isinstance(event, InlineQuery):
                    await event.answer(results=[], cache_time=1)
                await self.db.mark_ban_notified(user.id)
            except Exception as e:
                logger.info(f"Не удалось уведомить забаненного {user.id}: {e}")
        else:
            # Уже уведомляли — гасим «часики» на колбэках, но ничего не делаем
            if isinstance(event, CallbackQuery):
                try:
                    await event.answer()
                except Exception:
                    pass
            elif isinstance(event, InlineQuery):
                try:
                    await event.answer(results=[], cache_time=1)
                except Exception:
                    pass

        # Прерываем цепочку — апдемент не доходит до хендлеров
        return
