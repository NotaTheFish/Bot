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


# ── Анти-флуд: троттлинг с эскалацией до авто-бана ──────────────────────────

import time as _time
from collections import deque


class ThrottlingMiddleware(BaseMiddleware):
    """Ограничивает частоту действий на пользователя. Эскалация:
    1-е превышение — предупреждение, дальше молчаливый игнор 60с,
    3 волны за час — авто-бан на 24 часа."""

    # Лимиты: (окно_сек, макс_событий)
    LIMITS = {
        "message": (60, 20),
        "callback_query": (60, 40),
        "inline_query": (60, 30),
    }
    STRIKES_TO_BAN = 3       # волн превышений за час до авто-бана
    STRIKE_WINDOW = 3600     # окно накопления страйков
    MUTE_SECONDS = 60        # тишина после превышения
    AUTOBAN_HOURS = 24

    def __init__(self, db, config):
        self.db = db
        self.config = config
        # uid -> {kind -> deque[timestamps]}
        self._hits: dict = {}
        # uid -> [timestamps страйков]
        self._strikes: dict = {}
        # uid -> timestamp до которого молчим (warned)
        self._muted: dict = {}
        super().__init__()

    def _event_kind(self, event) -> str:
        if isinstance(event, Message):
            return "message"
        if isinstance(event, CallbackQuery):
            return "callback_query"
        if isinstance(event, InlineQuery):
            return "inline_query"
        return "message"

    def _cleanup(self):
        # Защита от разрастания памяти
        if len(self._hits) > 5000:
            self._hits.clear()
        if len(self._strikes) > 5000:
            self._strikes.clear()
        if len(self._muted) > 5000:
            self._muted.clear()

    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user is None:
            return await handler(event, data)
        # Админ вне ограничений
        if self.config.ADMIN_TG_ID and user.id == self.config.ADMIN_TG_ID:
            return await handler(event, data)

        uid = user.id
        now = _time.monotonic()
        kind = self._event_kind(event)
        window, limit = self.LIMITS.get(kind, (60, 20))

        # Если в муте — молча гасим (кроме инлайна, где нужно ответить пустым)
        muted_until = self._muted.get(uid, 0)
        if now < muted_until:
            if isinstance(event, InlineQuery):
                try:
                    await event.answer(results=[], cache_time=1)
                except Exception:
                    pass
            elif isinstance(event, CallbackQuery):
                try:
                    await event.answer()
                except Exception:
                    pass
            return  # не пропускаем дальше

        # Считаем события в окне
        bucket = self._hits.setdefault(uid, {}).setdefault(kind, deque())
        while bucket and now - bucket[0] > window:
            bucket.popleft()
        bucket.append(now)

        if len(bucket) <= limit:
            return await handler(event, data)

        # ── Превышение ──
        self._muted[uid] = now + self.MUTE_SECONDS
        bucket.clear()

        # Копим страйки (по монотонному времени)
        strikes = [t for t in self._strikes.get(uid, []) if now - t < self.STRIKE_WINDOW]
        strikes.append(now)
        self._strikes[uid] = strikes
        self._cleanup()

        # Авто-бан при накоплении страйков
        if len(strikes) >= self.STRIKES_TO_BAN:
            self._strikes.pop(uid, None)
            try:
                await self.db.ban_user(uid, user.username,
                                       hours=self.AUTOBAN_HOURS, reason="autoflood")
            except Exception as e:
                logger.error(f"Авто-бан не удался {uid}: {e}")
            admin_un = self.config.ADMIN_USERNAME
            contact = f"@{admin_un}" if admin_un else "администратору"
            try:
                if isinstance(event, Message):
                    await event.answer(
                        f"🚫 Вы временно заблокированы на 24 часа за флуд.\n"
                        f"По вопросам — {contact}.",
                        reply_markup=ReplyKeyboardRemove())
                elif isinstance(event, CallbackQuery):
                    await event.answer("🚫 Заблокировано за флуд на 24ч", show_alert=True)
            except Exception:
                pass
            return

        # Первое/второе превышение — предупреждение
        try:
            if isinstance(event, Message):
                await event.answer("⚠️ Слишком быстро. Подожди минуту, иначе временный бан.")
            elif isinstance(event, CallbackQuery):
                await event.answer("⚠️ Слишком быстро, подожди минуту", show_alert=True)
            elif isinstance(event, InlineQuery):
                await event.answer(results=[], cache_time=1)
        except Exception:
            pass
        return
