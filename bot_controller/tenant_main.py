"""
Изолированный контроллер для клиента (копия без конкурса, кодов доступа и т.п.).
Должен работать в отдельном процессе со своим BOT_TOKEN.

Запуск из корня репозитория:
  python -m bot_controller.tenant_main

Переменные окружения:
  DATABASE_URL
  BOT_TOKEN           — токен бота клиента (как при онбординге)
  TENANT_OWNER_USER_ID — telegram user id владельца (тот, кто прошёл /code)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup

from bot_controller.userbot_tasks_queue import create_userbot_task

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("tenant_controller")


class _TenantStates(StatesGroup):
    waiting_post = State()


def _env_str(name: str) -> str:
    return (os.getenv(name, "") or "").strip()


def _tenant_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🧷 Создать пост"), KeyboardButton(text="ℹ️ Профиль")]],
        resize_keyboard=True,
    )


async def _load_profile(pool: asyncpg.Pool, owner_id: int, bot_token: str) -> dict | None:
    row = await pool.fetchrow(
        """
        SELECT owner_user_id, workspace_key, worker_bot_token, telegram_api_hash, telegram_api_id,
               telethon_session, storage_chat_id, tz_name, enabled, singleton_lock_key
        FROM tenant_profiles
        WHERE owner_user_id = $1 AND enabled IS TRUE
        """,
        int(owner_id),
    )
    if not row:
        return None
    if (row["worker_bot_token"] or "").strip() != bot_token.strip():
        logger.warning("BOT_TOKEN mismatch for tenant owner_id=%s", owner_id)
        return None
    return dict(row)


async def amain() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except Exception:
        pass

    db_url = _env_str("DATABASE_URL")
    bot_token = _env_str("BOT_TOKEN")
    owner_raw = _env_str("TENANT_OWNER_USER_ID")

    if not db_url:
        logger.error("DATABASE_URL is required")
        sys.exit(1)
    if not bot_token or ":" not in bot_token:
        logger.error("BOT_TOKEN is required")
        sys.exit(1)
    try:
        owner_id = int(owner_raw)
    except ValueError:
        logger.error("TENANT_OWNER_USER_ID must be a positive integer")
        sys.exit(1)
    if owner_id <= 0:
        logger.error("TENANT_OWNER_USER_ID invalid")
        sys.exit(1)

    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
    profile = await _load_profile(pool, owner_id, bot_token)
    if not profile:
        logger.error(
            "Профиль tenant не найден или BOT_TOKEN не совпадает с записью для TENANT_OWNER_USER_ID=%s.",
            owner_id,
        )
        await pool.close()
        sys.exit(1)

    storage_chat_id = int(profile["storage_chat_id"])
    workspace_key = str(profile["workspace_key"])

    bot = Bot(bot_token)
    dp = Dispatcher(storage=MemoryStorage())

    async def profile_or_none(user_id: int | None) -> dict | None:
        if user_id is None or int(user_id) != owner_id:
            return None
        return await _load_profile(pool, owner_id, bot_token)

    @dp.message(CommandStart())
    async def on_start(message: Message):
        if message.chat.type != "private":
            return
        prof = await profile_or_none(message.from_user.id if message.from_user else None)
        if not prof:
            await message.answer("Нет доступа.")
            return
        await message.answer(
            "Контроллер клиента ✅\n"
            "Функции основного проекта здесь недоступны.\n\n"
            f"workspace: {workspace_key}\nStorage: `{storage_chat_id}`",
            parse_mode="Markdown",
            reply_markup=_tenant_menu_kb(),
        )

    @dp.message(Command("menu"))
    async def on_menu(message: Message, state: FSMContext):
        if message.chat.type != "private":
            return
        prof = await profile_or_none(message.from_user.id if message.from_user else None)
        if not prof:
            await message.answer("Нет доступа.")
            return
        await state.clear()
        await on_start(message)

    @dp.message(Command("tenant_post"))
    @dp.message(F.text == "🧷 Создать пост")
    async def start_post(message: Message, state: FSMContext):
        if message.chat.type != "private":
            return
        prof = await profile_or_none(message.from_user.id if message.from_user else None)
        if not prof:
            await message.answer("Нет доступа.")
            return
        await state.set_state(_TenantStates.waiting_post)
        await message.answer("Отправьте пост одним сообщением (текст или медиа).", reply_markup=_tenant_menu_kb())

    @dp.message(F.text == "ℹ️ Профиль")
    async def tenant_profile_card(message: Message):
        prof = await profile_or_none(message.from_user.id if message.from_user else None)
        if not prof:
            return
        lk = prof.get("singleton_lock_key") or "(см. БД tenant_profiles.singleton_lock_key)"
        await message.answer(
            "\n".join([
                "Профиль копии",
                f"owner: {prof['owner_user_id']}",
                f"workspace_key: {prof['workspace_key']}",
                f"STORAGE_CHAT_ID: {prof['storage_chat_id']}",
                f"TZ: {prof['tz_name']}",
                f"SINGLETON_LOCK_KEY (для worker): {lk}",
            ]),
            reply_markup=_tenant_menu_kb(),
        )

    @dp.message(_TenantStates.waiting_post, F.chat.type == "private")
    async def save_post(message: Message, state: FSMContext):
        prof = await profile_or_none(message.from_user.id if message.from_user else None)
        if not prof:
            await state.clear()
            return
        if not (
            message.text or message.caption or message.photo or message.video or message.document or message.animation
        ):
            await message.answer("Нужно текст или медиа.")
            return
        copied = await bot.copy_message(
            chat_id=int(prof["storage_chat_id"]),
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )
        run_at = datetime.now(timezone.utc)
        task_id = await create_userbot_task(
            pool,
            int(prof["storage_chat_id"]),
            [int(copied.message_id)],
            None,
            run_at,
            str(prof["workspace_key"]),
        )
        await state.clear()
        await message.answer(f"Пост скопирован в Storage и поставлен в очередь воркера ✅ task_id={task_id}")

    @dp.message(F.chat.type == "private")
    async def deny_other(message: Message, state: FSMContext):
        if message.from_user and int(message.from_user.id) == owner_id:
            await message.answer(
                "Команда не распознана. Используйте меню ниже или /menu",
                reply_markup=_tenant_menu_kb(),
            )
            return
        await message.answer("Нет доступа.")

    logger.info(
        "Tenant controller owner_id=%s workspace=%s storage=%s",
        owner_id,
        workspace_key,
        storage_chat_id,
    )
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(amain())
