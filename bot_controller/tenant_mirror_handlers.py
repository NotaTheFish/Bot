"""
Handlers for per-tenant mirror bots (client BOT_TOKEN), polled inside the main controller process.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import asyncpg
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup

try:
    from bot_controller.userbot_tasks_queue import create_userbot_task
except ImportError:  # noqa: WPS433
    from userbot_tasks_queue import create_userbot_task

logger = logging.getLogger(__name__)


class _TenantMirrorStates(StatesGroup):
    waiting_post = State()


def _tenant_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🧷 Создать пост"), KeyboardButton(text="ℹ️ Статус зеркала")]],
        resize_keyboard=True,
    )


def _mirror_status_ru(status: str | None) -> str:
    s = (status or "pending").strip().lower()
    return {
        "pending": "ожидает запуска",
        "created": "создано, запускается",
        "starting": "запускается",
        "running": "работает",
        "error": "ошибка",
    }.get(s, s)


def build_tenant_mirror_dispatcher(pool: asyncpg.Pool, profile: dict) -> tuple[Bot, Dispatcher]:
    owner_id = int(profile["owner_user_id"])
    bot_token = str(profile["worker_bot_token"] or "").strip()
    workspace_key = str(profile["workspace_key"])

    bot = Bot(bot_token)
    dp = Dispatcher(storage=MemoryStorage())

    async def _load_profile(user_id: int | None) -> dict | None:
        if user_id is None or int(user_id) != owner_id:
            return None
        row = await pool.fetchrow(
            """
            SELECT owner_user_id, workspace_key, worker_bot_token, storage_chat_id, tz_name, enabled,
                   mirror_status, mirror_last_error
            FROM tenant_profiles
            WHERE owner_user_id = $1 AND enabled IS TRUE
            """,
            owner_id,
        )
        if not row:
            return None
        if (row["worker_bot_token"] or "").strip() != bot_token:
            logger.warning("BOT_TOKEN mismatch for tenant owner_id=%s", owner_id)
            return None
        return dict(row)

    @dp.message(CommandStart())
    async def on_start(message: Message):
        if message.chat.type != "private":
            return
        prof = await _load_profile(message.from_user.id if message.from_user else None)
        if not prof:
            await message.answer("Нет доступа.")
            return
        st = _mirror_status_ru(str(prof.get("mirror_status") or ""))
        await message.answer(
            "Контроллер зеркала ✅\n"
            "Ваше зеркало работает на нашей инфраструктуре.\n\n"
            f"Статус: {st}",
            reply_markup=_tenant_menu_kb(),
        )

    @dp.message(Command("menu"))
    async def on_menu(message: Message, state: FSMContext):
        if message.chat.type != "private":
            return
        prof = await _load_profile(message.from_user.id if message.from_user else None)
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
        prof = await _load_profile(message.from_user.id if message.from_user else None)
        if not prof:
            await message.answer("Нет доступа.")
            return
        await state.set_state(_TenantMirrorStates.waiting_post)
        await message.answer("Отправьте пост одним сообщением (текст или медиа).", reply_markup=_tenant_menu_kb())

    @dp.message(F.text == "ℹ️ Статус зеркала")
    async def tenant_status_card(message: Message):
        prof = await _load_profile(message.from_user.id if message.from_user else None)
        if not prof:
            await message.answer("Нет доступа.")
            return
        st = _mirror_status_ru(str(prof.get("mirror_status") or ""))
        err = (prof.get("mirror_last_error") or "").strip()
        lines = ["Статус зеркала", f"• {st}"]
        if err and str(prof.get("mirror_status") or "").lower() == "error":
            lines.append(f"• Детали: {err[:400]}")
        lines.append("")
        lines.append("Рабочая область (техн.): " + workspace_key)
        await message.answer("\n".join(lines), reply_markup=_tenant_menu_kb())

    @dp.message(_TenantMirrorStates.waiting_post, F.chat.type == "private")
    async def save_post(message: Message, state: FSMContext):
        prof = await _load_profile(message.from_user.id if message.from_user else None)
        if not prof:
            await state.clear()
            return
        if not (
            message.text or message.caption or message.photo or message.video or message.document or message.animation
        ):
            await message.answer("Нужно текст или медиа.")
            return
        copied = await message.bot.copy_message(
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
        await message.answer(f"Пост скопирован в Storage и поставлен в очередь ✅ task_id={task_id}")

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
        "Tenant mirror dispatcher owner_id=%s workspace=%s",
        owner_id,
        workspace_key,
    )
    return bot, dp
