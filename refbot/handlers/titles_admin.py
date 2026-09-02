"""
Админка титулов: выдать/забрать титул игроку через карточку пользователя.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import ui, titles, profile as prof
from services.ui import btn
from config import SUPER_ADMINS


async def _can_manage(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


router = Router()


class TitleFSM(StatesGroup):
    name = State()


@router.callback_query(F.data.startswith("a_title:"))
async def cb_title_menu(c: CallbackQuery):
    if not await _can_manage(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    tg_id = int(c.data.split(":")[1])
    tlist = await titles.user_titles(tg_id)
    name = await prof.display_name(tg_id, link=False)
    kb = InlineKeyboardBuilder()
    await btn(kb, "➕ Выдать титул", f"a_title_add:{tg_id}")
    lines = [f"🏅 <b>Титулы игрока</b> ({name})", ""]
    if tlist:
        for t in tlist:
            mark = "⭐" if t["is_admin_grant"] else "•"
            active = " (активный)" if t["is_active"] else ""
            lines.append(f"{mark} {t['name']}{active}")
            await btn(kb, f"❌ Забрать: {t['name'][:20]}", f"a_title_del:{tg_id}:{t['id']}")
    else:
        lines.append("<i>Титулов нет.</i>")
    await btn(kb, "Назад", f"a_findback:{tg_id}", "back")
    kb.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("a_title_add:"))
async def cb_title_add(c: CallbackQuery, state: FSMContext):
    if not await _can_manage(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    tg_id = int(c.data.split(":")[1])
    await state.set_state(TitleFSM.name)
    await state.update_data(title_tg=tg_id)
    await ui.edit(c.message,
        "🏅 <b>Новый титул</b>\n\nНапиши название титула (например «Звезда»):",
        reply_markup=None)
    await c.answer()


@router.message(TitleFSM.name)
async def msg_title_name(msg: Message, state: FSMContext):
    if not await _can_manage(msg.from_user.id):
        return
    name = (msg.text or "").strip()
    if len(name) < 2 or len(name) > 40:
        return await ui.reply(msg, "Название титула — от 2 до 40 символов. Ещё раз:")
    if any(ord(ch) < 32 for ch in name) or "<" in name or ">" in name:
        return await ui.reply(msg, "Без спецсимволов и скобок. Ещё раз:")
    data = await state.get_data()
    tg_id = data.get("title_tg")
    await state.set_state(None)
    # админский титул -> помечается is_admin_grant
    tid = await titles.grant_title_by_name(tg_id, name, msg.from_user.id, admin_grant=True)
    pname = await prof.display_name(tg_id, link=False)
    kb = InlineKeyboardBuilder()
    await btn(kb, "К титулам", f"a_title:{tg_id}", "back")
    await ui.reply(msg, f"✅ Титул «<b>{name}</b>» выдан игроку {pname}.",
                   reply_markup=kb.as_markup())
    # уведомить игрока
    with contextlib.suppress(Exception):
        await ui.send(msg.bot, tg_id,
            f"🏅 Тебе вручили уникальный титул: <b>{name}</b>!\n"
            f"Выбрать его можно в профиле → Настроить профиль → Выбрать титул.")


@router.callback_query(F.data.startswith("a_title_del:"))
async def cb_title_del(c: CallbackQuery):
    if not await _can_manage(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    _, tg_s, tid_s = c.data.split(":")
    tg_id, tid = int(tg_s), int(tid_s)
    await titles.revoke_title(tg_id, tid)
    await c.answer("Титул забран.")
    # перерисовать меню титулов
    c.data = f"a_title:{tg_id}"
    await cb_title_menu(c)
