"""
Настройка профиля: установка/замена ника, выбор титула и персонального эмодзи.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import ui, settings, profile as prof
from services.ui import btn
from services.amount_parse import shk_fmt

router = Router()


class NickFSM(StatesGroup):
    entering = State()


async def _nick_price() -> int:
    """Цена замены ника в шимкоинах (центах), из настроек."""
    s = await settings.load()
    return int(s.get("nickname_price", 0) or 0)


@router.callback_query(F.data == "prof_setup")
async def cb_setup(c: CallbackQuery):
    p = await prof.get_profile(c.from_user.id)
    kb = InlineKeyboardBuilder()
    if p and p.get("nickname_set"):
        price = await _nick_price()
        label = f"✏️ Сменить ник ({shk_fmt(price)} 💠)" if price else "✏️ Сменить ник"
        await btn(kb, label, "prof_nick")
    else:
        await btn(kb, "✏️ Установить ник (бесплатно)", "prof_nick")
    await btn(kb, "🏅 Выбрать титул", "prof_titles")
    await btn(kb, "😎 Персональный эмодзи", "prof_emoji")
    await btn(kb, "Назад", "profile", "back")
    kb.adjust(1)
    cur_nick = f"\nТекущий ник: <b>{p['nickname']}</b>" if p and p.get("nickname") else ""
    await ui.edit(c.message,
        f"⚙️ <b>Настройка профиля</b>{cur_nick}\n\n"
        f"• Ник — твоё имя в боте (можно поставить один раз бесплатно).\n"
        f"• Титул и эмодзи — за достижения.",
        reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data == "prof_nick")
async def cb_nick(c: CallbackQuery, state: FSMContext):
    p = await prof.get_profile(c.from_user.id)
    # платная замена?
    if p and p.get("nickname_set"):
        price = await _nick_price()
        if price > 0:
            b = await db.balances(c.from_user.id)
            if b.get("shimcoins", 0) < price:
                return await c.answer(
                    f"Смена ника стоит {shk_fmt(price)} 💠, у тебя {shk_fmt(b.get('shimcoins',0))} 💠.",
                    show_alert=True)
    await state.set_state(NickFSM.entering)
    await ui.edit(c.message,
        "✏️ <b>Введи новый ник</b>\n\n"
        "До 32 символов. Буквы, цифры, пробел, дефис, подчёркивание.\n"
        "Эмодзи, ссылки и спецсимволы запрещены.",
        reply_markup=None)
    await c.answer()


@router.message(NickFSM.entering)
async def msg_nick(msg: Message, state: FSMContext):
    nick, err = prof.validate_nick(msg.text or "")
    if err:
        return await ui.reply(msg, f"⚠️ {err}\nПопробуй другой ник:")
    if await prof.nick_taken(nick, exclude_uid=msg.from_user.id):
        return await ui.reply(msg, "Этот ник уже занят. Придумай другой:")
    await state.set_state(None)
    p = await prof.get_profile(msg.from_user.id)
    # платная замена — списать ШК
    if p and p.get("nickname_set"):
        price = await _nick_price()
        if price > 0:
            async with db.pool().acquire() as conn:
                async with conn.transaction():
                    spent = await db.apply(conn, msg.from_user.id, "shimcoins", -price,
                                           "nick_change", f"nick:{msg.from_user.id}:{nick}")
                    if spent is None:
                        return await ui.reply(msg, "Не хватило шимкоинов на смену ника.")
    ok, err2 = await prof.set_nick(msg.from_user.id, nick, mark_set=True)
    if not ok:
        return await ui.reply(msg, f"⚠️ {err2}")
    await ui.reply(msg, f"✅ Ник установлен: <b>{nick}</b>")


# заглушки титул/эмодзи — следующие под-блоки
@router.callback_query(F.data == "prof_titles")
async def cb_titles_stub(c: CallbackQuery):
    await c.answer("Выбор титула — скоро (после блока достижений).", show_alert=True)


@router.callback_query(F.data == "prof_emoji")
async def cb_emoji_stub(c: CallbackQuery):
    await c.answer("Персональный эмодзи — скоро (после блока достижений).", show_alert=True)


@router.message(F.text.lower().startswith("!ценаника"))
async def cmd_nick_price(msg: Message):
    from config import SUPER_ADMINS
    is_admin = msg.from_user.id in SUPER_ADMINS or bool(await db.admin_chats(msg.from_user.id))
    if not is_admin:
        return
    from services.amount_parse import shk_parse
    parts = (msg.text or "").split()
    if len(parts) < 2:
        cur = await _nick_price()
        return await ui.reply(msg, f"Текущая цена смены ника: <b>{shk_fmt(cur)}</b> 💠\n"
                                   f"Изменить: <code>!ценаника 5</code>")
    price = shk_parse(parts[1])
    if price is None or price < 0:
        return await ui.reply(msg, "Нужно число (шимкоины). Пример: <code>!ценаника 5</code>")
    await settings.set("nickname_price", str(price), msg.from_user.id)
    await ui.reply(msg, f"✅ Цена смены ника: <b>{shk_fmt(price)}</b> 💠")
