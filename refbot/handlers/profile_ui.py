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
    # вернуть в профиль
    kb = InlineKeyboardBuilder()
    await btn(kb, "👤 В профиль", "profile", "back")
    await ui.reply(msg, f"✅ Ник установлен: <b>{nick}</b>", reply_markup=kb.as_markup())


# заглушки титул/эмодзи — следующие под-блоки
@router.callback_query(F.data == "prof_titles")
async def cb_titles(c: CallbackQuery):
    from services import titles
    tlist = await titles.user_titles(c.from_user.id)
    kb = InlineKeyboardBuilder()
    if not tlist:
        await btn(kb, "Назад", "prof_setup", "back")
        kb.adjust(1)
        await ui.edit(c.message,
            "🏅 <b>Титулы</b>\n\nУ тебя пока нет титулов. Их дают за достижения "
            "или вручает админ.", reply_markup=kb.as_markup())
        return await c.answer()
    lines = ["🏅 <b>Твои титулы</b>\n\nВыбери, какой показывать:"]
    for t in tlist:
        mark = "⭐" if t["is_admin_grant"] else "•"
        active = " ✅" if t["is_active"] else ""
        lines.append(f"{mark} {t['name']}{active}")
        await btn(kb, f"{t['name'][:25]}", f"prof_title_set:{t['id']}")
    await btn(kb, "🚫 Не показывать титул", "prof_title_set:0")
    await btn(kb, "Назад", "prof_setup", "back")
    kb.adjust(1)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("prof_title_set:"))
async def cb_title_set(c: CallbackQuery):
    from services import titles
    tid = int(c.data.split(":")[1])
    ok = await titles.set_active_title(c.from_user.id, tid if tid else None)
    if not ok:
        return await c.answer("Не удалось (титул не твой).", show_alert=True)
    await c.answer("Титул обновлён!" if tid else "Титул скрыт.")
    c.data = "prof_titles"
    await cb_titles(c)


@router.callback_query(F.data == "prof_emoji")
async def cb_emoji(c: CallbackQuery):
    emojis = await prof.user_emojis(c.from_user.id)
    kb = InlineKeyboardBuilder()
    if not emojis:
        await btn(kb, "Назад", "prof_setup", "back")
        kb.adjust(1)
        await ui.edit(c.message,
            "😎 <b>Персональный эмодзи</b>\n\nУ тебя пока нет эмодзи. Их дают за достижения "
            "или можно купить в магазине.", reply_markup=kb.as_markup())
        return await c.answer()
    p = await prof.get_profile(c.from_user.id)
    active = p.get("active_emoji") if p else None
    lines = ["😎 <b>Персональный эмодзи</b>\n\nПоказывается рядом с твоим ником. Выбери:"]
    # callback по ИНДЕКСУ (тег/символ в callback_data невалиден для Telegram)
    for i, e in enumerate(emojis):
        mark = " ✅" if e == active else ""
        # в тексте кнопки — символ-подложка (из тега вытащим видимый символ)
        label = _emoji_label(e)
        await btn(kb, f"{label}{mark}", f"prof_emoji_set:{i}")
    await btn(kb, "🚫 Не показывать", "prof_emoji_set:none")
    await btn(kb, "Назад", "prof_setup", "back")
    n = len(emojis)
    rows = [4] * (n // 4)
    if n % 4:
        rows.append(n % 4)
    rows += [1, 1]
    kb.adjust(*rows)
    await ui.edit(c.message, "\n".join(lines), reply_markup=kb.as_markup())
    await c.answer()


def _emoji_label(e: str) -> str:
    """Видимый символ эмодзи для текста кнопки (из premium-тега вытащить подложку)."""
    import re
    m = re.search(r">([^<]+)</tg-emoji>", e)
    return m.group(1) if m else e


@router.callback_query(F.data.startswith("prof_emoji_set:"))
async def cb_emoji_set(c: CallbackQuery):
    val = c.data.split(":", 1)[1]
    if val == "none":
        emoji = None
    else:
        # выбор по индексу из актуального списка эмодзи игрока
        emojis = await prof.user_emojis(c.from_user.id)
        try:
            emoji = emojis[int(val)]
        except (ValueError, IndexError):
            return await c.answer("Эмодзи не найден, обнови список.", show_alert=True)
    ok = await prof.set_active_emoji(c.from_user.id, emoji)
    if not ok:
        return await c.answer("Не удалось (эмодзи не твой).", show_alert=True)
    await c.answer("Эмодзи обновлён!" if emoji else "Эмодзи скрыт.")
    c.data = "prof_emoji"
    await cb_emoji(c)


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
