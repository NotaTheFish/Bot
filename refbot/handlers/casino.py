"""
Казино (в личке бота). Пока — кейсы. Рулетка платная и карточки — позже.

Доступ: rb_settings['casino.enabled'] = '0'|'1'. 0 — видит только админ (кнопки нет
у клиентов), 1 — все. Переключается в админке на лету.

Открытие кейса: выбрал кейс -> выбрал валюту -> оплата и приз в одной транзакции.
RTP ~85%, игрок всегда что-то получает. Каждое открытие логируется.
"""
import asyncio
import contextlib
import logging

import asyncpg
from aiogram import F, Router
from aiogram.types import CallbackQuery

import db
import keyboards as kb
from config import SUPER_ADMINS
from services import casino, settings, ui

router = Router()
log = logging.getLogger("casino")


async def _is_admin(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


async def casino_enabled() -> bool:
    return await casino.enabled()


async def casino_visible(uid: int) -> bool:
    """Видит ли пользователь казино: включено для всех ИЛИ он админ."""
    return await casino.visible(uid)


# ---------------- меню казино ----------------
@router.callback_query(F.data == "casino")
async def cb_casino(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино пока закрыто.", show_alert=True)
    note = "" if await casino_enabled() else "\n\n<i>🔴 Сейчас видно только админам.</i>"
    await ui.edit(c.message,
        "🎰 <b>Казино</b>\n\nВыбери игру:" + note,
        reply_markup=await kb.casino_menu())
    await c.answer()


@router.callback_query(F.data == "casino_cases")
async def cb_cases(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино пока закрыто.", show_alert=True)
    b = await db.balances(c.from_user.id)
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"📦 <b>Кейсы</b>\n\n"
        f"Твой баланс: {sx['e_mushrooms']} {b['mushrooms']:,} · "
        f"{sx['e_coins']} {b['coins']:,}\n\n"
        f"Открываешь кейс — всегда что-то выигрываешь. С небольшим шансом — джекпот ×10!"
        .replace(",", " "),
        reply_markup=await kb.casino_cases(casino.all_cases()))
    await c.answer()


@router.callback_query(F.data.startswith("case:"))
async def cb_case_pick(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    case_key = c.data.split(":")[1]
    title = casino.case_title(case_key)
    price_m = casino.case_price(case_key, "mushrooms")
    price_c = casino.case_price(case_key, "coins")
    sx = await settings.ctx()
    await ui.edit(c.message,
        f"📦 <b>{title}</b>\n\n"
        f"Цена: {price_m:,} {sx['e_mushrooms']} или {price_c:,} {sx['e_coins']}\n\n"
        f"Чем платишь?".replace(",", " "),
        reply_markup=await kb.case_currency(case_key))
    await c.answer()


@router.callback_query(F.data.startswith("casebuy:"))
async def cb_case_open(c: CallbackQuery):
    if not await casino_visible(c.from_user.id):
        return await c.answer("Казино закрыто.", show_alert=True)
    _, case_key, cur = c.data.split(":")
    uid = c.from_user.id
    if await db.is_banned(uid):
        return await c.answer("Ты заблокирован в системе.", show_alert=True)

    cost = casino.case_price(case_key, cur)
    b = await db.balances(uid)
    if b[cur] < cost:
        sx = await settings.ctx()
        return await c.answer(
            f"Не хватает: нужно {cost:,} {sx['e_' + cur]}, у тебя {b[cur]:,}."
            .replace(",", " "), show_alert=True)

    # ролл приза
    mult, _p = casino.roll_prize(case_key)
    won = casino.prize_amount(case_key, cur, mult)

    # оплата + приз в одной транзакции. idem уникален на каждое открытие (иначе
    # «Открыть ещё» в том же сообщении не сработает — message_id не меняется).
    import time
    stamp = int(time.time() * 1000)
    idem = f"case:{uid}:{case_key}:{stamp}:{cur}"
    try:
        async with db.pool().acquire() as conn:
            async with conn.transaction():
                await db.apply(conn, uid, cur, -cost, "case_buy", idem + ":pay")
                new_bal = await db.apply(conn, uid, cur, won, "case_win", idem + ":win")
    except asyncpg.CheckViolationError:
        return await c.answer("Не хватает баланса.", show_alert=True)
    except asyncpg.UniqueViolationError:
        return await c.answer("Повтори ещё раз.", show_alert=True)

    await db.log_case_open(uid, case_key, cur, cost, won, mult)
    await db.audit(uid, "case_open",
                   {"case": case_key, "cur": cur, "cost": cost, "won": won, "mult": mult})

    await c.answer("Открываю…")
    await _animate_case(c, case_key, cur, mult, won, new_bal)


async def _animate_case(c, case_key, cur, mult, won, new_bal):
    """Простая анимация открытия + результат."""
    e_cur = await settings.emoji_html(cur)
    title = casino.case_title(case_key)

    # короткая анимация «прокрутки»
    frames = ["📦", "📦✨", "📦💥", "🎁"]
    for f in frames:
        with contextlib.suppress(Exception):
            await ui.edit(c.message, f"<b>{title}</b>\n\n{f} открываем…")
        await asyncio.sleep(0.5)

    # подпись по редкости
    if mult == 10.0:
        head = "🎰💥 <b>ДЖЕКПОТ!!!</b> 💥🎰"
    elif mult == 5.0:
        head = "🔥💰 <b>ОГРОМНЫЙ ВЫИГРЫШ!</b> 💰🔥"
    elif mult >= 3.0:
        head = "💎 <b>КРУПНЫЙ ВЫИГРЫШ!</b>"
    elif mult >= 1.0:
        head = "🔥 <b>Неплохо!</b>"
    else:
        head = "🎁 <b>Открыто</b>"

    profit = won - casino.case_price(case_key, cur)
    profit_line = (f"В плюс: +{profit:,}" if profit > 0
                   else f"В этот раз в минус: {profit:,}" if profit < 0
                   else "Вернул своё")
    await ui.edit(c.message,
        f"{head}\n\n"
        f"Из «{title}» выпало: <b>{won:,}</b> {e_cur} (×{mult})\n"
        f"{profit_line}\n\n"
        f"Баланс: <b>{new_bal:,}</b> {e_cur}".replace(",", " "),
        reply_markup=await kb.case_again(case_key))


# ---------------- админ: переключатель доступа ----------------
@router.callback_query(F.data == "casino_admin")
async def cb_casino_admin(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await ui.edit(c.message,
        "🎰 <b>Казино — доступ</b>\n\n"
        "🔴 Только админ — клиенты не видят кнопку «Казино».\n"
        "🟢 Открыто всем — кнопка появляется у всех.",
        reply_markup=await kb.casino_admin_toggle(await casino_enabled()))
    await c.answer()


@router.callback_query(F.data == "casino_toggle")
async def cb_casino_toggle(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    new = "0" if await casino_enabled() else "1"
    await settings.set("casino.enabled", new, c.from_user.id)
    await db.audit(c.from_user.id, "casino_toggle", {"enabled": new})
    await ui.edit(c.message,
        "🎰 <b>Казино — доступ</b>\n\n"
        + ("🟢 Теперь открыто всем." if new == "1" else "🔴 Теперь только админ."),
        reply_markup=await kb.casino_admin_toggle(new == "1"))
    await c.answer("Готово")
