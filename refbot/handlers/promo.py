"""
Промокоды.

Админ: «Промокоды» в админке -> «Все промо» (список, удаление) и «Создать промо»
(FSM: код -> грибы -> активации -> срок). Награда в грибах, коины по курсу.

Игрок: пишет промокод текстом в ЛИЧКЕ (без команды). Ловим ТОЛЬКО когда нет
активного FSM-состояния (StateFilter(None)) и только в приватном чате — чтобы не
конфликтовать с вводом суммы вывода, названий розыгрышей и т.п.
"""
import contextlib
import logging

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import StateFilter
from aiogram.types import CallbackQuery, Message

import db
import keyboards as kb
from config import COIN_RATE, SUPER_ADMINS
from services import promo, settings, ui

router = Router()
log = logging.getLogger("promo")


async def _is_admin(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


class PromoNew(StatesGroup):
    code = State()
    reward = State()
    kind = State()
    acts = State()
    expiry = State()


# ==================== АДМИН ====================
@router.callback_query(F.data == "promo_menu")
async def cb_promo_menu(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await ui.edit(c.message, "🎟 <b>Промокоды</b>\n\nВыбери действие:",
                  reply_markup=await kb.promo_menu())
    await c.answer()


@router.callback_query(F.data == "promo_all")
async def cb_promo_all(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    rows = await db.promo_list()
    if not rows:
        return await ui.edit(c.message, "🎟 <b>Промокоды</b>\n\nПока нет ни одного.",
                             reply_markup=await kb.promo_back())
    lines = ["🎟 <b>Все промокоды</b>\n"]
    _kind_ic = {"rate": "🍄🪙", "mushrooms": "🍄", "coins": "🪙"}
    for p in rows:
        flag = "🔴" if not promo.is_active(p) else "🟢"
        kind = p["reward_kind"] if "reward_kind" in p else "rate"
        ic = _kind_ic.get(kind, "🍄🪙")
        lines.append(f"{flag} <code>{p['code']}</code> — {p['reward_mush']:,} {ic} "
                     f"({promo.status_line(p)})".replace(",", " "))
    await ui.edit(c.message, "\n".join(lines),
                  reply_markup=await kb.promo_list_kb(rows))
    await c.answer()


@router.callback_query(F.data.startswith("promo_view:"))
async def cb_promo_view(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    pid = int(c.data.split(":")[1])
    p = await db.promo_get(pid)
    if not p:
        return await c.answer("Промокод не найден.", show_alert=True)
    flag = "🔴 истёк/исчерпан" if not promo.is_active(p) else "🟢 активен"
    kind = p["reward_kind"] if "reward_kind" in p else "rate"
    kind_name = {"rate": "🍄🪙 по курсу (валюта игрока)",
                 "mushrooms": "🍄 только грибы",
                 "coins": "🪙 только коины"}.get(kind, "по курсу")
    await ui.edit(c.message,
        f"🎟 <b>{p['code']}</b>\n\n"
        f"Награда: {p['reward_mush']:,}\n"
        f"Выдача: {kind_name}\n"
        f"Статус: {flag}\n"
        f"{promo.status_line(p)}".replace(",", " "),
        reply_markup=await kb.promo_card(pid))
    await c.answer()


@router.callback_query(F.data.startswith("promo_del:"))
async def cb_promo_del(c: CallbackQuery):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    pid = int(c.data.split(":")[1])
    await db.promo_delete(pid)
    await db.audit(c.from_user.id, "promo_delete", {"id": pid})
    await c.answer("Удалён")
    # обновляем список
    await cb_promo_all(c)


# ---------- создание ----------
@router.callback_query(F.data == "promo_new")
async def cb_promo_new(c: CallbackQuery, state: FSMContext):
    if not await _is_admin(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await state.set_state(PromoNew.code)
    await ui.edit(c.message,
        "🎟 <b>Создание промокода</b>\n\nШаг 1/5 — введи кодовое слово (например "
        "<code>PROMO</code>).", reply_markup=await kb.promo_back())
    await c.answer()


@router.message(PromoNew.code)
async def promo_code_input(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    code = (msg.text or "").strip()
    if not code or len(code) > 40 or " " in code:
        return await ui.answer(msg, "Код без пробелов, до 40 символов. Ещё раз.")
    await state.update_data(code=code)
    await state.set_state(PromoNew.reward)
    await ui.answer(msg, f"Код: <b>{code}</b>\n\nШаг 2/5 — сколько выдавать? "
                    f"(коины начислятся по курсу). Например <code>50000</code> или "
                    f"<code>50к</code>.")


@router.message(PromoNew.reward)
async def promo_reward_input(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    from services.amount_parse import parse_amount
    amount = parse_amount(msg.text or "")
    if amount is None or amount <= 0:
        return await ui.answer(msg, "Нужно число. Например <code>50000</code> или <code>50к</code>.")
    await state.update_data(reward=amount)
    await state.set_state(PromoNew.kind)
    await ui.answer(msg,
        f"Награда: <b>{amount:,}</b>\n\n".replace(",", " ") +
        "Шаг 3/5 — в какой валюте выдавать?\n\n"
        "🍄🪙 <b>По курсу</b> — игрок получит в своей валюте (грибы→грибы, коины по курсу)\n"
        "🍄 <b>Только грибы</b> — все получают грибы\n"
        "🪙 <b>Только коины</b> — все получают коины",
        reply_markup=await kb.promo_kind())


@router.callback_query(PromoNew.kind, F.data.startswith("pkind:"))
async def promo_kind_choice(c: CallbackQuery, state: FSMContext):
    if not await _is_admin(c.from_user.id):
        return await state.clear()
    kind = c.data.split(":")[1]  # rate|mushrooms|coins
    await state.update_data(kind=kind)
    await state.set_state(PromoNew.acts)
    name = {"rate": "по курсу", "mushrooms": "только грибы", "coins": "только коины"}[kind]
    await ui.edit(c.message,
        f"Валюта: <b>{name}</b>\n\nШаг 4/5 — сколько активаций? "
        f"Число (например <code>100</code>) или <code>безлим</code>.")
    await c.answer()


@router.message(PromoNew.acts)
async def promo_acts_input(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    acts = promo.parse_acts(msg.text or "")
    if acts == "err":
        return await ui.answer(msg, "Число или <code>безлим</code>. Ещё раз.")
    await state.update_data(acts=acts)
    await state.set_state(PromoNew.expiry)
    lim = "безлимит" if acts is None else str(acts)
    await ui.answer(msg, f"Активаций: <b>{lim}</b>\n\nШаг 5/5 — срок годности? "
                    f"Например <code>1д</code>, <code>24ч</code>, <code>30м</code> "
                    f"или <code>безлим</code>.")


@router.message(PromoNew.expiry)
async def promo_expiry_input(msg: Message, state: FSMContext):
    if not await _is_admin(msg.from_user.id):
        return await state.clear()
    exp = promo.parse_expiry(msg.text or "")
    if exp == "err":
        return await ui.answer(msg, "Не понял срок. Например <code>1д</code>, "
                               "<code>24ч</code> или <code>безлим</code>.")
    data = await state.get_data()
    kind = data.get("kind", "rate")
    pid = await db.promo_create(data["code"], data["reward"], data["acts"],
                                exp, msg.from_user.id, reward_kind=kind)
    await db.audit(msg.from_user.id, "promo_create",
                   {"id": pid, "code": data["code"], "reward": data["reward"], "kind": kind})
    await state.clear()
    lim = "безлимит" if data["acts"] is None else str(data["acts"])
    when = "бессрочно" if exp is None else exp.strftime("%d.%m %H:%M МСК")
    kind_name = {"rate": "🍄🪙 по курсу (валюта игрока)",
                 "mushrooms": "🍄 только грибы",
                 "coins": "🪙 только коины"}[kind]
    await ui.answer(msg,
        f"✅ Промокод создан!\n\n"
        f"Код: <b>{data['code']}</b>\n"
        f"Награда: {data['reward']:,}\n"
        f"Выдача: {kind_name}\n"
        f"Активаций: {lim}\n"
        f"Срок: {when}".replace(",", " "),
        reply_markup=await kb.promo_back())


# ==================== ИГРОК: активация текстом в личке ====================
# StateFilter(None) — только когда НЕТ активного FSM (не мешаем вводу вывода и пр.)
@router.message(StateFilter(None), F.chat.type == "private", F.text,
                ~F.text.startswith("/"))
async def promo_try_activate(msg: Message):
    if await db.is_banned(msg.from_user.id):
        return
    code = (msg.text or "").strip()
    # игнорим служебный текст reply-кнопок и слишком длинное/пустое
    if not code or len(code) > 40 or " " in code or code in ("☰ Меню", "✖️ Скрыть"):
        return

    status, reward_mush, pid, kind = await db.promo_activate(msg.from_user.id, code, COIN_RATE)
    if status == "notfound":
        return  # молчим — это обычный текст, не промокод
    if status == "expired":
        return await ui.answer(msg, "⏳ Этот промокод больше не действует — истёк срок.")
    if status == "used_up":
        return await ui.answer(msg, "🚫 Промокод исчерпан — все активации закончились.")
    if status == "already":
        return await ui.answer(msg, "❗ Ты уже активировал этот промокод.")

    # ok — начисляем по режиму промокода:
    #   rate      — в валюте игрока (грибы→грибы, коины ×COIN_RATE)
    #   mushrooms — всегда грибы; coins — всегда коины
    if kind == "mushrooms":
        cur = "mushrooms"
        amount = reward_mush
    elif kind == "coins":
        cur = "coins"
        amount = reward_mush
    else:  # rate
        u = await db.get_user(msg.from_user.id)
        cur = u["currency"]
        amount = reward_mush * COIN_RATE if cur == "coins" else reward_mush
    sx = await settings.ctx()
    async with db.pool().acquire() as conn:
        async with conn.transaction():
            new_bal = await db.apply(conn, msg.from_user.id, cur, amount,
                                     "promo", f"promo:{pid}:{msg.from_user.id}")
    await db.audit(msg.from_user.id, "promo_use", {"id": pid, "amount": amount, "cur": cur})
    await ui.answer(msg,
        f"🎉 Промокод активирован!\n\n"
        f"Начислено: <b>{amount:,}</b> {sx['e_' + cur]} {sx['l_' + cur]}\n"
        f"Баланс: <b>{new_bal:,}</b> {sx['e_' + cur]}".replace(",", " "))
