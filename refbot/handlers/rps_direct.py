"""
КНБ direct-режим: создатель зовёт игроков по никам. Каждому — приглашение в ЛС
(принять/отклонить). Принял → ставка заморожена, он в лобби. Создатель жмёт
«Начать» (кто принял — играют, минимум 2) или автостарт когда все ответили.
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import matches, settings, ui
from services.ui import btn

router = Router()


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _name(uid: int) -> str:
    u = await db.get_user(uid)
    if u and u["username"]:
        return f"@{u['username']}"
    return (u["first_name"] if u else None) or str(uid)


# in-memory: mid -> {invited: set, answered: set} для отслеживания приглашений
_invites: dict[int, dict] = {}


async def send_invites(bot, mid: int, creator: int, invited: list[int], unknown: list[str]):
    """Разослать приглашения и показать создателю статус-панель."""
    m = await matches.get(mid)
    sx = await settings.ctx()
    e = sx["e_" + m["currency"]]
    _invites[mid] = {"invited": set(invited), "answered": set()}
    cname = await _name(creator)

    # приглашения игрокам
    for uid in invited:
        kb = InlineKeyboardBuilder()
        await btn(kb, "✅ Принять", f"rpsd_acc:{mid}")
        await btn(kb, "❌ Отклонить", f"rpsd_dec:{mid}")
        kb.adjust(2)
        with contextlib.suppress(Exception):
            await ui.send(bot, uid,
                f"🤓😎🥸 <b>Приглашение в игру!</b>\n\n"
                f"{cname} зовёт тебя в камень-ножницы-бумага.\n"
                f"Ставка: <b>{_fmt(m['stake'])}</b> {e}\n\n"
                f"Примешь? Ставка заморозится при согласии.",
                reply_markup=kb.as_markup())

    # панель создателя
    await _creator_panel(bot, mid, creator, unknown)


async def _creator_panel(bot, mid: int, creator: int, unknown: list[str] = None):
    m = await matches.get(mid)
    sx = await settings.ctx()
    e = sx["e_" + m["currency"]]
    inv = _invites.get(mid, {"invited": set(), "answered": set()})
    players = await matches.lobby_players(mid)   # уже принявшие (в лобби)
    accepted = [p["tg_id"] for p in players if p["tg_id"] != creator]

    lines = [f"🤓😎🥸 <b>Твоя игра (по приглашениям)</b>",
             f"Ставка: <b>{_fmt(m['stake'])}</b> {e}", ""]
    lines.append(f"Приглашено: {len(inv['invited'])}")
    lines.append(f"Приняли: {len(accepted)}")
    if unknown:
        lines.append(f"⚠️ Не нашёл: {', '.join('@'+u for u in unknown)}")
    lines.append("")
    lines.append("Жми «Начать», когда готов (минимум 2 игрока с тобой).")

    kb = InlineKeyboardBuilder()
    await btn(kb, "▶️ Начать", f"rpsd_start:{mid}")
    await btn(kb, "❌ Отменить", f"rpsd_cancel:{mid}")
    kb.adjust(2)
    with contextlib.suppress(Exception):
        await ui.send(bot, creator, "\n".join(lines), reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("rpsd_acc:"))
async def cb_accept(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.join_lobby(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    inv = _invites.get(mid)
    if inv:
        inv["answered"].add(c.from_user.id)
    await c.answer("Ты принял приглашение! Ставка заморожена.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("✅ Ты в игре! Жди старта от создателя.", reply_markup=None)
    # уведомим создателя (обновим панель)
    await _creator_panel(c.bot, mid, m["p1"])
    await _maybe_autostart(c.bot, mid)


@router.callback_query(F.data.startswith("rpsd_dec:"))
async def cb_decline(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    inv = _invites.get(mid)
    if inv:
        inv["answered"].add(c.from_user.id)
    await c.answer("Ты отклонил приглашение.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("❌ Ты отклонил приглашение.", reply_markup=None)
    m = await matches.get(mid)
    if m and m["status"] == "lobby":
        await _maybe_autostart(c.bot, mid)


async def _maybe_autostart(bot, mid: int):
    """Автостарт, когда все приглашённые ответили (приняли/отклонили)."""
    inv = _invites.get(mid)
    if not inv:
        return
    if inv["answered"] >= inv["invited"]:   # все ответили
        players = await matches.lobby_players(mid)
        if len(players) >= 2:
            await _do_start(bot, mid)
        else:
            # никто не принял — отменить с возвратом создателю
            m, _ = await matches.cancel_lobby(mid)
            if m:
                with contextlib.suppress(Exception):
                    await ui.send(bot, m["p1"],
                        "Никто не принял приглашение. Игра отменена, ставка возвращена.")


@router.callback_query(F.data.startswith("rpsd_start:"))
async def cb_start(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m = await matches.get(mid)
    if not m or m["status"] != "lobby":
        return await c.answer("Игра уже неактуальна.", show_alert=True)
    if c.from_user.id != m["p1"]:
        return await c.answer("Только создатель может начать.", show_alert=True)
    players = await matches.lobby_players(mid)
    if len(players) < 2:
        return await c.answer("Нужно минимум 2 игрока (с тобой).", show_alert=True)
    await c.answer("Игра начинается!")
    await _do_start(c.bot, mid)


async def _do_start(bot, mid: int):
    _invites.pop(mid, None)
    from handlers.rps_game import start_round_dm
    with contextlib.suppress(Exception):
        await start_round_dm(bot, mid, first=True)


@router.callback_query(F.data.startswith("rpsd_cancel:"))
async def cb_cancel(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.cancel_lobby(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    _invites.pop(mid, None)
    await c.answer("Игра отменена, ставки возвращены.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("❌ Игра отменена. Ставки возвращены.", reply_markup=None)
