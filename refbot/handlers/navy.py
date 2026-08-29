"""
Морской бой: команда, лобби на 2 игроков, вход в расстановку.
Команда: !шайн мб <ставка> <валюта>.
  reply на сообщение игрока -> прямой вызов его.
  без reply -> открытая онлайн-игра (появится в браузере «Найти игру»).
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import matches, settings, ui
from services.amount_parse import parse_amount

router = Router()

GAME = "navy"
_CUR_WORDS = {
    "грибы": "mushrooms", "гриб": "mushrooms", "грибов": "mushrooms",
    "коины": "coins", "коинов": "coins", "коин": "coins",
}


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _name(uid: int) -> str:
    u = await db.get_user(uid)
    if u and u["username"]:
        return f"@{u['username']}"
    return (u["first_name"] if u else None) or str(uid)


async def _guard_casino(uid: int) -> bool:
    from services import casino as casino_svc
    return await casino_svc.visible(uid)


async def handle_shine_navy(msg: Message, parts: list[str]):
    """!шайн мб <ставка> <валюта> [reply -> прямой вызов]."""
    if not await _guard_casino(msg.from_user.id):
        return
    bet = parse_amount(parts[1]) if len(parts) > 1 else None
    cur = None
    for w in parts[2:]:
        if w.lower() in _CUR_WORDS:
            cur = _CUR_WORDS[w.lower()]
            break
    if bet is None or bet <= 0 or cur is None:
        return await ui.reply(msg, "Формат: <code>!шайн мб ставка валюта</code>. "
                                   "Пример: <code>!шайн мб 5000 грибы</code>")
    uid = msg.from_user.id
    if await matches.has_active(uid):
        return await ui.reply(msg, "У тебя уже есть активная игра. Заверши или отмени её.")
    b = await db.balances(uid)
    if b.get(cur, 0) < bet:
        sx = await settings.ctx()
        return await ui.reply(msg, f"Не хватает: на балансе {_fmt(b[cur])} {sx['e_' + cur]}.")

    # reply -> прямой вызов конкретного игрока
    opponent = None
    if msg.reply_to_message and msg.reply_to_message.from_user:
        opp = msg.reply_to_message.from_user
        if opp.is_bot:
            return await ui.reply(msg, "Нельзя вызвать бота.")
        if opp.id == uid:
            return await ui.reply(msg, "С самим собой нельзя 🙂")
        opponent = opp.id

    mode = "direct" if opponent else "online"
    mid, err = await matches.create_navy(uid, cur, bet, mode, opponent, msg.chat.id)
    if err:
        return await ui.reply(msg, f"⚠️ {err}")

    sx = await settings.ctx()
    e = sx["e_" + cur]
    challenger = await _name(uid)
    if opponent:
        # прямой вызов — приглашение в чат
        oppname = await _name(opponent)
        k = InlineKeyboardBuilder()
        from services.ui import btn
        await btn(k, "⚓ Принять бой", f"navy_accept:{mid}")
        await btn(k, "❌ Отклонить", f"navy_decline:{mid}")
        k.adjust(2)
        await ui.send(msg.bot, msg.chat.id,
            f"⚓ <b>Морской бой!</b>\n\n{challenger} вызывает {oppname}.\n"
            f"Ставка: <b>{_fmt(bet)}</b> {e}\n\n{oppname}, принимаешь бой?",
            reply_markup=k.as_markup())
    else:
        # онлайн — открытый вызов, появится в браузере
        k = InlineKeyboardBuilder()
        from services.ui import btn
        await btn(k, "❌ Отменить поиск", f"navy_cancel:{mid}")
        sent = await ui.send(msg.bot, msg.chat.id,
            f"⚓ <b>Морской бой — ищу соперника…</b>\n\n"
            f"{challenger}, ставка <b>{_fmt(bet)}</b> {e}.\n"
            f"Средства заморожены. Поиск до 10 минут.",
            reply_markup=k.as_markup())
        from handlers.lobby_browser import refresh_all
        with contextlib.suppress(Exception):
            await refresh_all(msg.bot, GAME)


@router.callback_query(F.data.startswith("navy_accept:"))
async def cb_accept(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m, err = await matches.accept_invite(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    # морской бой: после принятия — фаза расстановки (не сразу бой)
    from datetime import datetime, timedelta, timezone
    dl = datetime.now(timezone.utc) + timedelta(minutes=matches.NAVY_PLACE_MINUTES)
    await db.pool().execute(
        "UPDATE rb_matches SET status='placement', move_deadline=$1 WHERE id=$2", dl, mid)
    await c.answer("Бой начинается! Расставляй корабли.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("⚓ Бой принят! Оба игрока расставляют корабли (15 мин).",
                                  reply_markup=None)
    from handlers.navy_place import start_placement
    with contextlib.suppress(Exception):
        await start_placement(c.bot, mid)


@router.callback_query(F.data.startswith("navy_decline:"))
async def cb_decline(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m = await matches.get(mid)
    if not m or m["status"] not in ("invited", "searching"):
        return await c.answer("Уже неактуально.", show_alert=True)
    await matches.decline_invite(mid, c.from_user.id)
    await c.answer("Вызов отклонён.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("❌ Вызов на морской бой отклонён. Ставка возвращена.",
                                  reply_markup=None)


@router.callback_query(F.data.startswith("navy_cancel:"))
async def cb_cancel(c: CallbackQuery):
    mid = int(c.data.split(":")[1])
    m = await matches.get(mid)
    if not m or m["p1"] != c.from_user.id:
        return await c.answer("Только создатель может отменить.", show_alert=True)
    await matches.cancel(mid, c.from_user.id)
    await c.answer("Поиск отменён, ставка возвращена.")
    with contextlib.suppress(Exception):
        await c.message.edit_text("❌ Поиск морского боя отменён. Ставка возвращена.",
                                  reply_markup=None)
    from handlers.lobby_browser import refresh_all
    with contextlib.suppress(Exception):
        await refresh_all(c.bot, GAME)


@router.callback_query(F.data.startswith("navy_join:"))
async def cb_navy_join(c: CallbackQuery):
    """Вступить в открытый морской бой из браузера «Найти игру»."""
    mid = int(c.data.split(":")[1])
    m, err = await matches.navy_join(mid, c.from_user.id)
    if err:
        return await c.answer(f"⚠️ {err}", show_alert=True)
    await c.answer("Бой начинается! Расставляй корабли.")
    from handlers.navy_place import start_placement
    with contextlib.suppress(Exception):
        await start_placement(c.bot, mid)
    from handlers.lobby_browser import refresh_all
    with contextlib.suppress(Exception):
        await refresh_all(c.bot, GAME)
