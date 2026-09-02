"""
Лобби-браузер: список открытых игр с кнопками «Вступить» внутри текста (Rich Messages).
Общий для КН (ttt) и КНБ (rps). Пагинация, «Создать свою», авто-fallback на инлайн.

Открывается из меню казино (в ЛС) кнопкой «Найти игру».
"""
import contextlib

from aiogram import F, Router
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

import db
from services import matches, settings, ui, richmsg

router = Router()

PAGE = 5   # игр на страницу

_GAME_TITLE = {"ttt": "Крестики-нолики", "rps": "Камень-ножницы-бумага", "navy": "Морской бой"}
_CUR_E = {"mushrooms": "🍄", "coins": "🪙"}

# реестр открытых браузеров: game -> {(chat_id, msg_id): (uid, page, is_rich)}
# чтобы обновлять список у всех, кто его смотрит, при изменениях (вступление/отмена)
_open_browsers: dict[str, dict] = {"ttt": {}, "rps": {}}


async def refresh_all(bot, game: str):
    """Обновить список открытых игр во всех открытых браузерах этого типа."""
    reg = _open_browsers.get(game, {})
    stale = []
    for (chat_id, msg_id), (uid, page, is_rich) in list(reg.items()):
        try:
            await show_browser(bot, chat_id, uid, game, page,
                               edit_msg_id=msg_id, is_rich=is_rich, _register=False)
        except Exception:
            stale.append((chat_id, msg_id))
    for k in stale:
        reg.pop(k, None)


def _fmt(n: int) -> str:
    return f"{n:,}".replace(",", " ")


async def _name(uid: int) -> str:
    from services import profile as _prof
    return await _prof.display_name(uid)


def _join_cb(game: str, mid: int) -> str:
    """callback для вступления по типу игры."""
    if game == "ttt":
        return f"ttt_join_chat:{mid}"
    if game == "navy":
        return f"navy_join:{mid}"
    return f"rps_join:{mid}"


async def show_browser(bot, chat_id: int, uid: int, game: str, page: int = 0,
                       edit_msg_id: int | None = None, is_rich: bool = True,
                       _register: bool = True):
    """Показать/обновить браузер открытых игр. game: 'ttt'|'rps'."""
    games, total = await matches.open_games(game, exclude_uid=uid,
                                            limit=PAGE, offset=page * PAGE)
    title = _GAME_TITLE.get(game, game)
    header = f"🔍 <b>Открытые игры: {title}</b>\n"

    # строки с кнопками в тексте
    rows = []
    for g in games:
        cname = await _name(g["p1"])
        e = _CUR_E.get(g["currency"], "")
        if game == "rps":
            info = f"🎮 <b>{_fmt(g['stake'])}</b> {e} · от {cname} · игроков: {g['players_count']}"
        else:
            info = f"🎮 <b>{_fmt(g['stake'])}</b> {e} · от {cname}"
        if g.get("is_mine"):
            info += " (твоя)"
            rows.append((info, []))   # свою игру без кнопки «вступить»
        else:
            rows.append((info, [("⚔️ Вступить", _join_cb(game, g["id"]))]))

    if not games:
        header += "\nПока нет открытых игр. Создай свою!"

    # управляющие кнопки (пагинация + создать) — через btn (премиум в иконку)
    from services.ui import btn
    ctrl = InlineKeyboardBuilder()
    pages = (total + PAGE - 1) // PAGE
    nav = []
    if page > 0:
        await btn(ctrl, "◀️", f"lb_page:{game}:{page-1}")
        nav.append(1)
    if page + 1 < pages:
        await btn(ctrl, "▶️", f"lb_page:{game}:{page+1}")
        nav.append(1)
    # «Создать свою» ведёт на штатный вход создания (обрабатывается с реальным FSM)
    create_cb = "ttt_new" if game == "ttt" else "rps_new_dm"
    await btn(ctrl, "➕ Создать свою", create_cb)
    await btn(ctrl, "Назад", "casino_games", "back")
    if total > PAGE:
        header += f"\n<i>Стр. {page+1} из {pages}</i>"

    # раскладка управляющих кнопок
    layout = []
    if nav:
        layout.append(len(nav))
    layout += [1, 1]
    ctrl.adjust(*layout)
    fallback = ctrl.as_markup()

    # rich-сообщение: кнопки «вступить» в тексте + управляющие снизу как fallback-kb.
    # (Управляющие кнопки идут обычной клавиатурой; «вступить» — в тексте через rich.)
    if edit_msg_id:
        await richmsg.edit_rich(bot, chat_id, edit_msg_id, header, rows,
                                fallback_kb=fallback, is_rich=is_rich,
                                reply_markup=fallback if is_rich else None)
        if _register:
            _open_browsers.setdefault(game, {})[(chat_id, edit_msg_id)] = (uid, page, is_rich)
    else:
        msg, rich_ok = await richmsg.send_rich(bot, chat_id, header, rows,
                                               fallback_kb=fallback,
                                               reply_markup=fallback)
        if msg and _register:
            _open_browsers.setdefault(game, {})[(chat_id, msg.message_id)] = (uid, page, rich_ok)
        return msg, rich_ok


@router.callback_query(F.data.startswith("lb_open:"))
async def cb_open(c: CallbackQuery):
    game = c.data.split(":")[1]
    # заменяем меню на браузер (новым сообщением, т.к. rich не редактирует обычное)
    with contextlib.suppress(Exception):
        await c.message.delete()
    await show_browser(c.bot, c.message.chat.id, c.from_user.id, game, 0)
    await c.answer()


@router.callback_query(F.data.startswith("lb_page:"))
async def cb_page(c: CallbackQuery):
    _, game, page = c.data.split(":")
    await show_browser(c.bot, c.message.chat.id, c.from_user.id, game, int(page),
                       edit_msg_id=c.message.message_id)
    await c.answer()
