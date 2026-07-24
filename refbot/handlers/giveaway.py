"""
Розыгрыши (giveaway): создание через FSM, меню, список.

Блок 2 — создание. Привязка/запуск/участие/розыгрыш/страйки — отдельными блоками.
Всё общение с чатом идёт через ui (премиум-эмодзи работают везде, кроме каналов —
там владелец добавит вручную).

Только главный админ (can_manage). Второстепенному кнопка «Розыгрыши» не видна.
"""
import contextlib
import logging

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

import db
import keyboards as kb
from config import SUPER_ADMINS
from services import settings, ui
from services.amount_parse import parse_amount, split_equally, fmt_amount

router = Router()
log = logging.getLogger("giveaway")


async def _can(uid: int) -> bool:
    """Розыгрышами управляет только главный админ."""
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


class GwNew(StatesGroup):
    title = State()
    key_on = State()
    key_off = State()
    announce = State()
    finish = State()
    # валюта выбирается кнопкой, не state
    places = State()
    prize_total = State()     # для «поровну»
    prize_manual = State()    # для «по местам»
    timer = State()


# ---------------- меню ----------------
@router.callback_query(F.data == "gw_menu")
async def cb_gw_menu(c: CallbackQuery, state: FSMContext):
    if not await _can(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    await state.clear()
    await ui.edit(c.message,
        "🎁 <b>Розыгрыши</b>\n\n"
        "Giveaway с подпиской: участник подписывается на чаты/каналы → участвует → "
        "случайные победители получают приз на баланс в боте.\n\n"
        "<i>Это не «конкурс активности» (/шимшайнуть) — там побеждают по числу "
        "сообщений. Тут — случайный розыгрыш среди подписчиков.</i>",
        reply_markup=await kb.gw_menu())
    await c.answer()


# ---------------- создание: старт ----------------
@router.callback_query(F.data == "gw_new")
async def cb_gw_new(c: CallbackQuery, state: FSMContext):
    if not await _can(c.from_user.id):
        return await c.answer("Только главный админ.", show_alert=True)
    await state.set_state(GwNew.title)
    await state.update_data(created_by=c.from_user.id)
    await ui.edit(c.message,
        "🎁 <b>Новый розыгрыш</b>\n\nШаг 1/8 — как назовём? Пришли название.",
        reply_markup=await kb.back_to("gw_menu"))
    await c.answer()


@router.message(GwNew.title)
async def gw_title(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    title = (msg.text or "").strip()
    if not title or len(title) > 100:
        return await ui.answer(msg, "Название до 100 символов. Ещё раз.")
    await state.update_data(title=title)
    await state.set_state(GwNew.key_on)
    await ui.answer(msg,
        "Шаг 2/8 — <b>ключ привязки</b>.\n\n"
        "Это команда, которой ты привяжешь чат/канал к розыгрышу. Напиши её в "
        "чате/канале — бот привяжет его.\n\n"
        "Например: <code>!шимм!</code>\n"
        "<i>Ключ должен быть уникальным среди активных розыгрышей.</i>")


@router.message(GwNew.key_on)
async def gw_key_on(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    key = (msg.text or "").strip()
    if not key or len(key) > 32 or " " in key:
        return await ui.answer(msg, "Ключ без пробелов, до 32 символов. Ещё раз.")
    # нельзя занять системные команды — иначе перехватят рулетку/привязку
    reserved = {"!шайн", "!шимм", "!отшимм", "!шимм шайн", "!шим шайн",
                "!баланс", "!шимм баланс", "!шим баланс"}
    if key.lower() in reserved:
        return await ui.answer(msg, "Этот ключ занят системной командой бота. Другой.")
    if await db.gw_key_taken(key):
        return await ui.answer(msg, "Этот ключ уже занят активным розыгрышем. Другой.")
    await state.update_data(key_on=key)
    await state.set_state(GwNew.key_off)
    await ui.answer(msg,
        "Шаг 3/8 — <b>ключ отвязки</b>.\n\n"
        f"Команда, чтобы отвязать чат/канал. Например: <code>!от{key.strip('!')}!</code>")


@router.message(GwNew.key_off)
async def gw_key_off(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    key = (msg.text or "").strip()
    if not key or len(key) > 32 or " " in key:
        return await ui.answer(msg, "Ключ без пробелов, до 32 символов. Ещё раз.")
    await state.update_data(key_off=key)
    await state.set_state(GwNew.announce)
    await ui.answer(msg,
        "Шаг 4/8 — <b>текст объявления</b>.\n\n"
        "Пришли пасту, которую бот опубликует при запуске. Премиум-эмодзи, жирный, "
        "курсив — всё сохранится (кроме каналов, там добавишь вручную).\n\n"
        "<i>Кнопку «Участвовать» бот добавит сам.</i>")


@router.message(GwNew.announce)
async def gw_announce(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    # сохраняем html_text, чтобы премиум и форматирование не потерялись
    html = msg.html_text if msg.text else ""
    if not html:
        return await ui.answer(msg, "Нужен текст. Ещё раз.")
    await state.update_data(announce_text=html)
    await state.set_state(GwNew.finish)
    await ui.answer(msg,
        "Шаг 5/8 — <b>текст завершения</b>.\n\n"
        "Пасту, которую бот покажет при подведении итогов (список победителей "
        "добавится автоматически).")


@router.message(GwNew.finish)
async def gw_finish(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    html = msg.html_text if msg.text else ""
    if not html:
        return await ui.answer(msg, "Нужен текст. Ещё раз.")
    await state.update_data(finish_text=html)
    await ui.answer(msg,
        "Шаг 6/8 — <b>валюта приза</b>. Выбери режим:",
        reply_markup=await kb.gw_reward_mode())


# ---------------- валюта ----------------
@router.callback_query(F.data.startswith("gwr:"))
async def cb_gw_reward(c: CallbackQuery, state: FSMContext):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    mode = c.data.split(":")[1]
    await state.update_data(reward_mode=mode)
    if mode == "other":
        await state.update_data(prizes=[], places=1, other_desc=None)
        await state.set_state(GwNew.places)
        await ui.edit(c.message,
            "🎀 <b>Другое</b> — приз выдашь лично.\n\n"
            "Сколько призовых мест? (1–50)",
            reply_markup=await kb.back_to("gw_menu"))
        return await c.answer()
    await state.set_state(GwNew.places)
    await ui.edit(c.message,
        "Шаг 7/8 — <b>сколько призовых мест</b>? (1–50)\n\n"
        "Пришли число.",
        reply_markup=await kb.back_to("gw_menu"))
    await c.answer()


@router.message(GwNew.places)
async def gw_places(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    t = (msg.text or "").strip()
    if not t.isdigit() or not (1 <= int(t) <= 50):
        return await ui.answer(msg, "Число от 1 до 50. Ещё раз.")
    places = int(t)
    await state.update_data(places=places)
    data = await state.get_data()
    mode = data["reward_mode"]

    if mode == "other":
        desc = data.get("_other_pending")
        # для other приза в валюте нет — сразу к таймеру
        await state.update_data(prizes=[{"place": i + 1} for i in range(places)])
        return await _ask_timer(msg, state)

    # грибы/коины/выбор/обе — спросим, как задать суммы
    await ui.answer(msg,
        f"Мест: <b>{places}</b>. Как задать призы?",
        reply_markup=await kb.gw_prize_method(both=(mode == "both")))


# ---------------- призы ----------------
@router.callback_query(F.data == "gwp:equal")
async def cb_prize_equal(c: CallbackQuery, state: FSMContext):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await state.set_state(GwNew.prize_total)
    data = await state.get_data()
    mode = data["reward_mode"]
    hint = {
        "mushrooms": "общую сумму в 🍄 грибах",
        "coins": "общую сумму в 🪙 коинах",
        "choice": "две суммы: 🍄 и 🪙 через «/» (напр. <code>1м / 20м</code>)",
        "both": "две суммы: 🍄 и 🪙 через «/» (напр. <code>1м / 20м</code>)",
    }[mode]
    await ui.edit(c.message,
        f"⚖️ <b>Поровну.</b> Пришли {hint}.\n\n"
        "Понимаю <code>1к</code>=1000, <code>1.5м</code>=1.5млн, пробелы. "
        "Остаток при делении сгорает.",
        reply_markup=await kb.back_to("gw_menu"))
    await c.answer()


@router.message(GwNew.prize_total)
async def gw_prize_total(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    data = await state.get_data()
    mode = data["reward_mode"]
    places = data["places"]
    t = (msg.text or "").strip()

    def build(mush_total, coin_total):
        mush = split_equally(mush_total, places) if mush_total else [0] * places
        coin = split_equally(coin_total, places) if coin_total else [0] * places
        return [{"place": i + 1, "mushrooms": mush[i], "coins": coin[i]}
                for i in range(places)]

    if mode in ("choice", "both"):
        parts = t.split("/")
        if len(parts) != 2:
            return await ui.answer(msg, "Нужно две суммы через «/». Например: <code>1м / 20м</code>")
        m = parse_amount(parts[0]); co = parse_amount(parts[1])
        if m is None or co is None:
            return await ui.answer(msg, "Не понял суммы. Например: <code>1м / 20м</code>")
        prizes = build(m, co)
    else:
        val = parse_amount(t)
        if val is None:
            return await ui.answer(msg, "Не понял сумму. Например: <code>100к</code> или <code>1.5м</code>")
        prizes = build(val if mode == "mushrooms" else 0,
                       val if mode == "coins" else 0)

    await state.update_data(prizes=prizes)
    await _show_prizes_then_timer(msg, state, prizes, mode)


@router.callback_query(F.data == "gwp:manual")
async def cb_prize_manual(c: CallbackQuery, state: FSMContext):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await state.set_state(GwNew.prize_manual)
    data = await state.get_data()
    mode = data["reward_mode"]
    ex = ("<code>100к</code>" if mode in ("mushrooms", "coins")
          else "<code>100к / 2м</code>")
    await ui.edit(c.message,
        f"✏️ <b>По местам.</b> Пришли призы по строкам, место за местом ({data['places']} шт).\n\n"
        f"Каждая строка — приз для места. Формат: {ex}\n"
        f"Для «на выбор»/«обе» — грибы/коины через «/».",
        reply_markup=await kb.back_to("gw_menu"))
    await c.answer()


@router.message(GwNew.prize_manual)
async def gw_prize_manual(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    data = await state.get_data()
    mode = data["reward_mode"]
    places = data["places"]
    lines = [l.strip() for l in (msg.text or "").splitlines() if l.strip()]
    if len(lines) != places:
        return await ui.answer(msg, f"Нужно ровно {places} строк (по строке на место). "
                                    f"Прислано {len(lines)}.")
    prizes = []
    for i, line in enumerate(lines):
        if mode in ("choice", "both"):
            parts = line.split("/")
            if len(parts) != 2:
                return await ui.answer(msg, f"Строка {i+1}: нужно грибы/коины через «/».")
            m = parse_amount(parts[0]); co = parse_amount(parts[1])
            if m is None or co is None:
                return await ui.answer(msg, f"Строка {i+1}: не понял суммы.")
            prizes.append({"place": i + 1, "mushrooms": m, "coins": co})
        else:
            v = parse_amount(line)
            if v is None:
                return await ui.answer(msg, f"Строка {i+1}: не понял сумму.")
            prizes.append({"place": i + 1,
                           "mushrooms": v if mode == "mushrooms" else 0,
                           "coins": v if mode == "coins" else 0})
    await state.update_data(prizes=prizes)
    await _show_prizes_then_timer(msg, state, prizes, mode)


async def _show_prizes_then_timer(msg, state, prizes, mode):
    """Показать разбивку призов и перейти к таймеру."""
    lines = []
    for p in prizes:
        if mode == "choice":
            lines.append(f"{p['place']} место — {fmt_amount(p['mushrooms'])} 🍄 "
                         f"или {fmt_amount(p['coins'])} 🪙")
        elif mode == "both":
            lines.append(f"{p['place']} место — {fmt_amount(p['mushrooms'])} 🍄 "
                         f"и {fmt_amount(p['coins'])} 🪙")
        elif mode == "mushrooms":
            lines.append(f"{p['place']} место — {fmt_amount(p['mushrooms'])} 🍄")
        else:
            lines.append(f"{p['place']} место — {fmt_amount(p['coins'])} 🪙")
    await ui.answer(msg, "🏆 <b>Призы</b>\n\n" + "\n".join(lines))
    await _ask_timer(msg, state)


async def _ask_timer(msg, state):
    await state.set_state(GwNew.timer)
    await ui.answer(msg,
        "Шаг 8/8 — <b>таймер</b>. Автозавершение по времени или вручную?",
        reply_markup=await kb.gw_timer_choice())


# ---------------- таймер + подтверждение ----------------
@router.callback_query(F.data == "gwt:none")
async def cb_timer_none(c: CallbackQuery, state: FSMContext):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await state.update_data(ends_at=None)
    await _confirm(c, state)


@router.callback_query(F.data == "gwt:set")
async def cb_timer_set(c: CallbackQuery, state: FSMContext):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await ui.edit(c.message,
        "⏰ Пришли дату и время завершения (МСК).\n\n"
        "Формат: <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
        "Например: <code>01.08.2026 20:00</code>",
        reply_markup=await kb.back_to("gw_menu"))
    await c.answer()


@router.message(GwNew.timer)
async def gw_timer_input(msg: Message, state: FSMContext):
    if not await _can(msg.from_user.id):
        return await state.clear()
    from datetime import datetime
    from zoneinfo import ZoneInfo
    t = (msg.text or "").strip()
    try:
        dt = datetime.strptime(t, "%d.%m.%Y %H:%M").replace(tzinfo=ZoneInfo("Europe/Moscow"))
    except ValueError:
        return await ui.answer(msg, "Не понял. Формат: <code>01.08.2026 20:00</code>")
    now = datetime.now(ZoneInfo("Europe/Moscow"))
    if dt <= now:
        return await ui.answer(msg, "Это время уже прошло. Дай будущую дату.")
    await state.update_data(ends_at=dt)
    await _confirm_msg(msg, state)


async def _confirm(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await ui.edit(c.message, _summary(data), reply_markup=await kb.gw_confirm())
    await c.answer()


async def _confirm_msg(msg: Message, state: FSMContext):
    data = await state.get_data()
    await ui.answer(msg, _summary(data), reply_markup=await kb.gw_confirm())


def _summary(data: dict) -> str:
    mode_h = {"mushrooms": "🍄 только грибы", "coins": "🪙 только коины",
              "choice": "🔄 на выбор", "both": "🍄🪙 обе сразу",
              "other": "🎀 другое (лично)"}[data["reward_mode"]]
    ends = data.get("ends_at")
    ends_h = ends.strftime("%d.%m.%Y %H:%M МСК") if ends else "вручную"
    return (f"🎁 <b>Проверь розыгрыш</b>\n\n"
            f"Название: <b>{data['title']}</b>\n"
            f"Ключ привязки: <code>{data['key_on']}</code>\n"
            f"Ключ отвязки: <code>{data['key_off']}</code>\n"
            f"Валюта: {mode_h}\n"
            f"Мест: <b>{data['places']}</b>\n"
            f"Завершение: {ends_h}\n\n"
            f"Всё верно?")


@router.callback_query(F.data == "gw_save")
async def cb_gw_save(c: CallbackQuery, state: FSMContext):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    data = await state.get_data()
    # финальная проверка ключа (мог занять другой розыгрыш, пока создавали)
    if await db.gw_key_taken(data["key_on"]):
        await state.clear()
        return await c.answer("Ключ уже заняли. Создай заново с другим.", show_alert=True)
    gid = await db.gw_create(data)
    await db.audit(c.from_user.id, "gw_create", {"id": gid, "title": data["title"]})
    await state.clear()
    await ui.edit(c.message,
        f"✅ Розыгрыш «{data['title']}» создан (черновик).\n\n"
        f"Теперь привяжи чаты/каналы: напиши <code>{data['key_on']}</code> в каждом. "
        f"Потом запусти его в «Мои розыгрыши».",
        reply_markup=await kb.gw_menu())
    await c.answer()


# ---------------- список / карточка ----------------
@router.callback_query(F.data == "gw_list")
async def cb_gw_list(c: CallbackQuery, state: FSMContext):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    await state.clear()
    gws = await db.gw_list()
    if not gws:
        return await c.answer("Розыгрышей пока нет.", show_alert=True)
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from services.ui import btn
    kbd = InlineKeyboardBuilder()
    status_h = {"draft": "черновик", "running": "идёт", "finished": "завершён"}
    for g in gws[:30]:
        await btn(kbd, f"{g['title']} · {status_h.get(g['status'], g['status'])}",
                  f"gw_open:{g['id']}")
    await btn(kbd, "Назад", "gw_menu", "back")
    kbd.adjust(1)
    await ui.edit(c.message, "🎁 <b>Мои розыгрыши</b>", reply_markup=kbd.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("gw_open:"))
async def cb_gw_open(c: CallbackQuery):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    gid = int(c.data.split(":")[1])
    g = await db.gw_get(gid)
    if not g:
        return await c.answer("Розыгрыш не найден.", show_alert=True)
    chats = await db.pool().fetch(
        "SELECT title, kind FROM rb_giveaway_chats WHERE giveaway_id=$1", gid)
    members = await db.pool().fetchval(
        "SELECT count(*) FROM rb_giveaway_members WHERE giveaway_id=$1", gid)
    status_h = {"draft": "черновик", "running": "идёт", "finished": "завершён"}
    chat_lines = "\n".join(f"  • {ch['title']} ({ch['kind']})" for ch in chats) or "  — пока не привязаны"
    await ui.edit(c.message,
        f"🎁 <b>{g['title']}</b>\n"
        f"Статус: {status_h.get(g['status'])}\n"
        f"Ключ: <code>{g['key_on']}</code>\n"
        f"Мест: {g['places']}\n"
        f"Участников: {members}\n\n"
        f"Привязано:\n{chat_lines}",
        reply_markup=await kb.gw_card(gid, g["status"]))
    await c.answer()


@router.callback_query(F.data.startswith("gw_del:"))
async def cb_gw_del(c: CallbackQuery):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    gid = int(c.data.split(":")[1])
    await db.gw_delete(gid)
    await db.audit(c.from_user.id, "gw_delete", {"id": gid})
    await c.answer("Удалён.", show_alert=True)
    await cb_gw_list(c, None) if False else None
    gws = await db.gw_list()
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from services.ui import btn
    kbd = InlineKeyboardBuilder()
    status_h = {"draft": "черновик", "running": "идёт", "finished": "завершён"}
    for g in gws[:30]:
        await btn(kbd, f"{g['title']} · {status_h.get(g['status'])}", f"gw_open:{g['id']}")
    await btn(kbd, "Назад", "gw_menu", "back")
    kbd.adjust(1)
    await ui.edit(c.message, "🎁 <b>Мои розыгрыши</b>", reply_markup=kbd.as_markup())


# ==================== ПРИВЯЗКА ПО КЛЮЧУ (Блок 3) ====================
from services import gw_invites


async def _handle_key(bot, chat, msg_id, text, from_user_id, is_channel):
    """
    Общая логика для чата и канала. Ловим ключ привязки/отвязки, привязываем,
    удаляем сообщение (или пишем в ЛС, если не смогли), уведомляем админа в ЛС.
    """
    key = (text or "").strip()

    gw_on = await db.gw_by_key_on(key)
    gw_off = await db.gw_by_key_off(key)
    if not gw_on and not gw_off:
        return  # не наш ключ

    # определить, кто админ бота, чтобы писать ему в ЛС (в канале from_user может быть None)
    admin_id = from_user_id
    kind = "channel" if is_channel else "chat"
    title = chat.title or str(chat.id)

    # попытка удалить сообщение с ключом
    deleted = False
    with contextlib.suppress(Exception):
        await bot.delete_message(chat.id, msg_id)
        deleted = True

    if gw_on:
        # создаём инвайт-ссылку (бот админ)
        link = await gw_invites.ensure_invite(bot, chat.id)
        await db.gw_bind_chat(gw_on["id"], chat.id, title, kind, link)
        note = (f"✅ {'Канал' if is_channel else 'Чат'} <b>{title}</b> привязан к "
                f"розыгрышу «{gw_on['title']}».")
        if not deleted:
            note += (f"\n\n⚠️ Не смог удалить твоё сообщение с ключом — удали сам "
                     f"(нет прав на удаление).")
        if not link:
            note += (f"\n\n⚠️ Не смог создать пригласительную ссылку. Дай боту право "
                     f"«Пригласительные ссылки» в настройках {'канала' if is_channel else 'чата'}.")
        await _notify_admin(bot, gw_on["created_by"], admin_id, note)

    elif gw_off:
        removed = await db.gw_unbind_chat(gw_off["id"], chat.id)
        note = (f"➖ {'Канал' if is_channel else 'Чат'} <b>{title}</b> отвязан от "
                f"розыгрыша «{gw_off['title']}»." if removed
                else f"Этот {'канал' if is_channel else 'чат'} и так не был привязан.")
        if not deleted:
            note += "\n\n⚠️ Сообщение с ключом удали сам — не было прав."
        await _notify_admin(bot, gw_off["created_by"], admin_id, note)


async def _notify_admin(bot, created_by, actor_id, text):
    """Пишем в ЛС тому, кто создал розыгрыш (и актору, если это другой человек)."""
    targets = {created_by}
    if actor_id:
        targets.add(actor_id)
    for t in targets:
        with contextlib.suppress(Exception):
            await ui.send(bot, t, text)


async def _is_gw_key(msg: Message) -> bool:
    """Фильтр: текст является ключом привязки/отвязки активного розыгрыша.
    Проверяет БД, чтобы НЕ перехватывать чужой текст (рулетку, обычные сообщения)."""
    text = (msg.text or "").strip()
    if not text or len(text) > 32 or " " in text:
        return False
    return bool(await db.gw_by_key_on(text) or await db.gw_by_key_off(text))


@router.message(F.chat.type.in_({"group", "supergroup"}), _is_gw_key)
async def catch_key_chat(msg: Message, bot):
    # только для главного админа: ключи привязки/отвязки работают от него
    if not await _can(msg.from_user.id):
        return
    await _handle_key(bot, msg.chat, msg.message_id, msg.text, msg.from_user.id, False)


@router.channel_post(_is_gw_key)
async def catch_key_channel(msg: Message, bot):
    # в канале from_user нет; привязку делает тот, кто постит от имени канала.
    # Уведомление уйдёт создателю розыгрыша.
    await _handle_key(bot, msg.chat, msg.message_id, msg.text, None, True)


# ==================== ЗАПУСК (Блок 4) ====================
from services import gw_publish


@router.callback_query(F.data.startswith("gw_run:"))
async def cb_gw_run(c: CallbackQuery):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    gid = int(c.data.split(":")[1])
    g = await db.gw_get(gid)
    if not g:
        return await c.answer("Не найден.", show_alert=True)
    if g["status"] != "draft":
        return await c.answer("Уже запущен или завершён.", show_alert=True)
    chats = await db.gw_chats(gid)
    if not chats:
        return await c.answer("Сначала привяжи хотя бы один чат/канал ключом.",
                              show_alert=True)

    await c.answer("Публикую…")
    ok, total, errors = await gw_publish.publish(c.bot, gid)
    await db.audit(c.from_user.id, "gw_run", {"id": gid, "ok": ok, "total": total})

    txt = f"🚀 Розыгрыш «{g['title']}» запущен.\n\nОпубликовано: {ok}/{total}"
    if errors:
        txt += "\n\n⚠️ Ошибки:\n" + "\n".join(f"• {e}" for e in errors[:5])
    await ui.edit(c.message, txt, reply_markup=await kb.gw_card(gid, "running"))


# ==================== УЧАСТИЕ (Блок 5) ====================
from aiogram.filters import CommandStart, CommandObject


@router.message(CommandStart(deep_link=True), F.text.regexp(r"/start\s+gw_\d+"))
async def gw_join_start(msg: Message, command: CommandObject, state: FSMContext):
    """Deep-link gw_<id> из кнопки «Участвовать» под объявлением."""
    await state.clear()
    code = (command.args or "").strip()
    if not code.startswith("gw_"):
        return
    try:
        gid = int(code[3:])
    except ValueError:
        return
    await db.upsert_user(msg.from_user.id, msg.from_user.username, msg.from_user.first_name)

    # забанен — вообще не пускаем
    if await db.is_banned(msg.from_user.id):
        return await ui.answer(msg, "🚫 Ты заблокирован в системе и не можешь участвовать.")

    g = await db.gw_get(gid)
    if not g or g["status"] != "running":
        return await ui.answer(msg, "Этот розыгрыш уже завершён или не найден.")

    await _show_join_screen(msg, gid, g)


async def _show_join_screen(msg, gid, g):
    chats = await db.gw_chats(gid)
    already = await db.gw_is_member(gid, msg.from_user.id)
    status = "✅ Ты уже участвуешь!" if already else "Подпишись на всё ниже и жми «Проверить»."
    await ui.answer(msg,
        f"🎁 <b>{g['title']}</b>\n\n"
        f"Чтобы участвовать, подпишись на все чаты и каналы:\n\n"
        f"{status}\n\n"
        f"<i>Если выйдешь из них до конца розыгрыша — не получишь приз и заработаешь "
        f"страйк.</i>",
        reply_markup=await kb.gw_subscribe(chats, gid, already))


@router.callback_query(F.data.startswith("gwjoin:"))
async def cb_gw_check(c: CallbackQuery):
    gid = int(c.data.split(":")[1])
    if await db.is_banned(c.from_user.id):
        return await c.answer("Ты заблокирован в системе.", show_alert=True)
    g = await db.gw_get(gid)
    if not g or g["status"] != "running":
        return await c.answer("Розыгрыш завершён.", show_alert=True)

    chats = await db.gw_chats(gid)
    missing = await gw_invites.check_all(c.bot, chats, c.from_user.id)
    if missing:
        names = ", ".join(m["title"] for m in missing)
        return await c.answer(f"Ты не подписан на: {names}. Подпишись и проверь снова.",
                              show_alert=True)

    # подписан везде. Если валюта на выбор — спросить, иначе сразу записать
    if g["reward_mode"] == "choice":
        await ui.edit(c.message,
            f"🎁 <b>{g['title']}</b>\n\nВыбери валюту приза — в ней получишь награду, "
            f"если выиграешь:",
            reply_markup=await kb.gw_currency_choice(gid))
        return await c.answer()

    await db.gw_join(gid, c.from_user.id)
    await db.audit(c.from_user.id, "gw_join", {"id": gid})
    await ui.edit(c.message,
        f"🎉 Ты участвуешь в «{g['title']}»!\n\n"
        f"Не выходи из чатов/каналов до конца — иначе приз сгорит и получишь страйк. "
        f"О результатах бот сообщит лично.")
    await c.answer("Записал!")


@router.callback_query(F.data.startswith("gwcur:"))
async def cb_gw_currency(c: CallbackQuery):
    _, gid_s, cur = c.data.split(":")
    gid = int(gid_s)
    if await db.is_banned(c.from_user.id):
        return await c.answer("Ты заблокирован.", show_alert=True)
    g = await db.gw_get(gid)
    if not g or g["status"] != "running":
        return await c.answer("Розыгрыш завершён.", show_alert=True)
    # перепроверим подписку (мог отписаться между шагами)
    chats = await db.gw_chats(gid)
    missing = await gw_invites.check_all(c.bot, chats, c.from_user.id)
    if missing:
        return await c.answer("Ты отписался от чата. Подпишись снова.", show_alert=True)

    await db.gw_join(gid, c.from_user.id, cur)
    await db.audit(c.from_user.id, "gw_join", {"id": gid, "currency": cur})
    cur_h = "🍄 грибах" if cur == "mushrooms" else "🪙 коинах"
    await ui.edit(c.message,
        f"🎉 Ты участвуешь в «{g['title']}»!\n\n"
        f"Приз получишь в {cur_h}, если выиграешь.\n"
        f"Не выходи из чатов до конца — иначе приз сгорит и получишь страйк.")
    await c.answer("Записал!")


# ==================== ЗАВЕРШЕНИЕ + СТРАЙКИ (Блок 6) ====================
from services import gw_draw
from services.amount_parse import fmt_amount


@router.callback_query(F.data.startswith("gw_members:"))
async def cb_gw_members(c: CallbackQuery):
    """Показать участников и КТО отписался — БЕЗ штрафа (штраф только на завершении)."""
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    gid = int(c.data.split(":")[1])
    g = await db.gw_get(gid)
    members = await db.gw_members(gid)
    if not members:
        return await c.answer("Пока никто не участвует.", show_alert=True)
    chats = await db.gw_chats(gid)

    await c.answer("Проверяю подписки…")
    lines = []
    subbed = gone = 0
    for m in members:
        missing = await gw_invites.check_all(c.bot, chats, m["tg_id"])
        u = await db.get_user(m["tg_id"])
        name = f"@{u['username']}" if u and u["username"] else (u["first_name"] if u else m["tg_id"])
        if missing:
            lines.append(f"❌ {name} — отписался")
            gone += 1
        else:
            lines.append(f"✅ {name}")
            subbed += 1

    text = (f"👥 <b>Участники «{g['title']}»</b>\n\n"
            f"Всего: {len(members)} · подписаны: {subbed} · отписались: {gone}\n\n"
            + "\n".join(lines[:50]) +
            "\n\n<i>Это просмотр. Страйки начислятся только при завершении.</i>")
    await ui.edit(c.message, text, reply_markup=await kb.gw_card(gid, "running"))


@router.callback_query(F.data.startswith("gw_finish:"))
async def cb_gw_finish(c: CallbackQuery):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    gid = int(c.data.split(":")[1])
    g = await db.gw_get(gid)
    if not g or g["status"] != "running":
        return await c.answer("Розыгрыш не запущен.", show_alert=True)

    await c.answer("Провожу розыгрыш…")
    await run_finish(c.bot, gid, by=c.from_user.id)
    await ui.edit(c.message, f"🏁 Розыгрыш «{g['title']}» завершён. Результаты опубликованы.",
                  reply_markup=await kb.gw_menu())


async def run_finish(bot, gid: int, by: int = None):
    """
    Ядро завершения — годится и для кнопки, и для таймера.
    Проводит розыгрыш, публикует результаты, шлёт личные уведомления.
    """
    g = await db.gw_get(gid)
    result = await gw_draw.finalize(bot, gid)
    if by:
        await db.audit(by, "gw_finish", {"id": gid, "winners": len(result["winners"])})

    # текст победителей для поста
    if result["winners"]:
        wlines = []
        for tg, place, prize, cur in result["winners"]:
            u = await db.get_user(tg)
            name = f"@{u['username']}" if u and u["username"] else (u["first_name"] if u else tg)
            wlines.append(f"{place}. {name} — {_prize_str(g['reward_mode'], prize, cur)}")
        winners_text = "🏆 <b>Победители:</b>\n" + "\n".join(wlines)
    else:
        winners_text = "😔 Победителей нет — недостаточно участников с подпиской."

    await gw_publish.publish_results(bot, gid, winners_text)

    # личные уведомления КАЖДОМУ участнику (только ему, не всем)
    winner_ids = {w[0] for w in result["winners"]}
    members = await db.gw_members(gid)
    for m in members:
        tg = m["tg_id"]
        with contextlib.suppress(Exception):
            if tg in winner_ids:
                w = next(x for x in result["winners"] if x[0] == tg)
                prize_s = _prize_str(g["reward_mode"], w[2], w[3])
                if g["reward_mode"] == "other":
                    await ui.send(bot, tg, f"🎉 Ты выиграл в «{g['title']}»!\n\n"
                                           f"Приз: {g.get('other_desc') or 'уточнит админ'}. "
                                           f"С тобой свяжутся лично.")
                else:
                    await ui.send(bot, tg, f"🎉 Ты выиграл в «{g['title']}»!\n\n"
                                           f"Приз: {prize_s} — уже на твоём балансе в боте.")
            elif tg in result["struck"]:
                n = await db.gw_get_strikes(tg)
                if tg in result["banned"]:
                    await ui.send(bot, tg, f"🚫 Ты отписался от чатов розыгрыша «{g['title']}» "
                                           f"и получил 3-й страйк — теперь заблокирован в боте.")
                else:
                    await ui.send(bot, tg, f"⚠️ Ты отписался от чатов розыгрыша «{g['title']}». "
                                           f"Приз сгорел, страйк {n}/3.")
            else:
                await ui.send(bot, tg, f"😔 Розыгрыш «{g['title']}» завершён. В этот раз не "
                                       f"повезло — не отчаивайся, будут ещё!")


def _prize_str(mode, prize, cur) -> str:
    if mode == "mushrooms":
        return f"{fmt_amount(prize['mushrooms'])} 🍄"
    if mode == "coins":
        return f"{fmt_amount(prize['coins'])} 🪙"
    if mode == "both":
        return f"{fmt_amount(prize['mushrooms'])} 🍄 + {fmt_amount(prize['coins'])} 🪙"
    if mode == "choice":
        c = cur or "mushrooms"
        return (f"{fmt_amount(prize['mushrooms'])} 🍄" if c == "mushrooms"
                else f"{fmt_amount(prize['coins'])} 🪙")
    return "приз (лично)"


# ---------------- панель страйков ----------------
@router.callback_query(F.data == "gw_strikes")
async def cb_gw_strikes(c: CallbackQuery):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    rows = await db.gw_strikes_list()
    if not rows:
        return await c.answer("Нарушителей нет.", show_alert=True)
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    from services.ui import btn
    kbd = InlineKeyboardBuilder()
    lines = []
    for r in rows[:30]:
        name = f"@{r['username']}" if r["username"] else (r["first_name"] or r["tg_id"])
        flag = "🚫 бан" if r["banned"] else f"{r['strikes']}/3"
        lines.append(f"{name} — {flag}")
        await btn(kbd, f"♻️ Сбросить {name}", f"gwstr_clear:{r['tg_id']}")
    await btn(kbd, "◀️ Назад", "gw_menu", "back")
    kbd.adjust(1)
    await ui.edit(c.message, "⚖️ <b>Страйки и баны</b>\n\n" + "\n".join(lines),
                  reply_markup=kbd.as_markup())
    await c.answer()


@router.callback_query(F.data.startswith("gwstr_clear:"))
async def cb_gw_strike_clear(c: CallbackQuery):
    if not await _can(c.from_user.id):
        return await c.answer("Нет доступа.", show_alert=True)
    tg = int(c.data.split(":")[1])
    await db.gw_clear_strikes(tg)
    await db.clear_ban(tg)
    await db.audit(c.from_user.id, "gw_strike_clear", {"tg_id": tg})
    await c.answer("Сброшено (страйки + бан).", show_alert=True)
    await cb_gw_strikes(c)


# ==================== ФОНОВЫЕ ЗАДАЧИ ====================
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


async def worker(bot):
    """
    Фоновый цикл: автозавершение по таймеру + автоудаление завершённых через неделю.
    Раз в минуту. Ошибки не роняют цикл.
    """
    log.info("giveaway worker запущен")
    while True:
        try:
            now = datetime.now(ZoneInfo("Europe/Moscow"))
            # таймеры
            due = await db.gw_due_timers(now)
            for gid in due:
                log.info("автозавершение розыгрыша %s по таймеру", gid)
                with contextlib.suppress(Exception):
                    await run_finish(bot, gid)
            # автоудаление завершённых старше недели
            cutoff = now - timedelta(days=7)
            old = await db.gw_finished_before(cutoff)
            for gid in old:
                log.info("автоудаление старого розыгрыша %s", gid)
                await db.gw_delete(gid)
        except Exception as e:
            log.warning("giveaway worker: %s", e)
        await asyncio.sleep(60)
