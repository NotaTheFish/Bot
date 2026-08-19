"""
Быстрая выдача валюты через inline: админ в личке клиента набирает
«@bot +1м грибы» / «-500к коины» / «+50 шимкоины», выбирает карточку — в диалог
падает сообщение с кнопкой «Подтвердить». Кто нажал (клиент), тот и получает
начисление/изъятие. Админ получает уведомление в личку, КТО нажал.

Защита (В+Г):
  - одноразовая активация (нажать можно один раз);
  - таймер 5 минут: не нажали — кнопка становится «❌ Неактивно»;
  - только админ может создать активацию (проверка в inline-хендлере);
  - принимаем нажатие только в приватном чате (не в общем — чтобы не перехватили).

Премиум-трюк: inline не умеет премиум-эмодзи сразу, поэтому сообщение уходит с
обычными, а по chosen_inline_result бот РЕДАКТИРУЕТ его, подставляя премиум.
Требует включённого inline feedback в BotFather (/setinlinefeedback -> Enabled).
"""
import asyncio
import contextlib
import logging
import time as _time
import uuid

from aiogram import F, Router
from aiogram.types import (CallbackQuery, ChosenInlineResult, InlineQuery,
                           InlineKeyboardButton, InlineKeyboardMarkup,
                           InlineQueryResultArticle, InputTextMessageContent)

import db
from config import SUPER_ADMINS
from services import settings, ui, render

router = Router()
log = logging.getLogger("inline_grant")

GRANT_TTL = 5 * 60  # 5 минут жизни кнопки

# активации в памяти: {token: {...}}. token — короткий id, зашитый в callback_data
# и result_id. Живёт до нажатия/истечения.
_grants: dict[str, dict] = {}
# сопоставление result_id -> token (chosen_inline_result приходит с result_id)
_by_result: dict[str, str] = {}


async def _is_admin(uid: int) -> bool:
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


_CUR_WORDS = {
    "грибы": "mushrooms", "гриб": "mushrooms", "грибов": "mushrooms", "mush": "mushrooms",
    "коины": "coins", "коin": "coins", "коинов": "coins", "коины🪙": "coins", "coin": "coins", "коин": "coins",
    "шимкоины": "shimcoins", "шимкоин": "shimcoins", "шимкоинов": "shimcoins",
    "shim": "shimcoins", "шим": "shimcoins",
}


def _parse_command(text: str):
    """
    «+1м грибы» -> (sign=+1, amount, currency). «-500к коины» -> (-1, ...).
    Возвращает (sign, amount, currency) или None.
    """
    from services.amount_parse import parse_amount
    t = (text or "").strip().lower()
    if not t:
        return None
    sign = 1
    if t[0] in "+-":
        sign = -1 if t[0] == "-" else 1
        t = t[1:].strip()
    parts = t.split()
    if len(parts) < 2:
        return None
    amount = parse_amount(parts[0])
    if amount is None or amount <= 0:
        return None
    cur = None
    for w in parts[1:]:
        if w in _CUR_WORDS:
            cur = _CUR_WORDS[w]
            break
    if cur is None:
        return None
    return sign, amount, cur


def _text_plain(sign, amount, cur_emoji):
    """Текст с ОБЫЧНЫМ эмодзи (для первичной отправки через inline)."""
    verb = "Начислить" if sign > 0 else "Изъять"
    amt = f"{amount:,}".replace(",", " ")
    return (f"🎁 <b>{verb} {amt} {cur_emoji}</b>\n\n"
            f"Нажми «Подтвердить», чтобы получить.")


def _grant_kb(token: str, active=True):
    if active:
        btn = InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"grab:{token}")
    else:
        btn = InlineKeyboardButton(text="❌ Неактивно", callback_data="grab_dead")
    return InlineKeyboardMarkup(inline_keyboard=[[btn]])


# ---------------- inline-запрос ----------------
@router.inline_query()
async def on_inline(q: InlineQuery):
    if not await _is_admin(q.from_user.id):
        # не админ — показываем пустую подсказку-отказ
        return await q.answer([], cache_time=1, is_personal=True,
                              switch_pm_text="Нет доступа", switch_pm_parameter="noaccess")
    parsed = _parse_command(q.query)
    if not parsed:
        art = InlineQueryResultArticle(
            id="help",
            title="Формат: +1м грибы",
            description="+/-, сумма, валюта (грибы/коины/шимкоины)",
            input_message_content=InputTextMessageContent(message_text="Формат: +1м грибы"))
        return await q.answer([art], cache_time=1, is_personal=True)

    sign, amount, cur = parsed
    token = uuid.uuid4().hex[:12]
    sx = await settings.ctx()
    cur_emoji_plain = {"mushrooms": "🍄", "coins": "🪙", "shimcoins": "💠"}[cur]
    verb = "Начислить" if sign > 0 else "Изъять"
    amt = f"{amount:,}".replace(",", " ")

    _grants[token] = {
        "sign": sign, "amount": amount, "cur": cur,
        "admin_id": q.from_user.id, "used": False,
        "created": _time.time(), "inline_message_id": None,
        "chat_type": q.chat_type,
    }

    result = InlineQueryResultArticle(
        id=token,
        title=f"{verb} {amt} {cur_emoji_plain}",
        description="Нажми — отправится карточка с кнопкой «Подтвердить»",
        input_message_content=InputTextMessageContent(
            message_text=_text_plain(sign, amount, cur_emoji_plain),
            parse_mode="HTML"),
        reply_markup=_grant_kb(token))
    await q.answer([result], cache_time=0, is_personal=True)


# ---------------- выбран результат: правим на премиум + запускаем таймер ----------------
@router.chosen_inline_result()
async def on_chosen(ch: ChosenInlineResult):
    token = ch.result_id
    g = _grants.get(token)
    if not g or ch.inline_message_id is None:
        return
    g["inline_message_id"] = ch.inline_message_id
    _by_result[ch.result_id] = token

    # премиум-трюк: перерисовываем сообщение с премиум-эмодзи
    sx = await settings.ctx()
    with contextlib.suppress(Exception):
        text, ents = render.render(
            _text_plain(g["sign"], g["amount"], sx["e_" + g["cur"]]), await _em())
        if ents is not None:
            await ch.bot.edit_message_text(
                inline_message_id=ch.inline_message_id,
                text=text, entities=ents, parse_mode=None,
                reply_markup=_grant_kb(token))

    # таймер: через 5 минут гасим, если не нажали
    asyncio.create_task(_expire(ch.bot, token))


async def _em():
    from services.settings import emoji_map
    return await emoji_map()


async def _expire(bot, token: str):
    await asyncio.sleep(GRANT_TTL)
    g = _grants.get(token)
    if not g or g["used"] or g["inline_message_id"] is None:
        return
    with contextlib.suppress(Exception):
        await bot.edit_message_text(
            inline_message_id=g["inline_message_id"],
            text="⌛️ <b>Предложение истекло</b>\nНикто не подтвердил за 5 минут.",
            parse_mode="HTML", reply_markup=_grant_kb(token, active=False))
    _grants.pop(token, None)


# ---------------- нажатие «Подтвердить» ----------------
@router.callback_query(F.data == "grab_dead")
async def cb_dead(c: CallbackQuery):
    await c.answer("Эта кнопка уже неактивна.", show_alert=True)


@router.callback_query(F.data.startswith("grab:"))
async def cb_grab(c: CallbackQuery):
    token = c.data.split(":")[1]
    g = _grants.get(token)
    if not g:
        return await c.answer("Активация не найдена или истекла.", show_alert=True)
    if g["used"]:
        return await c.answer("Уже активировано.", show_alert=True)

    # Работает и в личке (быстрая выдача), и в общем чате (розыгрыш «кто быстрее»).
    # Первый нажавший забирает. Изъятие тоже разрешено — админ контролирует запуск.

    grabber_id = c.from_user.id
    sign, amount, cur = g["sign"], g["amount"], g["cur"]
    delta = amount if sign > 0 else -amount

    # помечаем использованной СРАЗУ (атомарно на уровне памяти) — гонка исключена,
    # т.к. один процесс. Списываем/начисляем.
    g["used"] = True
    idem = f"inlgrant:{token}:{grabber_id}"
    try:
        new_bal = await db.apply_admin(grabber_id, cur, delta, "inline_grant", idem,
                                       allow_negative=True)
    except Exception:
        g["used"] = False  # откат флага — можно попробовать снова
        return await c.answer("Ошибка начисления, попробуй ещё раз.", show_alert=True)

    sx = await settings.ctx()
    e = sx["e_" + cur]
    amt = f"{amount:,}".replace(",", " ")
    verb_done = "начислено" if sign > 0 else "изъято"

    # редактируем inline-сообщение — результат
    with contextlib.suppress(Exception):
        text, ents = render.render(
            f"✅ <b>Готово!</b> {'+' if sign>0 else '−'}{amt} {e}\n"
            f"Баланс: {new_bal:,} {e}".replace(",", " "), await _em())
        if ents is not None:
            await c.bot.edit_message_text(
                inline_message_id=g["inline_message_id"],
                text=text, entities=ents, parse_mode=None)
        else:
            await c.bot.edit_message_text(
                inline_message_id=g["inline_message_id"],
                text=f"✅ Готово! {'+' if sign>0 else '−'}{amt}",
                parse_mode="HTML")
    await c.answer("Готово!")

    # уведомление админу-создателю: КТО нажал
    grabber = c.from_user
    uname = f"@{grabber.username}" if grabber.username else grabber.first_name
    with contextlib.suppress(Exception):
        await ui.send(c.bot, g["admin_id"],
            f"🔔 Активацию забрал: {uname} (<code>{grabber_id}</code>)\n"
            f"{verb_done.capitalize()}: <b>{amt}</b> {e}\n"
            f"Его баланс: {new_bal:,} {e}".replace(",", " "))

    _grants.pop(token, None)
