"""
Управление кастомными шаблонами: список, шаринг, удаление, получение по ключу.
"""
import secrets
import string
import logging
from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile
)
from aiogram.fsm.context import FSMContext
from aiogram.filters import StateFilter

from db import Database
from services.constructor import render_preview, LAYOUTS, FONTS
from aiogram.fsm.state import State, StatesGroup

router = Router()
logger = logging.getLogger(__name__)

import html as _html_mod


def _esc(t) -> str:
    """Эскейп имён шаблонов — они попадают в HTML-сообщения."""
    return _html_mod.escape(str(t or ""))


class ShareSG(StatesGroup):
    custom_uses = State()
    custom_days = State()


class RenameSG(StatesGroup):
    enter_name = State()


def _parse_uses(text: str):
    """Парсит число активаций. (ok, value|None, error). value=None — без лимита."""
    t = text.strip().lower()
    if t in ("0", "inf", "∞", "без лимита", "безлимит"):
        return True, None, ""
    if not t.isdigit():
        return False, None, "Нужно число, например 2."
    n = int(t)
    if n < 1:
        return False, None, "Минимум 1."
    if n > 100000:
        return False, None, "Максимум 100000."
    return True, n, ""


def _parse_duration(text: str):
    """Парсит срок: 13d / 13h. (ok, days|None, label, error). days=None — бессрочно."""
    import re as _re
    t = text.strip().lower().replace(" ", "")
    if t in ("0", "inf", "∞", "бессрочно", "бессрочный"):
        return True, None, "Бессрочно", ""
    m = _re.fullmatch(r"(\d+)([dhдч]?)", t)
    if not m:
        return False, None, "", "Формат: 13d или 6h."
    num = int(m.group(1))
    unit = m.group(2)
    if num < 1:
        return False, None, "", "Минимум 1."
    if unit in ("h", "ч"):
        if num > 8760:
            return False, None, "", "Максимум 8760 часов."
        return True, num / 24.0, f"{num} ч", ""
    if num > 365:
        return False, None, "", "Максимум 365 дней."
    return True, float(num), f"{num} дн", ""


def _gen_key() -> str:
    chars = string.ascii_lowercase + string.digits
    return "!" + "".join(secrets.choice(chars) for _ in range(8))


def _can_share(tpl: dict) -> bool:
    """Делиться можно если ты создатель ИЛИ отредактировал чужой шаблон."""
    return tpl["owner_id"] == tpl["creator_id"] or tpl["is_edited"]


def kb_templates_list(templates: list) -> InlineKeyboardMarkup:
    rows = []
    for tpl in templates:
        mark = "✏️" if tpl["is_edited"] else ""
        own = "👑" if tpl["owner_id"] == tpl["creator_id"] else "🎁"
        rows.append([InlineKeyboardButton(
            text=f"{own}{mark} {tpl['name']}",
            callback_data=f"mytpl:view:{tpl['id']}"
        )])
    rows.append([InlineKeyboardButton(text="➕ Создать новый", callback_data="mytpl:new")])
    rows.append([InlineKeyboardButton(text="🔑 Получить по ключу", callback_data="mytpl:bykey")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="menu:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_template_actions(tpl: dict) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="👁 Показать карточку", callback_data=f"mytpl:show:{tpl['id']}")],
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"mytpl:edit:{tpl['id']}")],
        [InlineKeyboardButton(text="🏷 Переименовать", callback_data=f"mytpl:rename:{tpl['id']}")],
    ]
    if _can_share(tpl):
        rows.append([InlineKeyboardButton(text="🔗 Поделиться (копия)", callback_data=f"mytpl:share:{tpl['id']}")])
        rows.append([InlineKeyboardButton(text="👑 Передать авторство", callback_data=f"mytpl:transfer:{tpl['id']}")])
        rows.append([InlineKeyboardButton(text="🗝 Мои ключи", callback_data=f"mytpl:keys:{tpl['id']}")])
    rows.append([InlineKeyboardButton(text="🗑 Удалить", callback_data=f"mytpl:del:{tpl['id']}")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="mytpl:list")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# Опции для гибких ключей
KEY_USES_OPTIONS = {
    "1": ("1 активация", 1),
    "5": ("5 активаций", 5),
    "10": ("10 активаций", 10),
    "inf": ("Без лимита", None),
}
KEY_DAYS_OPTIONS = {
    "1": ("1 день", 1),
    "7": ("7 дней", 7),
    "30": ("30 дней", 30),
    "inf": ("Бессрочно", None),
}


def _uses_label(max_uses) -> str:
    """Человекочитаемая подпись для количества активаций."""
    if max_uses is None:
        return "Без лимита"
    return f"{max_uses} актив."


def kb_share_uses(tpl_id: int) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=label, callback_data=f"sharekey:uses:{tpl_id}:{k}")]
            for k, (label, _) in KEY_USES_OPTIONS.items()]
    rows.append([InlineKeyboardButton(text="« Назад", callback_data=f"mytpl:view:{tpl_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_share_days(tpl_id: int, uses_key: str) -> InlineKeyboardMarkup:
    # uses_key больше не нужен в callback (значение хранится в FSM), но оставлен для совместимости
    rows = [[InlineKeyboardButton(text=label, callback_data=f"sharekey:days:{tpl_id}:x:{k}")]
            for k, (label, _) in KEY_DAYS_OPTIONS.items()]
    rows.append([InlineKeyboardButton(text="« Назад", callback_data=f"mytpl:share:{tpl_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def show_templates_list(message: Message, db: Database, user_id: int):
    templates = await db.list_custom_templates(user_id)
    if not templates:
        await message.answer(
            "🎨 У тебя пока нет кастомных шаблонов.\n\n"
            "Создай первый — собери карточку под себя!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Создать шаблон", callback_data="mytpl:new")],
                [InlineKeyboardButton(text="🔑 Получить по ключу", callback_data="mytpl:bykey")],
                [InlineKeyboardButton(text="« Назад", callback_data="menu:back")],
            ])
        )
        return
    await message.answer(
        f"🎨 <b>Твои шаблоны</b> ({len(templates)})\n\n"
        f"👑 — твои · 🎁 — полученные · ✏️ — отредактированные",
        reply_markup=kb_templates_list(templates)
    )


@router.callback_query(F.data == "mytpl:list")
async def cb_list(call: CallbackQuery, state: FSMContext, db: Database):
    await state.clear()
    await call.answer()
    await show_templates_list(call.message, db, call.from_user.id)


@router.callback_query(F.data == "mytpl:new")
async def cb_new(call: CallbackQuery, state: FSMContext, db: Database, config):
    await call.answer()
    from handlers.constructor import start_constructor
    await start_constructor(call.message, state, db, config.BOT_USERNAME)


@router.callback_query(F.data.startswith("mytpl:view:"))
async def cb_view(call: CallbackQuery, state: FSMContext, db: Database):
    await state.clear()
    tpl_id = int(call.data.split(":")[2])
    tpl = await db.get_custom_template(tpl_id)
    if not tpl or tpl["owner_id"] != call.from_user.id:
        await call.answer("Шаблон не найден", show_alert=True)
        return
    await call.answer()

    layout_name = LAYOUTS.get(tpl["layout"], tpl["layout"])
    font_name = FONTS.get(tpl["font"], (tpl["font"],))[0]
    origin = "Твой шаблон 👑" if tpl["owner_id"] == tpl["creator_id"] else \
             (f"Получен, отредактирован ✏️" if tpl["is_edited"] else
              f"Получен от @{tpl['creator_username'] or 'неизвестно'} 🎁")
    share_info = "✅ Можно делиться" if _can_share(tpl) else \
                 "🔒 Нельзя делиться (чужой шаблон, отредактируй чтобы стать владельцем)"

    await call.message.answer(
        f"🎨 <b>{_esc(tpl['name'])}</b>\n\n"
        f"Раскладка: {layout_name}\n"
        f"Шрифт: {font_name}\n"
        f"Статус: {origin}\n"
        f"{share_info}",
        reply_markup=kb_template_actions(tpl)
    )




@router.callback_query(F.data.startswith("mytpl:rename:"))
async def cb_rename(call: CallbackQuery, state: FSMContext, db: Database):
    tpl_id = int(call.data.split(":")[2])
    tpl = await db.get_custom_template(tpl_id)
    if not tpl or tpl["owner_id"] != call.from_user.id:
        await call.answer("Не найдено", show_alert=True)
        return
    await call.answer()
    await state.set_state(RenameSG.enter_name)
    await state.update_data(rename_tpl_id=tpl_id)
    await call.message.answer(
        f"🏷 Текущее название: <b>{_esc(tpl['name'])}</b>\n\n"
        "Пришли новое название (до 40 символов).\n\n"
        "Отправь /cancel чтобы отменить."
    )


@router.message(RenameSG.enter_name, F.text == "/cancel")
async def rename_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.")


@router.message(RenameSG.enter_name, F.text)
async def rename_input(message: Message, state: FSMContext, db: Database):
    name = message.text.strip()[:40]
    if not name:
        await message.answer("Пустое название. Пришли текст или /cancel.")
        return
    data = await state.get_data()
    tpl_id = data.get("rename_tpl_id")
    tpl = await db.get_custom_template(tpl_id) if tpl_id else None
    if not tpl or tpl["owner_id"] != message.from_user.id:
        await state.clear()
        await message.answer("Шаблон не найден.")
        return

    # Предупреждаем о дубле — из-за них и возникает путаница
    others = await db.list_custom_templates(message.from_user.id)
    dup = any(t["id"] != tpl_id and (t["name"] or "").lower() == name.lower()
              for t in (others or []))

    await state.clear()
    await db.update_custom_template(tpl_id, name=name)
    warn = ("\n\n⚠️ У тебя уже есть шаблон с таким названием — "
            "они будут выглядеть одинаково в списке." if dup else "")
    await message.answer(f"✅ Название: <b>{_esc(name)}</b>{warn}")

    tpl = await db.get_custom_template(tpl_id)
    await message.answer(
        f"🎨 <b>{_esc(tpl['name'])}</b>",
        reply_markup=kb_template_actions(tpl))


@router.callback_query(F.data.startswith("mytpl:edit:"))
async def cb_edit(call: CallbackQuery, state: FSMContext, db: Database, config):
    tpl_id = int(call.data.split(":")[2])
    tpl = await db.get_custom_template(tpl_id)
    if not tpl or tpl["owner_id"] != call.from_user.id:
        await call.answer("Не найдено", show_alert=True)
        return
    await call.answer()
    from handlers.constructor import start_constructor_edit
    await start_constructor_edit(call.message, state, db, tpl, config.BOT_USERNAME)

@router.callback_query(F.data.startswith("mytpl:show:"))
async def cb_show(call: CallbackQuery, db: Database, config):
    tpl_id = int(call.data.split(":")[2])
    tpl = await db.get_custom_template(tpl_id)
    if not tpl or tpl["owner_id"] != call.from_user.id:
        await call.answer("Не найдено", show_alert=True)
        return
    await call.answer()
    cfg = {
        "layout": tpl["layout"], "font": tpl["font"],
        "title_font": tpl.get("title_font", "caveat"),
        "text_color": tpl["text_color"], "accent_color": tpl["accent_color"],
        "bg_color": tpl["bg_color"], "bg_image": tpl["bg_image"],
        "creator_username": tpl["creator_username"], "is_edited": tpl["is_edited"],
        "extra_cfg": tpl.get("extra_cfg") or {},
    }
    png = await render_preview(cfg, config.BOT_USERNAME)
    await call.message.answer_photo(
        BufferedInputFile(png, filename="template.png"),
        caption=f"🎨 <b>{_esc(tpl['name'])}</b>"
    )


@router.callback_query(F.data.startswith("mytpl:share:"))
async def cb_share(call: CallbackQuery, state: FSMContext, db: Database, config):
    tpl_id = int(call.data.split(":")[2])
    tpl = await db.get_custom_template(tpl_id)
    if not tpl or tpl["owner_id"] != call.from_user.id:
        await call.answer("Не найдено", show_alert=True)
        return
    if not _can_share(tpl):
        await call.answer("Этот шаблон нельзя share — он чужой", show_alert=True)
        return
    await call.answer()
    await state.set_state(ShareSG.custom_uses)
    await state.update_data(share_tpl_id=tpl_id)
    await call.message.answer(
        f"🔗 <b>Поделиться «{_esc(tpl['name'])}»</b>\n\n"
        f"Получатель получит <b>копию</b> карточки (🎁).\n\n"
        f"Сколько раз можно активировать ключ? Выбери вариант "
        f"или напиши своё число в чат (например 2).",
        reply_markup=kb_share_uses(tpl_id)
    )


async def _ask_days(message: Message, state: FSMContext, tpl_id: int, max_uses):
    """Переход к выбору срока (после выбора активаций кнопкой или текстом)."""
    await state.set_state(ShareSG.custom_days)
    await state.update_data(share_tpl_id=tpl_id, share_uses=max_uses)
    await message.answer(
        "⏳ На какой срок выдать ключ? Выбери вариант или напиши свой срок в чат: "
        "<code>13d</code> — дни, <code>6h</code> — часы.",
        reply_markup=kb_share_days(tpl_id, "x")
    )


async def _finalize_share_key(message: Message, state: FSMContext, db: Database,
                               config, tpl_id: int, max_uses, expires_days,
                               uses_label: str, days_label: str):
    """Создаёт копию-ключ и показывает результат. Общая точка для кнопок и текста."""
    await state.clear()
    tpl = await db.get_custom_template(tpl_id)
    if not tpl:
        await message.answer("❌ Карточка не найдена.")
        return
    key = _gen_key()
    await db.create_template_key(key, tpl_id, key_type="copy",
                                 max_uses=max_uses, expires_days=expires_days)
    ref_link = f"https://t.me/{config.BOT_USERNAME}?start=tpl_{key[1:]}"
    await message.answer(
        f"🔗 <b>Ключ для «{_esc(tpl['name'])}» создан!</b>\n\n"
        f"🔑 Ключ: <code>{key}</code>\n\n"
        f"🌐 Реф-ссылка:\n<code>{ref_link}</code>\n\n"
        f"📊 Активаций: <b>{uses_label}</b>\n"
        f"⏳ Срок: <b>{days_label}</b>\n\n"
        f"<i>Управлять ключами можно в «🗝 Мои ключи».</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗝 Мои ключи", callback_data=f"mytpl:keys:{tpl_id}")],
            [InlineKeyboardButton(text="« Назад", callback_data=f"mytpl:view:{tpl_id}")],
        ])
    )


@router.callback_query(ShareSG.custom_uses, F.data.startswith("sharekey:uses:"))
async def cb_share_uses(call: CallbackQuery, state: FSMContext, db: Database):
    _, _, tpl_id, uses_key = call.data.split(":")
    await call.answer()
    max_uses = KEY_USES_OPTIONS[uses_key][1]
    await _ask_days(call.message, state, int(tpl_id), max_uses)


@router.message(ShareSG.custom_uses)
async def cb_share_uses_text(message: Message, state: FSMContext, db: Database):
    ok, value, err = _parse_uses(message.text or "")
    if not ok:
        await message.answer(f"❌ {err}")
        return
    data = await state.get_data()
    tpl_id = data.get("share_tpl_id")
    await _ask_days(message, state, tpl_id, value)


@router.callback_query(ShareSG.custom_days, F.data.startswith("sharekey:days:"))
async def cb_share_days(call: CallbackQuery, state: FSMContext, db: Database, config):
    parts = call.data.split(":")
    tpl_id = int(parts[2])
    days_key = parts[4]
    await call.answer()
    data = await state.get_data()
    max_uses = data.get("share_uses")
    expires_days = KEY_DAYS_OPTIONS[days_key][1]
    uses_label = _uses_label(max_uses)
    days_label = KEY_DAYS_OPTIONS[days_key][0]
    await _finalize_share_key(call.message, state, db, config, tpl_id,
                              max_uses, expires_days, uses_label, days_label)


@router.message(ShareSG.custom_days)
async def cb_share_days_text(message: Message, state: FSMContext, db: Database, config):
    ok, days, label, err = _parse_duration(message.text or "")
    if not ok:
        await message.answer(f"❌ {err}")
        return
    data = await state.get_data()
    tpl_id = data.get("share_tpl_id")
    max_uses = data.get("share_uses")
    uses_label = _uses_label(max_uses)
    await _finalize_share_key(message, state, db, config, tpl_id,
                              max_uses, days, uses_label, label)


@router.callback_query(F.data.startswith("mytpl:del:"))
async def cb_delete(call: CallbackQuery, db: Database):
    tpl_id = int(call.data.split(":")[2])
    ok = await db.delete_custom_template(tpl_id, call.from_user.id)
    if ok:
        await call.answer("🗑 Удалено")
        await call.message.answer(
            "🗑 Шаблон удалён.\n"
            "<i>Если ты им делился — у получателей он остаётся.</i>"
        )
        await show_templates_list(call.message, db, call.from_user.id)
    else:
        await call.answer("Не удалось удалить", show_alert=True)


# ── Передача авторства ─────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("mytpl:transfer:"))
async def cb_transfer(call: CallbackQuery, db: Database, config):
    tpl_id = int(call.data.split(":")[2])
    tpl = await db.get_custom_template(tpl_id)
    if not tpl or tpl["owner_id"] != call.from_user.id:
        await call.answer("Не найдено", show_alert=True)
        return
    if not _can_share(tpl):
        await call.answer("Этот шаблон чужой", show_alert=True)
        return
    await call.answer()

    # Только один активный ключ передачи на карточку
    if await db.has_transfer_key(tpl_id):
        await call.message.answer(
            f"⚠️ У этой карточки уже есть активный ключ передачи авторства.\n\n"
            f"Сначала удали его в «🗝 Мои ключи», потом создай новый.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🗝 Мои ключи", callback_data=f"mytpl:keys:{tpl_id}")],
                [InlineKeyboardButton(text="« Назад", callback_data=f"mytpl:view:{tpl_id}")],
            ])
        )
        return

    await call.message.answer(
        f"👑 <b>Передать авторство «{_esc(tpl['name'])}»</b>\n\n"
        f"⚠️ <b>Важно, это действие серьёзное:</b>\n"
        f"• карточка <b>исчезнет</b> из твоего списка\n"
        f"• у получателя станет <b>созданной</b> (👑), а не полученной\n"
        f"• твой водяной знак сменится на нового владельца у <b>всех</b>, кому ты её раздал\n"
        f"• передать можно <b>только одному</b> человеку\n\n"
        f"Создать ключ передачи авторства?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, создать ключ передачи", callback_data=f"transfergen:{tpl_id}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"mytpl:view:{tpl_id}")],
        ])
    )


@router.callback_query(F.data.startswith("transfergen:"))
async def cb_transfer_gen(call: CallbackQuery, db: Database, config):
    tpl_id = int(call.data.split(":")[1])
    tpl = await db.get_custom_template(tpl_id)
    if not tpl or tpl["owner_id"] != call.from_user.id:
        await call.answer("Не найдено", show_alert=True)
        return
    if await db.has_transfer_key(tpl_id):
        await call.answer("Ключ передачи уже существует", show_alert=True)
        return
    await call.answer()

    key = _gen_key()
    # Передача: всегда 1 активация, бессрочно
    await db.create_template_key(key, tpl_id, key_type="transfer",
                                 max_uses=1, expires_days=None)
    ref_link = f"https://t.me/{config.BOT_USERNAME}?start=tpl_{key[1:]}"

    await call.message.answer(
        f"👑 <b>Ключ передачи авторства создан!</b>\n\n"
        f"🔑 Ключ: <code>{key}</code>\n\n"
        f"🌐 Реф-ссылка:\n<code>{ref_link}</code>\n\n"
        f"⚠️ Как только этот ключ активируют — авторство уйдёт навсегда. "
        f"Передавай только тому, кому доверяешь.\n\n"
        f"<i>Передумал? Удали ключ в «🗝 Мои ключи».</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗝 Мои ключи", callback_data=f"mytpl:keys:{tpl_id}")],
            [InlineKeyboardButton(text="« Назад", callback_data=f"mytpl:view:{tpl_id}")],
        ])
    )


# ── Управление ключами ─────────────────────────────────────────────────────

def _key_status_label(db: Database, kr: dict) -> str:
    ok, reason = db._key_status(kr)
    if not ok:
        return "истёк" if reason == "expired" else "исчерпан"
    # Активен — показываем остаток
    parts = []
    if kr.get("max_uses") is not None:
        left = kr["max_uses"] - kr.get("uses_count", 0)
        parts.append(f"осталось {left}")
    else:
        parts.append("без лимита")
    if kr.get("expires_at"):
        import datetime as _dt
        exp = kr["expires_at"]
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=_dt.timezone.utc)
        delta = exp - _dt.datetime.now(_dt.timezone.utc)
        hours = max(0, int(delta.total_seconds() // 3600))
        if hours >= 24:
            parts.append(f"ещё {hours // 24}д")
        else:
            parts.append(f"ещё {hours}ч")
    else:
        parts.append("бессрочно")
    return ", ".join(parts)


def _build_keys_view(db: Database, tpl: dict, tpl_id: int, keys: list,
                     bot_username: str, show_links: bool):
    """Собирает текст и клавиатуру для экрана «Мои ключи».
    show_links=True — показывает реф-ссылку рядом с каждым ключом."""
    if not keys:
        text = (f"🗝 У карточки «{_esc(tpl['name'])}» пока нет ключей.\n\n"
                f"Создай ключ через «🔗 Поделиться» или «👑 Передать авторство».")
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="« Назад", callback_data=f"mytpl:view:{tpl_id}")]
        ])
        return text, kb

    lines = [f"🗝 <b>Ключи карточки «{_esc(tpl['name'])}»</b>\n"]
    rows = []
    for kr in keys:
        type_emoji = "👑" if kr["key_type"] == "transfer" else "🔗"
        type_label = "передача" if kr["key_type"] == "transfer" else "копия"
        status = _key_status_label(db, kr)
        lines.append(f"{type_emoji} <code>{kr['key']}</code> — {type_label} ({status})")
        if show_links:
            ref = f"https://t.me/{bot_username}?start=tpl_{kr['key'][1:]}"
            lines.append(f"   └ <code>{ref}</code>")
        rows.append([InlineKeyboardButton(
            text=f"🗑 {type_emoji} {kr['key']}",
            callback_data=f"delkey:{tpl_id}:{kr['key']}"
        )])

    # Кнопка-переключатель показа реф-ссылок
    if show_links:
        rows.append([InlineKeyboardButton(text="🙈 Скрыть реф-ссылки",
                                          callback_data=f"mytpl:keys:{tpl_id}")])
    else:
        rows.append([InlineKeyboardButton(text="🌐 Показать реф-ссылки",
                                          callback_data=f"mytpl:keyslinks:{tpl_id}")])
    lines.append("\n<i>Удали ключ, чтобы мгновенно перекрыть доступ при утечке.</i>")
    rows.append([InlineKeyboardButton(text="« Назад", callback_data=f"mytpl:view:{tpl_id}")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data.startswith("mytpl:keys:"))
async def cb_keys(call: CallbackQuery, db: Database, config):
    tpl_id = int(call.data.split(":")[2])
    tpl = await db.get_custom_template(tpl_id)
    if not tpl or tpl["owner_id"] != call.from_user.id:
        await call.answer("Не найдено", show_alert=True)
        return
    await call.answer()
    keys = await db.list_template_keys(tpl_id)
    text, kb = _build_keys_view(db, tpl, tpl_id, keys, config.BOT_USERNAME, show_links=False)
    # Если пришли из toggle (скрыть) — редактируем, иначе шлём новое
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        await call.message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("mytpl:keyslinks:"))
async def cb_keys_links(call: CallbackQuery, db: Database, config):
    tpl_id = int(call.data.split(":")[2])
    tpl = await db.get_custom_template(tpl_id)
    if not tpl or tpl["owner_id"] != call.from_user.id:
        await call.answer("Не найдено", show_alert=True)
        return
    await call.answer()
    keys = await db.list_template_keys(tpl_id)
    text, kb = _build_keys_view(db, tpl, tpl_id, keys, config.BOT_USERNAME, show_links=True)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        await call.message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("delkey:"))
async def cb_del_key(call: CallbackQuery, db: Database, config):
    _, tpl_id, key = call.data.split(":", 2)
    tpl_id = int(tpl_id)
    ok = await db.delete_template_key(key, call.from_user.id)
    if ok:
        await call.answer("🗑 Ключ удалён")
    else:
        await call.answer("Не удалось удалить", show_alert=True)
    keys = await db.list_template_keys(tpl_id)
    tpl = await db.get_custom_template(tpl_id)
    if not tpl:
        return
    text, kb = _build_keys_view(db, tpl, tpl_id, keys, config.BOT_USERNAME, show_links=False)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass


# ── Получение по ключу ─────────────────────────────────────────────────────

@router.callback_query(F.data == "mytpl:bykey")
async def cb_bykey(call: CallbackQuery, state: FSMContext):
    from handlers.constructor import ConstructorSG
    await call.answer()
    await state.set_state(ConstructorSG.enter_key)
    await call.message.answer(
        "🔑 Введи ключ шаблона (начинается с !):\n<i>Например: !abc12xyz</i>"
    )


import time as _time_mod

# Анти-брутфорс ключей: uid -> [timestamps неверных попыток]
_KEY_FAILS: dict = {}
_KEY_FAIL_LIMIT = 5       # неверных попыток
_KEY_FAIL_WINDOW = 3600   # за час


def _key_attempts_ok(user_id: int) -> bool:
    """False, если превышен лимит неверных попыток ключа за час."""
    now = _time_mod.time()
    fails = [t for t in _KEY_FAILS.get(user_id, []) if now - t < _KEY_FAIL_WINDOW]
    _KEY_FAILS[user_id] = fails
    if len(_KEY_FAILS) > 5000:
        _KEY_FAILS.clear()
    return len(fails) < _KEY_FAIL_LIMIT


def _key_fail(user_id: int):
    _KEY_FAILS.setdefault(user_id, []).append(_time_mod.time())


async def claim_template(user_id: int, username, key: str, db: Database, bot=None) -> tuple[bool, str]:
    """Активирует ключ. Тип 'copy' — создаёт копию; 'transfer' — передаёт авторство.
    bot — если передан, владелец карточки получит уведомление об активации."""
    # Анти-брутфорс: слишком много неверных ключей за час
    if not _key_attempts_ok(user_id):
        return False, "⏳ Слишком много неверных ключей. Попробуй через час."
    key_row = await db.get_template_key(key)
    if not key_row:
        _key_fail(user_id)
        return False, "❌ Ключ не найден."

    # Проверяем статус ключа (срок и лимит) ДО действий
    ok, reason = db._key_status(key_row)
    if not ok:
        if reason == "expired":
            return False, "❌ Срок действия этого ключа истёк."
        return False, "❌ Этот ключ исчерпал лимит активаций."

    src = await db.get_custom_template(key_row["template_id"])
    if not src:
        return False, "❌ Оригинальный шаблон не найден."

    # Имя того, кто активировал — для уведомления владельцу
    who = f"@{username}" if username else f"ID {user_id}"

    # ── Передача авторства ──
    if key_row["key_type"] == "transfer":
        # Нельзя передать самому себе
        if src["owner_id"] == user_id:
            return False, "❌ Нельзя передать авторство самому себе."
        # Атомарно используем ключ
        used_ok, used_reason = await db.consume_template_key(key, user_id)
        if not used_ok:
            return False, "❌ Ключ уже недействителен."
        old_owner = src["owner_id"]
        lineage = src.get("lineage_id") or src["id"]
        # Если у получателя ещё нет копии этой линии — создаём (станет «созданной»)
        existing = await db.get_user_template_by_lineage(user_id, lineage)
        if not existing:
            await db.create_custom_template(
                owner_id=user_id, creator_id=user_id, creator_username=username,
                lineage_id=lineage,
                name=src["name"], layout=src["layout"], font=src["font"],
                title_font=src.get("title_font", "caveat"),
                text_color=src["text_color"], accent_color=src["accent_color"],
                bg_color=src["bg_color"], bg_image=src["bg_image"],
                extra_cfg=src.get("extra_cfg") or {}, is_edited=False,
            )
        # Переносим авторство: водяной знак у всех копий → новый владелец,
        # у старого владельца карточка удаляется
        await db.transfer_authorship(lineage, old_owner, user_id, username)
        # Уведомляем бывшего владельца, что авторство ушло
        if bot:
            try:
                await bot.send_message(
                    old_owner,
                    f"👑 <b>Авторство передано</b>\n\n"
                    f"{who} активировал ключ передачи авторства карточки «{src['name']}».\n\n"
                    f"Теперь карточка принадлежит ему и пропала из твоего списка. "
                    f"У всех, кому ты её раздавал, водяной знак сменился на нового владельца."
                )
            except Exception as e:
                logger.info(f"Не удалось уведомить о передаче {old_owner}: {e}")
        return True, (f"👑 Тебе передали авторство карточки «{src['name']}»!\n"
                      f"Теперь она твоя — отображается как созданная, без чужого водяного знака.")

    # ── Обычная копия ──
    count = await db.count_custom_templates(user_id)
    if count >= 15:
        return False, "❌ У тебя уже 15 шаблонов — это максимум."

    # Атомарно используем активацию
    used_ok, used_reason = await db.consume_template_key(key, user_id)
    if not used_ok:
        if used_reason == "expired":
            return False, "❌ Срок действия ключа истёк."
        return False, "❌ Ключ исчерпал лимит активаций."

    # Создаём копию: owner=получатель, creator=оригинальный создатель,
    # lineage наследуется — чтобы при передаче авторства водяной знак обновился и здесь
    await db.create_custom_template(
        owner_id=user_id,
        creator_id=src["creator_id"],
        creator_username=src["creator_username"],
        lineage_id=src.get("lineage_id") or src["id"],
        name=src["name"],
        layout=src["layout"],
        font=src["font"],
        title_font=src.get("title_font", "caveat"),
        text_color=src["text_color"],
        accent_color=src["accent_color"],
        bg_color=src["bg_color"],
        bg_image=src["bg_image"],
        extra_cfg=src.get("extra_cfg") or {},
        is_edited=False,
    )
    # Уведомляем владельца карточки об активации копии
    if bot:
        # Сколько активаций осталось (для информативности)
        fresh_key = await db.get_template_key(key)
        left_note = ""
        if fresh_key and fresh_key.get("max_uses") is not None:
            left = fresh_key["max_uses"] - fresh_key.get("uses_count", 0)
            left_note = f"\nОсталось активаций: <b>{left}</b>"
        try:
            await bot.send_message(
                src["owner_id"],
                f"🔗 <b>Кто-то получил твою карточку</b>\n\n"
                f"{who} активировал ключ-копию карточки «{src['name']}».{left_note}"
            )
        except Exception as e:
            logger.info(f"Не удалось уведомить владельца {src['owner_id']}: {e}")
    return True, f"✅ Шаблон «{src['name']}» добавлен в твою коллекцию!"


@router.message(StateFilter(None), F.text.regexp(r"^!\w{6,12}$"))
async def cb_text_key(message: Message, db: Database, bot):
    """Ловит ключ вида !abc12xyz написанный прямо в чат (вне других сценариев)."""
    key = message.text.strip()
    ok, msg = await claim_template(message.from_user.id, message.from_user.username, key, db, bot)
    await message.answer(msg)
    if ok:
        await show_templates_list(message, db, message.from_user.id)
