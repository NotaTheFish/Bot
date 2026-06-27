"""
Управление кастомными шаблонами: список, шаринг, удаление, получение по ключу.
"""
import secrets
import string
from aiogram import Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    BufferedInputFile
)
from aiogram.fsm.context import FSMContext

from db import Database
from services.constructor import render_preview, LAYOUTS, FONTS

router = Router()


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
    ]
    if _can_share(tpl):
        rows.append([InlineKeyboardButton(text="🔗 Поделиться", callback_data=f"mytpl:share:{tpl['id']}")])
    rows.append([InlineKeyboardButton(text="🗑 Удалить", callback_data=f"mytpl:del:{tpl['id']}")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="mytpl:list")])
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
async def cb_list(call: CallbackQuery, db: Database):
    await call.answer()
    await show_templates_list(call.message, db, call.from_user.id)


@router.callback_query(F.data == "mytpl:new")
async def cb_new(call: CallbackQuery, state: FSMContext, db: Database, config):
    await call.answer()
    from handlers.constructor import start_constructor
    await start_constructor(call.message, state, db, config.BOT_USERNAME)


@router.callback_query(F.data.startswith("mytpl:view:"))
async def cb_view(call: CallbackQuery, db: Database):
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
        f"🎨 <b>{tpl['name']}</b>\n\n"
        f"Раскладка: {layout_name}\n"
        f"Шрифт: {font_name}\n"
        f"Статус: {origin}\n"
        f"{share_info}",
        reply_markup=kb_template_actions(tpl)
    )




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
        caption=f"🎨 <b>{tpl['name']}</b>"
    )


@router.callback_query(F.data.startswith("mytpl:share:"))
async def cb_share(call: CallbackQuery, db: Database, config):
    tpl_id = int(call.data.split(":")[2])
    tpl = await db.get_custom_template(tpl_id)
    if not tpl or tpl["owner_id"] != call.from_user.id:
        await call.answer("Не найдено", show_alert=True)
        return
    if not _can_share(tpl):
        await call.answer("Этот шаблон нельзя share — он чужой", show_alert=True)
        return
    await call.answer()

    key = _gen_key()
    await db.create_template_key(key, tpl_id)
    ref_link = f"https://t.me/{config.BOT_USERNAME}?start=tpl_{key[1:]}"

    await call.message.answer(
        f"🔗 <b>Шаблон «{tpl['name']}» готов к раздаче!</b>\n\n"
        f"🔑 Ключ (вставить в бота): <code>{key}</code>\n\n"
        f"🌐 Или реф-ссылка:\n<code>{ref_link}</code>\n\n"
        f"⚠️ Ключ и ссылка <b>одноразовые</b> — сработают только для одного человека. "
        f"Для нового получателя сделай новый ключ."
    )


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


# ── Получение по ключу ─────────────────────────────────────────────────────

@router.callback_query(F.data == "mytpl:bykey")
async def cb_bykey(call: CallbackQuery, state: FSMContext):
    from handlers.constructor import ConstructorSG
    await call.answer()
    await state.set_state(ConstructorSG.enter_key)
    await call.message.answer(
        "🔑 Введи ключ шаблона (начинается с !):\n<i>Например: !abc12xyz</i>"
    )


async def claim_template(user_id: int, username, key: str, db: Database) -> tuple[bool, str]:
    """Активирует ключ, создаёт копию шаблона для пользователя."""
    key_row = await db.get_template_key(key)
    if not key_row:
        return False, "❌ Ключ не найден."
    if key_row["used"]:
        return False, "❌ Этот ключ уже использован."

    # Лимит
    count = await db.count_custom_templates(user_id)
    if count >= 15:
        return False, "❌ У тебя уже 15 шаблонов — это максимум."

    src = await db.get_custom_template(key_row["template_id"])
    if not src:
        return False, "❌ Оригинальный шаблон не найден."

    # Создаём копию: owner=получатель, creator=оригинальный создатель, водяной знак сохраняется
    await db.create_custom_template(
        owner_id=user_id,
        creator_id=src["creator_id"],
        creator_username=src["creator_username"],
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
    await db.use_template_key(key, user_id)
    return True, f"✅ Шаблон «{src['name']}» добавлен в твою коллекцию!"


@router.message(F.text.regexp(r"^!\w{6,12}$"))
async def cb_text_key(message: Message, db: Database):
    """Ловит ключ вида !abc12xyz написанный прямо в чат."""
    key = message.text.strip()
    ok, msg = await claim_template(message.from_user.id, message.from_user.username, key, db)
    await message.answer(msg)
    if ok:
        await show_templates_list(message, db, message.from_user.id)
