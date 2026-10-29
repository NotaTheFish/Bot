import asyncio
import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db import Database

from aiogram.filters import Command

router = Router()
logger = logging.getLogger(__name__)


def _is_admin(user_id: int, config) -> bool:
    return config.ADMIN_TG_ID and user_id == config.ADMIN_TG_ID


class AdSG(StatesGroup):
    enter_ban = State()         # ввод ID/юзернейма для бана
    enter_unban = State()       # ввод ID для разбана


# ── Клавиатуры ─────────────────────────────────────────────────────────────

def kb_admin_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton(text="🏆 Топ продавцов", callback_data="admin:topsellers")],
        [InlineKeyboardButton(text="🚫 Баны", callback_data="admin:bans")],
        [InlineKeyboardButton(text="📋 Журнал действий", callback_data="admin:log")],
        [InlineKeyboardButton(text="« Назад", callback_data="menu:back")],
    ])


# ── Вход в админку ─────────────────────────────────────────────────────────

@router.message(Command("admin"))
async def cmd_admin(message: Message, config):
    if not _is_admin(message.from_user.id, config):
        return  # молча игнорируем для не-админов
    await message.answer(
        "🛠 <b>Админ-панель</b>\n\nВыбери раздел:",
        reply_markup=kb_admin_main()
    )


@router.callback_query(F.data == "admin:home")
async def cb_admin_home(call: CallbackQuery, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await call.answer()
    await call.message.answer(
        "🛠 <b>Админ-панель</b>\n\nВыбери раздел:",
        reply_markup=kb_admin_main()
    )


@router.callback_query(F.data == "admin:stats")
async def cb_admin_stats(call: CallbackQuery, db: Database, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await call.answer()
    s = await db.get_global_stats()
    await call.message.answer(
        f"📊 <b>Статистика бота</b>\n\n"
        f"👥 Пользователей всего: <b>{s['total_users']}</b>\n"
        f"🆕 Новых за 7 дней: <b>{s['users_7d']}</b>\n\n"
        f"🏪 Продавцов: <b>{s['sellers']}</b>\n"
        f"📢 Каналов подключено: <b>{s['channels']}</b>\n"
        f"🎨 Карточек в конструкторе: <b>{s['custom_tpls']}</b>\n\n"
        f"💬 Отзывов всего: <b>{s['reviews']}</b>\n"
        f"📅 Отзывов за 7 дней: <b>{s['reviews_7d']}</b>\n"
        f"🛒 Уникальных покупателей: <b>{s['buyers']}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="« Назад", callback_data="admin:home")]
        ])
    )


# ── Топ продавцов ──────────────────────────────────────────────────────────

@router.callback_query(F.data == "admin:log")
async def cb_admin_log(call: CallbackQuery, db: Database, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await call.answer()
    entries = await db.get_admin_log(20)
    if not entries:
        await call.message.answer(
            "📋 Журнал пуст.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="« Назад", callback_data="admin:home")]
            ]))
        return
    labels = {
        "ban": "🚫 Бан", "unban": "✅ Разбан", "ban_seller": "🚫 Бан продавца",
    }
    lines = ["📋 <b>Журнал действий</b> (последние 20)\n"]
    for e in entries:
        when = e["created_at"].strftime("%d.%m %H:%M")
        label = labels.get(e["action"], e["action"])
        det = f" — {e['details']}" if e.get("details") else ""
        lines.append(f"<code>{when}</code> {label}{det}")
    await call.message.answer(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="« Назад", callback_data="admin:home")]
        ]))


@router.callback_query(F.data == "admin:topsellers")
async def cb_top_sellers(call: CallbackQuery, db: Database, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await call.answer()
    top = await db.get_top_sellers(10)
    if not top:
        await call.message.answer(
            "🏆 <b>Топ продавцов</b>\n\nПока нет ни одного отзыва.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="« Назад", callback_data="admin:home")]
            ])
        )
        return

    lines = ["🏆 <b>Топ-10 продавцов по отзывам</b>\n"]
    rows = []
    medals = ["🥇", "🥈", "🥉"]
    for i, s in enumerate(top):
        place = medals[i] if i < 3 else f"{i+1}."
        uname = f"@{s['username']}" if s.get("username") else f"ID <code>{s['id']}</code>"
        lines.append(f"{place} <b>{s['shop_name']}</b> ({uname}) — {s['review_count']} отзывов")
        # Кнопка-ссылка только если есть юзернейм: tg://user?id= Telegram
        # отклоняет (BUTTON_USER_INVALID), если юзер не писал боту
        if s.get("username"):
            btn_text = f"{place} {s['shop_name']}"[:60]
            rows.append([InlineKeyboardButton(text=btn_text, url=f"https://t.me/{s['username']}")])

    rows.append([InlineKeyboardButton(text="« Назад", callback_data="admin:home")])
    await call.message.answer(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )


# ── Система банов ──────────────────────────────────────────────────────────

def kb_bans_menu(banned_count: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🚫 Забанить", callback_data="admin:ban_add")],
    ]
    if banned_count > 0:
        rows.append([InlineKeyboardButton(text=f"📋 Список банов ({banned_count})", callback_data="admin:ban_list")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="admin:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "admin:bans")
async def cb_bans(call: CallbackQuery, state: FSMContext, db: Database, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await state.clear()
    await call.answer()
    banned = await db.get_banned_users()
    await call.message.answer(
        "🚫 <b>Система банов</b>\n\n"
        "Забаненные не могут писать боту — он их игнорирует. "
        "Бан хранится по ID, поэтому смена юзернейма не помогает обойти его.\n\n"
        f"Сейчас в бане: <b>{len(banned)}</b>",
        reply_markup=kb_bans_menu(len(banned))
    )


@router.callback_query(F.data.startswith("admin:ban_seller:"))
async def cb_ban_seller(call: CallbackQuery, db: Database, bot, config):
    """Быстрый бан продавца из алерта о накрутке."""
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    seller_id = int(call.data.split(":")[2])
    seller = await db.get_seller(seller_id)
    uname = seller.get("username") if seller else None
    await db.ban_user(seller_id, uname, reason="накрутка отзывов")
    await db.log_admin_action("ban_seller", f"seller={seller_id} (накрутка)")
    await call.answer("🚫 Продавец забанен")
    # Уведомляем забаненного
    from aiogram.types import ReplyKeyboardRemove
    admin_un = config.ADMIN_USERNAME
    contact = f"@{admin_un}" if admin_un else "администратору"
    try:
        await bot.send_message(seller_id,
            f"🚫 Вы заблокированы за подозрение в накрутке отзывов.\nПо вопросам — {contact}.",
            reply_markup=ReplyKeyboardRemove())
        await db.mark_ban_notified(seller_id)
    except Exception:
        pass
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await call.message.answer(f"✅ Продавец (ID <code>{seller_id}</code>) забанен за накрутку.")


@router.callback_query(F.data == "admin:ban_add")
async def cb_ban_add(call: CallbackQuery, state: FSMContext, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await call.answer()
    await state.set_state(AdSG.enter_ban)
    await call.message.answer(
        "🚫 Пришли <b>ID</b> или <b>@юзернейм</b> пользователя, которого нужно забанить.\n\n"
        "Если по юзернейму — бот найдёт его ID (для этого пользователь должен быть знаком боту).\n\n"
        "Отправь /cancel чтобы отменить."
    )


@router.message(AdSG.enter_ban, F.text == "/cancel")
@router.message(AdSG.enter_unban, F.text == "/cancel")
async def cb_ban_cancel(message: Message, state: FSMContext, db: Database):
    await state.clear()
    banned = await db.get_banned_users()
    await message.answer("Отменено.", reply_markup=kb_bans_menu(len(banned)))


@router.message(AdSG.enter_ban)
async def cb_ban_process(message: Message, state: FSMContext, db: Database, bot, config):
    if not _is_admin(message.from_user.id, config):
        return
    raw = (message.text or "").strip()
    await state.clear()

    user_id = None
    username = None
    if raw.lstrip("-").isdigit():
        user_id = int(raw)
    else:
        # По юзернейму — резолвим ID
        username = raw.lstrip("@")
        user_id = await db.find_user_id_by_username(username)
        if not user_id:
            banned = await db.get_banned_users()
            await message.answer(
                f"❌ Не нашёл пользователя @{username} среди знакомых боту. "
                "Забанить можно по числовому ID.",
                reply_markup=kb_bans_menu(len(banned))
            )
            return

    if config.ADMIN_TG_ID and user_id == config.ADMIN_TG_ID:
        await message.answer("❌ Нельзя забанить самого себя.")
        return

    await db.ban_user(user_id, username)
    await db.log_admin_action("ban", f"user={user_id} (@{username or '—'})")

    # Сразу уведомляем забаненного и убираем у него reply-клавиатуру
    from aiogram.types import ReplyKeyboardRemove
    admin_un = config.ADMIN_USERNAME
    contact = f"@{admin_un}" if admin_un else "администратору"
    try:
        await bot.send_message(
            user_id,
            f"🚫 Вы были забанены в этом боте.\nДля разбана напишите {contact}.",
            reply_markup=ReplyKeyboardRemove()
        )
        # Помечаем что уже уведомили — чтобы middleware не дублировал
        await db.mark_ban_notified(user_id)
    except Exception as e:
        logger.info(f"Не удалось уведомить забаненного {user_id}: {e}")

    # Ссылка на аккаунт — только по юзернейму (tg://user?id= вызывает BUTTON_USER_INVALID)
    rows = []
    if username:
        who = f"@{username} (ID <code>{user_id}</code>)"
        rows.append([InlineKeyboardButton(text="👤 Открыть профиль", url=f"https://t.me/{username}")])
    else:
        who = f"ID <code>{user_id}</code>"
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="admin:bans")])

    banned = await db.get_banned_users()
    await message.answer(
        f"✅ Забанен: {who}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )


def _build_ban_list(banned: list):
    """Собирает текст и клавиатуру списка банов.
    Кнопка профиля — только по юзернейму (tg://user?id= даёт BUTTON_USER_INVALID)."""
    lines = ["🚫 <b>Забаненные пользователи</b>\n"]
    rows = []
    for b in banned[:20]:
        uname = f"@{b['username']}" if b.get("username") else "без юзернейма"
        # Метка типа бана: авто-флуд с таймером или вечный админ-бан
        tag = ""
        if b.get("reason") == "autoflood":
            tag = " 🤖флуд"
        if b.get("expires_at"):
            import datetime as _dt
            exp = b["expires_at"]
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=_dt.timezone.utc)
            left = exp - _dt.datetime.now(_dt.timezone.utc)
            hrs = max(0, int(left.total_seconds() // 3600))
            tag += f" ⏳{hrs}ч"
        lines.append(f"• <code>{b['id']}</code> ({uname}){tag}")
        btns = []
        if b.get("username"):
            btns.append(InlineKeyboardButton(text=f"👤 {uname}"[:30],
                                             url=f"https://t.me/{b['username']}"))
        btns.append(InlineKeyboardButton(text=f"✅ Разбан {b['id']}"[:30],
                                         callback_data=f"admin:unban:{b['id']}"))
        rows.append(btns)
    lines.append("\n<i>У кого нет юзернейма — профиль по ID через поиск.</i>")
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="admin:bans")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "admin:ban_list")
async def cb_ban_list(call: CallbackQuery, db: Database, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await call.answer()
    banned = await db.get_banned_users()
    if not banned:
        await call.message.answer("Список банов пуст.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="« Назад", callback_data="admin:bans")]
            ]))
        return
    text, kb = _build_ban_list(banned)
    await call.message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("admin:unban:"))
async def cb_unban(call: CallbackQuery, db: Database, bot, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    uid = int(call.data.split(":")[2])
    await db.unban_user(uid)
    await db.log_admin_action("unban", f"user={uid}")
    await call.answer("✅ Разбанен")

    # Уведомляем разбаненного и сразу возвращаем reply-клавиатуру
    from keyboards import kb_main_reply
    try:
        await bot.send_message(
            uid,
            "✅ Вы были разбанены! Снова можете пользоваться ботом.\n\n"
            "Нажми кнопку ниже или /start чтобы продолжить 👇",
            reply_markup=kb_main_reply()
        )
    except Exception as e:
        logger.info(f"Не удалось уведомить разбаненного {uid}: {e}")
    # Обновляем список
    banned = await db.get_banned_users()
    if not banned:
        try:
            await call.message.edit_text("✅ Все разбанены. Список пуст.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="« Назад", callback_data="admin:bans")]
                ]))
        except Exception:
            pass
        return
    text, kb = _build_ban_list(banned)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        pass
