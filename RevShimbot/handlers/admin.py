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
    enter_broadcast = State()   # ввод текста рассылки всем
    enter_pin = State()         # ввод текста для закрепа
    enter_ban = State()         # ввод ID/юзернейма для бана
    enter_unban = State()       # ввод ID для разбана


# ── Клавиатуры ─────────────────────────────────────────────────────────────

def kb_admin_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton(text="🏆 Топ продавцов", callback_data="admin:topsellers")],
        [InlineKeyboardButton(text="📣 Управление рекламой", callback_data="admin:ads")],
        [InlineKeyboardButton(text="🚫 Баны", callback_data="admin:bans")],
        [InlineKeyboardButton(text="« Назад", callback_data="menu:back")],
    ])


def kb_admin_ads(has_pins: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📢 Рассылка всем", callback_data="admin:ad_broadcast")],
        [InlineKeyboardButton(text="📌 Закреп (объявление)", callback_data="admin:ad_pin")],
    ]
    if has_pins:
        rows.append([InlineKeyboardButton(text="📌❌ Открепить рекламу", callback_data="admin:ad_unpin")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="admin:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_ad_confirm(kind: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data=f"admin:ad_send:{kind}"),
         InlineKeyboardButton(text="❌ Отмена", callback_data="admin:ads")],
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


# ── Управление рекламой ────────────────────────────────────────────────────

@router.callback_query(F.data == "admin:ads")
async def cb_admin_ads(call: CallbackQuery, state: FSMContext, db: Database, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await state.clear()
    await call.answer()
    test_note = ""
    if config.AD_TEST_ID:
        test_note = (f"\n\n⚠️ <b>Тестовый режим:</b> реклама уйдёт только пользователю "
                     f"<code>{config.AD_TEST_ID}</code>, а не всем.")
    else:
        test_note = "\n\n🟢 Боевой режим: реклама уйдёт <b>всем</b> пользователям бота."
    has_pins = await db.has_ad_pins()
    pin_note = "\n\n📌 Сейчас реклама <b>в закрепе</b>." if has_pins else ""
    await call.message.answer(
        "📣 <b>Управление рекламой</b>\n\n"
        "Два вида:\n"
        "• <b>Рассылка всем</b> — личное сообщение каждому пользователю бота\n"
        "• <b>Закреп</b> — отправляет сообщение и закрепляет его в чате с пользователем"
        + test_note + pin_note,
        reply_markup=kb_admin_ads(has_pins)
    )


@router.callback_query(F.data == "admin:ad_broadcast")
async def cb_ad_broadcast_start(call: CallbackQuery, state: FSMContext, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await call.answer()
    await state.set_state(AdSG.enter_broadcast)
    await call.message.answer(
        "📢 Пришли <b>текст рассылки</b> (можно с форматированием).\n"
        "Он уйдёт каждому пользователю бота как личное сообщение.\n\n"
        "Отправь /cancel чтобы отменить."
    )


@router.callback_query(F.data == "admin:ad_pin")
async def cb_ad_pin_start(call: CallbackQuery, state: FSMContext, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await call.answer()
    await state.set_state(AdSG.enter_pin)
    await call.message.answer(
        "📌 Пришли <b>текст объявления</b> для закрепа.\n"
        "Бот отправит его пользователю и закрепит в вашем чате.\n\n"
        "Отправь /cancel чтобы отменить."
    )


@router.message(AdSG.enter_broadcast, F.text == "/cancel")
@router.message(AdSG.enter_pin, F.text == "/cancel")
async def cb_ad_cancel(message: Message, state: FSMContext, db: Database):
    await state.clear()
    has_pins = await db.has_ad_pins()
    await message.answer("Отменено.", reply_markup=kb_admin_ads(has_pins))


@router.message(AdSG.enter_broadcast)
async def cb_ad_broadcast_text(message: Message, state: FSMContext, config):
    if not _is_admin(message.from_user.id, config):
        return
    # Сохраняем текст (с HTML-разметкой) для подтверждения
    await state.update_data(ad_text=message.html_text)
    await message.answer(
        "👀 <b>Предпросмотр рассылки:</b>\n\n" + message.html_text +
        "\n\n➖➖➖\nОтправить всем?",
        reply_markup=_kb_ad_confirm("broadcast")
    )


@router.message(AdSG.enter_pin)
async def cb_ad_pin_text(message: Message, state: FSMContext, config):
    if not _is_admin(message.from_user.id, config):
        return
    await state.update_data(ad_text=message.html_text)
    await message.answer(
        "👀 <b>Предпросмотр закрепа:</b>\n\n" + message.html_text +
        "\n\n➖➖➖\nОтправить и закрепить?",
        reply_markup=_kb_ad_confirm("pin")
    )


@router.callback_query(F.data.startswith("admin:ad_send:"))
async def cb_ad_send(call: CallbackQuery, state: FSMContext, db: Database, bot, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    kind = call.data.split(":")[2]  # broadcast | pin
    data = await state.get_data()
    ad_text = data.get("ad_text")
    await state.clear()
    if not ad_text:
        await call.answer("Текст потерян, начни заново", show_alert=True)
        return
    await call.answer()

    # Определяем получателей: тест-режим или все
    if config.AD_TEST_ID:
        targets = [config.AD_TEST_ID]
        mode_note = f"тестовый режим (только {config.AD_TEST_ID})"
    else:
        targets = await db.get_all_user_ids()
        mode_note = "все пользователи"

    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    status = await call.message.answer(
        f"⏳ Отправка ({mode_note})… Получателей: {len(targets)}"
    )

    sent, failed, blocked = 0, 0, 0
    for uid in targets:
        try:
            msg = await bot.send_message(uid, ad_text, parse_mode="HTML")
            if kind == "pin":
                try:
                    await bot.pin_chat_message(uid, msg.message_id, disable_notification=True)
                    # Запоминаем закреп, чтобы потом точно открепить и показать кнопку
                    await db.record_ad_pin(uid, msg.message_id)
                except Exception as e:
                    logger.info(f"Не удалось закрепить у {uid}: {e}")
            sent += 1
        except Exception as e:
            err = str(e).lower()
            if "blocked" in err or "deactivated" in err or "chat not found" in err:
                blocked += 1
                await db.mark_user_blocked(uid)
            else:
                failed += 1
                logger.info(f"Ошибка отправки рекламы {uid}: {e}")
        # Бережём лимиты Telegram: ~25-30 сообщений/сек безопасно
        await asyncio.sleep(0.05)

    kind_label = "Рассылка" if kind == "broadcast" else "Закреп"
    # После операции показываем актуальную клавиатуру (с кнопкой открепа если закрепили)
    has_pins = await db.has_ad_pins()
    await status.edit_text(
        f"✅ <b>{kind_label} завершена</b>\n\n"
        f"📨 Доставлено: <b>{sent}</b>\n"
        f"🚫 Заблокировали бота: <b>{blocked}</b>\n"
        f"⚠️ Прочие ошибки: <b>{failed}</b>",
        reply_markup=kb_admin_ads(has_pins)
    )


# ── Открепление рекламы ────────────────────────────────────────────────────

@router.callback_query(F.data == "admin:ad_unpin")
async def cb_ad_unpin_confirm(call: CallbackQuery, db: Database, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await call.answer()
    pins = await db.get_ad_pins()
    await call.message.answer(
        f"📌❌ Открепить рекламу?\n\n"
        f"Сейчас закреплено в <b>{len(pins)}</b> чат(ах). "
        "Бот снимет эти закрепы.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, открепить", callback_data="admin:ad_unpin_go"),
             InlineKeyboardButton(text="❌ Отмена", callback_data="admin:ads")],
        ])
    )


@router.callback_query(F.data == "admin:ad_unpin_go")
async def cb_ad_unpin_go(call: CallbackQuery, db: Database, bot, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await call.answer()
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    # Берём точные закрепы из БД (что бот реально закреплял)
    pins = await db.get_ad_pins()
    status = await call.message.answer(f"⏳ Открепляю… Закрепов: {len(pins)}")

    done, failed = 0, 0
    for uid, mid in pins:
        try:
            # Открепляем конкретное сообщение
            await bot.unpin_chat_message(uid, mid)
            done += 1
        except Exception as e:
            # Запасной вариант — снять все закрепы бота в этом чате
            try:
                await bot.unpin_all_chat_messages(uid)
                done += 1
            except Exception as e2:
                failed += 1
                logger.info(f"Не удалось открепить у {uid} (msg {mid}): {e2}")
        await asyncio.sleep(0.05)

    # Чистим записи о закрепах — реклама больше не закреплена
    await db.clear_ad_pins()

    await status.edit_text(
        f"✅ <b>Открепление завершено</b>\n\n"
        f"📌 Откреплено: <b>{done}</b>\n"
        f"⚠️ Ошибки: <b>{failed}</b>",
        reply_markup=kb_admin_ads(has_pins=False)
    )


# ── Топ продавцов ──────────────────────────────────────────────────────────

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
        lines.append(f"• <code>{b['id']}</code> ({uname})")
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
