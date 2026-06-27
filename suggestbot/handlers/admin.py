from aiogram import Router, F, Bot
from aiogram.filters import Filter
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    Message, CallbackQuery, InputMediaPhoto, InputMediaVideo, InlineKeyboardMarkup, InlineKeyboardButton
)

from config import ADMIN_IDS, CHANNEL_ID
from states import AdminStates
from db.queries import (
    get_submission, get_submission_content, update_submission_status,
    save_channel_msg, get_pending_submissions, get_setting, set_setting,
    get_user, ban_user, unban_user, get_banned_users, add_content, create_submission
)
from keyboards.admin_kb import (
    admin_menu_kb, accept_options_kb, author_btn_kb,
    settings_kb, banned_list_kb, pending_nav_kb, submission_admin_kb
)
from keyboards.user_kb import main_menu_kb
from utils.helpers import user_mention, submission_header
from utils.logger import log

router = Router()


class IsAdmin(Filter):
    async def __call__(self, event: Message | CallbackQuery) -> bool:
        user = event.from_user
        return user is not None and user.id in ADMIN_IDS


# ─── Меню Admin ───────────────────────────────────────────────────────────────

@router.message(IsAdmin(), F.text == "📋 Все актуальные заявки")
async def all_pending(message: Message, state: FSMContext) -> None:
    await state.clear()
    submissions = await get_pending_submissions()
    if not submissions:
        await message.answer("📭 Нет актуальных заявок.")
        return
    await state.update_data(pending_ids=[s["id"] for s in submissions], pending_idx=0)
    await _show_pending(message, submissions, 0, send_new=True)


async def _show_pending(event: Message | CallbackQuery, submissions: list, idx: int, send_new=False) -> None:
    sub = submissions[idx]
    sub_id = sub["id"]
    user_id = sub["user_id"]
    username = sub.get("username")
    first_name = sub.get("first_name", "")

    contents = await get_submission_content(sub_id)
    photos = [c for c in contents if c["content_type"] in ("photo", "video")]
    text_item = next((c for c in contents if c["content_type"] == "text"), None)
    caption = text_item["caption"] if text_item else (photos[0]["caption"] if photos else "")

    header = submission_header(sub_id, user_id, username, first_name)
    kb = pending_nav_kb(sub_id, idx, len(submissions))
    full_text = header + (caption or "—")

    chat_id = event.from_user.id if isinstance(event, CallbackQuery) else event.chat.id

    if photos:
        if len(photos) == 1:
            p = photos[0]
            if p["content_type"] == "photo":
                await event.bot.send_photo(chat_id, p["file_id"], caption=full_text, parse_mode="HTML")
            else:
                await event.bot.send_video(chat_id, p["file_id"], caption=full_text, parse_mode="HTML")
        else:
            media = []
            for i, p in enumerate(photos):
                cap = full_text if i == 0 else None
                if p["content_type"] == "photo":
                    media.append(InputMediaPhoto(media=p["file_id"], caption=cap, parse_mode="HTML"))
                else:
                    media.append(InputMediaVideo(media=p["file_id"], caption=cap, parse_mode="HTML"))
            await event.bot.send_media_group(chat_id, media)

        await event.bot.send_message(chat_id, f"Заявка <b>#{sub_id}</b>", reply_markup=kb, parse_mode="HTML")
    else:
        await event.bot.send_message(chat_id, full_text, reply_markup=kb, parse_mode="HTML")


@router.callback_query(IsAdmin(), F.data.startswith("nav:"))
async def navigate_pending(callback: CallbackQuery, state: FSMContext) -> None:
    await callback.answer()
    idx = int(callback.data.split(":")[1])
    data = await state.get_data()
    pending_ids: list = data.get("pending_ids", [])
    if not pending_ids or idx >= len(pending_ids):
        await callback.message.answer("Заявки не найдены.")
        return
    await state.update_data(pending_idx=idx)
    submissions = await get_pending_submissions()
    if not submissions:
        await callback.message.answer("📭 Больше нет актуальных заявок.")
        return
    await _show_pending(callback, submissions, idx)


# ─── Принять / Отклонить ──────────────────────────────────────────────────────

@router.callback_query(IsAdmin(), F.data.startswith("sub:accept:"))
async def accept_submission(callback: CallbackQuery, state: FSMContext) -> None:
    sub_id = int(callback.data.split(":")[2])
    await callback.answer()
    await state.update_data(current_sub_id=sub_id)
    await callback.message.answer(
        f"✅ Заявка <b>#{sub_id}</b>\nРедактировать текст перед публикацией?",
        parse_mode="HTML",
        reply_markup=accept_options_kb(sub_id),
    )


@router.callback_query(IsAdmin(), F.data.startswith("sub:reject:"))
async def reject_submission(callback: CallbackQuery, bot: Bot) -> None:
    sub_id = int(callback.data.split(":")[2])
    await update_submission_status(sub_id, "rejected", callback.from_user.id)
    await callback.answer("❌ Отклонено", show_alert=False)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await log(bot, f"❌ Заявка <b>#{sub_id}</b> отклонена админом {user_mention(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)}")


@router.callback_query(IsAdmin(), F.data.startswith("pub:back:"))
async def back_from_accept(callback: CallbackQuery) -> None:
    await callback.answer()
    await callback.message.delete()


# ─── Публикация: сразу ────────────────────────────────────────────────────────

@router.callback_query(IsAdmin(), F.data.startswith("pub:now:"))
async def publish_now(callback: CallbackQuery, state: FSMContext) -> None:
    sub_id = int(callback.data.split(":")[2])
    await callback.answer()
    await state.update_data(current_sub_id=sub_id, edit_mode=False)
    await callback.message.edit_text(
        f"👤 Добавить кнопку с именем автора в пост?",
        reply_markup=author_btn_kb(sub_id),
    )


# ─── Публикация: редактирование ───────────────────────────────────────────────

@router.callback_query(IsAdmin(), F.data.startswith("pub:edit:"))
async def publish_edit(callback: CallbackQuery, state: FSMContext) -> None:
    sub_id = int(callback.data.split(":")[2])
    await callback.answer()
    await state.update_data(current_sub_id=sub_id, edit_mode=True)
    await state.set_state(AdminStates.editing)
    await callback.message.answer(
        "✏️ Отправьте новый контент для публикации.\n"
        "Можно текст, фото, видео — или всё вместе.",
    )


@router.message(IsAdmin(), AdminStates.editing)
async def receive_edited_content(message: Message, state: FSMContext, bot: Bot) -> None:
    data = await state.get_data()
    sub_id: int = data.get("current_sub_id")
    edited_items: list = data.get("edited_items", [])

    if message.photo:
        edited_items.append({
            "type": "photo",
            "file_id": message.photo[-1].file_id,
            "caption": message.caption,
        })
    elif message.video:
        edited_items.append({
            "type": "video",
            "file_id": message.video.file_id,
            "caption": message.caption,
        })
    elif message.text:
        edited_items.append({"type": "text", "text": message.text})

    await state.update_data(edited_items=edited_items)

    # Ждём ещё сообщений (если медиагруппа) — показываем кнопку подтверждения
    await message.answer(
        f"Принято. Если всё — выберите что делать с кнопкой автора:",
        reply_markup=author_btn_kb(sub_id),
    )
    await state.set_state(None)


# ─── Финальная публикация в канал ─────────────────────────────────────────────

@router.callback_query(IsAdmin(), F.data.startswith("pub:author_"))
async def do_publish(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    parts = callback.data.split(":")
    show_author = parts[1] == "author_yes"
    sub_id = int(parts[2])

    await callback.answer()

    sub = await get_submission(sub_id)
    if not sub:
        await callback.message.answer("❌ Заявка не найдена.")
        return

    user_id = sub["user_id"]
    db_user = await get_user(user_id)
    username = db_user["username"] if db_user else None
    first_name = db_user["first_name"] if db_user else str(user_id)

    data = await state.get_data()
    edited_items: list = data.get("edited_items", [])
    edit_mode: bool = data.get("edit_mode", False)

    forward_mode = await get_setting("forward_mode")

    # Кнопка автора
    author_kb = None
    if show_author:
        name = f"@{username}" if username else first_name
        author_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"👤 {name}", url=f"tg://user?id={user_id}")]
        ])

    sent_msg = None

    if edit_mode and edited_items:
        # Публикуем отредактированный контент
        photos = [i for i in edited_items if i["type"] in ("photo", "video")]
        text_item = next((i for i in edited_items if i["type"] == "text"), None)
        caption = text_item["text"] if text_item else (photos[0].get("caption") if photos else None)

        if photos:
            if len(photos) == 1:
                p = photos[0]
                if p["type"] == "photo":
                    sent_msg = await bot.send_photo(CHANNEL_ID, p["file_id"], caption=caption,
                                                    parse_mode="HTML", reply_markup=author_kb)
                else:
                    sent_msg = await bot.send_video(CHANNEL_ID, p["file_id"], caption=caption,
                                                    parse_mode="HTML", reply_markup=author_kb)
            else:
                media = []
                for i, p in enumerate(photos):
                    cap = caption if i == 0 else None
                    if p["type"] == "photo":
                        media.append(InputMediaPhoto(media=p["file_id"], caption=cap, parse_mode="HTML"))
                    else:
                        media.append(InputMediaVideo(media=p["file_id"], caption=cap, parse_mode="HTML"))
                msgs = await bot.send_media_group(CHANNEL_ID, media)
                if msgs and author_kb:
                    # Добавляем кнопку отдельным постом
                    sent_msg = await bot.send_message(CHANNEL_ID, "👤 Автор:", reply_markup=author_kb)
                else:
                    sent_msg = msgs[0] if msgs else None
        elif caption:
            sent_msg = await bot.send_message(CHANNEL_ID, caption, parse_mode="HTML", reply_markup=author_kb)

    else:
        # Публикуем оригинал
        contents = await get_submission_content(sub_id)
        photos = [c for c in contents if c["content_type"] in ("photo", "video")]
        text_item = next((c for c in contents if c["content_type"] == "text"), None)
        caption = text_item["caption"] if text_item else (photos[0]["caption"] if photos else None)

        if forward_mode == "forward" and sub.get("source_chat_id") and sub.get("source_message_ids"):
            # Настоящий форвард — с плашкой "Переслано от"
            source_chat_id = sub["source_chat_id"]
            source_msg_ids = list(sub["source_message_ids"])
            try:
                fwd_msgs = await bot.forward_messages(
                    chat_id=CHANNEL_ID,
                    from_chat_id=source_chat_id,
                    message_ids=source_msg_ids,
                )
                sent_msg = fwd_msgs[-1] if fwd_msgs else None
                # Кнопку автора добавляем отдельным постом если нужна
                if author_kb and sent_msg:
                    sent_msg = await bot.send_message(CHANNEL_ID, "👤 Автор:", reply_markup=author_kb)
            except Exception as e:
                # Fallback на repost если форвард недоступен
                await callback.message.answer(
                    f"⚠️ Форвард недоступен (пользователь запретил). Публикую как репост.",
                )
                forward_mode = "repost"  # упадём в блок ниже

        if forward_mode == "repost":
            if photos:
                if len(photos) == 1:
                    p = photos[0]
                    if p["content_type"] == "photo":
                        sent_msg = await bot.send_photo(CHANNEL_ID, p["file_id"], caption=caption,
                                                        parse_mode="HTML", reply_markup=author_kb)
                    else:
                        sent_msg = await bot.send_video(CHANNEL_ID, p["file_id"], caption=caption,
                                                        parse_mode="HTML", reply_markup=author_kb)
                else:
                    media = []
                    for i, p in enumerate(photos):
                        cap = caption if i == 0 else None
                        if p["content_type"] == "photo":
                            media.append(InputMediaPhoto(media=p["file_id"], caption=cap, parse_mode="HTML"))
                        else:
                            media.append(InputMediaVideo(media=p["file_id"], caption=cap, parse_mode="HTML"))
                    msgs = await bot.send_media_group(CHANNEL_ID, media)
                    if msgs and author_kb:
                        sent_msg = await bot.send_message(CHANNEL_ID, "👤 Автор:", reply_markup=author_kb)
                    else:
                        sent_msg = msgs[0] if msgs else None
            elif caption:
                sent_msg = await bot.send_message(CHANNEL_ID, caption, parse_mode="HTML", reply_markup=author_kb)

    # Обновляем статус
    await update_submission_status(sub_id, "accepted", callback.from_user.id)
    if sent_msg:
        await save_channel_msg(sub_id, sent_msg.message_id)

    # Уведомляем юзера
    try:
        await bot.send_message(
            user_id,
            "🎉 <b>Ваша предложка принята!</b>\nСпасибо, она опубликована в канале.",
            parse_mode="HTML",
        )
    except Exception:
        pass

    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(f"✅ Заявка <b>#{sub_id}</b> опубликована!", parse_mode="HTML")
    await state.clear()

    await log(
        bot,
        f"✅ Заявка <b>#{sub_id}</b> принята и опубликована "
        f"админом {user_mention(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)}"
    )


# ─── Ответить юзеру ──────────────────────────────────────────────────────────

@router.callback_query(IsAdmin(), F.data.startswith("sub:reply:"))
async def start_reply(callback: CallbackQuery, state: FSMContext) -> None:
    parts = callback.data.split(":")
    sub_id = int(parts[2])
    user_id = int(parts[3]) if parts[3] != "0" else None

    if not user_id:
        sub = await get_submission(sub_id)
        user_id = sub["user_id"] if sub else None

    if not user_id:
        await callback.answer("Не удалось определить пользователя", show_alert=True)
        return

    await callback.answer()
    await state.set_state(AdminStates.replying)
    await state.update_data(reply_to_user_id=user_id, reply_sub_id=sub_id)
    await callback.message.answer(
        f"💬 Введите сообщение для пользователя (заявка #{sub_id}).\n"
        f"Отправьте /cancel для отмены."
    )


@router.message(IsAdmin(), AdminStates.replying)
async def send_reply(message: Message, state: FSMContext, bot: Bot) -> None:
    if message.text and message.text == "/cancel":
        await state.clear()
        await message.answer("Отменено.")
        return

    data = await state.get_data()
    target_user_id = data.get("reply_to_user_id")
    sub_id = data.get("reply_sub_id")

    try:
        await bot.send_message(
            target_user_id,
            f"📩 <b>Ответ администратора</b> по вашей заявке #{sub_id}:\n\n{message.text or '[медиа]'}",
            parse_mode="HTML",
        )
        await message.answer("✅ Ответ отправлен.")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить: {e}")

    await state.clear()


# ─── Бан / Разбан ────────────────────────────────────────────────────────────

@router.callback_query(IsAdmin(), F.data.startswith("sub:ban:"))
async def ban_from_submission(callback: CallbackQuery, bot: Bot) -> None:
    parts = callback.data.split(":")
    sub_id = int(parts[2])
    user_id = int(parts[3]) if parts[3] != "0" else None

    if not user_id:
        sub = await get_submission(sub_id)
        user_id = sub["user_id"] if sub else None

    if not user_id:
        await callback.answer("Не удалось определить пользователя", show_alert=True)
        return

    db_user = await get_user(user_id)
    await ban_user(user_id, "Забанен через предложку", callback.from_user.id)
    await callback.answer("🚫 Пользователь заблокирован", show_alert=True)

    name = f"@{db_user['username']}" if db_user and db_user["username"] else str(user_id)
    await log(bot, f"🚫 Пользователь {name} (<code>{user_id}</code>) заблокирован админом "
                   f"{user_mention(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)}")


@router.message(IsAdmin(), F.text == "🚫 Забаненные")
async def show_banned(message: Message) -> None:
    banned = await get_banned_users()
    if not banned:
        await message.answer("✅ Нет заблокированных пользователей.")
        return

    text = "🚫 <b>Заблокированные пользователи:</b>\n\n"
    for u in banned:
        name = f"@{u['username']}" if u["username"] else u["first_name"]
        text += f"• {name} (<code>{u['user_id']}</code>) — {u['ban_reason'] or '—'}\n"

    await message.answer(text, parse_mode="HTML", reply_markup=banned_list_kb(banned))


@router.callback_query(IsAdmin(), F.data.startswith("ban:unban:"))
async def do_unban(callback: CallbackQuery, bot: Bot) -> None:
    user_id = int(callback.data.split(":")[2])
    await unban_user(user_id)
    await callback.answer("✅ Разбанен", show_alert=False)

    db_user = await get_user(user_id)
    name = f"@{db_user['username']}" if db_user and db_user["username"] else str(user_id)
    await log(bot, f"🔓 Пользователь {name} (<code>{user_id}</code>) разбанен "
                   f"админом {user_mention(callback.from_user.id, callback.from_user.username, callback.from_user.first_name)}")

    banned = await get_banned_users()
    try:
        await callback.message.edit_reply_markup(reply_markup=banned_list_kb(banned))
    except Exception:
        pass


# ─── Настройки ────────────────────────────────────────────────────────────────

@router.message(IsAdmin(), F.text == "⚙️ Настройки")
async def show_settings(message: Message) -> None:
    forward_mode = await get_setting("forward_mode") or "repost"
    show_author = await get_setting("show_author_btn") or "true"
    await message.answer(
        "⚙️ <b>Настройки бота:</b>",
        parse_mode="HTML",
        reply_markup=settings_kb(forward_mode, show_author),
    )


@router.callback_query(IsAdmin(), F.data == "settings:toggle_forward")
async def toggle_forward(callback: CallbackQuery) -> None:
    current = await get_setting("forward_mode") or "repost"
    new_val = "forward" if current == "repost" else "repost"
    await set_setting("forward_mode", new_val)
    await callback.answer(f"Режим изменён: {new_val}")
    show_author = await get_setting("show_author_btn") or "true"
    await callback.message.edit_reply_markup(reply_markup=settings_kb(new_val, show_author))


@router.callback_query(IsAdmin(), F.data == "settings:toggle_author")
async def toggle_author(callback: CallbackQuery) -> None:
    current = await get_setting("show_author_btn") or "true"
    new_val = "false" if current == "true" else "true"
    await set_setting("show_author_btn", new_val)
    await callback.answer("Настройка обновлена")
    forward_mode = await get_setting("forward_mode") or "repost"
    await callback.message.edit_reply_markup(reply_markup=settings_kb(forward_mode, new_val))


# ─── Добавить канал ──────────────────────────────────────────────────────────

@router.message(IsAdmin(), F.text == "➕ Добавить канал")
async def add_channel_start(message: Message, state: FSMContext) -> None:
    await state.set_state(AdminStates.add_channel)
    await message.answer(
        "📢 <b>Добавление канала:</b>\n\n"
        "1. Добавьте бота в канал как администратора\n"
        "2. Выдайте ему права: <b>Публикация сообщений</b>\n"
        "   (остальные права бот использовать не будет)\n\n"
        "3. Перешлите сюда любое сообщение из канала.\n\n"
        "<i>Если выданы лишние права — бот всё равно работает корректно.</i>",
        parse_mode="HTML",
    )


@router.message(IsAdmin(), AdminStates.add_channel, F.forward_from_chat)
async def receive_channel(message: Message, state: FSMContext) -> None:
    channel = message.forward_from_chat
    await set_setting("channel_setup_done", "true")
    await state.clear()

    channel_done = await get_setting("channel_setup_done")
    await message.answer(
        f"✅ Канал <b>{channel.title}</b> (<code>{channel.id}</code>) зафиксирован!\n\n"
        f"Не забудьте прописать этот ID в переменную <code>CHANNEL_ID</code> в .env и перезапустить бота.",
        parse_mode="HTML",
        reply_markup=admin_menu_kb(channel_setup_done=True),
    )


# ─── Noop callback ───────────────────────────────────────────────────────────

@router.callback_query(F.data == "noop")
async def noop(callback: CallbackQuery) -> None:
    await callback.answer()
