import os
import random
import logging
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command

from database import db
from keyboards.kb import (
    main_menu_kb, giveaway_menu_kb, giveaway_list_kb,
    channels_kb, confirm_kb, edit_menu_kb, channels_edit_kb
)
from services.giveaway_service import (
    publish_announcement, sync_published_messages, refresh_all_invite_links,
    check_user_subscriptions, revalidate_participants, prepare_channels,
)

logger = logging.getLogger(__name__)
router = Router()

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

PLACE_EMOJI = {1: "🥇", 2: "🥈", 3: "🥉"}


def place_emoji(n: int) -> str:
    return PLACE_EMOJI.get(n, "🏅")


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


# ── FSM States ────────────────────────────────────────────────────────────────

class CreateGiveaway(StatesGroup):
    title = State()
    key = State()
    announcement = State()
    prize_places = State()
    prizes = State()
    channels = State()


class PublishGiveaway(StatesGroup):
    channel_id = State()


class EditAnnouncement(StatesGroup):
    waiting_text = State()


class EditPrizes(StatesGroup):
    prize_places = State()
    prizes = State()


class EditChannels(StatesGroup):
    adding = State()


# ── Helpers ───────────────────────────────────────────────────────────────────

async def giveaway_info_text(g: dict) -> str:
    prizes = await db.get_prizes(g['id'])
    channels = await db.get_channels(g['id'])
    count = await db.get_participant_count(g['id'])
    eligible = await db.get_participant_count(g['id'], only_eligible=True)

    status_map = {'active': '🟢 Активен', 'cancelled': '🔴 Отменён', 'finished': '🏁 Завершён'}
    status = status_map.get(g['status'], g['status'])

    prizes_text = "\n".join(
        f"  {place_emoji(p['place'])} {p['place']} место — {p['description']}"
        for p in prizes
    )
    channels_text = "\n".join(f"  • {ch['chat_title']}" for ch in channels) or "  (нет каналов)"

    unsub_note = ""
    if count != eligible:
        unsub_note = f" (из них отписалось: {count - eligible})"

    return (
        f"🎯 <b>{g['title']}</b>\n"
        f"🔑 Ключ: <code>!{g['key']}!</code>\n"
        f"📊 Статус: {status}\n"
        f"👥 Участников: <b>{count}</b>{unsub_note}\n\n"
        f"🏆 Призовые места:\n{prizes_text}\n\n"
        f"📡 Каналы:\n{channels_text}"
    )


# ── /start ────────────────────────────────────────────────────────────────────

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer(
        "👋 Привет! Это бот для проведения розыгрышей.\n\n"
        "Выбери действие в меню ниже:",
        reply_markup=main_menu_kb()
    )


# ── Список конкурсов ──────────────────────────────────────────────────────────

@router.message(F.text == "📋 Мои конкурсы")
async def my_giveaways(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    giveaways = await db.get_all_giveaways()
    if not giveaways:
        await message.answer("У тебя пока нет конкурсов.", reply_markup=main_menu_kb())
        return
    await message.answer("📋 <b>Все конкурсы:</b>", reply_markup=giveaway_list_kb(giveaways))


@router.message(F.text == "🏆 Выбрать конкурс")
async def choose_giveaway(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    giveaways = await db.get_active_giveaways()
    if not giveaways:
        await message.answer("Нет активных конкурсов.", reply_markup=main_menu_kb())
        return
    await message.answer("Выбери конкурс:", reply_markup=giveaway_list_kb(giveaways))


@router.callback_query(F.data.startswith("select_giveaway:"))
async def select_giveaway(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    giveaway_id = int(callback.data.split(":")[1])
    g = await db.get_giveaway_by_id(giveaway_id)
    if not g:
        await callback.answer("Конкурс не найден.", show_alert=True)
        return

    await state.update_data(current_giveaway_id=giveaway_id)
    text = await giveaway_info_text(g)
    await callback.message.answer(text, reply_markup=giveaway_menu_kb())
    await callback.answer()


@router.message(F.text == "◀️ Назад к списку")
async def back_to_list(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("Главное меню:", reply_markup=main_menu_kb())


@router.message(F.text == "◀️ Назад к конкурсу")
async def back_to_giveaway(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("Главное меню:", reply_markup=main_menu_kb())
        return
    g = await db.get_giveaway_by_id(giveaway_id)
    if not g:
        await message.answer("Главное меню:", reply_markup=main_menu_kb())
        return
    text = await giveaway_info_text(g)
    await message.answer(text, reply_markup=giveaway_menu_kb())


# ── Создание конкурса ─────────────────────────────────────────────────────────

@router.message(F.text == "➕ Создать конкурс")
async def start_create(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(CreateGiveaway.title)
    await message.answer(
        "🎯 <b>Создание нового конкурса</b>\n\n"
        "Шаг 1/5 — Введи <b>название</b> конкурса (например: «Шимм — розыгрыш грибов»):"
    )


@router.message(CreateGiveaway.title)
async def process_title(message: Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await state.set_state(CreateGiveaway.key)
    await message.answer(
        "Шаг 2/5 — Введи уникальный <b>ключ</b> конкурса (латиница или кириллица, без пробелов).\n"
        "Пример: <code>shimm</code>\n\n"
        "Этот ключ будет использоваться для запуска конкурса в чате командой <code>!ключ!</code>"
    )


@router.message(CreateGiveaway.key)
async def process_key(message: Message, state: FSMContext):
    key = message.text.strip().lower().replace(" ", "")
    if await db.key_exists(key):
        await message.answer(
            f"❌ Ключ <code>{key}</code> уже занят. Введи другой ключ:"
        )
        return
    await state.update_data(key=key)
    await state.set_state(CreateGiveaway.announcement)
    await message.answer(
        "Шаг 3/5 — Напиши <b>текст объявления</b> конкурса.\n"
        "Пример:\n<i>🎉 Конкурс от Шимм Шимма! Разыгрываем 1 000 000 грибов. "
        "Подпишитесь на каналы и нажмите «Участвовать». Итоги: 31.12.2025</i>"
    )


@router.message(CreateGiveaway.announcement)
async def process_announcement(message: Message, state: FSMContext):
    await state.update_data(announcement=message.text.strip())
    await state.set_state(CreateGiveaway.prize_places)
    await message.answer(
        "Шаг 4/5 — Сколько <b>призовых мест</b>? Введи число (например: <code>3</code>):"
    )


@router.message(CreateGiveaway.prize_places)
async def process_prize_places(message: Message, state: FSMContext):
    try:
        places = int(message.text.strip())
        if places < 1 or places > 20:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи корректное число от 1 до 20:")
        return

    await state.update_data(prize_places=places, prizes_collected=[], current_place=1)
    await state.set_state(CreateGiveaway.prizes)
    await message.answer(
        f"Шаг 5/5 — Теперь введи награду за каждое место.\n\n"
        f"{place_emoji(1)} Что получает победитель <b>1 места</b>?"
    )


@router.message(CreateGiveaway.prizes)
async def process_prizes(message: Message, state: FSMContext):
    data = await state.get_data()
    prizes_collected: list = data.get("prizes_collected", [])
    current_place: int = data.get("current_place", 1)
    total_places: int = data.get("prize_places", 1)

    prizes_collected.append({"place": current_place, "description": message.text.strip()})
    current_place += 1
    await state.update_data(prizes_collected=prizes_collected, current_place=current_place)

    if current_place <= total_places:
        await message.answer(
            f"{place_emoji(current_place)} Что получает победитель <b>{current_place} места</b>?"
        )
    else:
        await state.set_state(CreateGiveaway.channels)
        await state.update_data(channels_collected=[])
        await message.answer(
            "✅ Призы добавлены!\n\n"
            "Теперь добавь <b>каналы/чаты</b> для проверки подписки.\n"
            "Перешли любое сообщение из нужного канала, или введи ID чата вручную.\n\n"
            "Важно: бот должен быть добавлен в эти каналы с правами администратора.\n\n"
            "Когда добавишь все каналы — напиши <code>готово</code>."
        )


async def resolve_chat_from_message(message: Message, bot: Bot) -> tuple[int | None, str | None]:
    """Extract (chat_id, chat_title) from a forwarded message, sender_chat, forward_origin, or manual ID."""
    if message.forward_from_chat:
        return message.forward_from_chat.id, message.forward_from_chat.title
    if message.sender_chat:
        return message.sender_chat.id, message.sender_chat.title
    if hasattr(message, 'forward_origin') and message.forward_origin:
        origin = message.forward_origin
        if hasattr(origin, 'chat') and origin.chat:
            return origin.chat.id, origin.chat.title
    if message.text:
        raw = message.text.strip()
        try:
            return int(raw), None
        except ValueError:
            return None, None
    return None, None


async def verify_bot_in_chat(bot: Bot, chat_id: int) -> tuple[bool, str | None]:
    """Returns (ok, invite_link_or_error). Checks bot is admin and gets an invite link
    WITHOUT rotating an existing primary link (only generates if needed)."""
    try:
        bot_info = await bot.get_me()
        member = await bot.get_chat_member(chat_id, bot_info.id)
        if member.status not in ('administrator', 'creator'):
            return False, "Бот не является администратором в этом чате."
    except Exception as e:
        logger.error(f"get_chat_member({chat_id}) failed: {e}")
        return False, f"Бот не найден в чате. Ошибка: {e}"

    # Try to create an invite link only now (this is a NEW giveaway channel, so safe to create once)
    try:
        invite_link = await bot.export_chat_invite_link(chat_id)
        return True, invite_link
    except Exception as e:
        logger.warning(f"export_chat_invite_link({chat_id}) failed: {e}")
        numeric = str(chat_id).replace("-100", "").lstrip("-")
        return True, f"https://t.me/c/{numeric}"


@router.message(CreateGiveaway.channels)
async def process_channels(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    channels_collected: list = data.get("channels_collected", [])

    if message.text and message.text.strip().lower() in ("готово", "done"):
        if not channels_collected:
            await message.answer("❌ Добавь хотя бы один канал или чат.")
            return

        giveaway_id = await db.create_giveaway(
            title=data['title'],
            key=data['key'],
            announcement=data['announcement'],
            prize_places=data['prize_places']
        )
        for prize in data['prizes_collected']:
            await db.add_prize(giveaway_id, prize['place'], prize['description'])
        for ch in channels_collected:
            await db.add_channel(giveaway_id, ch['chat_id'], ch['chat_title'], ch['invite_link'])

        g = await db.get_giveaway_by_id(giveaway_id)
        text = await giveaway_info_text(g)

        await state.update_data(current_giveaway_id=giveaway_id)
        await state.set_state(None)

        await message.answer(
            f"🎉 <b>Конкурс создан!</b>\n\n{text}",
            reply_markup=giveaway_menu_kb()
        )
        return

    chat_id, chat_title = await resolve_chat_from_message(message, bot)

    if chat_id is None:
        await message.answer(
            "❌ Не понял. Перешли сообщение из канала или введи числовой ID.\n"
            "Когда добавишь все каналы — напиши <code>готово</code>."
        )
        return

    ok, result = await verify_bot_in_chat(bot, chat_id)
    if not ok:
        await message.answer(f"❌ {result}")
        return
    invite_link = result

    if not chat_title:
        chat_title = str(chat_id)

    if any(ch['chat_id'] == chat_id for ch in channels_collected):
        await message.answer(f"⚠️ Чат <b>{chat_title}</b> уже добавлен.")
        return

    channels_collected.append({
        "chat_id": chat_id,
        "chat_title": chat_title,
        "invite_link": invite_link
    })
    await state.update_data(channels_collected=channels_collected)

    await message.answer(
        f"✅ Добавлен: <b>{chat_title}</b>\n\n"
        f"Всего каналов: {len(channels_collected)}\n"
        "Перешли ещё одно сообщение или введи ID, либо напиши <code>готово</code>."
    )


# ── Запустить в канале ────────────────────────────────────────────────────────

@router.message(F.text == "📢 Запустить в канале")
async def launch_in_channel(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return

    g = await db.get_giveaway_by_id(giveaway_id)
    if not g or g['status'] != 'active':
        await message.answer("❌ Конкурс не активен.")
        return

    channels = await db.get_channels(giveaway_id)
    if not channels:
        await message.answer("❌ У конкурса нет каналов. Создай конкурс заново.")
        return

    await state.set_state(PublishGiveaway.channel_id)
    await message.answer(
        "В какой канал опубликовать объявление?\n"
        "Введи ID канала или перешли любое сообщение из него:",
    )


@router.message(PublishGiveaway.channel_id)
async def process_publish_channel(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")

    target_chat_id, _ = await resolve_chat_from_message(message, bot)
    if target_chat_id is None:
        await message.answer("❌ Введи числовой ID канала или перешли сообщение.")
        return

    try:
        await publish_announcement(bot, giveaway_id, target_chat_id)
        await state.set_state(None)
        await message.answer(
            f"✅ Объявление опубликовано в чат <code>{target_chat_id}</code>!\n\n"
            "Если позже изменишь текст, призы или каналы — это сообщение обновится автоматически.",
            reply_markup=giveaway_menu_kb()
        )
    except Exception as e:
        await message.answer(
            f"❌ Не удалось опубликовать. Убедись, что бот — администратор канала.\nОшибка: {e}"
        )


# ── Редактирование конкурса ───────────────────────────────────────────────────

@router.message(F.text == "✏️ Редактировать")
async def edit_menu(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return
    await message.answer(
        "✏️ Что хочешь изменить?\n\n"
        "Все изменения автоматически применятся к уже опубликованным объявлениям.",
        reply_markup=edit_menu_kb()
    )


@router.message(F.text == "📝 Изменить текст")
async def edit_text_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return
    g = await db.get_giveaway_by_id(giveaway_id)
    await state.set_state(EditAnnouncement.waiting_text)
    await message.answer(
        f"Текущий текст объявления:\n\n{g['announcement']}\n\n"
        "Пришли новый текст объявления:"
    )


@router.message(EditAnnouncement.waiting_text)
async def edit_text_process(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")

    await db.update_giveaway_text(giveaway_id, message.text.strip())
    await state.set_state(None)

    await message.answer("✅ Текст обновлён. Обновляю опубликованные сообщения...")
    await sync_published_messages(bot, giveaway_id)

    g = await db.get_giveaway_by_id(giveaway_id)
    text = await giveaway_info_text(g)
    await message.answer(text, reply_markup=giveaway_menu_kb())


@router.message(F.text == "🏆 Изменить призы")
async def edit_prizes_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return
    await state.set_state(EditPrizes.prize_places)
    await message.answer(
        "Сколько теперь призовых мест? Введи число (например: <code>3</code>):"
    )


@router.message(EditPrizes.prize_places)
async def edit_prizes_places(message: Message, state: FSMContext):
    try:
        places = int(message.text.strip())
        if places < 1 or places > 20:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи корректное число от 1 до 20:")
        return

    await state.update_data(new_prize_places=places, new_prizes_collected=[], new_current_place=1)
    await state.set_state(EditPrizes.prizes)
    await message.answer(
        f"{place_emoji(1)} Что получает победитель <b>1 места</b>?"
    )


@router.message(EditPrizes.prizes)
async def edit_prizes_collect(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    prizes_collected: list = data.get("new_prizes_collected", [])
    current_place: int = data.get("new_current_place", 1)
    total_places: int = data.get("new_prize_places", 1)
    giveaway_id = data.get("current_giveaway_id")

    prizes_collected.append({"place": current_place, "description": message.text.strip()})
    current_place += 1
    await state.update_data(new_prizes_collected=prizes_collected, new_current_place=current_place)

    if current_place <= total_places:
        await message.answer(
            f"{place_emoji(current_place)} Что получает победитель <b>{current_place} места</b>?"
        )
    else:
        await db.replace_prizes(giveaway_id, prizes_collected)
        await state.set_state(None)
        await message.answer("✅ Призы обновлены. Обновляю опубликованные сообщения...")
        await sync_published_messages(bot, giveaway_id)

        g = await db.get_giveaway_by_id(giveaway_id)
        text = await giveaway_info_text(g)
        await message.answer(text, reply_markup=giveaway_menu_kb())


@router.message(F.text == "📡 Изменить каналы")
async def edit_channels_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return

    channels = await db.get_channels(giveaway_id)
    await message.answer(
        "📡 Текущие каналы — нажми чтобы удалить:",
        reply_markup=channels_edit_kb(channels)
    )
    await state.set_state(EditChannels.adding)
    await message.answer(
        "Чтобы добавить новый канал — перешли сообщение из него или введи ID.\n"
        "Когда закончишь — напиши <code>готово</code>."
    )


@router.callback_query(F.data.startswith("del_channel:"))
async def del_channel(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(callback.from_user.id):
        return
    channel_id = int(callback.data.split(":")[1])
    await db.delete_channel(channel_id)

    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if giveaway_id:
        await sync_published_messages(bot, giveaway_id)

    await callback.answer("🗑 Канал удалён", show_alert=True)
    channels = await db.get_channels(giveaway_id) if giveaway_id else []
    try:
        await callback.message.edit_reply_markup(reply_markup=channels_edit_kb(channels))
    except Exception:
        pass


@router.message(EditChannels.adding)
async def edit_channels_add(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")

    if message.text and message.text.strip().lower() in ("готово", "done"):
        await state.set_state(None)
        await sync_published_messages(bot, giveaway_id)
        g = await db.get_giveaway_by_id(giveaway_id)
        text = await giveaway_info_text(g)
        await message.answer("✅ Готово! Сообщения обновлены.", reply_markup=giveaway_menu_kb())
        await message.answer(text)
        return

    chat_id, chat_title = await resolve_chat_from_message(message, bot)
    if chat_id is None:
        await message.answer(
            "❌ Не понял. Перешли сообщение из канала или введи числовой ID.\n"
            "Или напиши <code>готово</code>."
        )
        return

    existing = await db.get_channels(giveaway_id)
    if any(ch['chat_id'] == chat_id for ch in existing):
        await message.answer("⚠️ Этот чат уже добавлен в конкурс.")
        return

    ok, result = await verify_bot_in_chat(bot, chat_id)
    if not ok:
        await message.answer(f"❌ {result}")
        return

    if not chat_title:
        chat_title = str(chat_id)

    await db.add_channel(giveaway_id, chat_id, chat_title, result)
    await message.answer(
        f"✅ Добавлен: <b>{chat_title}</b>\n"
        "Перешли ещё одно сообщение или введи ID, либо напиши <code>готово</code>."
    )


# ── Обновить ссылки ───────────────────────────────────────────────────────────

@router.message(F.text == "🔄 Обновить ссылки")
async def refresh_links(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return

    await message.answer("🔄 Обновляю пригласительные ссылки и опубликованные сообщения...")
    try:
        updated = await refresh_all_invite_links(bot, giveaway_id)
        names = "\n".join(f"  • {ch['chat_title']}" for ch in updated)
        await message.answer(
            f"✅ Ссылки обновлены для {len(updated)} каналов:\n{names}\n\n"
            "Старые ссылки на эти каналы перестанут работать — это нормально, "
            "новые кнопки в объявлениях уже актуальны."
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка при обновлении: {e}")


# ── Участники ─────────────────────────────────────────────────────────────────

@router.message(F.text == "👥 Участники")
async def show_participants(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return
    count = await db.get_participant_count(giveaway_id)
    eligible = await db.get_participant_count(giveaway_id, only_eligible=True)
    g = await db.get_giveaway_by_id(giveaway_id)

    await message.answer(
        f"👥 Конкурс «{g['title']}»\n"
        f"Всего участников: <b>{count}</b>\n"
        f"С активной подпиской: <b>{eligible}</b>\n"
        f"Отписалось: <b>{count - eligible}</b>\n\n"
        "🔍 Проверяю подписки прямо сейчас..."
    )
    result = await revalidate_participants(bot, giveaway_id, ADMIN_ID)
    if result['newly_unsubscribed'] == 0 and result['resubscribed'] == 0:
        await message.answer("✅ Изменений не найдено — все подписки актуальны.")
    else:
        await message.answer(
            f"Проверено: {result['checked']}\n"
            f"Новых отписавшихся: {result['newly_unsubscribed']}\n"
            f"Вернулись: {result['resubscribed']}"
        )


# ── Завершить конкурс ─────────────────────────────────────────────────────────

@router.message(F.text == "✅ Завершить конкурс")
async def finish_giveaway_confirm(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return
    g = await db.get_giveaway_by_id(giveaway_id)
    prizes = await db.get_prizes(giveaway_id)

    await message.answer("🔍 Финальная проверка подписок перед розыгрышем...")
    await revalidate_participants(bot, giveaway_id, ADMIN_ID)

    eligible = await db.get_participant_count(giveaway_id, only_eligible=True)

    await message.answer(
        f"❓ Завершить конкурс «{g['title']}»?\n"
        f"Участников с активной подпиской: <b>{eligible}</b>\n"
        f"Призовых мест: <b>{len(prizes)}</b>\n\n"
        "Будут случайно выбраны победители среди подписанных участников.",
        reply_markup=confirm_kb("finish", giveaway_id)
    )


@router.callback_query(F.data.startswith("confirm_finish:"))
async def do_finish_giveaway(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(callback.from_user.id):
        return
    giveaway_id = int(callback.data.split(":")[1])
    g = await db.get_giveaway_by_id(giveaway_id)
    participants = await db.get_participants(giveaway_id, only_eligible=True)
    prizes = await db.get_prizes(giveaway_id)

    if not participants:
        await callback.message.edit_text("❌ Нет участников с активной подпиской для розыгрыша.")
        return

    places_count = min(len(prizes), len(participants))
    winners_pool = random.sample(participants, places_count)

    result_lines = [f"🏆 <b>Итоги конкурса «{g['title']}»</b>\n"]
    for i, winner in enumerate(winners_pool):
        prize = prizes[i]
        uname = f"@{winner['username']}" if winner['username'] else winner['full_name']
        await db.add_winner(
            giveaway_id, winner['user_id'], winner['username'],
            winner['full_name'], prize['place'], prize['description']
        )
        result_lines.append(
            f"{place_emoji(prize['place'])} <b>{prize['place']} место</b> — {uname}\n"
            f"   Приз: {prize['description']}"
        )

    await db.update_giveaway_status(giveaway_id, "finished")
    await state.update_data(current_giveaway_id=None)

    result_text = "\n\n".join(result_lines)
    await callback.message.edit_text(result_text)
    await callback.message.answer(
        "✅ Конкурс завершён! Скопируй результаты и опубликуй их вручную или через «Запустить в канале».",
        reply_markup=main_menu_kb()
    )
    await callback.answer()


# ── Отменить конкурс ──────────────────────────────────────────────────────────

@router.message(F.text == "❌ Отменить конкурс")
async def cancel_giveaway_confirm(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return
    g = await db.get_giveaway_by_id(giveaway_id)
    await message.answer(
        f"❓ Отменить конкурс «{g['title']}»?\nЭто действие нельзя отменить.",
        reply_markup=confirm_kb("cancel", giveaway_id)
    )


@router.callback_query(F.data.startswith("confirm_cancel:"))
async def do_cancel_giveaway(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    giveaway_id = int(callback.data.split(":")[1])
    g = await db.get_giveaway_by_id(giveaway_id)
    await db.update_giveaway_status(giveaway_id, "cancelled")
    await state.update_data(current_giveaway_id=None)
    await callback.message.edit_text(f"🔴 Конкурс «{g['title']}» отменён.")
    await callback.message.answer("Главное меню:", reply_markup=main_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "cancel_action")
async def cancel_action(callback: CallbackQuery):
    await callback.message.edit_text("Действие отменено.")
    await callback.answer()


# ── Удалить конкурс ───────────────────────────────────────────────────────────

@router.message(F.text == "🗑 Удалить конкурс")
async def delete_giveaway_confirm(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return
    g = await db.get_giveaway_by_id(giveaway_id)
    await message.answer(
        f"🗑 Удалить конкурс «{g['title']}» навсегда?\nВсе участники, призы и каналы будут удалены. Это нельзя отменить.",
        reply_markup=confirm_kb("delete", giveaway_id)
    )


@router.callback_query(F.data.startswith("confirm_delete:"))
async def do_delete_giveaway(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    giveaway_id = int(callback.data.split(":")[1])
    g = await db.get_giveaway_by_id(giveaway_id)
    title = g['title'] if g else str(giveaway_id)
    await db.delete_giveaway(giveaway_id)
    await state.update_data(current_giveaway_id=None)
    await callback.message.edit_text(f"🗑 Конкурс «{title}» удалён.")
    await callback.message.answer("Главное меню:", reply_markup=main_menu_kb())
    await callback.answer()


# ── Участие пользователей ─────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("participate:"))
async def handle_participate(callback: CallbackQuery, bot: Bot):
    giveaway_id = int(callback.data.split(":")[1])
    g = await db.get_giveaway_by_id(giveaway_id)

    if not g or g['status'] != 'active':
        await callback.answer("❌ Этот конкурс уже завершён.", show_alert=True)
        return

    user = callback.from_user
    channels = await db.get_channels(giveaway_id)
    not_subscribed = await check_user_subscriptions(bot, user.id, channels)

    if not_subscribed:
        ch_list = "\n".join(f"• {ch['chat_title']}" for ch in not_subscribed)
        await callback.answer(
            f"❌ Ты ещё не подписан на:\n{ch_list}\n\nПодпишись и попробуй снова.",
            show_alert=True
        )
        return

    username = user.username
    full_name = f"{user.first_name} {user.last_name or ''}".strip()
    added = await db.add_participant(giveaway_id, user.id, username, full_name)

    if added:
        await callback.answer("🎉 Ты зарегистрирован как участник!", show_alert=True)
    else:
        await callback.answer("✅ Ты уже участвуешь в этом конкурсе.", show_alert=True)
