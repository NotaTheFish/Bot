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
    channels_kb, confirm_kb, channel_select_kb
)

logger = logging.getLogger(__name__)
router = Router()

ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

PLACE_EMOJI = {1: "🥇", 2: "🥈", 3: "🥉"}


def place_emoji(n: int) -> str:
    return PLACE_EMOJI.get(n, f"#{n}")


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


# ── FSM States ────────────────────────────────────────────────────────────────

class CreateGiveaway(StatesGroup):
    title = State()
    key = State()
    announcement = State()
    prize_places = State()
    prizes = State()          # collecting prizes one by one
    channels = State()        # collecting channel IDs one by one


class SelectGiveaway(StatesGroup):
    waiting = State()


class PublishGiveaway(StatesGroup):
    channel_id = State()


# ── Helpers ───────────────────────────────────────────────────────────────────

async def check_subscriptions(bot: Bot, user_id: int, channels: list[dict]) -> list[dict]:
    """Returns list of channels the user is NOT subscribed to."""
    not_subscribed = []
    for ch in channels:
        try:
            member = await bot.get_chat_member(ch['chat_id'], user_id)
            if member.status in ('left', 'kicked', 'banned'):
                not_subscribed.append(ch)
        except Exception as e:
            logger.warning(f"Cannot check subscription for chat {ch['chat_id']}: {e}")
            not_subscribed.append(ch)
    return not_subscribed


async def giveaway_info_text(g: dict) -> str:
    prizes = await db.get_prizes(g['id'])
    channels = await db.get_channels(g['id'])
    count = await db.get_participant_count(g['id'])

    status_map = {'active': '🟢 Активен', 'cancelled': '🔴 Отменён', 'finished': '🏁 Завершён'}
    status = status_map.get(g['status'], g['status'])

    prizes_text = "\n".join(
        f"  {place_emoji(p['place'])} {p['place']} место — {p['description']}"
        for p in prizes
    )
    channels_text = "\n".join(f"  • {ch['chat_title']}" for ch in channels) or "  (нет каналов)"

    return (
        f"🎯 <b>{g['title']}</b>\n"
        f"🔑 Ключ: <code>!{g['key']}!</code>\n"
        f"📊 Статус: {status}\n\n"
        f"📣 Объявление:\n{g['announcement']}\n\n"
        f"🏆 Призовые места:\n{prizes_text}\n\n"
        f"📡 Каналы для подписки:\n{channels_text}\n\n"
        f"👥 Участников: <b>{count}</b>"
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
        # All prizes collected → move to channels
        await state.set_state(CreateGiveaway.channels)
        await state.update_data(channels_collected=[])
        await message.answer(
            "✅ Призы добавлены!\n\n"
            "Теперь добавь <b>каналы/чаты</b> для проверки подписки.\n"
            "Перешли любое сообщение из нужного канала, или введи ID чата вручную.\n\n"
            "Важно: бот должен быть добавлен в эти каналы с правами администратора.\n\n"
            "Когда добавишь все каналы — напиши <code>готово</code>."
        )


@router.message(CreateGiveaway.channels)
async def process_channels(message: Message, state: FSMContext, bot: Bot):
    data = await state.get_data()
    channels_collected: list = data.get("channels_collected", [])

    if message.text and message.text.strip().lower() in ("готово", "done"):
        if not channels_collected:
            await message.answer("❌ Добавь хотя бы один канал или чат.")
            return

        # Save everything to DB
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

    # Try to get chat_id from forwarded message or text
    chat_id = None
    chat_title = None
    invite_link = None

    # Case 1: forwarded from channel (standard)
    if message.forward_from_chat:
        chat_id = message.forward_from_chat.id
        chat_title = message.forward_from_chat.title
    # Case 2: sender_chat — channel post forwarded with hidden origin
    elif message.sender_chat:
        chat_id = message.sender_chat.id
        chat_title = message.sender_chat.title
    # Case 3: manual numeric ID
    elif message.text:
        raw = message.text.strip()
        try:
            chat_id = int(raw)
        except ValueError:
            await message.answer(
                "❌ Не понял. Перешли сообщение из канала или введи числовой ID.\n"
                "Когда добавишь все каналы — напиши <code>готово</code>."
            )
            return

    if chat_id is None:
        await message.answer(
            "❌ Не удалось определить чат.\n"
            "Попробуй переслать сообщение или ввести ID вручную."
        )
        return

    # Verify bot is in that chat and get info
    try:
        chat = await bot.get_chat(chat_id)
        chat_title = chat_title or chat.title or str(chat_id)
        try:
            invite_link = chat.invite_link
            if not invite_link:
                invite_link = await bot.export_chat_invite_link(chat_id)
        except Exception:
            numeric = str(chat_id).replace("-100", "").lstrip("-")
            invite_link = f"https://t.me/c/{numeric}"
    except Exception as e:
        from html import escape
        logger.error(f"get_chat({chat_id}) failed: {e}")
        await message.answer(
            f"❌ Не могу получить информацию о чате <code>{chat_id}</code>\n\n"
            f"Причина: <code>{escape(str(e))}</code>\n\n"
            "Убедись что:\n"
            "• Бот добавлен в чат/канал с правами администратора\n"
            "• ID верный (для каналов начинается с <code>-100</code>)"
        )
        return

    # Check for duplicates
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

    target_chat_id = None
    if message.forward_from_chat:
        target_chat_id = message.forward_from_chat.id
    elif message.text:
        try:
            target_chat_id = int(message.text.strip())
        except ValueError:
            await message.answer("❌ Введи числовой ID канала или перешли сообщение.")
            return

    g = await db.get_giveaway_by_id(giveaway_id)
    channels = await db.get_channels(giveaway_id)
    prizes = await db.get_prizes(giveaway_id)

    prizes_text = "\n".join(
        f"{place_emoji(p['place'])} {p['place']} место — {p['description']}"
        for p in prizes
    )

    text = (
        f"{g['announcement']}\n\n"
        f"🏆 <b>Призы:</b>\n{prizes_text}"
    )
    kb = channels_kb(channels, giveaway_id)

    try:
        await bot.send_message(target_chat_id, text, reply_markup=kb)
        await state.set_state(None)
        await message.answer(
            f"✅ Объявление опубликовано в чат <code>{target_chat_id}</code>!",
            reply_markup=giveaway_menu_kb()
        )
    except Exception as e:
        await message.answer(
            f"❌ Не удалось опубликовать. Убедись, что бот — администратор канала.\nОшибка: {e}"
        )


# ── Участники ─────────────────────────────────────────────────────────────────

@router.message(F.text == "👥 Участники")
async def show_participants(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return
    count = await db.get_participant_count(giveaway_id)
    g = await db.get_giveaway_by_id(giveaway_id)
    await message.answer(
        f"👥 Конкурс «{g['title']}»\n"
        f"Участников: <b>{count}</b>"
    )


# ── Завершить конкурс ─────────────────────────────────────────────────────────

@router.message(F.text == "✅ Завершить конкурс")
async def finish_giveaway_confirm(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    giveaway_id = data.get("current_giveaway_id")
    if not giveaway_id:
        await message.answer("❌ Сначала выбери конкурс.")
        return
    g = await db.get_giveaway_by_id(giveaway_id)
    count = await db.get_participant_count(giveaway_id)
    prizes = await db.get_prizes(giveaway_id)

    await message.answer(
        f"❓ Завершить конкурс «{g['title']}»?\n"
        f"Участников: <b>{count}</b>\n"
        f"Призовых мест: <b>{len(prizes)}</b>\n\n"
        "Будут случайно выбраны победители.",
        reply_markup=confirm_kb("finish", giveaway_id)
    )


@router.callback_query(F.data.startswith("confirm_finish:"))
async def do_finish_giveaway(callback: CallbackQuery, state: FSMContext, bot: Bot):
    if not is_admin(callback.from_user.id):
        return
    giveaway_id = int(callback.data.split(":")[1])
    g = await db.get_giveaway_by_id(giveaway_id)
    participants = await db.get_participants(giveaway_id)
    prizes = await db.get_prizes(giveaway_id)

    if not participants:
        await callback.message.edit_text("❌ Нет участников для розыгрыша.")
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
    not_subscribed = await check_subscriptions(bot, user.id, channels)

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