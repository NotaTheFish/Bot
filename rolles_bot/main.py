import asyncio
import logging
import re
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ChatMemberUpdated, MessageEntity, User
)
from aiogram.filters import Command, ChatMemberUpdatedFilter, IS_NOT_MEMBER, MEMBER, ADMINISTRATOR
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from database import db, COOLDOWN_MAX
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=settings.BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# ─────────────────────────────────────────
# Middleware: авто-обновление username
# ─────────────────────────────────────────
@dp.message.outer_middleware()
async def update_username_middleware(handler, message, data):
    """При каждом сообщении в группе обновляет username отправителя в БД."""
    if message.chat.type in ("group", "supergroup") and message.from_user:
        await db.update_member_username(
            message.chat.id,
            message.from_user.id,
            message.from_user.username  # None если нет username — тоже обновляем
        )
    return await handler(message, data)


# ─────────────────────────────────────────
# FSM States
# ─────────────────────────────────────────
class AdminStates(StatesGroup):
    waiting_clan_name_for_rename = State()
    waiting_new_clan_name = State()
    waiting_trigger_name = State()
    waiting_trigger_rename_old = State()
    waiting_new_trigger_name = State()
    waiting_key_chat_select = State()


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────
def main_menu_kb():
    """Стартовое меню — только выбор чата."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Выбрать чат", callback_data="select_chat")],
    ])


def chat_menu_kb():
    """Меню после выбора чата."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Кланы", callback_data="clans_menu")],
        [InlineKeyboardButton(text="🔑 Создать ключ доступа", callback_data="gen_key")],
        [InlineKeyboardButton(text="🔄 Сменить чат", callback_data="select_chat")],
    ])


def chats_kb(chats: list):
    buttons = [[InlineKeyboardButton(text=c["title"], callback_data=f"chat_{c['chat_id']}")] for c in chats]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def clans_kb(clans: list, chat_id: int):
    buttons = []
    for clan in clans:
        triggers = ", ".join(clan["triggers"]) if clan["triggers"] else "нет триггеров"
        buttons.append([InlineKeyboardButton(
            text=f"{clan['name']} [{triggers}]",
            callback_data=f"clan_{chat_id}_{clan['name']}"
        )])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def clan_actions_kb(chat_id: int, clan_name: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Переименовать клан", callback_data=f"rename_clan_{chat_id}_{clan_name}")],
        [InlineKeyboardButton(text="➕ Добавить триггер", callback_data=f"add_trigger_{chat_id}_{clan_name}")],
        [InlineKeyboardButton(text="🔖 Триггеры", callback_data=f"triggers_{chat_id}_{clan_name}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"clans_menu")],
    ])


def triggers_kb(chat_id: int, clan_name: str, triggers: list):
    buttons = [[InlineKeyboardButton(text=t, callback_data=f"trigger__{chat_id}__{clan_name}__{t}")] for t in triggers]
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"clan_{chat_id}_{clan_name}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def trigger_actions_kb(chat_id: int, clan_name: str, trigger: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Переименовать", callback_data=f"rename_trigger__{chat_id}__{clan_name}__{trigger}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del_trigger__{chat_id}__{clan_name}__{trigger}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"triggers_{chat_id}_{clan_name}")],
    ])


async def is_admin(user_id: int) -> bool:
    return user_id == settings.ADMIN_ID


async def is_chat_bot_admin(chat_id: int, user_id: int) -> bool:
    """Главный админ ИЛИ бот-админ конкретного чата."""
    if user_id == settings.ADMIN_ID:
        return True
    return await db.is_bot_admin(chat_id, user_id)


CHUNK_SIZE = 90  # максимум entities в одном сообщении (с запасом от лимита 100)


def build_mention_chunks(members: list[dict], header: str) -> list[tuple[str, list[MessageEntity]]]:
    """
    Разбивает участников на чанки по CHUNK_SIZE и строит сообщения с entities.
    Возвращает список (text, entities) — каждый элемент = одно сообщение.
    """
    chunks = [members[i:i + CHUNK_SIZE] for i in range(0, len(members), CHUNK_SIZE)]
    result = []
    for i, chunk in enumerate(chunks):
        prefix = header if i == 0 else ""
        parts = []
        entities = []
        offset = len(prefix)
        for m in chunk:
            display = f"@{m['username']}" if m.get("username") else "участник"
            entities.append(MessageEntity(
                type="text_mention",
                offset=offset,
                length=len(display),
                user=User(id=m["user_id"], is_bot=False, first_name=display)
            ))
            parts.append(display)
            offset += len(display) + 1
        text = prefix + " ".join(parts)
        result.append((text, entities))
    return result


async def get_active_chat_id(user_id: int, state: FSMContext) -> int | None:
    data = await state.get_data()
    return data.get("active_chat_id")


# ─────────────────────────────────────────
# Bot added to chat / /ShimmPandze command
# ─────────────────────────────────────────
@dp.message(Command("ShimmPandze"))
async def register_chat(message: Message):
    chat_id = message.chat.id
    chat_title = message.chat.title or str(chat_id)

    member = await bot.get_chat_member(chat_id, message.from_user.id)
    if member.status not in ("administrator", "creator"):
        return

    await db.register_chat(chat_id, chat_title)
    await message.reply("✅ Чат зарегистрирован для ShimmBot!")


# ─────────────────────────────────────────
# /start — только для админа в личке
# ─────────────────────────────────────────
@dp.message(Command("start"), F.chat.type == "private")
async def cmd_start(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return await message.answer("⛔ Доступ запрещён.")
    await state.clear()
    await message.answer("👋 Привет, админ! Выбери действие:", reply_markup=main_menu_kb())


# ─────────────────────────────────────────
# Callback: главное меню
# ─────────────────────────────────────────
@dp.callback_query(F.data == "main_menu")
async def cb_main_menu(call: CallbackQuery, state: FSMContext):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    data = await state.get_data()
    chat_id = data.get("active_chat_id")
    await state.set_state(None)
    if chat_id:
        chat = await db.get_chat(chat_id)
        title = chat["title"] if chat else str(chat_id)
        await call.message.edit_text(f"✅ Активный чат: *{title}*\n\nВыбери действие:", parse_mode="Markdown", reply_markup=chat_menu_kb())
    else:
        await call.message.edit_text("👋 Выбери чат:", reply_markup=main_menu_kb())


# ─────────────────────────────────────────
# Callback: выбор чата
# ─────────────────────────────────────────
@dp.callback_query(F.data == "select_chat")
async def cb_select_chat(call: CallbackQuery):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    chats = await db.get_chats()
    if not chats:
        return await call.answer("Нет зарегистрированных чатов.", show_alert=True)
    await call.message.edit_text("📋 Выбери чат:", reply_markup=chats_kb(chats))


@dp.callback_query(F.data.startswith("chat_"))
async def cb_chat_selected(call: CallbackQuery, state: FSMContext):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    chat_id = int(call.data.split("_", 1)[1])
    await state.update_data(active_chat_id=chat_id)
    chat = await db.get_chat(chat_id)
    title = chat["title"] if chat else str(chat_id)
    await call.message.edit_text(f"✅ Активный чат: *{title}*\n\nВыбери действие:", parse_mode="Markdown", reply_markup=chat_menu_kb())


# ─────────────────────────────────────────
# Callback: кланы
# ─────────────────────────────────────────
@dp.callback_query(F.data == "clans_menu")
async def cb_clans_menu(call: CallbackQuery, state: FSMContext):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    chat_id = await get_active_chat_id(call.from_user.id, state)
    if not chat_id:
        return await call.answer("Сначала выбери чат!", show_alert=True)
    clans = await db.get_clans(chat_id)
    if not clans:
        return await call.message.edit_text("В этом чате нет кланов.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")]
        ]))
    await call.message.edit_text("👥 Кланы:", reply_markup=clans_kb(clans, chat_id))


@dp.callback_query(F.data.regexp(r"^clan_(-?\d+)_(.+)$"))
async def cb_clan_selected(call: CallbackQuery):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    m = re.match(r"^clan_(-?\d+)_(.+)$", call.data)
    chat_id, clan_name = int(m.group(1)), m.group(2)
    members_count = await db.get_clan_member_count(chat_id, clan_name)
    await call.message.edit_text(
        f"👥 Клан: *{clan_name}*\nУчастников: {members_count}",
        parse_mode="Markdown",
        reply_markup=clan_actions_kb(chat_id, clan_name)
    )


# ─────────────────────────────────────────
# Переименовать клан
# ─────────────────────────────────────────
@dp.callback_query(F.data.startswith("rename_clan_"))
async def cb_rename_clan(call: CallbackQuery, state: FSMContext):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    parts = call.data.split("_", 3)  # rename_clan_{chat_id}_{clan_name}
    chat_id, clan_name = int(parts[2]), parts[3]
    await state.set_state(AdminStates.waiting_new_clan_name)
    await state.update_data(rename_chat_id=chat_id, rename_clan_old=clan_name)
    await call.message.edit_text(f"✏️ Введи новое название для клана *{clan_name}*:", parse_mode="Markdown")


@dp.message(AdminStates.waiting_new_clan_name, F.chat.type == "private")
async def process_new_clan_name(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data["rename_chat_id"]
    old_name = data["rename_clan_old"]
    new_name = message.text.strip()
    await db.rename_clan(chat_id, old_name, new_name)
    await state.clear()
    await state.update_data(active_chat_id=chat_id)
    await message.answer(f"✅ Клан переименован: *{old_name}<b> → </b>{new_name}*", parse_mode="HTML", reply_markup=chat_menu_kb())


# ─────────────────────────────────────────
# Добавить триггер
# ─────────────────────────────────────────
@dp.callback_query(F.data.startswith("add_trigger_"))
async def cb_add_trigger(call: CallbackQuery, state: FSMContext):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    parts = call.data.split("_", 3)  # add_trigger_{chat_id}_{clan_name}
    chat_id, clan_name = int(parts[2]), parts[3]
    await state.set_state(AdminStates.waiting_trigger_name)
    await state.update_data(trigger_chat_id=chat_id, trigger_clan=clan_name)
    await call.message.edit_text(f"➕ Введи название триггера для клана *{clan_name}*:", parse_mode="Markdown")


@dp.message(AdminStates.waiting_trigger_name, F.chat.type == "private")
async def process_trigger_name(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id = data["trigger_chat_id"]
    clan_name = data["trigger_clan"]
    trigger = message.text.strip().lower()
    await db.add_trigger(chat_id, clan_name, trigger)
    await state.clear()
    await state.update_data(active_chat_id=chat_id)
    await message.answer(f"✅ Триггер *{trigger}<b> добавлен в клан </b>{clan_name}*", parse_mode="HTML", reply_markup=chat_menu_kb())


# ─────────────────────────────────────────
# Список триггеров
# ─────────────────────────────────────────
@dp.callback_query(F.data.regexp(r"^triggers_(-?\d+)_(.+)$"))
async def cb_triggers_list(call: CallbackQuery):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    m = re.match(r"^triggers_(-?\d+)_(.+)$", call.data)
    chat_id, clan_name = int(m.group(1)), m.group(2)
    clan = await db.get_clan(chat_id, clan_name)
    if not clan or not clan["triggers"]:
        return await call.answer("У этого клана нет триггеров.", show_alert=True)
    await call.message.edit_text(f"🔖 Триггеры клана *{clan_name}*:", parse_mode="Markdown",
                                  reply_markup=triggers_kb(chat_id, clan_name, clan["triggers"]))


@dp.callback_query(F.data.regexp(r"^trigger__(-?\d+)__(.+)__(.+)$"))
async def cb_trigger_selected(call: CallbackQuery):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    m = re.match(r"^trigger__(-?\d+)__(.+)__(.+)$", call.data)
    chat_id, clan_name, trigger = int(m.group(1)), m.group(2), m.group(3)
    await call.message.edit_text(f"🔖 Триггер: *{trigger}*\nКлан: {clan_name}", parse_mode="Markdown",
                                  reply_markup=trigger_actions_kb(chat_id, clan_name, trigger))


# ─────────────────────────────────────────
# Удалить триггер
# ─────────────────────────────────────────
@dp.callback_query(F.data.regexp(r"^del_trigger__(-?\d+)__(.+)__(.+)$"))
async def cb_del_trigger(call: CallbackQuery):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    m = re.match(r"^del_trigger__(-?\d+)__(.+)__(.+)$", call.data)
    chat_id, clan_name, trigger = int(m.group(1)), m.group(2), m.group(3)
    await db.remove_trigger(chat_id, clan_name, trigger)
    await call.answer(f"🗑 Триггер {trigger} удалён.", show_alert=True)
    clan = await db.get_clan(chat_id, clan_name)
    if not clan or not clan["triggers"]:
        await call.message.edit_text(f"Клан *{clan_name}* — триггеров нет.", parse_mode="Markdown",
                                      reply_markup=clan_actions_kb(chat_id, clan_name))
    else:
        await call.message.edit_text(f"🔖 Триггеры клана *{clan_name}*:", parse_mode="Markdown",
                                      reply_markup=triggers_kb(chat_id, clan_name, clan["triggers"]))


# ─────────────────────────────────────────
# Переименовать триггер
# ─────────────────────────────────────────
@dp.callback_query(F.data.regexp(r"^rename_trigger__(-?\d+)__(.+)__(.+)$"))
async def cb_rename_trigger(call: CallbackQuery, state: FSMContext):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    m = re.match(r"^rename_trigger__(-?\d+)__(.+)__(.+)$", call.data)
    chat_id, clan_name, trigger = int(m.group(1)), m.group(2), m.group(3)
    await state.set_state(AdminStates.waiting_new_trigger_name)
    await state.update_data(rt_chat_id=chat_id, rt_clan=clan_name, rt_old=trigger)
    await call.message.edit_text(f"✏️ Введи новое название для триггера *{trigger}*:", parse_mode="Markdown")


@dp.message(AdminStates.waiting_new_trigger_name, F.chat.type == "private")
async def process_new_trigger_name(message: Message, state: FSMContext):
    data = await state.get_data()
    chat_id, clan_name, old_trigger = data["rt_chat_id"], data["rt_clan"], data["rt_old"]
    new_trigger = message.text.strip().lower()
    await db.rename_trigger(chat_id, clan_name, old_trigger, new_trigger)
    await state.clear()
    await state.update_data(active_chat_id=chat_id)
    await message.answer(f"✅ Триггер переименован: *{old_trigger}<b> → </b>{new_trigger}*", parse_mode="HTML", reply_markup=chat_menu_kb())


# ─────────────────────────────────────────────────────────────
# ЧАТОВЫЕ КОМАНДЫ
# ─────────────────────────────────────────────────────────────

async def user_is_chat_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except Exception:
        return False


@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"(?i)^\.шк\s+.+"))
async def chat_clan_command(message: Message):
    if not await user_is_chat_admin(message.chat.id, message.from_user.id):
        return

    chat_id = message.chat.id
    text = message.text.strip()
    # убираем .шк
    args = text[len(".шк"):].strip()

    # Удалить клан: .шк -название
    if args.startswith("-") and " " not in args:
        clan_name = args[1:].strip()
        deleted = await db.delete_clan(chat_id, clan_name)
        if deleted:
            await message.reply(f"🗑 Клан *{clan_name}* удалён.", parse_mode="HTML")
        else:
            await message.reply(f"❌ Клан *{clan_name}* не найден.", parse_mode="HTML")
        return

    # Переименовать клан: .шк !старое новое
    if args.startswith("!"):
        parts = args[1:].split(None, 1)
        if len(parts) == 2:
            old_name, new_name = parts
            ok = await db.rename_clan(chat_id, old_name.strip(), new_name.strip())
            if ok:
                await message.reply(f"✏️ Клан *{old_name}<b> → </b>{new_name}*", parse_mode="HTML")
            else:
                await message.reply(f"❌ Клан *{old_name}* не найден.", parse_mode="HTML")
        return

    # Остальные команды: первый токен = название клана
    parts = args.split(None, 1)
    clan_name = parts[0]
    rest = parts[1].strip() if len(parts) > 1 else ""

    if not rest:
        # Создать клан без триггеров (редкий кейс)
        result = await db.create_clan(chat_id, clan_name, [])
        if result is None:
            await message.reply(f"❌ Клан *{clan_name}* уже существует.", parse_mode="HTML")
        else:
            await message.reply(f"✅ Клан *{clan_name}* создан.", parse_mode="HTML")
        return

    # Удалить триггер: .шк клан -триггер
    if rest.startswith("-"):
        trigger = rest[1:].strip().lower()
        ok = await db.remove_trigger(chat_id, clan_name, trigger)
        if ok:
            await message.reply(f"🗑 Триггер *{trigger}<b> удалён из клана </b>{clan_name}*.", parse_mode="HTML")
        else:
            await message.reply(f"❌ Триггер не найден.", parse_mode="HTML")
        return

    # Добавить триггер: .шк клан +триггер
    if rest.startswith("+"):
        trigger = rest[1:].strip().lower()
        await db.add_trigger(chat_id, clan_name, trigger)
        await message.reply(f"➕ Триггер *{trigger}<b> добавлен в клан </b>{clan_name}*.", parse_mode="HTML")
        return

    # Переименовать триггер: .шк клан !старый новый
    if rest.startswith("!"):
        t_parts = rest[1:].split(None, 1)
        if len(t_parts) == 2:
            old_t, new_t = t_parts[0].strip().lower(), t_parts[1].strip().lower()
            ok = await db.rename_trigger(chat_id, clan_name, old_t, new_t)
            if ok:
                await message.reply(f"✏️ Триггер *{old_t}<b> → </b>{new_t}*", parse_mode="HTML")
            else:
                await message.reply(f"❌ Триггер *{old_t}* не найден.", parse_mode="HTML")
        return

    # Создать клан с триггерами: .шк клан триггер1, триггер2
    triggers = [t.strip().lower() for t in rest.split(",") if t.strip()]
    result = await db.create_clan(chat_id, clan_name, triggers)
    if result is None:
        await message.reply(f"❌ Клан *{clan_name}* уже существует.", parse_mode="HTML")
    else:
        t_str = ", ".join(triggers)
        await message.reply(f"✅ Клан *{clan_name}* создан. Триггеры: {t_str}", parse_mode="HTML")


# ─────────────────────────────────────────
# Вступить / выйти из клана
# ─────────────────────────────────────────
@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"(?i)^[+\-]шк\s+.+"))
async def join_leave_clan(message: Message):
    text = message.text.strip()
    action = text[0]  # + или -
    clan_name = text[3:].strip()  # убираем +шк / -шк
    chat_id = message.chat.id
    user_id = message.from_user.id
    real_username = message.from_user.username  # только настоящий @ник или None
    display_name = f"@{real_username}" if real_username else message.from_user.full_name

    clan = await db.get_clan(chat_id, clan_name)
    if not clan:
        await message.reply(f"❌ Клан <b>{clan_name}</b> не найден.", parse_mode="HTML")
        return

    if action == "+":
        added = await db.add_member(chat_id, clan_name, user_id, real_username)
        if added:
            await message.reply(f"✅ {display_name} вступил в клан <b>{clan_name}</b>!", parse_mode="HTML")
        else:
            await message.reply(f"ℹ️ Ты уже в клане <b>{clan_name}</b>.", parse_mode="HTML")
    else:
        removed = await db.remove_member(chat_id, clan_name, user_id)
        if removed:
            await message.reply(f"👋 {display_name} покинул клан <b>{clan_name}</b>.", parse_mode="HTML")
        else:
            await message.reply(f"ℹ️ Ты не состоишь в клане <b>{clan_name}</b>.", parse_mode="HTML")


# ─────────────────────────────────────────
# Список кланов: !шк
# ─────────────────────────────────────────
@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"(?i)^!шк$"))
async def list_clans(message: Message):
    chat_id = message.chat.id
    clans = await db.get_clans(chat_id)
    if not clans:
        await message.reply("В этом чате пока нет кланов.")
        return

    lines = ["📋 <b>Кланы этого чата:</b>\n"]
    for clan in clans:
        triggers = ", ".join(f"<code>.{t}</code>" for t in clan["triggers"]) if clan["triggers"] else "<i>триггеров нет</i>"
        count = await db.get_clan_member_count(chat_id, clan["name"])
        lines.append(f"👥 <b>{clan['name']}</b> ({count} уч.) — {triggers}")
    await message.reply("\n".join(lines), parse_mode="HTML")


# ─────────────────────────────────────────
# Созыв клана по триггеру: .триггер
# ─────────────────────────────────────────
@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"(?i)^\.[а-яёa-z0-9_-]+$"))
async def trigger_summon(message: Message):
    text = message.text.strip()
    trigger = text[1:].lower()
    chat_id = message.chat.id
    user_id = message.from_user.id

    clans = await db.get_clans_by_trigger(chat_id, trigger)
    if not clans:
        return  # не наш триггер — молчим

    # Кулдаун — только для не-админов
    if not await is_chat_bot_admin(chat_id, user_id):
        allowed, remaining = await db.check_cooldown(chat_id, user_id)
        if not allowed:
            mins = remaining // 60
            secs = remaining % 60
            await message.reply(
                f"⏳ Ты уже созывал {COOLDOWN_MAX} раза за 10 минут. "
                f"Подожди ещё {mins}м {secs}с.",
                parse_mode="HTML"
            )
            return

    # Собираем участников из всех кланов, дедуплицируем по user_id
    seen_ids: set[int] = set()
    clan_members: dict[str, list] = {}

    for clan in clans:
        members = await db.get_clan_members(chat_id, clan["name"])
        unique = [m for m in members if m["user_id"] not in seen_ids]
        if unique:
            clan_members[clan["name"]] = unique
            seen_ids.update(m["user_id"] for m in unique)

    if not clan_members:
        await message.reply("👥 Кланы пусты.", parse_mode="HTML")
        return

    caller = f"@{message.from_user.username}" if message.from_user.username else message.from_user.full_name
    clan_labels = ", ".join(clan_members.keys())
    all_members = [m for members in clan_members.values() for m in members]
    total = len(all_members)

    header = f"📣 Сбор [{clan_labels}]! (позвал {caller})\n"
    chunks = build_mention_chunks(all_members, header)

    first = True
    for chunk_text, chunk_entities in chunks:
        if first:
            sent = await message.reply(chunk_text, entities=chunk_entities)
            first = False
        else:
            await message.answer(chunk_text, entities=chunk_entities)

    # Счётчик отправленных
    if len(chunks) > 1:
        await message.answer(f"📨 Отправлено: {total} участников ({len(chunks)} сообщения)")
    else:
        # Дописываем счётчик к первому сообщению через edit
        try:
            new_text = chunks[0][0] + f"\n📨 {total} уч."
            await sent.edit_text(new_text, entities=chunks[0][1])
        except Exception:
            pass


# ─────────────────────────────────────────
# Созыв всех: !все (только бот-админы)
# ─────────────────────────────────────────
@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"(?i)^!все$"))
async def summon_all(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id

    if not await is_chat_bot_admin(chat_id, user_id):
        return

    members = await db.get_all_chat_members_tg(chat_id)
    caller = f"@{message.from_user.username}" if message.from_user.username else message.from_user.full_name

    if not members:
        await message.reply("👥 В базе нет участников кланов.")
        return

    header = f"📣 Всеобщий сбор! (позвал {caller})\n"
    chunks = build_mention_chunks(members, header)
    total = len(members)

    first = True
    for chunk_text, chunk_entities in chunks:
        if first:
            sent = await message.reply(chunk_text, entities=chunk_entities)
            first = False
        else:
            await message.answer(chunk_text, entities=chunk_entities)

    if len(chunks) > 1:
        await message.answer(f"📨 Отправлено: {total} участников ({len(chunks)} сообщения)")
    else:
        try:
            new_text = chunks[0][0] + f"\n📨 {total} уч."
            await sent.edit_text(new_text, entities=chunks[0][1])
        except Exception:
            pass


# ─────────────────────────────────────────
# Чистка призраков: !чистка (бот-админы)
# ─────────────────────────────────────────
@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"(?i)^!чистка$"))
async def cleanup_ghosts(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id

    if not await is_chat_bot_admin(chat_id, user_id):
        return

    status_msg = await message.reply("🔍 Проверяю участников кланов...")

    all_members = await db.get_all_chat_members_tg(chat_id)
    removed = 0

    for m in all_members:
        try:
            member = await bot.get_chat_member(chat_id, m["user_id"])
            if member.status in ("left", "kicked"):
                count = await db.remove_member_from_all_clans(chat_id, m["user_id"])
                removed += count
        except Exception:
            # Пользователь не найден — тоже удаляем
            count = await db.remove_member_from_all_clans(chat_id, m["user_id"])
            removed += count

    await status_msg.edit_text(
        f"✅ Чистка завершена.\n"
        f"Удалено записей: <b>{removed}</b>",
        parse_mode="HTML"
    )


# ─────────────────────────────────────────
# Активация ключа в чате: !XXXXXXXX
# ─────────────────────────────────────────
@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"(?i)^![A-F0-9]{8}$"))
async def activate_key(message: Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    key = message.text.strip()[1:].upper()

    used = await db.use_key(key, chat_id)
    if not used:
        return  # неверный ключ — молчим

    await db.add_bot_admin(chat_id, user_id)
    username = message.from_user.username or message.from_user.full_name
    await message.reply(f"✅ @{username} получил права бот-админа в этом чате!", parse_mode="HTML")


# ─────────────────────────────────────────
# Генерация ключа в личке (главный админ)
# ─────────────────────────────────────────
@dp.callback_query(F.data == "gen_key")
async def cb_gen_key(call: CallbackQuery, state: FSMContext):
    if not await is_admin(call.from_user.id):
        return await call.answer("⛔", show_alert=True)
    data = await state.get_data()
    chat_id = data.get("active_chat_id")
    if not chat_id:
        return await call.answer("Сначала выбери чат!", show_alert=True)

    key = await db.create_key(chat_id)
    chat = await db.get_chat(chat_id)
    title = chat["title"] if chat else str(chat_id)

    await call.message.edit_text(
        f"🔑 Ключ для чата <b>{title}</b>:\n\n"
        f"<code>!{key}</code>\n\n"
        f"Одноразовый. Отправь его нужному человеку — он введёт это в чат и получит права бот-админа.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")]
        ])
    )


# ─────────────────────────────────────────
# Запуск
# ─────────────────────────────────────────
async def main():
    await db.init()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
