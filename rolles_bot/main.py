import asyncio
import logging
import re
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ChatMemberUpdated
)
from aiogram.filters import Command, ChatMemberUpdatedFilter, IS_NOT_MEMBER, MEMBER, ADMINISTRATOR
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from database import db
from config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=settings.BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


# ─────────────────────────────────────────
# FSM States
# ─────────────────────────────────────────
class AdminStates(StatesGroup):
    waiting_clan_name_for_rename = State()
    waiting_new_clan_name = State()
    waiting_trigger_name = State()
    waiting_trigger_rename_old = State()
    waiting_new_trigger_name = State()


# ─────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────
def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Выбрать чат", callback_data="select_chat")],
        [InlineKeyboardButton(text="👥 Кланы", callback_data="clans_menu")],
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
    await state.clear()
    await call.message.edit_text("👋 Главное меню:", reply_markup=main_menu_kb())


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
    await call.message.edit_text(f"✅ Активный чат: *{title}*\n\nВыбери действие:", parse_mode="Markdown", reply_markup=main_menu_kb())


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
    await message.answer(f"✅ Клан переименован: *{old_name}* → *{new_name}*", parse_mode="Markdown", reply_markup=main_menu_kb())


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
    await message.answer(f"✅ Триггер *{trigger}* добавлен в клан *{clan_name}*", parse_mode="Markdown", reply_markup=main_menu_kb())


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
    await message.answer(f"✅ Триггер переименован: *{old_trigger}* → *{new_trigger}*", parse_mode="Markdown", reply_markup=main_menu_kb())


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
            await message.reply(f"🗑 Клан *{clan_name}* удалён.", parse_mode="Markdown")
        else:
            await message.reply(f"❌ Клан *{clan_name}* не найден.", parse_mode="Markdown")
        return

    # Переименовать клан: .шк !старое новое
    if args.startswith("!"):
        parts = args[1:].split(None, 1)
        if len(parts) == 2:
            old_name, new_name = parts
            ok = await db.rename_clan(chat_id, old_name.strip(), new_name.strip())
            if ok:
                await message.reply(f"✏️ Клан *{old_name}* → *{new_name}*", parse_mode="Markdown")
            else:
                await message.reply(f"❌ Клан *{old_name}* не найден.", parse_mode="Markdown")
        return

    # Остальные команды: первый токен = название клана
    parts = args.split(None, 1)
    clan_name = parts[0]
    rest = parts[1].strip() if len(parts) > 1 else ""

    if not rest:
        # Создать клан без триггеров (редкий кейс)
        await db.create_clan(chat_id, clan_name, [])
        await message.reply(f"✅ Клан *{clan_name}* создан.", parse_mode="Markdown")
        return

    # Удалить триггер: .шк клан -триггер
    if rest.startswith("-"):
        trigger = rest[1:].strip().lower()
        ok = await db.remove_trigger(chat_id, clan_name, trigger)
        if ok:
            await message.reply(f"🗑 Триггер *{trigger}* удалён из клана *{clan_name}*.", parse_mode="Markdown")
        else:
            await message.reply(f"❌ Триггер не найден.", parse_mode="Markdown")
        return

    # Добавить триггер: .шк клан +триггер
    if rest.startswith("+"):
        trigger = rest[1:].strip().lower()
        await db.add_trigger(chat_id, clan_name, trigger)
        await message.reply(f"➕ Триггер *{trigger}* добавлен в клан *{clan_name}*.", parse_mode="Markdown")
        return

    # Переименовать триггер: .шк клан !старый новый
    if rest.startswith("!"):
        t_parts = rest[1:].split(None, 1)
        if len(t_parts) == 2:
            old_t, new_t = t_parts[0].strip().lower(), t_parts[1].strip().lower()
            ok = await db.rename_trigger(chat_id, clan_name, old_t, new_t)
            if ok:
                await message.reply(f"✏️ Триггер *{old_t}* → *{new_t}*", parse_mode="Markdown")
            else:
                await message.reply(f"❌ Триггер *{old_t}* не найден.", parse_mode="Markdown")
        return

    # Создать клан с триггерами: .шк клан триггер1, триггер2
    triggers = [t.strip().lower() for t in rest.split(",") if t.strip()]
    await db.create_clan(chat_id, clan_name, triggers)
    t_str = ", ".join(triggers)
    await message.reply(f"✅ Клан *{clan_name}* создан. Триггеры: {t_str}", parse_mode="Markdown")


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
    username = message.from_user.username or message.from_user.full_name

    clan = await db.get_clan(chat_id, clan_name)
    if not clan:
        await message.reply(f"❌ Клан *{clan_name}* не найден.", parse_mode="Markdown")
        return

    if action == "+":
        added = await db.add_member(chat_id, clan_name, user_id, username)
        if added:
            await message.reply(f"✅ @{username} вступил в клан *{clan_name}*!", parse_mode="Markdown")
        else:
            await message.reply(f"ℹ️ Ты уже в клане *{clan_name}*.", parse_mode="Markdown")
    else:
        removed = await db.remove_member(chat_id, clan_name, user_id)
        if removed:
            await message.reply(f"👋 @{username} покинул клан *{clan_name}*.", parse_mode="Markdown")
        else:
            await message.reply(f"ℹ️ Ты не состоишь в клане *{clan_name}*.", parse_mode="Markdown")


# ─────────────────────────────────────────
# Созыв клана по триггеру: .триггер
# ─────────────────────────────────────────
@dp.message(F.chat.type.in_({"group", "supergroup"}), F.text.regexp(r"(?i)^\.[а-яёa-z0-9_-]+$"))
async def trigger_summon(message: Message):
    text = message.text.strip()
    trigger = text[1:].lower()
    chat_id = message.chat.id

    clan = await db.get_clan_by_trigger(chat_id, trigger)
    if not clan:
        return  # не наш триггер — молчим

    members = await db.get_clan_members(chat_id, clan["name"])
    if not members:
        await message.reply(f"👥 Клан *{clan['name']}* пуст.", parse_mode="Markdown")
        return

    mentions = " ".join(
        f"@{m['username']}" if m.get("username") else f"[id{m['user_id']}](tg://user?id={m['user_id']})"
        for m in members
    )
    caller = message.from_user.username or message.from_user.full_name
    await message.reply(
        f"📣 *{clan['name']}*, сбор! (позвал @{caller})\n{mentions}",
        parse_mode="Markdown"
    )


# ─────────────────────────────────────────
# Запуск
# ─────────────────────────────────────────
async def main():
    await db.init()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())