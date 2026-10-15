"""Персональный чёрный список продавца: блок пары продавец↔покупатель.
Заблокированный покупатель не может оставлять отзывы ЭТОМУ продавцу,
но другим продавцам — может."""
import logging
import html as _h
from aiogram import Router, F
from aiogram.types import (Message, CallbackQuery, InlineKeyboardMarkup,
                           InlineKeyboardButton)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db import Database

router = Router()
logger = logging.getLogger(__name__)


class BlacklistSG(StatesGroup):
    enter_user = State()


def _kb_blacklist(entries: list) -> InlineKeyboardMarkup:
    """Список забаненных с кнопками удаления + добавление вручную."""
    rows = []
    for e in entries[:20]:  # не раздуваем клавиатуру
        name = e.get("buyer_username") and f"@{e['buyer_username']}" \
            or e.get("buyer_name") or f"ID {e['buyer_id']}"
        rows.append([InlineKeyboardButton(
            text=f"❌ {name[:40]}",
            callback_data=f"bl:del:{e['buyer_id']}")])
    rows.append([InlineKeyboardButton(text="➕ Добавить по ID/@username",
                                      callback_data="bl:add")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="menu:profile")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _show_blacklist(message: Message, db: Database, seller_id: int, edit=False):
    entries = await db.list_blacklist(seller_id)
    if entries:
        lines = []
        for e in entries[:20]:
            name = e.get("buyer_username") and f"@{e['buyer_username']}" \
                or e.get("buyer_name") or f"ID {e['buyer_id']}"
            lines.append(f"• {_h.escape(name)}")
        body = "\n".join(lines)
        more = f"\n\n<i>…и ещё {len(entries) - 20}</i>" if len(entries) > 20 else ""
        text = (f"🚫 <b>Чёрный список</b> ({len(entries)})\n\n"
                f"Эти покупатели не могут оставлять тебе отзывы "
                f"(другим продавцам — могут):\n\n{body}{more}\n\n"
                "<i>Жми на пользователя, чтобы убрать из списка.</i>")
    else:
        text = ("🚫 <b>Чёрный список</b> пуст.\n\n"
                "Сюда можно занести надоедливого тролля, который спамит "
                "мусорными отзывами — он больше не сможет писать отзывы тебе "
                "(но другим продавцам — сможет).\n\n"
                "Добавить можно кнопкой ниже, или прямо под карточкой "
                "нежелательного отзыва в ЛС.")
    kb = _kb_blacklist(entries)
    if edit:
        try:
            await message.edit_text(text, reply_markup=kb)
            return
        except Exception:
            pass
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "blacklist:menu")
async def cb_blacklist_menu(call: CallbackQuery, db: Database):
    seller = await db.get_seller(call.from_user.id)
    if not seller:
        await call.answer("Сначала настрой шаблон", show_alert=True)
        return
    await call.answer()
    await _show_blacklist(call.message, db, call.from_user.id)


@router.callback_query(F.data.startswith("bl:del:"))
async def cb_blacklist_del(call: CallbackQuery, db: Database):
    buyer_id = int(call.data.split(":")[2])
    await db.remove_from_blacklist(call.from_user.id, buyer_id)
    await call.answer("✅ Убран из чёрного списка")
    await _show_blacklist(call.message, db, call.from_user.id, edit=True)


@router.callback_query(F.data == "bl:add")
async def cb_blacklist_add(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(BlacklistSG.enter_user)
    await call.message.answer(
        "➕ Пришли <b>ID</b> или <b>@username</b> покупателя, которого хочешь "
        "занести в чёрный список.\n\n"
        "<i>@username найдётся, только если этот человек уже пользовался ботом.</i>\n\n"
        "Отправь /cancel чтобы отменить."
    )


@router.message(BlacklistSG.enter_user, F.text == "/cancel")
async def bl_add_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.")


@router.message(BlacklistSG.enter_user, F.text)
async def bl_add_input(message: Message, state: FSMContext, db: Database):
    raw = message.text.strip()
    buyer_id = None
    buyer_username = None
    buyer_name = None

    if raw.lstrip("-").isdigit():
        buyer_id = int(raw)
    else:
        # Пробуем как @username
        found = await db.find_buyer_by_username(raw)
        if found:
            buyer_id = found["id"]
            buyer_username = found["username"]
        else:
            await message.answer(
                "⚠️ Не нашёл такого пользователя. Он должен был хоть раз "
                "пользоваться ботом.\n\n"
                "Пришли числовой ID, @username или /cancel."
            )
            return

    if buyer_id == message.from_user.id:
        await message.answer("🤔 Себя в чёрный список добавить нельзя.")
        return

    await state.clear()
    is_new = await db.add_to_blacklist(message.from_user.id, buyer_id,
                                       buyer_username=buyer_username, buyer_name=buyer_name)
    who = f"@{buyer_username}" if buyer_username else f"ID {buyer_id}"
    if is_new:
        await message.answer(f"🚫 <b>{_h.escape(who)}</b> добавлен в чёрный список. "
                             "Он больше не сможет оставлять тебе отзывы.")
    else:
        await message.answer(f"ℹ️ <b>{_h.escape(who)}</b> уже был в чёрном списке.")
    await _show_blacklist(message, db, message.from_user.id)


# ── Кнопка «🚫 В ЧС» под карточкой отзыва в ЛС продавца ─────────────────────

@router.callback_query(F.data.startswith("blfast:"))
async def cb_blacklist_fast(call: CallbackQuery, db: Database):
    """Быстрое добавление в ЧС одним нажатием под отзывом."""
    buyer_id = int(call.data.split(":")[1])
    if buyer_id == call.from_user.id:
        await call.answer("Себя нельзя", show_alert=True)
        return
    is_new = await db.add_to_blacklist(call.from_user.id, buyer_id)
    if is_new:
        await call.answer("🚫 Покупатель в чёрном списке — больше не напишет тебе отзывы",
                          show_alert=True)
    else:
        await call.answer("Этот покупатель уже в чёрном списке")
    # Убираем кнопку ЧС из-под карточки, чтобы не жали повторно
    try:
        kb = call.message.reply_markup
        if kb:
            new_rows = [[b for b in row if not (b.callback_data or "").startswith("blfast:")]
                        for row in kb.inline_keyboard]
            new_rows = [r for r in new_rows if r]
            await call.message.edit_reply_markup(
                reply_markup=InlineKeyboardMarkup(inline_keyboard=new_rows) if new_rows else None)
    except Exception:
        pass
