"""Настройки нумерации отзывов в канале (профиль → Нумерация отзывов)."""
import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from db import Database

router = Router()
logger = logging.getLogger(__name__)


def _render_number_html(tpl_html: str, number: int) -> str:
    """Подставляет номер в HTML-шаблон (с тегами <tg-emoji> для премиум-эмодзи).
    Отправляется с parse_mode=HTML."""
    return (tpl_html or "Отзыв №{n}").replace("{n}", str(number))

MODE_LABELS = {
    "off": "🚫 Выключена",
    "auto": "⚡ Авто (сразу после отзыва)",
    "ask": "❓ Спрашивать каждый раз",
}


class NumberingSG(StatesGroup):
    enter_start = State()
    enter_template = State()


def _kb_numbering(ch: dict) -> InlineKeyboardMarkup:
    mode = ch.get("numbering_mode", "off")
    rows = []
    for key, label in MODE_LABELS.items():
        mark = "✅ " if mode == key else ""
        rows.append([InlineKeyboardButton(text=f"{mark}{label}",
                                          callback_data=f"numbering:mode:{key}")])
    rows.append([InlineKeyboardButton(text="🔢 Стартовый номер",
                                      callback_data="numbering:start")])
    rows.append([InlineKeyboardButton(text="✏️ Шаблон текста",
                                      callback_data="numbering:template")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="menu:profile")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _numbering_text(ch: dict) -> str:
    mode = ch.get("numbering_mode", "off")
    start = ch.get("numbering_start", 1)
    tpl = ch.get("numbering_template", "Отзыв №{n}")
    preview = tpl.replace("{n}", str(start))
    return (
        "🔢 <b>Нумерация отзывов в канале</b>\n\n"
        "Бот может нумеровать каждый отзыв, который ты публикуешь в канал кнопкой «Принять».\n\n"
        f"• <b>Режим:</b> {MODE_LABELS.get(mode, mode)}\n"
        f"• <b>Стартовый номер:</b> {start}\n"
        f"• <b>Пример:</b> {preview}\n\n"
        "<i>Номер ставится отдельным сообщением после карточки. Если удалить последний "
        "отзыв, его номер переиспользуется. Удаления из середины оставляют пропуск.</i>\n\n"
        "<i>Премиум-эмодзи в шаблоне сохраняются автоматически — для этого бот "
        "публикует номер через служебный чат, поэтому появляется пометка "
        "«Переслано из…». Без премиум-эмодзи номер публикуется напрямую, без пометки.</i>"
    )


async def _show_menu(message: Message, db: Database, user_id: int, edit=False):
    ch = await db.get_seller_channel(user_id)
    if not ch or not ch["verified"]:
        await message.answer("Сначала подключи и верифицируй канал отзывов.")
        return
    text = _numbering_text(ch)
    kb = _kb_numbering(ch)
    if edit:
        try:
            await message.edit_text(text, reply_markup=kb)
            return
        except Exception:
            pass
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "numbering:menu")
async def cb_numbering_menu(call: CallbackQuery, db: Database):
    await call.answer()
    await _show_menu(call.message, db, call.from_user.id)


@router.callback_query(F.data.startswith("numbering:mode:"))
async def cb_numbering_mode(call: CallbackQuery, db: Database):
    mode = call.data.split(":")[2]
    if mode not in MODE_LABELS:
        await call.answer()
        return
    ch = await db.get_seller_channel(call.from_user.id)
    if not ch or not ch["verified"]:
        await call.answer("Канал не подключён", show_alert=True)
        return
    await db.set_numbering(call.from_user.id, numbering_mode=mode)
    await call.answer("✅ Режим обновлён")
    await _show_menu(call.message, db, call.from_user.id, edit=True)


@router.callback_query(F.data == "numbering:start")
async def cb_numbering_start(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(NumberingSG.enter_start)
    await call.message.answer(
        "🔢 Введи стартовый номер — от него бот начнёт нумеровать <b>новые</b> отзывы.\n\n"
        "Например, если у тебя уже 500 отзывов, введи <code>501</code> — следующий отзыв "
        "получит номер 501.\n\n"
        "Отправь /cancel чтобы отменить."
    )


@router.message(NumberingSG.enter_start, F.text == "/cancel")
async def num_start_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.")


@router.message(NumberingSG.enter_start)
async def num_start_input(message: Message, state: FSMContext, db: Database):
    txt = (message.text or "").strip()
    if not txt.isdigit():
        await message.answer("Нужно целое число, например 501. Попробуй ещё раз или /cancel.")
        return
    val = int(txt)
    if val < 1 or val > 10_000_000:
        await message.answer("Число должно быть от 1 до 10000000.")
        return
    await state.clear()
    await db.set_numbering(message.from_user.id, numbering_start=val)
    await message.answer(f"✅ Стартовый номер: <b>{val}</b>")
    await _show_menu(message, db, message.from_user.id)


@router.callback_query(F.data == "numbering:template")
async def cb_numbering_template(call: CallbackQuery, state: FSMContext):
    await call.answer()
    await state.set_state(NumberingSG.enter_template)
    await call.message.answer(
        "✏️ Пришли свой шаблон текста для номера.\n\n"
        "Используй <code>{n}</code> там, где должен быть номер. Можно добавить "
        "премиум-эмодзи и любое оформление.\n\n"
        "Примеры:\n"
        "• <code>Отзыв №{n}</code>\n"
        "• <code>Клиентская рекомендация №{n}</code>\n"
        "• <code>⭐️ Сделка #{n}</code>\n\n"
        "Отправь /cancel чтобы отменить."
    )


@router.message(NumberingSG.enter_template, F.text == "/cancel")
async def num_tpl_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.")


@router.message(NumberingSG.enter_template)
async def num_tpl_input(message: Message, state: FSMContext, db: Database):
    # Храним html_text — aiogram конвертирует премиум-эмодзи в <tg-emoji emoji-id="...">,
    # который бот может отправить с parse_mode=HTML (у владельца бота есть Premium)
    if not message.text or not message.text.strip():
        await message.answer("Пустой шаблон. Пришли текст или /cancel.")
        return
    if "{n}" not in message.text:
        await message.answer(
            "В шаблоне нет <code>{n}</code> — без него бот не поймёт, куда вставлять номер. "
            "Добавь {n} и пришли ещё раз."
        )
        return
    if len(message.text) > 300:
        await message.answer("Слишком длинный шаблон (макс 300 символов).")
        return
    tpl_html = message.html_text  # с тегами <tg-emoji> для премиум-эмодзи
    await state.clear()
    await db.set_numbering(message.from_user.id, numbering_template=tpl_html)
    # Превью — отправляем как HTML, чтобы увидеть премиум-эмодзи вживую
    preview = tpl_html.replace("{n}", "501")
    await message.answer("✅ Шаблон сохранён. Пример:")
    try:
        await message.answer(preview, parse_mode="HTML")
    except Exception:
        await message.answer(preview.replace("<tg-emoji", "").replace("</tg-emoji>", ""))
    await _show_menu(message, db, message.from_user.id)
