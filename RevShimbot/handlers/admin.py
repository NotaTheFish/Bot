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


# ── Клавиатуры ─────────────────────────────────────────────────────────────

def kb_admin_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton(text="📣 Управление рекламой", callback_data="admin:ads")],
        [InlineKeyboardButton(text="« Назад", callback_data="menu:back")],
    ])


def kb_admin_ads() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Рассылка всем", callback_data="admin:ad_broadcast")],
        [InlineKeyboardButton(text="📌 Закреп (объявление)", callback_data="admin:ad_pin")],
        [InlineKeyboardButton(text="📌❌ Открепить рекламу", callback_data="admin:ad_unpin")],
        [InlineKeyboardButton(text="« Назад", callback_data="admin:home")],
    ])


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
async def cb_admin_ads(call: CallbackQuery, state: FSMContext, config):
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
    await call.message.answer(
        "📣 <b>Управление рекламой</b>\n\n"
        "Два вида:\n"
        "• <b>Рассылка всем</b> — личное сообщение каждому пользователю бота\n"
        "• <b>Закреп</b> — отправляет сообщение и закрепляет его в чате с пользователем"
        + test_note,
        reply_markup=kb_admin_ads()
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
async def cb_ad_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.", reply_markup=kb_admin_ads())


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
    await status.edit_text(
        f"✅ <b>{kind_label} завершена</b>\n\n"
        f"📨 Доставлено: <b>{sent}</b>\n"
        f"🚫 Заблокировали бота: <b>{blocked}</b>\n"
        f"⚠️ Прочие ошибки: <b>{failed}</b>"
    )


# ── Открепление рекламы ────────────────────────────────────────────────────

@router.callback_query(F.data == "admin:ad_unpin")
async def cb_ad_unpin_confirm(call: CallbackQuery, config):
    if not _is_admin(call.from_user.id, config):
        await call.answer("Недоступно", show_alert=True)
        return
    await call.answer()
    if config.AD_TEST_ID:
        who = f"тестового пользователя (<code>{config.AD_TEST_ID}</code>)"
    else:
        who = "<b>всех пользователей</b> бота"
    await call.message.answer(
        f"📌❌ Открепить рекламу у {who}?\n\n"
        "Бот снимет закреплённые им сообщения в личных чатах.",
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

    if config.AD_TEST_ID:
        targets = [config.AD_TEST_ID]
        mode_note = f"тест ({config.AD_TEST_ID})"
    else:
        targets = await db.get_all_user_ids()
        mode_note = "все"

    status = await call.message.answer(f"⏳ Открепляю ({mode_note})… Чатов: {len(targets)}")

    done, failed = 0, 0
    for uid in targets:
        try:
            # Снимаем все закрепы бота в личном чате с пользователем
            await bot.unpin_all_chat_messages(uid)
            done += 1
        except Exception as e:
            failed += 1
            logger.info(f"Не удалось открепить у {uid}: {e}")
        await asyncio.sleep(0.05)

    await status.edit_text(
        f"✅ <b>Открепление завершено</b>\n\n"
        f"📌 Откреплено в чатах: <b>{done}</b>\n"
        f"⚠️ Ошибки: <b>{failed}</b>"
    )
