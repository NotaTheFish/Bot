import logging
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext

from payment_bot.config import settings
from payment_bot.db.queries import (
    create_deal, get_deal_by_id, close_deal,
    get_admin_deals, log_event,
)
from payment_bot.db.queries_users import get_admin_by_telegram_id
from payment_bot.keyboards.kb import (
    deal_filter_keyboard, deal_actions_keyboard,
    confirm_close_keyboard, cancel_keyboard, main_admin_menu, sub_admin_menu,
)
from payment_bot.utils.states import CreateDealFSM
from payment_bot.utils.tokens import generate_invite_token, deal_expires_at

logger = logging.getLogger(__name__)
router = Router()

_bot_username: str = ""


def set_bot_username(username: str):
    global _bot_username
    _bot_username = username


async def safe_edit(call: CallbackQuery, text: str, **kwargs):
    try:
        await call.message.edit_text(text, **kwargs)
    except Exception:
        pass


# ─── CREATE DEAL ──────────────────────────────────────────────────────────────

@router.callback_query(F.data == "deal:create")
async def cb_deal_create(call: CallbackQuery, state: FSMContext):
    await safe_edit(
        call,
        "💰 <b>Создание сделки</b>\n\n"
        "Введите сумму в USD (например: <code>50</code> или <code>12.50</code>):",
        reply_markup=cancel_keyboard(),
        parse_mode="HTML",
    )
    await state.set_state(CreateDealFSM.waiting_amount)
    await call.answer()


@router.message(CreateDealFSM.waiting_amount)
async def fsm_deal_amount(message: Message, state: FSMContext):
    text = message.text.strip().replace(",", ".")
    try:
        amount = float(text)
        if amount <= 0 or amount > 100_000:
            raise ValueError
    except ValueError:
        await message.answer("❌ Некорректная сумма. Введите число больше 0 (например: 50 или 12.50):")
        return

    tg_id = message.from_user.id
    admin = await get_admin_by_telegram_id(tg_id)
    if not admin:
        await message.answer("❌ Ошибка: администратор не найден.")
        await state.clear()
        return

    token = generate_invite_token()
    expires = deal_expires_at()

    deal = await create_deal(
        admin_id=admin["id"],
        base_price_usd=amount,
        invite_token=token,
        expires_at=expires,
    )

    await log_event(
        "deal_created",
        {"amount_usd": amount, "token": token},
        deal_id=deal["id"],
        admin_id=admin["id"],
    )

    bot_username = _bot_username or "YOUR_BOT_USERNAME"
    invite_link = f"https://t.me/{bot_username}?start={token}"

    await message.answer(
        f"✅ <b>Сделка создана!</b>\n\n"
        f"💵 Сумма: <b>{amount:.2f} USD</b>\n"
        f"⏰ Действует: <b>15 минут</b>\n\n"
        f"🔗 <b>Одноразовая ссылка для клиента:</b>\n"
        f"<code>{invite_link}</code>\n\n"
        f"Отправьте эту ссылку клиенту в личку.",
        parse_mode="HTML",
    )
    await state.clear()


# ─── LIST DEALS ───────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("deal:list:"))
async def cb_deal_list(call: CallbackQuery):
    tg_id = call.from_user.id
    admin = await get_admin_by_telegram_id(tg_id)
    if not admin:
        await call.answer("Ошибка")
        return

    status_filter = call.data.split(":")[-1]
    status = None if status_filter == "all" else status_filter

    deals = await get_admin_deals(admin["id"], status=status)

    if not deals:
        await safe_edit(
            call,
            "📋 Сделок не найдено.",
            reply_markup=deal_filter_keyboard(),
        )
        await call.answer()
        return

    STATUS_EMOJI = {
        "CREATED": "🆕",
        "WAITING_PAYMENT": "⏳",
        "PAID": "✅",
        "CLOSED": "🔒",
        "EXPIRED": "❌",
    }

    lines = []
    for d in deals[:20]:
        emoji = STATUS_EMOJI.get(d["status"], "•")
        lines.append(
            f"{emoji} #{d['id']} — <b>{d['base_price_usd']:.2f} USD</b> "
            f"[{d['status']}]"
        )

    text = "📋 <b>Ваши сделки:</b>\n\n" + "\n".join(lines)
    if len(deals) > 20:
        text += f"\n\n<i>Показано 20 из {len(deals)}</i>"

    await safe_edit(
        call,
        text,
        reply_markup=deal_filter_keyboard(),
        parse_mode="HTML",
    )
    await call.answer()


# ─── VIEW DEAL ────────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("deal:view:"))
async def cb_deal_view(call: CallbackQuery):
    deal_id = int(call.data.split(":")[-1])
    tg_id = call.from_user.id
    admin = await get_admin_by_telegram_id(tg_id)

    deal = await get_deal_by_id(deal_id)
    if not deal or deal["admin_id"] != admin["id"]:
        await call.answer("Сделка не найдена.")
        return

    STATUS_LABELS = {
        "CREATED": "🆕 Создана",
        "WAITING_PAYMENT": "⏳ Ожидает оплату",
        "PAID": "✅ Оплачена",
        "CLOSED": "🔒 Закрыта",
        "EXPIRED": "❌ Истекла",
    }

    text = (
        f"📄 <b>Сделка #{deal['id']}</b>\n\n"
        f"💵 Сумма: <b>{deal['base_price_usd']:.2f} USD</b>\n"
        f"Статус: {STATUS_LABELS.get(deal['status'], deal['status'])}\n"
        f"Валюта клиента: {deal['selected_currency'] or '—'}\n"
        f"Создана: {deal['created_at'].strftime('%d.%m.%Y %H:%M')}\n"
    )
    if deal["closed_at"]:
        text += f"Закрыта: {deal['closed_at'].strftime('%d.%m.%Y %H:%M')}\n"

    await safe_edit(
        call,
        text,
        reply_markup=deal_actions_keyboard(deal["id"], deal["status"]),
        parse_mode="HTML",
    )
    await call.answer()


# ─── CLOSE DEAL ───────────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("deal:close:"))
async def cb_deal_close_prompt(call: CallbackQuery):
    deal_id = int(call.data.split(":")[-1])
    deal = await get_deal_by_id(deal_id)
    if not deal or deal["status"] != "PAID":
        await call.answer("Сделка не может быть закрыта.")
        return

    await safe_edit(
        call,
        f"⚠️ Вы уверены, что хотите закрыть сделку <b>#{deal_id}</b> "
        f"на сумму <b>{deal['base_price_usd']:.2f} USD</b>?\n\n"
        f"Это действие необратимо. Убедитесь, что товар уже передан клиенту.",
        reply_markup=confirm_close_keyboard(deal_id),
        parse_mode="HTML",
    )
    await call.answer()


@router.callback_query(F.data.startswith("deal:close_confirm:"))
async def cb_deal_close_confirm(call: CallbackQuery):
    deal_id = int(call.data.split(":")[-1])
    tg_id = call.from_user.id
    admin = await get_admin_by_telegram_id(tg_id)

    deal = await get_deal_by_id(deal_id)
    if not deal or deal["admin_id"] != admin["id"] or deal["status"] != "PAID":
        await call.answer("Невозможно закрыть сделку.")
        return

    await close_deal(deal_id)
    await log_event(
        "deal_closed",
        {"closed_by": tg_id},
        deal_id=deal_id,
        admin_id=admin["id"],
    )

    if deal["client_id"]:
        try:
            from payment_bot.db.pool import get_pool
            pool = await get_pool()
            async with pool.acquire() as conn:
                user = await conn.fetchrow(
                    "SELECT telegram_id FROM pt_users WHERE id = $1",
                    deal["client_id"]
                )
            if user:
                await call.bot.send_message(
                    user["telegram_id"],
                    "✅ Сделка завершена. Спасибо за оплату!\n\nЕсли у вас есть вопросы — свяжитесь с продавцом напрямую."
                )
        except Exception as e:
            logger.warning(f"Could not notify client: {e}")

    is_main = tg_id == settings.MAIN_ADMIN_ID
    await safe_edit(
        call,
        f"🔒 Сделка #{deal_id} закрыта.\n"
        f"💵 {deal['base_price_usd']:.2f} USD зачислено в баланс.",
        reply_markup=main_admin_menu() if is_main else sub_admin_menu(),
    )
    await call.answer()


# ─── MENU ─────────────────────────────────────────────────────────────────────

@router.callback_query(F.data == "menu:main")
async def cb_menu_main(call: CallbackQuery, state: FSMContext):
    await state.clear()
    tg_id = call.from_user.id
    is_main = tg_id == settings.MAIN_ADMIN_ID
    await safe_edit(
        call,
        "👑 <b>Панель администратора</b>" if is_main else "🛠 <b>Панель администратора</b>",
        reply_markup=main_admin_menu() if is_main else sub_admin_menu(),
        parse_mode="HTML",
    )
    await call.answer()
