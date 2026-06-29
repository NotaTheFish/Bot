import logging
from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from payment_bot.db.queries import (
    get_deal_by_id, set_deal_currency, create_payment,
    confirm_payment, set_deal_paid, get_rate, log_event,
    get_payment_by_external_tx,
)
from payment_bot.db.pool import get_pool
from payment_bot.services.aggregators import get_aggregator, AggregatorError, CURRENCY_MAP
from payment_bot.utils.crypto import decrypt
from payment_bot.utils.tokens import generate_idempotency_key
import json

logger = logging.getLogger(__name__)
router = Router()


async def safe_edit(call: CallbackQuery, text: str, **kwargs):
    try:
        await call.message.edit_text(text, **kwargs)
    except Exception:
        pass


# ─── CURRENCY SELECTION ───────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("pay:currency:"))
async def cb_pay_currency(call: CallbackQuery):
    currency = call.data.split(":")[-1]
    tg_id = call.from_user.id

    pool = await get_pool()
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "SELECT * FROM pt_users WHERE telegram_id = $1", tg_id
        )
        if not user:
            await call.answer("Ошибка.")
            return

        deal = await conn.fetchrow(
            "SELECT * FROM pt_deals WHERE client_id = $1 AND status = 'WAITING_PAYMENT' ORDER BY created_at DESC LIMIT 1",
            user["id"]
        )
        if not deal:
            await call.answer("Активная сделка не найдена.")
            return

        admin = await conn.fetchrow(
            "SELECT * FROM pt_admins WHERE id = $1", deal["admin_id"]
        )

    if not admin or not admin["aggregator"]:
        await safe_edit(
            call,
            "❌ Продавец ещё не настроил способ приёма оплаты. Свяжитесь с ним напрямую."
        )
        await call.answer()
        return

    provider = admin["aggregator"]
    allowed = CURRENCY_MAP.get(provider, [])
    if currency not in allowed:
        await call.answer(f"Валюта {currency} не поддерживается.")
        return

    rate = await get_rate(currency)
    if not rate:
        await call.answer("Ошибка получения курса валют.")
        return

    amount_local = round(float(deal["base_price_usd"]) * rate, 2)
    amount_usd = float(deal["base_price_usd"])

    await set_deal_currency(deal["id"], currency)

    try:
        agg_data = json.loads(admin["aggregator_data"] or "{}")
        agg_data["api_key"] = decrypt(admin["aggregator_key"])
        aggregator = get_aggregator(provider, agg_data)
    except Exception as e:
        logger.error(f"Aggregator init error: {e}")
        await safe_edit(call, "❌ Ошибка настройки оплаты. Обратитесь к продавцу.")
        await call.answer()
        return

    order_id = f"deal_{deal['id']}_{generate_idempotency_key(deal['id'], provider)[-8:]}"
    idempotency_key = generate_idempotency_key(deal["id"], provider)

    await create_payment(
        deal_id=deal["id"],
        provider=provider,
        amount_original=amount_local,
        currency=currency,
        amount_usd_equivalent=amount_usd,
        idempotency_key=idempotency_key,
    )

    try:
        invoice = await aggregator.create_invoice(
            amount=amount_local,
            currency=currency,
            order_id=order_id,
        )
    except AggregatorError as e:
        logger.error(f"Invoice creation failed: {e}")
        await safe_edit(
            call,
            "❌ Не удалось создать счёт. Попробуйте другой способ оплаты или свяжитесь с продавцом."
        )
        await call.answer()
        return

    await log_event(
        "payment_invoice_created",
        {"provider": provider, "currency": currency, "amount": amount_local, "order_id": order_id},
        deal_id=deal["id"],
    )

    currency_symbols = {"RUB": "₽", "UAH": "₴", "EUR": "€", "USD": "$", "KZT": "₸", "BYN": "Br", "MDL": "L"}
    symbol = currency_symbols.get(currency, currency)

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(
        text=f"💳 Перейти к оплате {amount_local:.2f} {symbol}",
        url=invoice["payment_url"]
    ))
    builder.row(InlineKeyboardButton(
        text="✅ Я оплатил",
        callback_data=f"pay:done:{deal['id']}"
    ))

    await safe_edit(
        call,
        f"💳 <b>Оплата</b>\n\n"
        f"Сумма: <b>{amount_local:.2f} {symbol}</b>\n"
        f"({amount_usd:.2f} USD)\n\n"
        f"Нажмите кнопку ниже, перейдите к оплате, затем вернитесь и нажмите «Я оплатил».",
        reply_markup=builder.as_markup(),
        parse_mode="HTML",
    )
    await call.answer()


# ─── CLIENT CONFIRMS PAYMENT ──────────────────────────────────────────────────

@router.callback_query(F.data.startswith("pay:done:"))
async def cb_pay_done(call: CallbackQuery):
    deal_id = int(call.data.split(":")[-1])
    deal = await get_deal_by_id(deal_id)

    if not deal or deal["status"] not in ("WAITING_PAYMENT",):
        await call.answer("Статус сделки изменился.")
        return

    await safe_edit(
        call,
        "⏳ Спасибо! Ожидаем подтверждения платежа от платёжной системы.\n\n"
        "Это обычно занимает несколько секунд. Вы получите уведомление автоматически."
    )
    await call.answer()


# ─── WEBHOOK PROCESSING ───────────────────────────────────────────────────────

async def process_webhook_payment(
    bot,
    provider: str,
    payload: dict,
    signature: str = "",
) -> bool:
    try:
        pool = await get_pool()

        parsed = _parse_by_provider(provider, payload)
        if not parsed:
            logger.warning(f"Could not parse webhook from {provider}")
            return False

        order_id = parsed["order_id"]
        external_tx_id = parsed["external_tx_id"]
        status = parsed["status"]

        if status != "success":
            logger.info(f"Non-success webhook from {provider}: {status}")
            return False

        deal_id = _extract_deal_id(order_id)
        if not deal_id:
            logger.warning(f"Could not extract deal_id from order_id: {order_id}")
            return False

        deal = await get_deal_by_id(deal_id)
        if not deal:
            logger.warning(f"Deal not found: {deal_id}")
            return False

        if deal["status"] == "CLOSED":
            logger.info(f"Deal {deal_id} already closed, ignoring webhook")
            return True

        if deal["status"] not in ("WAITING_PAYMENT",):
            logger.warning(f"Deal {deal_id} in unexpected status: {deal['status']}")
            return False

        existing = await get_payment_by_external_tx(external_tx_id)
        if existing:
            logger.info(f"Duplicate webhook, tx already processed: {external_tx_id}")
            return True

        async with pool.acquire() as conn:
            payment = await conn.fetchrow(
                "SELECT * FROM pt_payments WHERE deal_id = $1 AND status = 'pending'",
                deal_id
            )

        if not payment:
            logger.warning(f"No pending payment for deal {deal_id}")
            return False

        expected = float(payment["amount_original"])
        received = float(parsed["amount"])
        if received < expected * 0.99:
            logger.warning(f"Amount mismatch: expected {expected}, got {received}")
            await log_event(
                "webhook_amount_mismatch",
                {"expected": expected, "received": received},
                deal_id=deal_id,
            )
            return False

        confirmed = await confirm_payment(
            idempotency_key=payment["idempotency_key"],
            external_tx_id=external_tx_id,
        )
        if not confirmed:
            return False

        await set_deal_paid(deal_id)

        await log_event(
            "payment_confirmed",
            {"provider": provider, "tx_id": external_tx_id, "amount": received},
            deal_id=deal_id,
        )

        if deal["client_id"]:
            try:
                async with pool.acquire() as conn:
                    user = await conn.fetchrow(
                        "SELECT telegram_id FROM pt_users WHERE id = $1",
                        deal["client_id"]
                    )
                if user:
                    await bot.send_message(
                        user["telegram_id"],
                        f"✅ <b>Оплата подтверждена!</b>\n\n"
                        f"Сумма: {payment['amount_original']:.2f} {payment['currency']}\n\n"
                        f"Продавец получил уведомление и скоро свяжется с вами для передачи товара.",
                        parse_mode="HTML",
                    )
            except Exception as e:
                logger.warning(f"Could not notify client: {e}")

        try:
            async with pool.acquire() as conn:
                admin_user = await conn.fetchrow(
                    """
                    SELECT u.telegram_id FROM pt_admins a
                    JOIN pt_users u ON u.id = a.user_id
                    WHERE a.id = $1
                    """,
                    deal["admin_id"]
                )
            if admin_user:
                await bot.send_message(
                    admin_user["telegram_id"],
                    f"💰 <b>Получена оплата по сделке #{deal_id}!</b>\n\n"
                    f"Сумма: <b>{deal['base_price_usd']:.2f} USD</b>\n"
                    f"Провайдер: {provider}\n\n"
                    f"Передайте товар клиенту и закройте сделку через бота.",
                    parse_mode="HTML",
                )
        except Exception as e:
            logger.warning(f"Could not notify admin: {e}")

        return True

    except Exception as e:
        logger.error(f"Webhook processing error: {e}", exc_info=True)
        return False


def _parse_by_provider(provider: str, payload: dict) -> dict | None:
    try:
        if provider == "lava":
            return {
                "order_id": payload.get("orderId", ""),
                "external_tx_id": payload.get("id", ""),
                "amount": float(payload.get("sum", 0)),
                "status": "success" if payload.get("status") == "success" else "fail",
            }
        if provider == "payok":
            return {
                "order_id": payload.get("payment_id", ""),
                "external_tx_id": payload.get("transaction", ""),
                "amount": float(payload.get("amount", 0)),
                "status": "success" if payload.get("status") == "success" else "fail",
            }
        if provider == "freekassa":
            return {
                "order_id": payload.get("MERCHANT_ORDER_ID", ""),
                "external_tx_id": payload.get("intid", ""),
                "amount": float(payload.get("AMOUNT", 0)),
                "status": "success",
            }
    except Exception:
        return None
    return None


def _extract_deal_id(order_id: str) -> int | None:
    try:
        parts = order_id.split("_")
        if parts[0] == "deal" and len(parts) >= 2:
            return int(parts[1])
    except Exception:
        pass
    return None
