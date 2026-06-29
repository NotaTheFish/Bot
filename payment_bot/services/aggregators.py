import hashlib
import hmac
import json
import logging
import aiohttp
from typing import Optional
from payment_bot.config import settings

logger = logging.getLogger(__name__)


# ─── BASE ─────────────────────────────────────────────────────────────────────

class AggregatorError(Exception):
    pass


# ─── LAVA.RU ──────────────────────────────────────────────────────────────────

class LavaAggregator:
    def __init__(self, api_key: str, shop_id: str):
        self.api_key = api_key
        self.shop_id = shop_id
        self.base_url = settings.LAVA_API_URL

    def _sign(self, payload: dict) -> str:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        return hmac.new(
            self.api_key.encode(),
            body.encode(),
            hashlib.sha256
        ).hexdigest()

    async def create_invoice(
        self,
        amount: float,
        currency: str,
        order_id: str,
        comment: str = "",
    ) -> dict:
        payload = {
            "shopId": self.shop_id,
            "sum": amount,
            "orderId": order_id,
            "currency": currency,
            "comment": comment,
        }
        headers = {
            "X-Api-Key": self.api_key,
            "Signature": self._sign(payload),
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}/business/invoice/create",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                data = await resp.json()
                if data.get("status") != 1:
                    raise AggregatorError(f"Lava error: {data}")
                return {
                    "payment_url": data["data"]["url"],
                    "invoice_id": data["data"]["id"],
                }

    def verify_webhook(self, payload: dict, signature: str) -> bool:
        expected = self._sign(payload)
        return hmac.compare_digest(expected, signature)

    def parse_webhook(self, payload: dict) -> dict:
        return {
            "order_id": payload.get("orderId"),
            "external_tx_id": payload.get("id"),
            "amount": float(payload.get("sum", 0)),
            "currency": payload.get("currency", "RUB"),
            "status": payload.get("status"),  # 'success'
        }


# ─── PAYOK.IO ─────────────────────────────────────────────────────────────────

class PayokAggregator:
    def __init__(self, api_key: str, shop_id: str, secret_key: str):
        self.api_key = api_key
        self.shop_id = shop_id
        self.secret_key = secret_key
        self.base_url = settings.PAYOK_API_URL

    def _sign_invoice(self, amount: float, payment_id: str, currency: str) -> str:
        sign_str = f"{amount}|{payment_id}|{self.shop_id}|{currency}|{self.secret_key}"
        return hashlib.md5(sign_str.encode()).hexdigest()

    def verify_webhook(self, payload: dict) -> bool:
        sign = payload.get("sign", "")
        amount = payload.get("amount", "")
        payment_id = payload.get("payment_id", "")
        currency = payload.get("currency", "RUB")
        expected = self._sign_invoice(amount, payment_id, currency)
        return hmac.compare_digest(expected, sign)

    async def create_invoice(
        self,
        amount: float,
        currency: str,
        order_id: str,
        desc: str = "Оплата",
    ) -> dict:
        sign = self._sign_invoice(amount, order_id, currency)
        params = {
            "amount": amount,
            "payment": order_id,
            "shop": self.shop_id,
            "currency": currency,
            "desc": desc,
            "sign": sign,
        }
        # Payok uses redirect URL approach
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        payment_url = f"https://payok.io/pay?{qs}"
        return {
            "payment_url": payment_url,
            "invoice_id": order_id,
        }

    def parse_webhook(self, payload: dict) -> dict:
        return {
            "order_id": payload.get("payment_id"),
            "external_tx_id": payload.get("transaction"),
            "amount": float(payload.get("amount", 0)),
            "currency": payload.get("currency", "RUB"),
            "status": "success" if payload.get("status") == "success" else "fail",
        }


# ─── FREEKASSA ────────────────────────────────────────────────────────────────

class FreekassaAggregator:
    def __init__(self, api_key: str, shop_id: str, secret1: str, secret2: str):
        self.api_key = api_key
        self.shop_id = shop_id
        self.secret1 = secret1
        self.secret2 = secret2
        self.base_url = settings.FREEKASSA_API_URL

    def _sign_order(self, amount: float, order_id: str, currency: str) -> str:
        sign_str = f"{self.shop_id}:{amount}:{self.secret1}:{currency}:{order_id}"
        return hashlib.md5(sign_str.encode()).hexdigest()

    def verify_webhook(self, payload: dict) -> bool:
        amount = payload.get("AMOUNT", "")
        order_id = payload.get("MERCHANT_ORDER_ID", "")
        sign = payload.get("SIGN", "")
        expected = hashlib.md5(
            f"{self.shop_id}:{amount}:{self.secret2}:{order_id}".encode()
        ).hexdigest()
        return hmac.compare_digest(expected, sign)

    async def create_invoice(
        self,
        amount: float,
        currency: str,
        order_id: str,
    ) -> dict:
        sign = self._sign_order(amount, order_id, currency)
        params = {
            "m": self.shop_id,
            "oa": amount,
            "currency": currency,
            "o": order_id,
            "s": sign,
            "lang": "ru",
        }
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        payment_url = f"https://pay.freekassa.ru/?{qs}"
        return {
            "payment_url": payment_url,
            "invoice_id": order_id,
        }

    def parse_webhook(self, payload: dict) -> dict:
        return {
            "order_id": payload.get("MERCHANT_ORDER_ID"),
            "external_tx_id": payload.get("intid"),
            "amount": float(payload.get("AMOUNT", 0)),
            "currency": payload.get("CUR_ID", "RUB"),
            "status": "success",  # Freekassa only sends successful webhooks
        }


# ─── FACTORY ──────────────────────────────────────────────────────────────────

def get_aggregator(provider: str, data: dict):
    if provider == "lava":
        return LavaAggregator(
            api_key=data["api_key"],
            shop_id=data.get("shop_id", ""),
        )
    if provider == "payok":
        return PayokAggregator(
            api_key=data["api_key"],
            shop_id=data["shop_id"],
            secret_key=data["secret"],
        )
    if provider == "freekassa":
        return FreekassaAggregator(
            api_key=data["api_key"],
            shop_id=data["shop_id"],
            secret1=data["secret"],
            secret2=data.get("secret2", ""),
        )
    raise AggregatorError(f"Unknown provider: {provider}")


CURRENCY_MAP = {
    "lava": ["RUB", "UAH", "EUR", "USD"],
    "payok": ["RUB", "UAH", "KZT", "BYN"],
    "freekassa": ["RUB", "UAH", "KZT", "BYN", "MDL"],
}
