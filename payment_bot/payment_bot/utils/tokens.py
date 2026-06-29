import secrets
import string
from datetime import datetime, timedelta
from payment_bot.config import settings


def generate_invite_token() -> str:
    """Generate a cryptographically secure one-time deal token."""
    alphabet = string.ascii_letters + string.digits
    return "deal_" + "".join(secrets.choice(alphabet) for _ in range(32))


def generate_admin_key_token() -> str:
    """Generate a one-time sub-admin activation key."""
    alphabet = string.ascii_uppercase + string.digits
    return "adm_" + "".join(secrets.choice(alphabet) for _ in range(24))


def deal_expires_at() -> datetime:
    return datetime.utcnow() + timedelta(minutes=settings.DEAL_TTL_MINUTES)


def admin_key_expires_at(hours: int = 24) -> datetime:
    return datetime.utcnow() + timedelta(hours=hours)


def generate_idempotency_key(deal_id: int, provider: str) -> str:
    return f"{provider}_{deal_id}_{secrets.token_hex(8)}"
