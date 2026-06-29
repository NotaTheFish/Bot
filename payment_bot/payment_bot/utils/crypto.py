import base64
from cryptography.fernet import Fernet
from payment_bot.config import settings


def _get_fernet() -> Fernet:
    key = settings.ENCRYPTION_KEY.encode()
    # Ensure key is valid base64 Fernet key
    return Fernet(key)


def encrypt(plaintext: str) -> str:
    f = _get_fernet()
    return f.encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    f = _get_fernet()
    return f.decrypt(ciphertext.encode()).decode()


def generate_key() -> str:
    """Generate a new Fernet key for use in ENCRYPTION_KEY env var."""
    return Fernet.generate_key().decode()
