def get_ref_link(bot_username: str, seller_id: int) -> str:
    return f"https://t.me/{bot_username}?start=seller_{seller_id}"


def get_inline_button_str(bot_username: str, seller_id: int) -> str:
    """
    Ссылка для inline-кнопки. При нажатии открывает поле ввода
    с уже подставленным @бот seller_ID — покупатель просто дописывает текст.
    """
    return f"https://t.me/{bot_username}?startinline=seller_{seller_id}%20"


def get_seller_tag(seller: dict) -> str:
    if seller.get("username"):
        return f"@{seller['username']}"
    return f"tg://user?id={seller['id']}"


def initials(name: str) -> str:
    parts = name.strip().split()
    return "".join(p[0].upper() for p in parts if p)[:2] or "??"
