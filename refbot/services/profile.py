"""
Профиль: кастомные ники, отображение имени со ссылкой на профиль, активный титул/эмодзи.
"""
import re
import html

import db

MAX_NICK = 32
# разрешены буквы (рус/лат), цифры, пробел, дефис, подчёркивание
_NICK_RE = re.compile(r"^[A-Za-zА-Яа-яЁё0-9 _\-]{1,32}$")
# запрещённые подстроки (ссылки, инъекции, разметка)
_BAD = ("http://", "https://", "www.", "t.me", "@", "<", ">", "\n", "\t",
        "tg://", ".com", ".ru", ".org", "://")


def validate_nick(raw: str) -> tuple[str | None, str]:
    """Проверить и нормализовать ник. Возвращает (ник, "") или (None, ошибка)."""
    if not raw:
        return None, "Пустой ник."
    nick = raw.strip()
    # отклоняем управляющие символы (переносы, табы и пр.) до нормализации
    if any(ord(ch) < 32 for ch in nick):
        return None, "Ник содержит недопустимые символы."
    nick = re.sub(r"\s+", " ", nick)   # схлопнуть пробелы
    if len(nick) < 2:
        return None, "Ник слишком короткий (мин. 2 символа)."
    if len(nick) > MAX_NICK:
        return None, f"Ник слишком длинный (макс. {MAX_NICK})."
    low = nick.lower()
    for bad in _BAD:
        if bad in low:
            return None, "Ник содержит запрещённые символы (ссылки, @, скобки — нельзя)."
    if not _NICK_RE.match(nick):
        return None, "Только буквы, цифры, пробел, дефис и подчёркивание. Эмодзи и символы нельзя."
    return nick, ""


async def nick_taken(nick: str, exclude_uid: int | None = None) -> bool:
    """Занят ли ник (регистронезависимо)."""
    row = await db.pool().fetchval(
        "SELECT tg_id FROM rb_users WHERE lower(nickname)=lower($1) "
        "AND ($2::bigint IS NULL OR tg_id<>$2)", nick, exclude_uid)
    return row is not None


async def get_profile(uid: int) -> dict | None:
    row = await db.pool().fetchrow(
        "SELECT tg_id, username, first_name, nickname, nickname_set, active_title, active_emoji "
        "FROM rb_users WHERE tg_id=$1", uid)
    return dict(row) if row else None


async def set_nick(uid: int, nick: str, mark_set: bool = True) -> tuple[bool, str]:
    """Установить ник. Проверяет уникальность."""
    nick, err = validate_nick(nick)
    if err:
        return False, err
    if await nick_taken(nick, exclude_uid=uid):
        return False, "Этот ник уже занят. Придумай другой."
    await db.pool().execute(
        "UPDATE rb_users SET nickname=$1, nickname_set=$2 WHERE tg_id=$3",
        nick, mark_set, uid)
    return True, ""


async def active_emoji(uid: int) -> str:
    """Активный персональный эмодзи игрока (символ) или пусто."""
    e = await db.pool().fetchval("SELECT active_emoji FROM rb_users WHERE tg_id=$1", uid)
    return e or ""


async def active_title_name(uid: int) -> str:
    """Название активного титула или пусто."""
    row = await db.pool().fetchrow(
        "SELECT t.name FROM rb_users u JOIN rb_titles t ON t.id=u.active_title "
        "WHERE u.tg_id=$1", uid)
    return row["name"] if row else ""


async def display_name(uid: int, with_emoji: bool = True, link: bool = True) -> str:
    """
    Главная функция отображения игрока. Если задан ник — показываем ник (со ссылкой
    на профиль по ID) + персональный эмодзи. Иначе @username или имя.
    """
    p = await get_profile(uid)
    if not p:
        return str(uid)
    emo = ""
    if with_emoji and p.get("active_emoji"):
        emo = p["active_emoji"] + " "
    if p.get("nickname"):
        nk = html.escape(p["nickname"])
        if link:
            return f'{emo}<a href="tg://user?id={uid}">{nk}</a>'
        return f"{emo}{nk}"
    # без ника — как раньше
    if p.get("username"):
        return f"{emo}@{p['username']}"
    return f"{emo}{p.get('first_name') or uid}"


async def user_emojis(uid: int) -> list[str]:
    """Персональные эмодзи игрока (полученные за достижения/магазин)."""
    rows = await db.pool().fetch(
        "SELECT emoji FROM rb_user_emojis WHERE tg_id=$1 ORDER BY got_at DESC", uid)
    return [r["emoji"] for r in rows]


async def set_active_emoji(uid: int, emoji: str | None) -> bool:
    """Выбрать активный эмодзи (или снять). Проверяет владение."""
    if emoji:
        owns = await db.pool().fetchval(
            "SELECT 1 FROM rb_user_emojis WHERE tg_id=$1 AND emoji=$2", uid, emoji)
        if not owns:
            return False
    await db.pool().execute(
        "UPDATE rb_users SET active_emoji=$1 WHERE tg_id=$2", emoji, uid)
    return True


def extract_emoji(msg) -> str | None:
    """Извлечь эмодзи из сообщения. Если это премиум (custom_emoji entity) —
    вернуть тег <tg-emoji emoji-id="...">символ</tg-emoji>, чтобы он рендерился
    именно этим премиумом, не подменяясь общим маппингом бота.
    Иначе — обычный символ."""
    text = (msg.text or "").strip()
    ents = msg.entities or []
    for e in ents:
        if e.type == "custom_emoji":
            # символ-подложка под этим entity
            sym = text[e.offset:e.offset + e.length]
            return f'<tg-emoji emoji-id="{e.custom_emoji_id}">{sym}</tg-emoji>'
    # обычный эмодзи — первый «символ» (может быть составным)
    return text.split()[0] if text else None
