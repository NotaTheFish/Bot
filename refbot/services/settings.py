"""
Кастомизация: эмодзи, названия и шаблон профиля правятся из админки без деплоя.

Хранение: rb_settings (key -> value). Кэш в памяти, инвалидируется при записи.
Премиум-эмодзи хранятся парой: emoji.<slot> = символ-фолбэк, premium.<slot> = custom_emoji_id.

Bot API 9.4 (09.02.2026): бот может слать custom_emoji, если у ВЛАДЕЛЬЦА бота
есть Telegram Premium — в личку, группы и супергруппы. Либо если боту куплен
доп. юзернейм на Fragment. Каналы не поддерживаются.
Дока молчит про editMessageText — проверяется кнопкой «🧪 Тест» в админке.
Если Telegram всё же отклонит entity — render.py молча откатится на символ-фолбэк.
"""
import logging

import asyncpg

import db

log = logging.getLogger(__name__)

# slot -> (описание для админки, дефолтный символ)
EMOJI_SLOTS = {
    "mushrooms": ("Грибы", "🍄"),
    "coins":     ("Коины", "🪙"),
    "profile":   ("Заголовок профиля", "👤"),
    "balance":   ("Баланс", "💰"),
    "hold":      ("На удержании", "⏳"),
    "paid":      ("Зачислено", "✅"),
    "lost":      ("Потеряно", "❌"),
    "refs":      ("Рефералы", "👥"),
    "link":      ("Ссылка", "🔗"),
    "roulette":  ("Рулетка", "🎰"),
    "settings":  ("Настройки", "⚙️"),
    "chat":      ("Чат", "📢"),
    "withdraw":  ("Вывод", "💸"),
    "admin":     ("Админка", "🛠"),
    "back":      ("Назад", "⬅️"),
    "top":       ("Топ", "🏆"),
}

LABEL_SLOTS = {
    "mushrooms": ("Название валюты 1", "Грибы"),
    "coins":     ("Название валюты 2", "Коины"),
}

DEFAULT_PROFILE = """{e_profile} <b>Профиль</b>
ID: <code>{id}</code>

{e_balance} <b>Баланс</b>
{e_mushrooms} {l_mushrooms}: <b>{bal_m}</b>
{e_coins} {l_coins}: <b>{bal_c}</b>

{e_hold} <b>На удержании</b>
{e_mushrooms} {hold_m}   {e_coins} {hold_c}

{e_refs} <b>Рефералы</b>
{e_paid} Зачислено: {paid}
{e_hold} Ждут: {hold}
{e_lost} Потеряно: {lost}
{chats}
{e_settings} Валюта: {e_cur} {l_cur}"""

# все плейсхолдеры, которые доступны в шаблоне профиля
PROFILE_KEYS = ["id", "bal_m", "bal_c", "hold_m", "hold_c", "paid", "hold", "lost",
                "chats", "e_cur", "l_cur"] + \
               [f"e_{s}" for s in EMOJI_SLOTS] + [f"l_{s}" for s in LABEL_SLOTS]

_cache: dict[str, str] | None = None
_warned = False


# services/settings.py: список эмодзи, реально встречающихся в сообщениях бота.
# Держим здесь, чтобы «Свободные замены» могли показать, что ещё не покрыто премиумом.
USED_EMOJI = [
    "🍄", "🪙", "🎰", "💰", "💸", "👤", "👥", "🏆", "🎫", "🎁", "🎟", "🏁",
    "📊", "📢", "🔗", "📥", "🧯", "✅", "🚫", "⏳", "❌", "🚩", "♻️", "🥳",
    "😡", "📅", "📜", "🕐", "😀", "🏷", "📝", "🧪", "👁", "👋", "🎉", "➕",
    "🗑", "✏️", "🔄", "🟢", "⚪️", "⭐️", "⌛",
]


async def load(force: bool = False) -> dict[str, str]:
    """
    Настройки — это косметика. Если таблицы ещё нет (setup.sql не накатан) или БД
    икнула — отдаём пустой кэш и работаем на дефолтах. Ронять бота из-за эмодзи нельзя.
    """
    global _cache, _warned
    if _cache is None or force:
        try:
            rows = await db.pool().fetch("SELECT key, value FROM rb_settings")
            _cache = {r["key"]: r["value"] for r in rows}
            _warned = False
        except asyncpg.UndefinedTableError:
            if not _warned:
                log.warning("rb_settings нет — работаю на стандартном оформлении. "
                            "Накати setup.sql, чтобы включить кастомизацию.")
                _warned = True
            _cache = {}
        except Exception as e:
            log.warning("не смог прочитать rb_settings (%s), беру дефолты", e)
            _cache = {}
    return _cache


async def get(key: str, default: str = "") -> str:
    return (await load()).get(key, default)


async def set(key: str, value: str, actor: int | None = None) -> bool:
    """False = таблицы нет, настройка не сохранена (нужен setup.sql)."""
    try:
        await db.pool().execute(
            """
            INSERT INTO rb_settings (key, value, updated_by, updated_at)
            VALUES ($1,$2,$3, now())
            ON CONFLICT (key) DO UPDATE
            SET value=EXCLUDED.value, updated_by=EXCLUDED.updated_by, updated_at=now()
            """, key, value, actor)
    except asyncpg.UndefinedTableError:
        log.warning("rb_settings нет — настройка %s не сохранена", key)
        return False
    await load(force=True)
    return True


async def unset(key: str) -> bool:
    try:
        await db.pool().execute("DELETE FROM rb_settings WHERE key=$1", key)
    except asyncpg.UndefinedTableError:
        return False
    await load(force=True)
    return True


async def emoji(slot: str) -> str:
    """Символ для слота (он же фолбэк, если премиум не пройдёт)."""
    return await get(f"emoji.{slot}", EMOJI_SLOTS[slot][1])


async def label(slot: str) -> str:
    return await get(f"label.{slot}", LABEL_SLOTS[slot][1])


async def emoji_map() -> dict[str, str]:
    """
    {символ: custom_emoji_id} — из слотов И из свободных замен.

    Свободные замены (free.<символ>) — главная фишка: премиум подставляется ПО СИМВОЛУ,
    а не по слоту. Замапил 📊 один раз — он станет премиумом в любом сообщении и на
    любой кнопке, где встретится. Без этого пришлось бы заводить слот под каждое
    эмодзи в коде и всё равно что-нибудь забыть.
    """
    s = await load()
    out = {}
    for slot, (_, dflt) in EMOJI_SLOTS.items():
        cid = s.get(f"premium.{slot}")
        if cid:
            out[s.get(f"emoji.{slot}", dflt)] = cid
    for k, v in s.items():
        if k.startswith("free."):
            out[k[5:]] = v
    return out


async def free_map() -> dict[str, str]:
    """Только свободные замены — для списка в админке."""
    s = await load()
    return {k[5:]: v for k, v in s.items() if k.startswith("free.")}


async def profile_template() -> str:
    return await get("profile.template", DEFAULT_PROFILE)


async def ctx() -> dict[str, str]:
    """Готовые {e_*} и {l_*} для подстановки в шаблоны."""
    s = await load()
    d = {}
    for slot, (_, dflt) in EMOJI_SLOTS.items():
        d[f"e_{slot}"] = s.get(f"emoji.{slot}", dflt)
    for slot, (_, dflt) in LABEL_SLOTS.items():
        d[f"l_{slot}"] = s.get(f"label.{slot}", dflt)
    return d


def validate_template(tpl: str) -> str:
    """Пустая строка = ок, иначе текст ошибки. Проверяем ДО сохранения."""
    try:
        tpl.format(**{k: "0" for k in PROFILE_KEYS})
    except KeyError as e:
        return f"Неизвестный плейсхолдер: {e}"
    except (IndexError, ValueError) as e:
        return f"Кривые скобки: {e}"
    if len(tpl) > 3000:
        return "Слишком длинный (лимит 3000 символов)"
    return ""
