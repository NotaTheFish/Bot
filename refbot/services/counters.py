"""
Счётчики событий — фундамент достижений. Каждое действие в боте инкрементит
счётчик игрока; достижения читают их и проверяют пороги.

Два вида:
  - накопительные: bump(uid, type, delta) — прибавляет (сыграл, вывел, потратил)
  - пиковые: bump_max(uid, type, value) — хранит МАКСИМУМ (крупнейший баланс/выигрыш за раз)

После изменения счётчика вызывается проверка достижений (achievements.check).
"""
import db

# ---- каталог типов счётчиков (документация; не обязательно все использовать) ----
# Валюты
C_EARNED_MUSH = "earned_mush"        # всего заработано грибов
C_EARNED_COIN = "earned_coin"
C_SPENT_MUSH = "spent_mush"          # всего потрачено
C_SPENT_COIN = "spent_coin"
C_SPENT_SHIM = "spent_shim"
C_MAX_MUSH = "max_mush"              # пиковый баланс грибов (за раз)
C_MAX_COIN = "max_coin"
C_MAX_SHIM = "max_shim"
# Вывод
C_WD_COUNT = "wd_count"              # выводов всего
C_WD_TOTAL = "wd_total"             # выведено суммарно
C_WD_MAX = "max_wd"                 # крупнейший вывод
# Казино
C_CASINO_PLAYED = "casino_played"           # сыграно всего (казино)
C_CASINO_WON = "casino_won"                 # выиграно раз
C_CASE_PLAYED = "case_played"
C_WHEEL_PLAYED = "wheel_played"
C_MINES_PLAYED = "mines_played"
C_JACKPOT = "jackpot"                       # джекпотов поймано
C_MAX_WIN = "max_win"                       # крупнейший выигрыш за раз
# Спин-рулетка (!шайн / ежедневка)
C_SPIN_COUNT = "spin_count"
C_SPIN_WON_TOTAL = "spin_won_total"
C_SPIN_MAX = "max_spin_win"
# PvP
C_PVP_PLAYED = "pvp_played"
C_PVP_WON = "pvp_won"
C_TTT_WON = "ttt_won"
C_RPS_WON = "rps_won"
C_NAVY_WON = "navy_won"
C_QUIZ_WON = "quiz_won"
# Конкурсы / розыгрыши
C_CONTEST_WON = "contest_won"
C_GIVEAWAY_WON = "giveaway_won"
C_GIVEAWAY_JOINED = "giveaway_joined"
# Промо / рефералы
C_PROMO_ENTERED = "promo_entered"
C_REFERRALS = "referrals"
# Магазин / бонусы
C_SHOP_BOUGHT = "shop_bought"
C_SHOP_SPENT = "shop_spent"
C_LUCK_USED = "luck_used"
# Мета
C_TITLES = "titles"
C_ACH_DONE = "ach_done"


async def bump(uid: int, counter_type: str, delta: int = 1) -> int:
    """Прибавить к накопительному счётчику. Возвращает новое значение."""
    val = await db.pool().fetchval(
        "INSERT INTO rb_counters (tg_id, counter_type, value, updated_at) "
        "VALUES ($1,$2,$3, now()) "
        "ON CONFLICT (tg_id, counter_type) DO UPDATE "
        "SET value = rb_counters.value + $3, updated_at = now() "
        "RETURNING value", uid, counter_type, delta)
    from services import achievements
    await achievements.check(uid, counter_type, val)
    return val


async def bump_max(uid: int, counter_type: str, value: int) -> int:
    """Обновить пиковый счётчик (хранит максимум). Возвращает текущий максимум."""
    val = await db.pool().fetchval(
        "INSERT INTO rb_counters (tg_id, counter_type, value, updated_at) "
        "VALUES ($1,$2,$3, now()) "
        "ON CONFLICT (tg_id, counter_type) DO UPDATE "
        "SET value = GREATEST(rb_counters.value, $3), updated_at = now() "
        "RETURNING value", uid, counter_type, value)
    from services import achievements
    await achievements.check(uid, counter_type, val)
    return val


async def get(uid: int, counter_type: str) -> int:
    return await db.pool().fetchval(
        "SELECT value FROM rb_counters WHERE tg_id=$1 AND counter_type=$2",
        uid, counter_type) or 0


async def get_all(uid: int) -> dict:
    rows = await db.pool().fetch(
        "SELECT counter_type, value FROM rb_counters WHERE tg_id=$1", uid)
    return {r["counter_type"]: r["value"] for r in rows}


async def casino_event(uid: int, game: str, won: int, is_jackpot: bool = False):
    """Единая фиксация казино-события: сыграл, выиграл, макс-выигрыш, джекпот.
    game: 'case'|'wheel'|'mines'. won — сумма выигрыша (0 если проигрыш)."""
    try:
        await bump(uid, C_CASINO_PLAYED)
        pgame = {"case": C_CASE_PLAYED, "wheel": C_WHEEL_PLAYED, "mines": C_MINES_PLAYED}.get(game)
        if pgame:
            await bump(uid, pgame)
        if won > 0:
            await bump(uid, C_CASINO_WON)
            await bump_max(uid, C_MAX_WIN, int(won))
        if is_jackpot:
            await bump(uid, C_JACKPOT)
    except Exception:
        pass


# человекочитаемые названия триггеров (для админки достижений)
TRIGGER_LABELS = {
    C_EARNED_MUSH: "Заработано грибов (всего)",
    C_EARNED_COIN: "Заработано коинов (всего)",
    C_SPENT_MUSH: "Потрачено грибов (всего)",
    C_SPENT_COIN: "Потрачено коинов (всего)",
    C_SPENT_SHIM: "Потрачено шимкоинов (всего)",
    C_MAX_MUSH: "Пиковый баланс грибов (за раз)",
    C_MAX_COIN: "Пиковый баланс коинов (за раз)",
    C_MAX_SHIM: "Пиковый баланс шимкоинов (за раз)",
    C_WD_COUNT: "Выводов (количество)",
    C_WD_TOTAL: "Выведено (суммарно)",
    C_WD_MAX: "Крупнейший вывод (за раз)",
    C_CASINO_PLAYED: "Сыграно в казино (всего)",
    C_CASINO_WON: "Побед в казино",
    C_CASE_PLAYED: "Открыто кейсов",
    C_WHEEL_PLAYED: "Прокруток колеса",
    C_MINES_PLAYED: "Сыграно в мины",
    C_JACKPOT: "Джекпотов поймано",
    C_MAX_WIN: "Крупнейший выигрыш в казино (за раз)",
    C_SPIN_COUNT: "Прокруток ежедневки",
    C_SPIN_WON_TOTAL: "Выиграно в ежедневке (всего)",
    C_SPIN_MAX: "Крупнейший выигрыш в ежедневке (за раз)",
    C_PVP_PLAYED: "Сыграно PvP (всего)",
    C_PVP_WON: "Побед в PvP",
    C_TTT_WON: "Побед в крестики-нолики",
    C_RPS_WON: "Побед в камень-ножницы-бумага",
    C_NAVY_WON: "Побед в морском бою",
    C_QUIZ_WON: "Побед в викторине",
    C_CONTEST_WON: "Побед в еженедельном конкурсе",
    C_GIVEAWAY_WON: "Побед в розыгрышах",
    C_GIVEAWAY_JOINED: "Участий в розыгрышах",
    C_PROMO_ENTERED: "Промокодов введено",
    C_REFERRALS: "Приглашено друзей",
    C_SHOP_BOUGHT: "Покупок в магазине",
    C_SHOP_SPENT: "Потрачено в магазине",
    C_LUCK_USED: "Активаций удачи",
    C_TITLES: "Получено титулов",
    C_ACH_DONE: "Выполнено достижений",
}
