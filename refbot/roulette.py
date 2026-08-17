"""
Рулетка.

Форма распределения: полосы с явными весами. Не lognormal, не формула —
именно таблица, потому что таблицу можно посмотреть глазами, посчитать EV
и точно знать, сколько ты платишь. Формулу с "красивым" хвостом ты не проверишь,
пока не станет поздно.

Плотность (шанс на единицу выигрыша) падает в обе стороны от 250:
    50–120   : 0.214 %/ед
    120–200  : 0.275 %/ед
    200–300  : 0.340 %/ед  <- мода
    300–500  : 0.090 %/ед
    500–1000 : 0.016 %/ед
    1000–3000: 0.00125 %/ед
    3000–10k : 0.00007 %/ед
"""
import secrets

from config import ROULETTE_BANDS, ROULETTE_ROUND_TO, COIN_RATE

_TOTAL_W = sum(w for _, _, w in ROULETTE_BANDS)


def _rand() -> float:
    """Криптостойкий рандом. random.random() для денег не используем."""
    return secrets.randbits(53) / (1 << 53)


def roll_mushrooms(boosted: bool = False) -> int:
    bands = boosted_bands() if boosted else ROULETTE_BANDS
    tw = sum(w for *_, w in bands)
    r = _rand() * tw
    acc = 0.0
    for low, high, w in bands:
        acc += w
        if r <= acc:
            val = low + _rand() * (high - low)
            val = int(round(val / ROULETTE_ROUND_TO) * ROULETTE_ROUND_TO)
            return max(low, min(high, val))
    return 250  # недостижимо, но пусть будет


def roll(currency: str) -> tuple[int, bool]:
    """
    Ежедневная прокрутка (!шайн). Возвращает (сумма, is_mega_jackpot).

    Сначала бросаем на джекпоты (ROULETTE_JACKPOTS) — редкие фиксированные суммы.
    Если ни один не выпал — полоса (roll_mushrooms).
    Если активна акция x5 (boost_active()) — джекпоты ×BOOST_JACKPOT_MULT и
    бустнутые полосы (крупное ×5, мелочь просажена).
    is_mega_jackpot=True только для джекпота с флагом пасты (миллион).
    Сумма в грибах; для коинов ×COIN_RATE.
    """
    from config import ROULETTE_JACKPOTS, BOOST_JACKPOT_MULT
    boost = boost_active()
    r = _rand()
    acc = 0.0
    for amount, chance, has_paste in ROULETTE_JACKPOTS:
        eff = chance * BOOST_JACKPOT_MULT if boost else chance
        acc += eff
        if r < acc:
            m = amount
            return (m * COIN_RATE if currency == "coins" else m), has_paste
    # джекпот не выпал — полоса (бустнутая или обычная)
    m = roll_mushrooms(boosted=boost)
    return (m * COIN_RATE if currency == "coins" else m), False


# ---------- акция x5 ----------
_boost_until: float = 0.0


def boost_active() -> bool:
    import time
    return time.time() < _boost_until


def boost_start(duration_sec: float) -> None:
    global _boost_until
    import time
    _boost_until = time.time() + duration_sec


def boost_stop() -> None:
    global _boost_until
    _boost_until = 0.0


def boost_seconds_left() -> int:
    import time
    return max(0, int(_boost_until - time.time()))


def boosted_bands():
    """Полосы во время акции: выше среднего ×5, ниже — /5."""
    tw = sum(w for *_, w in ROULETTE_BANDS)
    avg = sum(w / tw * (lo + hi) / 2 for lo, hi, w in ROULETTE_BANDS)
    out = []
    for lo, hi, w in ROULETTE_BANDS:
        mid = (lo + hi) / 2
        out.append((lo, hi, w * 5 if mid > avg else w / 5))
    return out


def roll_jackpot(currency: str) -> tuple[int, bool]:
    """
    Секретная джекпот-прокрутка (!шaйн). Возвращает (сумма, is_jackpot).
    С шансом JACKPOT_CHANCE% выпадает РОВНО JACKPOT_MAX (джекпот), иначе равномерно
    JACKPOT_MIN..JACKPOT_MAX-1 (выглядит как крупная обычная прокрутка).
    Сумма в грибах; для коинов умножается на COIN_RATE.
    """
    from config import JACKPOT_MIN, JACKPOT_MAX, JACKPOT_CHANCE, ROULETTE_ROUND_TO
    if _rand() * 100 < JACKPOT_CHANCE:
        m = JACKPOT_MAX
        is_jack = True
    else:
        val = JACKPOT_MIN + _rand() * (JACKPOT_MAX - 1 - JACKPOT_MIN)
        m = int(round(val / ROULETTE_ROUND_TO) * ROULETTE_ROUND_TO)
        m = max(JACKPOT_MIN, min(JACKPOT_MAX - 1, m))
        is_jack = False
    return (m * COIN_RATE if currency == "coins" else m), is_jack


def expected_value(currency: str = "mushrooms") -> float:
    """Средняя выплата за прокрутку, с учётом джекпотов. Для проверки перед запуском."""
    from config import ROULETTE_JACKPOTS
    band_ev = sum(w / _TOTAL_W * (low + high) / 2 for low, high, w in ROULETTE_BANDS)
    p_jack = sum(chance for _, chance, _ in ROULETTE_JACKPOTS)
    ev = band_ev * (1 - p_jack) + sum(amt * chance for amt, chance, _ in ROULETTE_JACKPOTS)
    return ev * COIN_RATE if currency == "coins" else ev


def band_stats() -> list[tuple[int, int, float, float]]:
    """(low, high, шанс %, плотность %/ед) — для отладки и для админки."""
    out = []
    for low, high, w in ROULETTE_BANDS:
        p = w / _TOTAL_W * 100
        out.append((low, high, p, p / (high - low)))
    return out


# ---------- анимация ----------
# Никаких ASCII-рамок. Telegram рендерит текст пропорциональным шрифтом:
# ╔═══╗ — узкие символы, 🍄 — широкий и разной ширины на разных ОС.
# Рамку из ═ и ║ выровнять невозможно в принципе, она всегда будет ползти.
# Поэтому рамку рисует сам Telegram через <blockquote> — она выровнена всегда.
WHEEL = ["🍄", "🪙", "🎲", "💎", "🌿", "🔥", "⭐️", "🧿"]
BAR_LEN = 5


def frame(i: int, e_roulette: str = "🎰") -> str:
    """
    Кадр прокрутки. Окно из 3 символов скользит по колесу, шкала заполняется.

    Шкала растёт ЛИНЕЙНО до полной и там остаётся (min, а не modulo). Раньше было
    i % (BAR_LEN+1) — на кадре BAR_LEN+1 шкала обнулялась и шла по второму кругу,
    из-за чего выглядело как «повис, сбросил, поехал снова». Теперь заполняется
    один раз: кадров ровно столько, чтобы дойти до полной, дальше — результат.
    """
    a = WHEEL[i % len(WHEEL)]
    b = WHEEL[(i + 3) % len(WHEEL)]
    c = WHEEL[(i + 6) % len(WHEEL)]
    filled = min(i, BAR_LEN)
    bar = "▰" * filled + "▱" * (BAR_LEN - filled)
    return (f"{e_roulette} <b>РУЛЕТКА</b>\n"
            f"<blockquote>{a}   ⟪ {b} ⟫   {c}\n"
            f"{bar}</blockquote>")


def result_card(name: str, amount: int, emoji: str, label: str,
                total: int, e_roulette: str = "🎰") -> str:
    n = f"{amount:,}".replace(",", " ")
    t = f"{total:,}".replace(",", " ")
    return (f"{e_roulette} <b>РУЛЕТКА</b>\n"
            f"<blockquote>{emoji}   ⟪ {emoji} ⟫   {emoji}\n"
            f"{'▰' * BAR_LEN}</blockquote>\n"
            f"👤 {name}\n"
            f"🎁 Выигрыш: <b>{n}</b> {emoji} {label}\n"
            f"💰 Баланс: <b>{t}</b>\n\n"
            f"<i>Следующая прокрутка — завтра</i>")


if __name__ == "__main__":
    print("EV грибы:", round(expected_value("mushrooms"), 1))
    print("EV коины:", round(expected_value("coins"), 1))
    for low, high, p, d in band_stats():
        print(f"{low:>6}-{high:<6} {p:5.2f}%  плотность {d:.5f} %/ед")
