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


def roll_mushrooms() -> int:
    r = _rand() * _TOTAL_W
    acc = 0.0
    for low, high, w in ROULETTE_BANDS:
        acc += w
        if r <= acc:
            val = low + _rand() * (high - low)
            val = int(round(val / ROULETTE_ROUND_TO) * ROULETTE_ROUND_TO)
            return max(low, min(high, val))
    return 250  # недостижимо, но пусть будет


def roll(currency: str) -> int:
    m = roll_mushrooms()
    return m * COIN_RATE if currency == "coins" else m


def expected_value(currency: str = "mushrooms") -> float:
    """Сколько ты платишь в среднем за одну прокрутку. Запусти перед запуском бота."""
    ev = sum(w / _TOTAL_W * (low + high) / 2 for low, high, w in ROULETTE_BANDS)
    return ev * COIN_RATE if currency == "coins" else ev


def band_stats() -> list[tuple[int, int, float, float]]:
    """(low, high, шанс %, плотность %/ед) — для отладки и для админки."""
    out = []
    for low, high, w in ROULETTE_BANDS:
        p = w / _TOTAL_W * 100
        out.append((low, high, p, p / (high - low)))
    return out


# ---------- анимация ----------
WHEEL = ["🍄", "🪙", "🎲", "💎", "🌿", "🔥", "⭐️", "🧿"]


def frame(i: int, currency_emoji: str) -> str:
    """Кадр прокрутки. Окно из 3 символов, скользит по колесу."""
    a = WHEEL[i % len(WHEEL)]
    b = WHEEL[(i + 3) % len(WHEEL)]
    c = WHEEL[(i + 6) % len(WHEEL)]
    bar = "▰" * (i % 6) + "▱" * (5 - i % 6)
    return (
        f"🎰 <b>РУЛЕТКА</b> 🎰\n"
        f"╔═══════════╗\n"
        f"║  {a}  ⟪ {b} ⟫  {c}  ║\n"
        f"╚═══════════╝\n"
        f"<i>{bar}</i>"
    )


def result_card(name: str, amount: int, currency: str, emoji: str, total: int) -> str:
    cur = "грибов" if currency == "mushrooms" else "коинов"
    return (
        f"🎰 <b>РУЛЕТКА</b> 🎰\n"
        f"╔═══════════╗\n"
        f"║  {emoji}  ⟪ {emoji} ⟫  {emoji}  ║\n"
        f"╚═══════════╝\n\n"
        f"👤 {name}\n"
        f"🎁 Выигрыш: <b>{amount:,}</b> {cur} {emoji}\n"
        f"💰 Баланс: <b>{total:,}</b>\n\n"
        f"<i>Следующая прокрутка — завтра</i>"
    ).replace(",", " ")


if __name__ == "__main__":
    print("EV грибы:", round(expected_value("mushrooms"), 1))
    print("EV коины:", round(expected_value("coins"), 1))
    for low, high, p, d in band_stats():
        print(f"{low:>6}-{high:<6} {p:5.2f}%  плотность {d:.5f} %/ед")
