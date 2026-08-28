"""
Камень-ножницы-бумага: чистая логика раунда (без БД и Telegram).
Мультиплеер на выбывание: играют, пока не останется один.

Знаки: 'rock' (камень), 'scissors' (ножницы), 'paper' (бумага).
Правила: камень бьёт ножницы, ножницы бьют бумагу, бумага бьёт камень.
"""

CHOICES = ("rock", "scissors", "paper")
BEATS = {"rock": "scissors", "scissors": "paper", "paper": "rock"}  # ключ бьёт значение

NAMES = {"rock": "Камень", "scissors": "Ножницы", "paper": "Бумага"}
EMOJI = {"rock": "🪨", "scissors": "✂️", "paper": "📄"}


def resolve_round(choices: dict[int, str]) -> tuple[str, list[int], list[int]]:
    """
    Разобрать раунд. choices: {tg_id: 'rock'|'scissors'|'paper'} — только активные игроки.
    Возвращает (исход, survivors, eliminated):
      - исход 'draw'  -> ничья (1 или 3 разных знака): survivors=все, eliminated=[]
      - исход 'decided' -> есть проигравшие: survivors=прошедшие, eliminated=выбывшие
    """
    distinct = set(choices.values())
    # ничья: все показали одно и то же ИЛИ присутствуют все три знака (цикл)
    if len(distinct) == 1 or len(distinct) == 3:
        return "draw", list(choices.keys()), []
    # ровно два разных знака: один бьёт другой
    a, b = list(distinct)
    winner_sign = a if BEATS[a] == b else b
    loser_sign = b if winner_sign == a else a
    survivors = [uid for uid, ch in choices.items() if ch == winner_sign]
    eliminated = [uid for uid, ch in choices.items() if ch == loser_sign]
    return "decided", survivors, eliminated


def round_summary(choices: dict[int, str], name_of) -> str:
    """Короткий текст итога раунда для показа (name_of: tg_id->строка имени)."""
    distinct = set(choices.values())
    if len(distinct) == 1:
        return f"Все выбрали {NAMES[list(distinct)[0]]} — ничья, играем заново!"
    if len(distinct) == 3:
        return "На столе все три знака — ничья, играем заново!"
    a, b = list(distinct)
    winner_sign = a if BEATS[a] == b else b
    loser_sign = b if winner_sign == a else a
    return (f"{EMOJI[winner_sign]} {NAMES[winner_sign]} бьёт "
            f"{NAMES[loser_sign]} {EMOJI[loser_sign]}")
