"""
Крестики-нолики: чистая логика доски (без БД и Telegram).
Доска — список из 9 строк: "" (пусто), "X" или "O". Клетки 0..8:
  0 1 2
  3 4 5
  6 7 8
"""
import random

WIN_LINES = [
    (0, 1, 2), (3, 4, 5), (6, 7, 8),   # ряды
    (0, 3, 6), (1, 4, 7), (2, 5, 8),   # столбцы
    (0, 4, 8), (2, 4, 6),              # диагонали
]


def empty_board() -> list[str]:
    return [""] * 9


def new_game(p1: int, p2: int) -> dict:
    """
    Новое состояние. Символы: создатель p1 — рандомно X или O, p2 — другой.
    Первый ход — рандомный игрок. Возвращает dict для сохранения в match.state.
    """
    p1_is_x = random.choice([True, False])
    first = random.choice([p1, p2])
    return {
        "board": empty_board(),
        "x": p1 if p1_is_x else p2,   # кто играет X
        "o": p2 if p1_is_x else p1,   # кто играет O
        "first": first,
    }


def symbol_of(state: dict, uid: int) -> str:
    """Символ игрока: 'X' или 'O'."""
    return "X" if state["x"] == uid else "O"


def apply_move(state: dict, uid: int, cell: int) -> tuple[bool, str]:
    """
    Поставить символ игрока uid в клетку cell. Меняет state["board"] на месте.
    Возвращает (успех, ошибка). Не проверяет очередь — это делает вызывающий.
    """
    board = state["board"]
    if cell < 0 or cell > 8:
        return False, "Неверная клетка."
    if board[cell]:
        return False, "Клетка занята."
    board[cell] = symbol_of(state, uid)
    return True, ""


def winner(state: dict) -> int | None:
    """
    tg_id победителя, 0 при ничьей (доска полна без победы), None если игра идёт.
    """
    board = state["board"]
    for a, b, c in WIN_LINES:
        if board[a] and board[a] == board[b] == board[c]:
            sym = board[a]
            return state["x"] if sym == "X" else state["o"]
    if all(board):   # доска полна, победителя нет
        return 0
    return None


def win_line(state: dict) -> tuple | None:
    """Выигрышная тройка клеток (для подсветки) или None."""
    board = state["board"]
    for line in WIN_LINES:
        a, b, c = line
        if board[a] and board[a] == board[b] == board[c]:
            return line
    return None


def other(state: dict, uid: int) -> int:
    """Соперник uid в этой игре."""
    return state["o"] if state["x"] == uid else state["x"]


# ---------------- рендер поля ----------------
CELL_EMPTY = "·"
SYM = {"X": "❌", "O": "⭕️", "": CELL_EMPTY}


def board_kb(state: dict, match_id: int, win=None):
    """Инлайн-клавиатура 3x3. Пустые клетки кликабельны (ttt_mv:<mid>:<cell>),
    занятые — показывают символ (клик игнорируется хендлером)."""
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    kb = InlineKeyboardBuilder()
    board = state["board"]
    for i in range(9):
        if board[i]:
            label = SYM[board[i]]
        else:
            label = CELL_EMPTY
        kb.button(text=label, callback_data=f"ttt_mv:{match_id}:{i}")
    kb.adjust(3, 3, 3)
    return kb.as_markup()
