"""
Морской бой: чистая логика (без БД и Telegram).
Поле 10x10. Корабли: 4 одиночных, 3 двойных, 2 тройных, 1 четверной.
Координаты: (row, col), 0..9. Буквы А-К = столбцы, цифры 1-10 = строки.
"""

SIZE = 10
# сколько кораблей каждой длины: длина -> количество
FLEET = {1: 4, 2: 3, 3: 2, 4: 1}
TOTAL_SHIP_CELLS = sum(length * count for length, count in FLEET.items())  # 20

# буквы столбцов (А-К без Ё, 10 штук)
COLS = ["А", "Б", "В", "Г", "Д", "Е", "Ж", "З", "И", "К"]


def col_letter(c: int) -> str:
    return COLS[c]


def letter_col(letter: str) -> int | None:
    letter = letter.upper()
    return COLS.index(letter) if letter in COLS else None


def new_field() -> dict:
    """Пустое состояние поля игрока.
      ships: список кораблей, каждый — список клеток [(r,c),...]
      shots_at_me: клетки, куда стрелял противник {(r,c): 'hit'|'miss'}
    """
    return {"ships": [], "shots_at_me": {}}


def all_ship_cells(field: dict) -> set:
    out = set()
    for ship in field["ships"]:
        out.update(tuple(c) for c in ship)
    return out


def _ck(cell) -> str:
    """Ключ клетки для JSON-совместимого словаря shots_at_me: 'r,c'."""
    return f"{cell[0]},{cell[1]}"


def _uck(key: str) -> tuple:
    """Обратно из ключа 'r,c' в (r,c)."""
    r, c = key.split(",")
    return (int(r), int(c))


def cells_adjacent(a: tuple, b: tuple) -> bool:
    """Соседние по вертикали/горизонтали (не по диагонали)."""
    dr = abs(a[0] - b[0])
    dc = abs(a[1] - b[1])
    return (dr == 1 and dc == 0) or (dr == 0 and dc == 1)


def ship_is_valid_line(cells: list) -> bool:
    """Клетки корабля образуют прямую линию (гориз/верт), смежные, без дыр."""
    if not cells:
        return False
    if len(cells) == 1:
        return True
    rows = {c[0] for c in cells}
    cols = {c[1] for c in cells}
    if len(rows) == 1:  # горизонтальный
        cs = sorted(c[1] for c in cells)
        return cs == list(range(cs[0], cs[0] + len(cs)))
    if len(cols) == 1:  # вертикальный
        rs = sorted(c[0] for c in cells)
        return rs == list(range(rs[0], rs[0] + len(rs)))
    return False


def can_place_cell(field: dict, cell: tuple, current_ship: list) -> tuple[bool, str]:
    """Можно ли добавить клетку cell к строящемуся кораблю current_ship.
    Проверяет: в границах, не занята, не касается ЧУЖИХ кораблей, смежна с текущим."""
    r, c = cell
    if not (0 <= r < SIZE and 0 <= c < SIZE):
        return False, "Клетка вне поля."
    occupied = all_ship_cells(field)
    if cell in occupied:
        return False, "Клетка уже занята."
    if tuple(cell) in [tuple(x) for x in current_ship]:
        return False, "Эта клетка уже в текущем корабле."
    # клетка не должна вплотную (включая диагонали) касаться ЧУЖИХ кораблей
    for dr in (-1, 0, 1):
        for dc in (-1, 0, 1):
            nb = (r + dr, c + dc)
            if nb in occupied:
                return False, "Слишком близко к другому кораблю."
    # если это не первая клетка корабля — должна быть смежна (гориз/верт) с одной из текущих
    if current_ship:
        if not any(cells_adjacent(cell, tuple(x)) for x in current_ship):
            return False, "Клетка должна примыкать к кораблю по горизонтали или вертикали."
        # и линия должна оставаться прямой
        test = [tuple(x) for x in current_ship] + [cell]
        if not ship_is_valid_line(test):
            return False, "Корабль должен быть прямой линией."
    return True, ""


def remaining_ships_to_place(field: dict) -> dict:
    """Сколько кораблей какой длины ещё осталось поставить."""
    placed = {}
    for ship in field["ships"]:
        L = len(ship)
        placed[L] = placed.get(L, 0) + 1
    remain = {}
    for L, cnt in FLEET.items():
        left = cnt - placed.get(L, 0)
        if left > 0:
            remain[L] = left
    return remain


def fleet_complete(field: dict) -> bool:
    """Все корабли расставлены."""
    return not remaining_ships_to_place(field)


def register_shot(target_field: dict, cell: tuple) -> str:
    """Выстрел по target_field в клетку cell.
    Возвращает 'miss' | 'hit' | 'sunk'. Обновляет shots_at_me цели."""
    cell = tuple(cell)
    ship_cells = all_ship_cells(target_field)
    if cell in ship_cells:
        target_field["shots_at_me"][_ck(cell)] = "hit"
        for ship in target_field["ships"]:
            sc = [tuple(x) for x in ship]
            if cell in sc:
                if all(target_field["shots_at_me"].get(_ck(x)) == "hit" for x in sc):
                    return "sunk"
                break
        return "hit"
    else:
        target_field["shots_at_me"][_ck(cell)] = "miss"
        return "miss"


def sunk_ship_cells(field: dict) -> set:
    """Клетки полностью потопленных кораблей."""
    out = set()
    for ship in field["ships"]:
        sc = [tuple(x) for x in ship]
        if all(field["shots_at_me"].get(_ck(x)) == "hit" for x in sc):
            out.update(sc)
    return out


def all_sunk(field: dict) -> bool:
    """Весь флот потоплен?"""
    cells = all_ship_cells(field)
    if not cells:
        return False
    return all(field["shots_at_me"].get(_ck(c)) == "hit" for c in cells)


# ---------------- рендер поля эмодзи ----------------
# дефолтные символы (игрок заменит на премиум через слоты)
EMO = {
    "water":  "🌊",   # вода / не стреляли
    "ship":   "🚢",   # свой целый корабль (только на «моём» поле)
    "hit":    "🔥",   # попадание
    "miss":   "💥",   # мимо
    "sunk":   "🪨",   # потопленный корабль
}


def render_my_field(field: dict, emo: dict = None) -> str:
    """«Мой флот»: показываем свои корабли + куда попал враг.
    Целый корабль — ship, попадание — hit, потопленный — sunk, промах врага — miss."""
    e = {**EMO, **(emo or {})}
    ship_cells = all_ship_cells(field)
    sunk = sunk_ship_cells(field)
    shots = field["shots_at_me"]
    return _render_grid(lambda r, c: (
        e["sunk"] if (r, c) in sunk else
        e["hit"] if shots.get(_ck((r, c))) == "hit" else
        e["miss"] if shots.get(_ck((r, c))) == "miss" else
        e["ship"] if (r, c) in ship_cells else
        e["water"]))


def render_shots_field(target_field: dict, emo: dict = None) -> str:
    """«Мои выстрелы» по противнику: только hit/miss/sunk, кораблей НЕ видно."""
    e = {**EMO, **(emo or {})}
    sunk = sunk_ship_cells(target_field)
    shots = target_field["shots_at_me"]
    return _render_grid(lambda r, c: (
        e["sunk"] if (r, c) in sunk else
        e["hit"] if shots.get(_ck((r, c))) == "hit" else
        e["miss"] if shots.get(_ck((r, c))) == "miss" else
        e["water"]))


def _render_grid(cell_fn) -> str:
    """Сетка 10x10 с подписями столбцов (А-К) и строк (1-10)."""
    # шапка столбцов
    header = "  " + "".join(COLS)
    lines = [header]
    for r in range(SIZE):
        row_label = str(r + 1).rjust(2)
        row = "".join(cell_fn(r, c) for c in range(SIZE))
        lines.append(f"{row_label}{row}")
    return "\n".join(lines)


# ---------------- пошаговое построение корабля (для UI расстановки) ----------------
def target_length_for(field: dict) -> int | None:
    """Какой корабль логично ставить следующим (самый длинный из оставшихся).
    Возвращает длину или None, если все расставлены."""
    remain = remaining_ships_to_place(field)
    if not remain:
        return None
    return max(remain.keys())


def can_start_ship_of_length(field: dict, length: int) -> bool:
    """Есть ли ещё нерасставленные корабли этой длины."""
    return remaining_ships_to_place(field).get(length, 0) > 0


def try_add_cell(field: dict, building: list, cell: tuple, target_len: int) -> tuple[bool, str]:
    """Добавить клетку к строящемуся кораблю building (длиной target_len).
    Возвращает (успех, ошибка). При успехе cell добавляется в building (мутирует)."""
    if len(building) >= target_len:
        return False, "Корабль уже нужной длины — подтверди или отмени."
    okc, err = can_place_cell(field, cell, building)
    if not okc:
        return False, err
    building.append(tuple(cell))
    return True, ""


def ship_ready(building: list, target_len: int) -> bool:
    """Строящийся корабль достиг нужной длины."""
    return len(building) == target_len


def commit_ship(field: dict, building: list):
    """Зафиксировать построенный корабль в поле."""
    field["ships"].append([tuple(c) for c in building])


def render_placement_field(field: dict, building: list, emo: dict = None) -> str:
    """Поле при расстановке: стоящие корабли + текущие клетки строящегося (мигают ship)."""
    e = {**EMO, **(emo or {})}
    ship_cells = all_ship_cells(field)
    building_set = {tuple(c) for c in building}
    return _render_grid(lambda r, c: (
        e["ship"] if (r, c) in ship_cells or (r, c) in building_set else
        e["water"]))
