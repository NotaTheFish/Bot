"""
Умный разбор сумм из текста админа: «1к» -> 1000, «1.5м» -> 1500000,
«100000», «1 000 000» (пробелы), «100k», «2M». Для призов розыгрыша.

parse_amount("1.5м") -> 1_500_000
parse_amount("100к")  -> 100_000
parse_amount("2 000") -> 2000
parse_amount("абв")   -> None
"""
import re

# множители суффиксов (рус + англ, регистр не важен)
_SUFFIX = {
    "к": 1_000, "k": 1_000, "тыс": 1_000,
    "м": 1_000_000, "m": 1_000_000, "млн": 1_000_000,
    "кк": 1_000_000,  # игровой сленг: «кк» = миллион
    "б": 1_000_000_000, "b": 1_000_000_000, "млрд": 1_000_000_000,
}


def parse_amount(text: str) -> int | None:
    """
    Разобрать одну сумму. None, если не число.
    Понимает: пробелы-разделители, запятую как точку, суффиксы к/к/м/kk и т.д.
    """
    if not text:
        return None
    s = text.strip().lower().replace(",", ".").replace(" ", "").replace("_", "")

    # суффикс из букв в конце (самый длинный совпавший)
    mult = 1
    for suf in sorted(_SUFFIX, key=len, reverse=True):
        if s.endswith(suf):
            mult = _SUFFIX[suf]
            s = s[: -len(suf)]
            break

    if not s:
        return None
    try:
        val = float(s)
    except ValueError:
        return None
    if val < 0:
        return None
    result = int(round(val * mult))
    return result if result > 0 else None


def split_equally(total: int, places: int) -> list[int]:
    """
    Распределить total на places мест поровну, округляя ВНИЗ.
    Остаток сгорает (по решению: не отдаём никому). Возвращает список призов.
    split_equally(100000, 3) -> [33333, 33333, 33333]  (остаток 1 сгорел)
    """
    if places <= 0:
        return []
    each = total // places
    return [each] * places


def fmt_amount(n: int) -> str:
    """Человеческий вид: 1500000 -> «1 500 000». Для показа админу и в постах."""
    return f"{n:,}".replace(",", " ")


# ==================== ШИМКОИНЫ В ЦЕНТАХ ====================
# Шимкоин = доллар, дробный до 2 знаков (центов). Храним ВЕЗДЕ как целое число
# ЦЕНТОВ (BIGINT): 154396787.53 ШК -> 15439678753 центов. Никаких float в хранении
# и расчётах баланса — только целые центы. Грибы/коины НЕ в центах (целые единицы).
#
# Единственные точки перевода — эти хелперы. В коде не должно быть «голого» /100.

SHK_CENTS = 100  # центов в одном шимкоине


def shk_parse(text: str) -> int | None:
    """
    Ввод шимкоинов -> центы (int). Понимает '20', '20.50', '20,50', '20 $', '$20',
    пробелы и суффиксы к/м (через parse_amount для целой части сумм вроде '1к$').
    Возвращает число ЦЕНТОВ или None.
      '20'    -> 2000
      '20.50' -> 2050
      '0.01'  -> 1
      '1к'    -> 100000  (1000 ШК = 100000 центов)
    """
    if not text:
        return None
    s = text.strip().lower().replace("$", "").replace(",", ".").replace(" ", "").replace("_", "")
    if not s:
        return None
    # суффиксы (к/м/…): если есть — это целые шимкоины, множим и переводим в центы
    for suf in sorted(_SUFFIX, key=len, reverse=True):
        if s.endswith(suf):
            base = s[: -len(suf)]
            try:
                val = float(base)
            except ValueError:
                return None
            if val < 0:
                return None
            cents = int(round(val * _SUFFIX[suf] * SHK_CENTS))
            return cents if cents > 0 else None
    # без суффикса — обычное дробное число шимкоинов
    try:
        val = float(s)
    except ValueError:
        return None
    if val < 0:
        return None
    cents = int(round(val * SHK_CENTS))
    return cents if cents > 0 else None


def shk_fmt(cents: int) -> str:
    """
    Центы -> строка для показа: '15439678753' -> '154 396 787.53'.
    Всегда 2 знака после точки (это деньги). Отрицательные — со знаком.
    """
    neg = cents < 0
    cents = abs(int(cents))
    whole, cc = divmod(cents, SHK_CENTS)
    whole_s = f"{whole:,}".replace(",", " ")
    return f"{'-' if neg else ''}{whole_s}.{cc:02d}"


def shk_to_float(cents: int) -> float:
    """Центы -> шимкоины как float. Только для отображения/расчёта курса, НЕ для хранения."""
    return cents / SHK_CENTS
