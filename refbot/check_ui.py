"""
Страховка от самой частой ошибки в этом проекте.

Премиум-эмодзи работают только если сообщение ушло через services/ui.py.
Стоит написать msg.reply(...) напрямую — эмодзи в этом месте молча станут плоскими,
и заметишь ты это по скриншоту, а не по ошибке. Так уже терялись cbind, профиль
и половина админки.

Запуск: python check_ui.py   (или в CI)
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent

# где сырой API оправдан
ALLOWED = {
    "services/ui.py": "сам слой отправки",
    "services/render.py": "низкоуровневый рендер",
    "handlers/skin.py": "тест премиума: нужна настоящая ошибка Telegram, а не фолбэк",
}

BAD_SEND = re.compile(
    r"await\s+(?:msg|message|m|c\.message)\.(?:reply|answer|edit_text)\(|"
    r"await\s+(?:bot|c\.bot|msg\.bot)\.send_message\("
)
# async-клавиатуры, вызванные без await -> в Telegram уедет корутина
KB_FUNCS = ("main_menu", "wd_menu", "admin_menu", "chat_admin_list", "chat_card",
            "skin_menu", "slot_list", "slot_card", "back_menu", "chat_picker",
            "link_card", "confirm", "admin_wd_card", "free_list")
BAD_KB = re.compile(r"(?<!await )\bkb\.(?:" + "|".join(KB_FUNCS) + r")\(")


def main() -> int:
    problems = []
    for f in sorted(list(ROOT.glob("handlers/*.py")) + list(ROOT.glob("services/*.py"))
                    + [ROOT / "main.py", ROOT / "keyboards.py"]):
        rel = f.relative_to(ROOT).as_posix()
        text = f.read_text()
        for i, line in enumerate(text.splitlines(), 1):
            # строка с «# noqa: ui» — намеренная прямая отправка (напр. пост в
            # КАНАЛ от имени канала, где премиум и не должен работать)
            if "# noqa: ui" in line:
                continue
            if rel not in ALLOWED and BAD_SEND.search(line):
                problems.append(f"{rel}:{i} отправка в обход ui — премиум не сработает\n"
                                f"    {line.strip()}")
            if BAD_KB.search(line):
                problems.append(f"{rel}:{i} клавиатура без await — уедет корутина\n"
                                f"    {line.strip()}")

    if problems:
        print("НАЙДЕНЫ ПРОБЛЕМЫ:\n")
        print("\n".join(problems))
        return 1
    print(f"OK — все отправки идут через ui, все клавиатуры awaited")
    print("исключения:", ", ".join(f"{k} ({v})" for k, v in ALLOWED.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
