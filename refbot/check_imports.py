#!/usr/bin/env python3
"""
Ловит обращения к модулям, которые не импортированы — источник NameError в рантайме,
который py_compile НЕ ловит (компиляция проходит, падает при вызове).

Три раза уже прилетало: contextlib в giveaway, contextlib в user, datetime в admin.
Гоняй перед каждой сборкой: python3 check_imports.py
"""
import ast
import builtins
import os
import sys

BUILTIN = set(dir(builtins)) | {"self", "cls", "__import__"}

# частые локальные переменные-объекты (не модули) — чтобы не шумело
LOCAL_HINTS = {
    "msg", "c", "state", "g", "gw", "ch", "kb", "kbd", "db", "ui", "m", "u", "r", "s",
    "w", "e", "t", "p", "x", "i", "n", "b", "kw", "cur", "act", "row", "data", "bot",
    "conn", "sx", "em", "link", "note", "chat", "user", "chats", "rows", "result",
    "command", "wu", "pool", "args", "text", "ents", "card", "frame", "label", "amount",
    "prize", "prizes", "winners", "total", "missing", "targets", "lines", "reply",
    "event", "me", "event_from_user", "wd", "gid", "uid", "tg", "cid", "mid", "res",
}


def check_file(path: str) -> list[str]:
    src = open(path).read()
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return []

    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                imported.add((a.asname or a.name).split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                imported.add(a.asname or a.name)

    defined = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(node.name)
        if isinstance(node, ast.arg):
            defined.add(node.arg)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            defined.add(node.id)

    out = []
    seen = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            name = node.value.id
            if (name not in imported and name not in defined
                    and name not in BUILTIN and name not in LOCAL_HINTS
                    and len(name) > 2 and name.islower()):
                key = f"{name}.{node.attr}"
                if key not in seen:
                    seen.add(key)
                    out.append(f"{path}:{node.lineno} — '{key}' (модуль '{name}' не импортирован?)")
    return out


def main() -> int:
    problems = []
    for root, _, files in os.walk("."):
        if "__pycache__" in root:
            continue
        for fn in files:
            if fn.endswith(".py"):
                problems += check_file(os.path.join(root, fn))
    if problems:
        print("НАЙДЕНЫ обращения к возможно-неимпортированным модулям:\n")
        print("\n".join(problems))
        return 1
    print("✔ импорты — чисто, все модульные обращения разрешаются")
    return 0


if __name__ == "__main__":
    sys.exit(main())
