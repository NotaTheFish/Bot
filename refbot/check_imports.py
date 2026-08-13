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


def check_undefined_names(path: str) -> list[str]:
    """
    Ловит использование частых имён-объектов (msg/c/callback...) там, где их нет ни
    в аргументах функции, ни в присваиваниях. Классика при копипасте между
    message- и callback-хендлерами (был баг: msg.from_user в функции с c: CallbackQuery).
    """
    import ast
    SUSPECTS = {"msg", "c", "callback", "query"}
    try:
        tree = ast.parse(open(path).read())
    except SyntaxError:
        return []
    out = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            local = {a.arg for a in node.args.args}
            local |= {a.arg for a in node.args.kwonlyargs}
            for n in ast.walk(node):
                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store):
                    local.add(n.id)
                if isinstance(n, ast.arg):
                    local.add(n.arg)
            for n in ast.walk(node):
                if (isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
                        and n.id in SUSPECTS and n.id not in local):
                    out.append(f"{path}:{n.lineno} — '{n.id}' в {node.name}() не определён "
                               f"(перепутан msg/c?)")
    return out


def main() -> int:
    problems = []
    for root, _, files in os.walk("."):
        if "__pycache__" in root:
            continue
        for fn in files:
            if fn.endswith(".py"):
                p = os.path.join(root, fn)
                problems += check_file(p)
                problems += check_undefined_names(p)
    if problems:
        print("НАЙДЕНЫ проблемы:\n")
        print("\n".join(sorted(set(problems))))
        return 1
    print("✔ импорты и имена — чисто")
    return 0


if __name__ == "__main__":
    sys.exit(main())
