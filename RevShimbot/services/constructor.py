"""
Конструктор карточек — рендер с динамическими раскладками, шрифтами, цветами.
"""
import asyncio
import base64
from datetime import datetime
import pytz

MSK = pytz.timezone("Europe/Moscow")


# ── Опции конструктора ─────────────────────────────────────────────────────

LAYOUTS = {
    "classic":     "📐 Классика — всё по центру",
    "header_user": "👤 Юзер сверху — аватар и ник в шапке",
    "left_align":  "⬅️ Слева — текст выровнен по левому краю",
    "compact":     "📦 Компактная — минимум отступов",
    "magazine":    "📰 Журнал — крупный заголовок",
    "split":       "🔀 Разделённая — юзер и товар по краям",
    "minimal":     "▫️ Минимал — только текст и подпись",
}

FONTS = {
    "montserrat":  ("🔤 Montserrat", "'Montserrat',sans-serif"),
    "playfair":    ("📖 Playfair Display", "'Playfair Display',serif"),
    "caveat":      ("✍️ Caveat", "'Caveat',cursive"),
    "spacegrotesk":("🚀 Space Grotesk", "'Space Grotesk',sans-serif"),
    "inter":       ("📰 Inter", "'Inter',sans-serif"),
    "oswald":      ("📐 Oswald", "'Oswald',sans-serif"),
    "comfortaa":   ("🫧 Comfortaa", "'Comfortaa',cursive"),
    "vt323":       ("🎮 Pixel (VT323)", "'VT323',monospace"),
    "cinzel":      ("🏛 Cinzel (англ.)", "'Cinzel',serif"),
    "jetbrains":   ("💻 JetBrains Mono", "'JetBrains Mono',monospace"),
    "bebas":       ("🅱️ Bebas Neue (англ.)", "'Bebas Neue',sans-serif"),
}

TEXT_COLORS = {
    "white":  ("⚪️ Белый", "#f0f0f0"),
    "cream":  ("🍦 Кремовый", "#f5e9c8"),
    "gold":   ("🟡 Золотой", "#ffd95a"),
    "amber":  ("🟠 Янтарный", "#ffb86c"),
    "cyan":   ("🔵 Голубой", "#7ad6ff"),
    "sky":    ("💙 Небесный", "#9bb8ff"),
    "green":  ("🟢 Зелёный", "#7ee787"),
    "mint":   ("🌿 Мятный", "#9af2cf"),
    "pink":   ("🩷 Розовый", "#ff9ec4"),
    "rose":   ("🌹 Розовый тёмный", "#ff7a9c"),
    "purple": ("🟣 Фиолетовый", "#c9a4ff"),
    "silver": ("🩶 Серебро", "#c8ccd4"),
}

ACCENT_COLORS = {
    "gold":   ("🟡 Золото", "#c9a84c"),
    "orange": ("🟠 Оранжевый", "#ff7a1a"),
    "amber":  ("🍯 Янтарь", "#f0a500"),
    "blue":   ("🔵 Синий", "#58a6ff"),
    "cyan":   ("🩵 Бирюзовый", "#2ad4d4"),
    "green":  ("🟢 Зелёный", "#3fb950"),
    "lime":   ("🟢 Лайм", "#7ee83f"),
    "pink":   ("🩷 Розовый", "#ff7eb6"),
    "magenta":("💗 Маджента", "#ff2e97"),
    "purple": ("🟣 Фиолет", "#a371f7"),
    "red":    ("🔴 Красный", "#ff6b6b"),
    "crimson":("❤️ Багровый", "#e01e5a"),
    "white":  ("⚪️ Белый", "#f5f5f5"),
    "black":  ("⚫️ Чёрный", "#1a1a1a"),
    "silver": ("🩶 Серебро", "#b8bcc4"),
}

BG_COLORS = {
    "dark_blue":   ("🌌 Тёмно-синий", "#1a1a2e"),
    "charcoal":    ("⚫️ Уголь", "#161b22"),
    "black":       ("🖤 Чёрный", "#0a0a0a"),
    "deep_purple": ("🟪 Глубокий фиолет", "#1e1633"),
    "forest":      ("🌲 Тёмный лес", "#15201a"),
    "wine":        ("🍷 Винный", "#2a1520"),
    "slate":       ("🪨 Сланец", "#1c2128"),
    "navy":        ("⚓️ Морской", "#0d1b2a"),
    "espresso":    ("☕️ Эспрессо", "#1c1410"),
    "midnight_green":("🌑 Тёмно-зелёный", "#0a1414"),
    "plum":        ("🍇 Слива", "#1a0f1f"),
    "graphite":    ("✏️ Графит", "#18181b"),
}

# ── Этап 1: расширенные настройки ──────────────────────────────────────────

# Градиентные фоны: (label, цвет1, цвет2, угол)
BG_GRADIENTS = {
    "none":        ("⬜️ Без градиента", None, None, None),
    "midnight":    ("🌃 Полночь", "#232850", "#0a0814", 180),
    "ocean":       ("🌊 Океан", "#0f2027", "#2c5364", 160),
    "sunset":      ("🌅 Закат", "#3a1c2e", "#6d2b3f", 160),
    "emerald":     ("💚 Изумруд", "#0a2818", "#1d5c3a", 180),
    "royal":       ("👑 Королевский", "#1e1633", "#4a2d7e", 160),
    "ember":       ("🔥 Угли", "#1a1010", "#5e2810", 180),
    "steel":       ("⚙️ Сталь", "#1c2128", "#454d5a", 160),
}

# Рамка карточки: (label, css-border, css-extra)
CARD_BORDERS = {
    "none":     ("⬜️ Без рамки", "none", ""),
    "thin":     ("➖ Тонкая", "1px solid {accent}", ""),
    "medium":   ("➗ Средняя", "2px solid {accent}", ""),
    "double":   ("🟰 Двойная", "3px double {accent}", ""),
    "glow":     ("✨ Свечение", "1px solid {accent}", "box-shadow:0 0 24px {accent}66, inset 0 0 12px {accent}22;"),
    "inset":    ("🔲 Внутренняя", "none", "box-shadow:inset 0 0 0 2px {accent}88;"),
}

# Скругление углов карточки
CARD_RADIUS = {
    "sharp":    ("⬛️ Острые", "0px"),
    "soft":     ("🔲 Мягкие", "12px"),
    "rounded":  ("🟦 Скруглённые", "24px"),
    "pill":     ("⭕️ Очень круглые", "36px"),
}

# Тень карточки
CARD_SHADOW = {
    "none":     ("⬜️ Без тени", ""),
    "soft":     ("🌫 Мягкая", "box-shadow:0 10px 40px rgba(0,0,0,0.55);"),
    "hard":     ("⬛️ Жёсткая", "box-shadow:14px 14px 0 rgba(0,0,0,0.35);"),
    "glowacc":  ("✨ Цветная", "box-shadow:0 0 60px {accent}88;"),
}

# Заливка углов (фон-подложка вокруг карточки когда углы скруглены)
CORNER_FILL = {
    "match":    ("🎯 Под фон карточки", "__match__"),
    "black":    ("⬛️ Чёрный", "#000000"),
    "white":    ("⬜️ Белый", "#ffffff"),
    "dark":     ("🌑 Тёмно-серый", "#0e0e14"),
    "telegram": ("💠 Фон Telegram", "#17212b"),
}

# Размер текста отзыва
TEXT_SIZES = {
    "small":   ("🔹 Мелкий", 14, 1.7),
    "medium":  ("🔸 Средний", 16, 1.8),
    "large":   ("🔶 Крупный", 19, 1.9),
}

# Дефолты для тумблеров видимости
VISIBILITY_DEFAULTS = {
    "show_date": True,
    "show_item": True,
    "show_avatar": True,
    "show_seller_tag": True,
    "show_quote": True,
}

# ── Этап 2: типографика и текстуры ─────────────────────────────────────────

# Эффекты заголовка магазина. Значение — функция от accent → CSS.
TITLE_EFFECTS = {
    "none":     ("⬜️ Обычный", ""),
    "outline":  ("🅾️ Обводка", "-webkit-text-stroke:1.5px {accent};paint-order:stroke fill;"),
    "neon":     ("💡 Неон", "text-shadow:0 0 6px {accent},0 0 14px {accent},0 0 28px {accent};"),
    "glow_soft":("🌟 Мягкое свечение", "text-shadow:0 0 18px {accent}99;"),
    "shadow3d": ("🎲 3D-тень", "text-shadow:2px 2px 0 rgba(0,0,0,0.5),4px 4px 0 {accent}55;"),
    "gradient": ("🌈 Градиентная заливка", "__gradient__"),
    "engrave":  ("🪨 Тиснение", "text-shadow:0 1px 0 rgba(255,255,255,0.25),0 -1px 0 rgba(0,0,0,0.6);"),
}

# Текстуры фона — накладываются ПОВЕРХ фона (цвет/градиент). CSS background-image слои.
BG_TEXTURES = {
    "none":     ("⬜️ Без текстуры", ""),
    "grid":     ("▦ Сетка", "linear-gradient({tc}11 1px,transparent 1px),linear-gradient(90deg,{tc}11 1px,transparent 1px)|22px 22px"),
    "dots":     ("⠿ Точки", "radial-gradient({tc}18 1.4px,transparent 1.4px)|22px 22px"),
    "diagonal": ("╱ Диагонали", "repeating-linear-gradient(45deg,{tc}0a 0,{tc}0a 1px,transparent 1px,transparent 12px)|auto"),
    "crosshatch":("▨ Штриховка", "repeating-linear-gradient(45deg,{tc}0a 0,{tc}0a 1px,transparent 1px,transparent 10px),repeating-linear-gradient(-45deg,{tc}0a 0,{tc}0a 1px,transparent 1px,transparent 10px)|auto"),
    "vignette": ("🎯 Виньетка", "radial-gradient(ellipse at center,transparent 55%,rgba(0,0,0,0.45) 100%)|auto"),
    "topglow":  ("🔆 Свет сверху", "radial-gradient(ellipse at top,{ac}22,transparent 60%)|auto"),
}


# ── Этап 3: пресеты-скины (готовые стили в один тап) ───────────────────────
# Каждый пресет — полный рецепт: базовые поля + extra_cfg.
PRESETS = {
    "cyberpunk": {
        "label": "🌃 Киберпанк",
        "base": {"layout": "classic", "font": "jetbrains", "title_font": "vt323",
                 "text_color": "#7ee787", "accent_color": "#ff2e97", "bg_color": "#0d0117"},
        "extra": {"bg_gradient": "royal", "card_border": "glow", "card_radius": "soft",
                  "card_shadow": "glowacc", "text_size": "medium", "corner_fill": "match",
                  "title_effect": "neon", "bg_texture": "grid"},
    },
    "premium_gold": {
        "label": "👑 Премиум золото",
        "base": {"layout": "classic", "font": "playfair", "title_font": "cinzel",
                 "text_color": "#f5e9c8", "accent_color": "#d4af37", "bg_color": "#15110a"},
        "extra": {"bg_gradient": "ember", "card_border": "double", "card_radius": "soft",
                  "card_shadow": "soft", "text_size": "medium", "corner_fill": "black",
                  "title_effect": "gradient", "bg_texture": "topglow"},
    },
    "roblox": {
        "label": "🎮 Roblox",
        "base": {"layout": "compact", "font": "montserrat", "title_font": "vt323",
                 "text_color": "#ffffff", "accent_color": "#00a2ff", "bg_color": "#0a1929"},
        "extra": {"bg_gradient": "ocean", "card_border": "medium", "card_radius": "rounded",
                  "card_shadow": "hard", "text_size": "medium", "corner_fill": "match",
                  "title_effect": "outline", "bg_texture": "dots"},
    },
    "minimal_white": {
        "label": "⬜️ Минимал-светлый",
        "base": {"layout": "left_align", "font": "inter", "title_font": "spacegrotesk",
                 "text_color": "#1a1a2e", "accent_color": "#2d6cdf", "bg_color": "#f4f5f7"},
        "extra": {"bg_gradient": "none", "card_border": "thin", "card_radius": "soft",
                  "card_shadow": "soft", "text_size": "medium", "corner_fill": "white",
                  "title_effect": "none", "bg_texture": "none"},
    },
    "neon_night": {
        "label": "💜 Неоновая ночь",
        "base": {"layout": "classic", "font": "spacegrotesk", "title_font": "bebas",
                 "text_color": "#e0d6ff", "accent_color": "#b14dff", "bg_color": "#0f0a1e"},
        "extra": {"bg_gradient": "royal", "card_border": "glow", "card_radius": "rounded",
                  "card_shadow": "glowacc", "text_size": "large", "corner_fill": "match",
                  "title_effect": "neon", "bg_texture": "none"},
    },
    "retro_terminal": {
        "label": "💻 Ретро-терминал",
        "base": {"layout": "left_align", "font": "jetbrains", "title_font": "vt323",
                 "text_color": "#33ff66", "accent_color": "#33ff66", "bg_color": "#020a02"},
        "extra": {"bg_gradient": "none", "card_border": "medium", "card_radius": "sharp",
                  "card_shadow": "none", "text_size": "medium", "corner_fill": "match",
                  "title_effect": "glow_soft", "bg_texture": "grid"},
    },
    "elegant_dark": {
        "label": "🖤 Элегантный тёмный",
        "base": {"layout": "magazine", "font": "playfair", "title_font": "playfair",
                 "text_color": "#e8e8ec", "accent_color": "#c9a84c", "bg_color": "#121214"},
        "extra": {"bg_gradient": "steel", "card_border": "thin", "card_radius": "soft",
                  "card_shadow": "soft", "text_size": "medium", "corner_fill": "dark",
                  "title_effect": "engrave", "bg_texture": "vignette"},
    },
    "ice_frost": {
        "label": "❄️ Ледяной",
        "base": {"layout": "classic", "font": "comfortaa", "title_font": "comfortaa",
                 "text_color": "#dff3ff", "accent_color": "#5fd4ff", "bg_color": "#0a1822"},
        "extra": {"bg_gradient": "ocean", "card_border": "glow", "card_radius": "rounded",
                  "card_shadow": "glowacc", "text_size": "medium", "corner_fill": "match",
                  "title_effect": "glow_soft", "bg_texture": "topglow"},
    },
}


# ── Этап 4: декоративные элементы ──────────────────────────────────────────

# Бейдж «Проверенный продавец» — варианты вида
VERIFIED_BADGES = {
    "none":     ("⬜️ Без бейджа", ""),
    "check":    ("✅ Галочка", "✓ Проверенный продавец"),
    "shield":   ("🛡 Щит", "🛡 Verified Seller"),
    "star":     ("⭐️ Звезда", "★ Trusted"),
    "crown":    ("👑 Корона", "👑 Premium"),
    "diamond":  ("💎 Алмаз", "💎 VIP"),
}

# Угловые орнаменты карточки
CORNER_ORNAMENTS = {
    "none":     ("⬜️ Без орнамента", ""),
    "lines":    ("📐 Уголки-линии", "lines"),
    "brackets": ("⌜ Скобки", "brackets"),
    "dots":     ("⠿ Точки", "dots"),
    "double":   ("⌟ Двойные", "double"),
}





FONT_LINKS = (
    '<link href="https://fonts.googleapis.com/css2?'
    'family=Montserrat:wght@400;600;700;800&'
    'family=Playfair+Display:wght@400;700&'
    'family=Caveat:wght@600;700&'
    'family=Space+Grotesk:wght@400;600&'
    'family=Inter:wght@300;400;600&'
    'family=Oswald:wght@400;600&'
    'family=Comfortaa:wght@400;700&display=swap" rel="stylesheet">'
    '<link href="https://fonts.googleapis.com/css2?family=VT323&display=swap" rel="stylesheet">'
    '<link href="https://fonts.googleapis.com/css2?family=Cinzel:wght@400;700;900&display=swap" rel="stylesheet">'
    '<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">'
    '<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&display=swap" rel="stylesheet">'
)


def _date_msk() -> str:
    months = ["января","февраля","марта","апреля","мая","июня",
              "июля","августа","сентября","октября","ноября","декабря"]
    now = datetime.now(MSK)
    return f"{now.day} {months[now.month-1]} {now.year} г."


def _stars(n: int) -> str:
    return "★" * n + "☆" * (5 - n)


def _watermark_html(creator_username, is_edited: bool) -> str:
    """Водяной знак создателя — только если есть username и шаблон не отредактирован."""
    if creator_username and not is_edited:
        return f'<div class="creator-wm">шаблон @{creator_username}</div>'
    return ""


# ── Генератор HTML по конфигу ──────────────────────────────────────────────

def _build_ornament(style: str, accent: str) -> str:
    """Генерирует HTML угловых орнаментов карточки."""
    if not style:
        return ""
    base = ("position:absolute;width:28px;height:28px;pointer-events:none;")
    if style == "lines":
        css = {
            "tl": f"top:14px;left:14px;border-top:2px solid {accent};border-left:2px solid {accent};",
            "tr": f"top:14px;right:14px;border-top:2px solid {accent};border-right:2px solid {accent};",
            "bl": f"bottom:14px;left:14px;border-bottom:2px solid {accent};border-left:2px solid {accent};",
            "br": f"bottom:14px;right:14px;border-bottom:2px solid {accent};border-right:2px solid {accent};",
        }
    elif style == "brackets":
        css = {
            "tl": f"top:12px;left:12px;border-top:3px solid {accent};border-left:3px solid {accent};border-top-left-radius:6px;",
            "tr": f"top:12px;right:12px;border-top:3px solid {accent};border-right:3px solid {accent};border-top-right-radius:6px;",
            "bl": f"bottom:12px;left:12px;border-bottom:3px solid {accent};border-left:3px solid {accent};border-bottom-left-radius:6px;",
            "br": f"bottom:12px;right:12px;border-bottom:3px solid {accent};border-right:3px solid {accent};border-bottom-right-radius:6px;",
        }
    elif style == "double":
        css = {
            "tl": f"top:14px;left:14px;border-top:1px solid {accent};border-left:1px solid {accent};box-shadow:inset 4px 4px 0 -3px {accent};",
            "tr": f"top:14px;right:14px;border-top:1px solid {accent};border-right:1px solid {accent};box-shadow:inset -4px 4px 0 -3px {accent};",
            "bl": f"bottom:14px;left:14px;border-bottom:1px solid {accent};border-left:1px solid {accent};box-shadow:inset 4px -4px 0 -3px {accent};",
            "br": f"bottom:14px;right:14px;border-bottom:1px solid {accent};border-right:1px solid {accent};box-shadow:inset -4px -4px 0 -3px {accent};",
        }
    elif style == "dots":
        d = f"position:absolute;width:6px;height:6px;border-radius:50%;background:{accent};pointer-events:none;"
        return (f'<span style="{d}top:16px;left:16px;"></span>'
                f'<span style="{d}top:16px;right:16px;"></span>'
                f'<span style="{d}bottom:16px;left:16px;"></span>'
                f'<span style="{d}bottom:16px;right:16px;"></span>')
    else:
        return ""
    return "".join(f'<span style="{base}{v}"></span>' for v in css.values())


def build_html(cfg: dict, data: dict) -> str:
    layout = cfg.get("layout", "classic")
    font_key = cfg.get("font", "montserrat")
    _, font_css = FONTS.get(font_key, FONTS["montserrat"])
    title_font_key = cfg.get("title_font", "caveat")
    _, title_font_css = FONTS.get(title_font_key, FONTS["caveat"])
    text_color = cfg.get("text_color", "#e0e0e0")
    accent = cfg.get("accent_color", "#c9a84c")
    bg_color = cfg.get("bg_color", "#1a1a2e")
    bg_image = cfg.get("bg_image")

    # ── Этап 1: расширенные настройки из extra_cfg ────────────────────────
    ex = cfg.get("extra_cfg") or {}
    vis = {**VISIBILITY_DEFAULTS, **{k: ex.get(k) for k in VISIBILITY_DEFAULTS if k in ex}}

    # Размер текста
    _ts_key = ex.get("text_size", "medium")
    _, review_fs, review_lh = TEXT_SIZES.get(_ts_key, TEXT_SIZES["medium"])

    # Рамка карточки
    _bd_key = ex.get("card_border", "none")
    _, bd_border_tpl, bd_extra_tpl = CARD_BORDERS.get(_bd_key, CARD_BORDERS["none"])
    card_border = bd_border_tpl.format(accent=accent) if bd_border_tpl != "none" else "none"
    card_border_extra = bd_extra_tpl.format(accent=accent)

    # Скругление
    _rd_key = ex.get("card_radius", "sharp")
    _, card_radius = CARD_RADIUS.get(_rd_key, CARD_RADIUS["sharp"])

    # Предварительно резолвим градиент (нужен для corner_fill и фона)
    _grad_key = ex.get("bg_gradient", "none")
    _grad_pre = BG_GRADIENTS.get(_grad_key, BG_GRADIENTS["none"])

    # Тень
    _sh_key = ex.get("card_shadow", "none")
    _, sh_tpl = CARD_SHADOW.get(_sh_key, CARD_SHADOW["none"])
    card_shadow = sh_tpl.format(accent=accent)

    # Заливка углов (подложка) — важно когда углы скруглены, иначе Telegram заливает белым
    _cf_key = ex.get("corner_fill", "match")
    _, _cf_val = CORNER_FILL.get(_cf_key, CORNER_FILL["match"])
    if _cf_val == "__match__":
        # Под фон карточки: берём bg_color, либо первый цвет градиента, либо тёмный
        if bg_image:
            corner_fill = "#0e0e14"
        elif _grad_pre and _grad_pre[1]:
            corner_fill = _grad_pre[1]
        else:
            corner_fill = bg_color
    else:
        corner_fill = _cf_val

    # Подложка нужна только если есть скругление, тень или внешняя рамка-свечение
    _needs_pad = (_rd_key != "sharp") or (_sh_key != "none") or (_bd_key == "glow")
    frame_pad = "28px" if _needs_pad else "0px"

    import html as _html
    stars = _stars(data["stars"]) if data.get("stars", 0) > 0 else ""
    shop = _html.escape(str(data["shop_name"]))
    seller_tag = _html.escape(str(data.get("seller_tag", ""))) if vis["show_seller_tag"] else ""
    review = data["review_text"]
    name = _html.escape(str(data["buyer_name"]))
    initials = _html.escape(str(data["buyer_initials"]))
    item = _html.escape(str(data.get("item_bought", ""))) if vis["show_item"] else ""
    date = _date_msk() if vis["show_date"] else ""
    creator_wm = _watermark_html(cfg.get("creator_username"), cfg.get("is_edited", False))
    bot_username = data.get("bot_username", "reviewbot")

    # Фон — картинка > градиент > цвет
    _grad = _grad_pre
    if bg_image:
        bg_style = (f"background-image:linear-gradient(180deg,rgba(0,0,0,0.55),rgba(0,0,0,0.85)),"
                    f"url('data:image/png;base64,{bg_image}');background-size:cover;background-position:center;")
    elif _grad[1]:
        bg_style = f"background:linear-gradient({_grad[3]}deg,{_grad[1]},{_grad[2]});"
    else:
        bg_style = f"background:{bg_color};"

    # ── Этап 2: текстура фона (накладывается слоем поверх) ────────────────
    _tx_key = ex.get("bg_texture", "none")
    _, _tx_tpl = BG_TEXTURES.get(_tx_key, BG_TEXTURES["none"])
    texture_layer = ""
    if _tx_tpl:
        tc = text_color.lstrip("#")
        ac = accent.lstrip("#")
        spec = _tx_tpl.replace("{tc}", "#" + tc).replace("{ac}", "#" + ac)
        img_part, _, size_part = spec.partition("|")
        texture_layer = f".card::before{{content:'';position:absolute;inset:0;pointer-events:none;z-index:0;background-image:{img_part};"
        if size_part and size_part != "auto":
            texture_layer += f"background-size:{size_part};"
        texture_layer += "}.card>*{position:relative;z-index:1;}"

    # ── Этап 2: эффект заголовка ──────────────────────────────────────────
    _te_key = ex.get("title_effect", "none")
    _, _te_tpl = TITLE_EFFECTS.get(_te_key, TITLE_EFFECTS["none"])
    title_effect_css = ""
    if _te_tpl == "__gradient__":
        # Градиентная заливка букв: accent → светлее
        title_effect_css = (f"background:linear-gradient(180deg,{accent},#ffffff);"
                            f"-webkit-background-clip:text;background-clip:text;"
                            f"-webkit-text-fill-color:transparent;")
    elif _te_tpl:
        title_effect_css = _te_tpl.format(accent=accent)

    # Базовая тень заголовка — только если эффект не задаёт свою тень/заливку
    _effects_with_own_shadow = {"neon", "glow_soft", "shadow3d", "engrave", "gradient"}
    title_shadow_base = "" if _te_key in _effects_with_own_shadow else "text-shadow:0 2px 6px rgba(0,0,0,0.5);"

    # ── Этап 4: декоративные элементы ─────────────────────────────────────
    # Бейдж «Проверенный продавец»
    _vb_key = ex.get("verified_badge", "none")
    _, _vb_text = VERIFIED_BADGES.get(_vb_key, VERIFIED_BADGES["none"])
    verified_html = ""
    if _vb_text:
        verified_html = (f'<div class="vbadge" style="display:inline-block;margin:0 auto 14px;'
                         f'padding:5px 16px;border-radius:20px;background:{accent}22;'
                         f'border:1px solid {accent}66;color:{accent};font-size:12px;'
                         f'font-weight:600;letter-spacing:.03em;">{_vb_text}</div>')

    # Угловые орнаменты
    _or_key = ex.get("corner_ornament", "none")
    _, _or_style = CORNER_ORNAMENTS.get(_or_key, CORNER_ORNAMENTS["none"])
    ornament_html = _build_ornament(_or_style, accent)

    # Логотип магазина (загружаемый, base64 PNG)
    logo_b64 = ex.get("logo_b64")
    logo_html = ""
    if logo_b64:
        logo_html = (f'<div style="text-align:center;margin-bottom:12px;">'
                     f'<img src="data:image/png;base64,{logo_b64}" '
                     f'style="max-height:64px;max-width:180px;object-fit:contain;"></div>')

    if vis["show_avatar"]:
        avatar = (f'<div class="av" style="background:url(data:image/jpeg;base64,{data["avatar_b64"]}) center/cover;'
                  f'border:2px solid {accent};"></div>'
                  if data.get("avatar_b64") else
                  f'<div class="av" style="background:rgba(255,255,255,0.08);border:2px solid {accent};color:{accent};">{initials}</div>')
    else:
        avatar = ""

    badge = f'<div class="badge">{item}</div>' if item else ""
    stars_html = f'<div class="stars">{stars}</div>' if stars else ""
    quote_html = '<div class="quote">"</div>' if vis["show_quote"] else ""
    quote_html_big = '<div class="quote" style="font-size:72px;">"</div>' if vis["show_quote"] else ""

    # Раскладки
    if layout == "header_user":
        body = f"""
        <div class="meta meta-top">{avatar}<div class="mi"><div class="name">{name}</div><div class="date">{date}</div></div>{badge}</div>
        <div class="divider"></div>
        <div class="shop">{shop}</div>
        <div class="seller">{seller_tag}</div>
        {stars_html}
        {quote_html}
        <div class="review">{review}</div>"""
    elif layout == "left_align":
        body = f"""
        <div class="shop" style="text-align:left;">{shop}</div>
        <div class="seller" style="text-align:left;">{seller_tag}</div>
        <div class="divider"></div>
        {stars_html}
        <div class="review" style="text-align:left;">{review}</div>
        <div class="divider"></div>
        <div class="meta">{avatar}<div class="mi"><div class="name">{name}</div><div class="date">{date}</div></div>{badge}</div>"""
    elif layout == "compact":
        body = f"""
        <div class="shop" style="font-size:22px;margin-bottom:2px;">{shop}</div>
        {stars_html}
        <div class="review" style="margin:10px 0;">{review}</div>
        <div class="meta">{avatar}<div class="mi"><div class="name">{name}</div><div class="date">{date}</div></div>{badge}</div>"""
    elif layout == "magazine":
        body = f"""
        <div class="shop" style="font-size:38px;font-weight:800;">{shop}</div>
        <div class="seller">{seller_tag}</div>
        <div class="divider"></div>
        {stars_html}
        {quote_html_big}
        <div class="review" style="font-size:18px;">{review}</div>
        <div class="divider"></div>
        <div class="meta">{avatar}<div class="mi"><div class="name">{name}</div><div class="date">{date}</div></div>{badge}</div>"""
    elif layout == "split":
        body = f"""
        <div class="shop">{shop}</div>
        <div class="seller">{seller_tag}</div>
        <div class="divider"></div>
        {stars_html}
        <div class="review">{review}</div>
        <div class="divider"></div>
        <div class="meta" style="justify-content:space-between;">
          <div style="display:flex;align-items:center;gap:12px;">{avatar}<div class="mi"><div class="name">{name}</div><div class="date">{date}</div></div></div>
          {badge}
        </div>"""
    elif layout == "minimal":
        body = f"""
        {stars_html}
        <div class="review" style="font-size:17px;">{review}</div>
        <div class="divider"></div>
        <div class="meta">{avatar}<div class="mi"><div class="name">{name}</div><div class="date">{date}</div></div></div>"""
    else:  # classic
        body = f"""
        <div class="shop">{shop}</div>
        <div class="seller">{seller_tag}</div>
        <div class="divider"></div>
        {stars_html}
        {quote_html}
        <div class="review">{review}</div>
        <div class="divider"></div>
        <div class="meta">{avatar}<div class="mi"><div class="name">{name}</div><div class="date">{date}</div></div>{badge}</div>"""

    # Декор сверху карточки: логотип + бейдж проверенного продавца
    _decor_top = ""
    if logo_html:
        _decor_top += logo_html
    if verified_html:
        _decor_top += f'<div style="text-align:center;">{verified_html}</div>'
    body = _decor_top + body

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
{FONT_LINKS}
<style>
*{{margin:0;padding:0;box-sizing:border-box;font-family:{font_css},"Noto Sans","Noto Sans CJK SC","Noto Sans Arabic","Noto Sans Coptic","Noto Sans Gothic","Noto Sans Symbols","Noto Sans Symbols 2","Noto Color Emoji","Symbola",sans-serif;}}
body{{width:800px;background:transparent;}}
.frame{{background:{corner_fill};padding:{frame_pad};display:inline-block;width:800px;}}
.card{{width:100%;{bg_style}padding:44px 56px 36px;position:relative;overflow:hidden;border:{card_border};border-radius:{card_radius};{card_border_extra}{card_shadow}}}
{texture_layer}
.shop{{font-family:{title_font_css},"Noto Sans","Noto Sans CJK SC","Noto Sans Arabic","Noto Sans Coptic","Noto Sans Symbols 2",sans-serif;font-weight:700;font-size:30px;color:{accent};text-align:center;margin-bottom:6px;position:relative;z-index:1;{title_shadow_base}{title_effect_css}}}
.seller{{font-size:13px;color:{text_color};opacity:0.6;text-align:center;margin-bottom:16px;}}
.divider{{height:1px;background:{accent};opacity:0.4;margin:16px 40px;}}
.stars{{font-size:20px;color:{accent};text-align:center;letter-spacing:4px;margin-bottom:14px;text-shadow:0 1px 4px rgba(0,0,0,0.5);}}
.quote{{font-size:60px;color:{accent};opacity:0.4;line-height:0.5;margin-bottom:10px;}}
.review{{font-size:{review_fs}px;color:{text_color};line-height:{review_lh};text-align:center;margin-bottom:20px;text-shadow:0 1px 4px rgba(0,0,0,0.4);}}
.meta{{display:flex;align-items:center;gap:14px;}}
.meta-top{{margin-bottom:4px;}}
.av{{width:44px;height:44px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-weight:600;font-size:14px;}}
.mi{{flex:1;}}
.name{{font-weight:600;font-size:15px;color:{text_color};text-shadow:0 1px 3px rgba(0,0,0,0.6);}}
.date{{font-size:12px;color:{text_color};opacity:0.5;}}
.badge{{font-size:12px;color:{bg_color if not bg_image else '#1a1a1a'};background:{accent};padding:5px 14px;border-radius:20px;white-space:nowrap;font-weight:600;}}
.wm{{text-align:right;font-size:10px;color:{text_color};opacity:0.3;margin-top:14px;}}
.creator-wm{{position:absolute;bottom:8px;left:16px;font-size:9px;color:{text_color};opacity:0.35;}}
</style></head><body>
<div class="frame">
<div class="card">
  {ornament_html}
  {body}
  <div class="wm">@{bot_username}</div>
  {creator_wm}
</div>
</div></body></html>"""


def _render(html: str) -> bytes:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 900, "height": 600}, device_scale_factor=2)
        page.set_content(html, wait_until="networkidle")
        try:
            # Принудительно загружаем все используемые шрифты (иначе часть не успевает)
            page.evaluate("""async () => {
                const families = ['Montserrat','Playfair Display','Caveat','Space Grotesk',
                    'Inter','Oswald','Comfortaa','VT323','Cinzel',
                    'JetBrains Mono','Bebas Neue'];
                await Promise.all(families.map(f => {
                    try { return Promise.all([
                        document.fonts.load('400 30px "'+f+'"'),
                        document.fonts.load('700 30px "'+f+'"')
                    ]); }
                    catch(e) { return Promise.resolve(); }
                }));
                await document.fonts.ready;
            }""")
        except Exception:
            pass
        page.wait_for_timeout(400)
        full_height = page.evaluate("document.body.scrollHeight")
        page.set_viewport_size({"width": 900, "height": int(full_height) + 60})
        page.wait_for_timeout(120)
        target = page.query_selector(".frame") or page.query_selector(".card")
        box = target.bounding_box()
        png = page.screenshot(type="png", clip={"x": box["x"], "y": box["y"],
                                                  "width": box["width"], "height": box["height"]})
        browser.close()
    return png


# Демо-данные для превью
PREVIEW_DATA = {
    "shop_name": "ШимШим Маркет",
    "seller_tag": "@shimshim",
    "buyer_name": "Тихий ангел",
    "buyer_initials": "ТА",
    "review_text": "Грибы пришли моментально, продавец топ! Рекомендую всем.",
    "item_bought": "Грибы",
    "stars": 5,
    "avatar_b64": None,
}


async def render_preview(cfg: dict, bot_username: str = "reviewbot") -> bytes:
    data = dict(PREVIEW_DATA)
    data["bot_username"] = bot_username
    html = build_html(cfg, data)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _render, html)


async def render_with_data(cfg: dict, data: dict) -> bytes:
    if data.get("avatar_bytes"):
        data["avatar_b64"] = base64.b64encode(data["avatar_bytes"]).decode()
    data.setdefault("bot_username", "reviewbot")
    html = build_html(cfg, data)
    # Пруфы: список (новое) или один (обратная совместимость)
    proofs = data.get("proof_b64_list") or ([data["proof_b64"]] if data.get("proof_b64") else [])
    if proofs:
        accent = data.get("accent_color_for_proof", cfg.get("accent_color", "#c9a84c"))
        from services.card_generator import _inject_proof
        html = _inject_proof(html, proofs, accent)
    if data.get("verify_code"):
        from services.card_generator import _inject_verify_code
        html = _inject_verify_code(html, data["verify_code"], data.get("bot_username", "RevShimbot"))
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _render, html)
