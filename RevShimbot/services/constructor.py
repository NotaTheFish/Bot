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
    "montserrat":  ("Montserrat", "'Montserrat',sans-serif"),
    "playfair":    ("Playfair Display", "'Playfair Display',serif"),
    "caveat":      ("Caveat", "'Caveat',cursive"),
    "spacegrotesk":("Space Grotesk", "'Space Grotesk',sans-serif"),
    "inter":       ("Inter", "'Inter',sans-serif"),
    "oswald":      ("Oswald", "'Oswald',sans-serif"),
    "comfortaa":   ("Comfortaa", "'Comfortaa',cursive"),
}

TEXT_COLORS = {
    "white":  ("⚪️ Белый", "#f0f0f0"),
    "gold":   ("🟡 Золотой", "#ffd95a"),
    "cyan":   ("🔵 Голубой", "#7ad6ff"),
    "green":  ("🟢 Зелёный", "#7ee787"),
    "pink":   ("🩷 Розовый", "#ff9ec4"),
    "purple": ("🟣 Фиолетовый", "#c9a4ff"),
}

ACCENT_COLORS = {
    "gold":   ("🟡 Золото", "#c9a84c"),
    "blue":   ("🔵 Синий", "#58a6ff"),
    "green":  ("🟢 Зелёный", "#3fb950"),
    "pink":   ("🩷 Розовый", "#ff7eb6"),
    "purple": ("🟣 Фиолет", "#a371f7"),
    "red":    ("🔴 Красный", "#ff6b6b"),
}

BG_COLORS = {
    "dark_blue":   ("🌌 Тёмно-синий", "#1a1a2e"),
    "charcoal":    ("⚫️ Уголь", "#161b22"),
    "deep_purple": ("🟪 Глубокий фиолет", "#1e1633"),
    "forest":      ("🌲 Тёмный лес", "#15201a"),
    "wine":        ("🍷 Винный", "#2a1520"),
    "slate":       ("🪨 Сланец", "#1c2128"),
}

# ── Этап 1: расширенные настройки ──────────────────────────────────────────

# Градиентные фоны: (label, цвет1, цвет2, угол)
BG_GRADIENTS = {
    "none":        ("⬜️ Без градиента", None, None, None),
    "midnight":    ("🌃 Полночь", "#1a1a2e", "#0f0c1d", 180),
    "ocean":       ("🌊 Океан", "#0f2027", "#203a43", 160),
    "sunset":      ("🌅 Закат", "#2a1520", "#3d1e2e", 160),
    "emerald":     ("💚 Изумруд", "#15201a", "#0a2818", 180),
    "royal":       ("👑 Королевский", "#1e1633", "#2d1b4e", 160),
    "ember":       ("🔥 Угли", "#1a1010", "#2e1508", 180),
    "steel":       ("⚙️ Сталь", "#1c2128", "#2d333b", 160),
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
    "soft":     ("🌫 Мягкая", "box-shadow:0 8px 32px rgba(0,0,0,0.4);"),
    "hard":     ("⬛️ Жёсткая", "box-shadow:0 12px 0 rgba(0,0,0,0.25);"),
    "glowacc":  ("✨ Цветная", "box-shadow:0 8px 40px {accent}44;"),
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


FONT_LINKS = (
    "https://fonts.googleapis.com/css2?"
    "family=Montserrat:wght@400;600;700;800&"
    "family=Playfair+Display:wght@400;700&"
    "family=Caveat:wght@600;700&"
    "family=Space+Grotesk:wght@400;600&"
    "family=Inter:wght@300;400;600&"
    "family=Oswald:wght@400;600&"
    "family=Comfortaa:wght@400;700&display=swap"
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

    # Тень
    _sh_key = ex.get("card_shadow", "none")
    _, sh_tpl = CARD_SHADOW.get(_sh_key, CARD_SHADOW["none"])
    card_shadow = sh_tpl.format(accent=accent)

    stars = _stars(data["stars"]) if data.get("stars", 0) > 0 else ""
    shop = data["shop_name"]
    seller_tag = data.get("seller_tag", "") if vis["show_seller_tag"] else ""
    review = data["review_text"]
    name = data["buyer_name"]
    initials = data["buyer_initials"]
    item = data.get("item_bought", "") if vis["show_item"] else ""
    date = _date_msk() if vis["show_date"] else ""
    creator_wm = _watermark_html(cfg.get("creator_username"), cfg.get("is_edited", False))
    bot_username = data.get("bot_username", "reviewbot")

    # Фон — картинка > градиент > цвет
    _grad_key = ex.get("bg_gradient", "none")
    _grad = BG_GRADIENTS.get(_grad_key, BG_GRADIENTS["none"])
    if bg_image:
        bg_style = (f"background-image:linear-gradient(180deg,rgba(0,0,0,0.55),rgba(0,0,0,0.85)),"
                    f"url('data:image/png;base64,{bg_image}');background-size:cover;background-position:center;")
    elif _grad[1]:
        bg_style = f"background:linear-gradient({_grad[3]}deg,{_grad[1]},{_grad[2]});"
    else:
        bg_style = f"background:{bg_color};"

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

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<link href="{FONT_LINKS}" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;font-family:{font_css},"Noto Sans","Noto Sans CJK SC","Noto Sans Arabic","Noto Sans Coptic","Noto Sans Gothic","Noto Sans Symbols","Noto Sans Symbols 2","Noto Color Emoji","Symbola",sans-serif;}}
body{{width:800px;background:transparent;}}
.card{{width:800px;{bg_style}padding:44px 56px 36px;position:relative;overflow:hidden;border:{card_border};border-radius:{card_radius};{card_border_extra}{card_shadow}}}
.shop{{font-family:{title_font_css},"Noto Sans","Noto Sans CJK SC","Noto Sans Arabic","Noto Sans Coptic","Noto Sans Symbols 2",sans-serif;font-weight:700;font-size:30px;color:{accent};text-align:center;margin-bottom:6px;text-shadow:0 2px 6px rgba(0,0,0,0.5);}}
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
<div class="card">
  {body}
  <div class="wm">@{bot_username}</div>
  {creator_wm}
</div></body></html>"""


def _render(html: str) -> bytes:
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 900, "height": 600}, device_scale_factor=2)
        page.set_content(html, wait_until="networkidle")
        try:
            page.evaluate("document.fonts.ready")
        except Exception:
            pass
        full_height = page.evaluate("document.body.scrollHeight")
        page.set_viewport_size({"width": 900, "height": int(full_height) + 40})
        page.wait_for_timeout(100)
        card = page.query_selector(".card")
        box = card.bounding_box()
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
    proof_b64 = data.get("proof_b64")
    if proof_b64:
        accent = data.get("accent_color_for_proof", cfg.get("accent_color", "#c9a84c"))
        proof_block = f"""<div style="margin-top:4px;">
  <div style="font-size:12px;color:{accent};letter-spacing:.08em;text-transform:uppercase;margin:18px 0 12px;display:flex;align-items:center;gap:8px;">
    <span style="flex:1;height:1px;background:{accent}33;"></span>
    📸 Доказательство сделки
    <span style="flex:1;height:1px;background:{accent}33;"></span>
  </div>
  <img src="data:image/png;base64,{proof_b64}" style="width:100%;border-radius:10px;border:1px solid {accent}44;display:block;">
</div>"""
        if '<div class="wm"' in html:
            idx = html.find('<div class="wm"')
            html = html[:idx] + proof_block + html[idx:]
        else:
            html = html.replace("</body>", proof_block + "</body>", 1)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _render, html)
