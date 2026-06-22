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

    stars = _stars(data["stars"]) if data.get("stars", 0) > 0 else ""
    shop = data["shop_name"]
    seller_tag = data.get("seller_tag", "")
    review = data["review_text"]
    name = data["buyer_name"]
    initials = data["buyer_initials"]
    item = data.get("item_bought", "")
    date = _date_msk()
    creator_wm = _watermark_html(cfg.get("creator_username"), cfg.get("is_edited", False))
    bot_username = data.get("bot_username", "reviewbot")

    # Фон — картинка или цвет
    if bg_image:
        bg_style = (f"background-image:linear-gradient(180deg,rgba(0,0,0,0.55),rgba(0,0,0,0.85)),"
                    f"url('data:image/png;base64,{bg_image}');background-size:cover;background-position:center;")
    else:
        bg_style = f"background:{bg_color};"

    avatar = (f'<div class="av" style="background:url(data:image/jpeg;base64,{data["avatar_b64"]}) center/cover;'
              f'border:2px solid {accent};"></div>'
              if data.get("avatar_b64") else
              f'<div class="av" style="background:rgba(255,255,255,0.08);border:2px solid {accent};color:{accent};">{initials}</div>')

    badge = f'<div class="badge">{item}</div>' if item else ""
    stars_html = f'<div class="stars">{stars}</div>' if stars else ""

    # Раскладки
    if layout == "header_user":
        body = f"""
        <div class="meta meta-top">{avatar}<div class="mi"><div class="name">{name}</div><div class="date">{date}</div></div>{badge}</div>
        <div class="divider"></div>
        <div class="shop">{shop}</div>
        <div class="seller">{seller_tag}</div>
        {stars_html}
        <div class="quote">"</div>
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
        <div class="quote" style="font-size:72px;">"</div>
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
        <div class="quote">"</div>
        <div class="review">{review}</div>
        <div class="divider"></div>
        <div class="meta">{avatar}<div class="mi"><div class="name">{name}</div><div class="date">{date}</div></div>{badge}</div>"""

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<link href="{FONT_LINKS}" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;font-family:{font_css};}}
body{{width:800px;background:transparent;}}
.card{{width:800px;{bg_style}padding:44px 56px 36px;position:relative;overflow:hidden;}}
.shop{{font-family:{title_font_css};font-weight:700;font-size:30px;color:{accent};text-align:center;margin-bottom:6px;text-shadow:0 2px 6px rgba(0,0,0,0.5);}}
.seller{{font-size:13px;color:{text_color};opacity:0.6;text-align:center;margin-bottom:16px;}}
.divider{{height:1px;background:{accent};opacity:0.4;margin:16px 40px;}}
.stars{{font-size:20px;color:{accent};text-align:center;letter-spacing:4px;margin-bottom:14px;text-shadow:0 1px 4px rgba(0,0,0,0.5);}}
.quote{{font-size:60px;color:{accent};opacity:0.4;line-height:0.5;margin-bottom:10px;}}
.review{{font-size:16px;color:{text_color};line-height:1.8;text-align:center;margin-bottom:20px;text-shadow:0 1px 4px rgba(0,0,0,0.4);}}
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
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _render, html)
