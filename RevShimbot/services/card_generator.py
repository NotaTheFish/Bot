import asyncio
import io
from datetime import datetime
from typing import Optional
import pytz

MSK = pytz.timezone("Europe/Moscow")

# ── Emoji helpers ──────────────────────────────────────────────────────────

import re as _re
import base64 as _b64

def _replace_emoji_with_img(text: str, emoji_map: dict, bg: str = "transparent", size: int = 20) -> str:
    """
    Заменяет кастомные TG-эмодзи (и обычные) на <img> теги.
    emoji_map: {char_or_id: base64_png}
    """
    if not emoji_map:
        return text
    for key, b64 in emoji_map.items():
        img_tag = (
            f'<img src="data:image/png;base64,{b64}" '
            f'style="width:{size}px;height:{size}px;'
            f'vertical-align:middle;border-radius:3px;'
            f'background:{bg};display:inline-block;" />'
        )
        text = text.replace(key, img_tag)
    return text


async def resolve_custom_emoji(
    text: str,
    entities: list,
    bot
) -> str:
    """
    Принимает текст и MessageEntity список.
    Для каждого custom_emoji entity:
      - скачивает стикер через getCustomEmojiStickers
      - конвертирует в PNG base64
      - заменяет символ в тексте на <img> тег
    Возвращает HTML-строку с встроенными эмодзи.
    """
    import base64 as _b64
    import io as _io

    if not entities:
        return _sanitize_text_for_html(text)

    # Собираем все custom_emoji_id
    custom_ids = []
    emoji_positions = []  # (offset, length, emoji_id)
    for entity in entities:
        if hasattr(entity, 'type') and entity.type == 'custom_emoji':
            cid = entity.custom_emoji_id
            custom_ids.append(cid)
            emoji_positions.append((entity.offset, entity.length, cid))

    if not custom_ids:
        return _sanitize_text_for_html(text)

    # Скачиваем все стикеры одним запросом (до 200 за раз)
    emoji_b64 = {}
    try:
        unique_ids = list(dict.fromkeys(custom_ids))  # убираем дубли, сохраняем порядок
        stickers = await bot.get_custom_emoji_stickers(unique_ids)
        for sticker in stickers:
            try:
                file = await bot.get_file(sticker.file_id)
                buf = await bot.download_file(file.file_path)
                raw = buf.read() if hasattr(buf, 'read') else bytes(buf)
                # Конвертируем WebP/TGS в PNG через Pillow
                from PIL import Image
                img = Image.open(_io.BytesIO(raw)).convert("RGBA")
                out = _io.BytesIO()
                img.save(out, "PNG")
                emoji_b64[sticker.custom_emoji_id] = _b64.b64encode(out.getvalue()).decode()
            except Exception:
                pass
    except Exception:
        return _sanitize_text_for_html(text)

    if not emoji_b64:
        return _sanitize_text_for_html(text)

    # Строим результирующий HTML посимвольно
    # text — unicode строка, offset/length в UTF-16 code units (Telegram считает так)
    # Конвертируем в UTF-16 для правильного маппинга
    text_utf16 = text.encode('utf-16-le')
    result_parts = []
    prev_end = 0  # в UTF-16 code units

    # Сортируем по offset
    sorted_pos = sorted(emoji_positions, key=lambda x: x[0])

    for offset, length, emoji_id in sorted_pos:
        # Текст до эмодзи
        before_bytes = text_utf16[prev_end*2 : offset*2]
        before_text = before_bytes.decode('utf-16-le')
        result_parts.append(_sanitize_text_for_html(before_text))

        # Сам эмодзи
        if emoji_id in emoji_b64:
            b64 = emoji_b64[emoji_id]
            result_parts.append(
                f'<img src="data:image/png;base64,{b64}" '                f'style="width:22px;height:22px;vertical-align:middle;'                f'display:inline-block;margin:0 1px;" />' 
            )
        else:
            # Фолбэк — оставляем оригинальный символ
            emoji_bytes = text_utf16[offset*2 : (offset+length)*2]
            result_parts.append(_sanitize_text_for_html(emoji_bytes.decode('utf-16-le')))

        prev_end = offset + length

    # Остаток текста после последнего эмодзи
    tail_bytes = text_utf16[prev_end*2:]
    result_parts.append(_sanitize_text_for_html(tail_bytes.decode('utf-16-le')))

    return "".join(result_parts)


def _sanitize_text_for_html(text: str) -> str:
    """Экранирует HTML спецсимволы в тексте отзыва."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def date_msk() -> str:
    months = ["января","февраля","марта","апреля","мая","июня",
              "июля","августа","сентября","октября","ноября","декабря"]
    now = datetime.now(MSK)
    return f"{now.day} {months[now.month-1]} {now.year} г."


def stars_html(n: int) -> str:
    return "★"*n + "☆"*(5-n)


def avatar_html(av_b64: Optional[str], initials: str,
                bg: str, border: str, text_color: str) -> str:
    if av_b64:
        return f'<div class="av" style="background:url(data:image/jpeg;base64,{av_b64}) center/cover;border:2px solid {border};"></div>'
    return f'<div class="av" style="background:{bg};border:2px solid {border};color:{text_color};">{initials}</div>'


# ══════════════════════════════════════════════════════════════════════════
# HTML TEMPLATES
# ══════════════════════════════════════════════════════════════════════════

def html_classic_gold(d: dict) -> str:
    stars = stars_html(d["stars"]) if d["stars_mode"] != "disabled" and d["stars"] > 0 else ""
    av = avatar_html(d.get("avatar_b64"), d["buyer_initials"], "#252545", "#c9a84c", "#c9a84c")
    badge = f'<div class="badge" style="background:#252545;border:1px solid #c9a84c;color:#c9a84c;">{d["item_bought"]}</div>' if d.get("item_bought") else ""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&family=Playfair+Display:wght@700&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{width:800px;background:transparent;}}
.card{{background:#1a1a2e;border-radius:0;padding:44px 56px 36px;position:relative;overflow:hidden;}}
.bar{{position:absolute;top:0;left:56px;right:56px;height:3px;background:linear-gradient(90deg,transparent,#c9a84c,#e8cc7a,#c9a84c,transparent);}}
.corner{{position:absolute;width:20px;height:20px;border-color:#c9a84c;border-style:solid;opacity:.7;}}
.c-tl{{top:12px;left:12px;border-width:1.5px 0 0 1.5px;}}
.c-tr{{top:12px;right:12px;border-width:1.5px 1.5px 0 0;}}
.c-bl{{bottom:12px;left:12px;border-width:0 0 1.5px 1.5px;}}
.c-br{{bottom:12px;right:12px;border-width:0 1.5px 1.5px 0;}}
.shop{{font-family:'Montserrat',sans-serif;font-weight:700;font-size:24px;color:#c9a84c;text-align:center;margin-bottom:8px;}}
.seller{{font-family:'Montserrat',sans-serif;font-size:13px;color:#666688;text-align:center;margin-bottom:16px;}}
.divider{{height:1px;background:linear-gradient(90deg,transparent,#c9a84c55,transparent);margin:0 40px 18px;}}
.stars{{font-size:19px;color:#c9a84c;text-align:center;letter-spacing:4px;margin-bottom:16px;}}
.quote{{font-family:'Playfair Display',serif;font-size:64px;color:#c9a84c;line-height:.5;opacity:.4;margin-bottom:10px;}}
.review{{font-family:'Montserrat','Noto Sans','Noto Sans CJK SC','Noto Sans Arabic','Noto Sans Coptic','Noto Sans Symbols 2','Noto Color Emoji','Symbola',sans-serif;font-size:16px;color:#d0d0e8;line-height:1.8;margin-bottom:24px;}}
.divider2{{height:1px;background:#2e2e50;margin-bottom:20px;}}
.meta{{display:flex;align-items:center;gap:14px;}}
.av{{width:44px;height:44px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-family:'Montserrat',sans-serif;font-weight:600;font-size:14px;}}
.meta-info{{flex:1;}}
.name{{font-family:'Montserrat','Noto Sans','Noto Sans CJK SC','Noto Sans Arabic','Noto Sans Coptic','Noto Sans Gothic','Noto Sans Symbols 2','Noto Color Emoji','Symbola',sans-serif;font-weight:600;font-size:15px;color:#e0e0f0;}}
.date{{font-family:'Montserrat',sans-serif;font-size:12px;color:#666688;margin-top:2px;}}
.badge{{font-family:'Montserrat',sans-serif;font-size:12px;padding:5px 14px;border-radius:20px;white-space:nowrap;}}
.wm{{text-align:right;font-family:'Montserrat',sans-serif;font-size:10px;color:#2e2e50;margin-top:14px;letter-spacing:.05em;}}
<style>
/* Системный Noto Color Emoji для обычных эмодзи */
* {{ font-family: inherit, "Noto Color Emoji", sans-serif; }}
</style></style></head><body>
<div class="card">
  <div class="bar"></div>
  <div class="corner c-tl"></div><div class="corner c-tr"></div>
  <div class="corner c-bl"></div><div class="corner c-br"></div>
  <div class="shop">{d["shop_name"]}</div>
  <div class="seller">{d["seller_tag"]}</div>
  <div class="divider"></div>
  {"<div class='stars'>"+stars+"</div>" if stars else ""}
  <div class="quote">"</div>
  <div class="review">{d.get("review_text_html", d["review_text"])}</div>
  <div class="divider2"></div>
  <div class="meta">
    {av}
    <div class="meta-info">
      <div class="name">{d["buyer_name"]}</div>
      <div class="date">{date_msk()}</div>
    </div>
    {badge}
  </div>
  <div class="wm">@{d.get("bot_username", "reviewbot")}</div>
</div></body></html>"""


def html_retro_paper(d: dict) -> str:
    stars = stars_html(d["stars"]) if d["stars_mode"] != "disabled" and d["stars"] > 0 else ""
    av = avatar_html(d.get("avatar_b64"), d["buyer_initials"], "#e8d5b0", "#c8a96e", "#4a3010")
    badge = f'<div class="badge">{d["item_bought"]}</div>' if d.get("item_bought") else ""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,700;1,400&family=IM+Fell+English:ital@0;1&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{width:800px;background:transparent;}}
.card{{background:#f4ead0;border-radius:0;padding:44px 56px 36px;position:relative;
  background-image:repeating-linear-gradient(transparent,transparent 31px,rgba(150,120,64,.1) 31px,rgba(150,120,64,.1) 32px);}}
.frame{{position:absolute;inset:10px;border:1.5px solid #c8a96e;border-radius:12px;pointer-events:none;}}
.frame::before{{content:'';position:absolute;inset:4px;border:.5px solid #c8a96e;border-radius:8px;opacity:.4;}}
.orn{{text-align:center;font-family:'Playfair Display',serif;font-size:16px;color:#a07840;letter-spacing:8px;margin-bottom:12px;}}
.shop{{font-family:'Playfair Display',serif;font-weight:700;font-size:26px;color:#2d1a08;text-align:center;margin-bottom:6px;}}
.seller{{font-family:'IM Fell English',serif;font-size:12px;color:#8b6030;text-align:center;margin-bottom:18px;letter-spacing:.1em;}}
.divider{{display:flex;align-items:center;gap:10px;margin:0 40px 18px;}}
.dline{{flex:1;height:.5px;background:#a07840;opacity:.5;}}
.dmark{{color:#a07840;font-size:12px;}}
.stars{{font-size:20px;color:#3a2a10;text-align:center;letter-spacing:4px;margin-bottom:16px;}}
.quote{{font-family:'Playfair Display',serif;font-size:60px;color:#c8a87a;line-height:.5;opacity:.45;margin-bottom:12px;}}
.review{{font-family:'IM Fell English','Noto Sans','Noto Sans CJK SC','Noto Sans Arabic','Noto Sans Coptic','Noto Sans Symbols 2','Noto Color Emoji','Symbola',serif;font-style:italic;font-size:16px;color:#1e1408;line-height:1.9;margin-bottom:22px;}}
.meta{{display:flex;align-items:center;gap:14px;}}
.av{{width:42px;height:42px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-family:'Playfair Display',serif;font-weight:700;font-size:14px;border:1.5px solid #c8a96e;}}
.meta-info{{flex:1;}}
.name{{font-family:'Playfair Display','Noto Sans','Noto Sans CJK SC','Noto Sans Arabic','Noto Sans Coptic','Noto Sans Gothic','Noto Sans Symbols 2','Noto Color Emoji','Symbola',serif;font-weight:700;font-size:15px;color:#1e1408;}}
.date{{font-family:'IM Fell English',serif;font-size:11px;color:#8b6030;margin-top:2px;}}
.badge{{font-family:'IM Fell English',serif;font-size:11px;color:#3a2a10;background:#e8d5b0;border:1px solid #c8a96e;padding:5px 12px;border-radius:2px;white-space:nowrap;}}
.wm{{text-align:right;font-family:'IM Fell English',serif;font-size:10px;color:#c8a96e;margin-top:14px;opacity:.6;}}
<style>
/* Системный Noto Color Emoji для обычных эмодзи */
* {{ font-family: inherit, "Noto Color Emoji", sans-serif; }}
</style></style></head><body>
<div class="card">
  <div class="frame"></div>
  <div class="orn">— ✦ —</div>
  <div class="shop">{d["shop_name"]}</div>
  <div class="seller">{d["seller_tag"]}</div>
  <div class="divider"><div class="dline"></div><span class="dmark">◆</span><div class="dline"></div></div>
  {"<div class='stars'>"+stars+"</div>" if stars else ""}
  <div class="quote">"</div>
  <div class="review">{d.get("review_text_html", d["review_text"])}</div>
  <div class="divider"><div class="dline"></div><span class="dmark">◆</span><div class="dline"></div></div>
  <div class="meta">
    {av}
    <div class="meta-info">
      <div class="name">{d["buyer_name"]}</div>
      <div class="date">{date_msk()}</div>
    </div>
    {badge}
  </div>
  <div class="wm">@{d.get("bot_username", "reviewbot")}</div>
</div></body></html>"""


def html_dark_slate(d: dict) -> str:
    stars = stars_html(d["stars"]) if d["stars_mode"] != "disabled" and d["stars"] > 0 else ""
    av = avatar_html(d.get("avatar_b64"), d["buyer_initials"], "#1c2128", "#30363d", "#58a6ff")
    badge = f'<div class="badge">{d["item_bought"]}</div>' if d.get("item_bought") else ""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&family=Inter:wght@300;400;500&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{width:800px;background:transparent;}}
.card{{background:#161b22;border-radius:0;padding:0;overflow:hidden;border:1px solid #30363d;}}
.accent{{height:3px;background:#58a6ff;}}
.inner{{padding:36px 52px 32px;}}
.header{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:6px;}}
.shop{{font-family:'Space Grotesk',sans-serif;font-weight:600;font-size:20px;color:#e6edf3;}}
.stars{{font-size:17px;color:#58a6ff;letter-spacing:3px;margin-top:4px;}}
.seller{{font-family:'Inter',sans-serif;font-size:12px;color:#58a6ff;margin-bottom:18px;}}
.divider{{height:1px;background:#21262d;margin-bottom:18px;}}
.comment{{font-family:'Inter',sans-serif;font-size:11px;color:#6e7681;margin-bottom:10px;letter-spacing:.03em;}}
.review-wrap{{border-left:2px solid #21262d;padding-left:16px;margin-bottom:22px;}}
.review{{font-family:'Inter','Noto Sans','Noto Sans CJK SC','Noto Sans Arabic','Noto Sans Coptic','Noto Sans Symbols 2','Noto Color Emoji','Symbola',sans-serif;font-weight:300;font-size:15px;color:#c9d1d9;line-height:1.8;}}
.meta{{background:#0d1117;border:1px solid #21262d;border-radius:10px;padding:14px 18px;display:flex;align-items:center;gap:14px;}}
.av{{width:38px;height:38px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-family:'Space Grotesk',sans-serif;font-weight:600;font-size:13px;border:1px solid #30363d;}}
.meta-info{{flex:1;}}
.name{{font-family:'Space Grotesk','Noto Sans','Noto Sans CJK SC','Noto Sans Arabic','Noto Sans Coptic','Noto Sans Gothic','Noto Sans Symbols 2','Noto Color Emoji','Symbola',sans-serif;font-weight:500;font-size:14px;color:#e6edf3;}}
.date{{font-family:'Inter',sans-serif;font-size:11px;color:#6e7681;margin-top:2px;}}
.badge{{font-family:'Inter',sans-serif;font-size:11px;color:#58a6ff;background:#1c2128;border:1px solid #30363d;padding:4px 12px;border-radius:20px;white-space:nowrap;}}
.wm{{text-align:right;font-family:'Inter',sans-serif;font-size:10px;color:#21262d;margin-top:12px;letter-spacing:.05em;}}
<style>
/* Системный Noto Color Emoji для обычных эмодзи */
* {{ font-family: inherit, "Noto Color Emoji", sans-serif; }}
</style></style></head><body>
<div class="card">
  <div class="accent"></div>
  <div class="inner">
    <div class="header">
      <div>
        <div class="shop">{d["shop_name"]}</div>
        <div class="seller">{d["seller_tag"]}</div>
      </div>
      {"<div class='stars'>"+stars+"</div>" if stars else ""}
    </div>
    <div class="divider"></div>
    <div class="comment">// отзыв покупателя</div>
    <div class="review-wrap"><div class="review">{d.get("review_text_html", d["review_text"])}</div></div>
    <div class="meta">
      {av}
      <div class="meta-info">
        <div class="name">{d["buyer_name"]}</div>
        <div class="date">{date_msk()}</div>
      </div>
      {badge}
    </div>
    <div class="wm">@{d.get("bot_username", "reviewbot")}</div>
  </div>
</div></body></html>"""


def html_clean_white(d: dict) -> str:
    stars = stars_html(d["stars"]) if d["stars_mode"] != "disabled" and d["stars"] > 0 else ""
    av = avatar_html(d.get("avatar_b64"), d["buyer_initials"], "#f5f5f5", "#e8e8e8", "#444")
    badge = f'<div class="badge">{d["item_bought"]}</div>' if d.get("item_bought") else ""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,300;0,400;0,500;1,300&family=DM+Serif+Display:ital@1&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{width:800px;background:transparent;}}
.card{{background:#fff;border-radius:0;padding:0;overflow:hidden;border:1px solid #e8e8e8;}}
.bar{{height:3px;background:#111;}}
.inner{{padding:36px 56px 32px;}}
.header{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px;}}
.shop{{font-family:'DM Sans',sans-serif;font-weight:500;font-size:22px;color:#111;letter-spacing:-.02em;}}
.stars{{font-size:17px;color:#111;letter-spacing:2px;margin-top:4px;}}
.seller{{font-family:'DM Sans',sans-serif;font-size:12px;color:#aaa;margin-bottom:22px;}}
.divider{{height:1px;background:#f0f0f0;margin-bottom:20px;}}
.quote{{font-family:'DM Serif Display',serif;font-style:italic;font-size:56px;color:#eee;line-height:.5;margin-bottom:12px;}}
.review{{font-family:'DM Sans','Noto Sans','Noto Sans CJK SC','Noto Sans Arabic','Noto Sans Coptic','Noto Sans Symbols 2','Noto Color Emoji','Symbola',sans-serif;font-weight:300;font-style:italic;font-size:15px;color:#333;line-height:1.85;margin-bottom:24px;}}
.divider2{{height:1px;background:#f0f0f0;margin-bottom:20px;}}
.meta{{display:flex;align-items:center;gap:14px;}}
.av{{width:42px;height:42px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-family:'DM Sans',sans-serif;font-weight:500;font-size:14px;border:1px solid #e8e8e8;}}
.meta-info{{flex:1;}}
.name{{font-family:'DM Sans','Noto Sans','Noto Sans CJK SC','Noto Sans Arabic','Noto Sans Coptic','Noto Sans Gothic','Noto Sans Symbols 2','Noto Color Emoji','Symbola',sans-serif;font-weight:500;font-size:14px;color:#111;}}
.date{{font-family:'DM Sans',sans-serif;font-size:11px;color:#aaa;margin-top:2px;}}
.badge{{font-family:'DM Sans',sans-serif;font-size:11px;color:#444;background:#f5f5f5;border:1px solid #e8e8e8;padding:5px 14px;border-radius:20px;white-space:nowrap;}}
.wm{{text-align:right;font-family:'DM Sans',sans-serif;font-size:10px;color:#ddd;margin-top:14px;letter-spacing:.08em;}}
<style>
/* Системный Noto Color Emoji для обычных эмодзи */
* {{ font-family: inherit, "Noto Color Emoji", sans-serif; }}
</style></style></head><body>
<div class="card">
  <div class="bar"></div>
  <div class="inner">
    <div class="header">
      <div>
        <div class="shop">{d["shop_name"]}</div>
        <div class="seller">{d["seller_tag"]}</div>
      </div>
      {"<div class='stars'>"+stars+"</div>" if stars else ""}
    </div>
    <div class="divider"></div>
    <div class="quote">"</div>
    <div class="review">{d.get("review_text_html", d["review_text"])}</div>
    <div class="divider2"></div>
    <div class="meta">
      {av}
      <div class="meta-info">
        <div class="name">{d["buyer_name"]}</div>
        <div class="date">{date_msk()}</div>
      </div>
      {badge}
    </div>
    <div class="wm">@{d.get("bot_username", "reviewbot")}</div>
  </div>
</div></body></html>"""


def html_sketch_paper(d: dict) -> str:
    stars = stars_html(d["stars"]) if d["stars_mode"] != "disabled" and d["stars"] > 0 else ""
    av = avatar_html(d.get("avatar_b64"), d["buyer_initials"], "#dfd0a8", "#a07840", "#4a3010")
    badge = f'<div class="badge">{d["item_bought"]}</div>' if d.get("item_bought") else ""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<link href="https://fonts.googleapis.com/css2?family=Caveat:wght@600;700&family=Montserrat:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{width:800px;background:transparent;}}
.card{{
  background:#f0ebe0;
  border-radius:0;
  padding:44px 64px 36px;
  position:relative;
  overflow:hidden;
  border:1px solid #c8b89a;
  background-image:repeating-linear-gradient(transparent,transparent 30px,rgba(140,110,60,.08) 30px,rgba(140,110,60,.08) 31px);
}}
.thread-left{{position:absolute;left:0;top:0;width:36px;height:100%;pointer-events:none;}}
.thread-right{{position:absolute;right:0;top:0;width:36px;height:100%;pointer-events:none;}}
.shop{{font-family:'Caveat',cursive;font-weight:700;font-size:28px;color:#1e1508;text-align:center;margin-bottom:6px;}}
.seller{{font-family:'Montserrat',sans-serif;font-size:12px;color:#7a6040;text-align:center;margin-bottom:16px;}}
.divider{{height:1px;background:#a07840;opacity:.4;margin:0 40px 16px;}}
.stars{{font-size:20px;color:#3a2a10;text-align:center;letter-spacing:4px;margin-bottom:14px;}}
.quote{{font-family:'Caveat',cursive;font-size:58px;color:#b09070;line-height:.5;opacity:.38;margin-bottom:12px;}}
.review{{font-family:'Montserrat','Noto Sans','Noto Sans CJK SC','Noto Sans Arabic','Noto Sans Coptic','Noto Sans Symbols 2','Noto Color Emoji','Symbola',sans-serif;font-weight:400;font-size:15px;color:#1a1005;line-height:1.8;margin-bottom:20px;}}
.divrow{{display:flex;align-items:center;gap:10px;margin:0 40px 18px;opacity:.4;}}
.dline{{flex:1;height:1px;background:#8b6030;}}
.dmark{{font-family:'Caveat',cursive;font-size:14px;color:#8b6030;}}
.meta{{display:flex;align-items:center;gap:14px;}}
.av{{width:42px;height:42px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-family:'Caveat',cursive;font-weight:700;font-size:15px;border:1.5px solid #a07840;}}
.meta-info{{flex:1;}}
.name{{font-family:'Caveat','Noto Sans','Noto Sans CJK SC','Noto Sans Arabic','Noto Sans Coptic','Noto Sans Gothic','Noto Sans Symbols 2','Noto Color Emoji','Symbola',cursive;font-weight:700;font-size:17px;color:#1a1005;}}
.date{{font-family:'Montserrat',sans-serif;font-size:11px;color:#8b6030;margin-top:2px;}}
.badge{{font-family:'Montserrat',sans-serif;font-size:11px;color:#3a2a10;background:#dfd0a8;border:1px solid #a07840;padding:5px 12px;border-radius:12px;white-space:nowrap;}}
.wm{{text-align:right;font-family:'Montserrat',sans-serif;font-size:10px;color:#c0a070;margin-top:14px;opacity:.55;}}
<style>
/* Системный Noto Color Emoji для обычных эмодзи */
* {{ font-family: inherit, "Noto Color Emoji", sans-serif; }}
</style></style></head><body>
<div class="card">
  <svg class="thread-left" viewBox="0 0 36 400" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M18,0 Q6,20 18,40 Q30,60 18,80 Q6,100 18,120 Q30,140 18,160 Q6,180 18,200 Q30,220 18,240 Q6,260 18,280 Q30,300 18,320 Q6,340 18,360 Q30,380 18,400" fill="none" stroke="rgba(30,18,8,.5)" stroke-width="2" stroke-linecap="round"/>
    <path d="M18,0 Q9,22 18,44 Q27,66 18,88 Q9,110 18,132 Q27,154 18,176 Q9,198 18,220 Q27,242 18,264 Q9,286 18,308 Q27,330 18,352 Q9,374 18,400" fill="none" stroke="rgba(30,18,8,.22)" stroke-width="1.2" stroke-linecap="round"/>
    <path d="M18,0 Q4,24 18,48 Q32,72 18,96 Q4,120 18,144 Q32,168 18,192 Q4,216 18,240 Q32,264 18,288 Q4,312 18,336 Q32,360 18,384 Q4,396 18,400" fill="none" stroke="rgba(30,18,8,.1)" stroke-width="0.8" stroke-linecap="round"/>
  </svg>
  <svg class="thread-right" viewBox="0 0 36 400" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M18,0 Q6,20 18,40 Q30,60 18,80 Q6,100 18,120 Q30,140 18,160 Q6,180 18,200 Q30,220 18,240 Q6,260 18,280 Q30,300 18,320 Q6,340 18,360 Q30,380 18,400" fill="none" stroke="rgba(30,18,8,.5)" stroke-width="2" stroke-linecap="round"/>
    <path d="M18,0 Q9,22 18,44 Q27,66 18,88 Q9,110 18,132 Q27,154 18,176 Q9,198 18,220 Q27,242 18,264 Q9,286 18,308 Q27,330 18,352 Q9,374 18,400" fill="none" stroke="rgba(30,18,8,.22)" stroke-width="1.2" stroke-linecap="round"/>
    <path d="M18,0 Q4,24 18,48 Q32,72 18,96 Q4,120 18,144 Q32,168 18,192 Q4,216 18,240 Q32,264 18,288 Q4,312 18,336 Q32,360 18,384 Q4,396 18,400" fill="none" stroke="rgba(30,18,8,.1)" stroke-width="0.8" stroke-linecap="round"/>
  </svg>
  <div class="shop">{d["shop_name"]}</div>
  <div class="seller">{d["seller_tag"]}</div>
  <div class="divider"></div>
  {"<div class='stars'>"+stars+"</div>" if stars else ""}
  <div class="quote">"</div>
  <div class="review">{d.get("review_text_html", d["review_text"])}</div>
  <div class="divrow"><div class="dline"></div><span class="dmark">✦</span><div class="dline"></div></div>
  <div class="meta">
    {av}
    <div class="meta-info">
      <div class="name">{d["buyer_name"]}</div>
      <div class="date">{date_msk()}</div>
    </div>
    {badge}
  </div>
  <div class="wm">@{d.get("bot_username", "reviewbot")}</div>
</div></body></html>"""


# ══════════════════════════════════════════════════════════════════════════
# Renderer
# ══════════════════════════════════════════════════════════════════════════

HTML_BUILDERS = {
    "classic_gold": html_classic_gold,
    "retro_paper":  html_retro_paper,
    "dark_slate":   html_dark_slate,
    "clean_white":  html_clean_white,
    "sketch_paper": html_sketch_paper,
}


def _render_html(html: str) -> bytes:
    from playwright.sync_api import sync_playwright
    import io as _io

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(
            viewport={"width": 900, "height": 600},
            device_scale_factor=2
        )
        page.set_content(html, wait_until="networkidle")
        try:
            page.evaluate("async () => { await document.fonts.ready; }")
        except Exception:
            pass
        page.wait_for_timeout(350)
        # Расширяем viewport под полную высоту контента (иначе высокие пруфы обрезаются)
        full_height = page.evaluate("document.body.scrollHeight")
        page.set_viewport_size({"width": 900, "height": int(full_height) + 40})
        page.wait_for_timeout(120)
        target = page.query_selector(".frame") or page.query_selector(".card")
        box = target.bounding_box()
        png = page.screenshot(
            type="png",
            clip={"x": box["x"], "y": box["y"],
                  "width": box["width"], "height": box["height"]}
        )
        browser.close()
    return png


def _inject_proof(html: str, proof_b64: str, accent: str = "#c9a84c") -> str:
    """Встраивает блок с фото-пруфом перед watermark/закрытием карточки."""
    proof_block = f"""<div style="margin-top:4px;">
  <div style="font-family:'Montserrat',sans-serif;font-size:12px;color:{accent};letter-spacing:.08em;text-transform:uppercase;margin:18px 0 12px;display:flex;align-items:center;gap:8px;">
    <span style="flex:1;height:1px;background:{accent}33;"></span>
    📸 Доказательство сделки
    <span style="flex:1;height:1px;background:{accent}33;"></span>
  </div>
  <img src="data:image/png;base64,{proof_b64}" style="width:100%;border-radius:10px;border:1px solid {accent}44;display:block;">
</div>"""
    # Вставляем перед watermark если есть, иначе перед последним </div> карточки
    if '<div class="wm"' in html:
        idx = html.find('<div class="wm"')
        return html[:idx] + proof_block + html[idx:]
    # fallback — перед закрытием body
    return html.replace("</body>", proof_block + "</body>", 1)


async def generate_card(data: dict) -> bytes:
    import asyncio, base64
    if data.get("avatar_bytes"):
        data["avatar_b64"] = base64.b64encode(data["avatar_bytes"]).decode()
    else:
        data["avatar_b64"] = None

    # Обрабатываем премиум и обычные эмодзи в тексте отзыва
    bot = data.get("bot")
    entities = data.get("entities")
    if bot and entities:
        data["review_text_html"] = await resolve_custom_emoji(
            data["review_text"], entities, bot
        )
    else:
        data["review_text_html"] = _sanitize_text_for_html(data.get("review_text", ""))

    # Пруф-фото (если есть) — base64
    proof_b64 = None
    if data.get("proof_bytes"):
        proof_b64 = base64.b64encode(data["proof_bytes"]).decode()

    template_id = data.get("template_id", "classic_gold")

    # Кастомный шаблон из конструктора — рендерим через constructor
    if isinstance(template_id, str) and template_id.startswith("custom_"):
        db = data.get("db")
        if db:
            tpl_db_id = int(template_id.replace("custom_", ""))
            ctpl = await db.get_custom_template(tpl_db_id)
            if ctpl:
                from services.constructor import render_with_data
                cfg = {
                    "layout": ctpl["layout"], "font": ctpl["font"],
                    "title_font": ctpl.get("title_font", "caveat"),
                    "text_color": ctpl["text_color"], "accent_color": ctpl["accent_color"],
                    "bg_color": ctpl["bg_color"], "bg_image": ctpl["bg_image"],
                    "creator_username": ctpl["creator_username"], "is_edited": ctpl["is_edited"],
                    "extra_cfg": ctpl.get("extra_cfg") or {},
                }
                cdata = {
                    "shop_name": data["shop_name"],
                    "seller_tag": data.get("seller_tag", ""),
                    "buyer_name": data["buyer_name"],
                    "buyer_initials": data["buyer_initials"],
                    "review_text": data.get("review_text_html") or data["review_text"],
                    "item_bought": data.get("item_bought", ""),
                    "stars": data.get("stars", 0),
                    "avatar_bytes": data.get("avatar_bytes"),
                    "bot_username": data.get("bot_username", "reviewbot"),
                    "proof_b64": proof_b64,
                    "accent_color_for_proof": ctpl["accent_color"],
                }
                return await render_with_data(cfg, cdata)

    builder = HTML_BUILDERS.get(template_id, html_classic_gold)
    html = builder(data)
    if proof_b64:
        # Подбираем акцент под шаблон
        accents = {
            "classic_gold": "#c9a84c", "retro_paper": "#c8a96e",
            "dark_slate": "#58a6ff", "clean_white": "#444",
            "sketch_paper": "#a07840",
        }
        html = _inject_proof(html, proof_b64, accents.get(template_id, "#c9a84c"))
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _render_html, html)
