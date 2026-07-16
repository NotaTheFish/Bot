"""
Админка: управление чатами + кастомизация внешнего вида.

Отключение чата = active=FALSE. НИЧЕГО не удаляется: балансы, леджер, рефералы,
привязки и burn остаются на месте. Включил обратно — всё продолжается с того же места.
"""
import contextlib
from html import escape

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

import db
import keyboards as kb
from services import settings, ui
from services.render import edit as r_edit, parse_free_pair, render

router = Router()

NO_TABLE = ("⚠️ Таблица <code>rb_settings</code> ещё не создана — настройка не сохранена.\n\n"
            "Накати <code>setup.sql</code> в Railway → Postgres → Console:\n"
            "<code>psql -U postgres -d railway -f /setup.sql</code>\n\n"
            "Бот при этом работает — просто на стандартном оформлении.")


class Skin(StatesGroup):
    emoji = State()
    label = State()
    tpl = State()
    free = State()


async def is_admin(uid: int) -> bool:
    from config import SUPER_ADMINS
    return uid in SUPER_ADMINS or bool(await db.admin_chats(uid))


async def deny(c: CallbackQuery) -> bool:
    if await is_admin(c.from_user.id):
        return False
    await c.answer("Нет доступа.", show_alert=True)
    return True


# ==================== ЧАТЫ ====================
@router.callback_query(F.data == "a_chats")
async def cb_chats(c: CallbackQuery):
    if await deny(c):
        return
    chats = await db.pool().fetch("SELECT * FROM rb_chats ORDER BY active DESC, created_at")
    if not chats:
        return await c.answer("Нет привязанных чатов. Напиши /bind в чате.", show_alert=True)
    await ui.edit(c.message, 
        "📢 <b>Чаты</b>\n\n"
        "🟢 активен — выдаёт ссылки, копит рефералов, крутит рулетку\n"
        "⚪️ отключён — новых начислений нет, <b>но весь прогресс сохранён</b>\n\n"
        "Отключение обратимо и ничего не стирает.",
        reply_markup=await kb.chat_admin_list(chats))
    await c.answer()


@router.callback_query(F.data.startswith("a_chat:"))
async def cb_chat_card(c: CallbackQuery):
    if await deny(c):
        return
    await _render_chat(c, int(c.data.split(":")[1]))


async def _render_chat(c: CallbackQuery, cid: int):
    ch = await db.pool().fetchrow("SELECT * FROM rb_chats WHERE chat_id=$1", cid)
    if not ch:
        return await c.answer("Чат не найден.", show_alert=True)
    sx = await settings.ctx()
    st = await db.pool().fetchrow(
        """
        SELECT count(*) FILTER (WHERE status='paid') paid,
               count(*) FILTER (WHERE status='hold') hold,
               count(*) FILTER (WHERE status='void') lost,
               COALESCE(sum(amount) FILTER (WHERE status='paid' AND currency='mushrooms'),0) pm,
               COALESCE(sum(amount) FILTER (WHERE status='paid' AND currency='coins'),0) pc
        FROM rb_referrals WHERE chat_id=$1
        """, cid)
    spins = await db.pool().fetchval("SELECT count(*) FROM rb_spins WHERE chat_id=$1", cid)
    links = await db.pool().fetchval("SELECT count(*) FROM rb_ref_links WHERE chat_id=$1", cid)
    off = ""
    if not ch["active"] and ch["deactivated_at"]:
        off = f"\n⚪️ Отключён {ch['deactivated_at']:%d.%m.%Y %H:%M}"
    await ui.edit(c.message, 
        f"{'🟢' if ch['active'] else '⚪️'} <b>{ch['title']}</b>\n"
        f"<code>{cid}</code>{off}\n\n"
        f"👥 Рефералов: ✅ {st['paid']} | ⏳ {st['hold']} | ❌ {st['lost']}\n"
        f"💰 Выплачено: 🍄 {st['pm']:,} | 🪙 {st['pc']:,}\n"
        f"🎰 Прокруток: {spins}\n"
        f"{sx['e_link']} Реф-ссылок выдано: {links}\n"
        f"🧯 Бюджет сегодня: {ch['budget_spent_mush']:,} / {ch['daily_budget_mush']:,} 🍄"
        .replace(",", " "),
        reply_markup=await kb.chat_card(cid, ch["active"]))
    await c.answer()


@router.callback_query(F.data.startswith("a_choff:"))
async def cb_chat_off(c: CallbackQuery):
    if await deny(c):
        return
    cid = int(c.data.split(":")[1])
    await db.pool().execute(
        "UPDATE rb_chats SET active=FALSE, deactivated_at=now(), deactivated_by=$1 "
        "WHERE chat_id=$2", c.from_user.id, cid)
    await db.audit(c.from_user.id, "chat_off", {"chat_id": cid})
    hold = await db.pool().fetchval(
        "SELECT count(*) FROM rb_referrals WHERE chat_id=$1 AND status='hold'", cid)
    await c.answer(
        f"Чат отключён. Прогресс сохранён.\n"
        f"{hold} холдов заморожены — включишь обратно, продолжат отсчёт.", show_alert=True)
    await _render_chat(c, cid)


@router.callback_query(F.data.startswith("a_chon:"))
async def cb_chat_on(c: CallbackQuery):
    if await deny(c):
        return
    cid = int(c.data.split(":")[1])
    await db.pool().execute(
        "UPDATE rb_chats SET active=TRUE, deactivated_at=NULL, deactivated_by=NULL "
        "WHERE chat_id=$1", cid)
    await db.audit(c.from_user.id, "chat_on", {"chat_id": cid})
    await c.answer("Чат снова активен.", show_alert=True)
    await _render_chat(c, cid)


# ==================== КАСТОМИЗАЦИЯ ====================
@router.callback_query(F.data == "a_skin")
async def cb_skin(c: CallbackQuery, state: FSMContext):
    if await deny(c):
        return
    await state.clear()
    await _render_skin(c)


async def _render_skin(c: CallbackQuery):
    s = await settings.load()
    prem = sum(1 for k in s if k.startswith("premium."))
    custom = sum(1 for k in s if k.startswith(("emoji.", "label.")))
    tpl = "свой" if s.get("profile.template") else "стандартный"
    await ui.edit(c.message, 
        f"🎨 <b>Кастомизация</b>\n\n"
        f"Изменённых эмодзи/названий: <b>{custom}</b>\n"
        f"Премиум-эмодзи: <b>{prem}</b>\n"
        f"Шаблон профиля: <b>{tpl}</b>\n\n"
        f"<i>Всё применяется сразу, без передеплоя.</i>",
        reply_markup=await kb.skin_menu())
    await c.answer()


# ---------- эмодзи ----------
@router.callback_query(F.data == "sk_emoji")
async def cb_sk_emoji(c: CallbackQuery, state: FSMContext):
    if await deny(c):
        return
    await state.clear()
    await _render_emoji(c)


async def _render_emoji(c: CallbackQuery):
    s = await settings.load()
    cur = {slot: s.get(f"emoji.{slot}", d) for slot, (_, d) in settings.EMOJI_SLOTS.items()}
    await ui.edit(c.message, 
        "😀 <b>Эмодзи</b>\n\nВыбери слот. ⭐️ = стоит премиум-эмодзи.",
        reply_markup=await kb.slot_list(settings.EMOJI_SLOTS, {**cur, **s}, "sk_e"))
    await c.answer()


@router.callback_query(F.data.startswith("sk_e:"))
async def cb_sk_slot(c: CallbackQuery, state: FSMContext):
    if await deny(c):
        return
    slot = c.data.split(":")[1]
    await state.set_state(Skin.emoji)
    await state.update_data(slot=slot)
    s = await settings.load()
    now = s.get(f"emoji.{slot}", settings.EMOJI_SLOTS[slot][1])
    cid = s.get(f"premium.{slot}")
    await ui.edit(c.message, 
        f"😀 <b>{settings.EMOJI_SLOTS[slot][0]}</b>\n\n"
        f"Сейчас: {now}" + (f" ⭐️ <code>{cid}</code>" if cid else "") + "\n\n"
        f"<b>Отправь эмодзи одним сообщением.</b>\n"
        f"Обычное — просто эмодзи.\n"
        f"Премиум — отправь премиум-эмодзи, бот сам вытащит его ID.",
        reply_markup=await kb.slot_card(slot, "sk_e", bool(cid)))
    await c.answer()


@router.message(Skin.emoji)
async def sk_emoji_input(msg: Message, state: FSMContext):
    if not await is_admin(msg.from_user.id):
        return await state.clear()
    slot = (await state.get_data())["slot"]
    text = (msg.text or "").strip()
    if not text or len(text) > 16:
        return await ui.answer(msg, "Нужно одно эмодзи. Попробуй ещё раз.")

    # премиум прилетает как custom_emoji entity — вытаскиваем id и символ-фолбэк
    ce = next((e for e in (msg.entities or []) if e.type == "custom_emoji"), None)
    if ce:
        b = text.encode("utf-16-le")
        ch = b[ce.offset * 2:(ce.offset + ce.length) * 2].decode("utf-16-le")
        if not await settings.set(f"emoji.{slot}", ch, msg.from_user.id):
            return await ui.answer(msg, NO_TABLE)
        await settings.set(f"premium.{slot}", ce.custom_emoji_id, msg.from_user.id)
        note = (f"⭐️ Премиум сохранён\nID: <code>{ce.custom_emoji_id}</code>\n"
                f"Фолбэк: {ch}\n\n"
                f"<i>Работает, если у владельца бота есть Telegram Premium "
                f"(либо боту куплен юзернейм на Fragment). Проверь кнопкой «🧪 Тест» — "
                f"он покажет реальный ответ Telegram.</i>")
    else:
        if not await settings.set(f"emoji.{slot}", text, msg.from_user.id):
            return await ui.answer(msg, NO_TABLE)
        await settings.unset(f"premium.{slot}")
        note = f"Сохранено: {text}"

    await state.clear()
    await db.audit(msg.from_user.id, "skin_emoji", {"slot": slot})
    await ui.answer(msg, f"✅ <b>{settings.EMOJI_SLOTS[slot][0]}</b>\n\n{note}",
                     reply_markup=await kb.skin_menu())


@router.callback_query(F.data.startswith("sk_prem_off:"))
async def cb_prem_off(c: CallbackQuery):
    if await deny(c):
        return
    slot = c.data.split(":")[1]
    await settings.unset(f"premium.{slot}")
    await c.answer("Премиум убран, символ остался.", show_alert=True)
    await _render_emoji(c)


# ---------- названия валют ----------
@router.callback_query(F.data == "sk_label")
async def cb_sk_label(c: CallbackQuery, state: FSMContext):
    if await deny(c):
        return
    await state.clear()
    s = await settings.load()
    cur = {slot: "" for slot in settings.LABEL_SLOTS}
    await ui.edit(c.message, 
        "🏷 <b>Названия валют</b>\n\n"
        f"Сейчас: <b>{await settings.label('mushrooms')}</b> / "
        f"<b>{await settings.label('coins')}</b>",
        reply_markup=await kb.slot_list(settings.LABEL_SLOTS, {**cur, **s}, "sk_l"))
    await c.answer()


@router.callback_query(F.data.startswith("sk_l:"))
async def cb_sk_label_slot(c: CallbackQuery, state: FSMContext):
    if await deny(c):
        return
    slot = c.data.split(":")[1]
    await state.set_state(Skin.label)
    await state.update_data(slot=slot)
    await ui.edit(c.message, 
        f"🏷 <b>{settings.LABEL_SLOTS[slot][0]}</b>\n\n"
        f"Сейчас: <b>{await settings.label(slot)}</b>\n\n"
        f"Отправь новое название текстом.",
        reply_markup=await kb.slot_card(slot, "sk_l", False))
    await c.answer()


@router.message(Skin.label)
async def sk_label_input(msg: Message, state: FSMContext):
    if not await is_admin(msg.from_user.id):
        return await state.clear()
    slot = (await state.get_data())["slot"]
    text = (msg.text or "").strip()
    if not text or len(text) > 32:
        return await ui.answer(msg, "От 1 до 32 символов.")
    if not await settings.set(f"label.{slot}", text, msg.from_user.id):
        return await ui.answer(msg, NO_TABLE)
    await state.clear()
    await db.audit(msg.from_user.id, "skin_label", {"slot": slot, "value": text})
    await ui.answer(msg, f"✅ Название: <b>{text}</b>", reply_markup=await kb.skin_menu())


# ---------- сброс слота ----------
@router.callback_query(F.data.startswith("sk_def:"))
async def cb_sk_default(c: CallbackQuery):
    if await deny(c):
        return
    _, prefix, slot = c.data.split(":")
    if prefix == "sk_e":
        await settings.unset(f"emoji.{slot}")
        await settings.unset(f"premium.{slot}")
    else:
        await settings.unset(f"label.{slot}")
    await c.answer("Сброшено к стандартному.", show_alert=True)
    await _render_skin(c)


# ---------- свободные замены ----------
@router.callback_query(F.data == "sk_free")
async def cb_free(c: CallbackQuery, state: FSMContext):
    if await deny(c):
        return
    await state.clear()
    await _render_free(c)


async def _render_free(c: CallbackQuery):
    pairs = await settings.free_map()
    body = "\n".join(f"  {ch} → <code>{cid}</code>" for ch, cid in pairs.items()) or "  пусто"
    await ui.edit(
        c.message,
        f"♻️ <b>Свободные замены</b>\n\n"
        f"Мапят <b>любой символ</b> на премиум-эмодзи. Работает везде, где этот символ "
        f"встретится: в тексте, на кнопках, в рулетке, в админке.\n\n"
        f"Слоты — только для смысловых вещей (какое эмодзи значит «грибы»). "
        f"А тут можно заменить хоть 📊, хоть 🔍, хоть 👁 — всё, что попадётся на глаза.\n\n"
        f"<b>Сейчас ({len(pairs)}):</b>\n{body}",
        reply_markup=await kb.free_list(pairs))
    await c.answer()


@router.callback_query(F.data == "sk_free_add")
async def cb_free_add(c: CallbackQuery, state: FSMContext):
    if await deny(c):
        return
    await state.set_state(Skin.free)
    await ui.edit(
        c.message,
        "➕ <b>Новая замена</b>\n\n"
        "Пришли <b>одним сообщением два эмодзи</b>:\n"
        "сначала обычное (которое заменяем), потом премиум.\n\n"
        "Например: <code>📊</code> затем премиум-эмодзи — и любая 📊 в боте "
        "станет анимированной.",
        reply_markup=await kb.free_list(await settings.free_map()))
    await c.answer()


@router.message(Skin.free)
async def free_input(msg: Message, state: FSMContext):
    if not await is_admin(msg.from_user.id):
        return await state.clear()
    plain, cid, err = parse_free_pair(msg.text or "", msg.entities)
    if err == "no_premium":
        return await ui.answer(msg, "⚠️ Премиум-эмодзи не найдено. Пришли <b>два</b> эмодзи "
                                    "одним сообщением: сначала обычное, потом премиум.")
    if err == "no_plain":
        return await ui.answer(msg, "⚠️ Перед премиумом нет обычного эмодзи. "
                                    "Порядок: сначала обычное, потом премиум.\n\n"
                                    "<i>Если оба эмодзи выглядят одинаково — это нормально, "
                                    "так и должно быть.</i>")
    if err == "too_long":
        return await ui.answer(msg, "⚠️ Первым должно быть одно эмодзи, а не текст.")

    if not await settings.set(f"free.{plain}", cid, msg.from_user.id):
        return await ui.answer(msg, NO_TABLE)
    await state.clear()
    await db.audit(msg.from_user.id, "skin_free", {"char": plain})
    await ui.answer(
        msg, f"✅ Замена сохранена: {plain} → премиум\n"
             f"ID: <code>{cid}</code>\n\n"
             f"Теперь {plain} везде в боте будет анимированной.",
        reply_markup=await kb.skin_menu())


@router.callback_query(F.data.startswith("sk_free_del:"))
async def cb_free_del(c: CallbackQuery):
    if await deny(c):
        return
    ch = c.data.split(":", 1)[1]
    await settings.unset(f"free.{ch}")
    await c.answer(f"Замена {ch} убрана.")
    await _render_free(c)


# ---------- шаблон профиля ----------
@router.callback_query(F.data == "sk_tpl")
async def cb_sk_tpl(c: CallbackQuery, state: FSMContext):
    if await deny(c):
        return
    await state.set_state(Skin.tpl)
    keys = "  ".join("{" + k + "}" for k in settings.PROFILE_KEYS)
    cur = await settings.profile_template()
    await ui.edit(c.message, 
        f"📝 <b>Шаблон профиля</b>\n\n"
        f"Отправь новый текст. Поддерживается HTML: <code>&lt;b&gt;</code>, "
        f"<code>&lt;i&gt;</code>, <code>&lt;code&gt;</code>.\n\n"
        f"<b>Плейсхолдеры:</b>\n<code>{keys}</code>\n\n"
        f"<code>e_*</code> — эмодзи слота, <code>l_*</code> — название валюты.\n"
        f"Перед сохранением проверю, что шаблон не падает.\n\n"
        f"<b>Текущий:</b>\n<pre>{cur.replace('<', '&lt;')}</pre>",
        reply_markup=await kb.slot_card("tpl", "sk_l", False))
    await c.answer()


@router.message(Skin.tpl)
async def sk_tpl_input(msg: Message, state: FSMContext):
    if not await is_admin(msg.from_user.id):
        return await state.clear()
    tpl = msg.html_text or ""
    err = settings.validate_template(tpl)
    if err:
        return await ui.answer(msg, f"⚠️ {err}\n\nИсправь и пришли снова.")
    if not await settings.set("profile.template", tpl, msg.from_user.id):
        return await ui.answer(msg, NO_TABLE)
    await state.clear()
    await db.audit(msg.from_user.id, "skin_tpl", {"len": len(tpl)})
    await ui.answer(msg, "✅ Шаблон сохранён. Открой «👤 Профиль» и глянь.",
                     reply_markup=await kb.skin_menu())


# ---------- тест премиума ----------
@router.callback_query(F.data == "sk_test")
async def cb_sk_test(c: CallbackQuery):
    """
    Эмпирическая проверка. Bot API 9.4 (09.02.2026) разрешил боту custom_emoji,
    если у ВЛАДЕЛЬЦА бота есть Telegram Premium — Fragment больше не обязателен.
    Но в доке сказано "messages directly sent", про editMessageText молчок.
    Анимация рулетки построена на редактировании, поэтому проверяем ОБА пути
    отдельно и показываем настоящую ошибку от Telegram, а не догадки.
    """
    if await deny(c):
        return
    em = await settings.emoji_map()
    if not em:
        return await c.answer("Премиум-эмодзи не настроены ни в одном слоте. "
                              "Сначала задай их в «😀 Эмодзи».", show_alert=True)
    await c.answer("Проверяю оба пути…")
    line = "  ".join(em.keys())
    res = []

    # 1) sendMessage — путь, который дока разрешает явно
    text, ents = render(f"1️⃣ <b>Отправка</b> (sendMessage)\n{line}", em)
    try:
        await c.bot.send_message(c.from_user.id, text, entities=ents, parse_mode=None)
        send_ok = True
        res.append("✅ <b>Отправка</b> — премиум принят")
    except Exception as e:
        send_ok = False
        res.append(f"❌ <b>Отправка</b> — отклонено\n<code>{escape(str(e))[:180]}</code>")

    # 2) editMessageText — на нём держится анимация рулетки
    try:
        m = await c.bot.send_message(c.from_user.id, "2️⃣ Редактирование…")
        text2, ents2 = render(f"2️⃣ <b>Редактирование</b> (editMessageText)\n{line}", em)
        await ui.edit(m, text2, entities=ents2, parse_mode=None)
        edit_ok = True
        res.append("✅ <b>Редактирование</b> — премиум принят")
    except Exception as e:
        edit_ok = False
        res.append(f"❌ <b>Редактирование</b> — отклонено\n<code>{escape(str(e))[:180]}</code>")

    if send_ok and edit_ok:
        verdict = ("🎉 Работает везде. Рулетка, профиль, баланс — всё с премиумом.")
    elif send_ok and not edit_ok:
        verdict = ("⚠️ Отправка — да, редактирование — нет.\n"
                   "Значит анимация рулетки премиум не потянет: она построена на "
                   "editMessageText. Скажи — переделаю финальную карточку на "
                   "отдельную отправку, тогда премиум будет и там.")
    elif not send_ok:
        verdict = ("❌ Премиум не проходит вообще.\n\n"
                   "Проверь: 1) Telegram Premium активен у аккаунта, которым ты "
                   "создавал бота в BotFather (важен именно ВЛАДЕЛЕЦ, не ты как "
                   "участник чата); 2) эмодзи взят из доступного тебе набора.\n"
                   "Альтернатива — доп. юзернейм боту на Fragment.")

    await c.bot.send_message(
        c.from_user.id,
        "🧪 <b>Тест премиум-эмодзи</b>\n\n" + "\n\n".join(res) +
        f"\n\n{verdict}\n\n"
        f"<i>Видишь анимированные эмодзи в пунктах выше — путь работает. "
        f"Видишь обычные, но галочка ✅ — Telegram принял, просто у тебя "
        f"не отрисовалось.</i>",
        reply_markup=await kb.skin_menu())


# ---------- полный сброс ----------
@router.callback_query(F.data == "sk_reset")
async def cb_sk_reset(c: CallbackQuery):
    if await deny(c):
        return
    await ui.edit(c.message, 
        "♻️ Сбросить <b>всю</b> кастомизацию — эмодзи, названия, шаблон?\n\n"
        "<i>Балансы и рефералы не тронутся, только внешний вид.</i>",
        reply_markup=await kb.confirm("sk_reset_yes"))
    await c.answer()


@router.callback_query(F.data == "sk_reset_yes")
async def cb_sk_reset_yes(c: CallbackQuery, state: FSMContext):
    if await deny(c):
        return
    try:
        await db.pool().execute("DELETE FROM rb_settings")
    except Exception:
        return await c.answer("Таблица rb_settings ещё не создана.", show_alert=True)
    await settings.load(force=True)
    await db.audit(c.from_user.id, "skin_reset", {})
    await c.answer("Всё сброшено к стандартному.", show_alert=True)
    await _render_skin(c)
