from aiogram.types import (InlineKeyboardButton, InlineKeyboardMarkup,
                           KeyboardButton, ReplyKeyboardMarkup)
from aiogram.utils.keyboard import InlineKeyboardBuilder

from services import settings
from services.ui import btn


async def main_menu(currency: str, is_admin: bool,
                    show_casino: bool = False) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "Профиль", "profile", "profile")
    await btn(kb, "Моя ссылка", "mylink", "link")
    await btn(kb, f"Валюта: {await settings.label(currency)}", "toggle_cur", currency)
    await btn(kb, "Мои рефералы", "myrefs", "refs")
    await btn(kb, "Вывод", "wd_menu", "withdraw")
    if show_casino:
        await btn(kb, "🎰 Казино", "casino")
    if is_admin:
        await btn(kb, "Админка", "admin", "admin")
    kb.adjust(2, 1, 2, 1, 1)
    return kb.as_markup()


async def wd_menu(has_active: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if has_active:
        await btn(kb, "✏️ Изменить сумму", "wd_cur")
        await btn(kb, "❌ Отменить заявку", "wd_cancel")
    else:
        await btn(kb, "Создать заявку", "wd_cur", "withdraw")
    await btn(kb, "Назад", "menu", "back")
    kb.adjust(1)
    return kb.as_markup()


async def wd_currency() -> InlineKeyboardMarkup:
    """Выбор валюты вывода."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "🍄 Грибы", "wdcur:mushrooms")
    await btn(kb, "🪙 Коины", "wdcur:coins")
    await btn(kb, "Назад", "wd_menu", "back")
    kb.adjust(2, 1)
    return kb.as_markup()


async def find_card(tg_id: int, banned: bool, manage: bool = True) -> InlineKeyboardMarkup:
    """Тумблер под найденным юзером: не забанен -> «🥳 бан» (забанит),
    забанен -> «😡 бан» (разбанит). У второстепенного тумблера нет — он не банит.
    Плюс начисление/изъятие валюты (только главный админ)."""
    kb = InlineKeyboardBuilder()
    if manage:
        await btn(kb, "➕ Зачислить", f"a_give:{tg_id}")
        await btn(kb, "➖ Изъять", f"a_take:{tg_id}")
        if banned:
            await btn(kb, "😡 бан", f"a_toggleban:{tg_id}")
        else:
            await btn(kb, "🥳 бан", f"a_toggleban:{tg_id}")
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(2, 1, 1)
    return kb.as_markup()


async def adj_currency(tg_id: int, action: str) -> InlineKeyboardMarkup:
    """Выбор валюты для начисления/изъятия. action = give|take."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "🍄 Грибы", f"a_adjcur:{action}:{tg_id}:mushrooms")
    await btn(kb, "🪙 Коины", f"a_adjcur:{action}:{tg_id}:coins")
    await btn(kb, "Отмена", f"a_findback:{tg_id}", "back")
    kb.adjust(2, 1)
    return kb.as_markup()


async def adj_amount(tg_id: int, action: str, cur: str, show_max: bool) -> InlineKeyboardMarkup:
    """Кнопка max (только для изъятия) + отмена. Сумму игрок вводит текстом."""
    kb = InlineKeyboardBuilder()
    if show_max:
        await btn(kb, "Забрать всё (max)", f"a_adjmax:{action}:{tg_id}:{cur}")
    await btn(kb, "Отмена", f"a_findback:{tg_id}", "back")
    kb.adjust(1)
    return kb.as_markup()


async def admin_wd_card(wid: int, version: int, tg_id: int | None = None,
                        user=None) -> InlineKeyboardMarkup:
    # version зашит в кнопку. Юзер поменял сумму -> version вырос -> старая кнопка мертва.
    kb = InlineKeyboardBuilder()
    if tg_id:
        name = (f"@{user['username']}" if user and user.get("username")
                else ((user["first_name"] if user else None) or str(tg_id)))
        url = (f"https://t.me/{user['username']}" if user and user.get("username")
               else f"tg://user?id={tg_id}")
        kb.button(text=f"👤 {name}", url=url)
    await btn(kb, "✅ Подтвердить вывод", f"wdok:{wid}:{version}")
    await btn(kb, "🚫 Отклонить", f"wdno:{wid}:{version}")
    kb.adjust(1)
    return kb.as_markup()


async def confirm_kbcast() -> InlineKeyboardMarkup:
    """Подтверждение массовой рассылки reply-клавиатуры."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "📢 Запустить рассылку", "a_kbcast_go")
    await btn(kb, "Отмена", "admin", "back")
    kb.adjust(1)
    return kb.as_markup()


async def admin_menu(manage: bool = True) -> InlineKeyboardMarkup:
    """
    manage=True  — главный админ: все кнопки, включая действия.
    manage=False — второстепенный (PAYOUT_ADMINS): видит всё, но «Кастомизация»
                   скрыта (это чистое действие). Баны/Чаты/Найти он видит, но
                   внутри кнопки-действия ему не дадут — там свой гейт.
    """
    kb = InlineKeyboardBuilder()
    await btn(kb, "Топ-25", "a_top", "top")
    await btn(kb, "🔍 Найти юзера", "a_find")
    await btn(kb, "💸 Заявки на вывод", "a_pending")
    await btn(kb, "🚩 На проверке", "a_flagged")
    await btn(kb, "📊 Сводка", "a_stats")
    await btn(kb, "Чаты", "a_chats", "chat")
    await btn(kb, "🚫 Баны", "a_bans")
    if manage:
        await btn(kb, "🎁 Розыгрыши", "gw_menu")
        await btn(kb, "🎰 Казино (доступ)", "casino_admin")
        await btn(kb, "🎟 Промокоды", "promo_menu")
        await btn(kb, "🎨 Кастомизация", "a_skin")
        await btn(kb, "📢 Разослать меню", "a_kbcast")
    await btn(kb, "Назад", "menu", "back")
    kb.adjust(2, 2, 2, 2, 1, 1, 1, 1, 1)
    return kb.as_markup()


async def chat_admin_list(chats, manage: bool = True) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for ch in chats:
        active = ch.get("referral") or ch.get("roulette") or ch.get("contest")
        mark = "🟢" if active else "⚪️"
        # Карточка (a_chat:) управляет рефералкой и существует только для чатов
        # с рефералкой. Чат без неё (только рулетка/конкурс) карточки не имеет.
        # Второстепенному все чаты некликабельны (просмотр без действий).
        if manage and ch.get("referral"):
            cb = f"a_chat:{ch['chat_id']}"
        else:
            cb = "a_noop"
        title = ch.get("title") or str(ch["chat_id"])
        await btn(kb, f"{mark} {title}", cb)
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(1)
    return kb.as_markup()


async def chat_card(chat_id: int, active: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if active:
        await btn(kb, "⚪️ Отключить чат", f"a_choff:{chat_id}")
    else:
        await btn(kb, "🟢 Включить обратно", f"a_chon:{chat_id}")
    await btn(kb, "К списку", "a_chats", "back")
    kb.adjust(1)
    return kb.as_markup()


async def skin_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "😀 Эмодзи", "sk_emoji")
    await btn(kb, "🏷 Названия валют", "sk_label")
    await btn(kb, "♻️ Свободные замены", "sk_free")
    await btn(kb, "⏳ Реакция ожидания", "sk_wait")
    await btn(kb, "📝 Шаблон профиля", "sk_tpl")
    await btn(kb, "🧪 Тест премиум-эмодзи", "sk_test")
    await btn(kb, "🗑 Сбросить всё", "sk_reset")
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(2, 1, 1, 1, 1, 1, 1)
    return kb.as_markup()


async def slot_list(slots: dict, current: dict, prefix: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for slot, (desc, _) in slots.items():
        star = " ⭐️" if current.get(f"premium.{slot}") else ""
        await btn(kb, f"{current.get(slot, '')} {desc}{star}".strip(), f"{prefix}:{slot}")
    await btn(kb, "Кастомизация", "a_skin", "back")
    kb.adjust(2)
    return kb.as_markup()


async def slot_card(slot: str, prefix: str, has_premium: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if has_premium:
        await btn(kb, "🗑 Убрать премиум", f"sk_prem_off:{slot}")
    await btn(kb, "♻️ Сброс к дефолту", f"sk_def:{prefix}:{slot}")
    await btn(kb, "Назад", "sk_emoji" if prefix == "sk_e" else "sk_label", "back")
    kb.adjust(1)
    return kb.as_markup()


async def pending_list(rows) -> InlineKeyboardMarkup:
    """Кнопка на каждую заявку — прислать карточку с кнопками заново."""
    kb = InlineKeyboardBuilder()
    for w in rows[:20]:
        await btn(kb, f"#{w['id']} — прислать карточку", f"a_wdcard:{w['id']}")
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(1)
    return kb.as_markup()


async def bans_panel(rows, links_mode: bool, manage: bool = True) -> InlineKeyboardMarkup:
    """
    links_mode=False — обычный вид: заголовок-строки (текст в сообщении), тут только действия.
    links_mode=True  — на месте действий появляются кнопки-ссылки tg://user на каждого.
    """
    kb = InlineKeyboardBuilder()
    if links_mode:
        for r in rows:
            name = f"@{r['username']}" if r["username"] else (r["first_name"] or str(r["tg_id"]))
            kb.button(text=f"👤 {name}", url=f"tg://user?id={r['tg_id']}")
        await btn(kb, "⬅️ Назад к списку", "a_bans")
        kb.adjust(1)
        return kb.as_markup()

    if rows:
        await btn(kb, "🔗 Ссылки на профили", "a_bans_links")
        if manage:
            await btn(kb, "♻️ Разбанить по ID", "a_unban")
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(1)
    return kb.as_markup()


async def confirm(action: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "✅ Да", action)
    await btn(kb, "❌ Нет", "a_skin")
    kb.adjust(2)
    return kb.as_markup()


async def free_list(pairs: dict) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for ch in pairs:
        await btn(kb, f"{ch}  ✕", f"sk_free_del:{ch}")
    await btn(kb, "➕ Добавить замену", "sk_free_add")
    await btn(kb, "Кастомизация", "a_skin", "back")
    kb.adjust(4, 1, 1)
    return kb.as_markup()


async def back_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "Меню", "menu", "back")
    return kb.as_markup()


async def chat_picker(chats, prefix: str) -> InlineKeyboardMarkup:
    """prefix: 'lnk' — выбор чата для реф-ссылки."""
    kb = InlineKeyboardBuilder()
    for ch in chats:
        await btn(kb, ch["title"], f"{prefix}:{ch['chat_id']}", "chat")
    await btn(kb, "Меню", "menu", "back")
    kb.adjust(1)
    return kb.as_markup()


async def link_card(multi: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if multi:
        await btn(kb, "🔄 Другой чат", "mylink", "chat")
    await btn(kb, "Меню", "menu", "back")
    kb.adjust(1)
    return kb.as_markup()


# ==================== РОЗЫГРЫШИ ====================
async def gw_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "➕ Создать розыгрыш", "gw_new")
    await btn(kb, "🎁 Мои розыгрыши", "gw_list")
    await btn(kb, "⚖️ Страйки и баны", "gw_strikes")
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(1)
    return kb.as_markup()


async def gw_reward_mode() -> InlineKeyboardMarkup:
    """Выбор режима валюты при создании."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "🍄 Только грибы", "gwr:mushrooms")
    await btn(kb, "🪙 Только коины", "gwr:coins")
    await btn(kb, "🔄 На выбор (грибы/коины)", "gwr:choice")
    await btn(kb, "🎁 Обе сразу (грибы + коины)", "gwr:both")
    await btn(kb, "🎀 Другое (выдам лично)", "gwr:other")
    kb.adjust(2, 1, 1, 1)
    return kb.as_markup()


async def gw_prize_method(both: bool = False) -> InlineKeyboardMarkup:
    """Как задать призы: поровну или вручную по местам."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "⚖️ Распределить поровну", "gwp:equal")
    await btn(kb, "✏️ Задать по местам", "gwp:manual")
    kb.adjust(1)
    return kb.as_markup()


async def gw_timer_choice() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "⏰ Задать таймер", "gwt:set")
    await btn(kb, "✋ Без таймера (вручную)", "gwt:none")
    kb.adjust(1)
    return kb.as_markup()


async def gw_confirm() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "✅ Создать", "gw_save")
    await btn(kb, "❌ Отмена", "gw_menu")
    kb.adjust(2)
    return kb.as_markup()


async def gw_card(gid: int, status: str) -> InlineKeyboardMarkup:
    """Карточка розыгрыша в «Мои розыгрыши»."""
    kb = InlineKeyboardBuilder()
    if status == "draft":
        await btn(kb, "🚀 Запустить", f"gw_run:{gid}")
    if status == "running":
        await btn(kb, "👥 Участники", f"gw_members:{gid}")
        await btn(kb, "🏁 Завершить", f"gw_finish:{gid}")
    await btn(kb, "🗑 Удалить", f"gw_del:{gid}")
    await btn(kb, "К списку", "gw_list", "back")
    kb.adjust(1)
    return kb.as_markup()


async def back_to(callback: str) -> InlineKeyboardMarkup:
    """Одна кнопка «Назад» на заданный callback."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "Назад", callback, "back")
    return kb.as_markup()


async def gw_subscribe(chats: list, gid: int, ready: bool) -> InlineKeyboardMarkup:
    """
    Экран участия в ЛС: кнопки-ссылки на подписку + «Проверить и участвовать».
    ready — если True, показываем кнопку участия (после проверки).
    Всегда есть выход «☰ Меню» — чтобы не застрять на этом экране (deep-link /start
    может возвращать сюда повторно).
    """
    kb = InlineKeyboardBuilder()
    for ch in chats:
        title = ch.get("title") or "чат"
        link = ch.get("invite_link")
        if link:
            kb.button(text=f"➡️ {title}", url=link)
    await btn(kb, "✅ Проверить подписку", f"gwjoin:{gid}")
    await btn(kb, "☰ Главное меню", "menu", "back")
    kb.adjust(1)
    return kb.as_markup()


async def gw_currency_choice(gid: int) -> InlineKeyboardMarkup:
    """Выбор валюты для режима choice."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "🍄 Грибы", f"gwcur:{gid}:mushrooms")
    await btn(kb, "🪙 Коины", f"gwcur:{gid}:coins")
    await btn(kb, "☰ Главное меню", "menu", "back")
    kb.adjust(2, 1)
    return kb.as_markup()


# ==================== REPLY-КЛАВИАТУРА ====================
def menu_reply() -> ReplyKeyboardMarkup:
    """
    Reply-клавиатура внизу: одна кнопка «☰ Меню».

    is_persistent НЕ ставим: с ним Telegram ломает нативную кнопку
    сворачивания/разворачивания (видна, но не жмётся — подтверждено багтрекером
    Telegram). Кнопка сворачивания для пользователя важнее вечного залипания.
    Если клавиатура пропала (Telegram сбрасывает её при синхронизации/долгих
    сессиях) — пользователь возвращает её командой /start.
    """
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="☰ Меню")]],
        resize_keyboard=True,
        input_field_placeholder="Жми ☰ Меню или пиши сообщение")


async def gw_skip_photo(step: str) -> InlineKeyboardMarkup:
    """Кнопка «Без фото» на шаге фото розыгрыша. step: announce|finish."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "⏭ Без фото", f"gwphoto_skip:{step}")
    await btn(kb, "Отмена", "gw_menu", "back")
    kb.adjust(1)
    return kb.as_markup()


async def to_menu() -> InlineKeyboardMarkup:
    """Одна кнопка «☰ Главное меню» — выход в главное меню из любого тупика."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "☰ Главное меню", "menu", "back")
    return kb.as_markup()


# ==================== КАЗИНО ====================
async def casino_menu() -> InlineKeyboardMarkup:
    """Меню казино: кейсы, рулетка, карточки. Слоты — позже."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "📦 Кейсы", "casino_cases")
    await btn(kb, "🎡 Рулетка", "wheel")
    await btn(kb, "🃏 Карточки", "mines")
    await btn(kb, "Назад", "menu", "back")
    kb.adjust(1)
    return kb.as_markup()


async def wheel_again(bet: int, cur: str) -> InlineKeyboardMarkup:
    """После спина колеса: крутить снова той же ставкой или назад."""
    kb = InlineKeyboardBuilder()
    await btn(kb, f"🎡 Крутить ещё ({bet:,})".replace(",", " "), f"wheelspin:{bet}:{cur}")
    await btn(kb, "🎰 Другая ставка", "wheel")
    await btn(kb, "Меню", "menu", "back")
    kb.adjust(1)
    return kb.as_markup()


async def wheel_back() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "Казино", "casino", "back")
    return kb.as_markup()


async def wheel_menu(style: str) -> InlineKeyboardMarkup:
    """Меню рулетки: переключатель стиля анимации + назад."""
    kb = InlineKeyboardBuilder()
    label = "🎞 Анимация: барабан" if style == "drum" else "🎞 Анимация: бегунок"
    await btn(kb, label, "wheel_anim_toggle")
    await btn(kb, "Казино", "casino", "back")
    kb.adjust(1)
    return kb.as_markup()


async def casino_cases(cases: list) -> InlineKeyboardMarkup:
    """Список кейсов. cases: [(key, название, цена_в_грибах)]."""
    kb = InlineKeyboardBuilder()
    for key, title, price in cases:
        await btn(kb, f"{title} — {price:,}".replace(",", " "), f"case:{key}")
    await btn(kb, "Назад", "casino", "back")
    kb.adjust(1)
    return kb.as_markup()


async def case_currency(case_key: str) -> InlineKeyboardMarkup:
    """Выбор валюты оплаты кейса."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "🍄 Грибами", f"casebuy:{case_key}:mushrooms")
    await btn(kb, "🪙 Коинами", f"casebuy:{case_key}:coins")
    await btn(kb, "Назад", "casino_cases", "back")
    kb.adjust(2, 1)
    return kb.as_markup()


async def case_again(case_key: str) -> InlineKeyboardMarkup:
    """После открытия: открыть ещё или назад."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "🔁 Открыть ещё", f"case:{case_key}")
    await btn(kb, "📦 К кейсам", "casino_cases")
    await btn(kb, "Меню", "menu", "back")
    kb.adjust(1)
    return kb.as_markup()


async def casino_admin_toggle(enabled: bool) -> InlineKeyboardMarkup:
    """Переключатель доступа к казино в админке."""
    kb = InlineKeyboardBuilder()
    state = "🟢 Открыто всем" if enabled else "🔴 Только админ"
    await btn(kb, f"Казино: {state} (переключить)", "casino_toggle")
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(1)
    return kb.as_markup()


# ==================== ПРОМОКОДЫ ====================
async def promo_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "📋 Все промо", "promo_all")
    await btn(kb, "➕ Создать промо", "promo_new")
    await btn(kb, "Админка", "admin", "back")
    kb.adjust(1)
    return kb.as_markup()


async def promo_kind() -> InlineKeyboardMarkup:
    """Выбор валюты выдачи промокода. По одному эмодзи на кнопку (премиум)."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "🔁 По курсу", "pkind:rate")
    await btn(kb, "🍄 Только грибы", "pkind:mushrooms")
    await btn(kb, "🪙 Только коины", "pkind:coins")
    kb.adjust(1)
    return kb.as_markup()


async def promo_back() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "Назад", "promo_menu", "back")
    return kb.as_markup()


async def promo_list_kb(rows) -> InlineKeyboardMarkup:
    """Список промо: активные кликабельны (открыть карточку с удалением)."""
    from services import promo as promo_svc
    kb = InlineKeyboardBuilder()
    for p in rows:
        # активные кликабельны, просроченные — просто в тексте (кнопкой удалить тоже даём)
        flag = "🔴" if not promo_svc.is_active(p) else "🟢"
        await btn(kb, f"{flag} {p['code']}", f"promo_view:{p['id']}")
    await btn(kb, "Назад", "promo_menu", "back")
    kb.adjust(1)
    return kb.as_markup()


async def promo_card(pid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    await btn(kb, "🗑 Удалить", f"promo_del:{pid}")
    await btn(kb, "Назад", "promo_all", "back")
    kb.adjust(1)
    return kb.as_markup()


# ==================== КАРТОЧКИ (mines) ====================
async def mines_bet(bets: list) -> InlineKeyboardMarkup:
    """Выбор ставки для карточек."""
    kb = InlineKeyboardBuilder()
    for b in bets:
        await btn(kb, f"{b:,}".replace(",", " "), f"mbet:{b}")
    await btn(kb, "Казино", "casino", "back")
    kb.adjust(1)
    return kb.as_markup()


async def mines_field(presets: dict, bet: int) -> InlineKeyboardMarkup:
    """Выбор поля (пресета) после ставки."""
    kb = InlineKeyboardBuilder()
    for key, (total, mines, label) in presets.items():
        await btn(kb, label, f"mfield:{bet}:{key}")
    await btn(kb, "Назад", "mines", "back")
    kb.adjust(1)
    return kb.as_markup()


async def mines_grid(total: int, opened: dict, mult: float = 1.0,
                     can_cashout: bool = False, cols: int = None) -> InlineKeyboardMarkup:
    """
    Игровое поле + кнопка «Забрать» снизу. opened: {index: '💎'|'💣'}.
    Неоткрытые — пустые кнопки. Раскладка до 6 в ряд.
    """
    import math
    if cols is None:
        cols = {9: 3, 16: 4, 25: 5, 36: 6}.get(total, min(6, int(math.isqrt(total)) or 1))
    kb = InlineKeyboardBuilder()
    for i in range(total):
        if i in opened:
            await btn(kb, opened[i], "mnoop")
        else:
            # закрытая карта — видимый символ (невидимый \u2063 Telegram иногда
            # схлопывал, и кнопка переставала нажиматься). 🎴 надёжно кликается.
            await btn(kb, "🎴", f"mopen:{i}")
    rows = [cols] * ((total + cols - 1) // cols)
    if can_cashout:
        await btn(kb, f"💰 Забрать (×{mult:g})", "mcashout")
        rows.append(1)
    kb.adjust(*rows)
    return kb.as_markup()


async def mines_after() -> InlineKeyboardMarkup:
    """После раунда: играть ещё (та же ставка+поле) или к настройкам."""
    kb = InlineKeyboardBuilder()
    await btn(kb, "🔁 Играть ещё", "magain")
    await btn(kb, "⚙️ Настройки", "mines")
    await btn(kb, "Меню", "menu", "back")
    kb.adjust(1)
    return kb.as_markup()
