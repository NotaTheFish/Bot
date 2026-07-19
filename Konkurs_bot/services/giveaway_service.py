import logging
import re
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

from database import db
from keyboards.kb import channels_kb

logger = logging.getLogger(__name__)

# Premium (custom) emoji are allowed for bots in private/group/supergroup chats
# when the bot OWNER has Telegram Premium (Bot API 9.4, Feb 2026).
# Channels are NOT covered — there we transparently fall back to plain emoji.
TG_EMOJI_RE = re.compile(r'<tg-emoji[^>]*>(.*?)</tg-emoji>', re.DOTALL | re.IGNORECASE)


def strip_custom_emoji(html_text: str) -> str:
    """Replace <tg-emoji ...>X</tg-emoji> with the fallback emoji X inside it."""
    return TG_EMOJI_RE.sub(r'\1', html_text)


def _is_emoji_error(err: Exception) -> bool:
    msg = str(err).lower()
    return "emoji" in msg or "premium" in msg


async def send_html_smart(bot: Bot, chat_id: int, html_text: str, reply_markup=None):
    """Send HTML; if the chat rejects custom emoji (e.g. channels), retry without them."""
    try:
        return await bot.send_message(chat_id, html_text, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if not _is_emoji_error(e):
            raise
        logger.info(f"Custom emoji rejected in {chat_id}, retrying with plain emoji: {e}")
        return await bot.send_message(chat_id, strip_custom_emoji(html_text), reply_markup=reply_markup)


async def edit_html_smart(bot: Bot, chat_id: int, message_id: int, html_text: str, reply_markup=None):
    """Edit with HTML; retry without custom emoji if the chat rejects them."""
    try:
        return await bot.edit_message_text(
            text=html_text, chat_id=chat_id, message_id=message_id, reply_markup=reply_markup
        )
    except TelegramBadRequest as e:
        if not _is_emoji_error(e):
            raise
        return await bot.edit_message_text(
            text=strip_custom_emoji(html_text), chat_id=chat_id,
            message_id=message_id, reply_markup=reply_markup
        )


# ── Channel title / invite link maintenance ────────────────────────────────────

async def ensure_channel_info(bot: Bot, channel: dict) -> dict:
    """Make sure chat_title is a real name (not raw ID). Returns updated dict (does not touch invite_link)."""
    title = channel.get('chat_title') or ''
    if not title or title.lstrip('-').isdigit():
        try:
            chat_info = await bot.get_chat(channel['chat_id'])
            title = chat_info.title or str(channel['chat_id'])
            await db.update_channel_title(channel['id'], title)
        except Exception as e:
            logger.warning(f"Could not fetch title for {channel['chat_id']}: {e}")
            title = str(channel['chat_id'])
    return {**channel, 'chat_title': title}


async def make_invite_link(bot: Bot, chat_id: int) -> str | None:
    """
    Create a proper public join link (https://t.me/+...) that works for users who
    are NOT yet members. Uses createChatInviteLink (a NEW named link that does not
    disturb the chat's primary link). Falls back to the public @username link if the
    chat has one. Returns None if nothing works.
    """
    # 1. Preferred: a fresh unlimited invite link (no join request, no expiry)
    try:
        result = await bot.create_chat_invite_link(chat_id, creates_join_request=False)
        return result.invite_link
    except Exception as e:
        logger.warning(f"create_chat_invite_link({chat_id}) failed: {e}")

    # 2. If the chat is public, use its @username link — always joinable
    try:
        chat = await bot.get_chat(chat_id)
        if getattr(chat, 'username', None):
            return f"https://t.me/{chat.username}"
    except Exception as e:
        logger.warning(f"get_chat({chat_id}) for username fallback failed: {e}")

    # 3. Last resort: the primary link via export (may rotate it, but better than nothing)
    try:
        return await bot.export_chat_invite_link(chat_id)
    except Exception as e:
        logger.error(f"All invite-link methods failed for {chat_id}: {e}")
        return None


async def refresh_invite_link(bot: Bot, channel: dict) -> dict:
    """Create a fresh working invite link and persist it."""
    new_link = await make_invite_link(bot, channel['chat_id'])
    if new_link:
        await db.update_channel_invite_link(channel['id'], new_link)
        return {**channel, 'invite_link': new_link}
    logger.error(f"Could not refresh invite link for {channel['chat_id']}")
    return channel


def _is_bad_invite_link(link: str | None) -> bool:
    """Detect the old broken internal-style link that non-members can't open."""
    if not link:
        return True
    return "t.me/c/" in link


async def get_or_create_invite_link(bot: Bot, channel: dict) -> dict:
    """Use the stored invite link if it's a valid public one; otherwise create a new one."""
    if not _is_bad_invite_link(channel.get('invite_link')):
        return channel
    return await refresh_invite_link(bot, channel)


async def prepare_channels(bot: Bot, giveaway_id: int) -> list[dict]:
    """Fetch channels, fix missing titles, ensure each has an invite link (without rotating existing ones)."""
    channels = await db.get_channels(giveaway_id)
    fixed = []
    for ch in channels:
        ch = await ensure_channel_info(bot, ch)
        ch = await get_or_create_invite_link(bot, ch)
        fixed.append(ch)
    return fixed


# ── Publish & live-edit announcements ──────────────────────────────────────────

async def publish_announcement(bot: Bot, giveaway_id: int, target_chat_id: int) -> int | None:
    """Send the announcement to a chat/channel and remember the message for later edits."""
    g = await db.get_giveaway_by_id(giveaway_id)
    if not g:
        return None
    channels = await prepare_channels(bot, giveaway_id)
    kb = channels_kb(channels, giveaway_id)

    msg = await send_html_smart(bot, target_chat_id, g['announcement'], reply_markup=kb)
    await db.add_published_message(giveaway_id, target_chat_id, msg.message_id)
    return msg.message_id


async def sync_published_messages(bot: Bot, giveaway_id: int):
    """Re-render all previously published announcement messages (text + buttons) with current data."""
    g = await db.get_giveaway_by_id(giveaway_id)
    if not g:
        return
    channels = await prepare_channels(bot, giveaway_id)
    kb = channels_kb(channels, giveaway_id)

    published = await db.get_published_messages(giveaway_id)
    for pm in published:
        try:
            await edit_html_smart(
                bot, pm['chat_id'], pm['message_id'], g['announcement'], reply_markup=kb
            )
        except TelegramBadRequest as e:
            # Message not modified, deleted, or too old to edit — ignore quietly
            logger.info(f"Could not edit message {pm['message_id']} in {pm['chat_id']}: {e}")
        except Exception as e:
            logger.warning(f"Unexpected error editing message {pm['message_id']}: {e}")


async def refresh_all_invite_links(bot: Bot, giveaway_id: int) -> list[dict]:
    """Manually regenerate invite links for every channel in a giveaway, then sync messages."""
    channels = await db.get_channels(giveaway_id)
    updated = []
    for ch in channels:
        ch = await ensure_channel_info(bot, ch)
        ch = await refresh_invite_link(bot, ch)
        updated.append(ch)
    await sync_published_messages(bot, giveaway_id)
    return updated


# ── Subscription verification ──────────────────────────────────────────────────

async def check_user_subscriptions(bot: Bot, user_id: int, channels: list[dict]) -> list[dict]:
    """Returns list of channels the user is NOT currently subscribed to."""
    not_subscribed = []
    for ch in channels:
        try:
            member = await bot.get_chat_member(ch['chat_id'], user_id)
            if member.status in ('left', 'kicked', 'banned'):
                not_subscribed.append(ch)
        except Exception as e:
            logger.warning(f"Cannot check subscription for chat {ch['chat_id']}: {e}")
            not_subscribed.append(ch)
    return not_subscribed


async def revalidate_participants(bot: Bot, giveaway_id: int, admin_id: int) -> dict:
    """
    Re-check every registered participant's subscriptions.
    Marks newly-unsubscribed users, un-marks resubscribed ones, notifies both
    the user and the admin. Returns a summary dict.
    """
    g = await db.get_giveaway_by_id(giveaway_id)
    if not g:
        return {"checked": 0, "newly_unsubscribed": 0, "resubscribed": 0}

    channels = await db.get_channels(giveaway_id)
    participants = await db.get_participants(giveaway_id)

    newly_unsubscribed = []
    resubscribed = []

    for p in participants:
        missing = await check_user_subscriptions(bot, p['user_id'], channels)
        was_unsubscribed = p.get('is_unsubscribed', False)

        if missing:
            if not was_unsubscribed:
                channel_names = ", ".join(ch.get('chat_title') or str(ch['chat_id']) for ch in missing)
                await db.mark_participant_unsubscribed(giveaway_id, p['user_id'], channel_names)
                newly_unsubscribed.append((p, channel_names))
        else:
            if was_unsubscribed:
                await db.mark_participant_resubscribed(giveaway_id, p['user_id'])
                resubscribed.append(p)

    # Notify users who just got disqualified
    for p, channel_names in newly_unsubscribed:
        try:
            await bot.send_message(
                p['user_id'],
                f"⚠️ Ты отписался от: <b>{channel_names}</b>\n\n"
                f"Конкурс «{g['title']}» — твоё участие приостановлено.\n"
                f"Подпишись обратно до окончания розыгрыша, чтобы сохранить шанс на приз."
            )
        except Exception as e:
            logger.info(f"Could not notify user {p['user_id']}: {e}")

    # Notify users who came back
    for p in resubscribed:
        try:
            await bot.send_message(
                p['user_id'],
                f"✅ Ты снова подписан на все каналы конкурса «{g['title']}».\n"
                f"Участие восстановлено!"
            )
        except Exception as e:
            logger.info(f"Could not notify user {p['user_id']}: {e}")

    # Notify admin with a summary (only if something changed)
    if newly_unsubscribed or resubscribed:
        lines = [f"🔍 <b>Проверка подписок — «{g['title']}»</b>\n"]
        if newly_unsubscribed:
            lines.append(f"❌ Отписались ({len(newly_unsubscribed)}):")
            for p, channel_names in newly_unsubscribed:
                uname = f"@{p['username']}" if p['username'] else p['full_name']
                lines.append(f"  • {uname} — от {channel_names}")
        if resubscribed:
            lines.append(f"\n✅ Вернулись ({len(resubscribed)}):")
            for p in resubscribed:
                uname = f"@{p['username']}" if p['username'] else p['full_name']
                lines.append(f"  • {uname}")
        try:
            await bot.send_message(admin_id, "\n".join(lines))
        except Exception as e:
            logger.warning(f"Could not notify admin: {e}")

    return {
        "checked": len(participants),
        "newly_unsubscribed": len(newly_unsubscribed),
        "resubscribed": len(resubscribed),
    }


# ── Final check at giveaway end: assign strikes + disqualify ───────────────────

async def finalize_subscription_check(bot: Bot, giveaway_id: int, admin_id: int) -> dict:
    """
    Run ONLY when the admin finishes a giveaway.
    Anyone who left a channel now gets a strike (global) AND is disqualified from THIS giveaway.
    Returns a summary for the admin.
    """
    g = await db.get_giveaway_by_id(giveaway_id)
    if not g:
        return {"struck": []}

    channels = await db.get_channels(giveaway_id)
    participants = await db.get_participants(giveaway_id)

    struck = []  # (participant, channel_names, new_strike_record)

    for p in participants:
        # Skip already-disqualified people (don't double-strike)
        if p.get('is_disqualified'):
            continue
        missing = await check_user_subscriptions(bot, p['user_id'], channels)
        if missing:
            channel_names = ", ".join(ch.get('chat_title') or str(ch['chat_id']) for ch in missing)
            await db.disqualify_participant(giveaway_id, p['user_id'])
            rec = await db.add_strike(p['user_id'], p['username'], p['full_name'])
            struck.append((p, channel_names, rec))

    # Notify struck users
    for p, channel_names, rec in struck:
        try:
            if rec['is_banned']:
                await bot.send_message(
                    p['user_id'],
                    "⛔️ <b>Вы были забанены</b>\n\n"
                    "Из-за слишком частого нарушения правил (3 страйка) "
                    "вы лишены права участвовать в любых конкурсах этого бота."
                )
            else:
                await bot.send_message(
                    p['user_id'],
                    f"⚠️ <b>Вы получили страйк</b>\n\n"
                    f"Вы покинули: {channel_names} до окончания конкурса «{g['title']}».\n"
                    f"Вы дисквалифицированы из этого конкурса.\n\n"
                    f"Текущее количество страйков: <b>{rec['strikes']}/3</b>\n"
                    f"Чем больше страйков — тем ниже шанс победы в будущих конкурсах. "
                    f"При 3 страйках — полный бан."
                )
        except Exception as e:
            logger.info(f"Could not notify struck user {p['user_id']}: {e}")

    # Admin summary
    if struck:
        lines = [f"⚖️ <b>Страйки по итогам «{g['title']}»</b>\n"]
        for p, channel_names, rec in struck:
            uname = f"@{p['username']}" if p['username'] else p['full_name']
            ban_note = " 🚫 БАН" if rec['is_banned'] else ""
            lines.append(f"  • {uname} — страйк {rec['strikes']}/3{ban_note} (вышел из {channel_names})")
        try:
            await bot.send_message(admin_id, "\n".join(lines))
        except Exception as e:
            logger.warning(f"Could not send strike summary to admin: {e}")

    return {"struck": struck}


async def pick_weighted_winners(participants: list[dict], places_count: int) -> list[dict]:
    """
    Pick winners using strike-based weights.
    Each participant's chance is scaled by db.strike_weight(global_strikes).
    Banned / 3-strike users have weight 0 and can't win.
    """
    import random as _random

    pool = []
    weights = []
    for p in participants:
        strikes = await db.get_strike_count(p['user_id'])
        w = db.strike_weight(strikes)
        if w > 0:
            pool.append(p)
            weights.append(w)

    winners = []
    available = list(pool)
    available_weights = list(weights)

    for _ in range(min(places_count, len(available))):
        chosen = _random.choices(available, weights=available_weights, k=1)[0]
        idx = available.index(chosen)
        winners.append(chosen)
        available.pop(idx)
        available_weights.pop(idx)

    return winners
