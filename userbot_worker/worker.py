import asyncio
import hashlib
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Optional

from telethon import TelegramClient, events
from telethon.errors import AuthKeyDuplicatedError, FloodWaitError, RPCError
from telethon.tl.types import Channel, Chat, InputPeerChannel, InputPeerChat, User

from .config import Settings
from .db import (
    claim_pending_tasks,
    disable_stale_userbot_targets,
    ensure_schema,
    get_task_status,
    get_worker_autoreply_contact,
    get_worker_autoreply_remaining,
    get_worker_autoreply_settings,
    get_worker_state_last_manual_outgoing_at,
    get_userbot_target_state,
    load_userbot_targets_by_chat_ids,
    load_enabled_userbot_targets,
    mark_task_done,
    mark_task_error,
    mark_userbot_target_disabled,
    mark_userbot_target_success,
    remap_userbot_target_chat_id,
    set_userbot_target_last_self_message_id,
    touch_worker_last_manual_outgoing_at,
    touch_worker_last_outgoing_at,
    upsert_worker_autoreply_contact,
    update_task_progress,
    upsert_userbot_target,
)
from .sender import SourceMessageMissingError, extract_migrated_chat_id, send_post_to_chat
from .tasks import remap_chat_id_everywhere

logger = logging.getLogger(__name__)

ONLINE_GUARD_SECONDS = 90

_DISABLE_RPC_ERRORS = {
    "ChatWriteForbiddenError",
    "ChatAdminRequiredError",
    "UserBannedInChannelError",
    "ChannelPrivateError",
    "ChatForbiddenError",
    "ChannelInvalidError",
    "ChannelPrivate",
}


def _build_chat_id_and_peer(entity) -> tuple[int, str, int, Optional[int]]:
    if isinstance(entity, Channel):
        peer_id = int(getattr(entity, "id", 0) or 0)
        access_hash = getattr(entity, "access_hash", None)
        chat_id = int(f"-100{peer_id}") if peer_id > 0 else 0
        return chat_id, "channel", peer_id, (int(access_hash) if access_hash is not None else None)

    if isinstance(entity, Chat):
        peer_id = int(getattr(entity, "id", 0) or 0)
        chat_id = -peer_id if peer_id > 0 else 0
        return chat_id, "chat", peer_id, None

    return 0, "", 0, None


def _chat_type_from_dialog(dialog) -> Optional[str]:
    entity = getattr(dialog, "entity", None)
    if entity is None:
        return None
    if getattr(dialog, "is_group", False):
        if bool(getattr(entity, "megagroup", False)):
            return "supergroup"
        return "group"
    if getattr(dialog, "is_channel", False):
        return "channel"
    return None


def _is_write_permission_error(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    markers = (
        "chatwriteforbidden",
        "chatadminrequired",
        "userbannedinchannel",
        "channelprivate",
        "chatsendmediaforbidden",
        "chat_send_media_forbidden",
        "forbidden",
        "not enough rights",
    )
    return type(exc).__name__ in _DISABLE_RPC_ERRORS or any(marker in text for marker in markers)


def _is_disconnect_state_error(exc: BaseException) -> bool:
    if isinstance(exc, ConnectionError):
        return True
    text = f"{type(exc).__name__} {exc}".lower()
    markers = (
        "cannot send requests while disconnected",
        "disconnected",
        "connection",
    )
    return any(marker in text for marker in markers)


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _is_offline_ok(*, last_outgoing_at: Optional[datetime], offline_threshold_minutes: int, now_utc: datetime) -> bool:
    if last_outgoing_at is None:
        return True

    delta_seconds = (now_utc - last_outgoing_at).total_seconds()
    if offline_threshold_minutes > 0:
        return delta_seconds >= offline_threshold_minutes * 60
    return delta_seconds >= ONLINE_GUARD_SECONDS


async def _handle_authkey_duplicated(client: TelegramClient, settings: Settings) -> datetime:
    logger.error("AuthKeyDuplicatedError — another session is using this account")
    await client.disconnect()
    return _now_utc() + timedelta(seconds=settings.authkey_duplicated_cooldown_seconds)


def _build_post_fingerprint(storage_chat_id: int, storage_message_ids: list[int]) -> str:
    payload = f"{int(storage_chat_id)}:{','.join(str(int(i)) for i in storage_message_ids)}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _skip_reason_by_history(*, settings: Settings, state, fingerprint: str, now_utc: datetime) -> Optional[str]:
    if not state:
        return None

    if settings.cooldown_minutes > 0 and state.last_success_post_at:
        if (now_utc - state.last_success_post_at) < timedelta(minutes=settings.cooldown_minutes):
            return "cooldown"

    if settings.anti_dup_minutes > 0 and state.last_post_fingerprint == fingerprint and state.last_post_at:
        if (now_utc - state.last_post_at) < timedelta(minutes=settings.anti_dup_minutes):
            return "anti_dup"

    return None


def build_input_peer(target) -> InputPeerChannel | InputPeerChat:
    if target.peer_type == "channel":
        if target.access_hash is None:
            raise ValueError(f"Missing access_hash for channel peer chat_id={target.chat_id}")
        return InputPeerChannel(channel_id=int(target.peer_id), access_hash=int(target.access_hash))
    if target.peer_type == "chat":
        return InputPeerChat(chat_id=int(target.peer_id))
    raise ValueError(f"Unsupported peer_type={target.peer_type} chat_id={target.chat_id}")


async def _apply_activity_gate(
    pool,
    client: TelegramClient,
    *,
    chat_id: int,
    known_last_self_message_id: Optional[int],
    my_id: int,
    threshold: int,
) -> tuple[bool, Optional[int], int]:
    last_self_message_id = known_last_self_message_id
    if last_self_message_id is None:
        async for message in client.iter_messages(chat_id, from_user="me", limit=1):
            last_self_message_id = int(message.id)
            await set_userbot_target_last_self_message_id(pool, chat_id=chat_id, message_id=last_self_message_id)
            break

    if threshold <= 0:
        return True, last_self_message_id, 0

    if last_self_message_id is None:
        return True, None, threshold

    activity_count = 0
    async for message in client.iter_messages(chat_id, min_id=int(last_self_message_id), limit=100):
        sender_id = int(getattr(message, "sender_id", 0) or 0)
        if sender_id != int(my_id):
            activity_count += 1
            if activity_count >= threshold:
                return True, last_self_message_id, activity_count
    return False, last_self_message_id, activity_count


async def sync_userbot_targets(pool, client: TelegramClient, settings: Settings) -> dict:
    total_seen = 0
    new_count = 0
    include_channels = settings.auto_targets_mode == "groups_and_channels"

    async for dialog in client.iter_dialogs():
        entity = getattr(dialog, "entity", None)
        if entity is None:
            continue
        if isinstance(entity, User) or getattr(dialog, "is_user", False):
            continue

        if not getattr(dialog, "is_group", False):
            if not (include_channels and getattr(dialog, "is_channel", False)):
                continue

        chat_type = _chat_type_from_dialog(dialog)
        if chat_type not in {"group", "supergroup", "channel"}:
            continue

        chat_id, peer_type, peer_id, access_hash = _build_chat_id_and_peer(entity)
        if not chat_id:
            continue
        if int(chat_id) == int(settings.storage_chat_id):
            continue
        if int(chat_id) in settings.blacklist_chat_ids:
            continue

        inserted = await upsert_userbot_target(
            pool,
            chat_id=int(chat_id),
            chat_type=chat_type,
            peer_type=peer_type,
            peer_id=peer_id,
            access_hash=access_hash,
            title=getattr(entity, "title", None),
            username=getattr(entity, "username", None),
        )
        total_seen += 1
        if inserted:
            new_count += 1

    disabled_count = await disable_stale_userbot_targets(pool, grace_days=settings.targets_stale_grace_days)
    logger.info("Disabled stale targets: %s", disabled_count)
    enabled_count = len(await load_enabled_userbot_targets(pool))
    stats = {
        "total": total_seen,
        "enabled": enabled_count,
        "new_count": new_count,
        "disabled_count": disabled_count,
    }
    logger.info("Targets sync done: %s", stats)
    return stats


async def _apply_chat_migration(
    *,
    settings: Settings,
    pool,
    task,
    old_chat_id: int,
    new_chat_id: int,
    is_storage_chat: bool,
) -> None:
    await remap_chat_id_everywhere(
        pool,
        old_chat_id=old_chat_id,
        new_chat_id=new_chat_id,
        is_storage_chat=is_storage_chat,
    )
    await remap_userbot_target_chat_id(pool, old_chat_id=old_chat_id, new_chat_id=new_chat_id)

    if is_storage_chat:
        try:
            object.__setattr__(settings, "storage_chat_id", int(new_chat_id))
        except Exception:
            logger.debug("Failed to update settings.storage_chat_id", exc_info=True)
        task.storage_chat_id = int(new_chat_id)


async def run_worker(settings: Settings, pool, client: TelegramClient, stop_event: asyncio.Event) -> None:
    logger.info("Worker started. poll=%ss", settings.worker_poll_seconds)
    await ensure_schema(pool)
    me = await client.get_me()
    my_id = int(me.id)

    @client.on(events.NewMessage(incoming=True))
    async def _handle_private_autoreply(event):
        if not event.is_private:
            return

        sender_id = int(getattr(event, "sender_id", 0) or 0)
        if sender_id <= 0 or sender_id == my_id:
            return

        sender = await event.get_sender()
        if sender is not None and bool(getattr(sender, "bot", False)):
            return

        settings_row = await get_worker_autoreply_settings(pool)
        await upsert_worker_autoreply_contact(pool, user_id=sender_id, replied=False)

        if not settings_row.enabled:
            return

        remaining = await get_worker_autoreply_remaining(
            pool,
            user_id=sender_id,
            cooldown_seconds=settings_row.cooldown_seconds,
        )
        contact = await get_worker_autoreply_contact(pool, user_id=sender_id)
        cooldown_ok = remaining <= 0
        last_replied_at = contact.get("last_replied_at") if contact else None
        is_first_message = last_replied_at is None

        now_utc = _now_utc()
        first_or_cooldown_ok = is_first_message or cooldown_ok
        should_reply = False
        if settings_row.trigger_mode == "first_message_only":
            should_reply = first_or_cooldown_ok
        elif settings_row.trigger_mode == "offline_over_minutes":
            last_manual_outgoing_at = await get_worker_state_last_manual_outgoing_at(pool)
            should_reply = _is_offline_ok(
                last_outgoing_at=last_manual_outgoing_at,
                offline_threshold_minutes=settings_row.offline_threshold_minutes,
                now_utc=now_utc,
            )
        else:  # both
            last_manual_outgoing_at = await get_worker_state_last_manual_outgoing_at(pool)
            offline_ok = _is_offline_ok(
                last_outgoing_at=last_manual_outgoing_at,
                offline_threshold_minutes=settings_row.offline_threshold_minutes,
                now_utc=now_utc,
            )
            should_reply = offline_ok and first_or_cooldown_ok

        if not cooldown_ok:
            return

        if not should_reply:
            return

        await event.respond(settings_row.reply_text)
        await upsert_worker_autoreply_contact(pool, user_id=sender_id, replied=True)


    @client.on(events.NewMessage(outgoing=True))
    async def _handle_private_manual_outgoing(event):
        if not event.is_private:
            return

        if bool(getattr(event, "via_bot_id", None)):
            return

        await touch_worker_last_manual_outgoing_at(pool)

    authkey_duplicated_cooldown_until: Optional[datetime] = None
    try:
        await sync_userbot_targets(pool, client, settings)
    except AuthKeyDuplicatedError:
        authkey_duplicated_cooldown_until = await _handle_authkey_duplicated(client, settings)
    except Exception:
        logger.exception("Initial userbot targets sync failed")
    last_targets_sync_at = _now_utc()

    while not stop_event.is_set():
        now_utc = _now_utc()
        if authkey_duplicated_cooldown_until and now_utc < authkey_duplicated_cooldown_until:
            await asyncio.sleep(min(settings.worker_poll_seconds, (authkey_duplicated_cooldown_until - now_utc).total_seconds()))
            continue
        if (now_utc - last_targets_sync_at).total_seconds() >= settings.targets_sync_seconds:
            try:
                await sync_userbot_targets(pool, client, settings)
            except AuthKeyDuplicatedError:
                authkey_duplicated_cooldown_until = await _handle_authkey_duplicated(client, settings)
                continue
            except Exception:
                logger.exception("Periodic userbot targets sync failed")
            last_targets_sync_at = now_utc

        if (now_utc - last_targets_sync_at).total_seconds() >= settings.targets_sync_seconds:
            try:
                await sync_userbot_targets(pool, client, settings)
            except AuthKeyDuplicatedError:
                authkey_duplicated_cooldown_until = await _handle_authkey_duplicated(client, settings)
                continue
            except Exception:
                logger.exception("Lazy pre-task userbot targets sync failed")
            last_targets_sync_at = now_utc

        try:
            tasks = await claim_pending_tasks(pool, limit=25, now=now_utc)
        except Exception:
            logger.exception("DB: failed to claim pending tasks")
            await asyncio.sleep(settings.worker_poll_seconds)
            continue

        if not tasks:
            await asyncio.sleep(settings.worker_poll_seconds)
            continue

        for task in tasks:
            if task.status == "canceled":
                await mark_task_done(
                    pool,
                    task_id=task.id,
                    sent_count=task.sent_count,
                    error_count=task.error_count,
                    last_error=task.last_error,
                    skipped_breakdown=task.skipped_breakdown,
                    final_status="canceled_done",
                )
                logger.info("Task canceled before processing task_id=%s", task.id)
                continue

            if task.target_chat_ids:
                task_targets = await load_userbot_targets_by_chat_ids(pool, chat_ids=list(task.target_chat_ids))
                target_map = {item.chat_id: item for item in task_targets}
                target_ids = [int(chat_id) for chat_id in task.target_chat_ids if int(chat_id) in target_map]
                source = "TASK"
            elif settings.target_chat_ids:
                env_targets = await load_userbot_targets_by_chat_ids(pool, chat_ids=list(settings.target_chat_ids))
                target_map = {item.chat_id: item for item in env_targets}
                target_ids = [int(chat_id) for chat_id in settings.target_chat_ids if int(chat_id) in target_map]
                source = "ENV"
            else:
                db_targets = await load_enabled_userbot_targets(pool)
                target_map = {item.chat_id: item for item in db_targets}
                target_ids = [item.chat_id for item in db_targets]
                source = "DB"
            logger.info("Targets source=%s count=%s task_id=%s", source, len(target_ids), task.id)

            filtered_target_ids: list[int] = []
            seen_target_ids: set[int] = set()
            for target_id in target_ids:
                normalized_target_id = int(target_id)
                if normalized_target_id == int(task.storage_chat_id):
                    continue
                if normalized_target_id in settings.blacklist_chat_ids:
                    continue
                if normalized_target_id in seen_target_ids:
                    continue
                seen_target_ids.add(normalized_target_id)
                filtered_target_ids.append(normalized_target_id)
            target_ids = filtered_target_ids

            if not target_ids:
                await update_task_progress(
                    pool,
                    task_id=task.id,
                    sent_count=task.sent_count,
                    error_count=task.error_count,
                    last_error="No targets: join group chats or enable AUTO_TARGETS_MODE, or set TARGET_CHAT_IDS.",
                    skipped_breakdown={"peer_missing": len(target_ids)},
                )
                await mark_task_error(
                    pool,
                    task_id=task.id,
                    attempts=task.attempts,
                    max_attempts=settings.max_task_attempts,
                    error="No targets: join group chats or enable AUTO_TARGETS_MODE, or set TARGET_CHAT_IDS.",
                    sent_count=task.sent_count,
                    error_count=task.error_count,
                )
                continue

            if not client.is_connected():
                logger.warning("Telegram client disconnected before task processing task_id=%s; reconnecting", task.id)
                try:
                    await client.connect()
                except AuthKeyDuplicatedError:
                    authkey_duplicated_cooldown_until = await _handle_authkey_duplicated(client, settings)
                    break
                except Exception as e:
                    logger.warning(
                        "Telegram client reconnect failed task_id=%s err=%s text=%s",
                        task.id,
                        type(e).__name__,
                        str(e),
                    )

            if not client.is_connected():
                logger.warning("Telegram client is still disconnected task_id=%s; marking task error", task.id)
                await update_task_progress(
                    pool,
                    task_id=task.id,
                    sent_count=task.sent_count,
                    error_count=task.error_count,
                    last_error="disconnected",
                )
                await mark_task_error(
                    pool,
                    task_id=task.id,
                    attempts=task.attempts,
                    max_attempts=settings.max_task_attempts,
                    error="disconnected",
                    sent_count=task.sent_count,
                    error_count=task.error_count,
                )
                continue

            sent = 0
            errors = 0
            skipped = 0
            skip_reasons: Counter[str] = Counter()
            last_error: Optional[str] = None
            disconnected_mid_task = False
            fingerprint = _build_post_fingerprint(int(task.storage_chat_id), list(task.storage_message_ids))

            for chat_id in target_ids:
                task_status = await get_task_status(pool, task_id=task.id)
                if task_status == "canceled":
                    last_error = "canceled_by_admin"
                    logger.info("Task canceled during processing task_id=%s", task.id)
                    break

                current_chat_id = int(chat_id)
                target_peer = target_map.get(current_chat_id)
                if target_peer is None:
                    skipped += 1
                    skip_reasons["peer_missing"] += 1
                    logger.info("chat_result=skipped reason=peer_missing task_id=%s chat_id=%s", task.id, current_chat_id)
                    continue
                retried_after_migration = False
                state = await get_userbot_target_state(pool, chat_id=current_chat_id)

                history_skip_reason = _skip_reason_by_history(
                    settings=settings,
                    state=state,
                    fingerprint=fingerprint,
                    now_utc=_now_utc(),
                )
                if history_skip_reason:
                    skipped += 1
                    skip_reasons[history_skip_reason] += 1
                    logger.info("chat_result=skipped reason=%s task_id=%s chat_id=%s", history_skip_reason, task.id, current_chat_id)
                    continue

                activity_threshold = int(settings.activity_gate_min_messages)
                if activity_threshold > 0:
                    try:
                        passed_activity, _, activity_count = await _apply_activity_gate(
                            pool,
                            client,
                            chat_id=current_chat_id,
                            known_last_self_message_id=(state.last_self_message_id if state else None),
                            my_id=my_id,
                            threshold=activity_threshold,
                        )
                    except AuthKeyDuplicatedError:
                        authkey_duplicated_cooldown_until = await _handle_authkey_duplicated(client, settings)
                        last_error = "authkey_duplicated"
                        disconnected_mid_task = True
                        break
                    except Exception as exc:
                        last_error = str(exc)
                        errors += 1
                        logger.warning("chat_result=error task_id=%s chat_id=%s err=%s text=%s", task.id, current_chat_id, type(exc).__name__, str(exc))
                        await update_task_progress(pool, task_id=task.id, sent_count=sent, error_count=errors, last_error=last_error)
                        continue
                else:
                    passed_activity = True
                    activity_count = 0

                if not passed_activity:
                    skipped += 1
                    skip_reasons["activity_gate"] += 1
                    logger.info(
                        "chat_result=skipped reason=activity_gate count=%s need=%s task_id=%s chat_id=%s",
                        activity_count,
                        activity_threshold,
                        task.id,
                        current_chat_id,
                    )
                    continue

                while True:
                    try:
                        target_entity = build_input_peer(target_peer)
                        sent_message_id = await send_post_to_chat(
                            client=client,
                            source_chat_id=int(task.storage_chat_id),
                            source_message_ids=task.storage_message_ids,
                            target=target_entity,
                            min_delay=settings.min_seconds_between_chats,
                            max_delay=settings.max_seconds_between_chats,
                        )
                        await touch_worker_last_outgoing_at(pool)
                        await mark_userbot_target_success(
                            pool,
                            chat_id=current_chat_id,
                            sent_message_id=int(sent_message_id),
                            fingerprint=fingerprint,
                        )
                        sent += 1
                        logger.info("chat_result=sent task_id=%s chat_id=%s sent_message_id=%s", task.id, current_chat_id, sent_message_id)
                        await update_task_progress(pool, task_id=task.id, sent_count=sent, error_count=errors, last_error=None)
                        break
                    except AuthKeyDuplicatedError:
                        authkey_duplicated_cooldown_until = await _handle_authkey_duplicated(client, settings)
                        last_error = "authkey_duplicated"
                        disconnected_mid_task = True
                        break
                    except ValueError as e:
                        last_error = str(e)
                        errors += 1
                        logger.warning("chat_result=error task_id=%s chat_id=%s err=ValueError text=%s", task.id, current_chat_id, last_error)
                        reason = "entity_missing" if "input entity" in last_error.lower() else "invalid_target"
                        if settings.targets_disable_on_entity_error:
                            await mark_userbot_target_disabled(pool, chat_id=current_chat_id, error_text=f"{reason}: {last_error}")
                        await update_task_progress(pool, task_id=task.id, sent_count=sent, error_count=errors, last_error=last_error)
                        break
                    except SourceMessageMissingError:
                        last_error = "source_message_missing"
                        logger.warning("task_aborted reason=source_message_missing task_id=%s", task.id)
                        await mark_task_error(
                            pool,
                            task_id=task.id,
                            attempts=settings.max_task_attempts,
                            max_attempts=settings.max_task_attempts,
                            error=last_error,
                            sent_count=sent,
                            error_count=errors,
                        )
                        break
                    except FloodWaitError as e:
                        wait_s = int(getattr(e, "seconds", 0) or 0)
                        await asyncio.sleep(max(wait_s, 1))
                        continue
                    except ConnectionError as e:
                        if not _is_disconnect_state_error(e):
                            raise
                        last_error = "disconnected_mid_task"
                        errors += 1
                        disconnected_mid_task = True
                        logger.warning(
                            "task_aborted reason=disconnected_mid_task task_id=%s chat_id=%s err=%s text=%s",
                            task.id,
                            current_chat_id,
                            type(e).__name__,
                            str(e),
                        )
                        await update_task_progress(
                            pool,
                            task_id=task.id,
                            sent_count=sent,
                            error_count=errors,
                            last_error=last_error,
                        )
                        break
                    except RPCError as e:
                        migrated_chat_id = extract_migrated_chat_id(e)
                        if retried_after_migration or migrated_chat_id is None:
                            last_error = str(e)
                            errors += 1
                            logger.warning("chat_result=error task_id=%s chat_id=%s err=%s text=%s", task.id, current_chat_id, type(e).__name__, str(e))
                            if _is_write_permission_error(e) or (settings.targets_disable_on_entity_error and "input entity" in str(e).lower()):
                                await mark_userbot_target_disabled(pool, chat_id=current_chat_id, error_text=last_error)
                            await update_task_progress(pool, task_id=task.id, sent_count=sent, error_count=errors, last_error=last_error)
                            break

                        error_text = str(e)
                        is_storage_migration = str(task.storage_chat_id) in error_text and str(current_chat_id) not in error_text
                        old_chat_id = int(task.storage_chat_id if is_storage_migration else current_chat_id)

                        await _apply_chat_migration(
                            settings=settings,
                            pool=pool,
                            task=task,
                            old_chat_id=old_chat_id,
                            new_chat_id=int(migrated_chat_id),
                            is_storage_chat=is_storage_migration,
                        )

                        if not is_storage_migration:
                            current_chat_id = int(migrated_chat_id)
                        retried_after_migration = True
                        continue
                    except Exception as e:
                        last_error = repr(e)
                        errors += 1
                        logger.warning("chat_result=error task_id=%s chat_id=%s err=%s text=%s", task.id, current_chat_id, type(e).__name__, str(e))
                        if _is_write_permission_error(e) or (settings.targets_disable_on_entity_error and "input entity" in str(e).lower()):
                            await mark_userbot_target_disabled(pool, chat_id=current_chat_id, error_text=last_error)
                        await update_task_progress(pool, task_id=task.id, sent_count=sent, error_count=errors, last_error=last_error)
                        break

                if disconnected_mid_task:
                    break

            skipped_breakdown = {
                "skipped_activity_gate": int(skip_reasons.get("activity_gate", 0)),
                "skipped_cooldown": int(skip_reasons.get("cooldown", 0)),
                "skipped_anti_dup": int(skip_reasons.get("anti_dup", 0)),
                **{k: int(v) for k, v in skip_reasons.items() if k not in {"activity_gate", "cooldown", "anti_dup"}},
            }
            logger.info(
                "Task summary task_id=%s sent=%s errors=%s skipped=%s skipped_breakdown=%s",
                task.id,
                sent,
                errors,
                skipped,
                skipped_breakdown,
            )

            if last_error == "source_message_missing":
                await update_task_progress(
                    pool,
                    task_id=task.id,
                    sent_count=sent,
                    error_count=errors,
                    last_error=last_error,
                    skipped_breakdown=skipped_breakdown,
                )
                continue

            if last_error == "canceled_by_admin":
                await mark_task_done(
                    pool,
                    task_id=task.id,
                    sent_count=sent,
                    error_count=errors,
                    last_error=last_error,
                    skipped_breakdown=skipped_breakdown,
                    final_status="canceled_done",
                )
                continue

            if last_error == "authkey_duplicated":
                await mark_task_error(
                    pool,
                    task_id=task.id,
                    attempts=task.attempts,
                    max_attempts=settings.max_task_attempts,
                    error=last_error,
                    sent_count=sent,
                    error_count=errors,
                )
                continue

            if last_error == "disconnected_mid_task":
                await mark_task_error(
                    pool,
                    task_id=task.id,
                    attempts=task.attempts,
                    max_attempts=settings.max_task_attempts,
                    error=last_error,
                    sent_count=sent,
                    error_count=errors,
                )
                continue

            if errors == 0:
                await mark_task_done(
                    pool,
                    task_id=task.id,
                    sent_count=sent,
                    error_count=errors,
                    last_error=None,
                    skipped_breakdown=skipped_breakdown,
                )
                continue

            err_text = last_error or f"Broadcast completed with {errors} errors."
            await update_task_progress(
                pool,
                task_id=task.id,
                sent_count=sent,
                error_count=errors,
                last_error=err_text,
                skipped_breakdown=skipped_breakdown,
            )
            await mark_task_error(
                pool,
                task_id=task.id,
                attempts=task.attempts,
                max_attempts=settings.max_task_attempts,
                error=err_text,
                sent_count=sent,
                error_count=errors,
            )

        await asyncio.sleep(settings.worker_poll_seconds)
