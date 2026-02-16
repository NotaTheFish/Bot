import asyncio
import hashlib
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import Channel, Chat, InputPeerChannel, InputPeerChat, User

from .config import Settings
from .db import (
    claim_pending_tasks,
    disable_stale_userbot_targets,
    ensure_schema,
    get_userbot_target_state,
    load_userbot_targets_by_chat_ids,
    load_enabled_userbot_targets,
    mark_task_done,
    mark_task_error,
    mark_userbot_target_disabled,
    mark_userbot_target_success,
    remap_userbot_target_chat_id,
    set_userbot_target_last_self_message_id,
    update_task_progress,
    upsert_userbot_target,
)
from .sender import extract_migrated_chat_id, send_post_to_chat
from .tasks import remap_chat_id_everywhere

logger = logging.getLogger(__name__)

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


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _build_post_fingerprint(storage_chat_id: int, storage_message_ids: list[int]) -> str:
    payload = f"{int(storage_chat_id)}:{','.join(str(int(i)) for i in storage_message_ids)}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def build_input_peer(target) -> InputPeerChannel | InputPeerChat:
    if target.peer_type == "channel":
        if target.access_hash is None:
            raise ValueError(f"Missing access_hash for channel peer chat_id={target.chat_id}")
        return InputPeerChannel(channel_id=int(target.peer_id), access_hash=int(target.access_hash))
    if target.peer_type == "chat":
        return InputPeerChat(chat_id=int(target.peer_id))
    raise ValueError(f"Unsupported peer_type={target.peer_type} chat_id={target.chat_id}")


async def _apply_activity_gate(pool, client: TelegramClient, *, chat_id: int, known_last_self_message_id: Optional[int]) -> tuple[bool, Optional[int], int]:
    last_self_message_id = known_last_self_message_id
    if last_self_message_id is None:
        async for message in client.iter_messages(chat_id, from_user="me", limit=1):
            last_self_message_id = int(message.id)
            await set_userbot_target_last_self_message_id(pool, chat_id=chat_id, message_id=last_self_message_id)
            break

    if last_self_message_id is None:
        return True, None, 999

    activity_count = 0
    async for message in client.iter_messages(chat_id, min_id=int(last_self_message_id), limit=100):
        if not getattr(message, "out", False):
            activity_count += 1
            if activity_count >= 5:
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

    disabled_count = await disable_stale_userbot_targets(pool, grace_days=30)
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

    try:
        await sync_userbot_targets(pool, client, settings)
    except Exception:
        logger.exception("Initial userbot targets sync failed")
    last_targets_sync_at = _now_utc()

    while not stop_event.is_set():
        now_utc = _now_utc()
        if (now_utc - last_targets_sync_at).total_seconds() >= settings.targets_sync_seconds:
            try:
                await sync_userbot_targets(pool, client, settings)
            except Exception:
                logger.exception("Periodic userbot targets sync failed")
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

            sent = 0
            errors = 0
            skipped = 0
            skip_reasons: Counter[str] = Counter()
            last_error: Optional[str] = None
            fingerprint = _build_post_fingerprint(int(task.storage_chat_id), list(task.storage_message_ids))

            for chat_id in target_ids:
                current_chat_id = int(chat_id)
                target_peer = target_map.get(current_chat_id)
                if target_peer is None:
                    skipped += 1
                    skip_reasons["peer_missing"] += 1
                    logger.info("chat_result=skipped reason=peer_missing task_id=%s chat_id=%s", task.id, current_chat_id)
                    continue
                retried_after_migration = False
                state = await get_userbot_target_state(pool, chat_id=current_chat_id)

                if state and state.last_success_post_at and (_now_utc() - state.last_success_post_at) < timedelta(minutes=10):
                    skipped += 1
                    skip_reasons["cooldown"] += 1
                    logger.info("chat_result=skipped reason=cooldown task_id=%s chat_id=%s", task.id, current_chat_id)
                    continue

                if state and state.last_post_fingerprint == fingerprint and state.last_post_at and (_now_utc() - state.last_post_at) < timedelta(minutes=10):
                    skipped += 1
                    skip_reasons["anti_dup"] += 1
                    logger.info("chat_result=skipped reason=anti_dup task_id=%s chat_id=%s", task.id, current_chat_id)
                    continue

                try:
                    passed_activity, _, activity_count = await _apply_activity_gate(
                        pool,
                        client,
                        chat_id=current_chat_id,
                        known_last_self_message_id=(state.last_self_message_id if state else None),
                    )
                except Exception as exc:
                    last_error = str(exc)
                    errors += 1
                    logger.warning("chat_result=error task_id=%s chat_id=%s err=%s text=%s", task.id, current_chat_id, type(exc).__name__, str(exc))
                    await update_task_progress(pool, task_id=task.id, sent_count=sent, error_count=errors, last_error=last_error)
                    continue

                if not passed_activity:
                    skipped += 1
                    skip_reasons["activity_gate"] += 1
                    logger.info(
                        "chat_result=skipped reason=activity_gate count=%s task_id=%s chat_id=%s",
                        activity_count,
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
                    except ValueError as e:
                        last_error = str(e)
                        errors += 1
                        logger.warning("chat_result=error task_id=%s chat_id=%s err=ValueError text=%s", task.id, current_chat_id, last_error)
                        reason = "entity_missing" if "input entity" in last_error.lower() else "invalid_target"
                        await mark_userbot_target_disabled(pool, chat_id=current_chat_id, error_text=f"{reason}: {last_error}")
                        await update_task_progress(pool, task_id=task.id, sent_count=sent, error_count=errors, last_error=last_error)
                        break
                    except FloodWaitError as e:
                        wait_s = int(getattr(e, "seconds", 0) or 0)
                        await asyncio.sleep(max(wait_s, 1))
                        continue
                    except RPCError as e:
                        migrated_chat_id = extract_migrated_chat_id(e)
                        if retried_after_migration or migrated_chat_id is None:
                            last_error = str(e)
                            errors += 1
                            logger.warning("chat_result=error task_id=%s chat_id=%s err=%s text=%s", task.id, current_chat_id, type(e).__name__, str(e))
                            if _is_write_permission_error(e):
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
                        if _is_write_permission_error(e):
                            await mark_userbot_target_disabled(pool, chat_id=current_chat_id, error_text=last_error)
                        await update_task_progress(pool, task_id=task.id, sent_count=sent, error_count=errors, last_error=last_error)
                        break

            logger.info(
                "Task summary task_id=%s sent=%s errors=%s skipped=%s skipped_breakdown=%s",
                task.id,
                sent,
                errors,
                skipped,
                dict(skip_reasons),
            )

            if errors == 0:
                await mark_task_done(
                    pool,
                    task_id=task.id,
                    sent_count=sent,
                    error_count=errors,
                    last_error=None,
                )
                continue

            err_text = last_error or f"Broadcast completed with {errors} errors."
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
