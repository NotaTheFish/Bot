from __future__ import annotations

import asyncio
import logging
import os
import signal
from contextlib import suppress

from telethon import TelegramClient
from telethon.sessions import StringSession

from .config import load_settings, settings_for_legacy_env_worker, settings_for_tenant_row
from .db import (
    acquire_singleton_lock,
    create_pool,
    list_mirror_tenant_profiles,
    release_singleton_lock,
    update_tenant_mirror_status,
)
from .worker import run_worker

logger = logging.getLogger(__name__)


def _should_run_legacy_env_worker(base) -> bool:
    """Use TELETHON_SESSION / TELEGRAM_API_* from .env for the primary workspace (not tenant:*)."""
    ws = (base.workspace_key or "").strip()
    if ws.startswith("tenant:"):
        return False
    return (
        base.telegram_api_id > 0
        and bool(base.telegram_api_hash)
        and bool(base.telethon_session.strip())
        and len(base.telethon_session.strip()) >= 20
    )


async def _connect_authorized_user_client(
    client: TelegramClient,
    *,
    workspace: str,
    credential_source: str,
) -> None:
    """
    Non-interactive login only. Never call client.start() without args on a server — it may read stdin
    (phone/code) and crash with EOFError.
    """
    await client.connect()
    if not await client.is_user_authorized():
        src_hint = (
            "Update TELETHON_SESSION (and matching TELEGRAM_API_ID / TELEGRAM_API_HASH) in the worker environment."
            if credential_source == "env"
            else "Update tenant_profiles.telethon_session (and API id/hash) for this workspace in the database."
        )
        raise RuntimeError(
            f"Telethon session is empty, invalid, or not authorized "
            f"(workspace={workspace!r}, credential_source={credential_source!r}). "
            f"{src_hint} Interactive login is disabled on the server."
        )


async def _run_one_tenant(
    *,
    base,
    pool,
    tenant: dict,
    stop_event: asyncio.Event,
) -> None:
    cred_src = str(tenant.get("credential_source") or "tenant_profiles")
    ws = str(tenant.get("workspace_key") or base.workspace_key)

    if cred_src == "env":
        settings = settings_for_legacy_env_worker(base)
        logger.info(
            "Worker credentials source=env workspace_key=%s (TELEGRAM_* from environment)",
            ws,
        )
    else:
        settings = settings_for_tenant_row(base, tenant)
        logger.info(
            "Worker credentials source=tenant_profiles workspace_key=%s (Telegram fields from database row)",
            ws,
        )

    client = TelegramClient(
        StringSession(settings.telethon_session),
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )
    try:
        with suppress(Exception):
            await update_tenant_mirror_status(pool, workspace_key=ws, status="starting")
        await _connect_authorized_user_client(client, workspace=ws, credential_source=cred_src)
        with suppress(Exception):
            await update_tenant_mirror_status(pool, workspace_key=ws, status="running")
        await run_worker(settings, pool, client, stop_event)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("Tenant worker stopped with error workspace=%s credential_source=%s", ws, cred_src)
        with suppress(Exception):
            await update_tenant_mirror_status(pool, workspace_key=ws, status="error", last_error=str(exc))
    finally:
        with suppress(Exception):
            await client.disconnect()


async def amain() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv()
    except Exception:
        pass

    base = load_settings()
    pool = await create_pool(base.database_url)
    lock_conn = None

    mgr_key = int(os.getenv("WORKER_MANAGER_LOCK_KEY") or os.getenv("SINGLETON_LOCK_KEY", "910001"))

    if base.singleton_lock_enabled:
        try:
            lock_conn = await acquire_singleton_lock(pool, mgr_key)
        except Exception as exc:
            logging.error("Failed to acquire singleton lock due to database error: %s", exc)
            await pool.close()
            return

        if lock_conn is None:
            logging.info(
                "Singleton lock key=%s is already held; another instance is running. Exiting.",
                mgr_key,
            )
            await pool.close()
            return

        logging.info("Acquired worker manager singleton lock key=%s", mgr_key)

    worker_specs: list[dict] = []

    mirror_rows = await list_mirror_tenant_profiles(pool)
    for row in mirror_rows:
        spec = dict(row)
        spec["credential_source"] = "tenant_profiles"
        worker_specs.append(spec)

    if _should_run_legacy_env_worker(base):
        worker_specs.insert(
            0,
            {
                "workspace_key": base.workspace_key,
                "credential_source": "env",
            },
        )

    if not worker_specs:
        logging.error(
            "No workers to run: no tenant:* rows in tenant_profiles with valid credentials, "
            "and legacy env worker disabled (set WORKSPACE_KEY to a non-tenant:* key and provide "
            "TELETHON_SESSION + TELEGRAM_API_ID + TELEGRAM_API_HASH in the environment)."
        )
        await release_singleton_lock(pool, lock_conn, mgr_key)
        await pool.close()
        return

    logger.info(
        "Scheduling %s worker(s): %s",
        len(worker_specs),
        ", ".join(
            f"{s.get('workspace_key')}[{s.get('credential_source')}]" for s in worker_specs
        ),
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass

    tasks = [
        asyncio.create_task(_run_one_tenant(base=base, pool=pool, tenant=t, stop_event=stop_event))
        for t in worker_specs
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        await release_singleton_lock(pool, lock_conn, mgr_key)
        await pool.close()


if __name__ == "__main__":
    asyncio.run(amain())
