from __future__ import annotations

import asyncio
import logging
import os
import signal
from contextlib import suppress

from telethon import TelegramClient
from telethon.sessions import StringSession

from .config import load_settings, settings_for_tenant_row
from .db import (
    acquire_singleton_lock,
    create_pool,
    list_enabled_tenant_profiles,
    release_singleton_lock,
    update_tenant_mirror_status,
)
from .worker import run_worker


async def _run_one_tenant(
    *,
    base,
    pool,
    tenant: dict,
    stop_event: asyncio.Event,
) -> None:
    ws = str(tenant["workspace_key"])
    settings = settings_for_tenant_row(base, tenant)
    client = TelegramClient(
        StringSession(settings.telethon_session),
        settings.telegram_api_id,
        settings.telegram_api_hash,
    )
    try:
        with suppress(Exception):
            await update_tenant_mirror_status(pool, workspace_key=ws, status="starting")
        await client.start()
        with suppress(Exception):
            await update_tenant_mirror_status(pool, workspace_key=ws, status="running")
        await run_worker(settings, pool, client, stop_event)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logging.exception("Tenant worker stopped with error workspace=%s", ws)
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

    tenants = await list_enabled_tenant_profiles(pool)
    if not tenants:
        if (
            base.telegram_api_id > 0
            and base.telegram_api_hash
            and base.telethon_session
            and len(base.telethon_session.strip()) >= 20
        ):
            tenants = [
                {
                    "workspace_key": base.workspace_key,
                    "telegram_api_id": base.telegram_api_id,
                    "telegram_api_hash": base.telegram_api_hash,
                    "telethon_session": base.telethon_session,
                    "storage_chat_id": base.storage_chat_id,
                }
            ]
        else:
            logging.error(
                "No rows in tenant_profiles and no TELETHON_SESSION in env; nothing to run."
            )
            await release_singleton_lock(pool, lock_conn, mgr_key)
            await pool.close()
            return

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, RuntimeError):
            pass

    tasks = [
        asyncio.create_task(_run_one_tenant(base=base, pool=pool, tenant=t, stop_event=stop_event))
        for t in tenants
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        await release_singleton_lock(pool, lock_conn, mgr_key)
        await pool.close()


if __name__ == "__main__":
    asyncio.run(amain())
