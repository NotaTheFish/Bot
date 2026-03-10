import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl

import aiohttp
import asyncpg
from aiohttp import web

logger = logging.getLogger(__name__)


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or "").strip()


def _env_int(name: str, default: int = 0) -> int:
    value = _env(name, str(default))
    try:
        return int(value)
    except ValueError:
        return default


BOT_TOKEN = _env("BOT_TOKEN")
DATABASE_URL = _env("DATABASE_URL")
API_HOST = _env("CONTEST_API_HOST", "0.0.0.0")
API_PORT = _env_int("CONTEST_API_PORT", 8080)
CONTEST_CHANNEL_ID = _env("CONTEST_CHANNEL_ID") or _env("CONTEST_VOTING_CHAT_ID")
MAX_VOTES_PER_USER = max(1, _env_int("CONTEST_MAX_VOTES_PER_USER", 3))


def _hash_optional(value: str | None) -> str | None:
    if not value:
        return None
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _telegram_secret(bot_token: str) -> bytes:
    return hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()


def parse_and_verify_init_data(init_data: str, bot_token: str, max_age_seconds: int = 3600) -> dict[str, Any]:
    if not init_data:
        raise ValueError("Не переданы данные Telegram WebApp.")

    pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    recv_hash = pairs.pop("hash", "")
    if not recv_hash:
        raise ValueError("Не удалось проверить Telegram-пользователя.")

    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    secret = _telegram_secret(bot_token)
    calc_hash = hmac.new(secret, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc_hash, recv_hash):
        raise ValueError("Telegram авторизация недействительна.")

    auth_date_raw = pairs.get("auth_date", "0")
    try:
        auth_date = int(auth_date_raw)
    except ValueError as exc:
        raise ValueError("Некорректное время авторизации Telegram.") from exc

    if int(time.time()) - auth_date > max_age_seconds:
        raise ValueError("Сессия Telegram устарела, откройте мини-приложение заново.")

    user_raw = pairs.get("user")
    if not user_raw:
        raise ValueError("Не передан Telegram user.")

    try:
        user = json.loads(user_raw)
    except json.JSONDecodeError as exc:
        raise ValueError("Некорректные данные Telegram user.") from exc

    user_id = int(user.get("id", 0) or 0)
    if user_id <= 0:
        raise ValueError("Некорректный Telegram user id.")

    return {"user_id": user_id, "user": user, "payload": pairs}


@dataclass(slots=True)
class VoteError(Exception):
    message: str
    code: str
    status: int = 400


class ContestAPI:
    def __init__(self) -> None:
        if not BOT_TOKEN:
            raise RuntimeError("BOT_TOKEN is required")
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL is required")
        if not CONTEST_CHANNEL_ID:
            raise RuntimeError("CONTEST_CHANNEL_ID or CONTEST_VOTING_CHAT_ID is required")

        self._pool: asyncpg.Pool | None = None
        self._http: aiohttp.ClientSession | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("DB pool is not initialized")
        return self._pool

    @property
    def http(self) -> aiohttp.ClientSession:
        if self._http is None:
            raise RuntimeError("HTTP session is not initialized")
        return self._http

    async def startup(self, _: web.Application) -> None:
        self._pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
        self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        await self.ensure_schema()

    async def cleanup(self, _: web.Application) -> None:
        if self._http is not None:
            await self._http.close()
        if self._pool is not None:
            await self._pool.close()

    async def ensure_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                ALTER TABLE contest_votes
                ADD COLUMN IF NOT EXISTS ip_hash TEXT
                """
            )
            await conn.execute(
                """
                ALTER TABLE contest_votes
                ADD COLUMN IF NOT EXISTS user_agent_hash TEXT
                """
            )
            await conn.execute(
                """
                ALTER TABLE contest_votes
                ADD COLUMN IF NOT EXISTS is_suspicious BOOLEAN NOT NULL DEFAULT FALSE
                """
            )

    def routes(self) -> list[web.RouteDef]:
        return [
            web.get("/health", self.health),
            web.get("/api/contest/entries/approved", self.approved_entries),
            web.get("/api/contest/votes/state", self.votes_state),
            web.post("/api/contest/votes/cast", self.cast_vote),
        ]

    async def health(self, _: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    async def approved_entries(self, _: web.Request) -> web.Response:
        rows = await self.pool.fetch(
            """
            SELECT
                e.id,
                e.user_id,
                e.storage_chat_id,
                e.storage_message_id,
                e.storage_message_ids,
                e.submitted_at,
                e.created_at,
                COALESCE(v.votes_count, 0) AS votes_count
            FROM contest_entries e
            LEFT JOIN (
                SELECT entry_id, COUNT(*)::INT AS votes_count
                FROM contest_votes
                GROUP BY entry_id
            ) v ON v.entry_id = e.id
            WHERE e.status = 'approved'
            ORDER BY e.submitted_at DESC NULLS LAST, e.id DESC
            """
        )
        items = [dict(row) for row in rows]
        return web.json_response({"ok": True, "items": items})

    async def votes_state(self, request: web.Request) -> web.Response:
        try:
            auth = self._extract_auth(request)
            used = await self.pool.fetchval(
                "SELECT COUNT(*)::INT FROM contest_votes WHERE voter_user_id = $1",
                auth["user_id"],
            )
            used = int(used or 0)
            return web.json_response(
                {
                    "ok": True,
                    "user_id": auth["user_id"],
                    "max_votes": MAX_VOTES_PER_USER,
                    "used_votes": used,
                    "remaining_votes": max(0, MAX_VOTES_PER_USER - used),
                }
            )
        except VoteError as exc:
            return web.json_response({"ok": False, "error_code": exc.code, "message": exc.message}, status=exc.status)

    async def cast_vote(self, request: web.Request) -> web.Response:
        try:
            auth = self._extract_auth(request)
            payload = await request.json()
            entry_id = int(payload.get("entry_id") or 0)
            if entry_id <= 0:
                raise VoteError("Выберите работу для голосования.", "bad_entry_id", 400)

            await self._ensure_channel_subscription(auth["user_id"])

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    entry = await conn.fetchrow(
                        """
                        SELECT id, user_id, status
                        FROM contest_entries
                        WHERE id = $1
                        FOR UPDATE
                        """,
                        entry_id,
                    )
                    if entry is None or entry["status"] != "approved":
                        raise VoteError("Работа недоступна для голосования.", "entry_unavailable", 404)
                    if int(entry["user_id"]) == int(auth["user_id"]):
                        raise VoteError("Нельзя голосовать за свою работу.", "self_vote_forbidden", 400)

                    used_votes = await conn.fetchval(
                        "SELECT COUNT(*)::INT FROM contest_votes WHERE voter_user_id = $1",
                        auth["user_id"],
                    )
                    if int(used_votes or 0) >= MAX_VOTES_PER_USER:
                        raise VoteError("Голосов не осталось.", "vote_limit_reached", 400)

                    duplicate = await conn.fetchval(
                        """
                        SELECT 1
                        FROM contest_votes
                        WHERE voter_user_id = $1 AND entry_id = $2
                        """,
                        auth["user_id"],
                        entry_id,
                    )
                    if duplicate:
                        raise VoteError("Вы уже голосовали за эту работу.", "duplicate_vote", 409)

                    ip_hash = _hash_optional(request.remote)
                    ua_hash = _hash_optional(request.headers.get("User-Agent"))
                    suspicious = await self._is_suspicious(conn, auth["user_id"], ip_hash, ua_hash)

                    await conn.execute(
                        """
                        INSERT INTO contest_votes (
                            voter_user_id,
                            entry_id,
                            ip_hash,
                            user_agent_hash,
                            is_suspicious,
                            created_at,
                            updated_at
                        )
                        VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
                        """,
                        auth["user_id"],
                        entry_id,
                        ip_hash,
                        ua_hash,
                        suspicious,
                    )

                    new_used = int(used_votes or 0) + 1

            return web.json_response(
                {
                    "ok": True,
                    "entry_id": entry_id,
                    "used_votes": new_used,
                    "remaining_votes": max(0, MAX_VOTES_PER_USER - new_used),
                    "is_suspicious": suspicious,
                }
            )
        except (ValueError, json.JSONDecodeError):
            return web.json_response({"ok": False, "error_code": "bad_request", "message": "Некорректный формат запроса."}, status=400)
        except VoteError as exc:
            return web.json_response({"ok": False, "error_code": exc.code, "message": exc.message}, status=exc.status)
        except asyncpg.UniqueViolationError:
            return web.json_response(
                {
                    "ok": False,
                    "error_code": "duplicate_vote",
                    "message": "Вы уже голосовали за эту работу.",
                },
                status=409,
            )

    async def _is_suspicious(
        self,
        conn: asyncpg.Connection,
        voter_user_id: int,
        ip_hash: str | None,
        user_agent_hash: str | None,
    ) -> bool:
        if not ip_hash or not user_agent_hash:
            return True

        shared_voters = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT voter_user_id)::INT
            FROM contest_votes
            WHERE ip_hash = $1
              AND created_at >= NOW() - INTERVAL '24 hours'
            """,
            ip_hash,
        )
        if int(shared_voters or 0) >= 8:
            return True

        repeated_same_ua = await conn.fetchval(
            """
            SELECT COUNT(*)::INT
            FROM contest_votes
            WHERE voter_user_id = $1
              AND user_agent_hash = $2
            """,
            voter_user_id,
            user_agent_hash,
        )
        return int(repeated_same_ua or 0) >= 2

    async def _ensure_channel_subscription(self, user_id: int) -> None:
        method_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getChatMember"
        async with self.http.get(method_url, params={"chat_id": CONTEST_CHANNEL_ID, "user_id": user_id}) as resp:
            data = await resp.json(content_type=None)

        if not data.get("ok"):
            logger.warning("getChatMember failed: %s", data)
            raise VoteError("Не удалось проверить подписку на канал, попробуйте позже.", "subscription_check_failed", 503)

        status = str(((data.get("result") or {}).get("status") or "")).lower()
        if status in {"left", "kicked"}:
            raise VoteError("Чтобы голосовать, нужно подписаться на канал.", "subscription_required", 403)

    def _extract_auth(self, request: web.Request) -> dict[str, Any]:
        init_data = request.headers.get("X-Telegram-Init-Data", "") or request.query.get("initData", "")
        if not init_data and request.can_read_body and request.content_type == "application/json":
            # fallback: some clients send init_data in body even for GET by mistake
            pass
        try:
            return parse_and_verify_init_data(init_data, BOT_TOKEN)
        except ValueError as exc:
            raise VoteError(str(exc), "auth_failed", 401) from exc


async def create_app() -> web.Application:
    api = ContestAPI()
    app = web.Application()
    app.add_routes(api.routes())
    app.on_startup.append(api.startup)
    app.on_cleanup.append(api.cleanup)
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    app = asyncio.run(create_app())
    web.run_app(app, host=API_HOST, port=API_PORT)


if __name__ == "__main__":
    main()