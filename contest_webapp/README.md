\# contest\_webapp (Railway)



Отдельный сервис Telegram Mini App для конкурса.



\## Railway

\- \*\*Root Directory:\*\* `contest\_webapp`

\- \*\*Start Command:\*\* `uvicorn main\_webapp:app --host 0.0.0.0 --port $PORT`



\## Обязательные ENV

\- `DATABASE\_URL`

\- `BOT\_TOKEN`

\- `CONTEST\_CHANNEL\_ID` (или `CONTEST\_VOTING\_CHAT\_ID`)



\## Опциональные ENV

\- `CONTEST\_MAX\_VOTES\_PER\_USER` (по умолчанию `3`)


## Дополнительно для админ-режима

- `ADMIN_IDS` (CSV Telegram user id) или `ADMIN_ID` (legacy)

## Новые API для mini app

- `GET /api/contest/state`
- `GET /api/contest/entries/approved`
- `GET /api/contest/my-draft`
- `POST /api/contest/draft/select`
- `POST /api/contest/draft/unselect`
- `POST /api/contest/votes/confirm`
- `GET /api/contest/admin/overview`
- `GET /api/contest/admin/entry/{id}/votes`
- `POST /api/contest/admin/submission/open`
- `POST /api/contest/admin/submission/close`
- `POST /api/contest/admin/voting/open`
- `POST /api/contest/admin/voting/close`

## Миграция

Примените SQL из:
- `contest_webapp/migrations/20260312_vote_draft_confirmation.sql`
- `contest_webapp/migrations/20260313_contest_display_order_anchor.sql`
перед выкладкой.

## Выдача изображений

Сервис `contest_webapp` поднимает локальный proxy-эндпоинт:

- `GET /api/contest/entries/{entry_id}/image`

Эндпоинт загружает изображение по метаданным `storage_chat_id` + `storage_message_id` (или первому id из `storage_message_ids`) через Telegram Bot API и возвращает бинарный ответ с корректным `Content-Type`.

`GET /api/contest/entries/approved` возвращает для каждой одобренной работы поле `image_url`, указывающее на этот локальный маршрут. Внешние `CONTEST_MEDIA_BASE_URL`/`MEDIA_BASE_URL` остаются только как fallback для совместимости.