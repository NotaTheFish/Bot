\# userbot\_worker



Worker исполняет задачи из `userbot\_tasks` и рассылает посты через Telethon от лица user-аккаунта.



\## ENV



\- `DATABASE\_URL`

\- `TELEGRAM\_API\_ID`

\- `TELEGRAM\_API\_HASH`

\- `TELETHON\_SESSION` (путь к `.session` файлу или string-session)

\- `MIN\_SECONDS\_BETWEEN\_CHATS` (например 20)

\- `MAX\_SECONDS\_BETWEEN\_CHATS` (например 40)

\- `WORKER\_POLL\_SECONDS` (например 10)

\- `ADMIN\_NOTIFY\_BOT\_TOKEN` (optional)

\- `ADMIN\_ID` (optional)

- `MAX\_TASK\_ATTEMPTS` (optional, по умолчанию `3`)



\## Запуск Windows



```powershell

cd userbot\_worker

python -m venv .venv

.\\.venv\\Scripts\\activate

pip install -r requirements.txt

set DATABASE\_URL=postgresql://...

set TELEGRAM\_API\_ID=123456

set TELEGRAM\_API\_HASH=...

set TELETHON\_SESSION=userbot.session

python -m userbot\_worker.main

```



\## Запуск Railway/VPS



```bash

cd userbot\_worker

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt

export DATABASE\_URL=postgresql://...

export TELEGRAM\_API\_ID=123456

export TELEGRAM\_API\_HASH=...

export TELETHON\_SESSION=/data/userbot.session

python -m userbot\_worker.main

```



\## Что делает



\- Берёт `pending`-задачи из `userbot\_tasks` атомарно (`FOR UPDATE SKIP LOCKED`), переводит в `processing` и увеличивает `attempts`.

\- Отправляет пост только в чаты из `target\_chat\_ids`; если массив пустой — использует `TARGET_CHAT_IDS` из env воркера.

\- Между чатами делает случайную задержку.

\- Обрабатывает `FloodWaitError` (ждёт и продолжает).

\- На успех обновляет `chats.last\_success\_post\_at`, сбрасывает счётчик сообщений.

\- Логирует попытки в `broadcast\_attempts`.





## Статусы задач (`userbot_tasks.status`)

- `pending` — задача ожидает выполнения или поставлена на ретрай.
- `processing` — задача взята воркером в обработку.
- `done` — задача завершена (включая частичный успех по чатам, если часть отправок прошла).
- `error` — задача завершена неуспешно и исчерпала лимит `MAX_TASK_ATTEMPTS`.

Поведение после обработки:

- при завершении отправки (в том числе частичном по чатам) задача получает `done`, а в БД сохраняются `sent_count`, `error_count`, `last_error`;
- при task-level ошибке до завершения и `attempts < MAX_TASK_ATTEMPTS` задача возвращается в `pending` с `last_error`;
- при task-level ошибке и `attempts >= MAX_TASK_ATTEMPTS` задача получает `failed` с `last_error`.
