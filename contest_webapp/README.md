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

