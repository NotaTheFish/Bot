\# Bot Project



\## Contest Mini App (web)



Фронтенд голосования лежит в `contest\_webapp/` и состоит из:



\- `index.html`

\- `styles.css`

\- `app.js`



\### Деплой статики



1\. Разместите папку `contest\_webapp` на любом статическом хостинге (Nginx, S3+CloudFront, Vercel, Netlify).

2\. Проверьте, что Mini App открывается по HTTPS URL, например:

&nbsp;  `https://your-domain.example/contest\_webapp/index.html`.

3\. При необходимости задайте в `index.html` до загрузки `app.js` глобальные переменные:

&nbsp;  - `window.CONTEST\_API\_BASE\_URL` — базовый URL API (если API на другом домене);

&nbsp;  - `window.CONTEST\_RULES\_URL` — ссылка на правила конкурса;

&nbsp;  - `window.CONTEST\_MEDIA\_BASE\_URL` — базовый URL для изображений работ.



\### Настройка Mini App URL в Telegram



1\. В BotFather откройте настройки вашего бота и задайте Web App URL.

2\. Укажите URL страницы голосования, например:

&nbsp;  `https://your-domain.example/contest\_webapp/index.html`.

3\. Убедитесь, что backend принимает `X-Telegram-Init-Data` и корректно валидирует `Telegram.WebApp.initData`.



\## Зеркала клиентов (multi-tenant SaaS)


Онбординг через `/code` в **основном** боте сохраняет учётные данные клиента в `tenant_profiles` (включая `workspace_key`). Клиенту показывается только статус зеркала; отдельный VPS/Railway, репозиторий и переменные вроде `DATABASE_URL` / `SINGLETON_LOCK_KEY` ему не нужны.


На стороне оператора обычно два процесса: **один** `bot_controller` (основной бот + опрос зеркальных ботов по токенам из БД) и **один** `userbot_worker`, который обслуживает всех арендаторов из той же базы (задачи и цели изолированы по `workspace_key`).


Остановка рассылки и служебный cleanup в основном контроллере по-прежнему затрагивают только задачи с `workspace_key=main`, чтобы не отменять задачи арендаторов.

