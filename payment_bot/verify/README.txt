Сюда кладутся файлы подтверждения домена от платёжных агрегаторов.

Когда Lava / Payok / Freekassa просят загрузить HTML-файл в корень сайта
(например lava-verify_b7da374ad60c5915.html):

1. Скачай файл который дал агрегатор
2. Положи его в эту папку (payment_bot/verify/)
3. Запушь в git → Railway задеплоит
4. Файл станет доступен по адресу:
   https://paymentbot-production-ace4.up.railway.app/lava-verify_b7da374ad60c5915.html
5. Нажми "Продолжить" в Lava

Бот автоматически отдаёт любой .html или .txt файл из этой папки.
