# ReviewBot

Telegram-бот для красивых отзывов.

## Структура

```
reviewbot/
├── bot.py               # Точка входа
├── config.py            # Конфиг из env
├── db.py                # База данных (asyncpg)
├── constants.py         # Константы и шаблоны
├── keyboards.py         # Все клавиатуры
├── handlers/
│   ├── start.py         # /start, меню продавца
│   ├── setup.py         # Wizard настройки шаблона
│   ├── review.py        # Flow покупателя
│   └── inline.py        # Inline mode
├── services/
│   └── card_generator.py  # Генерация PNG через Pillow
├── utils/
│   └── helpers.py       # Вспомогательные функции
└── fonts/               # TTF шрифты (добавить вручную)
    ├── Montserrat-Regular.ttf
    ├── Montserrat-SemiBold.ttf
    └── Montserrat-Bold.ttf
```

## Запуск

### 1. Переменные окружения

Скопируй `.env.example` в `.env` и заполни:

```
BOT_TOKEN=        # Токен от @BotFather
DATABASE_URL=     # PostgreSQL connection string
BOT_USERNAME=     # Username бота без @
```

### 2. Шрифты

Скачай Montserrat с Google Fonts и положи TTF в папку `fonts/`.

### 3. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 4. Запуск

```bash
python bot.py
```

### 5. Railway деплой

Добавь переменные окружения в Railway Variables.
`railway.toml` уже настроен — просто пушь в репозиторий.

## Таблицы БД

Префикс `rvb_` — не конфликтует с другими проектами.

- `rvb_sellers` — продавцы и их шаблоны
- `rvb_reviews` — сохранённые отзывы

Таблицы создаются автоматически при запуске.

## Inline mode

Включи в @BotFather: Bot Settings → Inline Mode → Enable.

## Шаблоны карточек

- `classic_gold` — тёмный премиум, золотые акценты ✅ реализован
- `retro_paper`  — крафт-стиль (TODO)
- `dark_slate`   — tech-стиль (TODO)
- `clean_white`  — минимализм (TODO)
- `sketch_paper` — карандаш на бумаге (TODO)
