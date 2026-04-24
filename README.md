# VK Sheets Metrics Sync

JS-скрипт для Railway Cron: раз в неделю забирает метрики ВКонтакте и пишет их в Google Sheets.

## Что собирается

### По паблику / сообществу
- текущее количество подписчиков `members_count`
- количество постов за период
- суммарные / средние / медианные просмотры
- лайки, комментарии, репосты
- вовлечения и ER
- лучший пост за период

### По постам
Если `VK_WRITE_POSTS_RAW=true`, скрипт пишет отдельную строку по каждому посту в лист `posts_raw`.

### Community stats
Если `VK_ENABLE_STATS=true`, скрипт пробует вызвать `stats.get` и добавить в summary:
- visitors
- views
- reach
- reach_subscribers
- mobile_reach
- subscribed
- unsubscribed

Если VK не даст доступ к `stats.get`, скрипт не упадёт, а запишет базовые метрики по постам.

## Railway

Start command:

```bash
npm start
```

Cron example, каждый понедельник в 09:00 UTC:

```cron
0 9 * * 1
```

## Google Sheets

Создай таблицу и дай доступ на редактирование email сервисного аккаунта.
Скрипт сам создаёт/обновляет заголовки в листах:

- `weekly_summary`
- `posts_raw`

## Переменные окружения

Смотри `.env.example`.

Особенно важно: `GOOGLE_PRIVATE_KEY` в Railway хранить одной строкой с `\n` вместо переносов.
