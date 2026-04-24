# VK Sheets Metrics Sync v7 boevoy

Собирает недельную аналитику ВКонтакте и пишет в Google Sheets.

## Что умеет

- Только полные недели: ПН 00:00 — ВС 23:59.
- Первый запуск: ретро-история по найденным постам или с `VK_BACKFILL_FROM`.
- Повторные запуски: upsert строк по ключу `group_id + week_start`.
- Пересчёт последних N недель через `VK_RECALC_PREVIOUS_WEEKS`.
- Поддержка `stats.get` с fallback на просмотры постов, если статистика недоступна.
- Rate-limit retry/backoff для VK.
- Русские названия колонок.
- Листы:
  - `weekly_summary` — недельная сводка.
  - `posts_raw_vk` — детализация по постам.

## Railway

Start command:

```bash
npm start
```

Рекомендуемые переменные:

```env
VK_ENABLE_STATS=true
VK_REQUEST_DELAY_MS=900
VK_STATS_REQUEST_DELAY_MS=1500
VK_RETRY_MAX=8
VK_RECALC_PREVIOUS_WEEKS=4
GOOGLE_SUMMARY_SHEET_NAME=weekly_summary
GOOGLE_POSTS_SHEET_NAME=posts_raw_vk
```

Если VK отдаёт `[6] Too many requests`, увеличьте задержки:

```env
VK_REQUEST_DELAY_MS=1200
VK_STATS_REQUEST_DELAY_MS=2200
```

## Google Sheets

Создайте листы `weekly_summary` и `posts_raw_vk` или задайте свои имена через env.
Расшарьте таблицу на email service account с правами редактора.
