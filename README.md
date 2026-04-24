# VK Sheets Metrics Sync v9.2 final fix

Боевой JS-скрипт для Railway: собирает метрики VK, пишет в Google Sheets, готовит dashboard_data, AI-рекомендации, A/B тесты, прогноз роста, клипы/шортсы и недельную ЦА.

## Главное в v9.2

- Новые листы больше не остаются пустыми: если VK не отдал данные, пишется диагностическая строка.
- Охваты считаются всегда: точные из stats.get, если доступны; иначе оценочные по просмотрам постов с предупреждением.
- ER по подписчикам заполняется оценочно, если нет исторической базы подписчиков.
- Время публикаций считается в Asia/Yekaterinburg.
- Rate limit VK обрабатывается retry/backoff.
- Нули не подставляются как будто это точные данные: пустые/недоступные поля получают предупреждения.

## Railway

Start command:

```bash
npm start
```

Рекомендуемые переменные:

```env
VK_ENABLE_STATS=true
VK_DAYS_BACK=180
VK_WALL_MAX_POSTS=500
VK_REQUEST_DELAY_MS=900
VK_STATS_REQUEST_DELAY_MS=1400
VK_RETRY_MAX=8
VK_REACH_SUBSCRIBERS_FALLBACK_RATIO=0.7
```

## Листы

- weekly_summary
- posts_raw_vk
- dashboard_data
- ai_insights
- ai_posts
- ab_tests
- growth_forecast
- clips_analytics
- audience_weekly

## Важно про VK

Если stats.get не отдаёт охваты, посетителей, подписки/отписки или демографию, это ограничение API/прав/размера сообщества. Скрипт в таких случаях пишет предупреждения и строит оценочные метрики только там, где это допустимо.
