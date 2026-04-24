# VK Sheets Metrics Sync v8.2

Исправленная версия после багов v8.1:

- не размазывает текущих подписчиков по историческим неделям;
- не пишет фейковые нули там, где VK `stats.get` не отдал данные;
- пишет предупреждения по каждой строке;
- считает время публикаций в `Asia/Yekaterinburg`;
- готовит листы `weekly_summary`, `posts_raw_vk`, `dashboard_data`, `ai_recommendations`, `ai_insights`;
- добавляет более умные рекомендации по форматам, времени, частоте и рискам.

## Railway

Start command:

```bash
npm start
```

Минимальные переменные:

```env
VK_TOKEN=...
VK_GROUPS=pickme_asbest,pick_me_ku
VK_ENABLE_STATS=true
VK_REQUEST_DELAY_MS=900
VK_STATS_REQUEST_DELAY_MS=1600
VK_RETRY_MAX=8
REPORT_TIMEZONE=Asia/Yekaterinburg
REPORT_TIMEZONE_LABEL=ЕКБ
GOOGLE_SHEETS_SPREADSHEET_ID=...
GOOGLE_SERVICE_ACCOUNT_EMAIL=...
GOOGLE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
```

## Важно про подписчиков

Историческое значение подписчиков нельзя точно восстановить из текущего `members_count`, если VK не отдаёт подписки/отписки через `stats.get` и раньше не было сохранённых снимков. Поэтому v8.2:

- ставит точное значение только когда может его обосновать;
- иначе оставляет пусто и пишет предупреждение;
- текущих подписчиков пишет отдельно в сыром смысле только в актуальном контексте, но не размазывает по истории.

## Важно про stats.get

Если VK не отдаёт посетителей, показы, охваты, подписки/отписки — это ограничение доступа/API. v8.2 не подменяет их нулями. Для части метрик используется fallback от просмотров постов, и это явно отмечается в `Источник охвата` и `Предупреждения`.
