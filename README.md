# VK Sheets Metrics Sync v5

Боевой JS-скрипт для еженедельного сбора аналитики пабликов ВКонтакте и записи в Google Sheets.

## Что собирает

### Лист `weekly_summary`

- подписчики на конец недели;
- публикации;
- просмотры постов;
- лайки;
- комментарии;
- репосты;
- вовлеченность;
- ER по подписчикам;
- ER по просмотрам;
- ER по охвату;
- посетители сообщества;
- просмотры/показы сообщества;
- охват всего;
- охват подписчиков;
- охват мобильный;
- подписались;
- отписались;
- чистый прирост подписчиков;
- WoW-дельты в абсолюте и процентах.

### Лист `posts_raw_vk`

- сырые данные по каждому посту;
- просмотры, лайки, комментарии, репосты;
- ER post view;
- ссылка на пост;
- признаки видео/фото/закрепа.

## Логика недель

Скрипт записывает только полные недели:

- начало: понедельник 00:00 UTC;
- конец: воскресенье 23:59 UTC;
- текущая неполная неделя не записывается.

## Первый и повторные запуски

Первый запуск для группы:

- если задан `VK_BACKFILL_FROM`, строит историю с этой даты;
- если `VK_BACKFILL_FROM` пустой, пытается построить историю от самого раннего найденного поста в пределах `VK_WALL_MAX_POSTS`.

Повторный запуск:

- пересчитывает последние `VK_RECALC_PREVIOUS_WEEKS` недель;
- обновляет уже существующие строки по ключу;
- добавляет только новые строки, если их ещё нет.

## Важное ограничение по подписчикам

Исторические подписчики считаются точно только если `stats.get` возвращает `Подписались` и `Отписались`.

Если `stats.get` недоступен или пустой, скрипт использует текущий `members_count` для всех исторических недель и ставит предупреждение в колонку `Предупреждения`.

## Важное ограничение по охватам

Если `stats.get` не отдаёт охваты, скрипт использует fallback:

```text
Охват всего = Просмотры постов
Показы = Просмотры постов
ER по охвату = ER по просмотрам
```

Источник будет указан в колонке `Источник охвата`.

## Переменные Railway

```env
VK_TOKEN=vk1.a.xxxxxxxxxxxxxxxxx
VK_GROUPS=pickme_asbest,pick_me_ku
VK_API_VERSION=5.199
VK_BACKFILL_FROM=
VK_WALL_MAX_POSTS=5000
VK_ENABLE_STATS=true
VK_RECALC_PREVIOUS_WEEKS=4
GOOGLE_SHEETS_SPREADSHEET_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
GOOGLE_SERVICE_ACCOUNT_EMAIL=your-service-account@your-project.iam.gserviceaccount.com
GOOGLE_PRIVATE_KEY=-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n
GOOGLE_SUMMARY_SHEET_NAME=weekly_summary
GOOGLE_POSTS_SHEET_NAME=posts_raw_vk
```

## Установка локально

```bash
npm install
cp .env.example .env
npm start
```

## Railway

Start Command:

```bash
npm start
```

Cron пример для запуска каждый понедельник в 09:00 UTC:

```cron
0 9 * * 1
```

## Требования к VK token

Нужен user access token, не group token.

Желательные права:

```text
groups,wall,stats,offline
```

## Требования к Google

1. Включить Google Sheets API.
2. Создать Service Account.
3. Создать JSON key.
4. Расшарить Google Sheet на email service account с правом редактора.
