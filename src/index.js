import 'dotenv/config';
import { google } from 'googleapis';

const CONFIG = {
  vkToken: requiredEnv('VK_TOKEN'),
  vkGroups: splitCsv(requiredEnv('VK_GROUPS')),
  vkVersion: process.env.VK_API_VERSION || '5.199',
  vkEnableStats: parseBool(process.env.VK_ENABLE_STATS, true),
  vkBackfillFrom: process.env.VK_BACKFILL_FROM || '',
  vkWallMaxPosts: parseInt(process.env.VK_WALL_MAX_POSTS || '5000', 10),
  recalcPreviousWeeks: parseInt(process.env.VK_RECALC_PREVIOUS_WEEKS || '4', 10),
  spreadsheetId: requiredEnv('GOOGLE_SHEETS_SPREADSHEET_ID'),
  serviceAccountEmail: requiredEnv('GOOGLE_SERVICE_ACCOUNT_EMAIL'),
  privateKey: normalizePrivateKey(requiredEnv('GOOGLE_PRIVATE_KEY')),
  summarySheetName: process.env.GOOGLE_SUMMARY_SHEET_NAME || 'weekly_summary',
  postsSheetName: process.env.GOOGLE_POSTS_SHEET_NAME || 'posts_raw_vk',
};

const SUMMARY_HEADERS = [
  'Ключ',
  'Дата сбора',
  'Неделя с',
  'Неделя по',
  'ID группы',
  'Короткое имя группы',
  'Название группы',
  'Подписчики на конец недели',
  'Публикации',
  'Просмотры постов',
  'Лайки',
  'Комментарии',
  'Репосты',
  'Вовлеченность',
  'ER по подписчикам, %',
  'ER по просмотрам, %',
  'ER по охвату, %',
  'Посетители сообщества',
  'Просмотры/показы сообщества',
  'Охват всего',
  'Охват подписчиков',
  'Охват мобильный',
  'Подписались',
  'Отписались',
  'Чистый прирост подписчиков',
  'Прирост подписчиков за неделю, %',
  'Дельта подписчиков WoW, абс.',
  'Дельта подписчиков WoW, %',
  'Дельта охвата WoW, абс.',
  'Дельта охвата WoW, %',
  'Дельта показов WoW, абс.',
  'Дельта показов WoW, %',
  'Дельта ER по охвату WoW, п.п.',
  'Дельта ER по охвату WoW, %',
  'Средние просмотры поста',
  'Медианные просмотры поста',
  'Лучший пост недели, ID',
  'Лучший пост недели, ссылка',
  'Источник охвата',
  'Предупреждения',
];

const POSTS_HEADERS = [
  'Ключ',
  'Дата сбора',
  'Неделя с',
  'Неделя по',
  'ID группы',
  'Короткое имя группы',
  'Название группы',
  'ID поста',
  'Дата поста',
  'Ссылка на пост',
  'Текст поста',
  'Просмотры',
  'Лайки',
  'Комментарии',
  'Репосты',
  'Вовлеченность',
  'ER post view, %',
  'Тип',
  'Есть видео/клип',
  'Есть фото',
  'Закреплен',
];

main().catch((error) => {
  console.error('Сбой синхронизации показателей VK.');
  console.error(error?.stack || error?.message || error);
  process.exitCode = 1;
});

async function main() {
  const now = new Date();
  const lastFullWeek = getLastCompletedWeek(now);
  if (!lastFullWeek) throw new Error('Не удалось определить последнюю полную неделю.');

  console.log(`VK metrics sync v5.0 started for ${CONFIG.vkGroups.length} groups. Last full week: ${fmtDate(lastFullWeek.start)}..${fmtDate(lastFullWeek.end)}`);

  const sheets = await createSheetsClient();
  await ensureSheetHeaders(sheets, CONFIG.summarySheetName, SUMMARY_HEADERS);
  await ensureSheetHeaders(sheets, CONFIG.postsSheetName, POSTS_HEADERS);

  const existingSummary = await readSheetObjects(sheets, CONFIG.summarySheetName, SUMMARY_HEADERS);
  const existingByKey = new Map(existingSummary.rows.map((r) => [r.obj['Ключ'], r]));
  const allSummaryRows = [];
  const allPostRows = [];

  for (const rawGroup of CONFIG.vkGroups) {
    const screenName = normalizeVkGroupInput(rawGroup);
    console.log(`Получение группы: ${screenName}`);
    const group = await fetchVkGroup(screenName);
    console.log(`Группа: ${group.name} (${group.id}), подписчиков сейчас: ${group.membersCount}`);

    const groupExisting = existingSummary.rows.filter((r) => String(r.obj['ID группы']) === String(group.id));
    const isFirstRunForGroup = groupExisting.length === 0;

    const initialStartDate = CONFIG.vkBackfillFrom
      ? startOfDay(parseDate(CONFIG.vkBackfillFrom))
      : isFirstRunForGroup
        ? null
        : addDays(lastFullWeek.start, -7 * Math.max(CONFIG.recalcPreviousWeeks, 1));

    const fetchedPosts = await fetchWallPosts(group.id, initialStartDate, CONFIG.vkWallMaxPosts);
    const earliestPostDate = fetchedPosts.length
      ? startOfWeekMonday(new Date(Math.min(...fetchedPosts.map((p) => p.date * 1000))))
      : lastFullWeek.start;

    const startWeek = CONFIG.vkBackfillFrom
      ? startOfWeekMonday(parseDate(CONFIG.vkBackfillFrom))
      : isFirstRunForGroup
        ? earliestPostDate
        : startOfWeekMonday(addDays(lastFullWeek.start, -7 * Math.max(CONFIG.recalcPreviousWeeks, 1)));

    const weeks = buildFullWeeks(startWeek, lastFullWeek.end);
    console.log(`${group.name}: найдено постов=${fetchedPosts.length}, недель к обработке=${weeks.length}`);

    const statsByWeek = CONFIG.vkEnableStats
      ? await fetchStatsByWeeksSafe(group.id, weeks)
      : new Map();

    const groupSummaryRows = [];
    for (const week of weeks) {
      const weekPosts = fetchedPosts.filter((post) => post.date >= toUnix(week.start) && post.date <= toUnix(week.end));
      const stats = statsByWeek.get(fmtDate(week.start)) || emptyStats();
      const row = buildWeeklySummaryRow({
        group,
        week,
        weekPosts,
        stats,
        currentMembersCount: group.membersCount,
      });
      groupSummaryRows.push(row);

      for (const post of weekPosts) {
        allPostRows.push(buildPostRawRow({ group, week, post }));
      }
    }

    applySubscriberHistory(groupSummaryRows, group.membersCount);
    applyWowDeltas(groupSummaryRows);
    allSummaryRows.push(...groupSummaryRows);

    const last = groupSummaryRows[groupSummaryRows.length - 1];
    console.log(`${group.name}: готово. Последняя неделя ${last?.['Неделя с'] || '-'}: публикаций=${last?.['Публикации'] || 0}, просмотров=${last?.['Просмотры постов'] || 0}, ER reach=${last?.['ER по охвату, %'] || 0}`);
  }

  await upsertRowsByKey(sheets, CONFIG.summarySheetName, SUMMARY_HEADERS, allSummaryRows, existingByKey);

  const existingPosts = await readSheetObjects(sheets, CONFIG.postsSheetName, POSTS_HEADERS);
  const existingPostsByKey = new Map(existingPosts.rows.map((r) => [r.obj['Ключ'], r]));
  await upsertRowsByKey(sheets, CONFIG.postsSheetName, POSTS_HEADERS, allPostRows, existingPostsByKey);

  console.log('Синхронизация метрик ВКонтакте успешно завершена.');
  console.table(allSummaryRows.slice(-10).map((r) => ({
    группа: r['Название группы'],
    неделя: r['Неделя с'],
    подписчики: r['Подписчики на конец недели'],
    посты: r['Публикации'],
    просмотры: r['Просмотры постов'],
    охват: r['Охват всего'],
    er_reach: r['ER по охвату, %'],
    предупреждения: r['Предупреждения'],
  })));
}

function buildWeeklySummaryRow({ group, week, weekPosts, stats, currentMembersCount }) {
  const views = sum(weekPosts.map((p) => countOf(p.views)));
  const likes = sum(weekPosts.map((p) => countOf(p.likes)));
  const comments = sum(weekPosts.map((p) => countOf(p.comments)));
  const reposts = sum(weekPosts.map((p) => countOf(p.reposts)));
  const engagement = likes + comments + reposts;
  const avgViews = weekPosts.length ? round(views / weekPosts.length, 2) : 0;
  const medianViews = median(weekPosts.map((p) => countOf(p.views)));
  const bestPost = [...weekPosts].sort((a, b) => countOf(b.views) - countOf(a.views))[0];

  const hasRealReach = stats.reachTotal > 0;
  const reachTotal = hasRealReach ? stats.reachTotal : views;
  const impressions = stats.impressions > 0 ? stats.impressions : views;
  const visitors = stats.visitors || 0;
  const subscribersEndPlaceholder = currentMembersCount;
  const erSubs = percent(engagement, subscribersEndPlaceholder);
  const erViews = percent(engagement, views);
  const erReach = percent(engagement, reachTotal);

  const warnings = [];
  if (!CONFIG.vkEnableStats) warnings.push('VK_ENABLE_STATS=false; охват и показы заменены просмотрами постов');
  if (CONFIG.vkEnableStats && !hasRealReach) warnings.push('stats.get не дал охват; охват заменен просмотрами постов');
  if (CONFIG.vkEnableStats && stats.isEmpty) warnings.push('stats.get пуст/недоступен для недели');

  return {
    'Ключ': `${group.id}|${fmtDate(week.start)}`,
    'Дата сбора': new Date().toISOString(),
    'Неделя с': fmtDate(week.start),
    'Неделя по': fmtDate(week.end),
    'ID группы': group.id,
    'Короткое имя группы': group.screenName || '',
    'Название группы': group.name || '',
    'Подписчики на конец недели': subscribersEndPlaceholder,
    'Публикации': weekPosts.length,
    'Просмотры постов': views,
    'Лайки': likes,
    'Комментарии': comments,
    'Репосты': reposts,
    'Вовлеченность': engagement,
    'ER по подписчикам, %': erSubs,
    'ER по просмотрам, %': erViews,
    'ER по охвату, %': erReach,
    'Посетители сообщества': visitors,
    'Просмотры/показы сообщества': impressions,
    'Охват всего': reachTotal,
    'Охват подписчиков': stats.reachSubscribers || 0,
    'Охват мобильный': stats.reachMobile || 0,
    'Подписались': stats.subscribed || 0,
    'Отписались': stats.unsubscribed || 0,
    'Чистый прирост подписчиков': stats.subscribed - stats.unsubscribed,
    'Прирост подписчиков за неделю, %': 0,
    'Дельта подписчиков WoW, абс.': 0,
    'Дельта подписчиков WoW, %': 0,
    'Дельта охвата WoW, абс.': 0,
    'Дельта охвата WoW, %': 0,
    'Дельта показов WoW, абс.': 0,
    'Дельта показов WoW, %': 0,
    'Дельта ER по охвату WoW, п.п.': 0,
    'Дельта ER по охвату WoW, %': 0,
    'Средние просмотры поста': avgViews,
    'Медианные просмотры поста': medianViews,
    'Лучший пост недели, ID': bestPost?.id || '',
    'Лучший пост недели, ссылка': bestPost ? postUrl(group, bestPost) : '',
    'Источник охвата': hasRealReach ? 'stats.get' : 'fallback: просмотры постов',
    'Предупреждения': warnings.join('; '),
  };
}

function buildPostRawRow({ group, week, post }) {
  const views = countOf(post.views);
  const likes = countOf(post.likes);
  const comments = countOf(post.comments);
  const reposts = countOf(post.reposts);
  const engagement = likes + comments + reposts;
  const attachments = Array.isArray(post.attachments) ? post.attachments : [];
  const hasVideo = attachments.some((a) => a.type === 'video');
  const hasPhoto = attachments.some((a) => a.type === 'photo');

  return {
    'Ключ': `${group.id}|${post.id}`,
    'Дата сбора': new Date().toISOString(),
    'Неделя с': fmtDate(week.start),
    'Неделя по': fmtDate(week.end),
    'ID группы': group.id,
    'Короткое имя группы': group.screenName || '',
    'Название группы': group.name || '',
    'ID поста': post.id,
    'Дата поста': new Date(post.date * 1000).toISOString(),
    'Ссылка на пост': postUrl(group, post),
    'Текст поста': String(post.text || '').replace(/\s+/g, ' ').slice(0, 1000),
    'Просмотры': views,
    'Лайки': likes,
    'Комментарии': comments,
    'Репосты': reposts,
    'Вовлеченность': engagement,
    'ER post view, %': percent(engagement, views),
    'Тип': post.post_type || '',
    'Есть видео/клип': hasVideo ? 'да' : 'нет',
    'Есть фото': hasPhoto ? 'да' : 'нет',
    'Закреплен': post.is_pinned ? 'да' : 'нет',
  };
}

function applySubscriberHistory(rows, currentMembersCount) {
  rows.sort((a, b) => a['Неделя с'].localeCompare(b['Неделя с']));

  const hasStatsGrowth = rows.some((r) => Number(r['Подписались']) || Number(r['Отписались']));
  if (!hasStatsGrowth) {
    for (const row of rows) {
      row['Подписчики на конец недели'] = currentMembersCount;
      row['ER по подписчикам, %'] = percent(row['Вовлеченность'], currentMembersCount);
      appendWarning(row, 'нет stats.get по подпискам; исторические подписчики = текущие');
    }
    return;
  }

  let running = currentMembersCount;
  for (let i = rows.length - 1; i >= 0; i -= 1) {
    const row = rows[i];
    row['Подписчики на конец недели'] = Math.max(0, Math.round(running));
    row['ER по подписчикам, %'] = percent(row['Вовлеченность'], row['Подписчики на конец недели']);
    const net = Number(row['Чистый прирост подписчиков']) || 0;
    running -= net;
  }

  for (let i = 0; i < rows.length; i += 1) {
    const prevSubscribers = i > 0 ? Number(rows[i - 1]['Подписчики на конец недели']) : Number(rows[i]['Подписчики на конец недели']) - Number(rows[i]['Чистый прирост подписчиков']);
    rows[i]['Прирост подписчиков за неделю, %'] = percent(rows[i]['Чистый прирост подписчиков'], prevSubscribers);
  }
}

function applyWowDeltas(rows) {
  rows.sort((a, b) => a['Неделя с'].localeCompare(b['Неделя с']));
  for (let i = 1; i < rows.length; i += 1) {
    const prev = rows[i - 1];
    const cur = rows[i];
    const subDelta = Number(cur['Подписчики на конец недели']) - Number(prev['Подписчики на конец недели']);
    const reachDelta = Number(cur['Охват всего']) - Number(prev['Охват всего']);
    const impressionsDelta = Number(cur['Просмотры/показы сообщества']) - Number(prev['Просмотры/показы сообщества']);
    const erDeltaPp = round(Number(cur['ER по охвату, %']) - Number(prev['ER по охвату, %']), 4);

    cur['Дельта подписчиков WoW, абс.'] = subDelta;
    cur['Дельта подписчиков WoW, %'] = percent(subDelta, Number(prev['Подписчики на конец недели']));
    cur['Дельта охвата WoW, абс.'] = reachDelta;
    cur['Дельта охвата WoW, %'] = percent(reachDelta, Number(prev['Охват всего']));
    cur['Дельта показов WoW, абс.'] = impressionsDelta;
    cur['Дельта показов WoW, %'] = percent(impressionsDelta, Number(prev['Просмотры/показы сообщества']));
    cur['Дельта ER по охвату WoW, п.п.'] = erDeltaPp;
    cur['Дельта ER по охвату WoW, %'] = percent(erDeltaPp, Number(prev['ER по охвату, %']));
  }
}

async function fetchVkGroup(groupInput) {
  const attempts = [
    { group_ids: groupInput, fields: 'members_count,screen_name' },
    { group_id: groupInput, fields: 'members_count,screen_name' },
  ];

  let lastError;
  for (const params of attempts) {
    try {
      const response = await vkApi('groups.getById', params);
      const item = Array.isArray(response) ? response[0] : Array.isArray(response?.groups) ? response.groups[0] : response?.[0];
      if (!item) throw new Error(`Пустой ответ VK для группы ${groupInput}`);
      return {
        id: Number(item.id),
        name: item.name || groupInput,
        screenName: item.screen_name || groupInput,
        membersCount: Number(item.members_count || 0),
      };
    } catch (error) {
      lastError = error;
      console.warn(`Попытка groups.getById не удалась: ${error.message}`);
    }
  }
  throw lastError;
}

async function fetchWallPosts(groupId, startDateOrNull, maxPosts) {
  const ownerId = -Math.abs(Number(groupId));
  const posts = [];
  const count = 100;
  let offset = 0;
  const minUnix = startDateOrNull ? toUnix(startDateOrNull) : 0;

  while (posts.length < maxPosts) {
    const response = await vkApi('wall.get', {
      owner_id: ownerId,
      count,
      offset,
      extended: 0,
    });
    const items = Array.isArray(response?.items) ? response.items : [];
    if (!items.length) break;

    let reachedOldPosts = false;
    for (const post of items) {
      if (post.copy_history) continue;
      // Закреп может быть старым и стоять первым. Не останавливаемся на нем.
      if (post.date < minUnix && !post.is_pinned) {
        reachedOldPosts = true;
        continue;
      }
      if (post.date >= minUnix) posts.push(post);
    }

    offset += items.length;
    if (reachedOldPosts && startDateOrNull) break;
    if (items.length < count) break;
  }

  return posts.slice(0, maxPosts);
}

async function fetchStatsByWeeksSafe(groupId, weeks) {
  const map = new Map();
  for (const week of weeks) {
    try {
      const stats = await fetchStatsForWeek(groupId, week);
      map.set(fmtDate(week.start), stats);
    } catch (error) {
      console.warn(`stats.get недоступен для группы ${groupId}, неделя ${fmtDate(week.start)}: ${error.message}`);
      map.set(fmtDate(week.start), { ...emptyStats(), isEmpty: true });
    }
  }
  return map;
}

async function fetchStatsForWeek(groupId, week) {
  const response = await vkApi('stats.get', {
    group_id: Math.abs(Number(groupId)),
    timestamp_from: toUnix(week.start),
    timestamp_to: toUnix(week.end),
    interval: 'day',
  });

  const days = Array.isArray(response) ? response : Array.isArray(response?.items) ? response.items : [];
  if (!days.length) return { ...emptyStats(), isEmpty: true };

  const result = emptyStats();
  result.isEmpty = false;

  for (const day of days) {
    const visitors = day.visitors || {};
    const reach = day.reach || {};
    const activity = day.activity || {};

    result.visitors += numberFrom(visitors.visitors, visitors.unique_visitors, day.visitors_count);
    result.impressions += numberFrom(visitors.views, visitors.impressions, reach.impressions, day.views);
    result.reachTotal += numberFrom(reach.reach, reach.total, day.reach);
    result.reachSubscribers += numberFrom(reach.reach_subscribers, reach.subscribers);
    result.reachMobile += numberFrom(reach.mobile_reach, reach.reach_mobile, reach.mobile);
    result.subscribed += numberFrom(activity.subscribed, day.subscribed);
    result.unsubscribed += numberFrom(activity.unsubscribed, day.unsubscribed);
  }

  return result;
}

async function vkApi(method, params = {}) {
  const url = new URL(`https://api.vk.com/method/${method}`);
  const query = {
    ...params,
    access_token: CONFIG.vkToken,
    v: CONFIG.vkVersion,
  };
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== null && value !== '') url.searchParams.set(key, String(value));
  }

  const response = await fetch(url, { method: 'GET' });
  const json = await response.json().catch(() => null);
  if (!response.ok || !json) {
    throw new Error(`HTTP ошибка VK ${response.status}: ${JSON.stringify(json)}`);
  }
  if (json.error) {
    console.error(`Ошибка VK API в ${method}: [${json.error.error_code}] ${json.error.error_msg}`);
    console.error(`Параметры метода VK: ${JSON.stringify(params)}`);
    throw new Error(`ошибка VK API в ${method}: [${json.error.error_code}] ${json.error.error_msg}`);
  }
  return json.response;
}

async function createSheetsClient() {
  const auth = new google.auth.JWT({
    email: CONFIG.serviceAccountEmail,
    key: CONFIG.privateKey,
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  });
  await auth.authorize();
  return google.sheets({ version: 'v4', auth });
}

async function ensureSheetHeaders(sheets, sheetName, headers) {
  await ensureSheetExists(sheets, sheetName);
  const range = `${quoteSheet(sheetName)}!1:1`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.spreadsheetId, range }).catch(() => ({ data: { values: [] } }));
  const existing = res.data.values?.[0] || [];
  if (headers.join('|') !== existing.slice(0, headers.length).join('|')) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: CONFIG.spreadsheetId,
      range,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [headers] },
    });
  }
}

async function ensureSheetExists(sheets, sheetName) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId: CONFIG.spreadsheetId });
  const exists = meta.data.sheets?.some((s) => s.properties?.title === sheetName);
  if (exists) return;
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: CONFIG.spreadsheetId,
    requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] },
  });
}

async function readSheetObjects(sheets, sheetName, headers) {
  const range = `${quoteSheet(sheetName)}!A:ZZ`;
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.spreadsheetId, range }).catch(() => ({ data: { values: [] } }));
  const values = res.data.values || [];
  const rows = [];
  for (let i = 1; i < values.length; i += 1) {
    const arr = values[i] || [];
    const obj = {};
    headers.forEach((h, idx) => { obj[h] = arr[idx] ?? ''; });
    if (obj['Ключ']) rows.push({ rowNumber: i + 1, obj, raw: arr });
  }
  return { values, rows };
}

async function upsertRowsByKey(sheets, sheetName, headers, rows, existingByKey) {
  const updates = [];
  const appends = [];

  for (const row of rows) {
    const values = headers.map((h) => row[h] ?? '');
    const existing = existingByKey.get(row['Ключ']);
    if (existing) updates.push({ rowNumber: existing.rowNumber, values });
    else appends.push(values);
  }

  for (const upd of updates) {
    await sheets.spreadsheets.values.update({
      spreadsheetId: CONFIG.spreadsheetId,
      range: `${quoteSheet(sheetName)}!A${upd.rowNumber}`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: [upd.values] },
    });
  }

  if (appends.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: CONFIG.spreadsheetId,
      range: `${quoteSheet(sheetName)}!A1`,
      valueInputOption: 'USER_ENTERED',
      insertDataOption: 'INSERT_ROWS',
      requestBody: { values: appends },
    });
  }

  console.log(`${sheetName}: обновлено=${updates.length}, добавлено=${appends.length}`);
}

function getLastCompletedWeek(now) {
  const thisWeekStart = startOfWeekMonday(now);
  const lastWeekStart = addDays(thisWeekStart, -7);
  return { start: lastWeekStart, end: endOfDay(addDays(lastWeekStart, 6)) };
}

function buildFullWeeks(start, end) {
  const weeks = [];
  let cur = startOfWeekMonday(start);
  const maxEnd = endOfDay(end);
  while (cur <= maxEnd) {
    const weekEnd = endOfDay(addDays(cur, 6));
    if (weekEnd <= maxEnd) weeks.push({ start: new Date(cur), end: weekEnd });
    cur = addDays(cur, 7);
  }
  return weeks;
}

function startOfWeekMonday(date) {
  const d = startOfDay(date);
  const day = d.getUTCDay();
  const diff = day === 0 ? -6 : 1 - day;
  return addDays(d, diff);
}

function startOfDay(date) {
  const d = new Date(date);
  d.setUTCHours(0, 0, 0, 0);
  return d;
}

function endOfDay(date) {
  const d = new Date(date);
  d.setUTCHours(23, 59, 59, 999);
  return d;
}

function addDays(date, days) {
  const d = new Date(date);
  d.setUTCDate(d.getUTCDate() + days);
  return d;
}

function parseDate(value) {
  const d = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(d.getTime())) throw new Error(`Некорректная дата: ${value}`);
  return d;
}

function toUnix(date) {
  return Math.floor(date.getTime() / 1000);
}

function fmtDate(date) {
  return date.toISOString().slice(0, 10);
}

function countOf(obj) {
  return Number(obj?.count ?? obj ?? 0) || 0;
}

function sum(values) {
  return values.reduce((acc, value) => acc + (Number(value) || 0), 0);
}

function median(values) {
  const arr = values.map(Number).filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
  if (!arr.length) return 0;
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 ? arr[mid] : round((arr[mid - 1] + arr[mid]) / 2, 2);
}

function percent(numerator, denominator) {
  const n = Number(numerator) || 0;
  const d = Number(denominator) || 0;
  if (!d) return 0;
  return round((n / d) * 100, 4);
}

function round(value, decimals = 2) {
  const factor = 10 ** decimals;
  return Math.round((Number(value) + Number.EPSILON) * factor) / factor;
}

function numberFrom(...values) {
  for (const value of values) {
    const n = Number(value);
    if (Number.isFinite(n) && n !== 0) return n;
  }
  return 0;
}

function emptyStats() {
  return {
    visitors: 0,
    impressions: 0,
    reachTotal: 0,
    reachSubscribers: 0,
    reachMobile: 0,
    subscribed: 0,
    unsubscribed: 0,
    isEmpty: true,
  };
}

function appendWarning(row, warning) {
  row['Предупреждения'] = row['Предупреждения'] ? `${row['Предупреждения']}; ${warning}` : warning;
}

function postUrl(group, post) {
  return `https://vk.com/wall-${Math.abs(Number(group.id))}_${post.id}`;
}

function normalizeVkGroupInput(input) {
  let value = String(input || '').trim();
  value = value.replace(/^https?:\/\/(m\.)?vk\.com\//i, '');
  value = value.replace(/^@/, '');
  value = value.split(/[/?#]/)[0];
  value = value.replace(/^club/i, '').replace(/^public/i, '');
  return value;
}

function splitCsv(value) {
  return String(value || '').split(',').map((v) => v.trim()).filter(Boolean);
}

function parseBool(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') return defaultValue;
  return ['1', 'true', 'yes', 'y', 'да'].includes(String(value).toLowerCase());
}

function normalizePrivateKey(value) {
  return String(value).replace(/\\n/g, '\n');
}

function quoteSheet(sheetName) {
  return `'${String(sheetName).replace(/'/g, "''")}'`;
}

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) throw new Error(`Не задана переменная окружения ${name}`);
  return value;
}
