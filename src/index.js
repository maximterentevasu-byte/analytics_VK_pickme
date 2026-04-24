import 'dotenv/config';
import { google } from 'googleapis';

const VK_API_VERSION = process.env.VK_API_VERSION || '5.199';
const WALL_MAX_POSTS = Number(process.env.VK_WALL_MAX_POSTS || 1000);
const ENABLE_STATS = String(process.env.VK_ENABLE_STATS || 'true').toLowerCase() !== 'false';
const WRITE_POSTS_RAW = String(process.env.VK_WRITE_POSTS_RAW || 'true').toLowerCase() !== 'false';
const RECALC_PREVIOUS_WEEKS = Number(process.env.VK_RECALC_PREVIOUS_WEEKS || 4);
const BACKFILL_FROM = (process.env.VK_BACKFILL_FROM || '').trim();
const REQUEST_DELAY_MS = Number(process.env.VK_REQUEST_DELAY_MS || 350);

const SUMMARY_HEADERS = [
  'Собрано в',
  'Неделя с',
  'Неделя по',
  'ID группы',
  'Короткое имя',
  'Название группы',
  'Подписчики сейчас',
  'Подписчики на конец недели, расчёт',
  'Посты за неделю',
  'Посты с просмотрами',
  'Просмотры постов',
  'Средние просмотры поста',
  'Медиана просмотров поста',
  'Максимум просмотров поста',
  'Лайки',
  'Комментарии',
  'Репосты',
  'Вовлечения',
  'Средние вовлечения на пост',
  'ER по подписчикам, %',
  'ER по охвату, %',
  'ER по просмотрам, %',
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
  'Постов в день',
  'Лучший пост ID',
  'Лучший пост URL',
  'Лучший пост просмотры',
  'Лучший пост лайки',
  'Лучший пост комментарии',
  'Лучший пост репосты',
  'Предупреждения'
];

const POSTS_HEADERS = [
  'Собрано в',
  'Неделя с',
  'Неделя по',
  'ID группы',
  'Короткое имя',
  'Название группы',
  'ID поста',
  'Дата поста',
  'URL поста',
  'Тип поста',
  'Просмотры',
  'Лайки',
  'Комментарии',
  'Репосты',
  'Вовлечения',
  'ER по подписчикам, %',
  'ER по просмотрам, %',
  'Текст, превью'
];

function getRequiredEnv(name) {
  const value = process.env[name];
  if (!value || !value.trim()) throw new Error(`Missing required env var: ${name}`);
  return value.trim();
}

function parseGroups(raw) {
  return raw.split(',').map((item) => item.trim()).filter(Boolean);
}

function normalizeGooglePrivateKey(raw) {
  return raw.replace(/\\n/g, '\n');
}

function normalizeVkGroupId(value) {
  return value
    .trim()
    .replace(/^https?:\/\/(m\.)?vk\.com\//i, '')
    .replace(/^vk\.com\//i, '')
    .replace(/^@/i, '')
    .replace(/^public/i, '')
    .replace(/^club/i, '')
    .replace(/\?.*$/, '')
    .replace(/\/$/, '');
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toEpochSeconds(date) {
  return Math.floor(date.getTime() / 1000);
}

function dateOnlyUtc(date) {
  return date.toISOString().slice(0, 10);
}

function dateFromYmd(ymd) {
  const [y, m, d] = ymd.split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d, 0, 0, 0));
}

function addDays(date, days) {
  const copy = new Date(date.getTime());
  copy.setUTCDate(copy.getUTCDate() + days);
  return copy;
}

function startOfIsoWeekUtc(date) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const day = d.getUTCDay() || 7; // Monday=1, Sunday=7
  d.setUTCDate(d.getUTCDate() - day + 1);
  return d;
}

function lastCompletedIsoWeek() {
  const now = new Date();
  const currentWeekStart = startOfIsoWeekUtc(now);
  const start = addDays(currentWeekStart, -7);
  const end = addDays(currentWeekStart, -1);
  return weekObject(start);
}

function weekObject(startDate) {
  const start = startOfIsoWeekUtc(startDate);
  const end = addDays(start, 6);
  const fromUnix = toEpochSeconds(start);
  const untilUnix = toEpochSeconds(new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate(), 23, 59, 59)));
  return {
    start,
    end,
    dateFrom: dateOnlyUtc(start),
    dateTo: dateOnlyUtc(end),
    fromUnix,
    untilUnix,
    key: dateOnlyUtc(start)
  };
}

function listWeeksInclusive(startDate, lastWeek) {
  const weeks = [];
  let cursor = startOfIsoWeekUtc(startDate);
  while (dateOnlyUtc(cursor) <= lastWeek.dateFrom) {
    weeks.push(weekObject(cursor));
    cursor = addDays(cursor, 7);
  }
  return weeks;
}

function round(value, digits = 2) {
  if (!Number.isFinite(value)) return 0;
  const m = 10 ** digits;
  return Math.round(value * m) / m;
}

function pctDelta(current, previous, digits = 2) {
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous === 0) return '';
  return String(round(((current - previous) / previous) * 100, digits));
}

function absDelta(current, previous) {
  if (!Number.isFinite(current) || !Number.isFinite(previous)) return '';
  return String(round(current - previous, 4));
}

function median(numbers) {
  const arr = numbers.filter((n) => Number.isFinite(n)).sort((a, b) => a - b);
  if (!arr.length) return 0;
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2;
}

function sum(numbers) {
  return numbers.reduce((acc, n) => acc + (Number(n) || 0), 0);
}

async function vkApi(method, params, token) {
  const url = new URL(`https://api.vk.com/method/${method}`);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') url.searchParams.set(key, String(value));
  });
  url.searchParams.set('access_token', token);
  url.searchParams.set('v', VK_API_VERSION);

  await sleep(REQUEST_DELAY_MS);
  const response = await fetch(url, {
    method: 'GET',
    headers: { Accept: 'application/json', 'User-Agent': 'vk-sheets-metrics-sync/4.0' }
  });

  if (!response.ok) throw new Error(`VK HTTP ${response.status} for ${method}`);
  const data = await response.json();

  if (data.error) {
    const code = data.error.error_code ?? 'unknown';
    const msg = data.error.error_msg ?? 'Unknown VK API error';
    console.error(`Ошибка VK API в ${method}: [${code}] ${msg}`);
    console.error(`Параметры метода VK: ${JSON.stringify(params)}`);
    console.error(`Ошибка VK raw: ${JSON.stringify(data.error)}`);
    throw new Error(`Ошибка VK API в ${method}: [${code}] ${msg}`);
  }
  return data.response;
}

function extractGroup(response) {
  if (Array.isArray(response)) return response[0] || null;
  if (response?.groups && Array.isArray(response.groups)) return response.groups[0] || null;
  if (response?.id) return response;
  return null;
}

async function fetchVkGroup(rawGroupId, token) {
  const groupId = normalizeVkGroupId(rawGroupId);
  let group = null;
  let lastResponse = null;

  try {
    lastResponse = await vkApi('groups.getById', { group_ids: groupId, fields: 'members_count,screen_name' }, token);
    group = extractGroup(lastResponse);
  } catch (error) {
    console.warn(`Первая попытка groups.getById не удалась: ${error.message}`);
  }

  if (!group) {
    lastResponse = await vkApi('groups.getById', { group_id: groupId, fields: 'members_count,screen_name' }, token);
    group = extractGroup(lastResponse);
  }

  if (!group) throw new Error(`VK не вернул данные группы "${rawGroupId}". Ответ: ${JSON.stringify(lastResponse)}`);

  return {
    requested_group_id: rawGroupId,
    normalized_group_id: groupId,
    group_id: Number(group.id),
    group_name: group.name || '',
    screen_name: group.screen_name || groupId,
    members_count: Number(group.members_count || 0)
  };
}

function classifyPost(post) {
  const types = (post.attachments || []).map((a) => a.type).filter(Boolean);
  if (types.includes('video')) return 'видео/клип';
  if (types.includes('photo')) return 'фото';
  if (types.includes('link')) return 'ссылка';
  if (types.includes('doc')) return 'документ';
  if (types.length) return types.join('+');
  return post.text ? 'текст' : 'неизвестно';
}

async function fetchWallPostsSince(group, token, fromUnix) {
  const ownerId = -Math.abs(group.group_id);
  const posts = [];
  let offset = 0;
  const count = 100;

  while (posts.length < WALL_MAX_POSTS) {
    const response = await vkApi('wall.get', { owner_id: ownerId, count, offset, extended: 0 }, token);
    const items = response?.items || [];
    if (!items.length) break;

    let foundOlderRegularPost = false;
    for (const post of items) {
      if (!post.date) continue;
      if (post.is_pinned) continue;
      if (post.date < fromUnix) {
        foundOlderRegularPost = true;
        continue;
      }
      if (post.post_type !== 'copy') posts.push(post);
      if (posts.length >= WALL_MAX_POSTS) break;
    }

    offset += items.length;
    if (items.length < count || foundOlderRegularPost) break;
  }

  return posts;
}

async function findEarliestAvailablePostDate(group, token) {
  const ownerId = -Math.abs(group.group_id);
  const firstPage = await vkApi('wall.get', { owner_id: ownerId, count: 1, offset: 0, extended: 0 }, token);
  const total = Number(firstPage?.count || 0);
  if (!total) return new Date();

  // VK may limit deep offsets. Try the last known offset first, then fall back gracefully.
  const offsets = [Math.max(total - 1, 0), Math.max(total - 100, 0), 0];
  for (const offset of offsets) {
    try {
      const response = await vkApi('wall.get', { owner_id: ownerId, count: 100, offset, extended: 0 }, token);
      const items = (response?.items || []).filter((p) => p.date && !p.is_pinned);
      if (items.length) {
        const minDate = Math.min(...items.map((p) => Number(p.date)));
        return new Date(minDate * 1000);
      }
    } catch (error) {
      console.warn(`Не удалось получить ранние посты offset=${offset}: ${error.message}`);
    }
  }
  return new Date();
}

function safeGetNumber(obj, keys) {
  for (const key of keys) {
    if (obj && Number.isFinite(Number(obj[key]))) return Number(obj[key]);
  }
  return 0;
}

function accumulateStats(response) {
  const rows = Array.isArray(response) ? response : [];
  const result = {
    visitors: 0,
    views: 0,
    reach: 0,
    reach_subscribers: 0,
    mobile_reach: 0,
    subscribed: 0,
    unsubscribed: 0
  };

  for (const day of rows) {
    const visitors = day.visitors || {};
    const reach = day.reach || {};
    const activity = day.activity || {};

    result.visitors += safeGetNumber(visitors, ['visitors', 'count', 'total']);
    result.views += safeGetNumber(visitors, ['views']);
    result.reach += safeGetNumber(reach, ['reach', 'total']);
    result.reach_subscribers += safeGetNumber(reach, ['reach_subscribers', 'subscribers']);
    result.mobile_reach += safeGetNumber(reach, ['mobile_reach']);
    result.subscribed += safeGetNumber(activity, ['subscribed']);
    result.unsubscribed += safeGetNumber(activity, ['unsubscribed']);
  }

  return result;
}

async function fetchCommunityStats(group, token, week) {
  if (!ENABLE_STATS) return { stats: {}, warning: 'VK_ENABLE_STATS=false' };
  try {
    const response = await vkApi('stats.get', {
      group_id: group.group_id,
      date_from: week.dateFrom,
      date_to: week.dateTo
    }, token);
    return { stats: accumulateStats(response), warning: '' };
  } catch (error) {
    return { stats: {}, warning: `stats.get недоступен: ${error.message}` };
  }
}

function postToMetrics(post, group) {
  const views = Number(post.views?.count || 0);
  const likes = Number(post.likes?.count || 0);
  const comments = Number(post.comments?.count || 0);
  const reposts = Number(post.reposts?.count || 0);
  const engagements = likes + comments + reposts;
  const url = `https://vk.com/wall-${group.group_id}_${post.id}`;

  return {
    post_id: post.id,
    date_iso: new Date(post.date * 1000).toISOString(),
    url,
    type: classifyPost(post),
    views,
    likes,
    comments,
    reposts,
    engagements,
    er_by_subscribers_pct: group.members_count ? round((engagements / group.members_count) * 100, 4) : 0,
    er_by_views_pct: views ? round((engagements / views) * 100, 4) : 0,
    text_preview: (post.text || '').replace(/\s+/g, ' ').slice(0, 180)
  };
}

function weekKeyForPost(post) {
  return dateOnlyUtc(startOfIsoWeekUtc(new Date(post.date * 1000)));
}

function buildRawSummary({ group, week, postsMetrics, statsData, collectedAt }) {
  const views = postsMetrics.map((p) => p.views);
  const engagements = postsMetrics.map((p) => p.engagements);
  const totalViews = sum(views);
  const totalLikes = sum(postsMetrics.map((p) => p.likes));
  const totalComments = sum(postsMetrics.map((p) => p.comments));
  const totalReposts = sum(postsMetrics.map((p) => p.reposts));
  const totalEngagements = sum(engagements);
  const topPost = [...postsMetrics].sort((a, b) => b.views - a.views || b.engagements - a.engagements)[0] || {};
  const stats = statsData.stats || {};
  const reach = Number(stats.reach || 0);
  const communityViews = Number(stats.views || 0);
  const subscribed = Number(stats.subscribed || 0);
  const unsubscribed = Number(stats.unsubscribed || 0);
  const netGrowth = subscribed - unsubscribed;
  const warnings = [statsData.warning].filter(Boolean).join(' | ');

  return {
    collectedAt,
    periodFrom: week.dateFrom,
    periodTo: week.dateTo,
    groupId: String(group.group_id),
    screenName: group.screen_name,
    groupName: group.group_name,
    membersNow: group.members_count,
    membersEndEstimate: 0,
    postsCount: postsMetrics.length,
    postsWithViews: views.filter((v) => v > 0).length,
    postViews: totalViews,
    avgPostViews: round(postsMetrics.length ? totalViews / postsMetrics.length : 0),
    medianPostViews: round(median(views)),
    maxPostViews: Math.max(0, ...views),
    likes: totalLikes,
    comments: totalComments,
    reposts: totalReposts,
    engagements: totalEngagements,
    avgEngagements: round(postsMetrics.length ? totalEngagements / postsMetrics.length : 0),
    erSubscribers: group.members_count ? round((totalEngagements / group.members_count) * 100, 4) : 0,
    erReach: reach ? round((totalEngagements / reach) * 100, 4) : 0,
    erViews: totalViews ? round((totalEngagements / totalViews) * 100, 4) : 0,
    visitors: Number(stats.visitors || 0),
    communityViews,
    reach,
    reachSubscribers: Number(stats.reach_subscribers || 0),
    mobileReach: Number(stats.mobile_reach || 0),
    subscribed,
    unsubscribed,
    netGrowth,
    growthPct: 0,
    deltaMembersAbs: '',
    deltaMembersPct: '',
    deltaReachAbs: '',
    deltaReachPct: '',
    deltaViewsAbs: '',
    deltaViewsPct: '',
    deltaErReachPp: '',
    deltaErReachPct: '',
    postsPerDay: round(postsMetrics.length / 7, 2),
    topPostId: topPost.post_id ? String(topPost.post_id) : '',
    topPostUrl: topPost.url || '',
    topPostViews: topPost.views || 0,
    topPostLikes: topPost.likes || 0,
    topPostComments: topPost.comments || 0,
    topPostReposts: topPost.reposts || 0,
    warnings
  };
}

function finalizeWeeklyDeltas(rows, group) {
  const sorted = rows.sort((a, b) => a.periodFrom.localeCompare(b.periodFrom));
  if (!sorted.length) return sorted;

  // Approximation: VK does not reliably expose historical member count snapshots here.
  // We reconstruct week-end subscribers backwards from the current count and weekly net growth.
  sorted[sorted.length - 1].membersEndEstimate = group.members_count;
  for (let i = sorted.length - 2; i >= 0; i--) {
    sorted[i].membersEndEstimate = sorted[i + 1].membersEndEstimate - sorted[i + 1].netGrowth;
  }

  for (let i = 0; i < sorted.length; i++) {
    const row = sorted[i];
    const prev = sorted[i - 1];
    row.growthPct = row.membersEndEstimate ? round((row.netGrowth / row.membersEndEstimate) * 100, 4) : 0;
    if (!prev) continue;
    row.deltaMembersAbs = absDelta(row.membersEndEstimate, prev.membersEndEstimate);
    row.deltaMembersPct = pctDelta(row.membersEndEstimate, prev.membersEndEstimate, 4);
    row.deltaReachAbs = absDelta(row.reach, prev.reach);
    row.deltaReachPct = pctDelta(row.reach, prev.reach, 2);
    row.deltaViewsAbs = absDelta(row.communityViews, prev.communityViews);
    row.deltaViewsPct = pctDelta(row.communityViews, prev.communityViews, 2);
    row.deltaErReachPp = absDelta(row.erReach, prev.erReach);
    row.deltaErReachPct = pctDelta(row.erReach, prev.erReach, 2);
  }
  return sorted;
}

function summaryToRow(row) {
  return [
    row.collectedAt,
    row.periodFrom,
    row.periodTo,
    row.groupId,
    row.screenName,
    row.groupName,
    String(row.membersNow),
    String(row.membersEndEstimate),
    String(row.postsCount),
    String(row.postsWithViews),
    String(row.postViews),
    String(row.avgPostViews),
    String(row.medianPostViews),
    String(row.maxPostViews),
    String(row.likes),
    String(row.comments),
    String(row.reposts),
    String(row.engagements),
    String(row.avgEngagements),
    String(row.erSubscribers),
    String(row.erReach),
    String(row.erViews),
    String(row.visitors),
    String(row.communityViews),
    String(row.reach),
    String(row.reachSubscribers),
    String(row.mobileReach),
    String(row.subscribed),
    String(row.unsubscribed),
    String(row.netGrowth),
    String(row.growthPct),
    String(row.deltaMembersAbs),
    String(row.deltaMembersPct),
    String(row.deltaReachAbs),
    String(row.deltaReachPct),
    String(row.deltaViewsAbs),
    String(row.deltaViewsPct),
    String(row.deltaErReachPp),
    String(row.deltaErReachPct),
    String(row.postsPerDay),
    row.topPostId,
    row.topPostUrl,
    String(row.topPostViews),
    String(row.topPostLikes),
    String(row.topPostComments),
    String(row.topPostReposts),
    row.warnings
  ];
}

function buildPostRows(group, week, postsMetrics, collectedAt) {
  return postsMetrics.map((p) => [
    collectedAt,
    week.dateFrom,
    week.dateTo,
    String(group.group_id),
    group.screen_name,
    group.group_name,
    String(p.post_id),
    p.date_iso,
    p.url,
    p.type,
    String(p.views),
    String(p.likes),
    String(p.comments),
    String(p.reposts),
    String(p.engagements),
    String(p.er_by_subscribers_pct),
    String(p.er_by_views_pct),
    p.text_preview
  ]);
}

async function getSheetsClient() {
  const auth = new google.auth.JWT({
    email: getRequiredEnv('GOOGLE_SERVICE_ACCOUNT_EMAIL'),
    key: normalizeGooglePrivateKey(getRequiredEnv('GOOGLE_PRIVATE_KEY')),
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
  await auth.authorize();
  return google.sheets({ version: 'v4', auth });
}

function columnLetter(n) {
  let s = '';
  while (n > 0) {
    const m = (n - 1) % 26;
    s = String.fromCharCode(65 + m) + s;
    n = Math.floor((n - m) / 26);
  }
  return s;
}

async function ensureSheetExists(sheets, spreadsheetId, sheetName) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const exists = meta.data.sheets?.some((s) => s.properties?.title === sheetName);
  if (!exists) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] }
    });
  }
}

async function readSheetValues(sheets, spreadsheetId, sheetName) {
  await ensureSheetExists(sheets, spreadsheetId, sheetName);
  const response = await sheets.spreadsheets.values.get({ spreadsheetId, range: `${sheetName}!A:ZZ` });
  return response.data.values || [];
}

async function clearAndWrite(sheets, spreadsheetId, sheetName, headers, rows) {
  await ensureSheetExists(sheets, spreadsheetId, sheetName);
  await sheets.spreadsheets.values.clear({ spreadsheetId, range: `${sheetName}!A:ZZ` });
  const lastCol = columnLetter(headers.length);
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${sheetName}!A1:${lastCol}${rows.length + 1}`,
    valueInputOption: 'RAW',
    requestBody: { values: [headers, ...rows] }
  });
}

function headersAreSame(existing, expected) {
  if (!existing?.length) return false;
  return expected.length === existing.length && expected.every((h, i) => existing[i] === h);
}

function rowKey(row, headers) {
  const groupIndex = headers.indexOf('ID группы');
  const weekIndex = headers.indexOf('Неделя с');
  if (groupIndex < 0 || weekIndex < 0) return '';
  return `${row[groupIndex]}|${row[weekIndex]}`;
}

function postRowKey(row, headers) {
  const groupIndex = headers.indexOf('ID группы');
  const weekIndex = headers.indexOf('Неделя с');
  const postIndex = headers.indexOf('ID поста');
  if (groupIndex < 0 || weekIndex < 0 || postIndex < 0) return '';
  return `${row[groupIndex]}|${row[weekIndex]}|${row[postIndex]}`;
}

function mergeRows(existingValues, expectedHeaders, newRows, keyFn) {
  const existingHeaders = existingValues[0] || [];
  const map = new Map();

  if (headersAreSame(existingHeaders, expectedHeaders)) {
    for (const row of existingValues.slice(1)) {
      const key = keyFn(row, expectedHeaders);
      if (key) map.set(key, row);
    }
  }

  for (const row of newRows) {
    const key = keyFn(row, expectedHeaders);
    if (key) map.set(key, row);
  }

  return [...map.values()].sort((a, b) => {
    const groupDiff = String(a[3] || '').localeCompare(String(b[3] || ''));
    if (groupDiff) return groupDiff;
    const dateDiff = String(a[1] || '').localeCompare(String(b[1] || ''));
    if (dateDiff) return dateDiff;
    return String(a[6] || '').localeCompare(String(b[6] || ''));
  });
}

function existingWeekKeys(values) {
  const headers = values[0] || [];
  const groupIndex = headers.indexOf('ID группы');
  const weekIndex = headers.indexOf('Неделя с');
  const result = new Set();
  if (groupIndex < 0 || weekIndex < 0) return result;
  for (const row of values.slice(1)) {
    if (row[groupIndex] && row[weekIndex]) result.add(`${row[groupIndex]}|${row[weekIndex]}`);
  }
  return result;
}

async function buildWeeksForGroup({ group, token, existingSummaryValues, lastWeek }) {
  const keys = existingWeekKeys(existingSummaryValues);
  const hasRowsForGroup = [...keys].some((k) => k.startsWith(`${group.group_id}|`));

  if (!hasRowsForGroup) {
    const start = BACKFILL_FROM ? dateFromYmd(BACKFILL_FROM) : await findEarliestAvailablePostDate(group, token);
    const weeks = listWeeksInclusive(start, lastWeek);
    console.log(`${group.group_name}: первый запуск/нет строк — ретро ${weeks.length} полных недель с ${weeks[0]?.dateFrom || 'n/a'}`);
    return weeks;
  }

  const recalcStart = addDays(lastWeek.start, -7 * Math.max(RECALC_PREVIOUS_WEEKS, 0));
  const weeks = listWeeksInclusive(recalcStart, lastWeek);

  // If a recent week is missing, it will be computed here. Older missing weeks can be backfilled by increasing VK_RECALC_PREVIOUS_WEEKS.
  console.log(`${group.group_name}: повторный запуск — пересчёт последних ${weeks.length} полных недель`);
  return weeks;
}

async function processGroup({ group, token, weeks, collectedAt }) {
  if (!weeks.length) return { summaryRows: [], postRows: [] };
  const fromUnix = weeks[0].fromUnix;
  const untilUnix = weeks[weeks.length - 1].untilUnix;
  const weekMap = new Map(weeks.map((w) => [w.key, { week: w, posts: [] }]));

  console.log(`${group.group_name}: загрузка постов за ${weeks[0].dateFrom}..${weeks[weeks.length - 1].dateTo}`);
  const posts = await fetchWallPostsSince(group, token, fromUnix);
  for (const post of posts) {
    if (post.date < fromUnix || post.date > untilUnix) continue;
    const key = weekKeyForPost(post);
    if (weekMap.has(key)) weekMap.get(key).posts.push(postToMetrics(post, group));
  }

  const rawSummaries = [];
  const allPostRows = [];
  for (const week of weeks) {
    console.log(`${group.group_name}: сбор недели ${week.dateFrom}..${week.dateTo}`);
    const statsData = await fetchCommunityStats(group, token, week);
    const postsMetrics = weekMap.get(week.key)?.posts || [];
    rawSummaries.push(buildRawSummary({ group, week, postsMetrics, statsData, collectedAt }));
    allPostRows.push(...buildPostRows(group, week, postsMetrics, collectedAt));
  }

  const finalized = finalizeWeeklyDeltas(rawSummaries, group);
  return { summaryRows: finalized.map(summaryToRow), postRows: allPostRows };
}

async function main() {
  const vkToken = getRequiredEnv('VK_TOKEN');
  const spreadsheetId = getRequiredEnv('GOOGLE_SHEETS_SPREADSHEET_ID');
  const summarySheetName = process.env.GOOGLE_SUMMARY_SHEET_NAME?.trim() || 'weekly_summary';
  const postsSheetName = process.env.GOOGLE_POSTS_SHEET_NAME?.trim() || 'posts_raw_vk';
  const groupIds = parseGroups(getRequiredEnv('VK_GROUPS'));
  const collectedAt = new Date().toISOString();
  const lastWeek = lastCompletedIsoWeek();

  console.log(`VK metrics sync v4.0 запущена для ${groupIds.length} групп. Последняя полная неделя: ${lastWeek.dateFrom}..${lastWeek.dateTo}`);
  console.log(`stats.get: ${ENABLE_STATS ? 'включён' : 'выключен'}, ретро-старт: ${BACKFILL_FROM || 'самый ранний доступный пост'}`);

  const sheets = await getSheetsClient();
  const existingSummaryValues = await readSheetValues(sheets, spreadsheetId, summarySheetName);
  const existingPostsValues = WRITE_POSTS_RAW ? await readSheetValues(sheets, spreadsheetId, postsSheetName) : [];

  const allSummaryRows = [];
  const allPostRows = [];

  for (const rawGroupId of groupIds) {
    console.log(`Получение группы: ${rawGroupId}`);
    const group = await fetchVkGroup(rawGroupId, vkToken);
    console.log(`Группа: ${group.group_name} (${group.group_id}), подписчиков сейчас: ${group.members_count}`);
    const weeks = await buildWeeksForGroup({ group, token: vkToken, existingSummaryValues, lastWeek });
    const result = await processGroup({ group, token: vkToken, weeks, collectedAt });
    allSummaryRows.push(...result.summaryRows);
    allPostRows.push(...result.postRows);
    console.log(`${group.group_name}: подготовлено недельных строк=${result.summaryRows.length}, строк постов=${result.postRows.length}`);
  }

  const mergedSummaryRows = mergeRows(existingSummaryValues, SUMMARY_HEADERS, allSummaryRows, rowKey);
  await clearAndWrite(sheets, spreadsheetId, summarySheetName, SUMMARY_HEADERS, mergedSummaryRows);

  if (WRITE_POSTS_RAW) {
    const mergedPostRows = mergeRows(existingPostsValues, POSTS_HEADERS, allPostRows, postRowKey);
    await clearAndWrite(sheets, spreadsheetId, postsSheetName, POSTS_HEADERS, mergedPostRows);
  }

  console.log('Синхронизация метрик ВКонтакте v4.0 успешно завершена.');
  console.table(allSummaryRows.map((row) => ({
    группа: row[5],
    неделя: `${row[1]}..${row[2]}`,
    подписчики: row[7],
    посты: row[8],
    охват: row[24],
    показы: row[23],
    er_reach_pct: row[20],
    предупреждения: row[46]
  })));
}

main().catch((error) => {
  console.error('Сбой синхронизации показателей VK v4.0.');
  console.error(error);
  process.exit(1);
});
