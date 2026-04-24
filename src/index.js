import 'dotenv/config';
import { google } from 'googleapis';

const VK_API_VERSION = process.env.VK_API_VERSION || '5.199';
const DAYS_BACK = Number(process.env.VK_DAYS_BACK || 7);
const WALL_MAX_POSTS = Number(process.env.VK_WALL_MAX_POSTS || 300);
const ENABLE_STATS = String(process.env.VK_ENABLE_STATS || 'false').toLowerCase() === 'true';
const WRITE_POSTS_RAW = String(process.env.VK_WRITE_POSTS_RAW || 'true').toLowerCase() !== 'false';

const SUMMARY_HEADERS = [
  'collected_at', 'period_from', 'period_to',
  'group_id', 'screen_name', 'group_name', 'members_count_current',
  'posts_count', 'posts_with_views_count', 'total_views', 'avg_views_per_post', 'median_views_per_post', 'max_views',
  'total_likes', 'total_comments', 'total_reposts', 'total_engagements', 'avg_engagements_per_post',
  'er_by_subscribers_pct', 'er_by_views_pct', 'likes_per_1000_subscribers', 'comments_per_1000_subscribers', 'reposts_per_1000_subscribers',
  'posts_per_day',
  'top_post_id', 'top_post_url', 'top_post_views', 'top_post_likes', 'top_post_comments', 'top_post_reposts',
  'stats_visitors', 'stats_views', 'stats_reach', 'stats_reach_subscribers', 'stats_mobile_reach', 'stats_subscribed', 'stats_unsubscribed',
  'warnings'
];

const POSTS_HEADERS = [
  'collected_at', 'period_from', 'period_to',
  'group_id', 'screen_name', 'group_name',
  'post_id', 'post_date', 'post_url', 'post_type',
  'views', 'likes', 'comments', 'reposts', 'engagements',
  'er_by_subscribers_pct', 'er_by_views_pct',
  'text_preview'
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

function toUnix(date) {
  return Math.floor(date.getTime() / 1000);
}

function formatDateOnly(date) {
  return date.toISOString().slice(0, 10);
}

function round(value, digits = 2) {
  if (!Number.isFinite(value)) return 0;
  const m = 10 ** digits;
  return Math.round(value * m) / m;
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

  const response = await fetch(url, {
    method: 'GET',
    headers: { Accept: 'application/json', 'User-Agent': 'vk-sheets-metrics-sync/2.0' }
  });

  if (!response.ok) throw new Error(`VK HTTP ${response.status} for ${method}`);

  const data = await response.json();
  if (data.error) {
    const code = data.error.error_code ?? 'unknown';
    const msg = data.error.error_msg ?? 'Unknown VK API error';
    throw new Error(`VK API error in ${method}: [${code}] ${msg}`);
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
    lastResponse = await vkApi('groups.getById', { group_id: groupId, fields: 'members_count,screen_name' }, token);
    group = extractGroup(lastResponse);
  } catch (error) {
    if (!String(error.message).includes('VK API error')) throw error;
    lastResponse = null;
  }

  if (!group) {
    lastResponse = await vkApi('groups.getById', { group_ids: groupId, fields: 'members_count,screen_name' }, token);
    group = extractGroup(lastResponse);
  }

  if (!group) {
    throw new Error(`VK did not return group data for "${rawGroupId}". Raw response: ${JSON.stringify(lastResponse)}`);
  }

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
  if (types.includes('video')) return 'video_or_clip';
  if (types.includes('photo')) return 'photo';
  if (types.includes('link')) return 'link';
  if (types.includes('doc')) return 'doc';
  if (types.length) return types.join('+');
  return post.text ? 'text' : 'unknown';
}

async function fetchWallPostsForPeriod(group, token, fromUnix, toUnix) {
  const ownerId = -Math.abs(group.group_id);
  const posts = [];
  let offset = 0;
  const count = 100;

  while (posts.length < WALL_MAX_POSTS) {
    const response = await vkApi('wall.get', { owner_id: ownerId, count, offset, extended: 0 }, token);
    const items = response?.items || [];
    if (!items.length) break;

    for (const post of items) {
      if (!post.date) continue;
      if (post.date < fromUnix) return posts;
      if (post.date <= toUnix && post.date >= fromUnix && !post.is_pinned && post.post_type !== 'copy') posts.push(post);
      if (posts.length >= WALL_MAX_POSTS) break;
    }

    offset += items.length;
    if (items.length < count) break;
  }

  return posts;
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

async function fetchCommunityStatsIfEnabled(group, token, dateFrom, dateTo) {
  if (!ENABLE_STATS) return { stats: {}, warning: 'VK_ENABLE_STATS=false' };

  try {
    const response = await vkApi('stats.get', {
      group_id: group.group_id,
      date_from: dateFrom,
      date_to: dateTo
    }, token);
    return { stats: accumulateStats(response), warning: '' };
  } catch (error) {
    return { stats: {}, warning: `stats.get failed: ${error.message}` };
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
    date: new Date(post.date * 1000).toISOString(),
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

function buildSummary(group, postsMetrics, statsData, dates, collectedAt) {
  const views = postsMetrics.map((p) => p.views);
  const engagements = postsMetrics.map((p) => p.engagements);
  const totalViews = sum(views);
  const totalLikes = sum(postsMetrics.map((p) => p.likes));
  const totalComments = sum(postsMetrics.map((p) => p.comments));
  const totalReposts = sum(postsMetrics.map((p) => p.reposts));
  const totalEngagements = sum(engagements);
  const topPost = [...postsMetrics].sort((a, b) => b.views - a.views || b.engagements - a.engagements)[0] || {};
  const stats = statsData.stats || {};
  const warnings = [statsData.warning].filter(Boolean).join(' | ');

  return [
    collectedAt, dates.dateFrom, dates.dateTo,
    String(group.group_id), group.screen_name, group.group_name, String(group.members_count),
    String(postsMetrics.length), String(views.filter((v) => v > 0).length), String(totalViews),
    String(round(postsMetrics.length ? totalViews / postsMetrics.length : 0)), String(round(median(views))), String(Math.max(0, ...views)),
    String(totalLikes), String(totalComments), String(totalReposts), String(totalEngagements),
    String(round(postsMetrics.length ? totalEngagements / postsMetrics.length : 0)),
    String(group.members_count ? round((totalEngagements / group.members_count) * 100, 4) : 0),
    String(totalViews ? round((totalEngagements / totalViews) * 100, 4) : 0),
    String(group.members_count ? round((totalLikes / group.members_count) * 1000, 2) : 0),
    String(group.members_count ? round((totalComments / group.members_count) * 1000, 2) : 0),
    String(group.members_count ? round((totalReposts / group.members_count) * 1000, 2) : 0),
    String(round(postsMetrics.length / DAYS_BACK, 2)),
    topPost.post_id ? String(topPost.post_id) : '', topPost.url || '', String(topPost.views || 0), String(topPost.likes || 0), String(topPost.comments || 0), String(topPost.reposts || 0),
    String(stats.visitors || 0), String(stats.views || 0), String(stats.reach || 0), String(stats.reach_subscribers || 0), String(stats.mobile_reach || 0), String(stats.subscribed || 0), String(stats.unsubscribed || 0),
    warnings
  ];
}

function buildPostRows(group, postsMetrics, dates, collectedAt) {
  return postsMetrics.map((p) => [
    collectedAt, dates.dateFrom, dates.dateTo,
    String(group.group_id), group.screen_name, group.group_name,
    String(p.post_id), p.date, p.url, p.type,
    String(p.views), String(p.likes), String(p.comments), String(p.reposts), String(p.engagements),
    String(p.er_by_subscribers_pct), String(p.er_by_views_pct),
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

async function ensureHeaderRow(sheets, spreadsheetId, sheetName, headers) {
  await ensureSheetExists(sheets, spreadsheetId, sheetName);
  const lastCol = columnLetter(headers.length);
  const range = `${sheetName}!A1:${lastCol}1`;
  const existing = await sheets.spreadsheets.values.get({ spreadsheetId, range });
  const current = existing.data.values?.[0] || [];
  const same = current.length === headers.length && current.every((cell, i) => cell === headers[i]);
  if (!same) {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption: 'RAW',
      requestBody: { values: [headers] }
    });
  }
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

async function appendRows(sheets, spreadsheetId, sheetName, rows) {
  if (!rows.length) return;
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${sheetName}!A:ZZ`,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: rows }
  });
}

async function main() {
  const vkToken = getRequiredEnv('VK_TOKEN');
  const spreadsheetId = getRequiredEnv('GOOGLE_SHEETS_SPREADSHEET_ID');
  const summarySheetName = process.env.GOOGLE_SUMMARY_SHEET_NAME?.trim() || 'weekly_summary';
  const postsSheetName = process.env.GOOGLE_POSTS_SHEET_NAME?.trim() || 'posts_raw';
  const groupIds = parseGroups(getRequiredEnv('VK_GROUPS'));

  const now = new Date();
  const from = new Date(now.getTime() - DAYS_BACK * 24 * 60 * 60 * 1000);
  const dates = { dateFrom: formatDateOnly(from), dateTo: formatDateOnly(now) };
  const fromUnix = toUnix(from);
  const untilUnix = toUnix(now);
  const collectedAt = now.toISOString();

  console.log(`Starting VK metrics sync for ${groupIds.length} group(s), period ${dates.dateFrom}..${dates.dateTo}`);

  const summaryRows = [];
  const allPostRows = [];

  for (const rawGroupId of groupIds) {
    console.log(`Fetching group: ${rawGroupId}`);
    const group = await fetchVkGroup(rawGroupId, vkToken);

    console.log(`Fetching wall posts: ${group.group_name} (${group.group_id})`);
    const posts = await fetchWallPostsForPeriod(group, vkToken, fromUnix, untilUnix);
    const postsMetrics = posts.map((post) => postToMetrics(post, group));

    console.log(`Fetching community stats: ${ENABLE_STATS ? 'enabled' : 'disabled'}`);
    const statsData = await fetchCommunityStatsIfEnabled(group, vkToken, dates.dateFrom, dates.dateTo);

    summaryRows.push(buildSummary(group, postsMetrics, statsData, dates, collectedAt));
    allPostRows.push(...buildPostRows(group, postsMetrics, dates, collectedAt));

    console.log(`${group.group_name}: members=${group.members_count}, posts=${postsMetrics.length}, views=${sum(postsMetrics.map((p) => p.views))}`);
  }

  const sheets = await getSheetsClient();
  await ensureHeaderRow(sheets, spreadsheetId, summarySheetName, SUMMARY_HEADERS);
  await appendRows(sheets, spreadsheetId, summarySheetName, summaryRows);

  if (WRITE_POSTS_RAW) {
    await ensureHeaderRow(sheets, spreadsheetId, postsSheetName, POSTS_HEADERS);
    await appendRows(sheets, spreadsheetId, postsSheetName, allPostRows);
  }

  console.log('VK metrics sync completed successfully.');
  console.table(summaryRows.map((row) => ({ group: row[5], members: row[6], posts: row[7], views: row[9], er_pct: row[18], warnings: row[37] })));
}

main().catch((error) => {
  console.error('VK metrics sync failed.');
  console.error(error);
  process.exit(1);
});
