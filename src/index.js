import axios from "axios";
import dotenv from "dotenv";
import { google } from "googleapis";

dotenv.config();

const env = process.env;

const CONFIG = {
  vkToken: requiredEnv("VK_TOKEN"),
  vkGroups: requiredEnv("VK_GROUPS").split(",").map((x) => x.trim()).filter(Boolean),
  vkVersion: env.VK_API_VERSION || "5.199",
  enableStats: String(env.VK_ENABLE_STATS || "false").toLowerCase() === "true",
  wallMaxPosts: toInt(env.VK_WALL_MAX_POSTS, 300),
  requestDelayMs: toInt(env.VK_REQUEST_DELAY_MS, 900),
  statsRequestDelayMs: toInt(env.VK_STATS_REQUEST_DELAY_MS, 1500),
  retryMax: toInt(env.VK_RETRY_MAX, 8),
  recalcPreviousWeeks: toInt(env.VK_RECALC_PREVIOUS_WEEKS, 4),
  viralThreshold: toFloat(env.VK_VIRAL_THRESHOLD, 2),
  engagementWeights: parseWeights(env.VK_ENGAGEMENT_WEIGHTS || "1,2,3"),
  backfillFrom: env.VK_BACKFILL_FROM || "",
  spreadsheetId: requiredEnv("GOOGLE_SHEETS_SPREADSHEET_ID"),
  serviceAccountEmail: requiredEnv("GOOGLE_SERVICE_ACCOUNT_EMAIL"),
  privateKey: requiredEnv("GOOGLE_PRIVATE_KEY").replace(/\\n/g, "\n"),
  summarySheet: env.GOOGLE_SUMMARY_SHEET_NAME || "weekly_summary",
  postsSheet: env.GOOGLE_POSTS_SHEET_NAME || "posts_raw_vk",
};

const SUMMARY_HEADERS = [
  "Ключ", "Дата сбора", "Неделя с", "Неделя по", "ID группы", "Короткое имя группы", "Название группы",
  "Подписчики на конец недели", "Публикации", "Просмотры постов", "Лайки", "Комментарии", "Репосты", "Вовлеченность",
  "ER по подписчикам, %", "ER по просмотрам, %", "ER по охвату, %",
  "Посетители сообщества", "Просмотры/показы сообщества", "Охват всего", "Охват подписчиков", "Охват мобильный",
  "Подписались", "Отписались", "Чистый прирост подписчиков", "Прирост подписчиков за неделю, %",
  "Дельта подписчиков WoW, абс.", "Дельта подписчиков WoW, %", "Дельта охвата WoW, абс.", "Дельта охвата WoW, %",
  "Дельта показов WoW, абс.", "Дельта показов WoW, %", "Дельта ER по охвату WoW, п.п.", "Дельта ER по охвату WoW, %",
  "Средние просмотры поста", "Медианные просмотры поста", "Лучший пост недели, ID", "Лучший пост недели, ссылка", "Источник охвата", "Предупреждения",
  "Вирусность, %", "Обсуждаемость, %", "Лайкабельность, %", "Индекс вовлеченности", "Вирусные посты", "Вирусные посты, %",
  "Стд. отклонение просмотров", "Частота публикаций в день", "Лучший пост по вовлеченности, ID", "Лучший пост по ER, ID",
  "Лучший день публикации", "Лучший час публикации", "Прогноз подписчиков через 4 недели", "Прогноз просмотров следующей недели", "AI-рекомендация"
];

const POST_HEADERS = [
  "Ключ", "Дата сбора", "Неделя с", "Неделя по", "ID группы", "Короткое имя группы", "Название группы",
  "ID поста", "Дата поста", "Ссылка", "Тип поста", "Текст", "Просмотры", "Лайки", "Комментарии", "Репосты", "Вовлеченность",
  "ER по просмотрам, %", "Вирусность, %", "Обсуждаемость, %", "Лайкабельность, %", "Индекс вовлеченности", "Вирусный пост"
];

function requiredEnv(name) {
  const value = env[name];
  if (!value) throw new Error(`Не задана переменная окружения ${name}`);
  return value;
}
function toInt(value, fallback) {
  const n = Number.parseInt(value, 10);
  return Number.isFinite(n) ? n : fallback;
}
function toFloat(value, fallback) {
  const n = Number.parseFloat(value);
  return Number.isFinite(n) ? n : fallback;
}
function parseWeights(raw) {
  const [like, comment, repost] = raw.split(",").map((x) => Number.parseFloat(x.trim()));
  return { like: like || 1, comment: comment || 2, repost: repost || 3 };
}
function sleep(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
function isoDate(date) { return date.toISOString().slice(0, 10); }
function toUnix(date) { return Math.floor(date.getTime() / 1000); }
function fromUnix(ts) { return new Date(ts * 1000); }
function pct(numerator, denominator) { return denominator ? (numerator / denominator) * 100 : 0; }
function round(value, digits = 4) { return Number.isFinite(value) ? Number(value.toFixed(digits)) : 0; }
function sum(values) { return values.reduce((a, b) => a + (Number(b) || 0), 0); }
function avg(values) { return values.length ? sum(values) / values.length : 0; }
function median(values) {
  if (!values.length) return 0;
  const arr = [...values].sort((a, b) => a - b);
  const mid = Math.floor(arr.length / 2);
  return arr.length % 2 ? arr[mid] : (arr[mid - 1] + arr[mid]) / 2;
}
function stdDev(values) {
  if (values.length < 2) return 0;
  const mean = avg(values);
  return Math.sqrt(avg(values.map((v) => Math.pow(v - mean, 2))));
}
function cleanGroupId(raw) {
  return String(raw).trim().replace(/^https?:\/\/vk\.com\//i, "").replace(/^@/, "").replace(/\/$/, "");
}
function mondayStart(date) {
  const d = new Date(date);
  d.setHours(0, 0, 0, 0);
  const day = d.getDay();
  const diff = day === 0 ? -6 : 1 - day;
  d.setDate(d.getDate() + diff);
  return d;
}
function addDays(date, days) { const d = new Date(date); d.setDate(d.getDate() + days); return d; }
function addWeeks(date, weeks) { return addDays(date, weeks * 7); }
function lastFullWeek() {
  const thisMonday = mondayStart(new Date());
  const start = addWeeks(thisMonday, -1);
  const end = addDays(thisMonday, -1);
  end.setHours(23, 59, 59, 999);
  return { start, end };
}
function weekEnd(start) { const e = addDays(start, 6); e.setHours(23, 59, 59, 999); return e; }
function weekKey(groupId, weekStart) { return `${groupId}_${isoDate(weekStart)}`; }
function postLink(groupId, postId) { return `https://vk.com/wall-${groupId}_${postId}`; }

async function vkApi(method, params = {}, delayMs = CONFIG.requestDelayMs) {
  let lastError;
  for (let attempt = 1; attempt <= CONFIG.retryMax; attempt++) {
    if (delayMs) await sleep(delayMs);
    try {
      const { data } = await axios.get(`https://api.vk.com/method/${method}`, {
        params: { ...params, access_token: CONFIG.vkToken, v: CONFIG.vkVersion },
        timeout: 30000,
      });
      if (data?.error) {
        const code = data.error.error_code;
        const msg = data.error.error_msg;
        if (code === 6 || code === 9 || code === 10) {
          lastError = new Error(`ошибка VK API в ${method}: [${code}] ${msg}`);
          const wait = delayMs + attempt * 1200;
          console.warn(`VK rate-limit/temporary error в ${method}, попытка ${attempt}/${CONFIG.retryMax}. Ждём ${wait}ms`);
          await sleep(wait);
          continue;
        }
        console.error(`Ошибка VK API в ${method}: [${code}] ${msg}`);
        console.error(`Параметры метода VK: ${JSON.stringify(params)}`);
        console.error(`Ошибка VK raw: ${JSON.stringify(data.error)}`);
        throw new Error(`ошибка VK API в ${method}: [${code}] ${msg}`);
      }
      return data.response;
    } catch (error) {
      lastError = error;
      if (attempt === CONFIG.retryMax) break;
      await sleep(delayMs + attempt * 1000);
    }
  }
  throw lastError;
}

async function fetchVkGroup(rawId) {
  const id = cleanGroupId(rawId);
  const attempts = [
    { group_ids: id, fields: "members_count,screen_name" },
    { group_id: id, fields: "members_count,screen_name" },
  ];
  for (const params of attempts) {
    try {
      const response = await vkApi("groups.getById", params);
      const group = Array.isArray(response) ? response[0] : response?.groups?.[0] || response?.items?.[0];
      if (group?.id) return group;
      console.warn(`groups.getById вернул неожиданный ответ для ${id}: ${JSON.stringify(response)}`);
    } catch (e) {
      console.warn(`Попытка groups.getById не удалась: ${e.message}`);
    }
  }
  throw new Error(`Не удалось получить группу VK: ${id}`);
}

async function fetchWallPosts(groupId, maxPosts) {
  const posts = [];
  const ownerId = -Math.abs(groupId);
  const pageSize = 100;
  for (let offset = 0; offset < maxPosts; offset += pageSize) {
    const count = Math.min(pageSize, maxPosts - offset);
    const response = await vkApi("wall.get", { owner_id: ownerId, count, offset });
    const items = response?.items || [];
    posts.push(...items.filter((p) => p && !p.is_pinned));
    if (items.length < count) break;
  }
  return posts;
}

async function fetchStats(groupId, start, end) {
  if (!CONFIG.enableStats) return { ok: false, source: "stats отключен", warning: "VK_ENABLE_STATS=false" };
  try {
    const response = await vkApi("stats.get", {
      group_id: groupId,
      timestamp_from: toUnix(start),
      timestamp_to: toUnix(end),
      interval: "day",
    }, CONFIG.statsRequestDelayMs);
    const days = Array.isArray(response) ? response : [];
    if (!days.length) return { ok: false, source: "fallback: просмотры постов", warning: "stats.get пустой ответ" };
    const result = {
      ok: true, source: "stats.get", warning: "", visitors: 0, views: 0, reachTotal: 0, reachSubscribers: 0, reachMobile: 0, subscribed: 0, unsubscribed: 0,
    };
    for (const day of days) {
      result.visitors += Number(day.visitors || 0);
      result.views += Number(day.views || 0);
      const reach = day.reach || {};
      result.reachTotal += Number(reach.reach || reach.total || 0);
      result.reachSubscribers += Number(reach.subscribers || 0);
      result.reachMobile += Number(reach.mobile_reach || reach.mobile || 0);
      result.subscribed += Number(day.subscribed || 0);
      result.unsubscribed += Number(day.unsubscribed || 0);
    }
    return result;
  } catch (e) {
    console.warn(`stats.get недоступен для группы ${groupId}, неделя ${isoDate(start)}: ${e.message}`);
    return { ok: false, source: "fallback: просмотры постов", warning: `stats.get недоступен: ${e.message}` };
  }
}

function inferPostType(post) {
  if (post.copy_history?.length) return "repost";
  const attachments = post.attachments || [];
  if (attachments.some((a) => a.type === "clip")) return "clip";
  if (attachments.some((a) => a.type === "video" && a.video?.platform === "VK Clips")) return "clip";
  if (attachments.some((a) => a.type === "video")) return "video";
  if (attachments.some((a) => ["photo", "album"].includes(a.type))) return "image";
  if ((post.text || "").trim()) return "text";
  return "other";
}

function groupPostsByWeeks(posts, minWeekStart, maxWeekStart) {
  const map = new Map();
  for (let d = new Date(minWeekStart); d <= maxWeekStart; d = addWeeks(d, 1)) {
    map.set(isoDate(d), []);
  }
  for (const post of posts) {
    const date = fromUnix(post.date);
    const ws = mondayStart(date);
    const key = isoDate(ws);
    if (map.has(key)) map.get(key).push(post);
  }
  return map;
}

function determineMinWeek(posts) {
  if (CONFIG.backfillFrom) return mondayStart(new Date(`${CONFIG.backfillFrom}T00:00:00`));
  if (!posts.length) return addWeeks(lastFullWeek().start, -CONFIG.recalcPreviousWeeks + 1);
  const earliest = posts.reduce((min, p) => Math.min(min, p.date), posts[0].date);
  return mondayStart(fromUnix(earliest));
}

function analyzePosts(posts, group) {
  const enriched = posts.map((p) => {
    const views = Number(p.views?.count || 0);
    const likes = Number(p.likes?.count || 0);
    const comments = Number(p.comments?.count || 0);
    const reposts = Number(p.reposts?.count || 0);
    const engagement = likes + comments + reposts;
    const weighted = likes * CONFIG.engagementWeights.like + comments * CONFIG.engagementWeights.comment + reposts * CONFIG.engagementWeights.repost;
    return { post: p, views, likes, comments, reposts, engagement, weighted, type: inferPostType(p), erView: pct(engagement, views) };
  });
  const viewsArr = enriched.map((x) => x.views);
  const medViews = median(viewsArr);
  const viralLimit = medViews * CONFIG.viralThreshold;
  const viralPosts = enriched.filter((x) => medViews > 0 && x.views > viralLimit);
  const bestByViews = [...enriched].sort((a, b) => b.views - a.views)[0];
  const bestByEngagement = [...enriched].sort((a, b) => b.engagement - a.engagement)[0];
  const bestByEr = [...enriched].sort((a, b) => b.erView - a.erView)[0];
  const hourScores = new Map();
  const dayScores = new Map();
  for (const item of enriched) {
    const d = fromUnix(item.post.date);
    const hour = d.getHours();
    const day = ["Вс", "Пн", "Вт", "Ср", "Чт", "Пт", "Сб"][d.getDay()];
    hourScores.set(hour, (hourScores.get(hour) || 0) + item.views);
    dayScores.set(day, (dayScores.get(day) || 0) + item.views);
  }
  const bestHour = [...hourScores.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] ?? "";
  const bestDay = [...dayScores.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] ?? "";
  return {
    enriched,
    count: enriched.length,
    views: sum(enriched.map((x) => x.views)), likes: sum(enriched.map((x) => x.likes)), comments: sum(enriched.map((x) => x.comments)), reposts: sum(enriched.map((x) => x.reposts)),
    engagement: sum(enriched.map((x) => x.engagement)), weighted: sum(enriched.map((x) => x.weighted)),
    avgViews: avg(viewsArr), medianViews: medViews, stdViews: stdDev(viewsArr),
    bestByViews, bestByEngagement, bestByEr, viralPosts, bestHour, bestDay,
  };
}

function makePostRows(week, group, analyzed) {
  return analyzed.enriched.map((x) => {
    const p = x.post;
    const postDate = fromUnix(p.date);
    const viral = analyzed.medianViews > 0 && x.views > analyzed.medianViews * CONFIG.viralThreshold;
    return [
      `${group.id}_${p.id}`, new Date().toISOString(), isoDate(week.start), isoDate(week.end), group.id, group.screen_name || "", group.name,
      p.id, postDate.toISOString(), postLink(group.id, p.id), x.type, (p.text || "").slice(0, 500),
      x.views, x.likes, x.comments, x.reposts, x.engagement, round(x.erView, 4), round(pct(x.reposts, x.views), 4), round(pct(x.comments, x.views), 4), round(pct(x.likes, x.views), 4), round(pct(x.weighted, x.views), 4), viral ? "да" : "нет"
    ];
  });
}

function recommendation(current, prev) {
  const tips = [];
  if (current.publications === 0) tips.push("Нет публикаций за неделю — нужен регулярный контент-план.");
  if (current.erReach < (prev?.erReach || 0)) tips.push("ER по охвату снизился — проанализируйте лучшие посты прошлой недели.");
  if (current.viralPosts > 0) tips.push("Есть вирусные посты — повторите формат/тему лучших публикаций.");
  if (current.bestHour !== "") tips.push(`Лучшее время по просмотрам: ${current.bestDay || "день не определён"}, ${current.bestHour}:00.`);
  if (!tips.length) tips.push("Динамика стабильная — масштабируйте частоту публикаций и тестируйте новые форматы.");
  return tips.join(" ");
}

function forecastNext(values, fallback = 0) {
  if (!values.length) return fallback;
  if (values.length === 1) return values[0];
  const diffs = [];
  for (let i = 1; i < values.length; i++) diffs.push(values[i] - values[i - 1]);
  return Math.max(0, values[values.length - 1] + avg(diffs.slice(-4)));
}

async function getSheets() {
  const auth = new google.auth.JWT({
    email: CONFIG.serviceAccountEmail,
    key: CONFIG.privateKey,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  return google.sheets({ version: "v4", auth });
}

async function readSheetValues(sheets, sheetName) {
  try {
    const res = await sheets.spreadsheets.values.get({ spreadsheetId: CONFIG.spreadsheetId, range: `${sheetName}!A:ZZ` });
    return res.data.values || [];
  } catch (e) {
    console.warn(`Не удалось прочитать лист ${sheetName}: ${e.message}`);
    return [];
  }
}

async function ensureHeader(sheets, sheetName, headers) {
  const values = await readSheetValues(sheets, sheetName);
  const first = values[0] || [];
  if (headers.every((h, i) => first[i] === h)) return;
  await sheets.spreadsheets.values.update({
    spreadsheetId: CONFIG.spreadsheetId,
    range: `${sheetName}!A1`,
    valueInputOption: "RAW",
    requestBody: { values: [headers] },
  });
}

async function upsertRowsByKey(sheets, sheetName, headers, rows) {
  await ensureHeader(sheets, sheetName, headers);
  const existing = await readSheetValues(sheets, sheetName);
  const keyToRow = new Map();
  existing.slice(1).forEach((row, idx) => { if (row[0]) keyToRow.set(row[0], idx + 2); });
  const toAppend = [];
  for (const row of rows) {
    const rowNumber = keyToRow.get(row[0]);
    if (rowNumber) {
      await sheets.spreadsheets.values.update({ spreadsheetId: CONFIG.spreadsheetId, range: `${sheetName}!A${rowNumber}`, valueInputOption: "RAW", requestBody: { values: [row] } });
    } else {
      toAppend.push(row);
    }
  }
  if (toAppend.length) {
    await sheets.spreadsheets.values.append({ spreadsheetId: CONFIG.spreadsheetId, range: `${sheetName}!A1`, valueInputOption: "RAW", insertDataOption: "INSERT_ROWS", requestBody: { values: toAppend } });
  }
}

async function main() {
  const lastWeek = lastFullWeek();
  console.log(`Синхронизация метрик ВКонтакте версии 7.0.1 запущена для ${CONFIG.vkGroups.length} групп. Последняя полная неделя: ${isoDate(lastWeek.start)}..${isoDate(lastWeek.end)}`);
  const sheets = await getSheets();
  const allSummaryRows = [];
  const allPostRows = [];

  for (const rawGroupId of CONFIG.vkGroups) {
    console.log(`Получение группы: ${rawGroupId}`);
    const group = await fetchVkGroup(rawGroupId);
    console.log(`Группа: ${group.name} (${group.id}), сейчас в группе ${group.members_count} участников`);

    const posts = await fetchWallPosts(group.id, CONFIG.wallMaxPosts);
    const minWeek = determineMinWeek(posts);
    const weeksMap = groupPostsByWeeks(posts, minWeek, lastWeek.start);
    const weekStarts = [...weeksMap.keys()].sort().map((d) => new Date(`${d}T00:00:00`));
    const weeksForProcessing = weekStarts.filter((ws) => ws <= lastWeek.start);
    console.log(`${group.name}: найдено постов=${posts.length}, недель для обработки=${weeksForProcessing.length}`);

    const weeklyObjects = [];
    for (const ws of weeksForProcessing) {
      const start = ws;
      const end = weekEnd(start);
      const weekPosts = weeksMap.get(isoDate(start)) || [];
      const analyzed = analyzePosts(weekPosts, group);
      const stats = await fetchStats(group.id, start, end);
      const fallbackReach = analyzed.views;
      const reachTotal = stats.ok ? stats.reachTotal : fallbackReach;
      const impressions = stats.ok ? stats.views : analyzed.views;
      const subscribersDelta = stats.ok ? (stats.subscribed - stats.unsubscribed) : 0;
      weeklyObjects.push({
        start, end, group, analyzed, stats,
        reachTotal, impressions, subscribersDelta,
        subscribersEnd: 0,
        publications: analyzed.count,
        erSubs: 0, erView: pct(analyzed.engagement, analyzed.views), erReach: pct(analyzed.engagement, reachTotal),
        viral: pct(analyzed.reposts, analyzed.views), discuss: pct(analyzed.comments, analyzed.views), likeable: pct(analyzed.likes, analyzed.views), weightedIndex: pct(analyzed.weighted, analyzed.views),
        viralPosts: analyzed.viralPosts.length,
        viralPostsPct: pct(analyzed.viralPosts.length, analyzed.count),
        bestHour: analyzed.bestHour,
        bestDay: analyzed.bestDay,
      });
      allPostRows.push(...makePostRows({ start, end }, group, analyzed));
    }

    // восстановление подписчиков: текущие минус будущие известные приросты
    let subscribers = Number(group.members_count || 0);
    for (let i = weeklyObjects.length - 1; i >= 0; i--) {
      weeklyObjects[i].subscribersEnd = subscribers;
      subscribers -= weeklyObjects[i].subscribersDelta || 0;
    }
    for (const w of weeklyObjects) w.erSubs = pct(w.analyzed.engagement, w.subscribersEnd);

    for (let i = 0; i < weeklyObjects.length; i++) {
      const w = weeklyObjects[i];
      const prev = weeklyObjects[i - 1];
      const viewsHist = weeklyObjects.slice(Math.max(0, i - 8), i + 1).map((x) => x.analyzed.views);
      const subsHist = weeklyObjects.slice(Math.max(0, i - 8), i + 1).map((x) => x.subscribersEnd);
      const forecastViews = forecastNext(viewsHist, w.analyzed.views);
      const forecastSubs4 = Math.round(w.subscribersEnd + (forecastNext(subsHist, w.subscribersEnd) - w.subscribersEnd) * 4);
      const warnings = [w.stats.warning].filter(Boolean).join("; ");
      const key = weekKey(group.id, w.start);
      allSummaryRows.push([
        key, new Date().toISOString(), isoDate(w.start), isoDate(w.end), group.id, group.screen_name || "", group.name,
        Math.round(w.subscribersEnd), w.publications, w.analyzed.views, w.analyzed.likes, w.analyzed.comments, w.analyzed.reposts, w.analyzed.engagement,
        round(w.erSubs), round(w.erView), round(w.erReach),
        w.stats.ok ? w.stats.visitors : 0, w.impressions, w.reachTotal, w.stats.ok ? w.stats.reachSubscribers : 0, w.stats.ok ? w.stats.reachMobile : 0,
        w.stats.ok ? w.stats.subscribed : 0, w.stats.ok ? w.stats.unsubscribed : 0, w.subscribersDelta, round(pct(w.subscribersDelta, Math.max(1, w.subscribersEnd - w.subscribersDelta))),
        prev ? Math.round(w.subscribersEnd - prev.subscribersEnd) : 0, prev ? round(pct(w.subscribersEnd - prev.subscribersEnd, prev.subscribersEnd)) : 0,
        prev ? Math.round(w.reachTotal - prev.reachTotal) : 0, prev ? round(pct(w.reachTotal - prev.reachTotal, prev.reachTotal)) : 0,
        prev ? Math.round(w.impressions - prev.impressions) : 0, prev ? round(pct(w.impressions - prev.impressions, prev.impressions)) : 0,
        prev ? round(w.erReach - prev.erReach) : 0, prev ? round(pct(w.erReach - prev.erReach, prev.erReach)) : 0,
        round(w.analyzed.avgViews), round(w.analyzed.medianViews), w.analyzed.bestByViews?.post?.id || "", w.analyzed.bestByViews ? postLink(group.id, w.analyzed.bestByViews.post.id) : "", w.stats.source, warnings,
        round(w.viral), round(w.discuss), round(w.likeable), round(w.weightedIndex), w.viralPosts, round(w.viralPostsPct), round(w.analyzed.stdViews), round(w.publications / 7, 4),
        w.analyzed.bestByEngagement?.post?.id || "", w.analyzed.bestByEr?.post?.id || "", w.bestDay, w.bestHour === "" ? "" : `${w.bestHour}:00`,
        forecastSubs4, round(forecastViews), recommendation(w, prev)
      ]);
    }
    const lastObj = weeklyObjects[weeklyObjects.length - 1];
    if (lastObj) console.log(`${group.name}: готово. Последняя неделя ${isoDate(lastObj.start)}: публикаций=${lastObj.publications}, просмотров=${lastObj.analyzed.views}, ER reach=${round(lastObj.erReach)}`);
  }

  // для скорости оставляем только ретро + последние пересчёты; upsert не создаст дублей
  await upsertRowsByKey(sheets, CONFIG.summarySheet, SUMMARY_HEADERS, allSummaryRows);
  await upsertRowsByKey(sheets, CONFIG.postsSheet, POST_HEADERS, allPostRows);
  console.log("Синхронизация метрик ВКонтакте успешно завершена.");
}

main().catch((error) => {
  console.error("Сбой синхронизации показателей VK.");
  console.error(error);
  process.exit(1);
});
