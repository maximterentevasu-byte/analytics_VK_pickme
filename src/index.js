import axios from "axios";
import { google } from "googleapis";
import dotenv from "dotenv";

dotenv.config();

const env = process.env;
const VK_API_VERSION = env.VK_API_VERSION || "5.199";
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function intEnv(name, def) { const v = parseInt(env[name] || "", 10); return Number.isFinite(v) ? v : def; }
function numEnv(name, def) { const v = parseFloat(env[name] || ""); return Number.isFinite(v) ? v : def; }
function boolEnv(name, def) { if (env[name] === undefined || env[name] === "") return def; return ["1","true","yes","y","да"].includes(String(env[name]).toLowerCase()); }
function splitCsv(v) { return String(v || "").split(",").map(s => s.trim()).filter(Boolean); }
function parseWeights(v) { const p = splitCsv(v).map(Number); return [p[0] || 1, p[1] || 2, p[2] || 3]; }

const CONFIG = {
  vkToken: env.VK_TOKEN,
  groups: splitCsv(env.VK_GROUPS),
  daysBack: intEnv("VK_DAYS_BACK", 180),
  backfillFrom: env.VK_BACKFILL_FROM || "",
  wallMaxPosts: intEnv("VK_WALL_MAX_POSTS", 1000),
  enableStats: boolEnv("VK_ENABLE_STATS", true),
  statsFallbackFromPostViews: boolEnv("VK_STATS_FALLBACK_FROM_POST_VIEWS", true),
  recalcPreviousWeeks: intEnv("VK_RECALC_PREVIOUS_WEEKS", 6),
  requestDelayMs: intEnv("VK_REQUEST_DELAY_MS", 900),
  statsDelayMs: intEnv("VK_STATS_REQUEST_DELAY_MS", 1500),
  retryMax: intEnv("VK_RETRY_MAX", 8),
  viralThreshold: numEnv("VK_VIRAL_THRESHOLD", 2),
  engagementWeights: parseWeights(env.VK_ENGAGEMENT_WEIGHTS || "1,2,3"),
  spreadsheetId: env.GOOGLE_SHEETS_SPREADSHEET_ID,
  serviceAccountEmail: env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
  privateKey: env.GOOGLE_PRIVATE_KEY,
  summarySheetName: env.GOOGLE_SUMMARY_SHEET_NAME || "weekly_summary",
  postsSheetName: env.GOOGLE_POSTS_SHEET_NAME || "posts_raw_vk",
};

const SUMMARY_HEADERS = [
  "Ключ","Дата сбора","Неделя с","Неделя по","ID группы","Короткое имя группы","Название группы",
  "Подписчики на конец недели","Публикации","Просмотры постов","Лайки","Комментарии","Репосты","Вовлеченность",
  "ER по подписчикам, %","ER по просмотрам, %","ER по охвату, %","Доля охвата подписчиков к подписчикам, %",
  "Посетители сообщества","Просмотры/показы сообщества","Охват всего","Охват подписчиков","Охват мобильный",
  "Подписались","Отписались","Чистый прирост подписчиков","Прирост подписчиков за неделю, %",
  "Дельта подписчиков WoW, абс.","Дельта подписчиков WoW, %","Дельта охвата WoW, абс.","Дельта охвата WoW, %",
  "Дельта показов WoW, абс.","Дельта показов WoW, %","Дельта ER по охвату WoW, п.п.","Дельта ER по охвату WoW, %",
  "Средние просмотры поста","Медианные просмотры поста","Лучший пост недели, ID","Лучший пост недели, ссылка","Лучший пост недели: просмотры",
  "Лучший пост недели: вовлеченность","Лучший пост недели: ER по просмотрам, %","Вирусность, %","Обсуждаемость, %","Лайкабельность, %",
  "Индекс вовлеченности","Вирусные посты, шт.","Вирусные посты, %","Стд. отклонение просмотров","Частота публикаций в день",
  "Лучший час публикации","Лучший день недели публикации","Прогноз просмотров следующей недели","Прогноз прироста подписчиков","AI-рекомендация",
  "Источник охвата","Предупреждения"
];

const POSTS_HEADERS = [
  "Ключ поста","Дата сбора","ID группы","Короткое имя группы","Название группы","Дата поста","Неделя с","ID поста","Ссылка","Тип поста","Текст поста",
  "Просмотры","Лайки","Комментарии","Репосты","Вовлеченность","ER по просмотрам, %","Вирусность, %","Обсуждаемость, %","Лайкабельность, %",
  "Индекс вовлеченности","Час публикации","День недели"
];

function dateIso(d) { return d.toISOString().slice(0, 10); }
function toUnix(d) { return Math.floor(d.getTime() / 1000); }
function fromUnix(ts) { return new Date(ts * 1000); }
function startOfDayUtc(d) { return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())); }
function endOfDayUtc(d) { const x = startOfDayUtc(d); x.setUTCDate(x.getUTCDate() + 1); x.setUTCSeconds(x.getUTCSeconds() - 1); return x; }
function mondayOfWeekUtc(d) { const x = startOfDayUtc(d); const day = x.getUTCDay() || 7; x.setUTCDate(x.getUTCDate() - day + 1); return x; }
function sundayOfWeekUtc(monday) { return endOfDayUtc(new Date(Date.UTC(monday.getUTCFullYear(), monday.getUTCMonth(), monday.getUTCDate() + 6))); }
function lastFullWeekMonday(now = new Date()) { const m = mondayOfWeekUtc(now); m.setUTCDate(m.getUTCDate() - 7); return m; }
function addDays(d, days) { const x = new Date(d); x.setUTCDate(x.getUTCDate() + days); return x; }
function addWeeks(d, weeks) { return addDays(d, weeks * 7); }
function pct(num, den) { return den ? (Number(num) / Number(den)) * 100 : ""; }
function safeFixed(v, digits = 4) { if (v === "" || v === null || v === undefined || Number.isNaN(Number(v))) return ""; return Number(Number(v).toFixed(digits)); }
function sum(arr) { return arr.reduce((a,b)=>a+(Number(b)||0),0); }
function avg(arr) { return arr.length ? sum(arr)/arr.length : ""; }
function median(arr) { const a=arr.map(Number).filter(Number.isFinite).sort((x,y)=>x-y); if(!a.length) return ""; const m=Math.floor(a.length/2); return a.length%2?a[m]:(a[m-1]+a[m])/2; }
function stddev(arr) { const a=arr.map(Number).filter(Number.isFinite); if(!a.length) return ""; const mean=sum(a)/a.length; return Math.sqrt(sum(a.map(x=>(x-mean)**2))/a.length); }
function deltaAbs(cur, prev) { return cur !== "" && prev !== "" && cur !== undefined && prev !== undefined ? Number(cur)-Number(prev) : ""; }
function deltaPct(cur, prev) { return cur !== "" && prev !== "" && Number(prev) !== 0 ? ((Number(cur)-Number(prev))/Number(prev))*100 : ""; }
function cleanGroup(raw) { return String(raw).trim().replace(/^https?:\/\/(m\.)?vk\.com\//, "").replace(/^@/, ""); }
function dayNameRu(day) { return ["Вс","Пн","Вт","Ср","Чт","Пт","Сб"][day]; }
function postLink(group, postId) { return `https://vk.com/wall-${group.id}_${postId}`; }
function warn(existing, text) { return existing ? `${existing}; ${text}` : text; }

async function vkApi(method, params = {}, delay = 0) {
  if (delay) await sleep(delay);
  for (let attempt=0; attempt<CONFIG.retryMax; attempt++) {
    await sleep(CONFIG.requestDelayMs);
    const res = await axios.get(`https://api.vk.com/method/${method}`, { params: { ...params, access_token: CONFIG.vkToken, v: VK_API_VERSION }, timeout: 30000 });
    if (res.data?.error) {
      const { error_code, error_msg } = res.data.error;
      if (error_code === 6) { await sleep(CONFIG.requestDelayMs * (attempt + 2)); continue; }
      const e = new Error(`Ошибка VK API в ${method}: [${error_code}] ${error_msg}`); e.vkError = res.data.error; e.params = params; throw e;
    }
    return res.data.response;
  }
  throw new Error(`VK API ${method}: превышено число повторов`);
}

async function fetchGroup(rawId) {
  const id = cleanGroup(rawId);
  let lastErr = null;
  for (const params of [{group_ids:id,fields:"members_count,screen_name"},{group_id:id,fields:"members_count,screen_name"}]) {
    try {
      const res = await vkApi("groups.getById", params);
      const group = Array.isArray(res) ? res[0] : res?.groups?.[0];
      if (group?.id) return group;
    } catch(e) { lastErr = e; console.warn(`groups.getById не удался для ${id}: ${e.message}`); }
  }
  throw lastErr || new Error(`Не удалось получить группу VK: ${id}`);
}

async function fetchWallPosts(groupId) {
  const owner_id = -Number(groupId);
  const all=[]; let offset=0;
  while (all.length < CONFIG.wallMaxPosts) {
    const count = Math.min(100, CONFIG.wallMaxPosts - all.length);
    const res = await vkApi("wall.get", { owner_id, offset, count });
    const items = res?.items || [];
    if (!items.length) break;
    all.push(...items); offset += items.length;
    if (items.length < count) break;
  }
  return all;
}

function n(...vals) { for (const v of vals) if (v !== undefined && v !== null && v !== "") return Number(v)||0; return 0; }
function parseStats(items) {
  const out={available:true, visitors:0, communityViews:0, reachTotal:0, reachSubscribers:0, reachMobile:0, subscribed:0, unsubscribed:0, warning:""};
  for (const item of items) {
    const visitors = item.visitors || {}, reach = item.reach || {}, activity = item.activity || {};
    out.visitors += n(visitors.visitors, item.visitors_count);
    out.communityViews += n(visitors.views, item.views, item.views_count);
    out.reachTotal += n(reach.reach, reach.total, item.reach);
    out.reachSubscribers += n(reach.reach_subscribers, reach.subscribers, item.reach_subscribers);
    out.reachMobile += n(reach.mobile_reach, reach.mobile, item.mobile_reach);
    out.subscribed += n(activity.subscribed, item.subscribed);
    out.unsubscribed += n(activity.unsubscribed, item.unsubscribed);
  }
  return out;
}
async function fetchStats(groupId, start, end) {
  if (!CONFIG.enableStats) return {available:false, warning:"VK_ENABLE_STATS=false"};
  try {
    const res = await vkApi("stats.get", { group_id:Number(groupId), timestamp_from:toUnix(start), timestamp_to:toUnix(end), interval:"day" }, CONFIG.statsDelayMs);
    const items = Array.isArray(res) ? res : (res?.stats || res?.items || []);
    if (!items.length) return {available:false, warning:"stats.get вернул пустой ответ"};
    return parseStats(items);
  } catch(e) { return {available:false, warning:e.message}; }
}

function postType(post) {
  if (post.copy_history?.length) return "repost";
  const a = post.attachments || [];
  if (a.some(x=>x.type==="clip")) return "clip";
  if (a.some(x=>x.type==="video")) return "video";
  if (a.some(x=>x.type==="photo")) return "image";
  return (post.text||"").trim() ? "text" : "other";
}
function postMetrics(post) {
  const views=Number(post.views?.count||0), likes=Number(post.likes?.count||0), comments=Number(post.comments?.count||0), reposts=Number(post.reposts?.count||0);
  const engagement=likes+comments+reposts; const [wL,wC,wR]=CONFIG.engagementWeights;
  return {views,likes,comments,reposts,engagement,erView:pct(engagement,views),virality:pct(reposts,views),discussion:pct(comments,views),likability:pct(likes,views),engagementIndex:pct(likes*wL+comments*wC+reposts*wR,views)};
}
function weekPosts(posts, start, end) { const from=toUnix(start), to=toUnix(end); return posts.filter(p => p.date >= from && p.date <= to); }
function bestBy(posts, fn) { return posts.length ? posts.reduce((best,p)=>fn(p)>fn(best)?p:best, posts[0]) : null; }
function bestPosting(posts, mode) {
  const b = new Map();
  for (const p of posts) { const d=fromUnix(p.date); const key=mode==="hour"?String(d.getUTCHours()).padStart(2,"0"):dayNameRu(d.getUTCDay()); const m=postMetrics(p); const x=b.get(key)||{views:0,count:0}; x.views+=m.views; x.count++; b.set(key,x); }
  let best="", val=-1; for (const [k,v] of b) { const avg=v.views/v.count; if(avg>val){val=avg;best=k;} } return best;
}
function forecastNext(values) { const v=values.map(Number).filter(Number.isFinite); if(v.length<2) return ""; const recent=v.slice(-4), diffs=[]; for(let i=1;i<recent.length;i++) diffs.push(recent[i]-recent[i-1]); return Math.max(0, recent.at(-1)+avg(diffs)); }

async function getSheets() {
  const privateKey = String(CONFIG.privateKey||"").replace(/^"|"$/g, "").replace(/\\n/g, "\n");
  const auth = new google.auth.JWT(CONFIG.serviceAccountEmail, null, privateKey, ["https://www.googleapis.com/auth/spreadsheets"]);
  return google.sheets({ version:"v4", auth });
}
async function readSheet(sheets, name) { try { const r=await sheets.spreadsheets.values.get({spreadsheetId:CONFIG.spreadsheetId, range:`${name}!A:ZZ`}); return r.data.values||[]; } catch { return []; } }
async function ensureHeaders(sheets, name, headers) {
  const data=await readSheet(sheets,name);
  if(!data.length){ await sheets.spreadsheets.values.update({spreadsheetId:CONFIG.spreadsheetId,range:`${name}!A1`,valueInputOption:"RAW",requestBody:{values:[headers]}}); return {header:headers, rows:[]}; }
  const existing=data[0], missing=headers.filter(h=>!existing.includes(h));
  if(missing.length){ const merged=[...existing,...missing]; await sheets.spreadsheets.values.update({spreadsheetId:CONFIG.spreadsheetId,range:`${name}!A1`,valueInputOption:"RAW",requestBody:{values:[merged]}}); return {header:merged, rows:data.slice(1)}; }
  return {header:existing, rows:data.slice(1)};
}
function rowsToObjects(rows, header) { return rows.map((r,idx)=>{ const o={__rowNumber:idx+2}; header.forEach((h,i)=>o[h]=r[i]??""); return o; }); }
function objectToRow(obj, header) { return header.map(h=>obj[h]??""); }
async function upsertRows(sheets, name, header, keyName, objects) {
  if(!objects.length) return;
  const data=await readSheet(sheets,name); const h=data[0]||header; const existing=rowsToObjects(data.slice(1),h); const byKey=new Map(existing.map(r=>[String(r[keyName]),r.__rowNumber])); const app=[];
  for (const obj of objects) { const row=objectToRow(obj,h), rowNo=byKey.get(String(obj[keyName])); if(rowNo){ await sheets.spreadsheets.values.update({spreadsheetId:CONFIG.spreadsheetId,range:`${name}!A${rowNo}`,valueInputOption:"RAW",requestBody:{values:[row]}}); } else app.push(row); }
  if(app.length) await sheets.spreadsheets.values.append({spreadsheetId:CONFIG.spreadsheetId,range:`${name}!A1`,valueInputOption:"RAW",insertDataOption:"INSERT_ROWS",requestBody:{values:app}});
}
function buildWeeks(start, last) { const weeks=[]; for(let d=new Date(start); d<=last; d=addWeeks(d,1)) weeks.push({start:new Date(d), end:sundayOfWeekUtc(d), key:dateIso(d)}); return weeks; }
function aiRecommendation(r){ const out=[]; if(Number(r["Публикации"]||0)<3) out.push("увеличить частоту публикаций"); if(Number(r["ER по просмотрам, %"]||0)<2) out.push("усилить CTA и интерактив"); if(Number(r["Вирусность, %"]||0)<0.5) out.push("добавить поводы для репостов"); if(Number(r["Вирусные посты, шт."]||0)>0) out.push("повторить формат вирусных постов"); return out.join("; ") || "продолжать тесты форматов"; }

function reconstructSubscribers(rows, currentMembers, lastKey) {
  const sorted=[...rows].sort((a,b)=>String(a["Неделя с"]).localeCompare(String(b["Неделя с"])));
  const last=sorted.find(r=>r["Неделя с"]===lastKey)||sorted.at(-1);
  if(last && !last["Подписчики на конец недели"]){ last["Подписчики на конец недели"]=currentMembers; last["Предупреждения"]=warn(last["Предупреждения"],"подписчики последней недели взяты как текущий снимок"); }
  for(let i=sorted.length-2;i>=0;i--){ const cur=sorted[i], next=sorted[i+1]; if(!cur["Подписчики на конец недели"] && next["Подписчики на конец недели"]!=="" && next["Чистый прирост подписчиков"]!==""){ cur["Подписчики на конец недели"]=Number(next["Подписчики на конец недели"])-Number(next["Чистый прирост подписчиков"]); cur["Предупреждения"]=warn(cur["Предупреждения"],"подписчики реконструированы назад по чистому приросту"); } }
  for(const r of sorted){ if(r["Подписчики на конец недели"]){ r["ER по подписчикам, %"]=safeFixed(pct(r["Вовлеченность"],r["Подписчики на конец недели"])); r["Доля охвата подписчиков к подписчикам, %"]=safeFixed(pct(r["Охват подписчиков"],r["Подписчики на конец недели"])); } }
}
function applyDeltas(rows, existing){ const combined=[...existing,...rows]; const groupId=rows[0]?.["ID группы"]; const series=combined.filter(r=>String(r["ID группы"])===String(groupId)).sort((a,b)=>String(a["Неделя с"]).localeCompare(String(b["Неделя с"]))); const byWeek=new Map(series.map(r=>[r["Неделя с"],r]));
  for(const r of rows){ const prevKey=dateIso(addWeeks(new Date(`${r["Неделя с"]}T00:00:00Z`),-1)); const p=byWeek.get(prevKey); if(p){ r["Дельта подписчиков WoW, абс."]=safeFixed(deltaAbs(r["Подписчики на конец недели"],p["Подписчики на конец недели"])); r["Дельта подписчиков WoW, %"]=safeFixed(deltaPct(r["Подписчики на конец недели"],p["Подписчики на конец недели"])); r["Дельта охвата WoW, абс."]=safeFixed(deltaAbs(r["Охват всего"],p["Охват всего"])); r["Дельта охвата WoW, %"]=safeFixed(deltaPct(r["Охват всего"],p["Охват всего"])); r["Дельта показов WoW, абс."]=safeFixed(deltaAbs(r["Просмотры/показы сообщества"],p["Просмотры/показы сообщества"])); r["Дельта показов WoW, %"]=safeFixed(deltaPct(r["Просмотры/показы сообщества"],p["Просмотры/показы сообщества"])); r["Дельта ER по охвату WoW, п.п."]=safeFixed(deltaAbs(r["ER по охвату, %"],p["ER по охвату, %"])); r["Дельта ER по охвату WoW, %"]=safeFixed(deltaPct(r["ER по охвату, %"],p["ER по охвату, %"])); if(r["Чистый прирост подписчиков"]!=="" && p["Подписчики на конец недели"]) r["Прирост подписчиков за неделю, %"]=safeFixed(pct(r["Чистый прирост подписчиков"],p["Подписчики на конец недели"])); } }
  const views=series.map(r=>Number(r["Просмотры постов"])).filter(Number.isFinite); const growth=series.map(r=>Number(r["Чистый прирост подписчиков"])).filter(Number.isFinite); for(const r of rows){ r["Прогноз просмотров следующей недели"]=safeFixed(forecastNext(views)); r["Прогноз прироста подписчиков"]=safeFixed(forecastNext(growth)); r["AI-рекомендация"]=aiRecommendation(r); }
}

async function main(){
  if(!CONFIG.vkToken||!CONFIG.groups.length||!CONFIG.spreadsheetId||!CONFIG.serviceAccountEmail||!CONFIG.privateKey) throw new Error("Не заданы обязательные env-переменные");
  const sheets=await getSheets(); const summaryInfo=await ensureHeaders(sheets,CONFIG.summarySheetName,SUMMARY_HEADERS); const postsInfo=await ensureHeaders(sheets,CONFIG.postsSheetName,POSTS_HEADERS); const existing=rowsToObjects(summaryInfo.rows,summaryInfo.header);
  const lastMonday=lastFullWeekMonday(); console.log(`VK v7.2: групп=${CONFIG.groups.length}, последняя полная неделя ${dateIso(lastMonday)}..${dateIso(sundayOfWeekUtc(lastMonday))}`);
  for(const raw of CONFIG.groups){ const group=await fetchGroup(raw); console.log(`Группа: ${group.name} (${group.id}), текущие подписчики=${group.members_count}`); const allPosts=await fetchWallPosts(group.id); const earliest=allPosts.length?startOfDayUtc(fromUnix(Math.min(...allPosts.map(p=>p.date)))):addDays(new Date(),-CONFIG.daysBack); const configured=CONFIG.backfillFrom?new Date(`${CONFIG.backfillFrom}T00:00:00Z`):addDays(new Date(),-CONFIG.daysBack); const start=mondayOfWeekUtc(CONFIG.backfillFrom?configured:earliest); const weeksAll=buildWeeks(start,lastMonday); const groupExisting=existing.filter(r=>String(r["ID группы"])===String(group.id)); const existingKeys=new Set(groupExisting.map(r=>r["Ключ"])); const recalcFrom=addWeeks(lastMonday,-Math.max(0,CONFIG.recalcPreviousWeeks-1)); const weeks=weeksAll.filter(w=>!existingKeys.has(`${group.id}_${w.key}`)||w.start>=recalcFrom); console.log(`${group.name}: постов=${allPosts.length}, недель к обработке=${weeks.length}`);
    const weeklyRows=[], postRows=[];
    for(const week of weeks){ const posts=weekPosts(allPosts,week.start,week.end); const metrics=posts.map(postMetrics); const views=metrics.map(m=>m.views); const likes=sum(metrics.map(m=>m.likes)), comments=sum(metrics.map(m=>m.comments)), reposts=sum(metrics.map(m=>m.reposts)), postViews=sum(views), engagement=likes+comments+reposts; const stats=await fetchStats(group.id,week.start,week.end); const warnings=[]; let reachSource="stats.get", visitors="", communityViews="", reachTotal="", reachSubscribers="", reachMobile="", subscribed="", unsubscribed="", netGrowth="";
      if(stats.available){ visitors=stats.visitors||""; communityViews=stats.communityViews||""; reachTotal=stats.reachTotal||""; reachSubscribers=stats.reachSubscribers||""; reachMobile=stats.reachMobile||""; subscribed=stats.subscribed||""; unsubscribed=stats.unsubscribed||""; netGrowth=(Number(subscribed)||0)-(Number(unsubscribed)||0); }
      else { warnings.push(`stats.get недоступен: ${stats.warning}`); if(CONFIG.statsFallbackFromPostViews && postViews){ reachTotal=postViews; reachSource="fallback: просмотры постов"; warnings.push("охват всего заменён просмотрами постов"); } else reachSource="нет данных"; }
      const existingSame=groupExisting.find(r=>r["Ключ"]===`${group.id}_${week.key}`); let subs=existingSame?.["Подписчики на конец недели"]?Number(existingSame["Подписчики на конец недели"]):""; if(week.key===dateIso(lastMonday)&&!subs){ subs=Number(group.members_count||0); if(!stats.available) warnings.push("подписчики последней недели взяты как текущий снимок"); } if(!subs && !stats.available) warnings.push("исторические подписчики недоступны без прошлых снимков или stats.get");
      const bestViews=bestBy(posts,p=>postMetrics(p).views), bestEng=bestBy(posts,p=>postMetrics(p).engagement), bestEr=bestBy(posts,p=>postMetrics(p).erView||0); const med=median(views); const viralCount=views.filter(v=>med&&v>med*CONFIG.viralThreshold).length;
      const row={"Ключ":`${group.id}_${week.key}`,"Дата сбора":new Date().toISOString(),"Неделя с":week.key,"Неделя по":dateIso(week.end),"ID группы":group.id,"Короткое имя группы":group.screen_name||cleanGroup(raw),"Название группы":group.name,"Подписчики на конец недели":subs,"Публикации":posts.length,"Просмотры постов":postViews,"Лайки":likes,"Комментарии":comments,"Репосты":reposts,"Вовлеченность":engagement,"ER по подписчикам, %":safeFixed(pct(engagement,subs)),"ER по просмотрам, %":safeFixed(pct(engagement,postViews)),"ER по охвату, %":safeFixed(pct(engagement,reachTotal)),"Доля охвата подписчиков к подписчикам, %":safeFixed(pct(reachSubscribers,subs)),"Посетители сообщества":visitors,"Просмотры/показы сообщества":communityViews,"Охват всего":reachTotal,"Охват подписчиков":reachSubscribers,"Охват мобильный":reachMobile,"Подписались":subscribed,"Отписались":unsubscribed,"Чистый прирост подписчиков":netGrowth,"Прирост подписчиков за неделю, %":"","Средние просмотры поста":safeFixed(avg(views)),"Медианные просмотры поста":safeFixed(med),"Лучший пост недели, ID":bestViews?.id||"","Лучший пост недели, ссылка":bestViews?postLink(group,bestViews.id):"","Лучший пост недели: просмотры":bestViews?postMetrics(bestViews).views:"","Лучший пост недели: вовлеченность":bestEng?postMetrics(bestEng).engagement:"","Лучший пост недели: ER по просмотрам, %":bestEr?safeFixed(postMetrics(bestEr).erView):"","Вирусность, %":safeFixed(pct(reposts,postViews)),"Обсуждаемость, %":safeFixed(pct(comments,postViews)),"Лайкабельность, %":safeFixed(pct(likes,postViews)),"Индекс вовлеченности":safeFixed(pct(likes*CONFIG.engagementWeights[0]+comments*CONFIG.engagementWeights[1]+reposts*CONFIG.engagementWeights[2],postViews)),"Вирусные посты, шт.":viralCount,"Вирусные посты, %":safeFixed(pct(viralCount,posts.length)),"Стд. отклонение просмотров":safeFixed(stddev(views)),"Частота публикаций в день":safeFixed(posts.length/7),"Лучший час публикации":bestPosting(posts,"hour"),"Лучший день недели публикации":bestPosting(posts,"day"),"Прогноз просмотров следующей недели":"","Прогноз прироста подписчиков":"","AI-рекомендация":"","Источник охвата":reachSource,"Предупреждения":warnings.join("; ")}; weeklyRows.push(row);
      for(const p of posts){ const m=postMetrics(p), d=fromUnix(p.date); postRows.push({"Ключ поста":`${group.id}_${p.id}`,"Дата сбора":new Date().toISOString(),"ID группы":group.id,"Короткое имя группы":group.screen_name||cleanGroup(raw),"Название группы":group.name,"Дата поста":d.toISOString(),"Неделя с":week.key,"ID поста":p.id,"Ссылка":postLink(group,p.id),"Тип поста":postType(p),"Текст поста":String(p.text||"").slice(0,300),"Просмотры":m.views,"Лайки":m.likes,"Комментарии":m.comments,"Репосты":m.reposts,"Вовлеченность":m.engagement,"ER по просмотрам, %":safeFixed(m.erView),"Вирусность, %":safeFixed(m.virality),"Обсуждаемость, %":safeFixed(m.discussion),"Лайкабельность, %":safeFixed(m.likability),"Индекс вовлеченности":safeFixed(m.engagementIndex),"Час публикации":String(d.getUTCHours()).padStart(2,"0"),"День недели":dayNameRu(d.getUTCDay())}); }
    }
    weeklyRows.sort((a,b)=>String(a["Неделя с"]).localeCompare(String(b["Неделя с"]))); reconstructSubscribers(weeklyRows,Number(group.members_count||0),dateIso(lastMonday)); applyDeltas(weeklyRows,existing); await upsertRows(sheets,CONFIG.summarySheetName,summaryInfo.header,"Ключ",weeklyRows); await upsertRows(sheets,CONFIG.postsSheetName,postsInfo.header,"Ключ поста",postRows); console.log(`${group.name}: обновлено недель=${weeklyRows.length}, постов=${postRows.length}`);
  }
  console.log("VK v7.2 завершена.");
}

main().catch(err=>{ console.error("Сбой синхронизации VK."); if(err?.vkError) console.error("VK raw error:",JSON.stringify(err.vkError)); if(err?.params) console.error("VK params:",JSON.stringify(err.params)); console.error(err.stack||err.message||err); process.exit(1); });
