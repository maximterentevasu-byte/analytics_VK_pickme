import axios from 'axios';
import { google } from 'googleapis';
import dotenv from 'dotenv';

dotenv.config();

const cfg = {
  vkToken: req('VK_TOKEN'),
  vkGroups: req('VK_GROUPS').split(',').map(s => s.trim()).filter(Boolean),
  vkVersion: process.env.VK_API_VERSION || '5.199',
  daysBack: intEnv('VK_DAYS_BACK', 180),
  wallMaxPosts: intEnv('VK_WALL_MAX_POSTS', 500),
  enableStats: boolEnv('VK_ENABLE_STATS', true),
  backfillFrom: process.env.VK_BACKFILL_FROM || '',
  requestDelayMs: intEnv('VK_REQUEST_DELAY_MS', 900),
  statsDelayMs: intEnv('VK_STATS_REQUEST_DELAY_MS', 1400),
  retryMax: intEnv('VK_RETRY_MAX', 8),
  recalcPreviousWeeks: intEnv('VK_RECALC_PREVIOUS_WEEKS', 6),
  reachSubscribersFallbackRatio: numEnv('VK_REACH_SUBSCRIBERS_FALLBACK_RATIO', 0.7),
  shortVideoMaxSeconds: intEnv('VK_SHORT_VIDEO_MAX_SECONDS', 75),
  sheetId: req('GOOGLE_SHEETS_SPREADSHEET_ID'),
  serviceEmail: req('GOOGLE_SERVICE_ACCOUNT_EMAIL'),
  privateKey: req('GOOGLE_PRIVATE_KEY').replace(/\\n/g, '\n'),
  sheets: {
    summary: process.env.GOOGLE_SUMMARY_SHEET_NAME || 'weekly_summary',
    posts: process.env.GOOGLE_POSTS_SHEET_NAME || 'posts_raw_vk',
    dashboard: process.env.GOOGLE_DASHBOARD_SHEET_NAME || 'dashboard_data',
    insights: process.env.GOOGLE_AI_INSIGHTS_SHEET_NAME || 'ai_insights',
    aiPosts: process.env.GOOGLE_AI_POSTS_SHEET_NAME || 'ai_posts',
    abTests: process.env.GOOGLE_AB_TESTS_SHEET_NAME || 'ab_tests',
    growth: process.env.GOOGLE_GROWTH_FORECAST_SHEET_NAME || 'growth_forecast',
    clips: process.env.GOOGLE_CLIPS_SHEET_NAME || 'clips_analytics',
    audience: process.env.GOOGLE_AUDIENCE_SHEET_NAME || 'audience_weekly',
  }
};

const HEADERS = {
  summary: ['Ключ','Дата сбора','Неделя с','Неделя по','ID группы','Короткое имя группы','Название группы','Подписчики на конец недели','Публикации','Просмотры постов','Лайки','Комментарии','Репосты','Вовлеченность','ER по подписчикам, %','ER по просмотрам, %','ER по охвату, %','Посетители сообщества','Просмотры/показы сообщества','Охват всего','Охват подписчиков','Охват мобильный','Доля охвата подписчиков к подписчикам, %','Подписались','Отписались','Чистый прирост подписчиков','Прирост подписчиков за неделю, %','Дельта подписчиков WoW, абс.','Дельта подписчиков WoW, %','Дельта охвата WoW, абс.','Дельта охвата WoW, %','Дельта показов WoW, абс.','Дельта показов WoW, %','Дельта ER по охвату WoW, п.п.','Дельта ER по охвату WoW, %','Средние просмотры поста','Медианные просмотры поста','Лучший пост недели, ID','Лучший пост недели, ссылка','Лучший пост недели: просмотры','Источник охвата','Вирусность, %','Обсуждаемость, %','Лайкабельность, %','Индекс вовлеченности','Лучший день публикации, ЕКБ','Лучший час публикации, ЕКБ','Рекомендация','Предупреждения'],
  posts: ['Ключ','Дата сбора','Неделя с','ID группы','Название группы','ID поста','Ссылка','Дата публикации ЕКБ','День недели ЕКБ','Час ЕКБ','Тип поста','Текст','Длина текста','Есть вопрос','Просмотры','Лайки','Комментарии','Репосты','Вовлеченность','ER по просмотрам, %','Вирусность, %','Обсуждаемость, %','Лайкабельность, %','Индекс вовлеченности','Видео секунд','Это клип/шортс','Предупреждения'],
  dashboard: ['Ключ','Неделя с','Неделя по','ID группы','Название группы','Подписчики','Публикации','Просмотры постов','Вовлеченность','ER подписчики %','ER просмотры %','ER охват %','Охват всего','Охват подписчиков','Доля охват/подписчики %','Вирусность %','Обсуждаемость %','Лайкабельность %','Индекс вовлеченности','Лучший час ЕКБ','Предупреждения'],
  insights: ['Ключ','Дата сбора','Неделя с','ID группы','Название группы','Тип вывода','Приоритет','Вывод','Рекомендация','Основание'],
  aiPosts: ['Ключ','Дата сбора','Неделя с','ID группы','Название группы','Идея поста','Формат','Лучшее время ЕКБ','Черновик текста','Почему это должно сработать','CTA','Основание'],
  abTests: ['Ключ','Дата сбора','Неделя с','ID группы','Название группы','Гипотеза','Группа A','Постов A','Средний ER A, %','Группа B','Постов B','Средний ER B, %','Победитель','Разница, п.п.','Вывод','Статус'],
  growth: ['Ключ','Дата сбора','Неделя с','ID группы','Название группы','Текущие подписчики','Средний недельный прирост','Прогноз LOW 4 недели','Прогноз MID 4 недели','Прогноз HIGH 4 недели','Прогноз LOW 12 недель','Прогноз MID 12 недель','Прогноз HIGH 12 недель','Уверенность','Основание','Предупреждения'],
  clips: ['Ключ','Дата сбора','Неделя с','ID группы','Название группы','ID поста','Ссылка','Дата публикации ЕКБ','Видео секунд','Просмотры','Лайки','Комментарии','Репосты','Вовлеченность','ER по просмотрам, %','Вирусность, %','Обсуждаемость, %','Лайкабельность, %','Индекс вовлеченности','Текст','Предупреждения'],
  audience: ['Ключ','Дата сбора','Неделя с','Неделя по','ID группы','Название группы','Сегмент','Значение','Количество / доля','Источник','Предупреждения']
};

const sleep = ms => new Promise(r => setTimeout(r, ms));
function req(name){ const v = process.env[name]; if(!v) throw new Error(`Не задана переменная ${name}`); return v; }
function intEnv(name, d){ const v = parseInt(process.env[name] || '', 10); return Number.isFinite(v) ? v : d; }
function numEnv(name, d){ const v = parseFloat(process.env[name] || ''); return Number.isFinite(v) ? v : d; }
function boolEnv(name, d){ const v = process.env[name]; if(v == null) return d; return ['1','true','yes','да'].includes(String(v).toLowerCase()); }
function pct(a,b){ return b ? round((a / b) * 100, 4) : ''; }
function round(n, d=2){ return Number.isFinite(n) ? Number(n.toFixed(d)) : ''; }
function sum(arr){ return arr.reduce((a,b)=>a+(Number(b)||0),0); }
function avg(arr){ const clean=arr.filter(n=>Number.isFinite(n)); return clean.length ? sum(clean)/clean.length : 0; }
function median(arr){ const clean=arr.filter(n=>Number.isFinite(n)).sort((a,b)=>a-b); if(!clean.length) return 0; const m=Math.floor(clean.length/2); return clean.length%2?clean[m]:(clean[m-1]+clean[m])/2; }
function std(arr){ const clean=arr.filter(n=>Number.isFinite(n)); if(!clean.length) return 0; const a=avg(clean); return Math.sqrt(avg(clean.map(x=>(x-a)**2))); }
function isoDate(d){ return d.toISOString().slice(0,10); }
function unix(d){ return Math.floor(d.getTime()/1000); }
function cleanGroupId(s){ return String(s).trim().replace(/^https?:\/\/vk\.com\//,'').replace(/^@/,'').replace(/^club/,'').replace(/^public/,''); }
function ekbDate(ts){ return new Date(ts*1000).toLocaleString('ru-RU',{timeZone:'Asia/Yekaterinburg',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'}); }
function ekbHour(ts){ return Number(new Date(ts*1000).toLocaleString('ru-RU',{timeZone:'Asia/Yekaterinburg',hour:'2-digit',hour12:false})); }
function ekbWeekday(ts){ return new Date(ts*1000).toLocaleString('ru-RU',{timeZone:'Asia/Yekaterinburg',weekday:'long'}); }
function mondayStartUTC(date){ const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate())); const day = d.getUTCDay() || 7; d.setUTCDate(d.getUTCDate() - day + 1); d.setUTCHours(0,0,0,0); return d; }
function lastFullWeek(){ const now = new Date(); const thisMon = mondayStartUTC(now); const start = new Date(thisMon); start.setUTCDate(start.getUTCDate()-7); const end = new Date(thisMon); end.setUTCSeconds(-1); return {start,end}; }
function weekEnd(start){ const e = new Date(start); e.setUTCDate(e.getUTCDate()+7); e.setUTCSeconds(-1); return e; }
function weekKey(groupId, start){ return `${groupId}_${isoDate(start)}`; }

async function vkApi(method, params = {}, extraDelay = cfg.requestDelayMs) {
  for (let i=0; i<cfg.retryMax; i++) {
    await sleep(extraDelay);
    try {
      const { data } = await axios.get(`https://api.vk.com/method/${method}`, { params: {...params, access_token: cfg.vkToken, v: cfg.vkVersion}, timeout: 30000 });
      if (data.error) {
        const code = data.error.error_code;
        if (code === 6 || code === 9 || code === 10) { await sleep(1200 + i*900); continue; }
        console.error(`Ошибка VK API в ${method}: [${code}] ${data.error.error_msg}`);
        console.error('Параметры:', JSON.stringify(params));
        throw new Error(`ошибка VK API в ${method}: [${code}] ${data.error.error_msg}`);
      }
      return data.response;
    } catch (e) {
      if (i === cfg.retryMax-1) throw e;
      await sleep(1000 + i*700);
    }
  }
}

async function fetchGroup(raw) {
  const id = cleanGroupId(raw);
  let response;
  try { response = await vkApi('groups.getById', { group_ids: id, fields: 'members_count,screen_name' }); }
  catch(e) { response = await vkApi('groups.getById', { group_id: id, fields: 'members_count,screen_name' }); }
  let group = Array.isArray(response) ? response[0] : response?.groups?.[0];
  if (!group) {
    response = await vkApi('groups.getById', { group_id: id, fields: 'members_count,screen_name' });
    group = Array.isArray(response) ? response[0] : response?.groups?.[0];
  }
  if (!group?.id) throw new Error(`Не удалось получить группу ${raw}. Ответ VK: ${JSON.stringify(response)}`);
  return group;
}

async function fetchWallPosts(groupId) {
  const owner_id = -Math.abs(groupId);
  const posts = [];
  let offset = 0;
  while (posts.length < cfg.wallMaxPosts) {
    const count = Math.min(100, cfg.wallMaxPosts - posts.length);
    const res = await vkApi('wall.get', { owner_id, count, offset, extended: 0 });
    const items = res?.items || [];
    if (!items.length) break;
    for (const p of items) posts.push(p);
    offset += items.length;
    if (items.length < count) break;
  }
  return posts.filter(p => !p.is_pinned);
}

async function fetchStats(groupId, week) {
  if (!cfg.enableStats) return { ok:false, warning:'VK_ENABLE_STATS=false' };
  try {
    const res = await vkApi('stats.get', { group_id: groupId, timestamp_from: unix(week.start), timestamp_to: unix(week.end), interval: 'day' }, cfg.statsDelayMs);
    const days = Array.isArray(res) ? res : [];
    if (!days.length) return { ok:false, warning:'stats.get пустой ответ' };
    const acc = { visitors:0, views:0, reachTotal:0, reachSubscribers:0, reachMobile:0, subscribed:0, unsubscribed:0, audienceRows:[] };
    for (const d of days) {
      acc.visitors += Number(d.visitors?.visitors || d.visitors || 0);
      acc.views += Number(d.visitors?.views || d.views || 0);
      acc.reachTotal += Number(d.reach?.reach || d.reach?.total || d.reach_total || 0);
      acc.reachSubscribers += Number(d.reach?.subscribers || d.reach_subscribers || 0);
      acc.reachMobile += Number(d.reach?.mobile || d.reach_mobile || 0);
      acc.subscribed += Number(d.activity?.subscribed || d.subscribed || 0);
      acc.unsubscribed += Number(d.activity?.unsubscribed || d.unsubscribed || 0);
      collectAudience(acc.audienceRows, d, week);
    }
    return { ok:true, ...acc, warning:'' };
  } catch (e) {
    return { ok:false, warning:`stats.get недоступен: ${e.message}` };
  }
}

function collectAudience(rows, day, week) {
  const sources = [
    ['Пол/возраст', day.sex_age], ['Города', day.cities], ['Страны', day.countries], ['Устройства', day.reach?.devices || day.devices]
  ];
  for (const [segment, arr] of sources) {
    if (!Array.isArray(arr)) continue;
    for (const item of arr) {
      const value = item.name || item.city_name || item.country_name || item.value || item.key || item.sex || JSON.stringify(item);
      const count = item.count ?? item.value ?? item.visitors ?? item.reach ?? '';
      rows.push({segment, value, count, source:'stats.get'});
    }
  }
}

function postType(p) {
  const at = p.attachments || [];
  if (p.copy_history?.length) return 'repost';
  if (at.some(a => a.type === 'video')) return isClip(p) ? 'clip' : 'video';
  if (at.some(a => ['photo','album'].includes(a.type))) return 'image';
  if (at.some(a => a.type === 'link')) return 'link';
  return 'text';
}
function videoSeconds(p){ const v=(p.attachments||[]).find(a=>a.type==='video')?.video; return Number(v?.duration || 0); }
function isClip(p){ const v=(p.attachments||[]).find(a=>a.type==='video')?.video; if(!v) return false; const title = `${v.title||''} ${v.platform||''}`.toLowerCase(); return title.includes('clip') || title.includes('клип') || Number(v.duration||9999) <= cfg.shortVideoMaxSeconds; }
function postMetrics(p){ const views=Number(p.views?.count||0), likes=Number(p.likes?.count||0), comments=Number(p.comments?.count||0), reposts=Number(p.reposts?.count||0); const engagement=likes+comments+reposts; return {views,likes,comments,reposts,engagement, erView:pct(engagement,views), viral:pct(reposts,views), discussion:pct(comments,views), likeable:pct(likes,views), index:pct(likes+2*comments+3*reposts,views)}; }
function link(group, p){ return `https://vk.com/wall-${group.id}_${p.id}`; }

function buildWeeks(posts) {
  const { start: lastStart } = lastFullWeek();
  let first;
  if (cfg.backfillFrom) first = new Date(`${cfg.backfillFrom}T00:00:00Z`);
  else if (posts.length) first = new Date(Math.min(...posts.map(p=>p.date*1000)));
  else { first = new Date(lastStart); first.setUTCDate(first.getUTCDate()-cfg.daysBack); }
  first = mondayStartUTC(first);
  const minByDays = new Date(lastStart); minByDays.setUTCDate(minByDays.getUTCDate()-cfg.daysBack);
  if (!cfg.backfillFrom && first < minByDays) first = mondayStartUTC(minByDays);
  const weeks=[];
  for (let d=new Date(first); d<=lastStart; d.setUTCDate(d.getUTCDate()+7)) weeks.push({start:new Date(d), end:weekEnd(d)});
  return weeks;
}
function postsForWeek(posts, week){ return posts.filter(p => p.date >= unix(week.start) && p.date <= unix(week.end)); }

function summarizeWeek(group, week, posts, stats, prevSummary) {
  const m = posts.map(postMetrics);
  const views = sum(m.map(x=>x.views)); const likes=sum(m.map(x=>x.likes)); const comments=sum(m.map(x=>x.comments)); const reposts=sum(m.map(x=>x.reposts)); const engagement=likes+comments+reposts;
  const warnings=[];
  let reachTotal = stats.ok && stats.reachTotal ? stats.reachTotal : views;
  let reachSubscribers = stats.ok && stats.reachSubscribers ? stats.reachSubscribers : Math.round(reachTotal * cfg.reachSubscribersFallbackRatio);
  let reachMobile = stats.ok && stats.reachMobile ? stats.reachMobile : '';
  let visitors = stats.ok && stats.visitors ? stats.visitors : '';
  let communityViews = stats.ok && stats.views ? stats.views : views;
  let subscribed = stats.ok && stats.subscribed ? stats.subscribed : '';
  let unsubscribed = stats.ok && stats.unsubscribed ? stats.unsubscribed : '';
  let netGrowth = (subscribed !== '' || unsubscribed !== '') ? (Number(subscribed||0)-Number(unsubscribed||0)) : '';
  if (!stats.ok) warnings.push(stats.warning || 'stats.get недоступен');
  if (!stats.ok || !stats.reachTotal) warnings.push('охват всего оценочный: взят по просмотрам постов');
  if (!stats.ok || !stats.reachSubscribers) warnings.push(`охват подписчиков оценочный: ${cfg.reachSubscribersFallbackRatio*100}% от охвата всего`);
  if (subscribed === '') warnings.push('подписки/отписки недоступны через stats.get');
  let subscribers = '';
  if (prevSummary && prevSummary.subscribers !== '' && netGrowth !== '') subscribers = Number(prevSummary.subscribers) + Number(netGrowth);
  else { subscribers = Number(group.members_count || 0); warnings.push('подписчики оценочные/текущие: историческое значение VK не отдал'); }
  const best = posts.slice().sort((a,b)=>postMetrics(b).views - postMetrics(a).views)[0];
  const bestViews = best ? postMetrics(best).views : '';
  const hours = groupBy(posts, p=>ekbHour(p.date), p=>postMetrics(p).views);
  const days = groupBy(posts, p=>ekbWeekday(p.date), p=>postMetrics(p).views);
  const bestHour = topKey(hours); const bestDay=topKey(days);
  const viewsArr = m.map(x=>x.views);
  const recommendation = makeShortRecommendation(posts, m, bestDay, bestHour, warnings);
  const rowObj = { subscribers, reachTotal, reachSubscribers, communityViews, erReach:pct(engagement, reachTotal) };
  const deltaSubsAbs = prevSummary && prevSummary.subscribers !== '' ? Number(subscribers)-Number(prevSummary.subscribers) : '';
  const deltaReachAbs = prevSummary && prevSummary.reachTotal !== '' ? Number(reachTotal)-Number(prevSummary.reachTotal) : '';
  const deltaViewsAbs = prevSummary && prevSummary.communityViews !== '' ? Number(communityViews)-Number(prevSummary.communityViews) : '';
  const deltaErAbs = prevSummary && prevSummary.erReach !== '' ? round(Number(rowObj.erReach)-Number(prevSummary.erReach),4) : '';
  return {
    ...rowObj, warnings, key: weekKey(group.id, week.start), collectedAt: new Date().toISOString(), weekStart: isoDate(week.start), weekEnd: isoDate(week.end), groupId: group.id, screenName: group.screen_name || '', name: group.name,
    posts: posts.length, views, likes, comments, reposts, engagement, erSubs:pct(engagement, subscribers), erViews:pct(engagement, views), visitors, reachMobile, subscribed, unsubscribed, netGrowth, growthPct: netGrowth!=='' ? pct(netGrowth, Number(subscribers)-Number(netGrowth)) : '',
    deltaSubsAbs, deltaSubsPct: deltaSubsAbs!=='' ? pct(deltaSubsAbs, Number(prevSummary?.subscribers||0)) : '', deltaReachAbs, deltaReachPct: deltaReachAbs!=='' ? pct(deltaReachAbs, Number(prevSummary?.reachTotal||0)) : '', deltaViewsAbs, deltaViewsPct: deltaViewsAbs!=='' ? pct(deltaViewsAbs, Number(prevSummary?.communityViews||0)) : '', deltaErAbs, deltaErPct: deltaErAbs!=='' ? pct(deltaErAbs, Number(prevSummary?.erReach||0)) : '',
    avgViews: round(avg(viewsArr),2), medViews: round(median(viewsArr),2), bestPostId: best?.id || '', bestPostLink: best ? link(group,best) : '', bestPostViews: bestViews, reachSource: stats.ok && stats.reachTotal ? 'stats.get' : 'fallback: views', viral:pct(reposts,views), discussion:pct(comments,views), likeable:pct(likes,views), index:pct(likes+2*comments+3*reposts,views), bestDay, bestHour, recommendation
  };
}

function groupBy(items, keyFn, valFn){ const o={}; for(const it of items){ const k=keyFn(it); if(k===''||k==null||Number.isNaN(k)) continue; o[k]=(o[k]||0)+Number(valFn(it)||0); } return o; }
function topKey(obj){ const e=Object.entries(obj).sort((a,b)=>b[1]-a[1])[0]; return e ? e[0] : ''; }
function makeShortRecommendation(posts, metrics, day, hour, warnings){
  if (!posts.length) return 'Нет постов за неделю: запланируйте 3–5 публикаций и повторите сбор.';
  const types = groupBy(posts, postType, p=>postMetrics(p).erView || 0);
  const bestType = topKey(types) || postType(posts[0]);
  const er = round(avg(metrics.map(x=>Number(x.erView)||0)),2);
  return `Фокус: ${bestType}. Лучшее время: ${day || 'не определено'} ${hour !== '' ? `${hour}:00 ЕКБ` : ''}. Средний ER view ${er}%. ${warnings.length ? 'Часть метрик оценочная.' : 'Данные достаточны.'}`;
}
function summaryToRow(s){ return [s.key,s.collectedAt,s.weekStart,s.weekEnd,s.groupId,s.screenName,s.name,s.subscribers,s.posts,s.views,s.likes,s.comments,s.reposts,s.engagement,s.erSubs,s.erViews,s.erReach,s.visitors,s.communityViews,s.reachTotal,s.reachSubscribers,s.reachMobile,pct(s.reachSubscribers,s.subscribers),s.subscribed,s.unsubscribed,s.netGrowth,s.growthPct,s.deltaSubsAbs,s.deltaSubsPct,s.deltaReachAbs,s.deltaReachPct,s.deltaViewsAbs,s.deltaViewsPct,s.deltaErAbs,s.deltaErPct,s.avgViews,s.medViews,s.bestPostId,s.bestPostLink,s.bestPostViews,s.reachSource,s.viral,s.discussion,s.likeable,s.index,s.bestDay,s.bestHour,s.recommendation,s.warnings.join('; ')]; }
function postToRow(group, week, p){ const m=postMetrics(p); const text=(p.text||'').replace(/\s+/g,' ').slice(0,500); const warn=[]; if(!m.views) warn.push('нет просмотров у поста'); return [`${group.id}_${p.id}`,new Date().toISOString(),isoDate(week.start),group.id,group.name,p.id,link(group,p),ekbDate(p.date),ekbWeekday(p.date),ekbHour(p.date),postType(p),text,text.length,text.includes('?')?'да':'нет',m.views,m.likes,m.comments,m.reposts,m.engagement,m.erView,m.viral,m.discussion,m.likeable,m.index,videoSeconds(p),isClip(p)?'да':'нет',warn.join('; ')]; }
function dashboardRow(s){ return [s.key,s.weekStart,s.weekEnd,s.groupId,s.name,s.subscribers,s.posts,s.views,s.engagement,s.erSubs,s.erViews,s.erReach,s.reachTotal,s.reachSubscribers,pct(s.reachSubscribers,s.subscribers),s.viral,s.discussion,s.likeable,s.index,s.bestHour,s.warnings.join('; ')]; }

async function getSheets(){ const auth = new google.auth.JWT(cfg.serviceEmail, null, cfg.privateKey, ['https://www.googleapis.com/auth/spreadsheets']); return google.sheets({version:'v4', auth}); }
async function ensureSheet(sheets, title, header){
  const meta = await sheets.spreadsheets.get({spreadsheetId: cfg.sheetId});
  const exists = meta.data.sheets.some(s => s.properties.title === title);
  if (!exists) await sheets.spreadsheets.batchUpdate({spreadsheetId: cfg.sheetId, requestBody:{requests:[{addSheet:{properties:{title}}}]}});
  const res = await sheets.spreadsheets.values.get({spreadsheetId: cfg.sheetId, range:`'${title}'!1:1`}).catch(()=>({data:{values:[]}}));
  const cur = res.data.values?.[0] || [];
  if (cur.join('|') !== header.join('|')) await sheets.spreadsheets.values.update({spreadsheetId: cfg.sheetId, range:`'${title}'!1:1`, valueInputOption:'RAW', requestBody:{values:[header]}});
}
async function clearAndWrite(sheets, title, header, rows){ await ensureSheet(sheets,title,header); await sheets.spreadsheets.values.clear({spreadsheetId:cfg.sheetId, range:`'${title}'!A2:ZZ`}); if(rows.length) await sheets.spreadsheets.values.update({spreadsheetId:cfg.sheetId, range:`'${title}'!A2`, valueInputOption:'RAW', requestBody:{values:rows}}); }
async function upsertRows(sheets, title, header, rows){
  await ensureSheet(sheets,title,header);
  const existing = await sheets.spreadsheets.values.get({spreadsheetId:cfg.sheetId, range:`'${title}'!A2:A`}).catch(()=>({data:{values:[]}}));
  const keyToRow = new Map((existing.data.values||[]).map((r,i)=>[r[0], i+2]));
  const append=[];
  for (const row of rows) {
    const n = keyToRow.get(row[0]);
    if (n) await sheets.spreadsheets.values.update({spreadsheetId:cfg.sheetId, range:`'${title}'!A${n}`, valueInputOption:'RAW', requestBody:{values:[row]}});
    else append.push(row);
  }
  if (append.length) await sheets.spreadsheets.values.append({spreadsheetId:cfg.sheetId, range:`'${title}'!A1`, valueInputOption:'RAW', insertDataOption:'INSERT_ROWS', requestBody:{values:append}});
}

function buildInsights(group, summaries, allPosts){
  const rows=[]; const last=summaries.at(-1); const prev=summaries.at(-2);
  if(!last) return [[`ins_${group.id}_none`,new Date().toISOString(),'',''+group.id,group.name,'Диагностика','Высокий','Нет недельных данных','Проверьте наличие постов и период VK_DAYS_BACK','Скрипт не нашёл полные недели']];
  rows.push([`ins_${last.key}_main`,new Date().toISOString(),last.weekStart,group.id,group.name,'Главный вывод','Высокий',`За неделю: ${last.posts} постов, ${last.views} просмотров, ER по просмотрам ${last.erViews}%.`,`Держать фокус на формате/времени: ${last.recommendation}`,`Лучший пост: ${last.bestPostLink || 'нет'}`]);
  if(prev) rows.push([`ins_${last.key}_trend`,new Date().toISOString(),last.weekStart,group.id,group.name,'Тренд','Средний',`Дельта просмотров WoW: ${last.deltaViewsAbs || 'н/д'}, дельта ER reach: ${last.deltaErAbs || 'н/д'} п.п.`,`Если просмотры падают — увеличить частоту и повторить топ-формат недели.`,`Сравнение с ${prev.weekStart}`]);
  const topType = topKey(groupBy(allPosts, postType, p=>postMetrics(p).views));
  rows.push([`ins_${last.key}_content`,new Date().toISOString(),last.weekStart,group.id,group.name,'Контент','Средний',`Лучший тип контента по просмотрам: ${topType || 'не определён'}.`,`Сделать 2–3 поста в формате ${topType || 'image/text'} в лучшее время недели.`,`Анализ ${allPosts.length} постов`]);
  return rows;
}
function buildAiPosts(group, summaries, allPosts){
  const last=summaries.at(-1); const top=allPosts.slice().sort((a,b)=>postMetrics(b).erView-postMetrics(a).erView)[0];
  if(!last) return [[`aip_${group.id}_none`,new Date().toISOString(),'',''+group.id,group.name,'Нет данных','text','19:00 ЕКБ','Опубликуйте пост с новинками и вопросом в конце.','Нет постов для анализа','Напишите в комментариях, что попробовать следующим.','fallback']];
  const type = top ? postType(top) : 'image'; const hour = last.bestHour !== '' ? `${last.bestHour}:00 ЕКБ` : '19:00 ЕКБ';
  return [
    [`aip_${last.key}_1`,new Date().toISOString(),last.weekStart,group.id,group.name,'Новинки недели + вопрос','image',hour,'🔥 Новинки уже в PICK ME! Что пробуем первым? Пишите номер вкуса в комментариях 👇','Вопросы в конце помогают собирать комментарии и повышать обсуждаемость.','Напишите номер вкуса в комментариях.',`лучший тип/пост: ${type}`],
    [`aip_${last.key}_2`,new Date().toISOString(),last.weekStart,group.id,group.name,'Повторить механику топ-поста',type,`${last.bestDay || 'лучший день'} ${hour}`,`Повторяем формат, который уже набрал максимум просмотров: короткий текст + фото/видео + понятный CTA.`,'Рекомендация основана на топ-посте недели.','Сохраняйте пост и приходите сегодня.',last.bestPostLink || 'нет топ-поста'],
    [`aip_${last.key}_3`,new Date().toISOString(),last.weekStart,group.id,group.name,'Опрос вкусов','text/image','20:00 ЕКБ','Помогите выбрать следующую поставку 👇 1) острое 2) сладкое 3) напитки 4) лапша. Что берём?','Опросы дают комментарии, а комментарии сильнее влияют на индекс вовлеченности.','Выберите вариант в комментариях.','AI rule based']
  ];
}
function buildAbTests(group, summaries, posts){
  const week = summaries.at(-1)?.weekStart || '';
  const tests=[];
  const mk=(name, aName, aPosts, bName, bPosts)=>{ const a=avg(aPosts.map(p=>Number(postMetrics(p).erView)||0)); const b=avg(bPosts.map(p=>Number(postMetrics(p).erView)||0)); const diff=round(a-b,4); const winner = aPosts.length && bPosts.length ? (diff>=0?'A':'B') : 'недостаточно данных'; tests.push([`abt_${group.id}_${name}_${week}`,new Date().toISOString(),week,group.id,group.name,name,aName,aPosts.length,round(a,4),bName,bPosts.length,round(b,4),winner,diff, winner==='недостаточно данных'?'Нужно больше постов в обеих группах.':`Победил вариант ${winner}.`, aPosts.length && bPosts.length ? 'активен':'диагностика']); };
  mk('Вопрос в тексте повышает ER?', 'С вопросом', posts.filter(p=>(p.text||'').includes('?')), 'Без вопроса', posts.filter(p=>!(p.text||'').includes('?')));
  mk('Короткие посты эффективнее длинных?', 'До 180 символов', posts.filter(p=>(p.text||'').length<=180), 'Больше 180 символов', posts.filter(p=>(p.text||'').length>180));
  mk('Вечер лучше дня?', '18–23 ЕКБ', posts.filter(p=>ekbHour(p.date)>=18), '0–17 ЕКБ', posts.filter(p=>ekbHour(p.date)<18));
  return tests;
}
function buildGrowth(group, summaries){
  const last=summaries.at(-1); if(!last) return [[`gr_${group.id}_none`,new Date().toISOString(),'',''+group.id,group.name,'','','','','','','','Низкая','Нет недельных данных','Нет базы для прогноза']];
  const deltas=summaries.map(s=>Number(s.netGrowth)).filter(Number.isFinite);
  let avgGrowth = deltas.length ? avg(deltas) : avg(summaries.map(s=>Number(s.deltaSubsAbs)).filter(Number.isFinite));
  if(!Number.isFinite(avgGrowth)) avgGrowth = 0;
  const cur=Number(last.subscribers)||Number(group.members_count)||0;
  const low=Math.max(0, avgGrowth*0.5), mid=avgGrowth, high=avgGrowth*1.5;
  const confidence = deltas.length>=4 ? 'Средняя' : 'Низкая';
  return [[`gr_${last.key}`,new Date().toISOString(),last.weekStart,group.id,group.name,cur,round(avgGrowth,2),round(cur+low*4),round(cur+mid*4),round(cur+high*4),round(cur+low*12),round(cur+mid*12),round(cur+high*12),confidence,`На основе ${summaries.length} недель и доступных приростов`,deltas.length?'':'прирост оценочный/нет stats.get']];
}
function buildClipsRows(group, posts){
  const clips=posts.filter(isClip);
  if(!clips.length) return [[`clip_${group.id}_none`,new Date().toISOString(),'',''+group.id,group.name,'','','','','','','','','','','','','','','Клипы/шортсы не найдены среди wall-постов или VK не отдаёт признак клипа.']];
  return clips.map(p=>{ const m=postMetrics(p); const w=mondayStartUTC(new Date(p.date*1000)); return [`clip_${group.id}_${p.id}`,new Date().toISOString(),isoDate(w),group.id,group.name,p.id,link(group,p),ekbDate(p.date),videoSeconds(p),m.views,m.likes,m.comments,m.reposts,m.engagement,m.erView,m.viral,m.discussion,m.likeable,m.index,(p.text||'').replace(/\s+/g,' ').slice(0,500),'']; });
}
function buildAudienceRows(group, weeks, statsByWeek){
  const rows=[];
  for(const week of weeks){ const st=statsByWeek.get(isoDate(week.start)); if(st?.audienceRows?.length){ for(const r of st.audienceRows) rows.push([`aud_${group.id}_${isoDate(week.start)}_${r.segment}_${r.value}`,new Date().toISOString(),isoDate(week.start),isoDate(week.end),group.id,group.name,r.segment,r.value,r.count,r.source,'']); }
    else rows.push([`aud_${group.id}_${isoDate(week.start)}_none`,new Date().toISOString(),isoDate(week.start),isoDate(week.end),group.id,group.name,'Данные недоступны','VK API не отдал демографию','','stats.get',st?.warning || 'нет sex_age/cities/countries/devices в ответе stats.get']); }
  }
  return rows;
}

async function main(){
  console.log(`VK metrics sync v9.2 final fix: групп=${cfg.vkGroups.length}`);
  const sheets = await getSheets();
  const allSummaryRows=[], allPostRows=[], allDashboard=[], allInsights=[], allAiPosts=[], allAb=[], allGrowth=[], allClips=[], allAudience=[];
  for (const raw of cfg.vkGroups) {
    console.log(`Получение группы: ${raw}`);
    const group = await fetchGroup(raw);
    console.log(`Группа: ${group.name} (${group.id}), сейчас подписчиков=${group.members_count}`);
    const allPosts = await fetchWallPosts(group.id);
    const weeks = buildWeeks(allPosts);
    console.log(`${group.name}: постов найдено=${allPosts.length}, недель=${weeks.length}`);
    const summaries=[]; const statsByWeek=new Map();
    for (const week of weeks) {
      const wp = postsForWeek(allPosts, week);
      const stats = await fetchStats(group.id, week);
      statsByWeek.set(isoDate(week.start), stats);
      const s = summarizeWeek(group, week, wp, stats, summaries.at(-1));
      summaries.push(s);
      allSummaryRows.push(summaryToRow(s));
      allDashboard.push(dashboardRow(s));
      for (const p of wp) allPostRows.push(postToRow(group, week, p));
    }
    allInsights.push(...buildInsights(group, summaries, allPosts));
    allAiPosts.push(...buildAiPosts(group, summaries, allPosts));
    allAb.push(...buildAbTests(group, summaries, allPosts));
    allGrowth.push(...buildGrowth(group, summaries));
    allClips.push(...buildClipsRows(group, allPosts));
    allAudience.push(...buildAudienceRows(group, weeks, statsByWeek));
  }
  await upsertRows(sheets, cfg.sheets.summary, HEADERS.summary, allSummaryRows);
  await clearAndWrite(sheets, cfg.sheets.posts, HEADERS.posts, allPostRows.length ? allPostRows : [['none',new Date().toISOString(),'','','','','','','','','','','','','','','','','','','','','','','','','Посты не найдены']]);
  await clearAndWrite(sheets, cfg.sheets.dashboard, HEADERS.dashboard, allDashboard);
  await clearAndWrite(sheets, cfg.sheets.insights, HEADERS.insights, allInsights);
  await clearAndWrite(sheets, cfg.sheets.aiPosts, HEADERS.aiPosts, allAiPosts);
  await clearAndWrite(sheets, cfg.sheets.abTests, HEADERS.abTests, allAb);
  await clearAndWrite(sheets, cfg.sheets.growth, HEADERS.growth, allGrowth);
  await clearAndWrite(sheets, cfg.sheets.clips, HEADERS.clips, allClips);
  await clearAndWrite(sheets, cfg.sheets.audience, HEADERS.audience, allAudience);
  console.log('Синхронизация v9.2 успешно завершена.');
}

main().catch(err => { console.error('Сбой синхронизации v9.2.'); console.error(err); process.exit(1); });
