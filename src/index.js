import axios from 'axios';
import dotenv from 'dotenv';
import { google } from 'googleapis';

dotenv.config();

const cfg = {
  vkToken: must('VK_TOKEN'),
  groups: must('VK_GROUPS').split(',').map(s => normalizeGroupId(s)).filter(Boolean),
  daysBack: intEnv('VK_DAYS_BACK', 180),
  backfillFrom: process.env.VK_BACKFILL_FROM || '',
  enableStats: boolEnv('VK_ENABLE_STATS', true),
  requestDelayMs: intEnv('VK_REQUEST_DELAY_MS', 900),
  statsDelayMs: intEnv('VK_STATS_REQUEST_DELAY_MS', 1600),
  retryMax: intEnv('VK_RETRY_MAX', 8),
  recalcWeeks: intEnv('VK_RECALC_PREVIOUS_WEEKS', 4),
  tz: process.env.REPORT_TIMEZONE || 'Asia/Yekaterinburg',
  tzLabel: process.env.REPORT_TIMEZONE_LABEL || 'ЕКБ',
  spreadsheetId: must('GOOGLE_SHEETS_SPREADSHEET_ID'),
  serviceEmail: must('GOOGLE_SERVICE_ACCOUNT_EMAIL'),
  privateKey: must('GOOGLE_PRIVATE_KEY').replace(/\\n/g, '\n'),
  summarySheet: process.env.GOOGLE_SUMMARY_SHEET_NAME || 'weekly_summary',
  postsSheet: process.env.GOOGLE_POSTS_SHEET_NAME || 'posts_raw_vk',
  dashboardSheet: process.env.GOOGLE_DASHBOARD_SHEET_NAME || 'dashboard_data',
  recommendationsSheet: process.env.GOOGLE_RECOMMENDATIONS_SHEET_NAME || 'ai_recommendations',
  insightsSheet: process.env.GOOGLE_INSIGHTS_SHEET_NAME || 'ai_insights',
};

const SUMMARY_HEADERS = [
  'Ключ','Дата сбора','Неделя с','Неделя по','ID группы','Короткое имя группы','Название группы',
  'Подписчики на конец недели','Публикации','Просмотры постов','Лайки','Комментарии','Репосты','Вовлеченность',
  'ER по подписчикам, %','ER по просмотрам, %','ER по охвату, %','Доля охвата подписчиков к подписчикам, %',
  'Посетители сообщества','Просмотры/показы сообщества','Охват всего','Охват подписчиков','Охват мобильный',
  'Подписались','Отписались','Чистый прирост подписчиков','Прирост подписчиков за неделю, %',
  'Дельта подписчиков WoW, абс.','Дельта подписчиков WoW, %','Дельта охвата WoW, абс.','Дельта охвата WoW, %',
  'Дельта показов WoW, абс.','Дельта показов WoW, %','Дельта ER по охвату WoW, п.п.','Дельта ER по охвату WoW, %',
  'Средние просмотры поста','Медианные просмотры поста','Лучший пост недели, ID','Лучший пост недели, ссылка','Лучший пост недели: просмотры',
  'Лучший пост по вовлеченности, ID','Лучший пост по ER, ID','Вирусность, %','Обсуждаемость, %','Лайкабельность, %','Индекс вовлеченности',
  'Вирусные посты, шт.','Вирусные посты, %','Стд. отклонение просмотров','Частота постинга, постов/день',
  'Лучший день публикации','Лучший час публикации','Лучший формат','Прогноз подписчиков на следующую неделю','Прогноз просмотров постов на следующую неделю',
  'AI рекомендация: контент','AI рекомендация: частота','AI рекомендация: время','Источник охвата','Предупреждения'
];

const POSTS_HEADERS = [
  'Ключ недели','Дата публикации ЕКБ','ID группы','Короткое имя группы','Название группы','ID поста','Ссылка',
  'Тип поста','Текст, первые 200 символов','Просмотры','Лайки','Комментарии','Репосты','Вовлеченность','ER по просмотрам, %','Час ЕКБ','День недели ЕКБ'
];

const REC_HEADERS = ['Дата сбора','ID группы','Название группы','Период','Рекомендация','Обоснование','Приоритет'];
const INSIGHT_HEADERS = ['Дата сбора','ID группы','Название группы','Период','Тренд','Вывод','Риск','Возможность'];

function must(name){ const v=process.env[name]; if(!v) throw new Error(`Не задана переменная ${name}`); return v; }
function intEnv(name, def){ const n=parseInt(process.env[name] || '',10); return Number.isFinite(n)?n:def; }
function boolEnv(name, def){ const v=process.env[name]; if(v==null) return def; return ['1','true','yes','да','on'].includes(String(v).toLowerCase()); }
function normalizeGroupId(s){ return String(s||'').trim().replace(/^https?:\/\/vk\.com\//,'').replace(/^@/,'').replace(/\/$/,''); }
function sleep(ms){ return new Promise(r=>setTimeout(r,ms)); }
function unix(d){ return Math.floor(d.getTime()/1000); }
function isoDate(d){ return d.toISOString().slice(0,10); }
function nvl(v){ return v == null || Number.isNaN(v) ? '' : v; }
function pct(num, den){ return den ? round((num/den)*100,4) : null; }
function round(n, digits=4){ if(n==null || !Number.isFinite(n)) return null; return Number(Number(n).toFixed(digits)); }
function sum(arr){ return arr.reduce((a,b)=>a+(Number(b)||0),0); }
function avg(arr){ const a=arr.filter(x=>Number.isFinite(x)); return a.length?sum(a)/a.length:null; }
function median(arr){ const a=arr.filter(x=>Number.isFinite(x)).sort((x,y)=>x-y); if(!a.length) return null; const m=Math.floor(a.length/2); return a.length%2?a[m]:(a[m-1]+a[m])/2; }
function std(arr){ const a=arr.filter(x=>Number.isFinite(x)); if(!a.length) return null; const mean=avg(a); return Math.sqrt(avg(a.map(x=>(x-mean)**2))); }
function safeDelta(cur, prev){ return cur!=null && prev!=null ? round(cur-prev,4) : null; }
function safeDeltaPct(cur, prev){ return cur!=null && prev ? round(((cur-prev)/prev)*100,4) : null; }
function weekKey(groupId, start){ return `${groupId}-${isoDate(start)}`; }
function postLink(screen, id){ return `https://vk.com/${screen}?w=wall-${screen.startsWith('club')||screen.startsWith('public')?screen.replace(/^(club|public)/,''):''}_${id}`; }
function wallPostLink(groupId, postId){ return `https://vk.com/wall-${groupId}_${postId}`; }

function fmtDateTz(date, opts={}){
  return new Intl.DateTimeFormat('ru-RU', { timeZone: cfg.tz, year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit', second:'2-digit', ...opts }).format(date);
}
function hourTz(ts){ return Number(new Intl.DateTimeFormat('ru-RU',{timeZone:cfg.tz,hour:'2-digit',hour12:false}).format(new Date(ts*1000))); }
function weekdayTz(ts){ return new Intl.DateTimeFormat('ru-RU',{timeZone:cfg.tz,weekday:'short'}).format(new Date(ts*1000)); }

function lastFullWeekUtc(){
  const now = new Date();
  const utcDay = now.getUTCDay() || 7;
  const mondayThisWeek = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - utcDay + 1, 0,0,0));
  const start = new Date(mondayThisWeek.getTime() - 7*86400000);
  const end = new Date(mondayThisWeek.getTime() - 1000);
  return {start,end};
}
function mondayUtcForDate(d){
  const day=d.getUTCDay()||7;
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()-day+1,0,0,0));
}
function buildWeeks(startDate, lastWeek){
  const weeks=[]; let cur=mondayUtcForDate(startDate);
  while(cur<=lastWeek.start){ const end=new Date(cur.getTime()+7*86400000-1000); weeks.push({start:new Date(cur),end}); cur=new Date(cur.getTime()+7*86400000); }
  return weeks;
}

async function vkApi(method, params={}, delayMs=cfg.requestDelayMs){
  let lastErr;
  for(let attempt=0; attempt<cfg.retryMax; attempt++){
    await sleep(delayMs + attempt*150);
    try{
      const {data} = await axios.get(`https://api.vk.com/method/${method}`, { params:{...params, access_token:cfg.vkToken, v:'5.199'}, timeout:30000 });
      if(data.error){
        const code=data.error.error_code;
        const msg=data.error.error_msg;
        if(code===6 || code===10){ lastErr=new Error(`VK API ${method}: [${code}] ${msg}`); await sleep(1200*(attempt+1)); continue; }
        throw new Error(`VK API ${method}: [${code}] ${msg}. Params=${JSON.stringify(params)} Raw=${JSON.stringify(data.error)}`);
      }
      return data.response;
    }catch(e){ lastErr=e; if(attempt===cfg.retryMax-1) break; await sleep(1000*(attempt+1)); }
  }
  throw lastErr;
}

async function fetchGroup(raw){
  const id=normalizeGroupId(raw);
  let response, group;
  try{ response=await vkApi('groups.getById',{group_ids:id, fields:'members_count,screen_name'}); group=Array.isArray(response)?response[0]:response?.groups?.[0]; }catch(e){ console.warn(`groups.getById group_ids не сработал для ${id}: ${e.message}`); }
  if(!group){ response=await vkApi('groups.getById',{group_id:id, fields:'members_count,screen_name'}); group=Array.isArray(response)?response[0]:response?.groups?.[0]; }
  if(!group?.id) throw new Error(`Не удалось получить группу ${id}. Ответ=${JSON.stringify(response)}`);
  return {...group, input:id, screen_name: group.screen_name || id};
}

async function fetchWallPosts(groupId){
  const all=[]; let offset=0; const count=100; const max=1000;
  while(offset<max){
    const res=await vkApi('wall.get',{owner_id:-groupId, count, offset, filter:'owner'});
    const items=res?.items || [];
    all.push(...items.filter(p=>!p.is_pinned));
    if(items.length<count) break;
    offset += count;
  }
  return all;
}

function parseStatNumber(day, paths){
  for(const path of paths){
    let v=day;
    for(const k of path.split('.')) v = v?.[k];
    if(typeof v==='number') return v;
  }
  return null;
}
async function fetchStatsWeek(groupId, week){
  if(!cfg.enableStats) return {available:false, warning:'stats.get отключён'};
  try{
    const response = await vkApi('stats.get',{group_id:groupId, timestamp_from:unix(week.start), timestamp_to:unix(week.end), interval:'day'}, cfg.statsDelayMs);
    const days = Array.isArray(response) ? response : [];
    if(!days.length) return {available:false, warning:'stats.get вернул пустой ответ'};
    const total = {visitors:0, views:0, reachTotal:0, reachSubscribers:0, reachMobile:0, subscribed:0, unsubscribed:0};
    let seen = {visitors:false, views:false, reachTotal:false, reachSubscribers:false, reachMobile:false, subscribed:false, unsubscribed:false};
    for(const d of days){
      const vals = {
        visitors: parseStatNumber(d,['visitors.visitors','visitors.count','visitors']),
        views: parseStatNumber(d,['visitors.views','views','activity.views']),
        reachTotal: parseStatNumber(d,['reach.reach','reach.total','reach_total','reach']),
        reachSubscribers: parseStatNumber(d,['reach.reach_subscribers','reach.subscribers','reach_subscribers']),
        reachMobile: parseStatNumber(d,['reach.mobile_reach','reach.mobile','reach_mobile']),
        subscribed: parseStatNumber(d,['activity.subscribed','subscribed']),
        unsubscribed: parseStatNumber(d,['activity.unsubscribed','unsubscribed'])
      };
      for(const [k,v] of Object.entries(vals)){ if(v!=null){ seen[k]=true; total[k]+=v; } }
    }
    for(const k of Object.keys(total)) if(!seen[k]) total[k]=null;
    return {available:true, ...total, warning:null};
  }catch(e){
    return {available:false, warning:`stats.get недоступен: ${e.message}`};
  }
}

function classifyPost(p){
  const a=p.attachments||[];
  if(p.copy_history?.length) return 'repost';
  if(a.some(x=>x.type==='clip')) return 'clip';
  if(a.some(x=>x.type==='video')) return 'video';
  if(a.some(x=>x.type==='photo')) return 'image';
  if((p.text||'').trim()) return 'text';
  return 'other';
}
function aggregatePosts(posts, week){
  const weekPosts = posts.filter(p=>p.date>=unix(week.start) && p.date<=unix(week.end));
  const views = weekPosts.map(p=>p.views?.count ?? 0);
  const likes=sum(weekPosts.map(p=>p.likes?.count ?? 0));
  const comments=sum(weekPosts.map(p=>p.comments?.count ?? 0));
  const reposts=sum(weekPosts.map(p=>p.reposts?.count ?? 0));
  const postViews=sum(views);
  const engagement=likes+comments+reposts;
  const bestByViews=[...weekPosts].sort((a,b)=>(b.views?.count??0)-(a.views?.count??0))[0] || null;
  const bestByEng=[...weekPosts].sort((a,b)=>((b.likes?.count??0)+(b.comments?.count??0)+(b.reposts?.count??0))-((a.likes?.count??0)+(a.comments?.count??0)+(a.reposts?.count??0)))[0] || null;
  const bestByEr=[...weekPosts].sort((a,b)=>pct((b.likes?.count??0)+(b.comments?.count??0)+(b.reposts?.count??0), b.views?.count??0)-pct((a.likes?.count??0)+(a.comments?.count??0)+(a.reposts?.count??0), a.views?.count??0))[0] || null;
  const med=median(views); const viralCount = med ? weekPosts.filter(p=>(p.views?.count??0)>med*2).length : 0;
  const byHour={}; const byDay={}; const byType={};
  for(const p of weekPosts){
    const v=p.views?.count??0; const e=(p.likes?.count??0)+(p.comments?.count??0)+(p.reposts?.count??0); const h=hourTz(p.date); const d=weekdayTz(p.date); const t=classifyPost(p);
    byHour[h]=(byHour[h]||{views:0,eng:0,count:0}); byHour[h].views+=v; byHour[h].eng+=e; byHour[h].count++;
    byDay[d]=(byDay[d]||{views:0,eng:0,count:0}); byDay[d].views+=v; byDay[d].eng+=e; byDay[d].count++;
    byType[t]=(byType[t]||{views:0,eng:0,count:0}); byType[t].views+=v; byType[t].eng+=e; byType[t].count++;
  }
  const bestHour = bestBucket(byHour); const bestDay = bestBucket(byDay); const bestType = bestBucket(byType);
  return {weekPosts, count:weekPosts.length, postViews, likes, comments, reposts, engagement, avgViews:avg(views), medianViews:med, stdViews:std(views), bestByViews, bestByEng, bestByEr, viralCount, viralPct:pct(viralCount, weekPosts.length), bestHour, bestDay, bestType, byType};
}
function bestBucket(obj){
  return Object.entries(obj).sort((a,b)=>((b[1].views/(b[1].count||1))- (a[1].views/(a[1].count||1))))[0]?.[0] ?? '';
}

async function sheetsClient(){
  const auth = new google.auth.JWT(cfg.serviceEmail, undefined, cfg.privateKey, ['https://www.googleapis.com/auth/spreadsheets']);
  return google.sheets({version:'v4', auth});
}
async function ensureSheet(sheets, title){
  const meta=await sheets.spreadsheets.get({spreadsheetId:cfg.spreadsheetId});
  if(!meta.data.sheets.some(s=>s.properties.title===title)){
    await sheets.spreadsheets.batchUpdate({spreadsheetId:cfg.spreadsheetId, requestBody:{requests:[{addSheet:{properties:{title}}}]}});
  }
}
async function writeSheet(title, headers, rows){
  const sheets=await sheetsClient(); await ensureSheet(sheets,title);
  await sheets.spreadsheets.values.clear({spreadsheetId:cfg.spreadsheetId, range:`${title}!A:ZZ`});
  await sheets.spreadsheets.values.update({spreadsheetId:cfg.spreadsheetId, range:`${title}!A1`, valueInputOption:'USER_ENTERED', requestBody:{values:[headers, ...rows]}});
}
async function readExisting(title){
  try{
    const sheets=await sheetsClient();
    const res=await sheets.spreadsheets.values.get({spreadsheetId:cfg.spreadsheetId, range:`${title}!A1:ZZ`});
    const values=res.data.values||[]; if(values.length<2) return [];
    const headers=values[0]; return values.slice(1).map(r=>Object.fromEntries(headers.map((h,i)=>[h,r[i]??''])));
  }catch{ return []; }
}

function buildPostRows(group, weekKeyStr, ag){
  return ag.weekPosts.map(p=>{
    const e=(p.likes?.count??0)+(p.comments?.count??0)+(p.reposts?.count??0);
    return [weekKeyStr, fmtDateTz(new Date(p.date*1000)), group.id, group.screen_name, group.name, p.id, wallPostLink(group.id,p.id), classifyPost(p), (p.text||'').replace(/\s+/g,' ').slice(0,200), p.views?.count??0, p.likes?.count??0, p.comments?.count??0, p.reposts?.count??0, e, nvl(pct(e,p.views?.count??0)), hourTz(p.date), weekdayTz(p.date)];
  });
}

function generateRecommendations(group, weekRows, lastAgg){
  const rows=[]; const now=fmtDateTz(new Date()); const period=weekRows.length?`${weekRows[0]['Неделя с']}..${weekRows.at(-1)['Неделя по']}`:'';
  const latest=weekRows.at(-1)||{};
  const bestType=latest['Лучший формат'] || lastAgg.bestType || '';
  const bestTime=[latest['Лучший день публикации'], latest['Лучший час публикации']].filter(Boolean).join(' ');
  if(bestType) rows.push([now, group.id, group.name, period, `Усилить формат: ${bestType}`, `Формат ${bestType} показал лучший средний результат по просмотрам/вовлечению за последние обработанные недели.`, 'Высокий']);
  if(bestTime) rows.push([now, group.id, group.name, period, `Публиковать чаще в лучшие окна: ${bestTime} (${cfg.tzLabel})`, 'Рекомендация построена по фактическим просмотрам постов с учётом часового пояса отчёта.', 'Высокий']);
  const er = num(latest['ER по просмотрам, %']);
  const posts = num(latest['Публикации']);
  if(posts!=null && posts<10) rows.push([now, group.id, group.name, period, 'Проверить увеличение частоты публикаций', `За последнюю неделю ${posts} публикаций. Можно протестировать +2–3 поста/неделю без резкого изменения формата.`, 'Средний']);
  if(er!=null && er<3) rows.push([now, group.id, group.name, period, 'Добавить механики вовлечения', `ER по просмотрам ${er}%. Добавить вопросы, выборы, просьбы комментировать, подборки и сравнения.`, 'Высокий']);
  if((latest['Предупреждения']||'').includes('stats.get')) rows.push([now, group.id, group.name, period, 'Не опираться на охваты как на точную метрику', 'VK не отдал stats.get полностью; охват/показы могут быть fallback от просмотров постов. Для управленческих решений использовать ER view и просмотры постов.', 'Высокий']);
  return rows;
}
function generateInsights(group, weekRows){
  const now=fmtDateTz(new Date()); const period=weekRows.length?`${weekRows[0]['Неделя с']}..${weekRows.at(-1)['Неделя по']}`:'';
  const last=weekRows.at(-1)||{}; const prev=weekRows.at(-2)||{};
  const v=num(last['Просмотры постов']); const pv=num(prev['Просмотры постов']);
  const er=num(last['ER по просмотрам, %']); const per=num(prev['ER по просмотрам, %']);
  let trend='недостаточно данных'; let conclusion='Нужно накопить больше недель.';
  if(v!=null && pv!=null){ const d=v-pv; trend=d>0?'рост просмотров':d<0?'падение просмотров':'стабильность'; conclusion=`Просмотры WoW: ${d>0?'+':''}${d}. ER view: ${er??'н/д'}%.`; }
  const risk = (er!=null && per!=null && er<per) ? 'Падает ER по просмотрам: аудитория реагирует слабее.' : 'Критичных рисков по ER не выявлено.';
  const opportunity = last['Лучший формат'] ? `Масштабировать формат ${last['Лучший формат']} и время ${last['Лучший час публикации']} (${cfg.tzLabel}).` : 'Накопить данные по форматам и времени.';
  return [[now, group.id, group.name, period, trend, conclusion, risk, opportunity]];
}
function num(v){ if(v===''||v==null) return null; const n=Number(String(v).replace(',','.')); return Number.isFinite(n)?n:null; }

async function main(){
  const {start:lastStart,end:lastEnd}=lastFullWeekUtc();
  console.log(`VK metrics sync v8.2 started. TZ=${cfg.tz}. Последняя полная неделя: ${isoDate(lastStart)}..${isoDate(lastEnd)}`);
  const existingSummary = await readExisting(cfg.summarySheet);
  const existingMap = new Map(existingSummary.map(r=>[r['Ключ'], r]));
  let allSummaryRows=[]; let allPostRows=[]; let allRecRows=[]; let allInsightRows=[];

  for(const rawGroup of cfg.groups){
    const group=await fetchGroup(rawGroup);
    console.log(`Группа: ${group.name} (${group.id}), текущие подписчики=${group.members_count}`);
    const posts=await fetchWallPosts(group.id);
    const earliestPostDate = posts.length ? new Date(Math.min(...posts.map(p=>p.date))*1000) : new Date(Date.now()-cfg.daysBack*86400000);
    const backfillDate = cfg.backfillFrom ? new Date(`${cfg.backfillFrom}T00:00:00Z`) : new Date(Math.max(Date.now()-cfg.daysBack*86400000, earliestPostDate.getTime()));
    const weeks=buildWeeks(backfillDate,{start:lastStart,end:lastEnd});
    console.log(`${group.name}: постов загружено=${posts.length}, недель=${weeks.length}`);

    const tmpRows=[];
    for(const week of weeks){
      const key=weekKey(group.id, week.start);
      const ag=aggregatePosts(posts,week);
      const stat=await fetchStatsWeek(group.id, week);
      const warnings=[];
      if(!stat.available) warnings.push(stat.warning || 'stats.get недоступен');

      const statsNet = stat.available && (stat.subscribed!=null || stat.unsubscribed!=null) ? (stat.subscribed||0)-(stat.unsubscribed||0) : null;
      let subscribersEnd=null;
      if(existingMap.has(key) && existingMap.get(key)['Подписчики на конец недели']) subscribersEnd=num(existingMap.get(key)['Подписчики на конец недели']);
      else warnings.push('исторические подписчики не восстановлены: нет сохранённого снимка и/или подписок/отписок из stats.get');

      let communityViews = stat.views;
      let reachTotal = stat.reachTotal;
      let reachSource = 'stats.get';
      if(reachTotal==null && ag.postViews>0){ reachTotal=ag.postViews; reachSource='fallback: просмотры постов'; warnings.push('охват всего рассчитан через просмотры постов, не точный reach VK'); }
      if(communityViews==null && ag.postViews>0){ communityViews=ag.postViews; warnings.push('показы сообщества заменены просмотрами постов, потому что stats.get не отдал показы'); }

      const erSubs = subscribersEnd ? pct(ag.engagement, subscribersEnd) : null;
      const erViews = pct(ag.engagement, ag.postViews);
      const erReach = pct(ag.engagement, reachTotal);
      const reachSubShare = stat.reachSubscribers!=null && subscribersEnd ? pct(stat.reachSubscribers, subscribersEnd) : null;
      const bestViews = ag.bestByViews?.views?.count ?? null;
      const bestViewsId = ag.bestByViews?.id ?? '';
      const bestEngId = ag.bestByEng?.id ?? '';
      const bestErId = ag.bestByEr?.id ?? '';

      const rowObj = Object.fromEntries(SUMMARY_HEADERS.map(h=>[h,'']));
      Object.assign(rowObj, {
        'Ключ': key, 'Дата сбора': fmtDateTz(new Date()), 'Неделя с': isoDate(week.start), 'Неделя по': isoDate(week.end),
        'ID группы': group.id, 'Короткое имя группы': group.screen_name, 'Название группы': group.name,
        'Подписчики на конец недели': nvl(subscribersEnd), 'Публикации': ag.count, 'Просмотры постов': ag.postViews,
        'Лайки': ag.likes, 'Комментарии': ag.comments, 'Репосты': ag.reposts, 'Вовлеченность': ag.engagement,
        'ER по подписчикам, %': nvl(erSubs), 'ER по просмотрам, %': nvl(erViews), 'ER по охвату, %': nvl(erReach),
        'Доля охвата подписчиков к подписчикам, %': nvl(reachSubShare),
        'Посетители сообщества': nvl(stat.visitors), 'Просмотры/показы сообщества': nvl(communityViews),
        'Охват всего': nvl(reachTotal), 'Охват подписчиков': nvl(stat.reachSubscribers), 'Охват мобильный': nvl(stat.reachMobile),
        'Подписались': nvl(stat.subscribed), 'Отписались': nvl(stat.unsubscribed), 'Чистый прирост подписчиков': nvl(statsNet),
        'Прирост подписчиков за неделю, %': '',
        'Средние просмотры поста': nvl(round(ag.avgViews,2)), 'Медианные просмотры поста': nvl(round(ag.medianViews,2)),
        'Лучший пост недели, ID': bestViewsId, 'Лучший пост недели, ссылка': bestViewsId ? wallPostLink(group.id,bestViewsId) : '', 'Лучший пост недели: просмотры': nvl(bestViews),
        'Лучший пост по вовлеченности, ID': bestEngId, 'Лучший пост по ER, ID': bestErId,
        'Вирусность, %': nvl(pct(ag.reposts, ag.postViews)), 'Обсуждаемость, %': nvl(pct(ag.comments, ag.postViews)),
        'Лайкабельность, %': nvl(pct(ag.likes, ag.postViews)), 'Индекс вовлеченности': nvl(pct(ag.likes + 2*ag.comments + 3*ag.reposts, ag.postViews)),
        'Вирусные посты, шт.': ag.viralCount, 'Вирусные посты, %': nvl(ag.viralPct), 'Стд. отклонение просмотров': nvl(round(ag.stdViews,2)),
        'Частота постинга, постов/день': round(ag.count/7,3), 'Лучший день публикации': ag.bestDay, 'Лучший час публикации': ag.bestHour,
        'Лучший формат': ag.bestType, 'Источник охвата': reachSource, 'Предупреждения': [...new Set(warnings)].join('; ')
      });
      tmpRows.push(rowObj);
      allPostRows.push(...buildPostRows(group,key,ag));
    }

    // If stats contains net growth, reconstruct subscribers for weeks where possible from current count backwards.
    let knownCurrent = Number(group.members_count) || null;
    let futureDelta = 0;
    for(let i=tmpRows.length-1; i>=0; i--){
      const r=tmpRows[i]; const net=num(r['Чистый прирост подписчиков']);
      if(r['Подписчики на конец недели']==='' && knownCurrent!=null && net!=null){
        const estimated = knownCurrent - futureDelta;
        r['Подписчики на конец недели'] = estimated;
        r['Предупреждения'] = appendWarn(r['Предупреждения'], 'подписчики восстановлены приблизительно от текущего значения и net growth stats.get');
      }
      if(net!=null) futureDelta += net;
    }

    // Deltas and forecasts.
    for(let i=0;i<tmpRows.length;i++){
      const r=tmpRows[i], p=tmpRows[i-1];
      const subs=num(r['Подписчики на конец недели']), prevSubs=num(p?.['Подписчики на конец недели']);
      const net=num(r['Чистый прирост подписчиков']);
      if(net!=null && prevSubs) r['Прирост подписчиков за неделю, %']=nvl(pct(net, prevSubs));
      r['Дельта подписчиков WoW, абс.']=nvl(safeDelta(subs,prevSubs));
      r['Дельта подписчиков WoW, %']=nvl(safeDeltaPct(subs,prevSubs));
      r['Дельта охвата WoW, абс.']=nvl(safeDelta(num(r['Охват всего']),num(p?.['Охват всего'])));
      r['Дельта охвата WoW, %']=nvl(safeDeltaPct(num(r['Охват всего']),num(p?.['Охват всего'])));
      r['Дельта показов WoW, абс.']=nvl(safeDelta(num(r['Просмотры/показы сообщества']),num(p?.['Просмотры/показы сообщества'])));
      r['Дельта показов WoW, %']=nvl(safeDeltaPct(num(r['Просмотры/показы сообщества']),num(p?.['Просмотры/показы сообщества'])));
      r['Дельта ER по охвату WoW, п.п.']=nvl(safeDelta(num(r['ER по охвату, %']),num(p?.['ER по охвату, %'])));
      r['Дельта ER по охвату WoW, %']=nvl(safeDeltaPct(num(r['ER по охвату, %']),num(p?.['ER по охвату, %'])));
      const recent=tmpRows.slice(Math.max(0,i-3),i+1);
      r['Прогноз просмотров постов на следующую неделю']=nvl(round(avg(recent.map(x=>num(x['Просмотры постов'])).filter(x=>x!=null)),0));
      const subSeries=recent.map(x=>num(x['Подписчики на конец недели'])).filter(x=>x!=null);
      r['Прогноз подписчиков на следующую неделю']=subSeries.length? nvl(round(subSeries.at(-1)+(avg(subSeries.slice(1).map((x,j)=>x-subSeries[j]))||0),0)) : '';
      r['AI рекомендация: контент']=recommendContent(r);
      r['AI рекомендация: частота']=recommendFrequency(r);
      r['AI рекомендация: время']=recommendTime(r);
    }

    allSummaryRows.push(...tmpRows.map(obj=>SUMMARY_HEADERS.map(h=>nvl(obj[h]))));
    allRecRows.push(...generateRecommendations(group,tmpRows,tmpRows.at(-1)||{}));
    allInsightRows.push(...generateInsights(group,tmpRows));
  }

  allSummaryRows.sort((a,b)=>String(a[4]).localeCompare(String(b[4])) || String(a[2]).localeCompare(String(b[2])));
  await writeSheet(cfg.summarySheet, SUMMARY_HEADERS, allSummaryRows);
  await writeSheet(cfg.postsSheet, POSTS_HEADERS, allPostRows);
  await writeSheet(cfg.dashboardSheet, SUMMARY_HEADERS.filter(h=>!['Предупреждения'].includes(h)), allSummaryRows.map(r=>r.slice(0, SUMMARY_HEADERS.length-1)));
  await writeSheet(cfg.recommendationsSheet, REC_HEADERS, allRecRows);
  await writeSheet(cfg.insightsSheet, INSIGHT_HEADERS, allInsightRows);
  console.log('Синхронизация v8.2 завершена успешно.');
}
function appendWarn(a,b){ return [a,b].filter(Boolean).join('; '); }
function recommendContent(r){ const f=r['Лучший формат']; const er=num(r['ER по просмотрам, %']); if(f && er!=null) return `Усилить ${f}: лучший формат недели, ER view ${er}%`; if(f) return `Тестировать больше ${f}`; return 'Накопить данные по форматам'; }
function recommendFrequency(r){ const freq=num(r['Частота постинга, постов/день']); if(freq==null) return ''; if(freq<1.5) return 'Плавно увеличить частоту: +2–3 поста в неделю'; if(freq>4) return 'Проверить усталость аудитории: высокая частота'; return 'Частота выглядит сбалансированной'; }
function recommendTime(r){ const h=r['Лучший час публикации']; const d=r['Лучший день публикации']; return h!=='' ? `Публиковать в ${d || 'лучшие дни'} около ${h}:00 (${cfg.tzLabel})` : 'Накопить данные по времени публикаций'; }

main().catch(e=>{ console.error('Сбой синхронизации v8.2.'); console.error(e?.stack || e?.message || e); process.exit(1); });
