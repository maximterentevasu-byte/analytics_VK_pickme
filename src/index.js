import axios from 'axios';
import dotenv from 'dotenv';
import { google } from 'googleapis';

dotenv.config();

const CFG = {
  vkToken: must('VK_TOKEN'),
  groups: must('VK_GROUPS').split(',').map(s => cleanGroupId(s)).filter(Boolean),
  enableStats: bool(process.env.VK_ENABLE_STATS, true),
  daysBack: int(process.env.VK_DAYS_BACK, 365),
  backfillFrom: process.env.VK_BACKFILL_FROM || '',
  recalcWeeks: int(process.env.VK_RECALC_PREVIOUS_WEEKS, 8),
  wallMaxPosts: int(process.env.VK_WALL_MAX_POSTS, 500),
  reqDelay: int(process.env.VK_REQUEST_DELAY_MS, 900),
  statsDelay: int(process.env.VK_STATS_REQUEST_DELAY_MS, 1600),
  retryMax: int(process.env.VK_RETRY_MAX, 8),
  viralThreshold: num(process.env.VK_VIRAL_THRESHOLD, 2),
  weights: parseWeights(process.env.VK_ENGAGEMENT_WEIGHTS || '1,2,3'),
  sheetId: must('GOOGLE_SHEETS_SPREADSHEET_ID'),
  serviceEmail: must('GOOGLE_SERVICE_ACCOUNT_EMAIL'),
  privateKey: must('GOOGLE_PRIVATE_KEY').replace(/\\n/g, '\n').replace(/^"|"$/g, ''),
  summarySheet: process.env.GOOGLE_SUMMARY_SHEET_NAME || 'weekly_summary',
  postsSheet: process.env.GOOGLE_POSTS_SHEET_NAME || 'posts_raw_vk',
  dashboardSheet: process.env.GOOGLE_DASHBOARD_SHEET_NAME || 'dashboard_data',
  recSheet: process.env.GOOGLE_RECOMMENDATIONS_SHEET_NAME || 'ai_recommendations'
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
  'Вирусные посты','Вирусные посты, %','Стд. отклонение просмотров','Постов в день','Лучший день недели','Лучший час публикации',
  'Лучший формат контента','Прогноз подписчиков через 4 недели','Прогноз просмотров следующей недели','AI рекомендация: что постить','AI рекомендация: когда постить','AI рекомендация: фокус',
  'Источник охвата','Предупреждения'
];

const POSTS_HEADERS = ['Ключ недели','Дата поста','ID группы','Название группы','ID поста','Ссылка','Тип поста','День недели','Час','Текст','Просмотры','Лайки','Комментарии','Репосты','Вовлеченность','ER по просмотрам, %','Вирусность, %','Обсуждаемость, %','Лайкабельность, %','Индекс вовлеченности','Вирусный пост'];
const DASH_HEADERS = ['Неделя с','Группа','Подписчики','Публикации','Просмотры постов','Вовлеченность','ER по подписчикам, %','ER по просмотрам, %','ER по охвату, %','Охват всего','Охват подписчиков','Доля охвата подписчиков к подписчикам, %','Вирусность, %','Обсуждаемость, %','Лайкабельность, %','Индекс вовлеченности','Лучший формат контента','Лучший день недели','Лучший час публикации','AI рекомендация: что постить','AI рекомендация: когда постить','Предупреждения'];
const REC_HEADERS = ['Дата сбора','ID группы','Название группы','Период анализа','Рекомендация','Основание','Приоритет'];

function must(name){ const v=process.env[name]; if(!v) throw new Error(`Нет переменной окружения ${name}`); return v; }
function int(v,d){ const n=parseInt(v,10); return Number.isFinite(n)?n:d; }
function num(v,d){ const n=Number(v); return Number.isFinite(n)?n:d; }
function bool(v,d){ if(v===undefined) return d; return ['1','true','yes','on'].includes(String(v).toLowerCase()); }
function parseWeights(s){ const [l,c,r]=s.split(',').map(Number); return {like:l||1, comment:c||2, repost:r||3}; }
function cleanGroupId(s){ return String(s||'').trim().replace(/^https?:\/\/vk\.com\//,'').replace(/^@/,'').replace(/\/$/,''); }
const sleep = ms => new Promise(r => setTimeout(r, ms));
const iso = d => d.toISOString().slice(0,10);
const unix = d => Math.floor(d.getTime()/1000);
function monday(d){ const x=new Date(Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate())); const day=x.getUTCDay() || 7; x.setUTCDate(x.getUTCDate()-day+1); x.setUTCHours(0,0,0,0); return x; }
function weekEnd(start){ const d=new Date(start); d.setUTCDate(d.getUTCDate()+6); d.setUTCHours(23,59,59,999); return d; }
function pct(a,b){ return b ? round((a/b)*100,4) : ''; }
function round(n,d=2){ return Number.isFinite(n) ? Number(n.toFixed(d)) : ''; }
function sum(a){ return a.reduce((x,y)=>x+(Number(y)||0),0); }
function avg(a){ return a.length ? sum(a)/a.length : 0; }
function median(a){ if(!a.length) return 0; const s=[...a].sort((x,y)=>x-y); const m=Math.floor(s.length/2); return s.length%2?s[m]:(s[m-1]+s[m])/2; }
function std(a){ if(!a.length) return 0; const av=avg(a); return Math.sqrt(avg(a.map(x=>(x-av)**2))); }
function dayRu(date){ return ['Вс','Пн','Вт','Ср','Чт','Пт','Сб'][date.getUTCDay()]; }
function postLink(g,p){ return `https://vk.com/${g.screen_name || 'club'+g.id}?w=wall-${g.id}_${p.id}`; }

async function vkApi(method, params={}, delay=CFG.reqDelay){
  await sleep(delay);
  let last;
  for(let i=0;i<CFG.retryMax;i++){
    try{
      const {data}=await axios.get(`https://api.vk.com/method/${method}`,{params:{...params, access_token:CFG.vkToken, v:'5.199'}, timeout:30000});
      if(data.error){
        last = data.error;
        if([6,9,10,29].includes(data.error.error_code)){ await sleep(1000*(i+1)); continue; }
        throw new Error(`Ошибка VK API в ${method}: [${data.error.error_code}] ${data.error.error_msg}. Params=${JSON.stringify(params)}`);
      }
      return data.response;
    }catch(e){ last=e; if(i===CFG.retryMax-1) break; await sleep(1000*(i+1)); }
  }
  throw new Error(`Ошибка VK API в ${method}: ${last?.error_msg || last?.message || JSON.stringify(last)}`);
}

async function fetchGroup(id){
  let response;
  for(const params of [{group_ids:id, fields:'members_count,screen_name'}, {group_id:id, fields:'members_count,screen_name'}]){
    try{
      response=await vkApi('groups.getById', params);
      const group = Array.isArray(response) ? response[0] : response?.groups?.[0];
      if(group?.id) return group;
    }catch(e){ console.warn(`groups.getById попытка не удалась: ${e.message}`); }
  }
  throw new Error(`Не удалось получить группу ${id}. Последний ответ: ${JSON.stringify(response)}`);
}

async function fetchPosts(group){
  const owner_id = -Number(group.id);
  const posts=[]; let offset=0; const page=100;
  while(posts.length < CFG.wallMaxPosts){
    const res = await vkApi('wall.get',{owner_id, count:Math.min(page, CFG.wallMaxPosts-posts.length), offset});
    const items=(res.items||[]).filter(p=>!p.is_pinned && p.date);
    posts.push(...items);
    if(!res.items || res.items.length<page) break;
    offset += page;
  }
  return posts;
}

async function fetchStats(groupId, ws, we){
  if(!CFG.enableStats) return {stats:null, warnings:['stats.get отключён']};
  try{
    const res = await vkApi('stats.get',{group_id:groupId, timestamp_from:unix(ws), timestamp_to:unix(we), interval:'day'}, CFG.statsDelay);
    const days = Array.isArray(res) ? res : [];
    if(!days.length) return {stats:null, warnings:['stats.get вернул пустой ответ']};
    const stats = {visitors:0, views:0, reachTotal:0, reachSubscribers:0, reachMobile:0, subscribed:0, unsubscribed:0};
    for(const d of days){
      stats.visitors += Number(d.visitors || d.activity?.visitors || 0);
      stats.views += Number(d.views || d.activity?.views || 0);
      stats.reachTotal += Number(d.reach?.reach || d.reach?.total || 0);
      stats.reachSubscribers += Number(d.reach?.reach_subscribers || d.reach?.subscribers || 0);
      stats.reachMobile += Number(d.reach?.mobile_reach || d.reach?.mobile || 0);
      stats.subscribed += Number(d.activity?.subscribed || d.subscribed || 0);
      stats.unsubscribed += Number(d.activity?.unsubscribed || d.unsubscribed || 0);
    }
    const allZero = Object.values(stats).every(v=>!v);
    return {stats: allZero ? null : stats, warnings: allZero ? ['stats.get доступен, но нужные поля пустые/нулевые'] : []};
  }catch(e){ return {stats:null, warnings:[`stats.get недоступен: ${e.message}`]}; }
}

function postType(p){
  const a=p.attachments||[];
  if(p.copy_history?.length) return 'repost';
  if(a.some(x=>x.type==='video' && (x.video?.type==='short_video' || x.video?.platform==='clips'))) return 'clip';
  if(a.some(x=>x.type==='video')) return 'video';
  if(a.some(x=>x.type==='photo')) return 'image';
  if(a.some(x=>x.type==='link')) return 'link';
  return 'text';
}

function aggregateWeek(group, weekStart, weekPosts, statsResult, currentMembers){
  const views = weekPosts.map(p=>p.views?.count||0);
  const likes = weekPosts.map(p=>p.likes?.count||0);
  const comments = weekPosts.map(p=>p.comments?.count||0);
  const reposts = weekPosts.map(p=>p.reposts?.count||0);
  const totalViews=sum(views), totalLikes=sum(likes), totalComments=sum(comments), totalReposts=sum(reposts), engagement=totalLikes+totalComments+totalReposts;
  const weighted = totalLikes*CFG.weights.like + totalComments*CFG.weights.comment + totalReposts*CFG.weights.repost;
  const med=median(views), sd=std(views);
  const bestViewsPost = [...weekPosts].sort((a,b)=>(b.views?.count||0)-(a.views?.count||0))[0];
  const bestEngPost = [...weekPosts].sort((a,b)=>((b.likes?.count||0)+(b.comments?.count||0)+(b.reposts?.count||0))-((a.likes?.count||0)+(a.comments?.count||0)+(a.reposts?.count||0)))[0];
  const bestErPost = [...weekPosts].sort((a,b)=>postErView(b)-postErView(a))[0];
  const viralPosts = weekPosts.filter(p=>(p.views?.count||0) > med * CFG.viralThreshold && med>0).length;
  const formatScores = scoreBy(weekPosts, p=>postType(p));
  const dayScores = scoreBy(weekPosts, p=>dayRu(new Date(p.date*1000)));
  const hourScores = scoreBy(weekPosts, p=>String(new Date(p.date*1000).getUTCHours()).padStart(2,'0'));
  const bestFormat = bestKey(formatScores), bestDay=bestKey(dayScores), bestHour=bestKey(hourScores);
  const s = statsResult.stats;
  const warnings = [...statsResult.warnings];
  let reachTotal=s?.reachTotal ?? '';
  let reachSubs=s?.reachSubscribers ?? '';
  let source='stats.get';
  if(!s?.reachTotal && totalViews){ reachTotal=totalViews; source='fallback: просмотры постов'; warnings.push('Охват всего заменён просмотрами постов'); }
  if(!s?.reachSubscribers && totalViews){ reachSubs=''; warnings.push('Охват подписчиков VK не отдал'); }
  const erReach = reachTotal ? pct(engagement, reachTotal) : '';
  return {
    key:`${group.id}_${iso(weekStart)}`,
    collectedAt:new Date().toISOString(), ws:iso(weekStart), we:iso(weekEnd(weekStart)), group,
    members: currentMembers, posts:weekPosts.length, views:totalViews, likes:totalLikes, comments:totalComments, reposts:totalReposts, engagement,
    erSubs: pct(engagement, currentMembers), erViews:pct(engagement,totalViews), erReach,
    reachShare: (reachSubs && currentMembers) ? pct(reachSubs,currentMembers) : '',
    visitors:s?.visitors ?? '', communityViews:s?.views ?? '', reachTotal, reachSubs, reachMobile:s?.reachMobile ?? '',
    subscribed:s?.subscribed ?? '', unsubscribed:s?.unsubscribed ?? '', netGrowth: s ? s.subscribed-s.unsubscribed : '', growthPct:'',
    avgViews:round(avg(views),2), medViews:round(med,2), bestViewsPost, bestEngPost, bestErPost,
    bestViews: bestViewsPost?.views?.count || '', virality:pct(totalReposts,totalViews), discussion:pct(totalComments,totalViews), likeability:pct(totalLikes,totalViews), engagementIndex:pct(weighted,totalViews),
    viralPosts, viralPct:pct(viralPosts, weekPosts.length), stdViews:round(sd,2), postsPerDay:round(weekPosts.length/7,2), bestDay,bestHour,bestFormat, source, warnings
  };
}

function postErView(p){ const v=p.views?.count||0; const e=(p.likes?.count||0)+(p.comments?.count||0)+(p.reposts?.count||0); return v?e/v:0; }
function scoreBy(posts, keyFn){ const m={}; for(const p of posts){ const k=keyFn(p); const v=(p.views?.count||0)+(((p.likes?.count||0)+(p.comments?.count||0)+(p.reposts?.count||0))*10); if(!m[k]) m[k]={score:0,count:0}; m[k].score+=v; m[k].count++; } return m; }
function bestKey(obj){ const e=Object.entries(obj).sort((a,b)=>(b[1].score/b[1].count)-(a[1].score/a[1].count))[0]; return e ? e[0] : ''; }

function addDeltas(rows){
  rows.sort((a,b)=>a.ws.localeCompare(b.ws));
  for(let i=0;i<rows.length;i++){
    const p=rows[i-1], r=rows[i];
    r.deltaMembers = p && r.members!=='' && p.members!=='' ? r.members-p.members : '';
    r.deltaMembersPct = p && p.members ? pct(r.deltaMembers,p.members) : '';
    r.deltaReach = p && r.reachTotal!=='' && p.reachTotal!=='' ? r.reachTotal-p.reachTotal : '';
    r.deltaReachPct = p && p.reachTotal ? pct(r.deltaReach,p.reachTotal) : '';
    r.deltaImpr = p && r.communityViews!=='' && p.communityViews!=='' ? r.communityViews-p.communityViews : '';
    r.deltaImprPct = p && p.communityViews ? pct(r.deltaImpr,p.communityViews) : '';
    r.deltaErReachPp = p && r.erReach!=='' && p.erReach!=='' ? round(r.erReach-p.erReach,4) : '';
    r.deltaErReachPct = p && p.erReach ? pct(r.deltaErReachPp,p.erReach) : '';
    if(r.netGrowth!=='' && p?.members) r.growthPct = pct(r.netGrowth,p.members);
    if(r.members && r.subscribed==='' && r.unsubscribed==='') r.warnings.push('Исторические подписчики недоступны: показан текущий members_count, не значение на конец недели');
  }
  const forecastMembers = forecast(rows.map(r=>Number(r.members)||0),4);
  const forecastViews = forecast(rows.map(r=>Number(r.views)||0),1);
  for(const r of rows){ r.forecastMembers=forecastMembers; r.forecastViews=forecastViews; const rec=recommend(r); r.recWhat=rec.what; r.recWhen=rec.when; r.recFocus=rec.focus; }
  return rows;
}
function forecast(vals, steps){ const clean=vals.filter(Boolean); if(clean.length<2) return ''; const diffs=[]; for(let i=1;i<clean.length;i++) diffs.push(clean[i]-clean[i-1]); return round(clean[clean.length-1]+avg(diffs)*steps,0); }
function recommend(r){
  const what = r.bestFormat ? `Усилить формат: ${r.bestFormat}. Он даёт лучший средний результат по просмотрам/вовлечению.` : 'Накопить больше постов для рекомендации по формату.';
  const when = r.bestDay||r.bestHour ? `Пробовать публикации: ${r.bestDay || 'лучший день не определён'}, ${r.bestHour ? r.bestHour+':00 UTC' : 'час не определён'}.` : 'Накопить больше постов для рекомендации по времени.';
  let focus = 'Фокус: регулярность публикаций и тест форматов.';
  if(Number(r.discussion)>Number(r.likeability)) focus='Фокус: темы, вызывающие обсуждение; комментарии сильнее лайков.';
  if(Number(r.virality)>1) focus='Фокус: репостопригодные посты — вирусность выше обычной.';
  return {what,when,focus};
}

function summaryRow(r){ return [r.key,r.collectedAt,r.ws,r.we,r.group.id,r.group.screen_name||'',r.group.name,r.members,r.posts,r.views,r.likes,r.comments,r.reposts,r.engagement,r.erSubs,r.erViews,r.erReach,r.reachShare,r.visitors,r.communityViews,r.reachTotal,r.reachSubs,r.reachMobile,r.subscribed,r.unsubscribed,r.netGrowth,r.growthPct,r.deltaMembers,r.deltaMembersPct,r.deltaReach,r.deltaReachPct,r.deltaImpr,r.deltaImprPct,r.deltaErReachPp,r.deltaErReachPct,r.avgViews,r.medViews,r.bestViewsPost?.id||'',r.bestViewsPost?postLink(r.group,r.bestViewsPost):'',r.bestViews,r.bestEngPost?.id||'',r.bestErPost?.id||'',r.virality,r.discussion,r.likeability,r.engagementIndex,r.viralPosts,r.viralPct,r.stdViews,r.postsPerDay,r.bestDay,r.bestHour,r.bestFormat,r.forecastMembers,r.forecastViews,r.recWhat,r.recWhen,r.recFocus,r.source,r.warnings.join('; ')]; }
function postRows(group, weekKey, posts){ return posts.map(p=>{ const v=p.views?.count||0,l=p.likes?.count||0,c=p.comments?.count||0,r=p.reposts?.count||0,e=l+c+r; const d=new Date(p.date*1000); const weighted=l*CFG.weights.like+c*CFG.weights.comment+r*CFG.weights.repost; return [weekKey, iso(d), group.id, group.name, p.id, postLink(group,p), postType(p), dayRu(d), d.getUTCHours(), (p.text||'').slice(0,500), v,l,c,r,e,pct(e,v),pct(r,v),pct(c,v),pct(l,v),pct(weighted,v),'']; }); }
function dashRow(r){ return [r.ws,r.group.name,r.members,r.posts,r.views,r.engagement,r.erSubs,r.erViews,r.erReach,r.reachTotal,r.reachSubs,r.reachShare,r.virality,r.discussion,r.likeability,r.engagementIndex,r.bestFormat,r.bestDay,r.bestHour,r.recWhat,r.recWhen,r.warnings.join('; ')]; }
function recRows(r){ return [[r.collectedAt,r.group.id,r.group.name,`${r.ws}..${r.we}`,r.recWhat,`Лучший формат: ${r.bestFormat || 'нет данных'}`,'Высокий'],[r.collectedAt,r.group.id,r.group.name,`${r.ws}..${r.we}`,r.recWhen,`Лучший день/час: ${r.bestDay || '-'} ${r.bestHour || '-'}`,'Средний'],[r.collectedAt,r.group.id,r.group.name,`${r.ws}..${r.we}`,r.recFocus,`Вирусность=${r.virality}; обсуждаемость=${r.discussion}; лайкабельность=${r.likeability}`,'Средний']]; }

async function sheets(){ const auth=new google.auth.JWT(CFG.serviceEmail,null,CFG.privateKey,['https://www.googleapis.com/auth/spreadsheets']); return google.sheets({version:'v4',auth}); }
async function ensureSheet(api, title, headers){
  const meta=await api.spreadsheets.get({spreadsheetId:CFG.sheetId});
  const exists=meta.data.sheets?.some(s=>s.properties?.title===title);
  if(!exists) await api.spreadsheets.batchUpdate({spreadsheetId:CFG.sheetId, requestBody:{requests:[{addSheet:{properties:{title}}}]}});
  await api.spreadsheets.values.update({spreadsheetId:CFG.sheetId, range:`'${title}'!A1`, valueInputOption:'RAW', requestBody:{values:[headers]}});
}
async function readSheet(api,title){ const res=await api.spreadsheets.values.get({spreadsheetId:CFG.sheetId, range:`'${title}'!A2:ZZ`}).catch(()=>({data:{values:[]}})); return res.data.values||[]; }
async function replaceRows(api,title,headers,rows){ await ensureSheet(api,title,headers); await api.spreadsheets.values.clear({spreadsheetId:CFG.sheetId, range:`'${title}'!A2:ZZ`}); if(rows.length) await api.spreadsheets.values.update({spreadsheetId:CFG.sheetId, range:`'${title}'!A2`, valueInputOption:'RAW', requestBody:{values:rows}}); }

function weekRange(posts){
  const lastFull = monday(new Date()); lastFull.setUTCDate(lastFull.getUTCDate()-7);
  let first = new Date(Date.now()-CFG.daysBack*86400000);
  if(CFG.backfillFrom) first = new Date(CFG.backfillFrom+'T00:00:00Z');
  if(posts.length){ const oldest=new Date(Math.min(...posts.map(p=>p.date))*1000); if(!CFG.backfillFrom && oldest>first) first=oldest; }
  let start=monday(first); const weeks=[];
  while(start<=lastFull){ weeks.push(new Date(start)); start.setUTCDate(start.getUTCDate()+7); }
  return weeks;
}

async function main(){
  console.log(`VK metrics sync v8 started. Groups=${CFG.groups.length}`);
  const api=await sheets();
  for(const [title,headers] of [[CFG.summarySheet,SUMMARY_HEADERS],[CFG.postsSheet,POSTS_HEADERS],[CFG.dashboardSheet,DASH_HEADERS],[CFG.recSheet,REC_HEADERS]]) await ensureSheet(api,title,headers);
  let allSummary=[], allPosts=[], allDash=[], allRecs=[];
  for(const gid of CFG.groups){
    console.log(`Получение группы: ${gid}`);
    const group=await fetchGroup(gid);
    console.log(`Группа: ${group.name} (${group.id}), подписчики сейчас: ${group.members_count}`);
    const posts=await fetchPosts(group);
    const weeks=weekRange(posts);
    console.log(`${group.name}: найдено постов=${posts.length}, недель=${weeks.length}`);
    const rows=[]; const postsRows=[];
    for(const ws of weeks){
      const we=weekEnd(ws); const weekPosts=posts.filter(p=>p.date>=unix(ws) && p.date<=unix(we));
      const stat=await fetchStats(group.id,ws,we);
      const row=aggregateWeek(group,ws,weekPosts,stat,group.members_count);
      rows.push(row); postsRows.push(...postRows(group,row.key,weekPosts));
    }
    addDeltas(rows);
    allSummary.push(...rows.map(summaryRow)); allPosts.push(...postsRows); allDash.push(...rows.map(dashRow)); allRecs.push(...rows.flatMap(recRows));
    const last=rows[rows.length-1]; if(last) console.log(`${group.name}: последняя неделя ${last.ws}, постов=${last.posts}, просмотров=${last.views}, предупреждения=${last.warnings.length}`);
  }
  await replaceRows(api,CFG.summarySheet,SUMMARY_HEADERS,allSummary);
  await replaceRows(api,CFG.postsSheet,POSTS_HEADERS,allPosts);
  await replaceRows(api,CFG.dashboardSheet,DASH_HEADERS,allDash);
  await replaceRows(api,CFG.recSheet,REC_HEADERS,allRecs);
  console.log('Синхронизация v8 завершена. Листы обновлены: weekly_summary, posts_raw_vk, dashboard_data, ai_recommendations');
}

main().catch(err=>{ console.error('Сбой синхронизации v8.'); console.error(err); process.exit(1); });
