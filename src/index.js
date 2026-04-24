import axios from "axios";
import { google } from "googleapis";
import dotenv from "dotenv";

dotenv.config();

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const {
  VK_TOKEN,
  VK_GROUPS,
  VK_DAYS_BACK = 90,
  VK_REQUEST_DELAY_MS = 800,
  VK_RETRY_MAX = 5,
  GOOGLE_SHEETS_SPREADSHEET_ID,
  GOOGLE_SERVICE_ACCOUNT_EMAIL,
  GOOGLE_PRIVATE_KEY,
  GOOGLE_SUMMARY_SHEET_NAME
} = process.env;

// ---------- VK API ----------

async function vkApi(method, params = {}) {
  for (let i = 0; i < VK_RETRY_MAX; i++) {
    try {
      const res = await axios.get(`https://api.vk.com/method/${method}`, {
        params: {
          ...params,
          access_token: VK_TOKEN,
          v: "5.199"
        }
      });

      if (res.data.error) {
        if (res.data.error.error_code === 6) {
          await sleep(1000 + i * 500);
          continue;
        }
        throw new Error(JSON.stringify(res.data.error));
      }

      return res.data.response;

    } catch (e) {
      if (i === VK_RETRY_MAX - 1) throw e;
      await sleep(1000);
    }
  }
}

// ---------- VK DATA ----------

async function fetchGroup(id) {
  const res = await vkApi("groups.getById", {
    group_ids: id,
    fields: "members_count"
  });
  return res[0];
}

async function fetchPosts(owner_id, count = 100) {
  const res = await vkApi("wall.get", {
    owner_id,
    count
  });
  return res.items || [];
}

// ---------- UTILS ----------

function median(arr) {
  const sorted = [...arr].sort((a,b)=>a-b);
  const mid = Math.floor(sorted.length/2);
  return sorted.length % 2
    ? sorted[mid]
    : (sorted[mid-1] + sorted[mid]) / 2;
}

function stdDev(arr) {
  const avg = arr.reduce((a,b)=>a+b,0)/arr.length;
  const variance = arr.reduce((a,b)=>a + Math.pow(b-avg,2),0)/arr.length;
  return Math.sqrt(variance);
}

// ---------- AI / FORECAST ----------

function forecastGrowth(history) {
  if (history.length < 2) return 0;
  const diffs = [];
  for (let i = 1; i < history.length; i++) {
    diffs.push(history[i] - history[i-1]);
  }
  return diffs.reduce((a,b)=>a+b,0)/diffs.length;
}

function bestPostingTime(posts) {
  const hours = {};
  posts.forEach(p => {
    const h = new Date(p.date * 1000).getHours();
    hours[h] = (hours[h] || 0) + p.views.count;
  });
  return Object.entries(hours).sort((a,b)=>b[1]-a[1])[0]?.[0];
}

// ---------- GOOGLE ----------

async function getSheets() {
  const auth = new google.auth.JWT(
    GOOGLE_SERVICE_ACCOUNT_EMAIL,
    null,
    GOOGLE_PRIVATE_KEY.replace(/\\n/g, "\n"),
    ["https://www.googleapis.com/auth/spreadsheets"]
  );

  const sheets = google.sheets({ version: "v4", auth });
  return sheets;
}

async function appendRow(values) {
  const sheets = await getSheets();

  await sheets.spreadsheets.values.append({
    spreadsheetId: GOOGLE_SHEETS_SPREADSHEET_ID,
    range: GOOGLE_SUMMARY_SHEET_NAME,
    valueInputOption: "RAW",
    requestBody: {
      values: [values]
    }
  });
}

// ---------- MAIN ----------

async function main() {
  console.log("VK v7 AI analytics started");

  const groups = VK_GROUPS.split(",");

  for (const g of groups) {

    console.log("Group:", g);

    const group = await fetchGroup(g);

    const posts = await fetchPosts(-group.id, 200);

    const views = posts.map(p => p.views?.count || 0);
    const likes = posts.map(p => p.likes?.count || 0);
    const comments = posts.map(p => p.comments?.count || 0);
    const reposts = posts.map(p => p.reposts?.count || 0);

    const sumViews = views.reduce((a,b)=>a+b,0);
    const sumLikes = likes.reduce((a,b)=>a+b,0);
    const sumComments = comments.reduce((a,b)=>a+b,0);
    const sumReposts = reposts.reduce((a,b)=>a+b,0);

    const engagement = sumLikes + sumComments + sumReposts;

    const erView = sumViews ? (engagement / sumViews) * 100 : 0;

    const viral = sumViews ? (sumReposts / sumViews) * 100 : 0;

    const bestHour = bestPostingTime(posts);

    const med = median(views);
    const std = stdDev(views);

    const forecast = forecastGrowth(views);

    await appendRow([
      new Date().toISOString(),
      g,
      group.name,
      group.members_count,
      posts.length,
      sumViews,
      sumLikes,
      sumComments,
      sumReposts,
      engagement,
      erView.toFixed(2),
      viral.toFixed(2),
      med,
      std.toFixed(2),
      bestHour,
      forecast.toFixed(2)
    ]);

    await sleep(VK_REQUEST_DELAY_MS);
  }

  console.log("Done");
}

main().catch(console.error);
