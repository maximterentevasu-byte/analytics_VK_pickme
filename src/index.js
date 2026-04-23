import 'dotenv/config';
import { google } from 'googleapis';

const VK_API_VERSION = process.env.VK_API_VERSION || '5.199';

function getRequiredEnv(name) {
  const value = process.env[name];
  if (!value || !value.trim()) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value.trim();
}

function parseGroups(raw) {
  return raw
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizeGooglePrivateKey(raw) {
  return raw.replace(/\\n/g, '\n');
}

function normalizeVkGroupId(value) {
  return value
    .trim()
    .replace(/^https?:\/\/vk\.com\//i, '')
    .replace(/^vk\.com\//i, '')
    .replace(/^public/i, '')
    .replace(/^club/i, '')
    .replace(/\/$/, '');
}

function extractGroupFromVkResponse(data) {
  if (!data || typeof data !== 'object') return null;

  if (Array.isArray(data.response) && data.response.length > 0) {
    return data.response[0];
  }

  if (data.response && Array.isArray(data.response.groups) && data.response.groups.length > 0) {
    return data.response.groups[0];
  }

  if (data.response && typeof data.response === 'object' && !Array.isArray(data.response) && data.response.id) {
    return data.response;
  }

  return null;
}

async function callVkGroupsGetById(groupId, token, usePluralParam = false) {
  const url = new URL('https://api.vk.com/method/groups.getById');
  url.searchParams.set(usePluralParam ? 'group_ids' : 'group_id', groupId);
  url.searchParams.set('fields', 'members_count');
  url.searchParams.set('access_token', token);
  url.searchParams.set('v', VK_API_VERSION);

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Accept: 'application/json',
      'User-Agent': 'vk-sheets-members-sync/1.1'
    }
  });

  if (!response.ok) {
    throw new Error(`VK HTTP ${response.status} for group "${groupId}"`);
  }

  return response.json();
}

async function fetchVkGroup(rawGroupId, token) {
  const groupId = normalizeVkGroupId(rawGroupId);

  let data = await callVkGroupsGetById(groupId, token, false);

  if (data.error) {
    const errorCode = data.error.error_code ?? 'unknown';
    const errorMsg = data.error.error_msg ?? 'Unknown VK API error';
    throw new Error(`VK API error for group "${rawGroupId}": [${errorCode}] ${errorMsg}`);
  }

  let group = extractGroupFromVkResponse(data);

  if (!group) {
    data = await callVkGroupsGetById(groupId, token, true);

    if (data.error) {
      const errorCode = data.error.error_code ?? 'unknown';
      const errorMsg = data.error.error_msg ?? 'Unknown VK API error';
      throw new Error(`VK API error for group "${rawGroupId}": [${errorCode}] ${errorMsg}`);
    }

    group = extractGroupFromVkResponse(data);
  }

  if (!group) {
    throw new Error(
      `VK did not return group data for "${rawGroupId}". ` +
      `Check the short name/URL and token permissions. Raw response: ${JSON.stringify(data)}`
    );
  }

  return {
    requested_group_id: rawGroupId,
    normalized_group_id: groupId,
    group_id: group.id,
    group_name: group.name,
    screen_name: group.screen_name || '',
    members_count: Number(group.members_count ?? 0)
  };
}

async function getSheetsClient() {
  const clientEmail = getRequiredEnv('GOOGLE_SERVICE_ACCOUNT_EMAIL');
  const privateKey = normalizeGooglePrivateKey(getRequiredEnv('GOOGLE_PRIVATE_KEY'));

  const auth = new google.auth.JWT({
    email: clientEmail,
    key: privateKey,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });

  await auth.authorize();

  return google.sheets({ version: 'v4', auth });
}

async function ensureHeaderRow(sheets, spreadsheetId, sheetName) {
  const range = `${sheetName}!A1:D1`;

  const existing = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range
  });

  const currentHeader = existing.data.values?.[0] ?? [];
  const expectedHeader = ['collected_at', 'group_id', 'group_name', 'members_count'];

  const isSameHeader =
    currentHeader.length === expectedHeader.length &&
    currentHeader.every((cell, index) => cell === expectedHeader[index]);

  if (!isSameHeader) {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range,
      valueInputOption: 'RAW',
      requestBody: {
        values: [expectedHeader]
      }
    });
  }
}

async function appendRows(sheets, spreadsheetId, sheetName, rows) {
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${sheetName}!A:D`,
    valueInputOption: 'RAW',
    insertDataOption: 'INSERT_ROWS',
    requestBody: {
      values: rows
    }
  });
}

async function main() {
  const vkToken = getRequiredEnv('VK_TOKEN');
  const spreadsheetId = getRequiredEnv('GOOGLE_SHEETS_SPREADSHEET_ID');
  const sheetName = process.env.GOOGLE_SHEET_NAME?.trim() || 'members_history';
  const groupIds = parseGroups(getRequiredEnv('VK_GROUPS'));

  if (groupIds.length === 0) {
    throw new Error('VK_GROUPS is empty. Add at least one group.');
  }

  console.log(`Starting sync for ${groupIds.length} VK group(s)...`);

  const results = [];
  for (const groupId of groupIds) {
    console.log(`Fetching members_count for: ${groupId}`);
    const group = await fetchVkGroup(groupId, vkToken);
    results.push(group);
  }

  const collectedAt = new Date().toISOString();
  const rows = results.map((group) => [
    collectedAt,
    String(group.group_id),
    group.group_name,
    String(group.members_count)
  ]);

  const sheets = await getSheetsClient();
  await ensureHeaderRow(sheets, spreadsheetId, sheetName);
  await appendRows(sheets, spreadsheetId, sheetName, rows);

  console.log('Sync completed successfully.');
  console.table(results.map((item) => ({
    requested_group_id: item.requested_group_id,
    normalized_group_id: item.normalized_group_id,
    group_id: item.group_id,
    group_name: item.group_name,
    members_count: item.members_count
  })));
}

main().catch((error) => {
  console.error('Sync failed.');
  console.error(error);
  process.exit(1);
});
