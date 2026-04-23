# VK → Google Sheets: weekly members count sync

Node.js script that collects the current number of subscribers (`members_count`) from one or more VK publics and appends the result to Google Sheets.

## What this project does

On each run, the script:

1. reads the list of VK publics from environment variables;
2. calls `groups.getById` for each public with `members_count`;
3. creates rows with the current timestamp;
4. appends the rows to Google Sheets.

## Table structure

Create a Google Sheet and a sheet tab like `members_history`.

Header row used by the script:

```text
collected_at | group_id | group_name | members_count
```

If the first row is empty or different, the script will rewrite `A1:D1` with this header.

## Requirements

- Node.js 20+
- VK token with access to read the required public data
- Google Cloud project with Google Sheets API enabled
- Service Account with edit access to the target Google Sheet

## Environment variables

Copy `.env.example` to `.env` for local testing.

```bash
cp .env.example .env
```

Fill these variables:

- `VK_TOKEN` — your VK API token
- `VK_GROUPS` — comma-separated VK public ids or screen names
- `VK_API_VERSION` — optional, default `5.199`
- `GOOGLE_SHEETS_SPREADSHEET_ID` — spreadsheet id from the URL
- `GOOGLE_SHEET_NAME` — optional, default `members_history`
- `GOOGLE_SERVICE_ACCOUNT_EMAIL` — service account email
- `GOOGLE_PRIVATE_KEY` — private key from the service account JSON

## Local run

Install dependencies:

```bash
npm install
```

Start the script:

```bash
npm start
```

## Railway deploy

1. Push this repository to GitHub.
2. Create a new Railway project.
3. Deploy from your GitHub repo.
4. Add all environment variables from `.env.example` to Railway Variables.
5. In the service settings, set the start command to:

```bash
npm start
```

6. Add a weekly cron expression in **Settings → Cron Schedule**.

Example: every Monday at 09:00 UTC

```cron
0 9 * * 1
```

## Google setup checklist

1. Create a Google Cloud project.
2. Enable **Google Sheets API**.
3. Create a **Service Account**.
4. Generate a JSON key for that Service Account.
5. Share the target Google Sheet with the Service Account email as **Editor**.
6. Put `client_email` into `GOOGLE_SERVICE_ACCOUNT_EMAIL`.
7. Put `private_key` into `GOOGLE_PRIVATE_KEY`.

## Notes

- The script exits after completion, which is exactly what you want for Railway cron runs.
- Keep secrets only in environment variables, never in GitHub.
- On the next steps, this same project can be extended with reach, views, post stats, ER, and weekly aggregates.

## Example output in logs

```text
Starting sync for 2 VK group(s)...
Fetching members_count for: club123456
Fetching members_count for: my_public_slug
Sync completed successfully.
┌─────────┬────────────────────┬──────────┬────────────────────┬───────────────┐
│ (index) │ requested_group_id │ group_id │ group_name         │ members_count │
├─────────┼────────────────────┼──────────┼────────────────────┼───────────────┤
│ 0       │ 'club123456'       │ 123456   │ 'Public One'       │ 15234         │
│ 1       │ 'my_public_slug'   │ 987654   │ 'Public Two'       │ 43890         │
└─────────┴────────────────────┴──────────┴────────────────────┴───────────────┘
```
