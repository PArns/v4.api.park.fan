# Search Console setup

How to connect Google Search Console to park.fan's admin: a Google service account that may read the
property, and two environment variables. Open this to switch the import on, to rotate its key, or
when `lastError` on the SEO page says Google refused something. What the import does and the shape of
its answers: [Search Console](../frontend/search-console.md). About fifteen minutes, once.

## 1. A project and the API

1. In the [Google Cloud console](https://console.cloud.google.com/) pick a project, or create one
   (`park-fan-seo`). It needs no billing.
2. **APIs & Services → Library → Google Search Console API → Enable.** Without it Google answers 403
   with "has not been used in project … or it is disabled".

## 2. The service account and its key

1. **IAM & Admin → Service Accounts → Create service account.** Name it `park-fan-gsc`. It needs no
   role in the project: what it may read is decided in Search Console, below.
2. Open the account, **Keys → Add key → Create new key → JSON.** The key file downloads once. It is a
   credential: it goes into the environment variable below and nowhere else, and the local copy is
   deleted afterwards.
3. Note the account's address, `park-fan-gsc@<project>.iam.gserviceaccount.com`.

## 3. Let the account read the property

In [Search Console](https://search.google.com/search-console), pick the property →
**Settings → Users and permissions → Add user**, paste the service account's address, permission
**Restricted** (read-only). If Google still answers 403 for a restricted user, use **Full**; the
import never writes.

The property is best a **domain property** (`sc-domain:park.fan`, verified through DNS): it covers
`http`, `https`, `www` and every subdomain, and the import keeps `park.fan` and `www.park.fan`. A
URL-prefix property (`https://park.fan/`) works too and counts only its own host.

## 4. The variables

On the API container (Coolify → the API → Environment), then redeploy:

| Variable | Value |
| --- | --- |
| `GSC_SERVICE_ACCOUNT_JSON` | the key file's content, raw or base64: `base64 -w0 park-fan-gsc.json` (a key with line breaks survives an env file better as base64) |
| `GSC_SITE_URL` | `sc-domain:park.fan`, exactly as the property is listed in Search Console |
| `GSC_IMPORT_DAYS` | optional; days fetched again on every run, default 3 |

`.env.example` and `.env.production.example` list them. Without the first two the module is off.

## 5. First import and check

The import runs nightly at 09:20 UTC. To start it now: **SEO page → "Jetzt importieren"** (editor or
owner), or `POST /v1/admin/search-console/import`. The first run backfills 16 months, newest days
first, one request pair per day; expect a quarter of an hour or more, and the status's `coverage`
growing while it runs. Check:

- the SEO page's status line, or `GET /v1/admin/search-console/status`, or the MCP tool `gsc_status`:
  `configured: true`, `lastImportAt` set, `lastError` null, `coverage` reaching back about 16 months
  and ending two or three days ago (Google's newest days are not final);
- the job `search-console` / `import` in the bull-board is completed, not failed.

## When `lastError` says…

| Message | Cause and fix |
| --- | --- |
| `GSC_SERVICE_ACCOUNT_JSON is neither JSON nor base64 of JSON` / `not valid JSON` / `has no client_email and private_key` | the variable is not the whole key file; paste it again, or base64 it |
| `GSC_SITE_URL is not set` / `is neither sc-domain:<domain> nor an http(s) URL` | the property is missing or mistyped |
| `Google refused the service account (400 invalid_grant …)` | the key was deleted or rotated, or the clock of the host is off; create a new key |
| `Search Console refused <account> for <property> (403)` | the account is not a user of the property (step 3), or the API is not enabled in the key's project (step 1); Google's own sentence follows |
| `Search Console has no property <…> for <account> (404)` | `GSC_SITE_URL` differs from the property's name in Search Console (a trailing slash, `https` against `http`) |
| `N failed days in a row` / `Search Console answered 5xx` | Google's side; the next run takes the days still missing |

## Rotating the key

Create a second key (step 2), set the variable, redeploy, check the status, then delete the first key
in the Cloud console. Removing the account from the property's users, or deleting it, switches the
import off at once.

## Not tested against Google

The code was built without real credentials. Its client is exercised against a mocked Google in unit
and e2e tests. If the first run fails in a way these steps do not explain, the message in `lastError`
and the API log (`SearchConsoleClient`) carry Google's own text.
