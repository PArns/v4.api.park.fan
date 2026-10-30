# Google Search Console (`/v1/admin/search-console/*`)

How park.fan is found in Google: the queries, the pages, the clicks, impressions, click-through rate
and position over time, imported nightly from Search Console and joined with the site statistic's own
numbers for visits that came from search. Admin and MCP only; nothing here is public. **PAR-1160.**
The owner's setup steps (Google Cloud, the property, the variables) are in
[Search Console setup](../admin/search-console-setup.md). The frontend half is the SEO page,
`/admin/traffic/seo`.

Without `GSC_SERVICE_ACCOUNT_JSON` and `GSC_SITE_URL` the module is off: the status says
`configured: false`, the import does nothing and the reports answer zeros.

## Endpoints

All on `AdminSearchConsoleController`: a session (`AdminAuthGuard`, any role unless noted),
`private, no-store`. The reports take the window of the [site statistic](site-analytics.md#reports)
(`days` 1–365, or `from` and `to`, at most 366 days, `to` today at the latest; both at once or one
of them alone is a 400) and `locale`, and echo `from`, `to`, `days` and `locale`.

| Method | Path                                                   | Answers                                                                                                  |
| ------ | ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| GET    | `status`                                               | `{ configured, siteUrl, lastImportAt, lastError, runningSince, coverage: { from, to } \| null, rows, types }`          |
| POST   | `import`                                               | editor and above, audited (`job.search-console/import`): `202 { queued: true }`; 409 when not configured |
| GET    | `queries?type=&page=&country=&device=&search=&limit=`  | the queries, most clicks first                                                                           |
| GET    | `pages?type=&query=&country=&device=&search=&limit=`   | the pages, most clicks first, with the site's own numbers                                                |
| GET    | `series?type=&interval=&page=&query=&country=&device=` | per day, week or month                                                                                   |

`limit` is 1–200, default 50. `page` is a path or a URL (lower-cased, no trailing slash, query and
fragment dropped); `query` is one search query, exactly, lower case; `country` is Search Console's
ISO alpha-3 (`deu`, `usa`, either case); `device` is `desktop`, `mobile` or `tablet`. `search` keeps
the listed queries (on `queries`) or paths (on `pages`) that contain the text, case ignored, with `%`
and `_` taken literally; the totals stay those of the whole report. `interval` is
`day`, `week` (ISO, from Monday) or `month`; `hour` is a 400. `type` is the [search type](#search-types). Without it: day up to 92 days, week up to
183, month beyond.

```json
{
  "from": "2026-10-22",
  "to": "2026-10-28",
  "days": 7,
  "locale": null,
  "totals": {
    "clicks": 102,
    "impressions": 2100,
    "ctr": 0.0486,
    "position": 4.0
  },
  "previous": {
    "clicks": 17,
    "impressions": 350,
    "ctr": 0.0486,
    "position": 4.0
  },
  "items": [
    {
      "query": "phantasialand wartezeiten",
      "clicks": 36,
      "impressions": 480,
      "ctr": 0.075,
      "position": 2.0,
      "change": { "clicks": 30, "position": 0.0 }
    }
  ]
}
```

`pages` has `path` for `query` and, per item, `site`: the site statistic's visits from the `search`
channel that **entered** on that path in the window, `{ visits, bounceRate, avgVisitSeconds }`
(bounce rate as there, `1 − engaged / visits`), `null` without such a visit. The site's window is the
whole one asked for; Search Console's is cut at its last day (below). `series` is
`{ from, to, days, locale, interval, series: [ { day, clicks, impressions, ctr, position } ] }`, a
point's `day` the period's first day, with `partial: true` when the window or the data cut it.

- **`ctr`** is `clicks / impressions`, 0..1, four places, computed when read. **`position`** is the
  impression-weighted average (the stored `position_sum` over the impressions), one decimal, 1 at the
  top, **`null` where there were no impressions** (a zero would read as the best rank).
- **`change`** is `{ clicks, position }`: clicks minus the previous window's, and position minus
  the previous position (negative means it moved up); `position` is null when either side has no
  impressions, and the whole `change` is null over 180 days.
- **`previous`** is the figures over the same number of days at the start of the window before, and
  null over 180 days or when those days have no data.

## Search types

Search Console splits its data by the `type` of search. The import keeps `web` (the default), `image`
(Google Images), `news` (the News tab in Search) and `discover`; `video` and `googleNews` are not
imported. The reports take **`type`** (`web`, `image`, `news`, `discover`; default `web`, anything else
is a 400) and answer from the batches of that type only, so a request without it, or with `type=web`,
is the response it was before, field for field. The rolling window ends at the last day **of that
type** with data, `site` on a `pages` item is `null` for every type but `web` (the site statistic
cannot split its visits by Google's type), and a type nothing was imported for answers zeros.

- **Discover has no queries.** `queries`, a `query` filter and a `country` or `device` filter on a
  `pages` list need query rows, so they answer **400** (`type=discover has no search queries …`);
  `pages` without filters and `series` (also with a `country`) work. The `gsc_queries` tool says the
  same.
- **Discover has no devices.** Google answers a `device` request for it with a 400,
  `Requests for Discover cannot be grouped by device`, so the import stores no device rows for
  Discover and a `device` filter on it answers **400** (`type=discover has no devices …`) rather
  than zeros.
- **Discover has no `byProperty` aggregation** (Google's API reference), so the import does not ask
  for the property level there. It stores the property row as the sum of the page rows, the position
  weighted by the impressions; Discover lists every page once a day, so that sum is the total. (A
  request without dimensions for `discover` was not tried against the live API: no credentials were
  available while this was built.)
- **`status.types`** is `{ web, image, news, discover }`, each `{ coverage: { from, to } | null, rows }`.
  `coverage` and `rows` at the top stay the `web` entry.

## What the numbers are

Search Console's own, imported as they are, with four things a reader of them must know:

- **Days are Pacific time**, Google's, not UTC like the site statistic's. A report over UTC days reads
  Google's days of the same dates.
- **The newest two or three days are not final.** The import asks for final data only, so those days
  have no rows yet. A rolling window (`days`, or none given) therefore ends at the last day with data
  (`coverage.to` of the status, counted only for days in the current layout) and starts `days - 1`
  before it, as Search Console's own "last 28 days" does; `from` and `to` of the response are that
  anchored window, the site's own numbers on the `pages` report cover it too, and `previous` is the
  `days` before it. Before the first import the window ends today. A fixed window (`from`/`to`) stays
  as asked and is read up to the last day with data, with `previous` cut to as many days from the start
  of the window before, so a change compares like with like.
- **Google leaves out the queries it anonymises** (rare ones, often a third or more of the
  impressions) as soon as a page is combined with a country or a device. The `queries` report therefore
  sums the queries Google lists, and its totals are lower than those of `pages` and `series`.
- **Which stored level answers what.** Measured for one day: no dimension 699 clicks and 49,366
  impressions, `country` alone and `device` alone the same, `page` alone 704 and 51,217 (Google
  aggregates by page there, so a page sum is a little above the property), while `page, country`,
  `page, device` and `page, country, device` return 199 clicks and 17,159 impressions and `query` 198
  and 16,743. Only the single-dimension requests are complete, so the import stores them as their own
  levels (below) and the reports pick one:

  | Request                                      | totals, `previous`, series                                                                             | the list                                                                                                                |
  | -------------------------------------------- | ------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
  | no country or device filter                  | the property row (what the Search Console chart shows); with a `page` or `locale` filter the page rows | `pages`: page rows; `queries`: query rows                                                                               |
  | `country` or `device` (one of them)          | the country or device rows, complete                                                                   | query rows: visible queries only, so the items sum to less than `totals`; the response carries `partialBreakdown: true` |
  | both filters, or one with `page` or `locale` | the query rows, visible queries only, `partialBreakdown: true`                                         | query rows                                                                                                              |
  | `query`                                      | the query rows                                                                                         | query rows                                                                                                              |

  `partialBreakdown` is absent otherwise. The position differs by aggregation too: for the property it
  is the site's topmost result, for a page that URL's. Google's BigQuery bulk export is the only source
  of complete page by country by device data including anonymised rows; it is not used here.

- **Google caps a day at about 50,000 rows** per kind, most clicks first; a day with more is cut.

## Storage: insert-only, a batch per import

| Table                    | Row                                                                                                                                                                                                                                         | Notes                                                                                              |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `search_console_rows`    | the day, `batch_id`, `search_type`, `page` (path, `''` on the property, country and device levels), `query` (null on the complete levels), `locale`, `country` and `device` (`''` where not asked), `clicks`, `impressions`, `position_sum` | hypertable on `day`, 30-day chunks, compressed after 35 days, one segment per batch; `HYPERTABLES` |
| `search_console_batches` | one import of one day and type: `day`, `search_type`, `imported_at`, `format`, `rows`, `clicks`, `impressions`, `position_sum`                                                                                                              | plain, a few hundred rows                                                                          |
| `search_console_runs`    | one run: when, trigger (`cron`, `manual`), ok, days checked, batches written, error                                                                                                                                                         | plain, pruned after 90 days                                                                        |

Five levels are stored per day. Complete rows without a query: **property** (one row, no dimension,
the day's totals), **page** (`country` and `device` empty), **country** and **device** (`page`
empty). And **query** rows (page × query × country × device), visible queries only. `ctr` is not
stored. `format` is the layout of the batch (`BATCH_FORMAT`, now 2; the column defaults to 1, so
adding it rewrites nothing). A day whose current batch has a lower format counts as never fetched: the
next run imports it again and the reports ignore it until then. The first run after this layout
arrived therefore re-imported the whole backfill, five requests a day.

**The type.** `search_type` is on both tables: `varchar(10) NOT NULL DEFAULT 'web'` on the batches
(a plain table), and **`varchar(10)` nullable with `DEFAULT 'web'`** on the rows. The rows table is a
compressed hypertable and TimescaleDB adds a column to the existing chunks, compressed ones included,
only as a nullable column or one with a constant default, without decompressing anything; the nullable
form with a default is the one every TimescaleDB version since 2.6 accepts, and an old row reads as
`web`. No report filters rows by `search_type`: the current batch of a day and type names the rows
(`batch_id = ANY(…)`), so even a chunk that read the column wrong could not mix types. Compression
settings are unchanged (segment by `batch_id`). Each (day, type) has its own current batch
(`currentBatches(…, type)`: `DISTINCT ON (day) … WHERE search_type = $3`), its own plan
(`planDays` per type, so a type with no batches is a full backfill) and its own change check
(`isUnchanged`); the retention job drops chunks and old batches regardless of the type.

**A day imported again replaces the earlier import without an UPDATE or a DELETE.** Each import of a
day is a new batch; the reports read only the newest batch of every day
(`currentBatches`: `DISTINCT ON (day) … ORDER BY day, id DESC` over the small table, then
`batch_id = ANY(…)` on the rows). The batch and its rows are written in one transaction, so a reader
sees the old batch or the new one, never half of either. The rows of the older batches stay until
their chunk is dropped; compression keeps a segment per batch, so a read skips them without
decompressing them. Three alternatives were weighed:

- **`DELETE` the day, then insert:** leaves dead tuples in a table VACUUM then has to visit, and a
  compressed chunk would have to be decompressed for it, which the site statistic's tables avoid on
  purpose.
- **`UPSERT` on `(day, page, query, country, device)`:** a unique index across a hypertable, and an
  `UPDATE` of a compressed row per figure that moved.
- **One chunk per day, dropped and rewritten:** a lock on the hypertable for every re-import and 480
  chunks for a year and a half, which the planner pays for on every report.

A day that did not change is not written at all: the next run compares the fresh download's row
count, clicks, impressions and position sum with the current batch's, and skips it. So the cost of
re-importing three days a night is five requests a day and, in the steady state, no write; storage
holds each day once plus the days Google revised. A day Google had nothing for gets a batch without
rows, and is asked about again until it is ten days old (`EMPTY_DAY_RETRY_DAYS`): Google's final
numbers usually arrive two days late, and a delay of a few more would otherwise leave the day empty
for good once it left the recent days (PAR-1181). An empty answer to that question writes nothing,
so the cost is the requests of the empty days, mostly `news` and `discover`. Retention is
`drop_chunks` past 500 days (16 months and the rest of a chunk) in the nightly job, under a 5 s
`lock_timeout` ([lock queues](../troubleshooting/timescaledb-lock-queues.md)), which also deletes
the batch rows of dropped days and the runs older than 90 days.

## Page URLs become the site statistic's paths

`pagePath` (`search-console-url.util.ts`) strips the origin, the query and the fragment, lower-cases
and drops the trailing slash, so `https://www.park.fan/DE/Parks/` is `/de/parks` and joins
`site_pageviews.entry` and `.path`, which are stored the same way. The several URLs of one page
(`http`, `https`, `www`, with and without the slash) merge into one row per country and device.
For a `sc-domain:` property only the bare domain and `www.` count: a subdomain's pages
(`api.park.fan/v1/…`) would otherwise land on the same paths as the site's; their rows are skipped
and counted (`skippedRows` in the log). A URL-prefix property counts its own host. `locale` is the
path's first segment if it is one of the six, otherwise empty.

## The import

Queue `search-console`, `import` nightly at 09:20 UTC and on demand
(`POST /v1/admin/search-console/import`, a second request within a minute queues nothing), and
`drop-old-chunks` at 01:55 ([job queues](../architecture/job-queues.md)).

1. **Which days.** For every type: the last `GSC_IMPORT_DAYS` days (default 3, 1–30) ending yesterday,
   newest first, then every day of the last 480 days (16 months, a few days short of Google's limit)
   that has no batch of that type or whose batch is in an older format, and every day of the last ten
   whose batch is empty. The units (day, type) of a run
   are processed newest day first. The first run after the types arrived is therefore the backfill of
   `image`, `news` and `discover`, newest days first; a run that stops resumes with the units still
   missing, because every unit is its own transaction.
2. **One unit:** `searchAnalytics.query` requests with the unit's `type`: no dimension, `[page]`,
   `[country]`, `[device]` and `[page, query, country, device]` for `web`, `image` and `news` (5), and
   `[page]` and `[country]` for `discover` (2: no property request, no device, no queries). That is up
   to 17 requests per day (5 + 5 + 5 + 2); a nightly run of three days is about 51 requests, plus up to
   seven older empty days of a type (35 for `news`, 14 for `discover`), and the
   one-off backfill of the three new types over 480 days is 480 × 12 = **5,760 requests**
   (more where a day has over 25,000 rows). `dataState: final`, 25,000 rows a request with
   `startRow` until a page is not full, four tries on a 429, a 5xx or a network failure with a
   doubling pause (or Google's `Retry-After`). **The client spaces requests at least 100 ms apart**
   (10 a second, 600 a minute) against Google's 1,200 a minute per site and user, so the backfill takes
   about ten minutes of waiting plus Google's own response time. A 401 renews the token once.
3. **Failure.** A refusal that every day would meet (403, 404, a rejected key) stops the run with a
   message that names the service account and the property; three failed days in a row stop it too;
   one failed day among good ones is listed and the run goes on. Any failure fails the job, so it
   reaches the [data-quality report](../architecture/job-queues.md#failed-jobs-retention-and-monitoring),
   and the run's error is the status's `lastError` until a later run succeeds.
4. **One run at a time:** a Redis lock (`search-console:import:lock`); a second job finds it taken and
   returns, without a run of its own. The lock lasts five minutes and the run renews it every minute,
   stamping its row's `heartbeat_at` with the same beat, so a run lost with its process (a deploy
   that could not wait) frees the lock within five minutes instead of holding off every run for
   hours (PAR-1182).
5. **A shutdown.** A deploy gives the jobs in flight 20 s
   ([a job a deploy stops](../architecture/job-queues.md#a-job-a-deploy-stops)). The run checks for a
   shutdown before each unit and stops there: it records itself as not ok with "Stopped by a shutdown
   after N unit(s)" as its error (the status's `lastError` until the next success), releases the
   lock, and the job ends without failing. The units it did not reach are the next run's: the days
   never fetched are planned anyway, the recent ones every night.
6. **Where a run shows.** The status's `runningSince` is the start of the run in progress, and the
   queue is on the admin's queue screen ([`queue-status`](../admin/endpoints.md)): a run is `active`
   there until it ends, and a failed one says why (`queue-failures?queue=search-console`).

`lastImportAt` is the end of the last successful run, `coverage` the first and last day with data in
the current `web` batches, `rows` their row count; `types` has the same per type. `runningSince` is
the start of the newest run while it has no end and has stamped `heartbeat_at` (or, before its
first stamp, started) within the lock's five minutes, `null` otherwise. While it is set an import
request queues a job that returns at once, so a client disables its button and waits for
`lastImportAt` or `lastError` to change. A run lost with its process stops counting when its lock
lapses.

## Google's API without a client library

`search-console.client.ts` and `google-auth.ts` speak Google's REST API with `fetch` and Node's
`crypto`: the service account's JWT (RS256, `iss`, `scope`, `aud`, an hour) exchanged at
`https://oauth2.googleapis.com/token` for an access token kept until a minute before it expires, and
`POST https://www.googleapis.com/webmasters/v3/sites/{siteUrl}/searchAnalytics/query` (and the
URL inspection endpoint above). The
`googleapis` package is about 245 MB unpacked (188.0.0) with every Google API in it, and
`google-auth-library` brings six direct dependencies, an HTTP client and a metadata client among
them, into the path of a credential, for one grant type and one endpoint; this is about 330 lines and
no new dependency. The scope is `webmasters.readonly`, so the account could not
change anything in Search Console if it wanted to. The token endpoint is fixed in code, not read from
the key file.

## URL inspection

`gsc_inspect_url` asks Google about single pages: `POST
https://searchconsole.googleapis.com/v1/urlInspection/index:inspect` with `{ inspectionUrl, siteUrl,
languageCode }`. It is the same API and the same `webmasters.readonly` scope as the reports (the
method's reference lists `webmasters` and `webmasters.readonly`), so the service account needs no new
permission and the [setup](../admin/search-console-setup.md) does not change. The client's token
handling, retries, pacing and error mapping (`SearchConsoleClient.inspectUrl`) are the import's.

- **Input:** `url` and/or `urls` (up to 10): a path (`/de/parks/…`) or a park.fan URL, made
  `https://park.fan` plus the path (`www`, `http`, query and fragment folded away). Another host is
  refused before any request, since it would cost one and only earn a 400.
- **Quota:** Google allows 2,000 inspections a day and 600 a minute per property. Answers are cached in
  Redis for **6 hours** per URL (`cached: true`), a counter per Pacific day
  (`search-console:inspections:<day>`, Google's quota day) stops at **1,800**, and a URL past it comes
  back with an `error` saying so while cached ones still answer. The client's spacing keeps the minute
  limit. Without Redis the tool still works, uncached and uncounted.
- **Output** per URL: `verdict`, `coverageState`, `indexingState`, `robotsTxtState`, `pageFetchState`,
  `lastCrawlTime`, `crawledAs`, `googleCanonical`, `userCanonical`, `canonicalMatches`, `sitemap`,
  `referringUrls` (at most 10), and `mobileUsability` `{ verdict, issues }` and `richResults`
  `{ verdict, types }` when Google reports them; or `{ url, error }` when Google refuses that URL.
  Beside the results, `quota: { usedToday, dailyLimit }`. Everything Google wrote is under
  `visitorData`.

```json
{
  "quota": { "usedToday": 12, "dailyLimit": 1800 },
  "visitorDataNotice": "…",
  "visitorData": {
    "results": [
      {
        "url": "https://park.fan/de/parks/europe/germany/bruehl/phantasialand",
        "verdict": "PASS",
        "coverageState": "Submitted and indexed",
        "indexingState": "INDEXING_ALLOWED",
        "robotsTxtState": "ALLOWED",
        "pageFetchState": "SUCCESSFUL",
        "lastCrawlTime": "2026-10-27T03:12:44Z",
        "crawledAs": "MOBILE",
        "googleCanonical": "https://park.fan/de/parks/europe/germany/bruehl/phantasialand",
        "userCanonical": "https://park.fan/de/parks/europe/germany/bruehl/phantasialand",
        "canonicalMatches": true,
        "sitemap": ["https://park.fan/sitemap.xml"],
        "referringUrls": [],
        "cached": false
      }
    ]
  }
}
```

There is no admin REST route for it: the tool is the interface.

## MCP tools

`gsc_status`, `gsc_queries`, `gsc_pages`, `gsc_series` (read-only, any role; all but the status take
`type`) and `gsc_inspect_url`, one file each in
`src/mcp/tools/`, [listed with the others](../admin/mcp.md#tools). They wrap `SearchConsoleService`,
and everything Google or a searcher wrote (queries, paths, Google's error message) comes back under
`visitorData` beside a `visitorDataNotice`, as the site statistic's tools do. Their descriptions carry
what a model must know: Pacific days, the lag, anonymised queries, `partialBreakdown`, that a lower position is better,
and what `previous` and `change` compare.

## Environment

| Variable                   | Meaning                                                                             |
| -------------------------- | ----------------------------------------------------------------------------------- |
| `GSC_SERVICE_ACCOUNT_JSON` | the service account's key file, raw JSON or base64 of it; empty ⇒ the module is off |
| `GSC_SITE_URL`             | the property: `sc-domain:park.fan` or a URL prefix such as `https://park.fan/`      |
| `GSC_IMPORT_DAYS`          | days every run fetches again, 1–30, default 3                                       |

A key that does not parse, or a missing property, leaves the module off and puts the reason in
`lastError`; nothing throws at boot.

## Limits

- No real credentials were available when this was built: the client is tested against a mocked
  Google (unit and e2e), and the first import against the real API is the first time its field
  names, the 403 text and the row counts meet reality.
- The country is Google's alpha-3, not the site statistic's alpha-2.
- Search Console's own limits apply: 16 months of history, the 50,000-row daily cut, final data two
  to three days late.
- Frontend: the SEO page does not offer a type switch yet; every request without `type` is `web`.
  A follow-up adds a `type` selector to `lib/admin/search-console.ts` and
  `app/admin/traffic/_components/search-console-*`.
- A change in Google's data after a day was imported is picked up only while the day is among the
  recent ones (three by default).
