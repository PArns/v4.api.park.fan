# Site statistic (`/v1/analytics/collect`, `/v1/admin/analytics/*`)

Our own cookieless, aggregate-only statistic of park.fan: page views, visits, visitors, any event the
site sends, and the admin's reports on them, Umami-style filters and an SEO view included. **PAR-1151.**
The frontend half (the beacon and `trackSiteEvent`) is `lib/analytics/site-analytics.ts` in park.fan.
The most-read rankings run on it too ([page views](page-views.md)).

## Why our own

Umami Cloud bills every event property, so the custom events (which search is used, from which page,
which tab, which filter) had to be few and thin. Here a property costs nothing, and the questions an
admin or Claude (through the admin's MCP tools) asks are a GROUP BY. It counts **hits only**: no
timings, no Web Vitals, no session length. Umami keeps the page views and the two Web Vitals events.

## Wire format

`POST /v1/analytics/collect`, JSON as `Content-Type: text/plain;charset=UTF-8` (what
`navigator.sendBeacon` sends, so the browser asks no CORS preflight) or `application/json`. The
`text/plain` body is read by `textBodyMiddleware` on this route only; every other route still refuses
text. Body at most 16 KiB (413), `{ v: 1, hits: [...] }` with 1–20 hits or 400; otherwise **204**,
counted or not, `no-store`.

| Field | Hit | Rule (`site-analytics.hits.ts`) | When it fails |
| --- | --- | --- | --- |
| `t` | all | `pageview` or `event` | hit dropped |
| `locale` | all | one of the six | hit dropped |
| `path` | all | `^/(en\|de\|nl\|fr\|es\|it)(/[a-z0-9][a-z0-9._~-]{0,99}){0,8}$`, first segment = `locale` | hit dropped |
| `pageType` | all | `^[a-z][a-z0-9_]{0,31}$` | hit dropped |
| `viewport` | all | `mobile`, `tablet`, `desktop` | hit dropped |
| `referrer` | page view | `^[a-z0-9][a-z0-9.-]{0,99}$`, not park.fan; a host, or `utm_source` | referrer dropped |
| `campaign` | page view | `utm_campaign`, lowercased, `^[a-z0-9][a-z0-9._+-]{0,99}$` | campaign dropped |
| `name` | event | `^[a-z][a-z0-9_]{1,47}$` | hit dropped |
| `props` | event | up to 8 in key order; key `^[a-z][a-z0-9_]{0,31}$`; value a finite number, a boolean or a string `^[\p{L}\p{N}][\p{L}\p{N} ._:/+#-]{0,119}$` | that prop dropped |
| `entity` | event | lowercase UUID of a park, ride, show or restaurant | entity dropped |

The server adds, per hit and never from the client: browser and OS family (`user-agent.util.ts`, seven
families each, no version), the country from the request address through the GeoLite2 lookup the API
already runs (`GeoipService.lookupCountry`, `zz` when unknown), and per visit the referrer **site** and
**channel** (`referrer.util.ts`): `google.de` and `www.google.com` are `google`; `search` (Google, Bing,
DuckDuckGo, Ecosia, Yahoo, Yandex, Baidu, Qwant, Startpage, Brave, Kagi, …), `ai` (ChatGPT,
Perplexity, Gemini, Copilot, Claude, …), `social`, `referral` (any other host, under its own name),
`campaign` (a `utm_source`, i.e. a referrer without a dot, or a `campaign`), `direct`. A new site is
one line in `REFERRER_RULES`.

## Adding an event

Nothing here. The frontend calls `trackSiteEvent(name, props, entity)` and the admin finds the name,
its props and their values in the data. Two rules hold everywhere:

- **Never free text**: no search query, no name or note somebody typed, nothing that identifies a
  person. A property is a choice from a set the site defines (a surface, a tab, a position bucket, a
  slug).
- **`page_read` is the one name the API knows**: it feeds the public rankings and keeps their rules —
  `kind` one of `article`, `news`, `park`, `attraction`; a post named by its `translationKey`
  (`PAGE_KEY_PATTERN`) and no entity, a park or ride by its UUID in `entity` and nothing else;
  otherwise the hit is dropped.

## Guards

There is no schema, so the guards are generic. All count per UTC day in Redis sets and counters, and
let everything through when Redis is away.

| Guard | Limit (`site-analytics.constants.ts`) | Past it |
| --- | --- | --- |
| hits per address bucket (IPv4, IPv6 /64) | 3000 (`MAX_HITS_PER_ADDRESS_PER_DAY`) | dropped |
| distinct event names | 300 (`MAX_EVENT_NAMES_PER_DAY`) | a new name dropped |
| distinct page types | 100 (`MAX_PAGE_TYPES_PER_DAY`) | stored as `other` |
| distinct property combinations (with the entity) per event | 2000 (`MAX_PROP_COMBINATIONS_PER_EVENT_PER_DAY`) | counted with props `{ "_overflow": "1" }`, no entity; `page_read` exempt |
| `page_read` per reader and page | 3 (`MAX_VIEWS_PER_READER_PER_DAY`) | dropped |
| `page_read` per address bucket | 500 (`MAX_VIEWS_PER_ADDRESS_PER_DAY`) | dropped |
| blocked names | the admin's list (`PUT /v1/admin/analytics/blocked-events/:name`) | dropped; each instance rereads the list within a minute |

A set guard is `SADD` then `SCARD` in one pipeline; a new member past the limit is taken out again
(`SREM`) and the hit treated as over. Two new members racing at the limit can both be refused.

## Privacy

- **Not counted at all**: bots (`isBotUserAgent`, as for page views, a missing User-Agent included),
  `DNT: 1`, `Sec-GPC: 1`. In production only a request whose `Origin` (or `Referer`) is
  `https://park.fan` or `https://www.park.fan`; a preview, a local build or a script without one is
  answered 204 and not counted. The frontend asks for none of this to be bypassed: it sends nothing
  under Do Not Track, GPC, the privacy page's opt-out, or off the production hosts.
- **No address and no User-Agent is stored**, in Postgres, in Redis or in a log line
  (`LoggingInterceptor` leaves the address out for this path). The address becomes the bucket for the
  cap, the country, and an HMAC; the User-Agent becomes two families and an HMAC.
- **Every Redis key that stands for a reader is an HMAC-SHA256** under `ANALYTICS_HASH_SECRET`
  (falling back to `PAGE_VIEWS_HASH_SECRET`, then to a random per-process secret with a warning),
  prefixed `site-analytics|` so it never equals a page-view key. The secret lives in the environment,
  never in Redis.
- **What a visit key holds**: the visit's first page view (language, path, page type, viewport,
  referrer site, channel, campaign) and its page-view count, for 30 minutes after its last hit. No
  address, no User-Agent, no time.
- Every key expires: the visit 30 minutes after its last hit, the daily caps and guard sets a minute
  after their UTC day, the day's visitor HyperLogLog three days after it, the hour's at the end of the
  next hour past the day, the realtime minute after 10 and 35 minutes. Only the flush buffers carry no
  TTL (below), and they hold counts.

## Visits, visitors, bounce, realtime

- A **visit** starts on a page view whose visit key (address bucket and User-Agent) does not exist; the
  key slides 30 minutes on every hit, events included. Its first page view's row counts `visits = 1`,
  and every later row of the visit carries the visit's `entry`, `referrer`, `channel` and `campaign`.
- **Bounce**: the second page view of a visit adds `engaged = 1` under the dimensions of the visit's
  first page view (in the current window), so whatever a filter selects, its bounce rate is
  `1 − engaged / visits`.
- **Visitors**: a HyperLogLog per UTC day, all languages and per language (and per hour, for the
  one-day view), of HMAC(day, bucket, User-Agent). The nightly `snapshot-visitors` inserts the last two
  finished days into `site_visitor_days`, once (a day already written stays; a lost key is skipped, not
  written as zero); recent days are read live. A reader who comes back on another day counts again,
  so a window's visitors are the sum of its days. Visitors cannot be split by anything but the
  language: with a filter they are `null`. HyperLogLog's standard error is 0.81 %.
- **Realtime**: a HyperLogLog per minute (10 minutes TTL), active visitors = one `PFCOUNT` over the last
  five; a counter per minute, page views over the last 30. All languages or one; filters do not apply.
- **Redis down**: page views and events still count (written straight to Postgres, below); visits,
  bounce, visitors and realtime undercount for that time, and the guards let everything through.

## Storage: insert-only, no dead tuples

**Hot path in Redis.** A request costs three Redis round trips and no Postgres: the daily cap; one
pipeline for the guards, the visit, the visitors and the realtime minute; and `HINCRBY` of the counts
into the current five-minute window's hash, `site-analytics:buf:<window>`, plus the window's id in
`site-analytics:buf:windows`. A field is a measure letter (`v` views, `s` visits, `e` engaged, `n`
events) followed by the row's dimensions as JSON, so the flusher rebuilds the row from the field
alone. **The buffers have no TTL**: this Redis runs `volatile-lru`, which only evicts keys that have
one. When the buffer write fails, the request's counts are written to Postgres directly (no marker,
beside whatever the flusher writes for that window), so a Redis outage loses no hit.

**The flusher** (`site-analytics` / `flush`, every five minutes at minute 1, 6, 11, …;
`SiteAnalyticsStorageService.flush`): takes a Redis lock (`SET NX PX`, 4 minutes) so one instance
flushes; claims every window that ended at least a minute ago by `RENAME` to
`…:flushing`; writes it in one transaction together with its marker row in `site_analytics_flushes`
(`ON CONFLICT DO NOTHING`); deletes the claimed key and the window id after the commit. A crash leaves
the claimed key, which the next run writes; a window whose marker exists is not written again. Counts
that reach a window after it was claimed (a request slower than a minute) are dropped with it.
Reports therefore lag by one to six minutes; realtime does not.

**The tables** are TimescaleDB hypertables (`HYPERTABLES`), never updated and never deleted from:

| Table | Row | Compression |
| --- | --- | --- |
| `site_pageviews` | window, `locale`, `path`, `page_type`, `viewport`, `browser`, `os`, `country`, the visit's `entry`, `referrer`, `channel`, `campaign`; `views`, `visits`, `engaged` | 7-day chunks, compressed after 7 days, segment by `locale`, order by `bucket DESC` |
| `site_events` | window, `name`, the same dimensions, `props_key` (`k=v` in key order, `&`-joined), `props` (`jsonb`), `entity`; `count` | the same, segment by `name` |
| `site_analytics_flushes` | the written windows | 30-day chunks |

Every dimension sits on every row ("combined rows"), so any dimension filters any other and every
breakdown is one GROUP BY. In practice that is close to a row per hit, which compression absorbs
(below). The nightly `drop-old-chunks` drops chunks wholly older than 400 days (`RETENTION_DAYS`) with
`drop_chunks` under a 5-second `lock_timeout`, one table at a time; a chunk it cannot lock waits for
the next night rather than queueing the API behind it
([lock queues](../troubleshooting/timescaledb-lock-queues.md)). A dropped chunk is a dropped table:
nothing for VACUUM. `site_visitor_days` (seven rows a day), `site_blocked_events` and
`site_entity_merges` are small plain tables.

**Merges.** A merged park, ride, show or restaurant is not rewritten in the compressed rows: the merge
records `loser → winner` in `site_entity_merges` (`recordEntityMerge`, a `custom` entry of all four
dependency lists in `src/parks/utils/merge-dependencies.ts`), always pointing at the final survivor,
and the reports and rankings read `COALESCE(winner_id, entity)`.

## Reports

Days are UTC days, not a park's: a reader belongs to no park
([park timezone](../rules/park-timezone.md)). Every admin report takes `days` (1–365, with today),
`locale`, and `filter=<dim>:<value>` (repeatable, AND; dim ∈ `path`, `page_type`, `entry`,
`referrer`, `channel`, `campaign`, `country`, `viewport`, `browser`, `os`, `locale`), echoes `from`,
`to`, `days`, `locale`, `filters`, and has a series point per day, or per hour of today when `days=1`
(`day` then holds `2026-10-28T07:00:00Z`).
`previous` is the same number of days right before. A visit dimension (`entry`, `referrer`,
`channel`, `campaign`) counts visits started, a view being a visit, and carries a bounce rate; the
others count page views, with the visits that started on the value beside them. A direct visit has no
referrer and none is listed.

| Endpoint | Answers |
| --- | --- |
| `POST /v1/analytics/collect` | the beacon (above) |
| `GET /v1/admin/analytics/overview` | page views, visits, visitors, events, views per visit, bounce rate; previous; series; realtime |
| `GET /v1/admin/analytics/breakdown?dim=&limit=` | views and visits per value of one dimension, share, bounce rate on visit dimensions |
| `GET /v1/admin/analytics/events` | every name seen in the window or the one before, and every blocked one (`blocked: true`), with count, previous, series |
| `GET /v1/admin/analytics/events/:name` | total, every prop key seen with its 50 top values and `otherCount`, the 50 top combinations, by page type and viewport, the 50 top entities (name and frontend path; `kind` null once gone), series |
| `GET /v1/admin/analytics/events/:name/crosstab?row=&col=` | one event by two sides: `page_type`, `viewport`, `locale`, `day`, a filter dimension or `prop:<key>`; 30 values per side by count, the rest `other`; `day` keeps every day |
| `GET /v1/admin/analytics/search` | the `search_*` events by the `surface` values seen: opened, typed, clicks, dead ends, `clickRate`, `noResultRate`; by page type and viewport; result kinds, positions, browse items; series |
| `GET /v1/admin/analytics/seo/landing-pages?channel=search&limit=` | the pages visits from one channel entered on: visits, previous, change, bounce rate, top 5 sources |
| `GET /v1/admin/analytics/seo/trends?channel=search` | rising and falling landing pages, 25 each, at least 10 visits over both windows |
| `GET /v1/admin/analytics/seo/not-found` | paths that showed `pageType: not_found`, with their visits' referrers (a broken link inside the site has none) |
| `GET /v1/admin/analytics/seo/uncovered?kind=park\|attraction` | parks or rides no search visit entered on: matched on the frontend path in any language, so a ride renamed since shows as uncovered |
| `GET`, `PUT`, `DELETE /v1/admin/analytics/blocked-events[/:name]` | the block list; writes need `editor` and are audited |

All admin routes need a session (`AdminAuthGuard`, any role) and answer `private, no-store`.
`SiteAnalyticsService` and `SiteAnalyticsSeoService` return exactly these shapes and are exported for
the admin's MCP tools.

## Measurements

Measured 2026-10-28 on PostgreSQL 18.1 with TimescaleDB 2.24.0 in a scratch container (2 CPUs, 3 GB,
`shared_buffers` 512 MB) on a host shared with other builds, so the times are upper bounds. Data: a
synthetic year at ten times today's traffic, ~16,000 page views and ~19,000 events a day, one row per
hit in five-minute buckets, power-law spreads over 150 parks, 5,000 rides, 900 posts, six languages, 80
countries and the channel mix of today's referrers.

**Size.** 5.84 M page-view rows were 1,241 MB uncompressed and 137 MB compressed (9×); 7.01 M event
rows 2,105 MB and 168 MB (12.5×). With this week's two uncompressed chunks a year at ten times
today's traffic is **430 MB**, and stays there: chunks past 400 days are dropped.

**Write.** 20,000 beacons (a pageview and 0–2 events each, the day's 119 windows so far) through the
real collector: `collect()` p50 0.8 ms, p95 3.4 ms, p99 11 ms, Redis only; Redis at 17.6 MB with
every visit key of the run still alive. The flusher wrote the 119 windows, 23,692 page-view and 19,651
event rows, in 2.6 s, **22 ms a window**. Afterwards `pg_stat_user_tables` over the chunks read 0
updates, 0 deletes and **0 dead tuples**.

**Reads** (median of three after one run, ms; 10× traffic):

| Report | 1 day | 30 days | 365 days |
| --- | --- | --- | --- |
| overview | 47 | 230 | 1,621 |
| overview, `filter=channel:search` | 27 | 255 | 1,226 |
| breakdown `path` | 1 | 246 | 2,824 |
| breakdown `referrer` / `country` | 4 / 2 | 69 / 87 | 950 / 953 |
| events | 44 | 314 | 2,133 |
| event `tab_changed` | 3 | 110 | 1,289 |
| crosstab | 2 | 43 | 428 |
| search | 3 | 177 | 2,677 |
| SEO landing pages / trends / not found | 13 / 25 / 1 | 382 / 461 / 172 | 2,657 / 1,695 / 1,637 |
| `/page-views/top` news / attractions (7, 30, 90 days) | | 21–197 / 31–151 | |
| admin page-views (7, 30, 90 days) | | 194 / 281 / 1,370 | |

The days are grouped with `time_bucket` and printed in code: grouping by `to_char(date_trunc(…))` per
row made the 60-day event series 569 ms instead of 75, and the year's overview 8.3 s instead of 1.6.
A year's report reads every row of the year once; at today's traffic (a tenth) that is a few hundred
milliseconds. If year views become the daily tool, insert-only daily rollups written by the nightly
job are the next step.

## Limits

- Visitors are per day and language only; with any other filter they are `null`.
- Counts reaching a window after the flusher claimed it are lost (needs a request slower than a
  minute).
- A `utm_source` is told from a referrer host by its missing dot.
- With Redis down the guards and the visit are off; hits count, visits do not.
