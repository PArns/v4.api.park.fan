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
`text/plain` body is read by `collectBodyMiddleware` on this route only; every other route still
refuses text. Body at most 16 KiB, as text or JSON (413), `{ v: 1, hits: [...] }` with 1–20 hits or
400; otherwise **204**, counted or not, `no-store`, and `Cross-Origin-Resource-Policy: cross-origin`
(helmet's `same-origin` would make the browser block the beacon's empty answer and log an error).

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

**No location-derived entity.** `nearby_parks_loaded` and every `compass_*` event lose their `entity`
on the way in, whatever a client sends (`LOCATION_DERIVED_EVENTS`): the park near a reader would be
where the reader is. Their props stay. The list is one constant; a new event of that kind goes there.

The server adds, per hit and never from the client: browser and OS family (`user-agent.util.ts`, seven
families each, no version, from the first 512 characters), the country from the request address
through the GeoLite2 lookup the API already runs (`GeoipService.lookupCountry`, `zz` when unknown),
and per visit the referrer **site** and **channel** (`referrer.util.ts`): `google.de` and
`www.google.com` are `google`; `search` (Google, Bing, DuckDuckGo, Ecosia, Yahoo, Yandex, Baidu,
Qwant, Startpage, Brave, Kagi, …), `ai` (ChatGPT, Perplexity, Gemini, Copilot, Claude, …), `social`,
`referral` (any other host, under its own name), `campaign` (a `utm_source`, i.e. a referrer without a
dot, or a `campaign`), `direct`. Only a search engine's own host is `search`: `mail.google.com`,
`docs.google.com`, `maps.google.de` or `news.google.com` send readers as any site does and are
`referral`. A new site is one line in `REFERRER_RULES`.

## Adding an event

Nothing here. The frontend calls `trackSiteEvent(name, props, entity)` and the admin finds the name,
its props and their values in the data. Two rules hold everywhere:

- **Never free text**: no search query, no name or note somebody typed, nothing that identifies a
  person. A property is a choice from a set the site defines (a surface, a tab, a position bucket, a
  slug). An entity only where it names what the reader looked at, never where they are.
- **`page_read` is the one name the API knows**: it feeds the public rankings and keeps their rules.
  `kind` is one of `article`, `news`, `park`, `attraction`, or the hit is dropped. A post is named by
  its `translationKey` in `key` (`PAGE_KEY_PATTERN`, else dropped) and keeps no entity, one sent is
  dropped; a park or ride is named by its UUID in `entity` (none, dropped) and keeps no other prop.

## Guards

There is no schema, so the guards are generic. Each counts per UTC day in Redis.

| Guard | Limit (`site-analytics.constants.ts`) | Past it |
| --- | --- | --- |
| hits per address (IPv4, IPv6 /56) | 3000 (`MAX_HITS_PER_ADDRESS_PER_DAY`) | dropped |
| hits per five-minute window, all readers | 5000 (`MAX_HITS_PER_WINDOW`) | dropped |
| distinct event names; `page_read` exempt | 300, 20 new per address (`SITE_GUARDS.names`) | a new name dropped |
| distinct page types | 100, 10 new per address | stored as `other` |
| distinct paths | 20,000, 200 new per address | stored as `(other)`, the entry too |
| distinct referring sites outside the known search engines, AI assistants and social networks, and `utm_source` values | 1000, 10 new per address | stored as `(other)`, channel kept |
| distinct campaigns | 200, 5 new per address | stored as `(other)` |
| property combinations (with the entity) per event; `page_read` exempt | 2000, 100 new per address | counted with props `{ "_overflow": "1" }`, no entity |
| `page_read` per reader and page | 3 (`MAX_VIEWS_PER_READER_PER_DAY`) | dropped |
| `page_read` per address and page, whatever the User-Agent | 10 (`MAX_VIEWS_PER_ADDRESS_AND_PAGE_PER_DAY`) | dropped |
| `page_read` per address | 200 (`MAX_VIEWS_PER_ADDRESS_PER_DAY`) | dropped |
| `page_read` of a park or ride that does not exist | the ids, cached ten minutes (`SiteEntityNames.knownIds`) | dropped |
| blocked names | the admin's list (`PUT /v1/admin/analytics/blocked-events/:name`) | dropped; each instance rereads the list within a minute |

Each value guard is one Lua script (`site-analytics-admit.ts`), atomic in Redis: a value in today's
set passes; **a value in yesterday's set joins without using the address's budget**, up to twice the
limit; any other joins while the set is under its limit and the address has budget left. So whoever
fills a set just after midnight cannot lock out the values the site really sends, and one address
cannot fill a set alone. Blocking a name also takes it out of today's names set and drops its
combinations, which frees its place at once. A carrier gateway shared by many readers can use up an
address's budget for new paths; its later new paths count under `(other)`. Every drop and every
overflow value is counted per reason (`DROP_REASONS`) in the day's drop counts, which the system
health report shows.

**The address** is `getRateLimitIp` ([which address that is](../deployment/edge-and-throttling.md#which-address-a-request-is)),
an IPv4 address whole and an IPv6 address as its **/56** for every cap (`ADDRESS_CAP_IPV6_PREFIX`):
providers delegate a /56 (or a /48) to one customer, who can take a new /64 per request, so capped per
/64 one customer would have 256 budgets. A **reader** (the visit, the visitor, three reads of one page)
is the /64 and the User-Agent. A request without an address shares one budget with every other such
request.

## Privacy

- **Not counted at all**: bots (`isBotUserAgent`, as for page views, a missing User-Agent included),
  `DNT: 1`, `Sec-GPC: 1`. In production only a request whose `Origin` (or `Referer`) is
  `https://park.fan` or `https://www.park.fan` (`isFromSite`); a preview, a local build or a script
  without one is answered 204 and not counted. The frontend asks for none of this to be bypassed: it
  sends nothing under Do Not Track, GPC, the privacy page's opt-out, or off the production hosts.
- **No address and no User-Agent is stored**, in Postgres, in Redis or in a log line
  (`LoggingInterceptor` leaves the address out for this path). The address becomes the bucket for the
  caps, the country, and a pseudonym; the User-Agent becomes two families and a pseudonym.
- **Every Redis key that stands for a reader or an address is pseudonymous for one UTC day**: an
  HMAC-SHA256 under that day's key, `HMAC(secret, day)` (`dailyHashKey`), where the secret is
  `ANALYTICS_HASH_SECRET` (falling back to `PAGE_VIEWS_HASH_SECRET`, then to a random per-process
  secret with a warning). Two days' keys of one reader have nothing in common, so nothing in Redis
  links one day to the next. Whoever holds the secret can still test a given address against a key of
  a day that has not expired; the secret lives in the environment, never in Redis. A visit that runs
  over midnight UTC is two visits.
- **What a visit key holds**: the visit's first page view (language, path, page type, viewport,
  referrer site, channel, campaign) and its page-view count, for 30 minutes after its last hit. No
  address, no User-Agent, no time.
- **What a row holds**: an hour, never a moment, and counts. A page view's row carries the page and
  the device (language, path, page type, viewport, browser, OS, country) and **nothing of the visit**;
  a visit's row carries the first page and the device plus the entry, referrer site, channel and
  campaign; an event's row carries the page, the device, the event and its props and entity, and
  nothing of the visit. So no row ties a page beyond a visit's first to where the reader came from.
  At today's traffic most rows still count one hit: what makes them anonymous is that they hold no
  identifier and no visit thread, not that they are large.

| Redis key family | Holds | Lives at most |
| --- | --- | --- |
| `site-analytics:visit:<hmac>` | a visit's first page view and count | 30 min after its last hit, and its UTC day |
| `site-analytics:cap:`, `read:`, `read-page:`, `read-addr:`, `new:<hmac>` | a reader's or an address's counts today | to the end of its UTC day + 1 min (≤ 24 h 1 min) |
| `site-analytics:uv:<day>:<scope>` | the day's visitor HyperLogLog | to the end of its day + 1 h (≤ 25 h) |
| `site-analytics:uvh:<hour>:<scope>` | the hour's visitor HyperLogLog | to the end of its day + 1 min |
| `site-analytics:rt:uv:`, `rt:pv:<minute>` | the minute's active visitors, page views | 10 min, 35 min |
| `site-analytics:guard:<guard>:<day>` | the day's admitted names, page types, paths, referrer sites, campaigns, combination hashes | to the end of the next day + 1 min (≤ 48 h); no reader in them |
| `site-analytics:window-hits:<window>` | all hits of a five-minute window | 15 min |
| `site-analytics:drops:<day>` | drop and overflow counts by reason | to the end of the next day + 1 min |
| `site-analytics:buf:<window>`, `…:flushing` | a window's counts until written | no TTL; written within about six minutes |
| `site-analytics:buf:<window>:parked` | a window the flusher gave up on | 7 days |

## Visits, visitors, bounce, realtime

- A **visit** starts on a page view whose visit key (the /64 and the User-Agent, under the day's key)
  does not exist; the key slides 30 minutes on every hit, events included. The page view that opens it
  writes a visit row (`visits = 1`) under its page, device and how the visit began.
- **Bounce**: the second page view of a visit adds `engaged = 1` to a visit row with the same
  dimensions as its start (in the hour of the second page view), so whatever a filter selects, its
  bounce rate is `1 − engaged / visits`.
- **Visitors**: a HyperLogLog per UTC day, all languages and per language (and per hour, for the
  one-day view), of the reader's daily pseudonym. Four runs of `snapshot-visitors` in the hour after
  midnight insert yesterday's counts into `site_visitor_days`, once (a day already written stays; a
  lost key is skipped, not written as zero); today and, until then, yesterday are read live. A reader
  who comes back on another day counts again, so a window's visitors are the sum of its days.
  Visitors cannot be split by anything but the language: with a filter they are `null`. HyperLogLog's
  standard error is 0.81 %.
- **Languages**: a visitor counts in every language they read a page in, a visit in the language of
  its first page. Under `locale=de` a reader who entered in English and read on in German is a visitor
  and no visit, so visitors can exceed visits there.
- **Realtime**: a HyperLogLog per minute (10 minutes TTL), active visitors = one `PFCOUNT` over the last
  five; a counter per minute, page views over the last 30. All languages or one; filters do not apply.
- **Redis down**: page views and events still count, held in this instance's memory (below) without
  guards, visits or visitors; visits, bounce, visitors and realtime undercount for that time.

## Storage: insert-only, no dead tuples

**Hot path in Redis.** A request costs three Redis round trips and no Postgres: the address's and the
window's caps; one pipeline for the guards, the visit, the visitors and the realtime minute; and one
`MULTI` with `HINCRBY` of the counts into the current five-minute window's hash,
`site-analytics:buf:<window>`, the window's id in `site-analytics:buf:windows`, the new visit's start
and the drop counts, so all of it lands or none. A field is a measure letter (`v` views, `s` visits,
`e` engaged, `n` events) followed by the row's dimensions as JSON, so the flusher rebuilds the row
from the field alone. **The buffers have no TTL**: this Redis runs `volatile-lru`, which only evicts
keys that have one.

**Redis away.** The counts go into a buffer in the instance's memory (`FallbackBuffer`), at most
5000 fields (`FALLBACK_MAX_FIELDS`), which a timer writes to Postgres once a minute, a transaction per
window and without a marker. What does not fit is dropped and counted (`dropped:fallback`), and the
counts reach Redis's drop counts with the next request that gets through. A `MULTI` field Redis
refuses goes the same way, that field only, so nothing is counted twice. So an outage costs Postgres a
small transaction per window and minute, whatever the traffic.

**The flusher** (`site-analytics` / `flush`, every five minutes at minute 1, 6, 11, …;
`SiteAnalyticsStorageService.flush`) takes a Redis lock (`SET NX PX`, 4 minutes) so one instance
flushes, and then for every window that ended at least a minute ago, oldest first:

1. claims it: `RENAME` to `…:flushing` and a claim id in the hash (`HSETNX ~claim`);
2. reads it in `HSCAN` steps of 1000 fields;
3. writes it in one transaction together with its claim's marker row in `site_analytics_flushes`
   (`ON CONFLICT (bucket, claim) DO NOTHING`), in `INSERT … SELECT FROM unnest(…)` statements of at
   most 5000 rows;
4. deletes the claimed key and the window's id, and puts the id back if a late count made a new key
   for the window in the meantime: that key is claimed anew on the next run, under a new claim.

A crash leaves the claimed key, which the next run writes under the same claim, so a claim is never
written twice. A window that fails is retried by the next runs while the others go on; after 12 failed
attempts (`MAX_FLUSH_ATTEMPTS`, an hour) it is parked under `…:parked` for a week and listed in the
health report. Reports lag by one to six minutes; realtime does not.

**The health report** (`GET /v1/admin/system-health`, and the MCP `system_health` tool) has a
`siteAnalytics` section (`readSiteAnalyticsBacklog`): windows waiting, those ready for the next run,
the oldest one's age, parked windows, the last write, and today's and yesterday's drop counts by
reason; `healthy` is false once a window waits past three runs or one is parked. A failing flush job
also reaches the data-quality report's failing jobs.

**The tables** are TimescaleDB hypertables (`HYPERTABLES`), never updated and never deleted from:

| Table | Row | Compression |
| --- | --- | --- |
| `site_pageviews` | the UTC hour, `locale`, `path`, `page_type`, `viewport`, `browser`, `os`, `country`, `entry`, `referrer`, `channel`, `campaign`; `views`, `visits`, `engaged`. A page view's row has the last four empty and `views`; a visit's row has them and `visits`, `engaged` | 7-day chunks, compressed after 7 days, segment by `locale`, order by `bucket DESC` |
| `site_events` | the UTC hour, `name`, the page dimensions, `props_key` (`k=v` in key order, `&`-joined), `props` (`jsonb`), `entity`; `count` | the same, segment by `name` |
| `site_analytics_flushes` | the written claims of each window | 30-day chunks |

A row's `bucket` is its hour (`bucketOf`); the flusher still writes every five minutes, so an hour
holds up to twelve rows per combination, summed when read. The nightly `drop-old-chunks` drops chunks
wholly older than 400 days (`RETENTION_DAYS`) with `drop_chunks` under a 5-second `lock_timeout`, one
table at a time; a chunk it cannot lock waits for the next night rather than queueing the API behind it
([lock queues](../troubleshooting/timescaledb-lock-queues.md)). A dropped chunk is a dropped table:
nothing for VACUUM. `site_visitor_days` (seven rows a day), `site_blocked_events` and
`site_entity_merges` are small plain tables.

**Schema.** The entities declare the bucket index `create_hypertable` makes
(`site_pageviews_bucket_idx`, `site_events_bucket_idx`) and the `props` default as a plain `{}`: with
the default as an SQL expression, `synchronize` altered the column on every start, an `ACCESS
EXCLUSIVE` lock on the hypertable. The e2e suite asserts that `synchronize` finds nothing to change in
these tables after the hypertables are made.

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

**The window before** (`previous`) is the same number of days right before, and ends at the same hour
of its last day as now: today is under way, so a change compares today so far with the same part of a
day, not with a whole one (the hour itself is taken whole on both sides). Visitors of the window before
count its last day whole, since visitors are per day. Over 180 days (`MAX_COMPARABLE_DAYS`) there is
no window before, since most of it would lie past the retention: `previous` and every change are
`null`, and the trends report refuses the window.

**Visit dimensions** (`entry`, `referrer`, `channel`, `campaign`) live only on the rows that record a
visit. A breakdown by one counts visits started (`measure: "visits"`, a view being a visit) and
carries a bounce rate. A filter on one selects visits by how they began:

- the overview reports visits and the bounce rate of those visits; page views, events and views per
  visit are `null`, since no page view or event row carries where its visit came from;
- a breakdown by any dimension counts visits started (`measure: "visits"`): `dim=path` with
  `channel:search` lists the pages search visits **entered** on, not every page they read later;
  `dim=country` the countries of search visits;
- landing pages, trends and uncovered read visits anyway and take it;
- the reports that count page views or events (not-found, events, one event, crosstab, search) answer
  **400**.

The other dimensions filter every kind of row. A direct visit has no referrer and none is listed.
`(other)` is the value of everything past a day's guard (see Guards).

| Endpoint | Answers |
| --- | --- |
| `POST /v1/analytics/collect` | the beacon (above) |
| `GET /v1/admin/analytics/overview` | page views, visits, visitors, events, views per visit, bounce rate; previous; series; realtime |
| `GET /v1/admin/analytics/breakdown?dim=&limit=` | views or visits (`measure`) per value of one dimension, share, bounce rate when counting visits |
| `GET /v1/admin/analytics/events` | every name seen in the window or the one before, and every blocked one (`blocked: true`), with count, previous, series |
| `GET /v1/admin/analytics/events/:name` | total, every prop key seen with its 50 top values and `otherCount`, the 50 top combinations, by page type and viewport, the 50 top entities (name and frontend path; `kind` null once gone), series |
| `GET /v1/admin/analytics/events/:name/crosstab?row=&col=` | one event by two sides: `page_type`, `viewport`, `locale`, `day`, `path`, `country`, `browser`, `os` or `prop:<key>`; 30 values per side by count, the rest `__rest__` (a value nothing real can be: a page type may well be `other`), an empty value `(none)`; `day` keeps every day |
| `GET /v1/admin/analytics/search` | the `search_*` events by the `surface` values seen: opened, typed, clicks, dead ends, `clickRate` (at most 1: the search page counts one opening per visit to it, its results are clicked many times), `noResultRate`; by page type and viewport; result kinds, positions, browse items; series |
| `GET /v1/admin/analytics/seo/landing-pages?channel=search&limit=` | the pages visits from one channel entered on: visits, previous, change, bounce rate, top 5 sources |
| `GET /v1/admin/analytics/seo/trends?channel=search` | rising and falling landing pages, 25 each, at least 10 visits over both windows; `days` at most 180 |
| `GET /v1/admin/analytics/seo/not-found` | paths that showed `pageType: not_found`, with the referrers of the visits that **entered** on them (`visits` each): a Google result pointing at a dead page shows `google`; a broken link inside the site brings views and no referrer, whatever the visit came from |
| `GET /v1/admin/analytics/seo/uncovered?kind=park\|attraction` | parks or rides no search visit entered on: matched on the frontend path in any language, so a ride renamed since shows as uncovered |
| `GET`, `PUT`, `DELETE /v1/admin/analytics/blocked-events[/:name]` | the block list; writes need `editor` and are audited |

All admin routes need a session (`AdminAuthGuard`, any role) and answer `private, no-store`.
`SiteAnalyticsService` and `SiteAnalyticsSeoService` return exactly these shapes and are exported for
the admin's MCP tools.

## Measurements

Measured 2026-10-28 on PostgreSQL 18.1 with TimescaleDB 2.24.0 in a scratch container (2 CPUs, 3 GB,
`shared_buffers` 512 MB) on a host shared with other builds, so the times are upper bounds. Data: 90
synthetic days at ten times today's traffic, ~16,000 page views (40 % of them opening a visit) and
~19,000 events a day, summed per five-minute window and stored under the hour as the flusher does,
power-law spreads over 150 parks, 5,000 rides, 900 posts, six languages, 80 countries and the channel
mix of today's referrers.

**Size.** The 1.44 M page views made 2.02 M rows (1.44 M page-view rows, 0.58 M visit rows): at this
traffic a five-minute window rarely holds two hits with the same dimensions. Compressed, 311 MB became
33 MB (9.4×) and the events' 1.73 M rows 361 MB → 27 MB (13.5×). A year at ten times today's traffic
comes to about **410 MB** with this week's two uncompressed chunks (156 MB page views and 128 MB events
compressed, 120 MB uncompressed), and stays there: chunks past 400 days are dropped.

**Write.** 20,000 beacons (a page view and 0–2 events each, the day's 148 windows so far) through the
real collector, its Lua guards included: `collect()` p50 0.95 ms, p95 2.5 ms, p99 6.8 ms, Redis only;
Redis at 18.2 MB with every visit key of the run still alive. The flusher wrote the 148 windows,
29,663 page-view and 19,582 event rows, in 2.8 s, **19 ms a window**; `pg_stat_user_tables` over the
chunks read 0 updates, 0 deletes and **0 dead tuples** afterwards.

**Reads** (median of three after one run, ms; 10× traffic):

| Report | 1 day | 30 days | 90 days |
| --- | --- | --- | --- |
| overview / `filter=country:de` / `filter=channel:search` | 8 / 9 / 8 | 226 / 125 / 85 | 286 / 182 / 193 |
| breakdown `path` / under `channel:search` | 1 / 2 | 267 / 122 | 571 / 244 |
| breakdown `referrer` / `country` | 2 / 1 | 58 / 87 | 140 / 228 |
| events | 9 | 237 | 475 |
| event `tab_changed` | 5 | 89 | 276 |
| crosstab | 1 | 39 | 97 |
| search | 2 | 108 | 289 |
| SEO landing pages / trends / not found | 6 / 10 / 1 | 277 / 253 / 145 | 592 / 369 / 469 |
| `/page-views/top` news / attractions (7, 30, 90 days) | | 14 / 19, 47 / 52, 79 / 137 | |
| admin page-views (7, 30, 90 days) | | 85, 382, 1,421 | |

The days are grouped with `time_bucket` and printed in code: grouping by `to_char(date_trunc(…))` per
row made a 60-day event series 569 ms instead of 75. A report reads every row of its window once, so
a year's report takes about four times the 90-day figure; at today's traffic (a tenth) it is a few
hundred milliseconds. If year views become the daily tool, insert-only daily rollups written by the
nightly job are the next step.

## Limits

- Visitors are per day and language only; with any other filter they are `null`.
- Page views and events cannot be filtered by how their visit began (see Reports).
- A `utm_source` is told from a referrer host by its missing dot.
- With Redis down the guards, visits and visitors are off and an instance holds at most 5000 fields in
  memory; past that, hits are dropped and counted.
- The first 512 characters of a User-Agent decide browser, OS and bot.
