# Site statistic (`/v1/analytics/collect`, `/v1/admin/analytics/*`)

Our own cookieless, aggregate-only statistic of park.fan: page views, visits, visitors, any event the
site sends, and the admin's reports on them, Umami-style filters and an SEO view included. **PAR-1151.**
The frontend half (the beacon and `trackSiteEvent`) is `lib/analytics/site-analytics.ts` in park.fan.
The most-read rankings run on it too ([page views](page-views.md)).

## Why our own

Umami Cloud bills every event property, so the custom events (which search is used, from which page,
which tab, which filter) had to be few and thin. Here a property costs nothing, and the questions an
admin or Claude (through the admin's MCP tools) asks are a GROUP BY. It counts hits, and per page
view the seconds the page was in the foreground; no Web Vitals, and no time the browser sends. Umami
keeps the page views and the two Web Vitals events.

## Wire format

`POST /v1/analytics/collect`, JSON as `Content-Type: text/plain;charset=UTF-8` (what
`navigator.sendBeacon` sends, so the browser asks no CORS preflight) or `application/json`. The
`text/plain` body is read by `collectBodyMiddleware` on this route only; every other route still
refuses text. Body at most 16 KiB, as text or JSON (413), `{ v: 1, hits: [...] }` with 1–20 hits or
400; otherwise **204**, counted or not, `no-store`, and `Cross-Origin-Resource-Policy: cross-origin`
(helmet's `same-origin` would make the browser block the beacon's empty answer and log an error).

| Field | Hit | Rule (`site-analytics.hits.ts`) | When it fails |
| --- | --- | --- | --- |
| `t` | all | `pageview`, `event` or `engagement` | hit dropped |
| `locale` | all | one of the six | hit dropped |
| `path` | all | `^/(en\|de\|nl\|fr\|es\|it)(/[a-z0-9][a-z0-9._~-]{0,99}){0,8}$`, first segment = `locale` | hit dropped |
| `pageType` | all | `^[a-z][a-z0-9_]{0,31}$` | hit dropped |
| `viewport` | all | `mobile`, `tablet`, `desktop` | hit dropped |
| `referrer` | page view | `^[a-z0-9][a-z0-9.-]{0,99}$`, not park.fan; a host, or `utm_source` | referrer dropped |
| `campaign` | page view | `utm_campaign`, lowercased, `^[a-z0-9][a-z0-9._+-]{0,99}$` | campaign dropped |
| `name` | event | `^[a-z][a-z0-9_]{1,47}$` | hit dropped |
| `props` | event | up to 8 in key order; key `^[a-z][a-z0-9_]{0,31}$`; value a finite number, a boolean or a string `^[\p{L}\p{N}][\p{L}\p{N} ._:/+#-]{0,119}$` | that prop dropped |
| `entity` | event | lowercase UUID of a park, ride, show or restaurant | entity dropped |
| `seconds` | engagement | a whole number, 1–1800 (`MAX_ENGAGEMENT_SECONDS`) | hit dropped |

**Engagement.** The frontend times each page view while the tab is visible and reports it as
`{ t: "engagement", locale, path, pageType, viewport, seconds }`, with the page's later hits or when
the page is hidden; a page view may report in parts, which add up. It is a hit like any other: it
counts against the address's cap and passes the path and page-type guards. Its seconds go to the
page's row (`seconds`) and, while the visit's key knows how the visit began, to the visit's start row
(`visit_seconds`); without a visit (no address, Redis away, the visit already ended) to the page
only. The browser sends no time: the seconds land in the hour the report arrives in.

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
`referral`. A new site is one line in `REFERRER_RULES`. The **browser language** is the primary
subtag of the first `Accept-Language` tag, lowercased (`de-AT, en;q=0.8` → `de`), and `zz` for
anything that is not two letters (`browser-language.util.ts`). The site's language is the path's;
this one says what the browser asked for, and only the rows of `site_pageviews` carry it.

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

Each value guard is one Lua script (`ADMIT_SCRIPT` in `site-analytics-scripts.ts`), atomic in Redis: a value in today's
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
  (no log line of this API carries an address, [the rule](../rules/no-address-in-logs.md)). The address becomes the bucket for the
  caps, the country, and a pseudonym; the User-Agent becomes two families and a pseudonym.
- **Every Redis key that stands for a reader or an address is pseudonymous for one UTC day**: an
  HMAC-SHA256 under that day's key, `HMAC(secret, day)` (`dailyHashKey`), where the secret is
  `ANALYTICS_HASH_SECRET` (falling back to `PAGE_VIEWS_HASH_SECRET`, then to a random per-process
  secret with a warning). Two days' keys of one reader have nothing in common, so nothing in Redis
  links one day to the next. Whoever holds the secret can still test a given address against a key of
  a day that has not expired; the secret lives in the environment, never in Redis. A visit that runs
  over midnight UTC is two visits.
- **What a visit key holds** (`site-analytics:visit:<hmac>`, a hash): `c` how the visit began (its
  first page view's language, path, page type and viewport, the referrer site, channel and
  campaign), `n` its page views, `s` and `l` the server's clock in UTC seconds at its first and its
  last hit, and `x` the exit row its last page view would make (that page's language, path, page
  type and viewport, browser, OS, country and browser language, with how the visit began). It lives
  40 minutes past the last hit (`VISIT_KEY_TTL_SECONDS`) and is deleted when the flush ends the
  visit. No address, no User-Agent. The two times, and the open visits' score that repeats the last,
  are the only moments Redis keeps about a reader; they end with the visit and never reach Postgres,
  which stores only the difference, summed.
- **The open visits** (`site-analytics:visits:open`) are a sorted set of the visit pseudonyms, scored
  by the last hit, from which the flush picks the idle ones. A member goes when its visit ends, and
  the collector drops any member older than two hours (`OPEN_VISIT_MAX_AGE_SECONDS`), so even with
  the flusher stopped no pseudonym stays there longer.
- **What a row holds**: an hour, never a moment, and counts. A page view's row carries the page and
  the device (language, path, page type, viewport, browser, OS, country, browser language), `views`
  and `seconds`, and **nothing of the visit**; a visit's start row carries the first page and the
  device plus the entry, referrer site, channel and campaign, with `visits`, `engaged` and
  `visit_seconds`; an exit row carries the last page and the device plus how the visit began, with
  `exits` and `visit_span_seconds` (last hit minus first, summed); an event's row carries the page,
  the device without the browser language, the event and its props and entity, and nothing of the
  visit. So no row ties a page in the middle of a visit to where the reader came from; an exit row
  ties the last page to it, as the start row ties the first. At today's traffic most rows still
  count one hit: what makes them anonymous is that they hold no identifier and no visit thread, not
  that they are large.

| Redis key family | Holds | Lives at most |
| --- | --- | --- |
| `site-analytics:visit:<hmac>` | how a visit began, its page views, its first and last hit, its exit row | until the flush ends the visit (30 to 35 min after its last hit), at most 40 min after it |
| `site-analytics:visits:open` | the open visits' pseudonyms, scored by their last hit | a member until its visit ends; never older than 2 h |
| `site-analytics:cap:`, `read:`, `read-page:`, `read-addr:`, `new:<hmac>` | a reader's or an address's counts today | to the end of its UTC day + 1 min (≤ 24 h 1 min) |
| `site-analytics:uv:<day>:<scope>` | the day's visitor HyperLogLog | to the end of its day + 1 h (≤ 25 h) |
| `site-analytics:uvh:<hour>:<scope>` | the hour's visitor HyperLogLog | to the end of its day + 1 min |
| `site-analytics:rt:uv:`, `rt:pv:`, `rt:ev:<minute>:<scope>` | the minute's visitor HyperLogLog, page views, events | 35 min |
| `site-analytics:live:<list>:<minute>:<scope>` | the minute's page views by page, country and viewport, visits started by channel and referrer site, events by name; no reader in them | 35 min |
| `site-analytics:guard:<guard>:<day>` | the day's admitted names, page types, paths, referrer sites, campaigns, combination hashes | to the end of the next day + 1 min (≤ 48 h); no reader in them |
| `site-analytics:window-hits:<window>` | all hits of a five-minute window | 15 min |
| `site-analytics:drops:<day>` | drop and overflow counts by reason | to the end of the next day + 1 min |
| `site-analytics:buf:<window>`, `…:flushing` | a window's counts until written | no TTL; written within about six minutes |
| `site-analytics:buf:<window>:parked` | a window the flusher gave up on | 7 days |

## Visits, visitors, bounce, realtime

- A **visit** starts on a page view whose visit key (the /64 and the User-Agent, under the day's key)
  does not exist; every hit, events and engagements included, moves its last hit. The page view that
  opens it writes a visit row (`visits = 1`) under its page, device and how the visit began.
- A visit **ends** after 30 minutes without a hit (`VISIT_IDLE_SECONDS`). The flush finds it among
  the open visits, so it ends 30 to 35 minutes after its last hit, and a hit in between still belongs
  to it. Ending it adds one `exit` to the window of its last hit, under its last page view's
  dimensions and how it began, and `visit_span_seconds` = its last hit minus its first by the
  server's clock; one Lua script (`CLOSE_VISIT_SCRIPT`) checks that the last hit is still the one
  read, so a hit that arrives meanwhile keeps the visit open. A bounce's exit has its start's
  dimensions, so it adds to the start's row when both fall in one window. A visit whose key expired
  first (the flusher stopped for 40 minutes) loses its exit, counted as `dropped:exit`.
- **Time**: `avgTimeOnPageSeconds` = `seconds / views` and `avgVisitSeconds` =
  `visit_seconds / visits`, whole seconds, `null` with nothing to divide by. A page view that never reported its
  seconds (a page cached with the old beacon, a report lost, a tab closed before it was sent) counts
  as zero, so both are lower bounds.
- **Bounce**: the second page view of a visit adds `engaged = 1` to a visit row with the same
  dimensions as its start (in the hour of the second page view), so whatever a filter selects, its
  bounce rate is `1 − engaged / visits`.
- **Visitors**: a HyperLogLog per UTC day, all languages and per language (and per hour, for today's
  hours in an hourly series), of the reader's daily pseudonym. Four runs of `snapshot-visitors` in the hour after
  midnight insert yesterday's counts into `site_visitor_days`, once (a day already written stays; a
  lost key is skipped, not written as zero); today and, until then, yesterday are read live. A reader
  who comes back on another day counts again, so a window's visitors are the sum of its days.
  Visitors cannot be split by anything but the language: with a filter they are `null`. HyperLogLog's
  standard error is 0.81 %.
- **Languages**: a visitor counts in every language they read a page in, a visit in the language of
  its first page. Under `locale=de` a reader who entered in English and read on in German is a visitor
  and no visit, so visitors can exceed visits there.
- **Realtime**: a HyperLogLog per minute, active visitors = one `PFCOUNT` over the last five; a
  counter per minute, page views over the last 30. All languages or one; filters do not apply.
- **The live view** (`GET /v1/admin/analytics/live`) reads only Redis: the same minute keys, an
  events counter per minute, and per minute and language a hash per list (`LIVE_LISTS`) that the
  request's `MULTI` adds to, every key with a 35-minute TTL (`LIVE_KEY_TTL_SECONDS`). One pipeline
  per call, so an admin page may poll it every five seconds. Redis away answers 503 rather than
  zeros.
- **Redis down**: page views and events still count, held in this instance's memory (below) without
  guards, visits or visitors; visits, bounce, visitors and realtime undercount for that time.

## Storage: insert-only, no dead tuples

**Hot path in Redis.** A request costs three Redis round trips and no Postgres: the address's and the
window's caps; one pipeline for the guards, the visit, the visitors and the realtime minute; and one
`MULTI` with `HINCRBY` of the counts into the current five-minute window's hash,
`site-analytics:buf:<window>`, the window's id in `site-analytics:buf:windows`, the visit's key and
its place among the open visits, the live view's minute and the drop counts, so all of it lands or
none. A field is a measure letter (`v` views, `d` seconds, `s` visits, `e` engaged, `t` visit
seconds, `x` exits, `y` visit span seconds, `n` events) followed by the row's dimensions as JSON, so
the flusher rebuilds the row from the field alone. **The buffers have no TTL**: this Redis runs
`volatile-lru`, which only evicts keys that have one.

**Redis away.** The counts go into a buffer in the instance's memory (`FallbackBuffer`), at most
5000 fields (`FALLBACK_MAX_FIELDS`), which a timer writes to Postgres once a minute, a transaction per
window and without a marker. What does not fit is dropped and counted (`dropped:fallback`), and the
counts reach Redis's drop counts with the next request that gets through. A `MULTI` field Redis
refuses goes the same way, that field only, so nothing is counted twice. So an outage costs Postgres a
small transaction per window and minute, whatever the traffic.

**The flusher** (`site-analytics` / `flush`, every five minutes at minute 1, 6, 11, …;
`SiteAnalyticsStorageService.flush`) takes a Redis lock (`SET NX PX`, 4 minutes) so one instance
flushes, ends the visits idle for 30 minutes (`closeIdleVisits`, at most 20,000 a run), whose exits
go into the windows of their last hits, and then for every window that ended at least a minute ago,
oldest first:

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
the oldest one's age, parked windows, the visits not yet ended (`openVisits`), the last write, and
today's and yesterday's drop counts by reason; `healthy` is false once a window waits past three runs or one is parked. A failing flush job
also reaches the data-quality report's failing jobs.

**The tables** are TimescaleDB hypertables (`HYPERTABLES`), never updated and never deleted from:

| Table | Row | Compression |
| --- | --- | --- |
| `site_pageviews` | the UTC hour, `locale`, `path`, `page_type`, `viewport`, `browser`, `os`, `country`, `browser_language`, `entry`, `referrer`, `channel`, `campaign`; `views`, `seconds`, `visits`, `engaged`, `visit_seconds`, `exits`, `visit_span_seconds`. A page view's row has the four visit dimensions empty, `views` and `seconds`; a visit's start row has them and `visits`, `engaged`, `visit_seconds`; an exit row has them, its last page, `exits` and `visit_span_seconds` | 7-day chunks, compressed after 7 days, segment by `locale`, order by `bucket DESC` |
| `site_events` | the UTC hour, `name`, the page dimensions without the browser language, `props_key` (`k=v` in key order, `&`-joined), `props` (`jsonb`), `entity`; `count` | the same, segment by `name` |
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
([park timezone](../rules/park-timezone.md)). Every admin report takes a window, `locale`,
`interval`, and `filter=<dim>:<value>` (repeatable, AND; dim ∈ `path`, `page_type`, `country`,
`viewport`, `browser`, `os`, `locale`, `browser_language`, and the visit dimensions `entry`,
`referrer`, `channel`, `campaign`), and echoes `from`, `to`, `days`, `interval`, `locale`, `filters`.

**The window** is `days` (1–365, the last days with today; 30 without one) or `from` and `to`
(`YYYY-MM-DD`, both included, `to` today at the latest, at most 366 days). Both at once, one of
`from` and `to` alone, or a date that does not exist is a 400.

**The series** has a point per period of `interval`: `hour` (at most two days), `day`, `week` (ISO
weeks, from Monday, UTC) or `month`. Without one it is the hour for a one-day window, the day up to
92 days, the week up to 183 and the month beyond. A point's `day` is the period's start
(`2026-10-28`, or `2026-10-28T07:00:00Z` for an hour; a week's Monday; the first of a month), zeros
included; hours run up to the current one. A period the window cuts at either end, or that is still
under way (today, this hour), carries `partial: true`; a whole one has no `partial` field. The SQL
groups hours or days, and the code folds days into weeks and months. Visitors of an hour are known
for today only (the hour HyperLogLogs end with their day): an earlier day's hour has
`visitors: null`. A week's or a month's visitors are the sum of its days in the window, as the totals
count them.

**The window before** (`previous`) is the same number of days right before `from`. When the window
ends today it ends at the same hour of its last day as now: today is under way, so a change compares
today so far with the same part of a day, not with a whole one (the hour itself is taken whole on
both sides). A `from`–`to` window that ended before today compares with a whole window before.
Visitors of the window before count its last day whole, since visitors are per day. Over 180 days
(`MAX_COMPARABLE_DAYS`) there is no window before, since most of it would lie past the retention:
`previous` and every change are `null`, and the trends report refuses the window.

**Visit dimensions** (`entry`, `referrer`, `channel`, `campaign`) live only on the rows that record a
visit. A breakdown by one counts visits started (`measure: "visits"`, a view being a visit) and
carries a bounce rate and the average visit. A filter on one selects visits by how they began:

- the overview reports visits, the bounce rate and the average visit of those visits; page views,
  events, views per visit and time on page are `null`, since no page view or event row carries where
  its visit came from;
- a breakdown by any dimension counts visits started (`measure: "visits"`): `dim=path` with
  `channel:search` lists the pages search visits **entered** on, not every page they read later;
  `dim=country` the countries of search visits; `dim=exit` the pages search visits ended on;
- landing pages, trends and uncovered read visits anyway and take it;
- the reports that count page views or events (not-found, events, one event, crosstab, search, the
  traffic crosstab with `metric=pageviews`) answer **400**. Events carry no browser language either,
  so the event reports refuse `browser_language` too.

The other dimensions filter every kind of row. A direct visit has no referrer and none is listed.
`(other)` is the value of everything past a day's guard (see Guards).

**The figures.** `pageviews`, `visits` (started), `visitors`, `events`, `viewsPerVisit`,
`bounceRate`, `avgVisitSeconds` and `avgTimeOnPageSeconds` (see Visits) make up the overview's
`totals` and `previous`, and every point of its series. A breakdown by a page dimension lists, per
value, `views`, `visits`, `share` and `avgTimeOnPageSeconds`; by `path` also `exits` (visits that
ended there) and `exitRate` (`exits / views`, at most 1). A breakdown that counts visits lists
`bounceRate` and `avgVisitSeconds` instead. `dim=exit` counts visits ended (`measure: "exits"`), per
last page; `views` and `visits` both hold that count. Landing pages and trends carry
`avgVisitSeconds` of the window.

**The traffic crosstab** (`GET /v1/admin/analytics/crosstab?row=&col=&metric=`) draws traffic as the
event crosstab draws an event: `metric=pageviews` (the default) counts page views over two of the
page dimensions or `day`; `metric=visits` counts visits started over the page dimensions (the first
page's), the visit dimensions and `day`, and with `exit` on a side the visits ended, the page
dimensions then being the last page's. Same answer as the event crosstab, `metric` in place of
`name`: 30 values per side by count, the rest under `__rest__`, `(none)` for an empty value, `day`
stepping by the interval.

```json
{
  "from": "2026-10-22",
  "to": "2026-10-28",
  "days": 7,
  "interval": "day",
  "locale": null,
  "filters": [],
  "metric": "visits",
  "row": "channel",
  "col": "exit",
  "rows": ["search", "direct"],
  "cols": ["/de/news", "/de"],
  "cells": [{ "row": "search", "col": "/de/news", "count": 1 }],
  "total": 1
}
```

**The live view** (`GET /v1/admin/analytics/live?locale=`, any role, `no-store`) answers from Redis
only, for a page that polls every five seconds:

```json
{
  "at": "2026-10-28T12:34:56Z",
  "activeVisitors": 0,
  "perMinute": [
    {
      "minute": "2026-10-28T12:04:00Z",
      "pageviews": 0,
      "events": 0,
      "visitors": 0
    }
  ],
  "pages": [{ "path": "/de/…", "pageType": "park", "views": 0 }],
  "countries": [{ "country": "DE", "views": 0 }],
  "channels": [{ "channel": "search", "visits": 0 }],
  "referrers": [{ "referrer": "google", "visits": 0 }],
  "devices": [{ "viewport": "mobile", "views": 0 }],
  "events": [{ "name": "search_opened", "count": 0 }]
}
```

`perMinute` is the last 30 whole minutes and the current one, oldest first (31 points), a minute's
`visitors` distinct within that minute; `activeVisitors` the distinct visitors with a hit in the last
five minutes, the current one included. `pages` (top 10), `countries` (all, upper case, `ZZ` unknown)
and `devices` count the page views of those five minutes; `channels` (all) and `referrers` (top 10)
the visits started in the last 30; `events` (top 15) the events of the last 30. Lists are most
counted first, values seen only; filters do not apply. 503 when Redis does not answer.

| Endpoint | Answers |
| --- | --- |
| `POST /v1/analytics/collect` | the beacon (above) |
| `GET /v1/admin/analytics/overview` | the figures above for the window and the one before; a series with all of them; realtime |
| `GET /v1/admin/analytics/breakdown?dim=&limit=` | views, visits or exits (`measure`) per value of one dimension, share, time on page, exits on a path, bounce and average visit when counting visits; `dim=exit` the last pages |
| `GET /v1/admin/analytics/crosstab?row=&col=&metric=` | page views or visits by two dimensions (above) |
| `GET /v1/admin/analytics/live?locale=` | the last half hour from Redis (above) |
| `GET /v1/admin/analytics/events` | every name seen in the window or the one before, and every blocked one (`blocked: true`), with count, previous, series |
| `GET /v1/admin/analytics/events/:name` | total, every prop key seen with its 50 top values and `otherCount`, the 50 top combinations, by page type and viewport, the 50 top entities (name and frontend path; `kind` null once gone), series |
| `GET /v1/admin/analytics/events/:name/crosstab?row=&col=` | one event by two sides: `page_type`, `viewport`, `locale`, `day`, `path`, `country`, `browser`, `os` or `prop:<key>`; 30 values per side by count, the rest `__rest__` (a value nothing real can be: a page type may well be `other`), an empty value `(none)`; `day` keeps every period of the interval |
| `GET /v1/admin/analytics/search` | the `search_*` events by the `surface` values seen: opened, typed, clicks, dead ends, `clickRate` (at most 1: the search page counts one opening per visit to it, its results are clicked many times), `noResultRate`; by page type and viewport; result kinds, positions, browse items; series |
| `GET /v1/admin/analytics/seo/landing-pages?channel=search&limit=` | the pages visits from one channel entered on: visits, previous, change, bounce rate, average visit, top 5 sources |
| `GET /v1/admin/analytics/seo/trends?channel=search` | rising and falling landing pages, 25 each, at least 10 visits over both windows, with the window's average visit; at most 180 days |
| `GET /v1/admin/analytics/seo/not-found` | paths that showed `pageType: not_found`, with the referrers of the visits that **entered** on them (`visits` each): a Google result pointing at a dead page shows `google`; a broken link inside the site brings views and no referrer, whatever the visit came from |
| `GET /v1/admin/analytics/seo/uncovered?kind=park\|attraction` | parks or rides no search visit entered on: matched on the frontend path in any language, so a ride renamed since shows as uncovered |
| `GET`, `PUT`, `DELETE /v1/admin/analytics/blocked-events[/:name]` | the block list; writes need `editor` and are audited |

All admin routes need a session (`AdminAuthGuard`, any role) and answer `private, no-store`.
`SiteAnalyticsService`, `SiteAnalyticsSeoService` and `SiteAnalyticsLiveService` return exactly these
shapes and are exported for the admin's MCP tools.

## Measurements

Measured 2026-10-28 on PostgreSQL 18.1 with TimescaleDB 2.24.0 in a scratch container (2 CPUs, 3 GB,
`shared_buffers` 512 MB) on a host shared with other builds, so the times are upper bounds. Data: 90
synthetic days at ten times today's traffic, ~16,000 page views (40 % of them opening a visit, half
of those reaching a second page) and ~19,000 events a day, summed per five-minute window and stored
under the hour as the flusher does, power-law spreads over 150 parks, 5,000 rides, 900 posts, six
languages, 80 countries, the browser languages and the channel mix of today's referrers. 80 % of the
page views report their seconds, a quarter of those in the next window; every visit has an exit, a
bounce's on its start row when both fall in one window.

**Size.** The 1.44 M page views made 2.66 M rows: 1.44 M page-view rows, 0.29 M rows of seconds
reported in a later window, 0.58 M visit rows and 0.36 M exit rows. Compressed, 455 MB became 55 MB
(8.2×) and the events' 1.73 M rows 361 MB → 27 MB (13.5×). A year at ten times today's traffic comes
to about **530 MB** with the two uncompressed chunks of the current fortnight (261 MB page views and
128 MB events compressed, 138 MB uncompressed), and stays there: chunks past 400 days are dropped.
Before the seconds, exits and browser language it was about 410 MB.

**Write.** 20,000 beacons (a page view, its seconds four times in five, and 0–2 events each, the
day's 168 windows so far) through the real collector, its Lua guards and the live view's counters
included: `collect()` p50 1.6 ms, p95 4.1 ms, p99 9.3 ms, Redis only (0.95, 2.5 and 6.8 ms before
the visit hash and the live counters). The flusher ended 1,552 idle visits and wrote the 168 windows,
39,099 page-view and 19,611 event rows, in 3.2 s, **19 ms a window**; `pg_stat_user_tables` over the
chunks read 0 updates, 0 deletes and **0 dead tuples** afterwards. The run takes seconds, so every
key it wrote was still alive at the end: Redis held 34 MB and 27,851 live keys, where production
keeps only the last 35 minutes of live keys.

**Reads** (median of three after one run, ms; 10× traffic):

| Report | 1 day | 30 days | 90 days |
| --- | --- | --- | --- |
| overview / `filter=country:de` / `filter=channel:search` | 9 / 6 / 11 | 260 / 189 / 129 | 390 / 225 / 172 |
| overview by week |  |  | 375 |
| breakdown `path` / under `channel:search` | 2 / 2 | 325 / 142 | 745 / 400 |
| breakdown `exit` / under `channel:search` | 2 / 2 | 317 / 156 | 756 / 462 |
| breakdown `referrer` / `country` / `browser_language` | 2 / 2 / 1 | 87 / 143 / 115 | 220 / 326 / 331 |
| traffic crosstab `os` × `viewport` / `channel` × `exit` | 1 / 1 | 92 / 385 | 236 / 1,020 |
| events | 13 | 234 | 369 |
| event `tab_changed` | 3 | 96 | 251 |
| crosstab | 1 | 40 | 95 |
| search | 2 | 114 | 306 |
| SEO landing pages / trends / not found | 8 / 12 / 1 | 365 / 315 / 174 | 650 / 465 / 506 |
| `/page-views/top` news / attractions (7, 30, 90 days) |  | 14 / 20, 47 / 52, 92 / 171 |  |
| admin page-views (7, 30, 90 days) |  | 87, 355, 1,005 |  |

The days are grouped with `time_bucket` and printed in code: grouping by `to_char(date_trunc(…))` per
row made a 60-day event series 569 ms instead of 75. A report reads every row of its window once, so
a year's report takes about four times the 90-day figure; at today's traffic (a tenth) it is a few
hundred milliseconds. The visits ended by their last page (`exit`, and `channel` × `exit`) read every
row for its `exits`, which the compressed batches cannot skip. If year views become the daily tool,
insert-only daily rollups written by the nightly job are the next step.

## Limits

- Visitors are per day and language only; with any other filter they are `null`, and so is an hour
  before today.
- Time on page and visit are lower bounds: a page view that never reported counts zero seconds.
- A visit ends 30 to 35 minutes after its last hit, as the flush runs, and its exit reaches the
  reports with that run.
- `visit_span_seconds` is stored on the exit rows and in no report yet.
- Page views and events cannot be filtered by how their visit began (see Reports).
- A `utm_source` is told from a referrer host by its missing dot.
- With Redis down the guards, visits and visitors are off and an instance holds at most 5000 fields in
  memory; past that, hits are dropped and counted.
- The first 512 characters of a User-Agent decide browser, OS and bot.
