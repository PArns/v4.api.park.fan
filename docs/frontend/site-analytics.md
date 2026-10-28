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

| Field      | Hit        | Rule (`site-analytics.hits.ts`)                                                                                                              | When it fails     |
| ---------- | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------- | ----------------- |
| `t`        | all        | `pageview`, `event` or `engagement`                                                                                                          | hit dropped       |
| `locale`   | all        | one of the six                                                                                                                               | hit dropped       |
| `path`     | all        | `^/(en\|de\|nl\|fr\|es\|it)(/[a-z0-9][a-z0-9._~-]{0,99}){0,8}$`, first segment = `locale`                                                    | hit dropped       |
| `pageType` | all        | `^[a-z][a-z0-9_]{0,31}$`                                                                                                                     | hit dropped       |
| `referrer` | page view  | `^[a-z0-9][a-z0-9.-]{0,99}$` with a dot in it, not park.fan: the host of an external referrer                                                | referrer dropped  |
| `name`     | event      | `^[a-z][a-z0-9_]{1,47}$`                                                                                                                     | hit dropped       |
| `props`    | event      | up to 8 in key order; key `^[a-z][a-z0-9_]{0,31}$`; value a finite number, a boolean or a string `^[\p{L}\p{N}][\p{L}\p{N} ._:/+#-]{0,119}$` | that prop dropped |
| `entity`   | event      | lowercase UUID of a park, ride, show or restaurant                                                                                           | entity dropped    |
| `seconds`  | engagement | a whole number, 1–1800 (`MAX_ENGAGEMENT_SECONDS`)                                                                                            | hit dropped       |

**Engagement.** The frontend times each page view while the tab is visible and reports it as
`{ t: "engagement", locale, path, pageType, seconds }`, with the page's later hits or when
the page is hidden; a page view may report in parts, which add up. It is a hit like any other: it
counts against the address's cap and passes the path and page-type guards. Its seconds go to the
page's row (`seconds`) and, while the visit's key knows how the visit began, to the visit's start row
(`visit_seconds`); without a visit (no address, Redis away, the visit already ended) to the page
only. The browser sends no time: the seconds land in the hour the report arrives in.

A hit carries no device class and no window width, and the beacon reads no query parameter of the
landing URL: `utm_source` and `utm_campaign` are not read, and `referrer` is the external host and
nothing else. A field the table does not list is ignored, so a tab that still runs an older script and
sends `viewport` or `campaign` is counted all the same and neither is stored.

**No location-derived entity.** `nearby_parks_loaded` and every `compass_*` event lose their `entity`
on the way in, whatever a client sends (`LOCATION_DERIVED_EVENTS`): the park near a reader would be
where the reader is. Their props stay. The list is one constant; a new event of that kind goes there.

The server adds, per hit and never from the client: browser and OS family and the **device class**
(`user-agent.util.ts`, seven families each and three classes, no version, from the first 512
characters), the country from the request address
through the GeoLite2 lookup the API already runs (`GeoipService.lookupCountry`, `zz` when unknown),
and per visit the referrer **site** and **channel** (`referrer.util.ts`): `google.de` and
`www.google.com` are `google`; `search` (Google, Bing, DuckDuckGo, Ecosia, Yahoo, Yandex, Baidu,
Qwant, Startpage, Brave, Kagi, …), `ai` (ChatGPT, Perplexity, Gemini, Copilot, Claude, …), `social`,
`referral` (any other host, under its own name), `direct` (no referrer). Only a search engine's own host is `search`: `mail.google.com`,
`docs.google.com`, `maps.google.de` or `news.google.com` send readers as any site does and are
`referral`. A new site is one line in `REFERRER_RULES`. The **browser language** is the primary
subtag of the first `Accept-Language` tag, lowercased (`de-AT, en;q=0.8` → `de`), and `zz` for
anything that is not two letters (`browser-language.util.ts`). The site's language is the path's;
this one says what the browser asked for, and only the rows of `site_pageviews` carry it.

**The device class** (`deviceClassOf`, `mobile`, `tablet` or `desktop`, `SITE_DEVICES`) is read from the
request's User-Agent alone, in this order, with plain `includes` tests (no regular expression, so no
input makes it slow):

1. `tablet`: the User-Agent says `iPad`, `Tablet`, `Kindle`, `Silk` or `PlayBook`;
2. `mobile`: it says `Mobi` (which is `Mobile`, `IEMobile`, `Opera Mobi`), `iPhone`, `iPod`,
   `Windows Phone` or `Opera Mini`;
3. `tablet`: it says `Android` and none of the above, which is an Android without `Mobile`: Chrome and
   Firefox leave `Mobile` out on a tablet;
4. `desktop`: everything else.

The tablet names come first because an iPad and Silk also say `Mobile`; Windows Phone and Opera Mini
come before the Android rule because they also say `Android`. The browser asks for no client hint, and
the beacon sends no window width, so the class is what the header says and nothing more. **Known
limits:** an iPad on iPadOS 13 or later asks for desktop sites and sends the User-Agent of a Mac, so it
counts as a `desktop`; so does a phone or tablet whose browser asked for the desktop site (`X11`,
`Macintosh`); an Android device that says no `Mobile` and is no tablet (a television, a car) counts as a
`tablet`. One reader has one class for the whole visit, since the visit key is the address and the
User-Agent.

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

| Guard                                                                                        | Limit (`site-analytics.constants.ts`)                             | Past it                                                 |
| -------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------- |
| hits per address (IPv4, IPv6 /56)                                                            | 3000 (`MAX_HITS_PER_ADDRESS_PER_DAY`)                             | dropped                                                 |
| hits per five-minute window, all readers                                                     | 5000 (`MAX_HITS_PER_WINDOW`)                                      | dropped                                                 |
| distinct event names; `page_read` exempt                                                     | 300, 20 new per address (`SITE_GUARDS.names`)                     | a new name dropped                                      |
| distinct page types                                                                          | 100, 10 new per address                                           | stored as `other`                                       |
| distinct paths                                                                               | 20,000, 200 new per address                                       | stored as `(other)`, the entry too                      |
| distinct referring sites outside the known search engines, AI assistants and social networks | 1000, 10 new per address                                          | stored as `(other)`, channel kept                       |
| property combinations (with the entity) per event; `page_read` exempt                        | 2000, 100 new per address                                         | counted with props `{ "_overflow": "1" }`, no entity    |
| `page_read` per reader and page                                                              | 3 (`MAX_VIEWS_PER_READER_PER_DAY`)                                | dropped                                                 |
| `page_read` per address and page, whatever the User-Agent                                    | 10 (`MAX_VIEWS_PER_ADDRESS_AND_PAGE_PER_DAY`)                     | dropped                                                 |
| `page_read` per address                                                                      | 200 (`MAX_VIEWS_PER_ADDRESS_PER_DAY`)                             | dropped                                                 |
| `page_read` of a park or ride that does not exist                                            | the ids, cached ten minutes (`SiteEntityNames.knownIds`)          | dropped                                                 |
| blocked names                                                                                | the admin's list (`PUT /v1/admin/analytics/blocked-events/:name`) | dropped; each instance rereads the list within a minute |

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
  caps, the country, and a pseudonym; the User-Agent becomes two families, a device class and a pseudonym.
- **Every Redis key that stands for a reader or an address is pseudonymous for one UTC day**: an
  HMAC-SHA256 under that day's key, `HMAC(secret, day)` (`dailyHashKey`), where the secret is
  `ANALYTICS_HASH_SECRET` (falling back to `PAGE_VIEWS_HASH_SECRET`, then to a random per-process
  secret with a warning). Two days' keys of one reader have nothing in common, so nothing in Redis
  links one day to the next. Whoever holds the secret can still test a given address against a key of
  a day that has not expired; the secret lives in the environment, never in Redis. A visit that runs
  over midnight UTC is two visits.
- **What a visit key holds** (`site-analytics:visit:<hmac>`, a hash): `c` how the visit began (its
  first page view's language, path and page type, the referrer site and channel), `n` its page views, `s` and `l` the server's clock in UTC seconds at its first and its
  last hit, and `x` the exit row its last page view would make (that page's language, path, page
  type with the device, browser, OS, country and browser language, with how the visit began). It lives
  40 minutes past the last hit (`VISIT_KEY_TTL_SECONDS`) and is deleted when the flush ends the
  visit. No address, no User-Agent. The two times, and the open visits' score that repeats the last,
  are the only moments Redis keeps about a reader; they end with the visit and never reach Postgres,
  which stores only the difference, summed.
- **The open visits** (`site-analytics:visits:open`) are a sorted set of the visit pseudonyms, scored
  by the last hit, from which the flush picks the idle ones. A member goes when its visit ends, and
  the collector drops any member older than two hours (`OPEN_VISIT_MAX_AGE_SECONDS`), so even with
  the flusher stopped no pseudonym stays there longer.
- **What a row holds**: an hour, never a moment, and counts. A page view's row carries the page and
  its reader's classes (language, path, page type, device class, browser, OS, country, browser
  language), `views` and `seconds`, and **nothing of the visit**; a visit's start row carries the
  first page and the classes plus the entry, referrer site and channel, with `visits`, `engaged` and
  `visit_seconds`; an exit row carries the last page and the classes plus how the visit began, with
  `exits` and `visit_span_seconds` (last hit minus first, summed); an event's row carries the page,
  the classes without the browser language, the event and its props and entity, and nothing of the
  visit. So no row ties a page in the middle of a visit to where the reader came from; an exit row
  ties the last page to it, as the start row ties the first. At today's traffic most rows still
  count one hit: what makes them anonymous is that they hold no identifier and no visit thread, not
  that they are large.

| Redis key family                                                         | Holds                                                                                                                               | Lives at most                                                                             |
| ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `site-analytics:visit:<hmac>`                                            | how a visit began, its page views, its first and last hit, its exit row                                                             | until the flush ends the visit (30 to 35 min after its last hit), at most 40 min after it |
| `site-analytics:visits:open`                                             | the open visits' pseudonyms, scored by their last hit                                                                               | a member until its visit ends; never older than 2 h                                       |
| `site-analytics:cap:`, `read:`, `read-page:`, `read-addr:`, `new:<hmac>` | a reader's or an address's counts today                                                                                             | to the end of its UTC day + 1 min (≤ 24 h 1 min)                                          |
| `site-analytics:uv:<day>:<scope>`                                        | the day's visitor HyperLogLog                                                                                                       | to the end of its day + 25 h (≤ 49 h)                                                     |
| `site-analytics:uvh:<hour>:<scope>`                                      | the hour's visitor HyperLogLog                                                                                                      | to the end of its day + 25 h (≤ 49 h), as the day's, so the snapshot can read it          |
| `site-analytics:uvd:<day>:<scope>:<dim>:<value>`                         | the day's visitor HyperLogLog of one country, device, browser, OS or channel (`dim`), all languages or one                          | to the end of its day + 25 h (≤ 49 h), as the day's                                       |
| `site-analytics:uvd-index:<day>`                                         | the `scope\|dim\|value` of every `uvd` key the day wrote, so the snapshot finds them without a scan; no reader in it                | the same                                                                                  |
| `site-analytics:rt:uv:`, `rt:pv:`, `rt:ev:<minute>:<scope>`              | the minute's visitor HyperLogLog, page views, events                                                                                | 35 min                                                                                    |
| `site-analytics:live:<list>:<minute>:<scope>`                            | the minute's page views by page, country and device, visits started by channel and referrer site, events by name; no reader in them | 35 min                                                                                    |
| `site-analytics:guard:<guard>:<day>`                                     | the day's admitted names, page types, paths, referrer sites, combination hashes                                                     | to the end of the next day + 1 min (≤ 48 h); no reader in them                            |
| `site-analytics:window-hits:<window>`                                    | all hits of a five-minute window                                                                                                    | 15 min                                                                                    |
| `site-analytics:drops:<day>`                                             | drop and overflow counts by reason                                                                                                  | to the end of the next day + 1 min                                                        |
| `site-analytics:buf:<window>`, `…:flushing`                              | a window's counts until written                                                                                                     | no TTL; written within about six minutes                                                  |
| `site-analytics:buf:<window>:parked`                                     | a window the flusher gave up on                                                                                                     | 7 days                                                                                    |

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
- **Visitors**: a HyperLogLog per UTC day, all languages and per language, and per hour (for an hourly
  series), of the reader's daily pseudonym. `snapshot-visitors` runs every quarter hour from midnight to 04:00 and
  inserts yesterday's counts into `site_visitor_days`, the 24 hours of each language into
  `site_visitor_hours` and the counts by dimension (below) into `site_visitor_dim_days`, once (a row
  already written stays; a lost day key is skipped, not written as zero, and then so are its hours; an
  hour or a language without a key on a day that was counted had no visitors and is written as zero);
  today and, until then, yesterday are read live. A reader who
  comes back on another day counts again, so a window's visitors are the sum of its days, and one who
  comes back in another hour counts in each, so the hours of a day do not add up to its visitors.
  HyperLogLog's standard error is 0.81 %.
- **Visitors by dimension**: a HyperLogLog cannot be intersected with another, so a filter has
  visitors only where its value was counted into a key of its own. Each page-view batch with a reader
  also adds the reader's pseudonym to one day HyperLogLog per value of `country`, `device`,
  `browser` and `os` (the reader's own, which the day does not change: the reader is the address and
  the User-Agent), for all languages and for each language of the batch's page views, and, when the
  batch opens the visit, to the one of the visit's `channel`, for all languages and for the visit's
  language only (a visit is of its first page's language, so `locale=de` with `channel:search` counts
  the readers of search visits that began in German). The keys are listed in the day's `uvd-index`
  set. An overflow or empty value is not counted; the five lists are closed (the country's two
  letters, `zz` unknown), so a day has a few hundred keys at most. The snapshot
  (`snapshotVisitorDims`, in the same job) inserts yesterday's counts into `site_visitor_dim_days`
  once, a key that is gone or counted nobody skipped. A reader counts once per value and day, so the
  values of a dimension add up to the day's visitors within the estimate's error; under `channel` a
  reader whose visits began from two channels counts under both.
- **Which filters give visitors** (`visitorBasisOf`, the one place that decides): none, or exactly one
  filter on `country`, `device`, `browser`, `os` or `channel`, with or without the language
  (`locale=`). `visitors` is then a number in the totals, the window before and the day, week and
  month series: the sum of the days, today and, until the snapshot has run, yesterday from the live
  key. **No count**, `null`: two filters (two dimensions, or two values of one), a filter on the
  path, page type, entry, referrer, browser language or `locale`, and an hourly series under any
  filter (the hours are counted per language only). A breakdown by one of the five, with no filter
  or a filter on that same dimension, lists each value's `visitors`; under any other filter, or by
  any other dimension, its items carry none.
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
its place among the open visits, the reader's visitor keys by dimension, the live view's minute and
the drop counts, so all of it lands or none. A field is a measure letter (`v` views, `d` seconds, `s` visits, `e` engaged, `t` visit
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

**The tables** are TimescaleDB hypertables (`HYPERTABLES`), never updated and never deleted from; how long
each keeps its rows is [below](#retention):

| Table                    | Row                                                                                                                                                                                                                                                                                                                                                                                                                                                         | Compression                                                                        |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `site_pageviews`         | the UTC hour, `locale`, `path`, `page_type`, `device`, `browser`, `os`, `country`, `browser_language`, `entry`, `referrer`, `channel`; `views`, `seconds`, `visits`, `engaged`, `visit_seconds`, `exits`, `visit_span_seconds`. A page view's row has the three visit dimensions empty, `views` and `seconds`; a visit's start row has them and `visits`, `engaged`, `visit_seconds`; an exit row has them, its last page, `exits` and `visit_span_seconds` | 7-day chunks, compressed after 7 days, segment by `locale`, order by `bucket DESC` |
| `site_events`            | the UTC hour, `name`, the page dimensions (with `device`) without the browser language, `props_key` (`k=v` in key order, `&`-joined), `props` (`jsonb`), `entity`; `count`                                                                                                                                                                                                                                                                                  | the same, segment by `name`                                                        |
| `site_analytics_flushes` | the written claims of each window                                                                                                                                                                                                                                                                                                                                                                                                                           | 30-day chunks                                                                      |
| `site_pageviews_daily`   | `site_pageviews` summed per UTC day and [cuboid](#the-daily-level): the day's midnight as `bucket`, `cuboid`, `locale`, the dimensions the cuboid names (the others `''`), the seven sums as `bigint`                                                                                                                                                                                                                                                       | 7-day chunks, compressed after 7 days, segment by `cuboid`, order by `bucket DESC` |
| `site_events_daily`      | `site_events` the same way: `cuboid`, `locale`, `name` and the dimensions the cuboid names, the properties with their key, `count` as `bigint`                                                                                                                                                                                                                                                                                                              | the same                                                                           |

A row's `bucket` is its hour (`bucketOf`); the flusher still writes every five minutes, so an hour
holds up to twelve rows per combination, summed when read. The nightly `drop-old-chunks` drops a
chunk with `drop_chunks` under a 5-second `lock_timeout` once all of it is older than its table's
retention ([retention](#retention)); a chunk it cannot lock waits for the next night rather than
queueing the API behind it ([lock queues](../troubleshooting/timescaledb-lock-queues.md)). A dropped
chunk is a dropped table: nothing for VACUUM. `site_visitor_days` (seven rows a day),
`site_visitor_hours` (168 a day), `site_visitor_dim_days` (the visitors of each country, device,
browser, OS and channel by language and day: the UTC day, `scope` (`all` or a language), `dim`, `value`
and `visitors`, primary key `(day, scope, dim, value)`, a few hundred rows a day, written once per
finished day by `snapshot-visitors`) and `site_rollup_days` (a marker per cuboid and day) are small
plain tables whose old rows the same job deletes, a few hundred a night; `site_blocked_events`,
`site_entity_merges` and `site_analytics_imports` are small plain tables, kept.

**Schema.** The entities declare the bucket index `create_hypertable` makes
(`site_pageviews_bucket_idx`, `site_events_bucket_idx`, and the same for the daily tables, which also
have an index on `(cuboid, bucket)` for the chunks not compressed yet) and the `props` default as a plain `{}`: with
the default as an SQL expression, `synchronize` altered the column on every start, an `ACCESS
EXCLUSIVE` lock on the hypertable. The e2e suite asserts that `synchronize` finds nothing to change in
these tables after the hypertables are made.

**Merges.** A merged park, ride, show or restaurant is not rewritten in the compressed rows: the merge
records `loser → winner` in `site_entity_merges` (`recordEntityMerge`, a `custom` entry of all four
dependency lists in `src/parks/utils/merge-dependencies.ts`), always pointing at the final survivor,
and the reports and rankings read `COALESCE(winner_id, entity)`.

## The daily level

A year of the hourly rows is what makes a long report slow: ten million rows are read for every figure,
however small the answer. So the `site-analytics` / `rollup-days` job sums every finished UTC day into
`site_pageviews_daily` and `site_events_daily`, and the reports read whole past days there and only the
hours that are not rolled up from the hourly rows: today, a day the job has not reached, and the hour a
window before ends at. The answers are the same either way (below); only the rows read differ.

**Cuboids.** Summing a day over every dimension would not shrink it: a page view's row is nearly unique
at this traffic (25,300 rows a day with every dimension but the hour, against 29,600 hourly). So the
daily rows are groupings of the day by a few dimensions at a time, **cuboids**. A cuboid's rows carry
`locale` (every cuboid keeps the language), the dimensions it names, and the sums; the other dimension
columns hold `''`. There are 40 for page views (`PAGEVIEW_CUBOIDS`): the day alone (1); the path with
its page type (1); each dimension a panel groups by (page type, device, browser, OS, country, browser
language, entry page, referrer, channel: 9); the entry page with referrer, channel and page type, for
the SEO reports (1); and every pair of the eight narrow dimensions (page type, device, browser, OS,
country, browser language, channel, referrer: 28), a filter on one with a panel on another. A pair with
the path or the entry page has 12,000 to 15,000 rows a day, against the hourly rows' 29,600, and is
not kept. There are four for events (`EVENT_CUBOIDS`): the name; the name with its property combination;
the name with page type, device and property combination (an event's detail, the search report's
openings); the name with the entity.
Their ids are stored in every row and marker, so they never move: a new grouping takes the next number
and the job fills it in for the old days.

**What a report reads.** Every statement lists the columns it reads, the window's language and filters
included, and `chooseCuboid` takes the smallest cuboid that has them all; with none (a filter on the
path or entry page, three filters, a crosstab of the path with another dimension, an event filtered by
country) it reads the hourly rows as before. `rowsOf` then reads, for each whole day of the window that
has a marker, the cuboid's rows, and the hourly rows for the rest, in one `UNION ALL` with the same
language and filters in both branches. The branches select only the cuboid's columns, so a statement
that reads one the cuboid lacks fails instead of reading `''`. A statement that reads no day (a
breakdown, a crosstab without `day`, the properties and entities of one event) first sums each branch
over the columns it reads, which lets Postgres aggregate in parallel; not for the path and entry page,
whose hundred thousand values a year cost more to sum twice. The hourly series (`interval=hour`, at most
two days) reads the hourly rows.

**The same answer.** Every figure is a sum, and a sum of sums is the sum, so a cuboid's rows give what
the hourly rows give. Every list that came out of a `GROUP BY` without an `ORDER BY` (the sides of a
crosstab and its cells, an event's page types and property combinations, the search report's surfaces,
result kinds and browse items) is ordered by count and then by name, not by the order the database
returned the rows, so two plans cannot give two orders. The checks: the unit specs pin the cuboid
numbers, the choice, the day split and the SQL; `site-analytics.e2e-spec.ts` seeds twelve days, asks
810 report URLs (windows, filters, languages, intervals) from the hourly rows, rolls the days up,
asks them again and expects every body to be equal; and the same comparison ran on the synthetic year,
with the 49 cuboids it had then (nine named `campaign`): 1,127 answers (every report; windows from a day to a year, eight filter sets, two languages) were
identical, and the 40 the reports refuse (a visit filter on events, say) were refused both ways.

**The job** (`SiteAnalyticsStorageService.rollupDays`, 02:35, 08:35, 14:35 and 20:35 UTC; a run with
nothing to do costs three small queries). It rolls the days from the oldest hourly row (at most 400 days
back, `DAILY_RETENTION_DAYS`: an older day would go with the daily rows' next chunk) to the last day that has settled: ended two hours ago (`ROLLUP_SETTLE_SECONDS`, the flusher's
retries and the visits' exits are in by then) and before any flush window still waiting in Redis. For
each such day it adds the cuboids that have no marker in `site_rollup_days`, oldest day first, a day in
one transaction under an advisory lock (and a Redis lock for the run): one statement per cuboid, `INSERT
… SELECT … GROUP BY` of the day's hourly rows, that writes the rows and the marker (with the rows'
count) together and only if the cuboid has no marker for the day. Nothing is updated or deleted. Before
a day commits, each cuboid written is summed and compared with the day's hourly totals; one that does
not add up rolls the day back, fails the job (it is retried by the next run and reaches the failing-jobs
report) and leaves the day to be read hourly. The first run backfills what the hourly tables hold, at
about 1.3 s a day on an idle machine (365 days took 581 s on a shared host), and stops starting days
after 25 minutes (`ROLLUP_RUN_BUDGET_MS`); a new cuboid is filled in the same way.

**What the reports know about a day.** `SiteReportRunner.rolledDays` reads the markers of a cuboid (a
day is rolled once and never taken back) and keeps them a minute, so a day just rolled is read hourly
for a minute longer, and when they cannot be read the reports read the hourly rows. A statement takes a
day from the daily rows only if its cuboid has the day's marker; rows and marker go in together, so
there is no half-written day.

**The window before's last day.** A window that ends today compares with a window before that ends at the same
hour, so that today so far meets the same part of a day. Where that day lies before the hourly horizon
([retention](#retention)) its hourly rows are gone and its day is rolled up, so a statement a cuboid
serves takes the whole day from the daily rows (`takeRolledDaysWhole`): the window before then holds up
to one day in ninety more than its share, rather than lacking the hours.

**Why not continuous aggregates.** One takes a plain `GROUP BY`, no grouping sets, so each cuboid
would be a view of its own with a materialization hypertable and a refresh policy; and the one that
fits a single view, every dimension, does not shrink the data (above). Measured on the same year, a
view over every dimension held 9.2 M rows (the hourly rows are 10.8 M) in 3.0 GB uncompressed, took
6 min 40 s to refresh, and a path breakdown over it took 7.2 s. A single late hourly row marks its
day for a refresh that deletes the day's rows in the materialization hypertable and writes them
again (3.0 s here). And while the view exists Postgres refuses to drop a column of `site_pageviews`
that it reads, which is what `synchronize` does when an entity changes (the entities are the
schema). Insert-only rows written once, with a marker, give the same reads without any of that.

## Retention

The nightly `site-analytics` / `drop-old-chunks` (01:47 UTC, `SiteAnalyticsStorageService.dropOldChunks`)
keeps each table as long as its figures are of use. The numbers are named constants in
`site-analytics.constants.ts`, per hypertable in `HYPERTABLE_RETENTION_DAYS`:

| Table                                                                 | Kept     | Constant                       | Removed by                                                                                                                                                                                                        |
| --------------------------------------------------------------------- | -------- | ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `site_pageviews`, `site_events`                                       | 90 days  | `HOURLY_RETENTION_DAYS`        | chunks dropped whole, once every day with rows in them is rolled up                                                                                                                                               |
| `site_analytics_flushes`                                              | 90 days  | `HOURLY_RETENTION_DAYS`        | chunks dropped whole (30-day chunks): a claim only has to outlive its window's retry, and follows the hourly rows it stands for                                                                                   |
| `site_pageviews_daily`, `site_events_daily`                           | 400 days | `DAILY_RETENTION_DAYS`         | chunks dropped whole                                                                                                                                                                                              |
| `site_visitor_days`, `site_visitor_hours`                             | 400 days | `DAILY_RETENTION_DAYS`         | rows deleted                                                                                                                                                                                                      |
| `site_visitor_dim_days`                                               | 400 days | `DAILY_RETENTION_DAYS`         | rows deleted                                                                                                                                                                                                      |
| `site_rollup_days`                                                    | 414 days | `ROLLUP_MARKER_RETENTION_DAYS` | rows deleted: a marker outlives the daily rows it vouches for, which live up to a chunk past 400 days, so a report never reads a day hourly while its daily rows are there and the job never rolls a day up twice |
| `site_blocked_events`, `site_entity_merges`, `site_analytics_imports` | kept     |                                |                                                                                                                                                                                                                   |

The hourly rows carry every dimension at once; the daily rows only a few at a time (the
[cuboids](#the-daily-level)), which is what a comparison between years needs. So a statement a cuboid
serves reaches 400 days back, and one that needs the hourly rows 90 or so.

**How long a row really lives.** The week chunks are cut on Thursdays (TimescaleDB counts them from the
Unix epoch), and a chunk goes when all of it is older than the cutoff, the UTC midnight 90 days before
today. A row written on a chunk's last day is dropped by the run on the 91st day after it, one on its
first day by the run on the 97th: **an hourly row lives 90 to 97 days** (and the two hours to the run),
a daily row 400 to 407, a flush claim (30-day chunks) 90 to 120, a marker 414. The most-read rankings read the hourly rows for up to 90 days
(`MAX_ADMIN_WINDOW_DAYS` of the page views), which the retention holds whole. `hourlyHorizon` is the first
day of the oldest chunk the job keeps: from that UTC day on, every hour is there.

**The drop guard.** An hourly chunk is the only copy of what the daily rows have not summed yet, so the job
drops it only when every UTC day with rows in it has the marker of every page-view cuboid and every event
cuboid in `site_rollup_days` (`unrolledDays`; a day without rows, such as the first days of a table's first
chunk, needs none). For each chunk due, `dropOldChunks` lists it from `timescaledb_information.chunks`,
reads the days that have rows (`SELECT DISTINCT` over the chunk) and their markers, and then either drops
that one chunk (`drop_chunks` bounded by the chunk's own range, under the `lock_timeout`) or keeps it.
A kept chunk is logged (a warning with the count of chunks and days), returned in `skipped`, and fails the job
once the chunks that could go are gone, as a day that does not add up fails `rollup-days`: the job reaches
the data-quality report's failing jobs with the first days and the count. The chunk goes the night after
`rollup-days` has rolled its days up; nothing is lost meanwhile, and the job fails every night until then.
A cuboid added to the lists is rolled up for the days that still have hourly rows, so it makes the guard
wait for that run, and for days whose hourly rows are already gone it cannot be filled in: a report
that needs it finds those days empty, which `hourlyFrom` says (below).

## Reports

Days are UTC days, not a park's: a reader belongs to no park
([park timezone](../rules/park-timezone.md)). Every admin report takes a window, `locale`,
`interval`, and `filter=<dim>:<value>` (repeatable, AND; dim ∈ `path`, `page_type`, `country`,
`device`, `browser`, `os`, `locale`, `browser_language`, and the visit dimensions `entry`,
`referrer`, `channel`), and echoes `from`, `to`, `days`, `interval`, `locale`, `filters` and `hourlyFrom`.

**The window** is `days` (1–365, the last days with today; 30 without one) or `from` and `to`
(`YYYY-MM-DD`, both included, `to` today at the latest, at most 366 days). Both at once, one of
`from` and `to` alone, or a date that does not exist is a 400.

**The series** has a point per period of `interval`: `hour` (at most two days), `day`, `week` (ISO
weeks, from Monday, UTC) or `month`. Without one it is the hour for a one-day window, the day up to
92 days, the week up to 183 and the month beyond. A point's `day` is the period's start
(`2026-10-28`, or `2026-10-28T07:00:00Z` for an hour; a week's Monday; the first of a month), zeros
included; hours run up to the current one. A period the window cuts at either end, or that is still
under way (today, this hour), carries `partial: true`; a whole one has no `partial` field. The SQL
groups hours or days, and the code folds days into weeks and months. The visitors of an hour come from
its HyperLogLog for today (and for yesterday until the snapshot has run), and from `site_visitor_hours`
for every earlier day since the snapshot keeps hours; an hour of a day before that has
`visitors: null`. A week's or a month's visitors are the sum of its days in the window, as the totals
count them. Under a filter (above) the hours have none.

**`hourlyFrom`** says when a report could not cover its window. The hourly rows reach back to the
[horizon](#retention) only. A statement that no cuboid serves (a filter on the path or the entry page,
three filters, an event filtered by country, browser or system, a crosstab of the path with another
dimension) and an hourly series (`interval=hour`) read them, and for a window or a window before that
starts before the horizon they find nothing for the days whose chunk is dropped: the report answers
with what it has and `hourlyFrom` is `YYYY-MM-DD`, the horizon, the first UTC day its hourly figures are
complete from. It is null when every statement was served in full, which is every statement a
cuboid serves, however long the window. It is on every report that takes a window, in one place
(`echo`, the `WindowDto` of the API and each MCP tool's echo), set by the statements themselves as they
plan their read (`SiteAnalyticsWindow.reach`, `rowsOf`), never per report. The figures a report
reads from daily rows are complete either way; `hourlyFrom` does not say which of the report's figures are
cut, only that some read hourly rows before the horizon, in the window, in `previous` or in both (a window
of 60 days that starts after the horizon has a window before that starts 120 days back). A day counts
only if it is rolled up (the first cuboid's marker): a statistic younger than the horizon has dropped
nothing and answers null, and so does a day whose chunk waits for its roll-up. `previous` keeps its
meaning: null past 180 days, a window otherwise, partial when `hourlyFrom` says so.

**The window before** (`previous`) is the same number of days right before `from`. When the window
ends today it ends at the same hour of its last day as now: today is under way, so a change compares
today so far with the same part of a day, not with a whole one (the hour itself is taken whole on
both sides). A `from`–`to` window that ended before today compares with a whole window before.
Visitors of the window before count its last day whole, since visitors are per day. Over 180 days
(`MAX_COMPARABLE_DAYS`) there is no window before, since most of it would lie past the daily rows'
retention:
`previous` and every change are `null`, and the trends report refuses the window.

**Visit dimensions** (`entry`, `referrer`, `channel`) live only on the rows that record a
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
  "devices": [{ "device": "mobile", "views": 0 }],
  "events": [{ "name": "search_opened", "count": 0 }]
}
```

`perMinute` is the last 30 whole minutes and the current one, oldest first (31 points), a minute's
`visitors` distinct within that minute; `activeVisitors` the distinct visitors with a hit in the last
five minutes, the current one included. `pages` (top 10), `countries` (all, upper case, `ZZ` unknown)
and `devices` count the page views of those five minutes; `channels` (all) and `referrers` (top 10)
the visits started in the last 30; `events` (top 15) the events of the last 30. Lists are most
counted first, values seen only; filters do not apply. 503 when Redis does not answer.

**Bounds.** Every report, the admin's and its MCP tools', runs through `SiteReportRunner`, which keeps
a slow report from taking the database with it. The Traffic page asks for sixteen at once, and each
click on a filter asks again.

| Setting                               | Default | What it bounds                                             |
| ------------------------------------- | ------- | ---------------------------------------------------------- |
| `SITE_ANALYTICS_REPORT_CONCURRENCY`   | 4       | reports one process runs at once; the rest wait their turn |
| `SITE_ANALYTICS_REPORT_QUEUE`         | 32      | reports that may wait; one more is refused at once         |
| `SITE_ANALYTICS_REPORT_QUEUE_WAIT_MS` | 30000   | how long one waits for a place before it is refused        |
| `SITE_ANALYTICS_STATEMENT_TIMEOUT_MS` | 15000   | how long one statement of a report may run                 |
| `SITE_ANALYTICS_REPORT_WORK_MEM_MB`   | 32      | memory of each sort and hash of a report's statement       |

A refused or stopped report answers **503** with `{ statusCode: 503, error, reason, message }`: `reason:
"busy"` ("Too many statistic reports are running at once. Try again in a few seconds.") or `reason:
"timeout"` ("The report took longer than 15 s and was stopped. Try a shorter window, a coarser interval
or a filter."). The timeout, the memory and `timescaledb.enable_chunkwise_aggregation = off` are set
with `set_config(…, true)` inside the transaction each statement runs in, so only that statement has
them and the pool's other work keeps its own; Postgres cancels the statement and the connection goes
back. Chunk-wise aggregation builds a hash table per chunk and merges them, which a grouping by path
pays for fifty times over; one table over all chunks was two to three times faster on every report, on
the hourly rows too. The wait for a place is not part of the timeout, so a report waits at most 30 s
and runs at most 15 s a statement (up to four statements of one report run in parallel). The live view,
which reads Redis only, is outside it. The admin's MCP tools answer a 503 as any failed call
("failed on the server; try again or narrow the call").

| Endpoint                                                          | Answers                                                                                                                                                                                                                                                                                                |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `POST /v1/analytics/collect`                                      | the beacon (above)                                                                                                                                                                                                                                                                                     |
| `GET /v1/admin/analytics/overview`                                | the figures above for the window and the one before; a series with all of them; realtime                                                                                                                                                                                                               |
| `GET /v1/admin/analytics/breakdown?dim=&limit=`                   | views, visits or exits (`measure`) per value of one dimension, share, time on page, exits on a path, bounce and average visit when counting visits, and `visitors` per value of a country, device, browser, OS or channel breakdown (above); `dim=exit` the last pages                                 |
| `GET /v1/admin/analytics/crosstab?row=&col=&metric=`              | page views or visits by two dimensions (above)                                                                                                                                                                                                                                                         |
| `GET /v1/admin/analytics/live?locale=`                            | the last half hour from Redis (above)                                                                                                                                                                                                                                                                  |
| `GET /v1/admin/analytics/events`                                  | every name seen in the window or the one before, and every blocked one (`blocked: true`), with count, previous, series                                                                                                                                                                                 |
| `GET /v1/admin/analytics/events/:name`                            | total, every prop key seen with its 50 top values and `otherCount`, the 50 top combinations, by page type and device, the 50 top entities (name and frontend path; `kind` null once gone), series                                                                                                      |
| `GET /v1/admin/analytics/events/:name/crosstab?row=&col=`         | one event by two sides: `page_type`, `device`, `locale`, `day`, `path`, `country`, `browser`, `os` or `prop:<key>`; 30 values per side by count, the rest `__rest__` (a value nothing real can be: a page type may well be `other`), an empty value `(none)`; `day` keeps every period of the interval |
| `GET /v1/admin/analytics/search`                                  | the `search_*` events by the `surface` values seen: opened, typed, clicks, dead ends, `clickRate` (at most 1: the search page counts one opening per visit to it, its results are clicked many times), `noResultRate`; by page type and device; result kinds, positions, browse items; series          |
| `GET /v1/admin/analytics/seo/landing-pages?channel=search&limit=` | the pages visits from one channel entered on: visits, previous, change, bounce rate, average visit, top 5 sources                                                                                                                                                                                      |
| `GET /v1/admin/analytics/seo/trends?channel=search`               | rising and falling landing pages, 25 each, at least 10 visits over both windows, with the window's average visit; at most 180 days                                                                                                                                                                     |
| `GET /v1/admin/analytics/seo/not-found`                           | paths that showed `pageType: not_found`, with the referrers of the visits that **entered** on them (`visits` each): a Google result pointing at a dead page shows `google`; a broken link inside the site brings views and no referrer, whatever the visit came from                                   |
| `GET /v1/admin/analytics/seo/uncovered?kind=park\|attraction`     | parks or rides no search visit entered on: matched on the frontend path in any language, so a ride renamed since shows as uncovered                                                                                                                                                                    |
| `GET`, `PUT`, `DELETE /v1/admin/analytics/blocked-events[/:name]` | the block list; writes need `editor` and are audited                                                                                                                                                                                                                                                   |

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
128 MB events compressed, 138 MB uncompressed). Before the seconds, exits and browser language it was
about 410 MB. These sizes hold a year of hourly rows; they are kept [90 to 97 days](#retention).

**Write.** 20,000 beacons (a page view, its seconds four times in five, and 0–2 events each, the
day's 168 windows so far) through the real collector, its Lua guards and the live view's counters
included: `collect()` p50 1.6 ms, p95 4.1 ms, p99 9.3 ms, Redis only (0.95, 2.5 and 6.8 ms before
the visit hash and the live counters). The flusher ended 1,552 idle visits and wrote the 168 windows,
39,099 page-view and 19,611 event rows, in 3.2 s, **19 ms a window**; `pg_stat_user_tables` over the
chunks read 0 updates, 0 deletes and **0 dead tuples** afterwards. The run takes seconds, so every
key it wrote was still alive at the end: Redis held 34 MB and 27,851 live keys, where production
keeps only the last 35 minutes of live keys.

**The daily level** (PAR-1157) was measured on 365 synthetic days of the same shape (5.84 M page
views in 10.8 M hourly rows, 331 MB, and 7.0 M event rows, 185 MB, compressed after seven days), in a
scratch container with 4 CPUs, 5 GB, `shared_buffers` 1 GB and `work_mem` 32 MB on a host shared with
other builds, so the times are upper bounds. It ran with 49 page-view cuboids, nine of them naming
`campaign`; the 40 kept now are the other ones, so every size below is an upper bound too.

- **Size.** The 49 page-view cuboids of that run hold 18.2 M rows, 50,000 a day (8,400 for the path, 6,700 for
  the SEO grouping, 6 to 3,200 for the others), 3,197 MB that compressed to 324 MB (9.9×), 439 MB
  with the two uncompressed chunks of the current fortnight; the four event cuboids 4.3 M rows,
  742 MB to 70 MB (10.5×), 99 MB. The year takes about **1.05 GB**, the hourly rows' 0.52 and the
  daily 0.54, and a tenth of that at today's traffic. `pg_stat_user_tables` read 0 updates, 0 deletes
  and **0 dead tuples** on both tables and the 19,398 markers after the backfill. With the
  [retention](#retention) (hourly rows 97 days at most, daily 400 days) the steady state is the daily
  level's 400 days plus 97 days of hourly rows: about 0.8 GB at ten times today's traffic, scaled from
  the per-day sizes above (about 1.1 MB a compressed day on each level, and the uncompressed
  fortnight), not measured again.
- **Writing.** A compressed day takes 1.1 s for those 49 page-view cuboids (50,000 rows) and 0.2 s for
  the four event cuboids (11,700 rows); the 365-day backfill 581 s on the shared host. Rows and
  marker of a cuboid are one statement.
- **Reading, a year.** The Traffic page asks for sixteen reports at once (the overview, the events,
  thirteen breakdowns, one crosstab), here through the runner's four places. Median of three, ms:

| Reports, 365 days                                                      | Hourly rows only                    | Daily level            |
| ---------------------------------------------------------------------- | ----------------------------------- | ---------------------- |
| the Traffic page, all at once                                          | 17,600                              | **2,850**              |
| the Traffic page under `country:de` / `channel:search`                 | 8,400 / 10,200                      | 2,550 / 4,900          |
| the SEO page (landing pages, trends over 180 days, not found), at once | 3,500                               | 1,600                  |
| event, crosstab and search reports, at once                            | 3,400                               | 1,350                  |
| overview / events                                                      | 1,114 / 1,554                       | 65 / 101               |
| breakdown by channel / page type / device / browser / OS               | 1,075 / 1,061 / 1,112 / 1,024 / 846 | 26 / 29 / 28 / 29 / 26 |
| breakdown by referrer / country / browser language / language          | 695 / 1,240 / 1,236 / 377           | 77 / 91 / 93 / 22      |
| breakdown by path / entry page / exit                                  | 2,780 / 1,570 / 2,660               | 2,439 / 696 / 1,920    |
| crosstab OS × device                                                   | 743                                 | 41                     |

The path, the entry page and the exit are what is left: a grouping by 125,000 paths a year sums three
million daily rows in one hash table, however they are read. The slowest page is the one under a
visit filter (`channel:search`), whose breakdowns of the path, entry page and exit have no cuboid and
read the hourly rows.

**Reads by window** on the daily level (median of three after one run, ms; each cell "unfiltered /
`country:de` / `channel:search`" where the row says so):

| Report                                                    | 1 day        | 30 days         | 90 days         | 365 days            |
| --------------------------------------------------------- | ------------ | --------------- | --------------- | ------------------- |
| overview / `country:de` / `channel:search`                | 38 / 14 / 10 | 59 / 188 / 23   | 105 / 345 / 41  | 124 / 573 / 52      |
| overview by week                                          | 21           | 54              | 79              | 247                 |
| breakdown `path` / under `channel:search`                 | 3 / 3        | 367 / 211       | 927 / 644       | 2,695 / 1,863       |
| breakdown `exit` / under `channel:search`                 | 4 / 3        | 315 / 303       | 656 / 613       | 2,184 / 2,460       |
| breakdown `referrer` / `country` / `browser_language`     | 3 / 4 / 3    | 10 / 13 / 24    | 42 / 33 / 36    | 163 / 119 / 150     |
| traffic crosstab `os` × `device` / `channel` × `exit`     | 3 / 3        | 7 / 509         | 20 / 1,239      | 41 / 3,163          |
| events                                                    | 14           | 28              | 73              | 138                 |
| event `tab_changed`                                       | 8            | 120             | 186             | 659                 |
| crosstab                                                  | 4            | 42              | 140             | 536                 |
| search                                                    | 9            | 96              | 269             | 1,174               |
| SEO landing pages / trends (180 days at most) / not found | 14 / 23 / 2  | 293 / 383 / 117 | 716 / 930 / 256 | 1,793 / 1,533 / 725 |

The one-day column reads the hourly rows: a one-day window is drawn by the hour. The `channel` ×
`exit` crosstab reads them for any window, since no cuboid pairs the path with another dimension.
The most-read reads (`/page-views/top`, the admin's page views) are unchanged and read the hourly
event rows: 14 / 20, 47 / 52 and 92 / 171 ms for news and attractions at 7, 30 and 90 days, and 87,
355 and 1,005 ms for the admin's, measured before this change on the 90-day set with 2 CPUs.

The days are grouped with `time_bucket` and printed in code: grouping by `to_char(date_trunc(…))` per
row made a 60-day event series 569 ms instead of 75. Before the daily level a report read every row of
its window once, so a year's took about four times the 90-day figure.

## Limits

- Visitors are per day. They are given without a filter and under one filter on country, device,
  browser, OS or channel, with or without the language; under any other filter they are `null`, and
  so is an hourly series under any filter.
- Time on page and visit are lower bounds: a page view that never reported counts zero seconds.
- A visit ends 30 to 35 minutes after its last hit, as the flush runs, and its exit reaches the
  reports with that run.
- `visit_span_seconds` is stored on the exit rows and rolled up with them, and in no report yet.
- A filter on the path or the entry page, three or more filters, and a pair of a panel and a filter
  that no cuboid holds (anything with the path or the entry page) read the hourly rows: a year of
  them takes seconds, not milliseconds, and is the first thing the statement timeout stops. They
  only reach back 90 to 97 days; for older days the report answers with what it has and says so in
  `hourlyFrom`.
- A window older than the daily rows (400 days) has nothing in either: the figures are zero and
  `hourlyFrom` does not say so.
- An hourly series (`interval=hour`) over a day past the horizon has page views and events of zero and
  `hourlyFrom` set; its visitors still come from `site_visitor_hours`, which keeps 400 days.
- A day is read from the daily rows only once the job has rolled it (two hours past midnight at the
  earliest, and up to six hours with a failed run); until then, and for today, it is read hourly.
  A row written to the hourly tables after its day was rolled up is not in the daily rows.
- Hours of the days before `site_visitor_hours` existed have `visitors: null`.
- Page views and events cannot be filtered by how their visit began (see Reports).
- The referrer is the host the browser reports and nothing else: a link's `utm_` parameters are not
  read, so a newsletter or an ad is a `referral` under its own host, or `direct` when the mail client
  or the app sends none.
- With Redis down the guards, visits and visitors are off and an instance holds at most 5000 fields in
  memory; past that, hits are dropped and counted.
- The first 512 characters of a User-Agent decide browser, OS, device class and bot.
- The device class is the User-Agent's word, not the screen's: an iPad on iPadOS 13 or later counts as
  a desktop (it sends a Mac's User-Agent), a browser that asked for the desktop site does too, and an
  Android television counts as a tablet (see the device class under Wire format).
