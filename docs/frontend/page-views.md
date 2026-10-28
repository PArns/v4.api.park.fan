# Page views (`/v1/page-views`)

The frontend's rankings: the most read guides and news, and the admin's overview of what is read in
which language. **PAR-1089**, served from the [site statistic](site-analytics.md) since **PAR-1151**:
a counted read is the event `page_read`, and the rankings read it from `site_events`. The frontend
half (beacon, lists, badge) is `docs/features/most-read.md` in park.fan.

## The one event with a schema

The site statistic takes any event name and any properties. `page_read` is the exception, because it
feeds lists on the site: a fixed list of kinds (`PAGE_VIEW_KINDS`,
`src/page-views/page-view.constants.ts`), a key per page, and the rule that ties one to the other.
A hit that breaks it is dropped (`validateHit` in `src/site-analytics/site-analytics.hits.ts`).

| Kind         | Key                                            |
| ------------ | ---------------------------------------------- |
| `article`    | the post's `translationKey`                    |
| `news`       | the post's `translationKey`                    |
| `park`       | the park's UUID, lowercase, nothing else       |
| `attraction` | the attraction's UUID, lowercase, nothing else |

In the event a post's key is the prop `key` beside `kind`, and an entity sent with it is dropped; a
park's or ride's UUID is the event's `entity` (a read without one is dropped) and `kind` its only
prop. A UUID rather than a slug for parks and rides,
because a slug changes with a rename and the counts would split. A free-form key under `park` would
reach the admin's `IN (…)` on a uuid column and fail the whole overview, so the deprecated `POST`
checks it too (`isPageKeyFor`).

## Every ranking is per language

`locale` is part of the key and the top endpoint requires it. Most of the site's traffic is German,
so one list over all languages would be the German list for every reader.

## Storage

A read is a row of `site_events` with `name = 'page_read'`, written through the statistic's Redis
buffer and five-minute flusher ([storage](site-analytics.md#storage-insert-only-no-dead-tuples)):
insert-only, compressed after a week, chunks dropped after 400 days. A ranking sums `count` per key
over the window's rows, so it lags the beacon by one to six minutes, far less than its hour of cache.
The counts end up in Postgres, not Redis: this Redis runs `volatile-lru`, and a ranking that
silently loses a day under memory pressure ranks wrong without saying so. The buffer holds a window
for minutes and has no TTL, which that policy never evicts. A read's row carries its hour, the page
(`kind` and `key`, or the park's or ride's id), the language and the device, nothing of the visit.

**`page_view_days`** (`PageViewDay`), the old per-day upsert table, is no longer written. At the
first start after the deploy, `SiteAnalyticsStorageService.importPageViewDays` copies its rows once
into `site_events` (a row per page, language and day at 00:00 UTC, no path, viewport, browser or OS,
country `zz`), in one transaction with the marker `page_view_days` in `site_analytics_imports`, so a
second instance or a restart copies nothing. A park or ride key that is not a UUID is left behind,
and so are the few reads an old instance still counts there during the rolling deploy. The table
stays, entity included, until a later release drops it.

**Merges.** When the admin merges a park or a ride into another, the compressed rows are not
rewritten: the merge records `loser → winner` in `site_entity_merges` (`recordEntityMerge`, a
`custom` entry of `PARK_DEPENDENCIES` and `ATTRACTION_DEPENDENCIES` in
`src/parks/utils/merge-dependencies.ts`), and both rankings read
`COALESCE(winner_id, entity)`, so the survivor carries both pages' reads. `mergePageViewDays` still
moves the old table's rows, for as long as it exists.

## What is not counted

- **Bots**, by User-Agent (`isBotUserAgent`): crawlers (Googlebot runs JavaScript and would fire
  the beacon), Google's testing and AI fetchers, agents fetching for a user, link previews, uptime
  and speed checks, headless browsers, HTTP libraries, and a request without a User-Agent. `bot`
  counts only as a word ending and not after `cu`/`ro`: CUBOT phones and devices named "Robot" are
  people.
- **Do Not Track and Global Privacy Control.** A request with `DNT: 1` or `Sec-GPC: 1` is answered
  204 and not counted, even if a page's beacon failed to ask first.
- **More than three reads of one page per reader and UTC day** (`MAX_VIEWS_PER_READER_PER_DAY`). A
  reader is the IPv6 /64 or IPv4 address and the User-Agent: behind a carrier gateway one address is
  many readers. The page is kind and key, not the language, so reading a post in two languages is
  still one reader.
- **More than ten reads of one page per address and UTC day, whatever the User-Agent**
  (`MAX_VIEWS_PER_ADDRESS_AND_PAGE_PER_DAY`). Rotating the User-Agent passes the per-reader cap, not
  this one, so one machine adds at most ten reads a day to any page's ranking.
- **More than 200 reads per address and UTC day** (`MAX_VIEWS_PER_ADDRESS_PER_DAY`), across all
  pages: what keeps one machine from spreading reads over every key it can make up. The whole site
  counts about 1,600 page views a day, so 200 from one address is far past any person and still room
  for a gateway. The statistic's own cap of 3000 hits per address and day applies on top.
- **A park or ride that does not exist.** The collector checks a read's id against every park's and
  ride's (`SiteEntityNames.knownIds`, cached for ten minutes; without the list, as after a start with
  the database away, reads pass), and the public ranking lists a park or ride only while it exists,
  so neither a made-up id nor a deleted one reaches the site. The admin overview counts every read
  stored, a deleted page's unnamed. The frontend drops post keys it does not know.
- **Off park.fan**, in production: both endpoints count only a request from the site's own origin
  (`isFromSite`, [privacy](site-analytics.md#privacy)).

The caps are counted in `SiteAnalyticsCollectService` for every `page_read`, whichever endpoint
brought it, before the hit reaches the buffer. An **address** there is an IPv4 address whole and an
IPv6 address as its /56 (`addressBucket`, `src/common/utils/request.util.ts`): a provider delegates a
/56 to one customer, who can take a new /64 per request, so a cap per /64 would give one customer
256 budgets. The address is `getRateLimitIp`
([which address that is](../deployment/edge-and-throttling.md#which-address-a-request-is)).

**No address is stored**, in Postgres, in Redis or in the log. Each cap is a Redis counter
(`site-analytics:read:<hmac>`, `read-page:<hmac>`, `read-addr:<hmac>`) whose key is an HMAC-SHA256
under the UTC day's key, itself `HMAC(ANALYTICS_HASH_SECRET, day)` (falling back to
`PAGE_VIEWS_HASH_SECRET`), of address, User-Agent and page (or of address and page, or address).
The keys are pseudonymous for one day: two days' keys of one reader have nothing in common. The
secret lives in the environment and never in Redis, so a key cannot be traced back to an address by
trying all of them; with the secret, only by testing a given one, and only until the key expires a
minute after its UTC day ends. The counters go out in the statistic's one guard pipeline, and
`EXPIRE … NX` on every call heals a key whose first expiry was lost. Without either variable each
process makes a random secret at start-up (and logs a warning): addresses stay out of Redis all the
same, only a restart or a second instance counts a reader again. With Redis down the caps are off and
the reads wait in the instance's memory, which reaches Postgres within a minute
([Redis away](site-analytics.md#storage-insert-only-no-dead-tuples)). `LoggingInterceptor` leaves the
address out of its line for `/v1/page-views` and `/v1/analytics/collect`.

The browser sends the read straight to api.park.fan, never through the frontend's relay, so the
address and the User-Agent are the reader's own. The frontend's beacon sends each page once per tab,
only on park.fan, after a few seconds on the page, and not at all under Do Not Track, GPC or after
the analytics opt-out on the privacy page. It now sends the read as a `page_read` hit in the site
statistic's batch; `POST /v1/page-views` records the same event and stays for one release as a
deprecated alias, so a page cached with the old beacon keeps counting. It keeps nothing in the
browser's storage, so a reader who reloads is held to three a day by the cap alone.

`POST` always answers **204**, counted or not: a caller that could tell the two apart could probe
the caps and the bot rule. Both writes answer `Cross-Origin-Resource-Policy: cross-origin`, since a
beacon is a no-cors request from park.fan and helmet's `same-origin` would have the browser block the
empty answer and log an error for every read.

## Endpoints

| Endpoint                                                | Who      | Cache                           |
| ------------------------------------------------------- | -------- | ------------------------------- |
| `POST /v1/analytics/collect`, a `page_read` hit         | browser  | `no-store`                      |
| `POST /v1/page-views` `{ kind, key, locale }`, deprecated | browser  | `no-store`                      |
| `GET /v1/page-views/top?kind=&locale=&days=30&limit=10` | frontend | 1 h (`CacheControlInterceptor`) |
| `GET /v1/admin/page-views?days=30&limit=25&locale=`     | admin    | `no-store` (admin path)         |

`days` on the ranking is **7, 30 or 90** (`TOP_WINDOW_DAYS`) and `limit` at most 50
(`MAX_TOP_LIMIT`): a free `days` and `limit` would be a hundred thousand distinct URLs, each a miss
at the edge and an aggregate here. The admin takes 1–90 days and a limit of 1–100. Windows are UTC
days with today included.

The ranking returns keys and counts only; the frontend resolves a post's key against its manifest
and drops keys it does not know (a deleted post, a junk key someone posted). The admin answer
carries, per kind, the total and the split by language, the most viewed pages with their own split,
and the views per day, every day of the window including empty ones. With `locale` the pages are
ranked by that language's views, so a page read mostly in Dutch is found too. Parks and attractions
come with name, context and URL.

## Performance

Both rankings read only `page_read` rows: `site_events` is segmented by `name` when compressed, so
a ranking decompresses the reads and none of the other events. Measured 2026-10-28 on 90 synthetic
days at ten times today's traffic ([measurements](site-analytics.md#measurements)): the news ranking
takes 14, 47 and 79 ms for 7, 30 and 90 days, the attraction ranking 19, 52 and 137 ms, the admin
overview 85, 382 and 1,421 ms. The public rankings sit behind an hour of cache at Cloudflare and the
frontend. Nothing here updates a row, so there are no storage options, no dead tuples and nothing
for VACUUM; the old table's `fillfactor` and autovacuum settings stay on it until it goes.
