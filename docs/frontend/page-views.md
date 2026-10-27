# Page views (`/v1/page-views`)

A counter for the frontend's rankings: the most read guides and news, and the admin's overview of
what is read in which language. **PAR-1089.** The frontend half (beacon, lists, badge) is
`docs/features/most-read.md` in park.fan.

## Why a counter and not an event store

Umami already records every page view and every event, and its dashboard answers "which URL, how
often". What it cannot do is feed a list on the site: its data sits behind a paid API, outside our
cache, and is shaped by what the tracker could see (ad blockers, the analytics opt-out). So this is
deliberately narrow: a fixed list of kinds, a key per page, a language, a day, a number. A new
ranking adds its kind to `PAGE_VIEW_KINDS` (`src/page-views/page-view.constants.ts`); there is no
free-form tag, because an unauthenticated write that accepts any tag is a key-value store.

| Kind         | Key                                            |
| ------------ | ---------------------------------------------- |
| `article`    | the post's `translationKey`                    |
| `news`       | the post's `translationKey`                    |
| `park`       | the park's UUID, lowercase, nothing else       |
| `attraction` | the attraction's UUID, lowercase, nothing else |

A UUID rather than a slug for parks and rides, because a slug changes with a rename and the counts
would split. The write checks the key against its kind (`isPageKeyFor`): a free-form key under
`park` would reach the admin's `IN (…)` on a uuid column and fail the whole overview.

## Every ranking is per language

`locale` is part of the key and the top endpoint requires it. Most of the site's traffic is German,
so one list over all languages would be the German list for every reader.

## Storage

`page_view_days` (`PageViewDay`), primary key `(kind, key, locale, day)`, one upsert per counted view
(`ON CONFLICT … DO UPDATE SET views = views + 1`), index `(kind, locale, day)` for the rankings.
`day` is the UTC date: a view belongs to a reader, and readers sit in every zone. The table grows by
one row per page and language viewed that day, and keeps them: no ranking reads further back than
90 days, but at the measured size (below) a year costs less than a hundred megabytes, so there is
no prune job yet.

Postgres, not Redis: this Redis runs `volatile-lru`, and a ranking that silently loses a day under
memory pressure ranks wrong without saying so.

**Merges.** When the admin merges a park or a ride into another, its rows move to the survivor's
UUID (`mergePageViewDays` in `src/parks/utils/merge-dependencies.ts`, a `custom` entry of
`PARK_DEPENDENCIES` and `ATTRACTION_DEPENDENCIES`); a day both have in one language is added up,
because both rows are real readers of the one page.

## What is not counted

- **Bots**, by User-Agent (`isBotUserAgent`): crawlers (Googlebot runs JavaScript and would fire
  the beacon), Google's testing and AI fetchers, agents fetching for a user, link previews, uptime
  and speed checks, headless browsers, HTTP libraries, and a request without a User-Agent. `bot`
  counts only as a word ending and not after `cu`/`ro`: CUBOT phones and devices named "Robot" are
  people.
- **Do Not Track and Global Privacy Control.** A request with `DNT: 1` or `Sec-GPC: 1` is answered
  204 and not counted, even if a page's beacon failed to ask first.
- **More than three views of one page per reader and UTC day** (`MAX_VIEWS_PER_READER_PER_DAY`). A
  reader is the address bucket and the User-Agent: behind a carrier gateway one address is many
  readers. The page is kind and key, not the language, so reading a post in two languages is still
  one reader.
- **More than 500 views per address bucket and UTC day** (`MAX_VIEWS_PER_ADDRESS_PER_DAY`), across
  all pages: what keeps one machine from writing a row for every key it can make up, or from
  rotating its User-Agent past the first cap.

The **address bucket** is an IPv4 address whole and an IPv6 address as its /64 (`addressBucket`,
`src/common/utils/request.util.ts`): a host with a /64 can take a new address per request, so a cap
on the full IPv6 address caps nothing. The address is `getRateLimitIp`: Cloudflare's
`CF-Connecting-IP` only on a request that carries `CF_ORIGIN_SECRET` (`carriesCfOriginSecret`, the
same test the throttler makes), otherwise the socket's.

**No address is stored**, in Postgres, in Redis or in the log. Each cap is a Redis counter
(`page-views:seen:<hmac>`, `page-views:addr:<hmac>`) whose key is an HMAC-SHA256 under
`PAGE_VIEWS_HASH_SECRET` of day, bucket, User-Agent and page (or of day and bucket). The secret
lives in the environment and never in Redis, so a key cannot be traced back to an address by trying
all of them; with the secret, only by testing a given one, and only until the key expires a minute
after its UTC day ends (up to 25 hours). Both counters go out in one pipeline, and `EXPIRE … NX` on
every call heals a key whose first expiry was lost. Without the variable each process makes a random
secret at start-up (and logs a warning): addresses stay out of Redis all the same, only a restart or
a second instance counts a reader again. With Redis down, the view counts. `LoggingInterceptor`
leaves the address out of its line for `/v1/page-views`.

The browser sends the write straight to api.park.fan, never through the frontend's relay, so the
address and the User-Agent are the reader's own. The frontend's beacon sends each page once per tab,
only on park.fan, after a few seconds on the page, and not at all under Do Not Track, GPC or after
the analytics opt-out on the privacy page. It keeps nothing in the browser's storage, so a reader
who reloads is held to three a day by the cap alone.

`POST` always answers **204**, counted or not: a caller that could tell the two apart could probe
the caps and the bot rule.

## Endpoints

| Endpoint                                                | Who      | Cache                           |
| ------------------------------------------------------- | -------- | ------------------------------- |
| `POST /v1/page-views` `{ kind, key, locale }`           | browser  | `no-store`                      |
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

## Performance and maintenance

Measured on 2026-10-27 against PostgreSQL 18 (production runs `timescaledb:latest-pg18`), with the
table exactly as TypeORM creates it, filled with a power-law spread over the real catalogue sizes
(7,152 rides, 212 parks, ~100 guides, a news post every day and a half, read in the three weeks
after it appears; German twice English, the rest smaller):

| Data set                     | Rows  | Rows / day | Views / day | Heap   | Indexes |
| ---------------------------- | ----- | ---------- | ----------- | ------ | ------- |
| Today's traffic, one year    | 474 k | 1.3 k      | 1.9 k       | 40 MB  | 55 MB   |
| Twenty times that, two years | 7.5 M | 10.3 k     | 39 k        | 656 MB | 955 MB  |

| Query                                    | One year | Stress     | Plan                                       |
| ---------------------------------------- | -------- | ---------- | ------------------------------------------ |
| top, news, 7 days                        | 0.2 ms   | 0.15 ms    | index `(kind, locale, day)`                |
| top, articles, 30 days                   | 1.7 ms   | 2 ms       | same                                       |
| top, attractions, 90 days (worst public) | 17 ms    | 114 ms     | same, bitmap                               |
| upsert of an existing row                | 0.3 ms   | 0.4 ms     | primary key                                |
| admin: totals / per day, 90 days         | 37/23 ms | 260/240 ms | skip scan of `(kind, locale, day)` (PG 18) |
| admin: ranking, 90 days                  | 165 ms   | 1.1 s      | one scan, two window functions             |
| merge of the most read ride (+ delete)   | 53 ms    | 160 ms     | primary key, `(kind, key)` prefix          |

The public rankings sit behind an hour of cache at Cloudflare and the frontend, so they run a few
times an hour per kind and language. The admin ranking first joined a second scan back for the
split by language; one pass with `SUM … OVER (PARTITION BY kind, key)` and `DENSE_RANK` gives the
same rows (checked row for row) in 40 % of the time.

**Key order.** The primary key is larger than the heap under stress, because `key` comes second and
a day's rows land all over the index. `(kind, locale, day, key)` was tried, which appends and could
drop the second index: 13 % smaller, but every query slower (articles 2 → 11 ms, the merge
155 → 447 ms, the admin up to twice). The current order stays.

**Updates and VACUUM.** Every counted view after a page's first that day updates the same row. One
day of stress traffic (40,000 views, 25,000 rows) replayed view by view:

| `fillfactor`  | HOT updates | Heap, one year |
| ------------- | ----------- | -------------- |
| 100 (default) | 7.6 %       | 40 MB          |
| 90            | 31.7 %      | 45 MB          |
| 80            | 57.2 %      | 51 MB          |
| 70            | 77.3 %      | 58 MB          |

A HOT update writes no index entry and is pruned on the page by the next read; the rest leave an
entry in both indexes for VACUUM. A VACUUM after a day of stress traffic took 92 ms and read 1.6 %
of the table, because the visibility map skips every older day. What was wrong was the trigger: at
the default `autovacuum_vacuum_scale_factor` of 0.2 a table this size waits for 1.5 M dead rows,
about a hundred days. So `PageViewsService.onModuleInit` sets `fillfactor=80` and
`autovacuum_vacuum_scale_factor=0.01` (`PAGE_VIEW_STORAGE_OPTIONS`) once, when `pg_class.reloptions`
lacks them; `synchronize` cannot express either. The table is not on the nightly VACUUM list
(`VACUUM_TARGETS`): autovacuum handles it in milliseconds.

Reproduce: the generator, the churn replay and the `EXPLAIN` set are plain SQL against a scratch
database; the numbers above are from that run, not from production.
