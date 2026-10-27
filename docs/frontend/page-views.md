# Page views (`/v1/page-views`)

A counter for the frontend's rankings: the most read guides and news, and the admin's overview of
what is read in which language. **PAR-1089.**

## Why a counter and not an event store

Umami already records every page view and every event, and its dashboard answers "which URL, how
often". What it cannot do is feed a list on the site: its data sits behind a paid API, outside our
cache, and is shaped by what the tracker could see (ad blockers, the analytics opt-out). So this is
deliberately narrow: a fixed list of kinds, a key per page, a language, a day, a number. A new
ranking adds its kind to `PAGE_VIEW_KINDS` (`src/page-views/page-view.constants.ts`); there is no
free-form tag, because an unauthenticated write that accepts any tag is a key-value store.

| Kind         | Key                         |
| ------------ | --------------------------- |
| `article`    | the post's `translationKey` |
| `news`       | the post's `translationKey` |
| `park`       | the park's UUID             |
| `attraction` | the attraction's UUID       |

A UUID rather than a slug for parks and rides, because a slug changes with a rename and the counts
would split.

## Every ranking is per language

`locale` is part of the key and the top endpoint requires it. Most of the site's traffic is German,
so one list over all languages would be the German list for every reader.

## Storage

`page_view_days` (`PageViewDay`), primary key `(kind, key, locale, day)`, one upsert per counted view
(`ON CONFLICT … DO UPDATE SET views = views + 1`), index `(kind, locale, day)` for the rankings.
`day` is the UTC date: a view belongs to a reader, and readers sit in every zone. The table grows by
one row per page and language viewed that day.

Postgres, not Redis: this Redis runs `volatile-lru`, and a ranking that silently loses a day under
memory pressure ranks wrong without saying so.

## What is not counted

- **Bots**, by User-Agent (`isBotUserAgent`): crawlers (Googlebot runs JavaScript and would fire
  the beacon), link previews, uptime and speed checks, headless browsers, HTTP libraries, and a
  request without a User-Agent.
- **More than three views of one page in one language per address and UTC day**
  (`MAX_VIEWS_PER_ADDRESS_PER_DAY`). Three, not one, because a household or an office shares an
  address.

**No address is stored**, in Postgres or in Redis. The cap is a Redis counter
`page-views:seen:<key>`, where the key is an HMAC-SHA256 of day, address, kind, page key and locale
under `PAGE_VIEWS_HASH_SECRET`. The secret lives in the environment and never in Redis, so a key
cannot be traced back to an address by trying all of them, and the key expires a minute after its
UTC day ends. Without the variable each process makes a random secret at start-up (and logs a
warning): addresses stay out of Redis all the same, only a restart or a second instance counts a
reader again. With Redis down, the view counts.

The browser sends the write straight to api.park.fan, never through the frontend's relay, so the
address (`getRateLimitIp`) and the User-Agent are the reader's own. The frontend's beacon also
counts each page once per browser and day, only after a few seconds on the page, and not at all
under Do Not Track or after the analytics opt-out on the privacy page.

`POST` always answers **204**, counted or not: a caller that could tell the two apart could probe
the cap and the bot rule.

## Endpoints

| Endpoint                                                | Who      | Cache                           |
| ------------------------------------------------------- | -------- | ------------------------------- |
| `POST /v1/page-views` `{ kind, key, locale }`           | browser  | `no-store`                      |
| `GET /v1/page-views/top?kind=&locale=&days=30&limit=10` | frontend | 1 h (`CacheControlInterceptor`) |
| `GET /v1/admin/page-views?days=30&limit=25`             | admin    | `no-store` (admin path)         |

`days` is 1–90 UTC days with today included, `limit` 1–500 for the ranking and 1–100 for the admin.
The ranking returns keys and counts only; the frontend resolves a post's key against its manifest and
drops keys it does not know (a deleted post, a junk key someone posted). The admin answer carries,
per kind, the total and the split by language, the most viewed pages with their own split, and the
views per day; parks and attractions come with name, context and URL.
