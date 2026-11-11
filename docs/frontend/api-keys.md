# Data API keys

Which routes need a data key, how a client sends one, and how park.fan's own frontend gets
through without one. Open it before adding a route that returns a forecast, or a forecast field
to a route that has none. The terms behind it are on park.fan:
<https://park.fan/en/impressum#data-use>.

## The rule

Every forecast the ML stack produces leaves the API only to park.fan's own frontend or to the
holder of a data key. Live and historical data stay open without a key.

| Route                                      | Without a key                                                                     | With a key                      |
| ------------------------------------------ | --------------------------------------------------------------------------------- | ------------------------------- |
| `…/{park}/calendar`, `/best-days`          | 401                                                                               | the full answer                 |
| `…/{park}/predictions/yearly`, `/plan/day` | 401                                                                               | the full answer                 |
| `…/{park}` (park detail)                   | the answer without `attractions[].bestVisitTimes`                                 | all of it with `?forecast=true` |
| `…/{park}/attractions/{slug}`              | without `hourlyForecast`, `bestVisitTimes`, `predictionAccuracy`                  | all of it with `?forecast=true` |
| `…/{park}/stats/day`                       | without `forecast`, `predicted`, `forecastError`                                  | all of it with `?forecast=true` |
| `/v1/favorites`                            | without the attractions' `hourlyForecast`, `bestVisitTimes`, `predictionAccuracy` | all of it with `?forecast=true` |

`?forecast=true` without a key is a 401, not a silent omission. The mixed routes take the flag
rather than reacting to the key alone because Cloudflare caches by URL and ignores `X-API-Key`:
one URL answered two ways would hand a keyed caller the stripped copy, or a keyless caller the
full one.

Not counted as forecasts: the weather forecast (Open-Meteo), `forecasts[]` on a ride
(ThemeParks.wiki's own field), `typicalWaits` and `ropeDrop` (historical), `/v1/ml/*` (accuracy
and model metadata). Parky's chat (`/v1/assistant/chat`) and the admin MCP call the forecast
services directly and are guarded by their own session and token.

## Sending a key

`X-API-Key: pfk_…`, or `Authorization: Bearer pfk_…`. Never in the query string: a URL ends up in
logs and in cache keys.

A refusal is `401` with `error: "api-key-required"`, `"api-key-invalid"` or `"api-key-expired"`
(with `expiresAt`), `terms` (the URL above) and `WWW-Authenticate: ApiKey header="X-API-Key"`.

`GET /v1/api-keys/verify` checks exactly the key presented: 200 with `usage`, `allParks`,
`allowedParkIds` and, for a key with an end date, `expiresAt`; 401 otherwise, and with `?park=continent/country/city/park` a
403 when the key does not cover that park. It is not behind the forecast guard, whose bypass key
would confirm any value, and it counts nothing against the allowance.

## park.fan's own frontend

The frontend has a data key of its own: usage `internal`, never counted against an hourly
allowance (a new internal key gets none), issued in
`/admin/api-keys` and set as `API_DATA_KEY` in its server environment, which
`getServerApiHeaders` sends as `X-API-Key` on every server call. It is revocable and listed like
any other key.

Before that key existed, the throttle bypass key (`x-auth-key`, `THROTTLE_BYPASS_KEYS`) opened the
forecast routes, and it still does until `FORECAST_BYPASS_KEY=off`. A request that carries both is
checked against its data key first, so the frontend's key is counted and marked as used from the
moment it is set, and the bypass key only stands in when the data key is missing or refused. Set
`FORECAST_BYPASS_KEY=off` once the frontend's key shows up in its statistics: the bypass key also lifts the rate limits and makes the API believe the
caller's `X-Forwarded-For` ([edge and throttling](../deployment/edge-and-throttling.md)), and it
should open nothing else. Never hand it to a data consumer.

## Hourly allowance

Every data key has `requests_per_hour` (default 1,000; null means only the general per-address
limit). `ApiKeyService.countRequest` counts each request the key authorises in a Redis counter per
key and clock hour (UTC), so the count starts over at every full hour; `INCR` and `EXPIRE … NX`
go in one `MULTI`, so a counter never outlives its hour. If Redis fails, the request is not
counted rather than refused. Over the allowance the
answer is `429` with `error: "api-key-rate-limited"`, `retryAfterSeconds` and `Retry-After`
until the next full hour. Each keyed answer reports `X-API-Key-Hourly-Limit`,
`X-API-Key-Hourly-Remaining` and `X-API-Key-Hourly-Reset` (seconds). park.fan's own frontend is
not counted. Only requests that reach the API count: an answer Cloudflare serves from its cache
does not, and its `Remaining` header is the one from when it was fetched.

## Statistics

`ApiKeyService.countRequest` counts every request a key makes, `internal` and unlimited keys
included, in the same Redis round trip as the hourly allowance: per UTC day
(`api-key:day:<id>:<YYYY-MM-DD>`, kept 32 days) and per UTC month (`api-key:month:<id>:<YYYY-MM>`,
kept 13 months). A request its allowance refuses with 429 is counted a second time under
`api-key:day-limited:…`. The admin list returns them as `stats` (30 days, oldest first, and 12
months), the admin page draws them. Only requests a key got through with count: a missing,
unknown, expired or park-refused key is not attributed to anyone. Like the allowance, the counting
fails open.

## Parks

A key can be limited to some parks (`allowed_park_ids`, ids rather than slugs so a renamed park
keeps its licence; null means every park, and an empty list is refused, so removing a key's last
park cannot open them all). `authorizeForecastRequest` reads the park from the route's
`:continent/:country/:city/:parkSlug`, resolves it through `ParksService`, and through the rename
aliases for an old path so the route's 301 is reached, and answers 403
`api-key-park-not-allowed` for any other park, before the request counts against the hourly
allowance. A route that names no park (`/favorites`) answers such a key 403
`api-key-park-required`, because it could reach any park's forecasts; `?park=` counts only on
`/verify`, or a limited key could name its own park there and reach any other.

## End date

A key can carry an end date (`expires_at`, null for never), for a trial or an agreement that runs
for a season. Each keyed answer reports it in `X-API-Key-Expires` (ISO 8601). From that moment the
key is refused with 401 `api-key-expired`, before it counts against its allowance, and its requests
fall under the keyless limits again; a `PATCH` with a later date brings it back, and revoking stays the way to end a
key for good. The date sits in the cached lookup and is compared on every request, so the five
minutes a lookup is cached never stretch it. An end date in the past is a 400 on create and edit.

## Caching

Cloudflare caches by URL and does not see `X-API-Key`. On Pro a request header cannot be part of
the cache key (custom cache keys with headers are Enterprise only), but the Cache Rules **Vary**
setting is available on every plan: when the origin sends `Vary: X-API-Key, X-Auth-Key` and a
rule names both headers with `passthrough`, Cloudflare keeps one copy per header value.

**Default (`FORECAST_SHARED_CACHE` unset): private.** `keepResponsePrivate` wraps the response's
`setHeader`, so whichever interceptor or handler sets `Cache-Control` afterwards, it leaves as
`private`, with the `max-age` it asked for and no `s-maxage`; `CDN-Cache-Control` is dropped.

**`FORECAST_SHARED_CACHE=vary`: shared per key.** `varyResponseByKey` lets a success keep a
public policy, capped at five minutes and without stale windows (`cappedSharedCacheControl`), since
nothing here can purge a revoked key's copies, and merges `X-API-Key` and the bypass header into every `Vary` the
response gets, `compression`'s `Accept-Encoding` included. A refusal or an error stays
`private, no-store`. A key sent as `Authorization: Bearer` stays private, because the rule does
not name that header. Turn it on only after the rule below is live and tested: without the rule
Cloudflare ignores the `Vary` and hands one key's answer to every caller.

Either way the guard settles the policy before it decides, because a guard runs before every
interceptor and its 401 would otherwise leave with no `Cache-Control` at all. Copies Cloudflare
cached before the key existed live out their TTL; a purge of `api.park.fan` ends them at once,
and a purge by URL removes every Vary copy of that URL.

### The Cloudflare rule

The `vary` object goes into the Cache Rule that already makes `api.park.fan` eligible for cache
(phase `http_request_cache_settings`), or into a new rule with the same expression placed after
it:

```json
"action_parameters": {
  "cache": true,
  "vary": {
    "default": { "action": "bypass" },
    "headers": {
      "x-api-key": { "action": "passthrough" },
      "x-auth-key": { "action": "passthrough" },
      "accept-encoding": { "action": "normalize" }
    }
  }
}
```

`default: bypass` keeps any other header an answer varies on out of the cache rather than mixing
copies. Cloudflare's documentation does not say how a request without a named header is matched,
so the switch-on is tested, not assumed:

```bash
KEY=pfk_…   # a key from /admin/api-keys
URL=https://api.park.fan/v1/parks/europe/germany/rust/europa-park/best-days
curl -sI -H "X-API-Key: $KEY" $URL | grep -iE 'cf-cache-status|^vary|cache-control'   # MISS
curl -sI -H "X-API-Key: $KEY" $URL | grep -iE 'cf-cache-status'                       # HIT
curl -sI $URL | grep -iE '^HTTP|cf-cache-status'                                      # 401, never HIT
```

If the last line ever shows a `HIT`, unset `FORECAST_SHARED_CACHE` and purge the cache.

## Without a key

Public data reads (`/v1/parks/**`, `/v1/discovery/**` except `nearby`, `/v1/search`, the public
`/v1/analytics/*` reads, `/v1/glossary/**`, `/v1/sitemap/**`, `/v1/page-views/top`, `/v1/ml/**`)
from a caller without an active data key are limited to `THROTTLE_ANON_LIMIT` per minute
(default 60) and `THROTTLE_ANON_HOURLY_LIMIT` per hour (default 1,000) per address, on top of the
general circuit breaker. Past one, a 429 `rate-limited` says a free key lifts it. The routes
park.fan's own pages call from the browser (assistant, site statistic, favourites, nearby, trips,
push) are not on the list: one carrier-NAT address can carry a whole park's visitors. A made-up
key counts as none. `isAnonymousDataRequest` and `CfThrottlerGuard.handleRequest`.

## Issuing keys

`/v1/admin/api-keys` (owner only, audited as `api-key.create`, `api-key.update` and
`api-key.revoke`): `GET` lists every key, active first, with `requestsThisHour`;
`POST { name, contact?, usage?, note?, requestsPerHour?, allowedParkIds?, expiresAt? }` returns `{ key, item }` with the key in
clear exactly once; `PATCH /:id { requestsPerHour?, allowedParkIds?, expiresAt? }` changes any of them from the next request on
(an unknown park id is a 400); `DELETE /:id` revokes. The table `api_keys` stores the SHA-256 of the
key and its first ten characters. `ApiKeyService.lookup` caches a found key in Redis for five
minutes and an unknown one for one minute, and stores only where nothing is cached (`NX`). A
revocation writes the refusal over the entry and a `PATCH` writes the new state, so a lookup that
read the old row a moment earlier cannot put it back. `last_used_at` is written at most every ten
minutes per key, and a process asks Redis about it at most that often.

## Adding a forecast

- A route whose whole answer is a forecast: `@RequiresApiKey()`.
- A forecast field on a route that has other data: `@ForecastFields("path.to.field")` on the
  route, with the field's path in `omitPaths` syntax (`attractions[].bestVisitTimes`). A route
  that validates its query with a DTO under `forbidNonWhitelisted` needs `forecast` on the DTO,
  as `FavoritesQueryDto` has.
- Add the row to the table above.
