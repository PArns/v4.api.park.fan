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
logs and in cache keys. `GET /v1/api-keys/verify` answers 200 with the key's `usage` for an
active key and 401 otherwise; park.fan's MCP server uses it to check a key an agent passed on.

A refusal is `401` with `error: "api-key-required"` or `"api-key-invalid"`, `terms` (the URL
above) and `WWW-Authenticate: ApiKey header="X-API-Key"`.

## park.fan's own frontend

The frontend server sends the throttle bypass key (`x-auth-key`, `THROTTLE_BYPASS_KEYS`) on every
call, and `authorizeForecastRequest` admits it like a data key. Do not hand that key to a data
consumer: it also lifts the rate limits and makes the API believe the caller's
`X-Forwarded-For` ([edge and throttling](../deployment/edge-and-throttling.md)).

## Hourly allowance

Every data key has `requests_per_hour` (default 1,000; null means only the general per-address
limit). `ApiKeyService.countRequest` counts each request the key authorises in a Redis counter per
key and clock hour (UTC), so the count starts over at every full hour. Over the allowance the
answer is `429` with `error: "api-key-rate-limited"`, `retryAfterSeconds` and `Retry-After`
until the next full hour. Each keyed answer reports `X-API-Key-Hourly-Limit`,
`X-API-Key-Hourly-Remaining` and `X-API-Key-Hourly-Reset` (seconds). park.fan's own frontend is
not counted. Only requests that reach the API count: an answer Cloudflare serves from its cache
does not, and its `Remaining` header is the one from when it was fetched.

## Caching

Cloudflare caches by URL and does not see `X-API-Key`. On Pro a request header cannot be part of
the cache key (custom cache keys with headers are Enterprise only), but the Cache Rules **Vary**
setting is available on every plan: when the origin sends `Vary: X-API-Key, X-Auth-Key` and a
rule names both headers with `passthrough`, Cloudflare keeps one copy per header value.

**Default (`FORECAST_SHARED_CACHE` unset): private.** `keepResponsePrivate` wraps the response's
`setHeader`, so whichever interceptor or handler sets `Cache-Control` afterwards, it leaves as
`private`, with the `max-age` it asked for and no `s-maxage`; `CDN-Cache-Control` is dropped.

**`FORECAST_SHARED_CACHE=vary`: shared per key.** `varyResponseByKey` lets a success keep the
public policy its route chose and merges `X-API-Key` and the bypass header into every `Vary` the
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

## Issuing keys

`/v1/admin/api-keys` (owner only, audited as `api-key.create`, `api-key.update` and
`api-key.revoke`): `GET` lists every key, active first, with `requestsThisHour`;
`POST { name, contact?, usage?, note?, requestsPerHour? }` returns `{ key, item }` with the key in
clear exactly once; `PATCH /:id { requestsPerHour }` changes the allowance from the next request
on; `DELETE /:id` revokes. The table `api_keys` stores the SHA-256 of the
key and its first ten characters. `ApiKeyService.resolve` caches a valid key in Redis for five
minutes and an unknown or revoked one for one minute; a revocation deletes the cached entry, and
`last_used_at` is written at most every ten minutes per key.

## Adding a forecast

- A route whose whole answer is a forecast: `@RequiresApiKey()`.
- A forecast field on a route that has other data: `@ForecastFields("path.to.field")` on the
  route, with the field's path in `omitPaths` syntax (`attractions[].bestVisitTimes`). A route
  that validates its query with a DTO under `forbidNonWhitelisted` needs `forecast` on the DTO,
  as `FavoritesQueryDto` has.
- Add the row to the table above.
