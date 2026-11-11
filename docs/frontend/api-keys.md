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

## Caching

A keyed answer is never shared. `keepResponsePrivate` wraps the response's `setHeader`, so
whichever interceptor or handler sets `Cache-Control` afterwards, it leaves as `private`, with the
`max-age` it asked for and no `s-maxage`; `CDN-Cache-Control` is dropped. The guard does this
before it decides, because a guard runs before every interceptor and its 401 would otherwise
leave with no `Cache-Control` at all. Copies Cloudflare cached before this change live out their
TTL; a purge of `api.park.fan` ends them at once.

## Issuing keys

`/v1/admin/api-keys` (owner only, audited as `api-key.create` and `api-key.revoke`): `GET` lists
every key, active first; `POST { name, contact?, usage?, note? }` returns `{ key, item }` with the
key in clear exactly once; `DELETE /:id` revokes. The table `api_keys` stores the SHA-256 of the
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
