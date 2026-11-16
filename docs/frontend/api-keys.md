# Data API keys

Which routes need a data key and which permission, how a client sends one, how a developer gets
one, and how park.fan's own frontend gets through. Open it before adding a public route, or a
field to a route that mixes data of several kinds. The terms behind it are on park.fan:
<https://park.fan/en/impressum#data-use>; the developer portal, where keys are requested and
their holders manage them, is <https://park.fan/en/developers>.

## The rule

Without a key a caller reads the live wait times and nothing else
(`GET /v1/parks/{…}/{park}/wait-times`, `KEYLESS_PERMISSIONS` in `src/api-keys/permissions.ts`).
Every other public route needs a data key that carries the route's permission. A key carries a
list of permissions, granted one by one or a group at a time:

| Group     | Permission             | Opens                                                                                                         |
| --------- | ---------------------- | ------------------------------------------------------------------------------------------------------------- |
| live      | `live.waits`           | `/wait-times`; open without a key                                                                             |
| live      | `live.schedule`        | `/schedule`; `schedule`, show times and opening hours in the park and ride detail                             |
| live      | `live.weather`         | `/weather`, `/weather/forecast`, `/weather/nowcast`; `weather` in the park detail                             |
| parks     | `parks.directory`      | `/v1/parks` lists, `/popular`, `/v1/discovery/**`, `/v1/search`, `/v1/sitemap/attractions`                    |
| parks     | `parks.details`        | the park detail `…/{park}`, `/v1/favorites`                                                                   |
| parks     | `parks.seasons`        | `…/{park}/seasons`; the season and maintenance fields of the park and ride detail                             |
| rides     | `rides.details`        | `…/{park}/attractions`, the ride detail; heights, single rider, virtual queue, coordinates in the park detail |
| rides     | `rides.profiles`       | `rideProfile` (manufacturer, figures, elements); `/v1/glossary/terms/**`                                      |
| stats     | `stats.live`           | crowd level, occupancy, trend, the comparison with a typical day                                              |
| stats     | `stats.history`        | `/stats`, `/stats/hourly`, `/stats/day`; history, typical waits, rope drop, downtime                          |
| stats     | `stats.platform`       | `/v1/analytics/{realtime,geo-live,ticker,best-times}`, `/v1/page-views/top`                                   |
| stats     | `stats.accuracy`       | `/v1/ml/accuracy/system`, `/v1/trip-regions/{region}/accuracy`, `predictionAccuracy`                          |
| forecasts | `forecasts.crowd`      | `/calendar`, `/best-days`, `/predictions/yearly`, `/parky-tip`; a region's `/calendar`, `/best-days`, `/when` |
| forecasts | `forecasts.waits`      | `?forecast=true`: `bestVisitTimes`, `hourlyForecast`, the forecast in a day curve                             |
| planner   | `planner.day`          | `/plan/day`, `POST /plan/optimize`                                                                            |
| planner   | `planner.trip`         | a region's `/plan` and `/plan/ideas`                                                                          |
| travel    | `travel.regions`       | `/v1/trip-regions`, `/v1/trip-regions/{region}`                                                               |
| travel    | `travel.tickets`       | a region's `/tickets`                                                                                         |
| travel    | `travel.school-breaks` | a region's `/school-breaks`                                                                                   |

`GET /v1/api-keys/permissions` serves the catalog with a description of each, and
`GET /v1/api-keys/verify` names the permissions of the key it checks.

- **Each route names its permission.** `@ApiPermission("…")` (`api-permission.decorator.ts`)
  puts the guard on the route and the security entry in Swagger; `@OpenRoute("why")` marks a route
  that serves no licensed data (health, push, trips, Parky, the site statistic's beacon, OAuth,
  the key check and the catalog). `api-permission.coverage.spec.ts` imports every controller and
  fails on a route that is neither behind the admin's guard nor classified, and on a second route
  open without a key.
- **The two mixed answers lose what the caller may not read.** The park and the ride detail need
  `parks.details` and `rides.details`; `@PermissionFields("park" | "ride")` then drops each field
  whose permission the caller lacks (`PERMISSION_FIELDS`), on a copy, never on the cached object.
- **Refusals.** Without a key on any route but `/wait-times`: 401 `api-key-required`, naming the
  permission and the portal. A key without the permission: 403 `api-key-permission-missing` with
  `requiredPermission`, before the request counts against the allowance. A key that is unknown,
  revoked or past its end date is refused on `/wait-times` too.
- **Legacy keys.** A key issued before permissions has `permissions = null` and reads every
  permission (`LEGACY_KEY_PERMISSIONS`), so no key lost a route on the day permissions arrived. The
  admin shows it as legacy until it gets a list.
- **`internal`.** park.fan's own key (`usage: internal`) holds every permission whatever its list
  says, plus `internal`, which the ML monitoring routes and `/v1/parks/debug/missing-geocode`
  need and nobody else gets. The throttle bypass key counts as the same while it still opens data
  routes (`FORECAST_BYPASS_KEY` not `off`).
- **Counting.** Every keyed request a route lets through counts against the key's hourly allowance
  and in its statistics, not only forecasts as before.
- **Caching.** A keyless answer on `/wait-times` keeps its public policy. Every keyed answer, and
  every refusal, is private (or varies on the key with `FORECAST_SHARED_CACHE=vary`, below), so
  Cloudflare never hands one caller's copy to another.

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

A keyless caller reaches only `/wait-times` now (above). The throttle below still counts it.
Public data reads (`/v1/parks/**`, `/v1/discovery/**` except `nearby`, `/v1/search`, the public
`/v1/analytics/*` reads, `/v1/glossary/**`, `/v1/sitemap/**`, `/v1/page-views/top`, `/v1/ml/**`)
from a caller without an active data key are limited to `THROTTLE_ANON_LIMIT` per minute
(default 60) and `THROTTLE_ANON_HOURLY_LIMIT` per hour (default 1,000) per address, on top of the
general circuit breaker. Past one, a 429 `rate-limited` says a free key lifts it. The routes
park.fan's own pages call from the browser (assistant, site statistic, favourites, nearby, trips,
push) are not on the list: one carrier-NAT address can carry a whole park's visitors. A made-up
key counts as none. `isAnonymousDataRequest` and `CfThrottlerGuard.handleRequest`.

## Issuing keys

`/v1/admin/api-keys` (owner only, audited as `api-key.create`, `api-key.update`,
`api-key.revoke`, `api-key.rotate`, `api-key.previous.revoke` and `api-key.delete`): `GET` lists
every key, active first, with `requestsThisHour`;
`POST { name, contact?, usage?, note?, requestsPerHour?, allowedParkIds?, expiresAt?, permissions? }` returns `{ key, item }` with the key in
clear exactly once (without `permissions` the key reads every permission, as a legacy key does); `PATCH /:id { requestsPerHour?, allowedParkIds?, expiresAt?, permissions? }` changes any of them from the next request on
(an unknown park id is a 400); `DELETE /:id` revokes; rotation is below. The table `api_keys`
stores the SHA-256 of the key and its first ten characters. `ApiKeyService.lookup` caches a found
key in Redis for five minutes and an unknown one for one minute, and stores only where nothing is
cached (`NX`). A revocation writes the refusal over the entry and a `PATCH` writes the new state, so
a lookup that read the old row a moment earlier cannot put it back. `last_used_at` is written at
most every ten minutes per key, and a process asks Redis about it at most that often.

`POST /:id/delete` deletes a revoked key for good, so it leaves the list; an active key is a 409
and has to be revoked first, so its holder is always refused before anything is erased. The row
goes, and with it every Redis counter of the key (hour, day, refusals, month) and its last-use
markers: nothing else reads them, since the list shows each key's own use and no total across
keys. Both secrets' lookups are written as refused again, in case the revocation's refusal never
reached Redis. No table points at `api_keys`; the audit rows name the key by id and label, so its
history stays readable there, `api-key.delete` with usage, prefix and both dates included.

## Rotation

A key whose secret leaked, or whose holder changes hands, gets a new secret without losing what
the key is: name, usage, allowance, parks, end date and statistics stay, because they hang on the
row's id, not on the secret. A rotation is an `UPDATE` of the same row, not a new key.

`POST /v1/admin/api-keys/:id/rotate { graceHours }` (0 to 720, whole hours) generates the secret
exactly as a create does and answers `{ key, item }` in the same shape, the key in clear exactly
once. The old secret's hash moves to `previous_key_hash` and keeps working until
`previous_key_expires_at`, so the holder can deploy the new one without an outage; with
`graceHours: 0` there is no previous secret and the old one is refused from the next request on.
A rotation during a grace period replaces the secret that grace kept, which stops at once. A
revoked key, or one past its end date, is a 409: its new secret would open nothing.
`DELETE /v1/admin/api-keys/:id/previous` ends a running grace period now (404 when none runs).

- **Both secrets are the same key** while the grace runs: one id, one allowance, one set of
  counters. The old secret reports the end of its grace as its end date when that comes first, so
  `X-API-Key-Expires` and `/v1/api-keys/verify` tell its holder when it stops. After the grace it
  is `api-key-invalid`, as an unknown value is.
- **The cache never outlives the grace.** The old secret's lookup is cached for five minutes at
  most and never past `previous_key_expires_at` (`PX`, the remaining milliseconds), and the entry
  carries the grace's end, compared on every read like an end date. A rotation writes the refusal
  over the old secret's entry when it ends it (no grace, a second rotation, `DELETE …/previous`, a
  revocation, which refuses both secrets), for the reason a revocation does.
- **Each secret has its own last use.** `last_used_at` is the current secret's, `previous_last_used_at`
  the old one's, written with the same ten-minute throttle, and the write names the hash, so a
  request with the old secret never marks the new one used. A rotation moves `last_used_at` to
  `previous_last_used_at` and starts the new secret's at null: the admin sees whether anyone has
  switched. `rotated_at` is the last rotation.
- **No hash leaves the API.** The item adds `rotatedAt`, `previousExpiresAt` (null without a
  previous secret, in the past once its grace has run out) and `previousLastUsedAt`; the audit rows
  hold the prefixes, `graceHours` and the grace's end, never a secret.

The four columns are nullable and were added by `synchronize`
([the entities are the schema](../rules/the-entities-are-the-schema.md)); a key that was never
rotated has them null and behaves as before.

## Developer accounts and requests

A developer registers on the portal and gets an account in `admin_users` with the role
`developer`, ranked below `viewer` (`ADMIN_ROLE_RANK`). Every admin route's minimum is `viewer` or
above, so the account reaches only the handlers that name `developer`: its own `me`, logout,
password, sessions and second factor in `AdminAuthController`, and `DeveloperController`. It sees
no MCP tool and cannot mint a token or approve an OAuth grant; `developer-role.spec.ts` lists
exactly the handlers it reaches. `ADMIN_REQUIRE_TOTP` does not apply to it.

- `POST /v1/developer/register { email, displayName?, password, turnstileToken? }`: creates the
  account and signs it in, with the login's answer. Turnstile as at the login; 409
  `account-exists` for a taken address. Audited as `developer.register`.
- `GET /v1/developer/overview`: the caller's keys (with `requestsThisHour` and `stats`) and
  requests.
- `POST /v1/developer/requests`: a key request (`CreateApiKeyRequestDto`: project, website,
  kind, platforms, languages, description, open source with repository and licence, revenue,
  permissions, endpoints, volume, parks, server-side key, caching, the page with the backlink,
  message, locale, terms). `tier` is `free` only for open source that earns nothing but donations
  (`requestTier`), `paid` otherwise. At most three undecided at once. `DELETE …/requests/:id`
  withdraws an undecided one.
- `POST /v1/developer/keys/:id/rotate { graceHours }`, `DELETE …/keys/:id/previous`,
  `DELETE …/keys/:id`: the admin's rotation, end of grace and revocation, on the caller's own keys
  only (`ApiKeyService.findOwned`); another account's key id is a 404.

Every request is decided by hand, by the owner, at `/v1/admin/api-key-requests`: `GET` lists
them with the applicant, `POST /:id/approve { permissions, name?, usage?, requestsPerHour?,
allowedParkIds?, expiresAt?, note?, message? }` issues a key bound to the applicant
(`owner_user_id`, `request_id`) with `revealed_at` null, `POST /:id/reject { reason? }`,
`POST /:id/reopen` and `DELETE /:id`. Audited as `api-key-request.*`. Nobody sees an approved
key's first secret: its holder picks it up on the dashboard, which is a rotation with
`graceHours: 0` and sets `revealed_at`. The API sends no mail; the dashboard shows the decision
and the owner's message.

## Adding a route

- A public route names its permission: `@ApiPermission("parks.directory")`, or
  `@ApiPermission("forecasts.crowd", { parkRequired: true })` where a route without a park could
  reach any park's data and a key limited to some parks must be refused. A route that licenses no
  data is `@OpenRoute("why")`.
- A new permission goes into `API_PERMISSIONS`, a group, `API_PERMISSION_DESCRIPTIONS` and the
  frontend's mirror (`lib/developers/permissions.ts` in park.fan) in the same change.
- A field of a new kind on the park or ride detail goes into `PERMISSION_FIELDS` under its
  permission; a forecast field stays with `@ForecastFields("path.to.field")`, whose
  `?forecast=true` asks for `forecasts.waits`. A route that validates its query with a DTO under
  `forbidNonWhitelisted` needs `forecast` on the DTO, as `FavoritesQueryDto` has.
- Add the row to the table above.
