# Edge caching, throttling and request trust

The cache headers the API sends to Cloudflare and browsers, the global rate limiter, and which client
address each limiter believes. Open it before giving a route a cache policy, adding a limiter, or
changing a `THROTTLE_*` or `CF_ORIGIN_SECRET*` variable. Which origins may call the API from a
browser: [CORS](cors.md). Why a write needs a second revalidation after the edge TTL:
[publish a write outside-in](../rules/publish-outside-in.md).

## Cache headers

Three interceptors, innermost first. The global one stays out of a response that already carries a
`Cache-Control`.

| Interceptor | Sends | Where |
| --- | --- | --- |
| `HttpCacheInterceptor(maxAge, sMaxAge?, isDegraded?)` (`src/common/interceptors/cache.interceptor.ts`) | `public, max-age=<maxAge>, s-maxage=<sMaxAge ?? maxAge>, stale-while-revalidate=<maxAge ≤ 120 ? maxAge : 2 × maxAge>`; `CDN-Cache-Control` only when `sMaxAge` differs (no route uses the split today); `no-store` when `isDegraded(body)` is true | per route, on most public reads; each TTL is on the route's contract page under [`docs/frontend/`](../frontend/README.md) |
| `NoCdnCacheInterceptor` (`no-cdn-cache.interceptor.ts`) | `private, no-store, no-cache, must-revalidate` and `Pragma: no-cache` | answers that depend on the caller: `/v1/favorites`, `/v1/discovery/nearby`, `POST /v1/page-views`, `POST /v1/analytics/collect`, the `trips`, `push`, `push/ride-alerts` and `push/show-follows` controllers, and `/v1/mcp` |
| `CacheControlInterceptor` (`cache-control.interceptor.ts`, global in `src/main.ts`) | by path, see below | every other response |

The global policy by path, first match wins:

| Request | `Cache-Control` |
| --- | --- |
| not `GET`/`HEAD` | `no-store, no-cache, must-revalidate` |
| `/` | none from here; `AppController` sets `public, max-age=3600` and `86400` itself |
| a path containing `/api` (Swagger UI, `/api-json`) | 1 h for `-json` and `swagger` paths, else 5 min |
| `/admin` or `/ml` as a path segment | `private, no-store, no-cache, must-revalidate` |
| `/health` | `public, max-age=2, s-maxage=2` |
| `/page-views/top` | 1 h, `stale-while-revalidate=86400` |
| anything else | `public, max-age=300, s-maxage=300, stale-while-revalidate=600` |

The file has more branches, but none changes an answer: `/holidays` matches no route, and
`/predictions`, `/attractions`, `/search` and the rest either sit behind the `/parks` branch or give
the same 5 minutes. A policy for one route goes on the route, not into this list. The global
interceptor adds `Vary: Accept-Encoding` to its own policies, and `Last-Modified` to any success whose
payload carries `lastUpdated`, `updatedAt` or `timestamp`. ETags and `304`s come from Express's own weak ETag; no code here makes them.

What to keep in mind when giving a route a policy:

- **Only a success gets a public policy.** `HttpCacheInterceptor` and `CacheControlInterceptor` both
  set their header in `tap()`, which runs only on success, so an error leaves the API without any
  `Cache-Control`. A route whose answer depends on the caller's address, credentials or a secret id
  uses `NoCdnCacheInterceptor`, which sets its header before the handler, so its 400, 404 and 429 are
  `private` as well.
- **An answer that says "try again shortly" is not cached.** Pass `isDegraded`, as `/plan/day` does
  for `ridesUnavailable.reason === "data_unavailable"`; otherwise the edge keeps the failure for up to
  `maxAge` plus the stale window.
- **Guard on `response.headersSent`.** Both interceptors skip a client that has already gone: setting a
  header then throws `ERR_HTTP_HEADERS_SENT`, which Nest logged as a 500 (48–178 a day on the
  park-detail routes). A new interceptor keeps the guard.
- **A route may set its own header.** The nowcast aligns `max-age` to its `nextUpdateAt` with no
  stale window ([weather](../frontend/weather.md)).
- **This service cannot purge Cloudflare.** A change reaches the edge when the TTL runs out, which is
  why the publishing order revalidates twice.

## The global throttler

`CfThrottlerGuard` (`src/common/guards/cf-throttler.guard.ts`) is the `APP_GUARD`, configured by
`getThrottlerOptions()` (`src/common/throttler/throttler.config.ts`). It is a circuit breaker against
retry loops and scrapers, not a fairness limiter: one carrier-NAT address can be hundreds of visitors,
and Cloudflare absorbs most reads before they reach the origin.

| Variable | Default | Effect |
| --- | --- | --- |
| `THROTTLE_LIMIT` | `300` | requests per key per window; `0` does not register the guard, which also switches off every `@Throttle()` |
| `THROTTLE_TTL` | `60` | window in seconds |
| `THROTTLE_BYPASS_HEADER` | `x-auth-key` | header that carries a bypass key (lowercased) |
| `THROTTLE_BYPASS_KEYS` | empty | comma-separated keys; a request carrying one skips the guard. Empty means no bypass |

- **Counted in process memory.** No storage is configured, so the count is per container and starts
  again at every restart.
- **Skipped:** `@SkipThrottle()` (the health controller) and every request with a valid bypass key,
  which park.fan sends on every server-side call. The `@Throttle()` overrides on the admin login
  (10 a minute), `change-password` (5), `totp/begin` (10) and `totp/disable` (5) are skipped for it too,
  so they only bind callers that do not go through the frontend. The OAuth endpoints of the admin MCP (`register` 10,
  `authorize` 30, `token` 60, `revoke` 30) are such callers: MCP clients call them directly.
- **Read the variables lazily.** `ConfigModule` loads `.env` after the imports run; a module-level
  `process.env` read falls back to its default silently. This is why the config file exports functions.

## Which address a request is

`carriesCfOriginSecret()` (`throttler.config.ts`) decides whether a request may name its own client
address. While `CF_ORIGIN_SECRET` is empty (the default) every request may. Once it is set, only a
request whose `CF_ORIGIN_SECRET_HEADER` (default `x-cf-origin-secret`) carries that value may; the
header is meant to be added by a Cloudflare Transform Rule or cloudflared. Both `.env` examples list
the two variables; nothing in this repository records whether production sets them.

| Reader | Request with a bypass key | May name its address | May not |
| --- | --- | --- | --- |
| `CfThrottlerGuard.getTracker` | not counted | `CF-Connecting-IP`, else the first `X-Forwarded-For` hop, else the socket | the socket |
| `getRateLimitIp` (`src/common/utils/request.util.ts`): trips, push writes, page views | the first `X-Forwarded-For` hop (the visitor the frontend forwards) | `CF-Connecting-IP`, else the socket | the socket |
| `clientIp` (`src/admin/auth/admin-auth.guard.ts`): admin login, sessions, audit | the first `X-Forwarded-For` hop | `CF-Connecting-IP`, else the first `X-Forwarded-For` hop, else `req.ip`, whatever `CF_ORIGIN_SECRET` says | same |
| `getClientIp` (`request.util.ts`): GeoIP for favorites and nearby only | first `X-Forwarded-For` hop, then `CF-Connecting-IP` and three more headers, unvalidated ([location resolution](../architecture/location-resolution.md)) | same | same |

Every limiter key must pass `isIP()`: an unvalidated header lets a caller send a fresh string per
request and open a new bucket each time. The socket is `req.socket.remoteAddress`, never `req.ip`:
with `trust proxy` on, `req.ip` is the caller-written leftmost `X-Forwarded-For` hop.

The traps:

- **Unset, a caller that reaches the origin directly can rotate its address.** Any valid
  `CF-Connecting-IP` is believed, so every request can land in a fresh bucket of the throttler and of
  every `getRateLimitIp` limiter.
- **Set without the Cloudflare rule, all visitors share a bucket.** No request carries the header,
  so every key falls back to the socket, which is a Cloudflare edge or the proxy in front of the
  container, never the visitor. For every request without a bypass key (page views always, trips and
  push whenever the browser calls the API directly) the 300-a-minute breaker, the write limits and the
  500 counted page views a day then apply to everybody at once. Add the Transform Rule first, then set
  the variable.
- **A bypass key also vouches for `X-Forwarded-For`.** The frontend's relay must forward the visitor's
  address (park.fan: `getForwardedForHeaders` in `lib/api/relay.ts`), or all its users share one
  bucket. A leaked key skips the throttler and the admin login's Turnstile and can name any address;
  rotate it through `THROTTLE_BYPASS_KEYS`, where old and new keys can overlap.
- **The admin login ignores `CF_ORIGIN_SECRET`.** `clientIp` believes forwarded headers from anyone;
  Turnstile is the gate for callers outside the frontend ([admin authentication](../admin/authentication.md)).

## What this means for the per-feature limiters

The guard skips the frontend, and the frontend carries almost every real write, so a `@Throttle()` on
a write route would limit everybody except its real users. A feature that has to be limited counts in
Redis itself, on every request:

| Limiter | Key | Limit | Contract |
| --- | --- | --- | --- |
| `TripWriteRateLimitService` | `getRateLimitIp` | 20 creates and 600 updates or deletes an hour | [trips](../frontend/trips.md) |
| `PushFollowWriteRateLimitService` (through `PushFollowAccessGuard.writeGuard`) | `getRateLimitIp` | 60 an hour each for `subscription`, `ride-alert`, `show-follow` | [push](../frontend/push.md) |
| the caps in `SiteAnalyticsCollectService` | `addressBucket(getRateLimitIp, 56)` (an IPv6 address counts as its /56; a reader is the /64 and the User-Agent) | 3000 hits per address and UTC day, 5000 per five-minute window for everybody; for `page_read` also 3 per reader and page, 10 per address and page, 200 per address; new values per address in the day's guards | [site statistic](../frontend/site-analytics.md#guards), [page views](../frontend/page-views.md) |
| `AdminLoginRateLimitService` | `clientIp` and the account | failures per 15 min: 25 per address, 10 per account, 10 per account and action | [admin authentication](../admin/authentication.md) |
| `OAuthClientService.register` | `addressBucket(getRateLimitIp)` | 10 client registrations an hour |
| `OAuthAuthorizationService.authorize` | `addressBucket(getRateLimitIp)`, and the client id | 30 pending authorization requests per 10 minutes each | [admin MCP](../admin/mcp.md#security) |

All five let a request through when Redis fails. Without a readable address the trip, push, page-view
statistic and registration limits do not count at all and the admin login counts the account only: a limiter that refuses everybody because a
header went missing is worse than one that briefly counts nothing.
