# Location resolution and GeoIP

How `GET /v1/discovery/nearby` and `GET /v1/favorites` find the user's coordinates — from
`lat`/`lng`, or from the client IP via MaxMind GeoLite2-City — and why both answers are never shared
by a CDN. Open this when distances are missing or nearby answers 400.

## Resolution order

1. Query `lat` and `lng`, if both are valid (lat ∈ [-90, 90], lng ∈ [-180, 180]).
2. Query `ip` — for debugging, forces a GeoIP lookup of that address.
3. The client IP from the request (`getClientIp`, `src/common/utils/request.util.ts`): the first entry
   of `X-Forwarded-For`, then `CF-Connecting-IP`, `True-Client-IP`, `X-Real-IP`, `X-Forwarding-IP`,
   `req.ip`, `socket.remoteAddress`; `normalizeIp` strips a port and the `::ffff:` prefix.
4. `GeoipService.lookupCoordinates(ip)` → `{ latitude, longitude }` or `null` (city level).

| Endpoint | Controller | Without coordinates |
| --- | --- | --- |
| `GET /v1/discovery/nearby` | `src/location/location.controller.ts` | `400` |
| `GET /v1/favorites` | `src/favorites/favorites.controller.ts` | `200`, no `userLocation`, no distances |

A client should send `lat`/`lng` when it has them; otherwise the proxy in front must pass the real
client IP (`X-Forwarded-For` or `CF-Connecting-IP`).

## GeoIP database

- `GeoipModule` (global) / `GeoipService` in `src/geoip/`; `isAvailable()` says whether the MMDB is
  loaded.
- File: `GEOIP_DATABASE_PATH`, default `./data/GeoLite2-City.mmdb`. When it is missing and
  `GEOIP_MAXMIND_ACCOUNT_ID` / `GEOIP_MAXMIND_LICENSE_KEY` are set, it is downloaded in the background
  at startup (boot is not blocked); see `.env.example`.
- Refreshed by `geoip-update` / `update-geolite2-city` every two days
  ([job queues](job-queues.md)).

## Never cached by a CDN

The answer can depend on the client IP, so both endpoints use `NoCdnCacheInterceptor`
(`Cache-Control: private, no-store, no-cache, must-revalidate`, `Pragma: no-cache`).
