# Location Resolution & GeoIP

## Overview

Several endpoints need a user location (coordinates) for distance-based behaviour:

- **`GET /v1/discovery/nearby`** – Find nearby parks or rides; **location required** (lat/lng or derived from IP).
- **`GET /v1/favorites`** – Optional **user location** for distance in meters on parks, attractions, shows, restaurants.

When the client does not send `lat` and `lng`, the API can derive coordinates from the client IP using MaxMind GeoLite2-City (GeoIP).

## Resolution Order

The same order is used for both nearby and favorites:

1. **Query params `lat` and `lng`** – If both valid (lat ∈ [-90, 90], lng ∈ [-180, 180]), use them.
2. **Query param `ip`** – Optional; for debugging or when the client wants to force a specific IP for GeoIP lookup.
3. **Client IP from request** – From headers or socket, then normalized (see below).
4. **GeoIP lookup** – Resolve IP to city-level coordinates via GeoLite2-City.

If after this no coordinates are available:

- **Nearby**: Returns `400` with a message to provide lat/lng or ensure GeoIP and client IP are available.
- **Favorites**: Request still succeeds; `userLocation` is omitted and no distances are returned.

## Client IP and Normalization

- **Utils**: `src/common/utils/request.util.ts` – `getClientIp(req)`, `getFirstHeader(req, name)`, `normalizeIp(ip)`.
- **Header order**: `X-Forwarded-For` (first entry) → `CF-Connecting-IP` → `True-Client-IP` → `X-Real-IP` → `X-Forwarding-IP` → `req.ip` → `socket.remoteAddress`.
- **Normalization**: Strip port (e.g. `1.2.3.4:8080`), strip IPv4-mapped prefix `::ffff:`.

## GeoIP (MaxMind GeoLite2-City)

- **Module**: `src/geoip/` – `GeoipModule` (global), `GeoipService`.
- **Database**: MMDB file (path from `GEOIP_DATABASE_PATH`, default `./data/GeoLite2-City.mmdb`).
- **Download**: MaxMind Basic Auth; requires `GEOIP_MAXMIND_ACCOUNT_ID` and `GEOIP_MAXMIND_LICENSE_KEY`. If the file is missing and credentials are set, the DB is downloaded asynchronously on startup (start not blocked). See [.env.example](../../.env.example) and [Setup](../development/setup.md).
- **Updates**: BullMQ job `geoip-update` / `update-geolite2-city` runs every 48 hours (see [Job Queues](job-queues.md)).
- **Lookup**: `GeoipService.lookupCoordinates(ip)` returns `{ latitude, longitude }` or `null`; `isAvailable()` indicates if the DB is loaded.

## Endpoints Summary

| Endpoint                 | Location required? | Behaviour without location          |
|--------------------------|--------------------|-------------------------------------|
| `GET /v1/discovery/nearby` | Yes                | 400 Bad Request                     |
| `GET /v1/favorites`        | No                 | 200 OK, no `userLocation`, no distances |

## Frontend / API Consumers

- Prefer sending `lat` and `lng` when the client has GPS or user-selected position.
- Without lat/lng, ensure the request carries the real client IP (e.g. proxy sets `X-Forwarded-For` or `CF-Connecting-IP`) so GeoIP can resolve. Optional `ip` query param is for debugging only.
