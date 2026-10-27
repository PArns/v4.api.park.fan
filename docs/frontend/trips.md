# Stored plans (`/v1/trips`)

The contract for storing a trip-planner plan on the server: the id as the only
credential, full-replace writes, the payload floor, expiry, and the per-address
write limits (whose address rule and 429 shape the push routes share). Push
notifications built on a stored plan are in [push](./push.md).

The planner lives in the browser's `localStorage` (no account, works offline). A
plan is stored here only for what needs the server to have seen it: **sharing a
link** and **a notification that knows what is next**.

## The id is the credential

There are no visitor accounts, so knowing the id is the whole authorisation.

| call                    | answers                                                                                                   |
| ----------------------- | --------------------------------------------------------------------------------------------------------- |
| `POST /v1/trips`        | **201** `{ id, payload, expiresAt, updatedAt }`                                                           |
| `GET /v1/trips/:id`     | **200** the trip, to anyone holding the id; **404** if absent or expired                                  |
| `PUT /v1/trips/:id`     | **replaces** it, never merges (the browser is the only writer that knows what was deleted); **404** for an id that does not exist — never a create, or a caller could pick ids and overwrite a trip by guessing one |
| `DELETE /v1/trips/:id`  | **204** when the row goes, **404** when nothing is there, **429** over the write limit                    |

- The id is 96 bits of `randomBytes`, 16 base64url characters, never derived from
  the plan. An id that is not `[A-Za-z0-9_-]{1,32}` is a 404 without a database
  lookup.
- **A UI that shows the link must say it is a shareable secret**, not a private
  document.
- **Expiry.** A trip lives 400 days from its last write; the daily `trip-sweep-cron`
  removes the rest. An expired trip reads as absent (404) on every verb, whether or
  not the sweep has run.
- **Delete exists so switching push off makes a plan gone, not unreachable** — the
  browser forgets the id at that moment, and anyone who kept it could otherwise read
  and overwrite the plan for the rest of its 400 days. Deleting also clears `tripId`
  on every push subscription pointing at the trip, in the same transaction, and
  **keeps those rows**: a subscription belongs to the browser and also carries its
  ride alerts and followed shows. The sweep clears dangling pointers the same way.
- Both writers lock the row (`SELECT … FOR UPDATE`) and take `trips` before
  `push_subscriptions`: the first stops an in-flight `PUT` from resurrecting a
  just-deleted plan, the second stops the delete and the sweep deadlocking.
- None of these responses is CDN-cacheable (`NoCdnCacheInterceptor`): each answers
  for one visitor's own data.

## What counts as a plan

`POST`/`PUT` take `{ "payload": … }` and store it **verbatim** — the planner's shape
is the frontend's, and versioning it in two repos would make them disagree. The API
insists only on a floor (`src/trips/trip-payload.util.ts`), because an
unauthenticated write endpoint that takes any JSON is a free key-value store:

- an object with a numeric `version` and a map of `parks`,
- each park with a `slug` and optionally a map of days,
- each day with a `YYYY-MM-DD` `date` and optionally a list of entries with an `id`
  and a numeric `startMinute`,
- at most 60 parks, 400 days per park, 200 entries per day, **256 KB** of UTF-8.

Everything below that skeleton passes through untouched. A push feature that reads
the plan (`geo`, `timezone`, `attractionName`, `label`) is described in
[push](./push.md).

## Write limits

Per address, **20 creates and 600 updates an hour**
(`TripWriteRateLimitService`), by the module's own Redis counter rather than the
global throttler — that one skips our own frontend, so it would have limited
everybody except the planner. A `DELETE` spends the **update** bucket (a third
bucket would hand a script a fresh allowance for guessing ids). The count happens
before the lookup on every verb, so a miss costs an attempt like a hit.

**The address is `getRateLimitIp`** (`src/common/utils/request.util.ts`):
`X-Forwarded-For` only on a request carrying a valid throttle bypass key (our own
frontend proxy), then `CF-Connecting-IP` only on a request carrying
`CF_ORIGIN_SECRET` (it provably came through Cloudflare), else the socket address.
Trusting a caller-written `X-Forwarded-For` let a script buy a fresh bucket per
request. The push, ride-alert and show-follow limiters use the same key.

**A 429 says how long to wait**, in two places that agree: the body's
`retryAfterSeconds` (exact) and a `Retry-After` header in whole seconds, rounded up.
A browser calling cross-origin must read the body: `Retry-After` is not a
CORS-safelisted header and nothing exposes it. The header is for non-browser
clients and for the **global** throttler, whose 429 carries no
`retryAfterSeconds`.
