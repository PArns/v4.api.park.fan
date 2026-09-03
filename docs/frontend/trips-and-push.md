# Stored plans and web push (`/v1/trips`, `/v1/push`)

Two endpoints that exist for one reason: the planner has always lived in
`localStorage`, which is the right default — no account, works offline, belongs
to nobody but the visitor — and two things a plan is asked for need the server to
have seen it. **Sharing a link**, and **a notification that knows what is next.**

---

## 1. The id is the credential

`POST /v1/trips` stores a plan and returns its id. There is no account system for
visitors and none is being built, so knowing the id is the whole of the
authorisation:

- 96 bits of `randomBytes`, 16 base64url characters, never derived from the
  plan's contents (a park slug and a date would be guessable in an afternoon),
- `GET /v1/trips/:id` returns it to anyone holding the id,
- `PUT /v1/trips/:id` **replaces** it — never merges. The browser holds the whole
  plan and is the only writer that knows what was deleted; merging server-side
  would resurrect an entry somebody removed.
- A `PUT` to an id that does not exist is a **404**, not a create. Creating at a
  caller-chosen id would let an attacker pick their own ids and overwrite a trip
  by guessing one.

**A UI that shows the link has to say this plainly.** It is a shareable secret,
not a private document.

An expired trip reads as absent (404), because whether the sweep has run yet is
not the caller's business. Trips live 400 days from their last write and a daily
job removes the rest.

## 2. What counts as a plan

`POST`/`PUT` take `{ "payload": … }` and store the payload **verbatim**: the
planner's shape is a frontend concern, and versioning it in two repositories
would guarantee the two disagree.

What this API does insist on is a floor, because an unauthenticated write
endpoint that accepts any JSON is a free key-value store with a good domain in
front of it, and it *will* be found:

- an object with a numeric `version` and a map of `parks`,
- each park with a `slug` and (optionally) a map of days,
- each day with a `YYYY-MM-DD` `date` and (optionally) a list of entries carrying
  an `id` and a numeric `startMinute`,
- at most 60 parks, 400 days per park, 200 entries per day,
- **256 KB** of UTF-8 in total.

Everything below that skeleton is passed through untouched.

Writes are rate-limited per address — 20 creates and 600 updates an hour — by the
module's own limiter rather than the global throttler, which skips our own
frontend and would therefore have enforced the limit against everybody except the
planner.

## 3. Push: ask before offering the switch

`GET /v1/push` answers `{ available, publicKey?, topics }`. **The browser must
ask this before it offers a notification control**: `available: false` means this
deploy has no VAPID keypair, and a switch that turns on and does nothing is the
worst state this feature has.

`POST /v1/push/subscriptions` takes `{ endpoint, p256dh, auth, tripId, locale?,
timezone?, topics? }` and answers 204. It is an **upsert on the endpoint**: a push
service hands the same URL back every time a page re-subscribes, and inserting
would deliver every notification once per page load.

It refuses, rather than storing something that can never produce a notification:

| status | when |
| --- | --- |
| 503 | this deploy has no VAPID keypair |
| 404 | no trip with that id |
| 400 | the endpoint is not an https URL **at a known push service**, or no known topic is left |

The endpoint host is checked against the four push services (FCM, Mozilla, WNS,
Apple), extensible through `PUSH_ENDPOINT_HOSTS`. That check is not tidiness: the
stored string is a URL this server POSTs to on a schedule with nobody watching,
and accepting an arbitrary one makes the subscribe endpoint a request forwarder
pointed wherever the caller likes.

`DELETE /v1/push/subscriptions` with `{ endpoint }` forgets a browser and is
idempotent — a browser that revoked permission has no way to know whether its
subscription ever reached us.

### Topics

`next-up` — the next block in the plan is about to start. That is the whole list,
and it is short on purpose: "a ride in your plan stopped running" and "rain is
moving in" are both obviously wanted and both need a producer that does not exist
yet. A topic goes in the same commit as the thing that sends it, or not at all.

### What arrives

```json
{ "title": "In 10 Min.: Taron", "body": "14:30 Uhr, Phantasialand", "url": "/", "tag": "next-up:phantasialand:2026-10-17:e17:870" }
```

Written in the **subscriber's** language (de, en, nl, fr, es, it; anything else
falls back to English) because the job runs with no request to read an
`Accept-Language` from. The `tag` is load-bearing: a phone in a pocket for two
hours must surface one banner rather than four about moments that have all passed.

A notification goes out 10 to 20 minutes before a block starts, from a job that
ticks every five minutes — so a block is seen on two or three consecutive runs and
one missed run costs nothing. The duplicate that implies is absorbed by a Redis
marker per (endpoint, event) and, failing that, by the `tag` on the device.

## 4. Operational notes

- Neither endpoint is CDN-cacheable. A trip is one visitor's, and a shared edge
  copy would hand the next reader somebody else's plan.
- Subscriptions and trips live in **Postgres**, not Redis: this instance runs
  `allkeys-lru`, so an evicted counter merely resets a rate-limit window, while an
  evicted subscription is a visitor who silently stops being notified.
- A subscription is deleted the first time a push service answers 404 or 410 (the
  browser is gone and the service is saying so) and after 8 consecutive other
  failures, which reset on the next success.
- With no VAPID keypair the endpoints answer 503 and the job does not run. Push
  guards nothing, so taking the API down over a missing key would make every other
  endpoint depend on a feature nobody asked for.

## Related

- [`/plan/day`](./plan-day-endpoint.md) — the per-day series a stored plan is built around
- `.env.example` — `VAPID_*` and `PUSH_ENDPOINT_HOSTS`
