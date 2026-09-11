# Stored plans and web push (`/v1/trips`, `/v1/push`, `/v1/push/ride-alerts`, `/v1/push/show-follows`)

Two endpoints that exist for one reason: the planner has always lived in
`localStorage`, which is the right default — no account, works offline, belongs
to nobody but the visitor — and two things a plan is asked for need the server to
have seen it. **Sharing a link**, and **a notification that knows what is next.**

Push itself grew a second and third reason after that: a visitor wants to know
when a **specific show** is about to start, or when a **specific ride's** wait
time drops low enough — neither of which has anything to do with a stored
plan. Sections 4 and 5 cover those; sections 1–3 are unchanged from when push
existed only for the trip planner.

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
- `DELETE /v1/trips/:id` removes it, by the same rule: whoever knows the id may.
  **204** when the row goes, **404** when there is nothing behind the id, **429**
  when the address is over the write limit.

**Deleting exists because switching push off must not make a plan unreachable
instead of gone.** The browser forgets the id at that moment; anybody who kept it
— a log, a backup, an old device — could otherwise read and overwrite the plan
for the rest of its 400 days, while its owner could not reach it at all.

Deleting also clears the `tripId` on every push subscription pointing at the
trip, in the same transaction, and **keeps those rows**: a subscription is the
browser's, not the trip's, and the same row carries that browser's ride alerts
and followed shows. It is exactly what `POST /v1/push/unsubscribe` with a
`tripId` does for one endpoint, one level wider. The nightly sweep does the same
for what it removes — it clears every pointer that no longer resolves, so a
subscription is never left holding an id the notification job will chase for the
life of the browser.

Both writers take the row (`SELECT … FOR UPDATE`) before they touch it, and both
take `trips` before `push_subscriptions`. The first is what stops a `PUT` in
flight from putting a just-deleted plan back at the same id with a fresh expiry,
after the browser has already dropped that id; the second is what stops the
delete and the sweep from deadlocking on the pair.

**A UI that shows the link has to say this plainly.** It is a shareable secret,
not a private document.

An expired trip reads as absent (404), because whether the sweep has run yet is
not the caller's business. Trips live 400 days from their last write and a daily
job removes the rest. That holds for `DELETE` too — an expired row answers 404
and is left to the sweep, so there is one definition of what exists rather than
one per verb.

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
planner. A `DELETE` counts against the **update** bucket rather than one of its
own: it needs an id the caller already knew and it adds no row, and a third
bucket would hand a script a fresh allowance for guessing ids. The count happens
before the lookup on every verb, so a miss costs an attempt exactly as a hit
does — otherwise enumeration would be free, since a miss is the only answer an
enumeration ever gets.

## 3. Push: ask before offering the switch

`GET /v1/push` answers `{ available, publicKey?, topics }`. **The browser must
ask this before it offers a notification control**: `available: false` means this
deploy has no VAPID keypair, and a switch that turns on and does nothing is the
worst state this feature has.

`POST /v1/push/subscriptions` takes `{ endpoint, p256dh, auth, tripId?, locale?,
timezone?, topics? }` and answers 204. It is an **upsert on the endpoint**: a push
service hands the same URL back every time a page re-subscribes, and inserting
would deliver every notification once per page load.

**`tripId` and `topics` are both optional, and omitting either is not the same
as clearing it.** A browser that only wants a ride alert or a followed show
sends neither — it has nothing to say about the trip planner — and the write
leaves whatever this endpoint already had for those two fields untouched. Only
a value that is actually **sent** is validated and stored: `tripId`, if
present, must name a trip that exists; `topics`, if present, must contain at
least one topic this deploy sends. The one row in `push_subscriptions` is
shared by all three features (a trip, a followed show, a ride alert), because
a push service issues one endpoint per browser regardless of why it
subscribed.

It refuses, rather than storing something that can never produce a notification:

| status | when |
| --- | --- |
| 503 | this deploy has no VAPID keypair |
| 404 | `tripId` was sent but names no trip |
| 400 | the endpoint is not an https URL **at a known push service**, or `topics` was sent with no known topic left |

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

## 4. Ride alerts (`/v1/push/ride-alerts`)

A visitor watches an attraction and asks to be told once its STANDBY wait
drops below a threshold they pick — independent of any trip, so subscribing
needs no `tripId` at all (see §3's preserve-on-omit note).

`GET ?endpoint=` lists a browser's alerts; `POST { endpoint, attractionId,
thresholdMinutes }` upserts one (1–240 minutes); `DELETE { endpoint,
attractionId }` removes one, idempotently. A cap of 100 alerts per
subscription exists for the same reason `trips` caps its payload — an
unauthenticated write endpoint that accepts unbounded writes is a free
key-value store. Writes are rate-limited per address by the module's own
Redis counter, same reasoning as `TripWriteRateLimitService`: the global
throttler is bypassed for our own frontend, which is where all real traffic
comes from.

The write refuses two things a stored alert could never do anything about:

| status | when |
| --- | --- |
| 404 | no subscription for that endpoint, or no such (non-retired) attraction |
| 400 | `thresholdMinutes` out of range, or this park's wait times can never be read (`getNoLiveWaitTimesReason`) |

Being out of season is **not** refused — an alert may be set up ahead of a
ride's season, it simply will not fire until the ride is confirmed running
(`isCurrentlyInSeason(...) !== false`, never `=== true` — the same rule the
seasonal-attractions doc describes, applied here so an alert cannot fire
against a ride the detector has merely not understood yet).

**There is no cron for this.** `RideAlertsService.checkAndNotify` runs
straight out of `WaitTimesProcessor`, once per park per five-minute poll,
right after that park's attractions are saved — the same cycle that already
decided whether to write a `queue_data` row, not a fixed rota of its own. It:

1. Skips the whole park if `getNoLiveWaitTimesReason` is set — permanent, not
   a freshness check.
2. Filters the polled attraction ids down to the ones anybody has an alert
   on (almost always none — most rides have no watcher).
3. Reads each one's current STANDBY reading (status + wait) and drops any
   that are not `OPERATING`, have no wait time, or are out of season.
4. Hands the readings and the alert rows to `diffRideAlerts` — a pure
   function with no I/O of its own (`ride-alerts/ride-alert-transitions.ts`).

`diffRideAlerts` is the whole of the decision, and the whole of the dedupe:
each `RideAlert` carries an `armed` boolean rather than a Redis marker,
because a threshold crossing has no lead window to overlap on consecutive
ticks the way a "starts in 10–20 minutes" trip block does — it is evaluated
once, on the cycle that produced the reading. `armed: true` means the next
reading under the threshold fires and flips it to `false`; it flips back to
`true` only once the wait genuinely rises back to the threshold or above,
which is what lets the same alert fire again later the same day if a queue
builds back up and drops a second time.

```json
{ "title": "Taron: nur noch 15 Min.", "body": "Phantasialand", "url": "/parks/europe/germany/bruehl/phantasialand/taron", "tag": "ride-alert:3f2c…" }
```

## 5. Followed shows (`/v1/push/show-follows`)

The same shape as ride alerts, minus the threshold: `GET ?endpoint=` lists,
`POST { endpoint, showId }` upserts (no parameter — following an already-
followed show is a no-op), `DELETE { endpoint, showId }` unfollows. Same
100-per-subscription cap, same address-rate-limited writes, same 404 for a
subscription-less endpoint or a show that does not exist. Unlike a ride,
there is no "can this park's data ever be trusted" gate — a show is either
found or it is not.

**This one DOES run on the existing five-minute push cron**, as a second,
independent branch of the same job that sends trip-planner notifications
(`PushNotificationProcessor.handleDue`) — a `ShowFollow` row is not a trip
topic, so it is read alongside, not filtered through, `subscription.topics`.
Each tick: load every follow, batch-fetch each followed show's current live
status (`ShowsService.findBatchCurrentStatusByShows`, the same
staleness-checked, already-day-projected data the park endpoint itself
serves), and hand the operating ones to `dueShowNotifications` — a pure
function in `show-follows/show-follow-notifications.ts`.

That function needs **no timezone math to decide whether a showtime is
due**, unlike `dueNotifications`: `ShowLiveData.showtimes[].startTime` is
already a full, park-day-projected ISO instant by the time it gets there, so
subtracting it from "now" is a plain duration. The park's timezone is only
read to *format* the display clock (`atTime`, e.g. `"20:30"`) — a show whose
timezone this deploy cannot resolve or format is skipped entirely rather than
shown in the wrong hour, the same rule §3 follows for trips. The window is
25–35 minutes before the showtime (wider than the five-minute tick, for the
same "one missed run costs nothing" reason as the trip window), and the
dedupe key is the showtime's own ISO string — already unique, so unlike a
trip block's `startMinute` it needs no separate date component.

```json
{ "title": "In 30 Min.: Feuerwerk", "body": "20:30 Uhr, Europa-Park", "url": "/parks/europe/germany/rust/europa-park#shows", "tag": "show-start:9ab1…:2026-10-17T18:30:00.000Z" }
```

## 6. Operational notes

- None of these endpoints is CDN-cacheable. Every one of them answers for one
  visitor's own data, and a shared edge copy would hand the next reader
  somebody else's plan, alerts, or follows.
- Subscriptions, trips, alerts and follows all live in **Postgres**, not
  Redis: this instance runs `allkeys-lru`, so an evicted counter merely
  resets a rate-limit window, while an evicted subscription (or an evicted
  alert) is a visitor who silently stops being notified.
- A subscription is deleted the first time a push service answers 404 or 410 (the
  browser is gone and the service is saying so) and after 8 consecutive other
  failures, which reset on the next success. `ride_alerts` and `show_follows`
  cascade with it at the database level — an alert has no purpose once its
  subscription is gone, so there is no separate sweep job for either.
- With no VAPID keypair the endpoints answer 503 and neither cron branch does
  any work. Push guards nothing, so taking the API down over a missing key
  would make every other endpoint depend on a feature nobody asked for.

## Related

- [`/plan/day`](./plan-day-endpoint.md) — the per-day series a stored plan is built around
- [Attraction status and seasonality](../architecture/attraction-status-and-seasonality.md) — the `isCurrentlyInSeason` rule §4's check reuses
- [Live wait-times availability](./live-wait-times-availability.md) — `getNoLiveWaitTimesReason`, the gate §4 checks per park
- `.env.example` — `VAPID_*` and `PUSH_ENDPOINT_HOSTS`
