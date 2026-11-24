# Web push (`/v1/push`, `/v1/push/ride-alerts`, `/v1/push/show-follows`)

The push contract: asking whether push works, subscribing a browser, the topics
(`next-up`, `rain-soon`, `morning-plan`; `plan-change` for the app), ride alerts, followed shows, quiet hours,
and how subscriptions die. One `push_subscriptions` row per browser serves every
feature. Stored plans themselves are in [trips](./trips.md), which also owns the
per-address rate-limit rule and the 429 shape every route here shares.

## Ask before offering the switch

`GET /v1/push` answers `{ available, publicKey?, topics }`. **A browser must ask
before it shows a notification control**: `available: false` means this deploy has
no VAPID keypair, and a switch that turns on and does nothing is the worst state
this feature has. Without the keypair every write answers 503 and no job branch
does any work — push never takes the rest of the API down with it.

## Subscribing a browser

`POST /v1/push/subscriptions` with `{ endpoint, p256dh, auth, tripId?, locale?,
timezone?, topics? }` → **204**. An **upsert on `endpoint`**: a push service hands
the same URL back on every re-subscribe.

**`tripId`, `topics` and `timezone` are optional, and omitting one is not clearing
it.** A browser that only wants a ride alert sends none, and the write leaves what
was stored untouched. Only a value actually sent is validated and stored: `tripId`
must name a trip that exists, `topics` must contain at least one topic this deploy
sends. Sending a `timezone` that cannot be used does clear it.

| status | when                                                                                                       |
| ------ | ---------------------------------------------------------------------------------------------------------- |
| 503    | no VAPID keypair                                                                                           |
| 404    | `tripId` sent but names no trip                                                                            |
| 400    | the endpoint is not an https URL **at a known push service**, or `topics` sent with no known topic left    |
| 429    | more than 60 subscription writes an hour from this address (own bucket)                                    |

Known push services are FCM, Mozilla, WNS and Apple (`src/push/push-config.ts`),
extensible through `PUSH_ENDPOINT_HOSTS`. The check is security, not tidiness: the
stored URL is one this server POSTs to on a schedule, and an arbitrary one would
make the endpoint a request forwarder.

`DELETE /v1/push/subscriptions` with `{ endpoint }` forgets a browser, idempotently.

## Topics

`PUSH_TOPICS` = `next-up`, `rain-soon`, `morning-plan`. A topic ships in the same
change as the job that sends it, or not at all. All three run inside the
five-minute `push-notifications-cron` (`PushNotificationProcessor`), as does the
show branch below; ride alerts run elsewhere.

Every message is written in the **subscriber's** language (de, en, nl, fr, es, it;
anything else English), because the job has no request to read a language from.
The `tag` is load-bearing: a phone in a pocket for two hours surfaces one banner,
not four about moments that have passed. A sent event leaves a Redis marker
(`push:sent:<hash(endpoint)>:<hash(key)>`, 45 minutes) as the first dedupe; the
device's `tag` is the second.

### `next-up` — the next block of a stored plan

Sent 10–20 minutes before a block starts (`LEAD_MIN`/`LEAD_MAX_MIN`,
`src/push/notification-planner.ts`), so a block is seen on two or three ticks and one
missed run costs nothing.

```json
{ "title": "In 10 Min.: Taron", "body": "14:30 Uhr, Phantasialand", "url": "/", "tag": "next-up:phantasialand:2026-10-17:e17:870" }
```

### `morning-plan` — the day's plan before the park opens (PAR-895)

For subscriptions with a `tripId` and the topic: one message per park and park day,
due from 60 minutes before opening but not before 07:00 park-local
(`isMorningPlanDue`), only while the park is still closed, and only when the plan
has at least one named entry for the park's today. The opening is the earliest
`OPERATING` schedule entry of that day (`ParksService.getScheduleForDate`), the park
resolved through the plan's `geo` path; no `geo`, zone, schedule or `openingTime`
means no message. Names come from the stored payload (`attractionName`, or a custom
block's `label`); nothing is resolved server-side. Its marker lives 12 hours
(the window can be an hour wide); a subscriber in quiet hours gets no marker and is
tried again on later ticks.

```json
{ "title": "Phantasialand öffnet um 09:00 Uhr", "body": "09:30 Taron\n10:00 Black Mamba", "url": "/parks/europe/germany/bruehl/phantasialand", "tag": "morning-plan:phantasialand:2026-10-17" }
```

### `rain-soon` — rain is about to reach a park (PAR-848)

`RainSoonPushService` (`src/queues/services/rain-soon-push.service.ts`), for
subscribers with the topic who have either a plan with a block on the park's today
or a ride alert on one of its rides. A plan names parks by slug plus `geo`
(continent, country, city), and the geo path resolves them; a park without `geo` or
`timezone` is skipped.

- **When.** `decideRainSoon` (`src/push/rain-soon.ts`) reads the park's nowcast and
  sends when rain starts at most 30 minutes ahead (`RAIN_LEAD_MIN`, the frontend's
  `SHELTER_LEAD_MINUTES`). Rain already falling is not "soon": nothing is sent and
  the park is marked raining.
- **Once per front.** Per-park Redis state `push:rain-soon:<parkId>`: `warned` (3 h)
  then `raining` (6 h), deleted when the rain stops, which re-arms the park for a
  second front. The first write is an atomic `SET NX`. A forecast that stops
  showing rain does not re-arm; a Redis error skips the park.
- **What it says.** "Regen ab etwa 14:20 Uhr", the park and, when the park passes
  the gate, up to three open covered rides with their standby wait
  (`RAIN_PUSH_RIDE_LIMIT`). The gate and ranking are the frontend's
  (`lib/utils/covered-rides.ts`: `coveredOfferReady`, `COVERED_MIN_KNOWN_SHARE` 0.5,
  `rankCoveredRides`) **written once more** here and pinned by `rain-soon.spec.ts` —
  change both repos or neither. No walking distance (a push does not know where the
  visitor stands). A ride counts as open only with a fresh (15 min), non-heartbeat
  `OPERATING` standby reading. A park with no readable wait-time source names no
  rides.

```json
{ "title": "Regen ab etwa 14:20 Uhr", "body": "Phantasialand. Überdacht und offen: Maus au Chocolat (15 Min.), Fly (20 Min.)", "url": "/parks/europe/germany/bruehl/phantasialand", "tag": "rain-soon:<parkId>:2026-10-17:<startMs>" }
```

The frontend switches for `rain-soon` and `morning-plan` are open tickets (PAR-849,
PAR-896).

## Ride alerts (`/v1/push/ride-alerts`)

A browser watches a ride, independent of any trip.

- `GET ?endpoint=` lists a browser's alerts (a missing `endpoint` is a 400, never
  "any subscription"). `POST { endpoint, attractionId, thresholdMinutes }` upserts
  a **wait alert** (1–240 min) → 200 with the alert. `POST { endpoint,
  attractionId, kind: "reopen" }` upserts a **reopen alert** (`thresholdMinutes`
  ignored and `null`). One alert per ride and browser: sending the other kind
  replaces it. `DELETE { endpoint, attractionId }` removes one, idempotently.
- At most 100 alerts per subscription (`MAX_RIDE_ALERTS_PER_SUBSCRIPTION`), 60
  writes an hour per address in their own bucket.
- **404**: no subscription for that endpoint, or no such non-retired attraction.
  **400**: threshold out of range; the park's wait times can never be read
  (`getNoLiveWaitTimesReason`); a reopen alert on a free-flow attraction
  (`openWithPark`, it has no closed-to-open flank); over the cap.
- **Out of season is not refused**: the alert waits until the ride is confirmed
  running (`isCurrentlyInSeason(...) !== false` — [seasonality](./seasonality.md)).

**No cron.** `RideAlertsService.checkAndNotify` runs from `WaitTimesProcessor`, once
per park per five-minute poll, right after the park's attractions are saved. It
skips a park with no readable source, narrows the polled rides to watched ones,
reads each one's current STANDBY reading and drops any that is not a fresh,
non-heartbeat `OPERATING` reading with a wait, or out of season, then hands readings
and alerts to the pure `diffRideAlerts` (`src/ride-alerts/ride-alert-transitions.ts`).

- **Wait alerts** dedupe with an `armed` column, not Redis: the next reading under
  the threshold fires and disarms; the alert re-arms when the wait rises back to the
  threshold or above, or when a park-local day boundary passes (a ride closed
  overnight produces no reading to re-arm it).
- **Reopen alerts** read the status, never the wait. `armed` means "last seen not
  operating". The first fresh (15 min), non-heartbeat `OPERATING` reading in season
  fires and the alert is **spent** — it never re-arms (or every morning would send
  it again); a new `POST` asks anew. Because `queue_data` stores changes, the status
  is read over the last 6 hours (`REOPEN_STATUS_WINDOW_MINUTES`); only the
  `OPERATING` row has to be fresh. A new alert starts armed only if the ride is seen
  not operating now. Out of season reads as not operating. No day-boundary re-arm.

```json
{ "title": "Taron: nur noch 15 Min.", "body": "Phantasialand", "url": "/parks/europe/germany/bruehl/phantasialand/taron", "tag": "ride-alert:3f2c…" }
```

## Followed shows (`/v1/push/show-follows`)

`GET ?endpoint=` lists (retired shows left out, their rows kept);
`POST { endpoint, showId, startTime? }` upserts → 200; `DELETE { endpoint, showId }`
unfollows idempotently. At most 100 per subscription, 60 writes an hour per address;
404 for an unknown endpoint or a show that does not exist or is retired.

- **`startTime`** (ISO instant) pins one performance; omitted means whichever
  performance comes next, on any day — what a show card's bell files. Re-following
  overwrites `startTime`. A pinned follow is swept 24 h after its performance
  (`EXPIRY_GRACE_HOURS`); an open-ended one never is. The response carries
  `startTime`, the park's `timezone` and `path` to render it.
- A second branch of the five-minute job: each tick loads every follow and reads
  each show's **verified** showtimes for today
  (`ShowsService.getShowtimeInstantsOnDate` — checked against their own date, so the
  feed's stale entries cannot fire; `findBatchCurrentStatusByShows` supplies only
  name and timezone), then the pure `dueShowNotifications`
  (`src/show-follows/show-follow-notifications.ts`).
- Due **25–35 minutes** before the showtime, and again **8–14 minutes** before for a
  follow made too late for the first window (`SHOW_LEAD_*`, `SHOW_LATE_LEAD_*`). No
  timezone math decides due-ness (the instant is absolute); the zone only formats
  the clock, and a show whose zone cannot be resolved is skipped. A pinned follow
  fires only for its own performance. Dedupe key: the showtime's ISO string.

```json
{ "title": "In 30 Min.: Feuerwerk", "body": "20:30 Uhr, Europa-Park", "url": "/parks/europe/germany/rust/europa-park#shows", "tag": "show-start:9ab1…:2026-10-17T18:30:00.000Z" }
```

## iOS app (APNs, `/v1/push/devices`)

The app has no browser endpoint and no account. It registers its **install**:
`POST /v1/push/devices` with `{ installId (UUID), token (APNs hex), kinds,
newsParks?, locale?, timezone? }` → 200 `{ endpoint, kinds, newsParks }`. The row
is an ordinary `push_subscriptions` row with `endpoint = apns:<installId>`,
`apnsToken` set and empty web keys, so the app then calls
`/v1/push/ride-alerts` and `/v1/push/show-follows` with that `endpoint` exactly as
a browser does, and every trigger above reaches it. The install id, not the
token, is the identity: a rotated token keeps the alerts. A token registered by
another install (a restore onto a new phone) moves, so one phone never gets a
banner twice. `DELETE /v1/push/devices { installId }` forgets the install with
its alerts and follows. Same 429 bucket as subscribe.

- **The device chooses, the server enforces.** `kinds` is the device's whole
  choice and replaces what was stored: `ride-wait`, `ride-reopen`, `show`,
  `news`, `plan-change`, plus the trip topics. `PushService.send` drops any message whose kind
  (`pushKindOf`: its `kind`, else its tag prefix) the device did not choose, for
  every producer. An empty list keeps the alerts and sends nothing.
- **Transport.** `src/push/apns.ts`: ES256 provider token (refreshed every 50
  minutes), HTTP/2 to `api.push.apple.com` (`APNS_PRODUCTION=false`: sandbox),
  `apns-expiration` 15 minutes like the web TTL, the tag as collapse id (hashed
  past 64 bytes). Payload: `aps.alert {title, body}` plus `url` (the same
  site-relative path a browser opens) and `kind`.
- **Dead tokens.** 410, or 400 `BadDeviceToken` / `DeviceTokenNotForTopic`,
  deletes the row; anything else (403, 429, 5xx, timeout) leaves it alone —
  those hit every device at once and are never one device's fault.
- **No key, no push.** Without all of `APNS_KEY_ID`, `APNS_TEAM_ID`,
  `APNS_BUNDLE_ID`, `APNS_KEY`, `GET /v1/push` answers `apns: false`, registering
  is a 503 and no device is sent anything.

### `news` — a new post in the news export

`NewsPushService`, a fourth half of the five-minute job, for devices with
`news`. It reads `<NEWS_EXPORT_BASE>/news/<locale>.json` (the file the app
itself reads, so a post is announced when the app can open it) for each locale
that has readers. A slug first added to the Redis set `push:news:seen:<locale>`
(no TTL, so never evicted) is new; a missing set is filled silently, and a post
published more than 6 hours ago is never announced. `newsParks` narrows to posts
about those parks (geo paths, else park slugs). Quiet hours drop it.

```json
{ "title": "Europa-Park startet in den Winter", "body": "<teaser>", "url": "/news/europa-park-wintersaison-2026", "tag": "news:europa-park-wintersaison-2026" }
```

### `plan-change` — something a stored plan was built around changed

`PlanChangePushService`, a fifth half of the five-minute job, **for devices
only**: rows with an `apnsToken`, a `tripId` and the kind. The website has no
switch or wording for it, so it is not a `PUSH_TOPICS` entry. A device links its
plan by sending `tripId` on `POST /v1/push/devices` (omitted keeps, `null`
clears, an unknown or expired id is a 404, checked under the trip's row lock like
`subscribe`). The plan is the ordinary `/v1/trips` payload; the linked device also
receives `next-up` and `morning-plan` for it when it chose those kinds.

Each plan day from the park's today up to 120 days out is compared with a
**baseline**: what the device last heard (the facts when watching started, then
the facts of each sent digest). Rules (`src/push/plan-change.ts`):

| rule      | what changes                                                                                                   | when read                       |
| --------- | -------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| hours     | earliest OPERATING opening / latest closing of the day moves, or the day turns `CLOSED` (or back)              | every watched day               |
| shows     | a planned performance is no longer in the day's `scheduled` times (nearest time named), or a scheduled show drops out of a still-scheduled day (cancelled) | today; a projection says nothing |
| rides     | a planned, not yet ridden ride goes from running to not running or back (reopen-alert reading: any non-OPERATING standby row is down, only a fresh non-heartbeat OPERATING row is open) | today, while the park is open |
| waits     | the `/plan/day` forecast at the planned slot moves by ≥ 15 min **and** ≥ 30 %                                  | up to 7 days out                |

Unknown on either side says nothing (a first sight is the baseline), except a
show first seen away from its planned time. A digest names at most three
changes and counts the rest. **Pacing**: the first digest of a day goes at once,
then at most one every 30 minutes, and one holding only wait changes at most
every 3 hours. A change not sent (pacing, quiet hours) stays pending against the
baseline, so a blip that reverts in between is never sent and a drift adds up.

**State and bounds.** The baseline lives in Redis
(`push:plan-change:<hash(tripId)>:<slug>:<date>`) until three days after the
date; an evicted one restarts in silence. At most 150 plan days per tick
(`MAX_PLAN_DAYS_PER_RUN`), 31 per trip, trips in random order, 60 s of wall
clock, each day isolated; nothing at all without the `APNS_*` variables. A ride
first seen down is not remembered, so a ride opening in the morning is not
"running again"; `/plan/day` comes from its shared
quarter-hour cache and is only read for days with rides or shows within the
show/forecast reach.

```json
{ "title": "Dein Plan: Phantasialand, Sa., 17.10.", "body": "Öffnungszeiten jetzt 09:00–17:00 Uhr (vorher 09:00–18:00)\nTaron ist gerade außer Betrieb", "url": "/trip-planner?park=europe%2Fgermany%2Fbruehl%2Fphantasialand&date=2026-10-17", "tag": "plan-change:phantasialand:2026-10-17", "kind": "plan-change" }
```

## Quiet hours

**Nothing is sent between 23:00 and 07:00 in the subscriber's own timezone** (the
`timezone` sent on subscribe, never the park's; `src/push/quiet-hours.ts`), checked
at each trigger, for every feature. Every trigger fires inside the park's hours, so
the window exists for the subscriber who is elsewhere — a Magic Kingdom plan read in
Berlin puts a 21:00 block at 03:00. 22:00–08:00 was rejected: a park open until
23:00 would lose the last hour of "your next block starts in ten minutes".

- **Held back means dropped**, not deferred: trip blocks and show reminders leave
  their windows on their own and no marker is written; a ride alert keeps its
  `armed` row and fires on a fresh crossing later; `morning-plan` retries until the
  park opens.
- **No stored zone means send** (old rows, or a zone this deploy cannot resolve).
- Two named limits: the zone is the one the browser last sent, so a traveller
  carries the home zone into the park until the frontend writes it again (PAR-266);
  and a performance from 23:14 loses its late reminder, from 23:35 its regular one
  (PAR-267).

## Operational notes

- None of these endpoints is CDN-cacheable (`NoCdnCacheInterceptor`).
- Subscriptions, trips, alerts and follows live in **Postgres**, not Redis: this Redis
  runs `volatile-lru`, and an evicted subscription is a visitor who silently stops
  being notified.
- A subscription is deleted the first time a push service answers 404 or 410, and
  after 8 consecutive other failures (`MAX_FAILURES`, reset on success).
  `ride_alerts` and `show_follows` cascade with it in the database.
- Configuration: `VAPID_*`, `PUSH_ENDPOINT_HOSTS`, `APNS_*` and
  `NEWS_EXPORT_BASE` in `.env.example`.
