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

| status | when                                                                                                    |
| ------ | ------------------------------------------------------------------------------------------------------- |
| 503    | no VAPID keypair                                                                                        |
| 404    | `tripId` sent but names no trip                                                                         |
| 400    | the endpoint is not an https URL **at a known push service**, or `topics` sent with no known topic left |
| 429    | more than 60 subscription writes an hour from this address (own bucket)                                 |

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
All texts follow one pattern (`src/push/push-messages.ts`): the **title** is the
thing and the number or time that decides ("Taron: 15 Min."), never the kind's
name; a long name is cut so the number stays (`fit`, 32 characters with the pictogram). The **place**
is a line of its own: `aps.alert.subtitle` on a device, the first line of `body`
in a browser, whose payload stays `{ title, body, url, tag }`. The **body** is the
reason or the next number. No exclamation mark; times are the park's clock.
The examples below are the browser's payload; what a device gets on top is in
[the device payload](#the-device-payload).
The `tag` is load-bearing: a phone in a pocket for two hours surfaces one banner,
not four about moments that have passed. A sent event leaves a Redis marker
(`push:sent:<hash(endpoint)>:<hash(key)>`, 45 minutes) as the first dedupe; the
device's `tag` is the second.

**One pictogram per kind** leads the title, the same in all six languages, for
browsers and devices alike (`PUSH_PICTOGRAMS` in `src/push/push-config.ts`, put
on in `PushService.send`): a stack is then read by its first character. It
leads the title and not the subtitle because the title is the one line every
surface shows. Each is a single code point that is an emoji without a variation
selector; a text carries no other emoji, which `push-messages.spec.ts` pins.

| kind          | pictogram | kind                           | pictogram |
| ------------- | --------- | ------------------------------ | --------- |
| `ride-wait`   | ⏳        | `plan-change`                  | 🔄        |
| `ride-reopen` | ✅        | `morning-plan`                 | 🌅        |
| `next-up`     | 🎢        | `news`                         | 📰        |
| `show`        | 🎭        | `guides`                       | 📖        |
| `rain-soon`   | ☔        | the app's own reminder (local) | 🚶        |

**Parky speaks in three subtitles** and nowhere else: "Guten Morgen" on the
morning plan, "Parky hat Neues für dich" on news and "Parky hat einen neuen
Guide" on guides (each in the reader's language).

### `next-up` — the next block of a stored plan

Sent 10–20 minutes before a block starts (`LEAD_MIN`/`LEAD_MAX_MIN`,
`src/push/notification-planner.ts`), so a block is seen on two or three ticks and one
missed run costs nothing.

```json
{
  "title": "🎢 Taron in 10 Min.",
  "body": "Phantasialand\nGeplant für 14:30 Uhr.",
  "url": "/trip-planner?park=europe%2Fgermany%2Fbruehl%2Fphantasialand&date=2026-10-17",
  "tag": "next-up:phantasialand:2026-10-17:e17:870"
}
```

The `url` is the plan day (`/` for a plan whose park has no `geo`).

### `morning-plan` — the day's plan before the park opens (PAR-895)

For subscriptions with a `tripId` and the topic: one message per park and park day,
due from 60 minutes before opening but not before 07:00 park-local
(`isMorningPlanDue`), only while the park is still closed, and only when the plan
has at least one named entry for the park's today. The opening is the earliest
`OPERATING` schedule entry of that day (`ParksService.getScheduleForDate`), the park
resolved through the plan's `geo` path; no `geo`, zone, schedule or `openingTime`
means no message. Names come from the stored payload (`attractionName`, or a custom
block's `label`); nothing is resolved server-side. The body's first sentence is the
day as `/plan/day` reads it (`context.crowdLevel`, `weather.tempMax`,
`weather.rainChance`; each left out when unknown, the whole sentence when that read
fails), its second the first two entries. "Guten Morgen" is the one greeting any
message carries. Its marker lives 12 hours
(the window can be an hour wide); a subscriber in quiet hours gets no marker and is
tried again on later ticks.

```json
{
  "title": "🌅 Phantasialand öffnet um 09:00 Uhr",
  "body": "Guten Morgen\nAndrang niedrig, bis 14 °C, 20 % Regenrisiko. Zuerst Taron um 09:30 Uhr, dann Black Mamba.",
  "url": "/parks/europe/germany/bruehl/phantasialand",
  "tag": "morning-plan:phantasialand:2026-10-17"
}
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
{
  "title": "☔ Regen ab etwa 14:20 Uhr",
  "body": "Phantasialand\nÜberdacht und offen: Maus au Chocolat (15 Min.), Fly (20 Min.)",
  "url": "/parks/europe/germany/bruehl/phantasialand",
  "tag": "rain-soon:<parkId>:2026-10-17"
}
```

The tag is one per park and park day, so the warning of a second front replaces
the first on the screen.

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
  overnight produces no reading to re-arm it). The same alert notifies at most every
  30 minutes (`WAIT_ALERT_GAP_MS`): a queue hovering at the threshold stays armed and
  fires once the gap has passed and the wait is still under it.
- **Reopen alerts** read the status, never the wait. `armed` means "last seen not
  operating". The first fresh (15 min), non-heartbeat `OPERATING` reading in season
  fires and the alert is **spent** — it never re-arms (or every morning would send
  it again); a new `POST` asks anew. Because `queue_data` stores changes, the status
  is read over the last 6 hours (`REOPEN_STATUS_WINDOW_MINUTES`); only the
  `OPERATING` row has to be fresh. A new alert starts armed only if the ride is seen
  not operating now. Out of season reads as not operating. No day-boundary re-arm.

```json
{
  "title": "⏳ Taron: 15 Min.",
  "body": "Phantasialand\nUnter deiner Grenze von 20 Min.",
  "url": "/parks/europe/germany/bruehl/phantasialand/taron",
  "tag": "ride-alert:3f2c…"
}
```

A reopen alert is "Taron ist wieder offen" with the park as its body.

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
{
  "title": "🎭 Feuerwerk in 30 Min.",
  "body": "Europa-Park\nBeginn 20:30 Uhr.",
  "url": "/parks/europe/germany/rust/europa-park#shows",
  "tag": "show-start:9ab1…:2026-10-17T18:30:00.000Z"
}
```

## iOS app (APNs, `/v1/push/devices`)

The app has no browser endpoint and no account. It registers its **install**:
`POST /v1/push/devices` with `{ installId (UUID), token (APNs hex), kinds,
newsParks?, locale?, timezone?, environment? }` → 200 `{ endpoint, kinds, newsParks }`. The row
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
  `news`, `guides`, `plan-change`, plus the trip topics. `PushService.send` drops any message whose kind
  (`pushKindOf`: its `kind`, else its tag prefix) the device did not choose, for
  every producer. An empty list keeps the alerts and sends nothing.
- **Transport.** `src/push/apns.ts`: ES256 provider token (made new every 50
  minutes, never within 20, for both environments), HTTP/2 with one connection
  per environment, the tag as collapse id (hashed past 64 bytes). What the
  request carries per kind is [the device payload](#the-device-payload).
- **Both environments, no setting.** A token works only where it was issued:
  the sandbox (`api.sandbox.push.apple.com`) for a build installed from Xcode,
  production (`api.push.apple.com`) for TestFlight and the App Store; the other
  answers 400 `BadDeviceToken`. The app may say which with `environment:
"sandbox" | "production"` at registration (its `aps-environment`
  entitlement: `development` is the sandbox). A row without one
  (`push_subscriptions.apnsEnvironment` null: an older app, or a new token
  sent without the field) is tried in production first; `BadDeviceToken` there
  is asked again in the other environment, and the one that accepts is stored
  on the row. The same retry runs for a row whose stored environment refuses
  its token. `APNS_PRODUCTION=false` only makes the sandbox the first try. A
  server that does not know the field yet answers 400 to a body with it, so the
  app sends it only once this is deployed.
- **Dead tokens.** 410, 400 `DeviceTokenNotForTopic`, or 400 `BadDeviceToken`
  from both environments deletes the row. Anything else (403, 429, 5xx,
  timeout, a second environment that gave no verdict) leaves it alone — those
  hit every device at once and are never one device's fault — and its reason is
  kept in `apnsLastError` until the next send that arrives. A key Apple
  restricts to one environment answers 403 `BadEnvironmentKeyIdInToken` in the
  other; then the first environment's `BadDeviceToken` stands. The circuit
  breaker counts unanswered requests per environment, so a wrong-environment
  answer never opens it.
- **No key, no push.** Without a key `GET /v1/push` answers `apns: false`,
  registering is a 503 and no device is sent anything. The key is the `.p8`
  file uploaded in the admin (below); the variables `APNS_KEY_ID`,
  `APNS_TEAM_ID`, `APNS_BUNDLE_ID`, `APNS_KEY` are the fallback while none is
  uploaded.

### The APNs key in the admin (`/v1/admin/push/apns`)

The owner uploads Apple's `AuthKey_<KEYID>.p8` on the admin's page "Push
(iOS)"; nothing is set on the server and nothing restarts. `ApnsAdminService`
keeps it in the one row of `apns_settings`, the PEM sealed with AES-256-GCM
under `ASSISTANT_SECRET_KEY` exactly as Parky's Claude key is
(`src/assistant/assistant-secret.util.ts`); without that variable the upload is
refused with 409. An uploaded key wins over the `APNS_*` variables, is used
from the next send on (new connections, new provider token) and makes `GET
/v1/push` answer `apns: true`. No answer and no audit row carries the key: both
name it by `fingerprint`, the first 8 hex characters of the SHA-256 over its
public key. Routes and roles: [admin endpoints](../admin/endpoints.md#push-to-the-ios-app-adminpushapns).

- **Upload** (`PUT key`, JSON `{ content, filename?, keyId?, teamId?,
bundleId? }`): `content` is the file's text. The key id comes from the file
  name unless sent (ten capitals or digits; 400 asking for it when neither has
  one), `teamId` defaults to `KC5QAXDX94`, `bundleId` to `fan.park.app`. Stored
  only when the text holds a PKCS#8 EC P-256 private key that signs a provider
  token; a certificate, an RSA or public key, anything over 4096 characters or
  anything else is a 400 in German that never repeats the content.
- **Check** (`POST check`, and after every upload): one request per environment
  on a connection of its own, a background push (`content-available`, priority
  5, expiration 0) to 32 random bytes as the device token. 400 `BadDeviceToken`
  means Apple accepted the provider token, the team and the topic and only
  misses the device, so nobody is notified; 403 `InvalidProviderToken` is a key
  id or team that does not fit the key, `TopicDisallowed`/`BadTopic` the bundle
  id, `BadEnvironmentKeyIdInToken` a key Apple restricts to the other
  environment. `explainApnsAnswer` (`apns-key.ts`) turns each reason into the
  German sentence the page shows. The last answer per environment is stored
  with the key's fingerprint and shown only for the key in use.
- **Test** (`POST test { deviceId }`): a real alert to one registered device,
  whatever kinds it chose, with the bookkeeping of any send (environment
  remembered, dead token deleted). Checks and tests share 20 requests per ten
  minutes per account.
- **Status** (`GET`): source (`database`, `environment`, none), ids,
  fingerprint, who uploaded and when, the checks, devices by environment and
  kind, and Apple's refusals of the last 24 hours by reason
  (`push:apns:errors:<UTC hour>` in Redis: a lost counter costs a number, not a
  notification).

### A device's self-test (`POST /v1/push/devices/test`)

The app's hidden "Push-Tests" (presentation mode) asks for a sample of one kind:
`{ installId, kind, locale? }` → 200 `{ state, status?, reason?, environment? }`,
`state` one of `sent`, `apns-error` (Apple's `status` and `reason` beside it),
`not-configured` (no APNs key) or `no-token`.

- **Only to itself.** The target is the token stored for `apns:<installId>`;
  there is no token or device parameter. The install id is the device's
  credential exactly as for every other device call (a random UUID only the app
  and this server know; in the body, not the path, so no access log keeps it).
  An unknown install answers `no-token`, the same as a row without a token.
- **Limits.** 20 an hour per install (`push-test-install`) and 60 an hour per
  address (`push-test`), both counted before the row is read
  (`PushFollowWriteRateLimitService`). `PUSH_SELF_TEST=false` answers 404: an
  abuse switch that needs no release. Default on, since the worst a caller can
  do is send itself notifications.
- **The real thing.** `kind` is one of `ride-wait`, `ride-reopen`, `next-up`,
  `plan-change`, `rain-soon`, `morning-plan`, `news`, `guides`
  (`PUSH_SELF_TEST_KINDS`); `selfTestMessage` (`src/push/push-self-test.ts`)
  writes it with the kind's own writer and `buildApnsRequest` delivers it, so
  pictogram, subtitle, thread, category, level, photo and expiry are the real
  ones. The data is a visit to Phantasialand (Taron with its photo, times from
  the park's clock now); news and guides are the newest post of the reader's
  export. Its text says nothing about a test.
- **Bypassed:** the device's choice of kinds, quiet hours, every cool-down and
  dedupe, and `TIME_SENSITIVE_CAP` (a test neither goes calm nor uses up the
  day's share). **Kept:** the circuit breaker, the environment retry and
  Apple's bookkeeping, so a dead token is deleted.
- **The marker.** `test: "true"` in the payload, `test=1` on the `url` (what
  the app's tap and "Umplanen" forward), the collapse id `<kind>:test`
  (a test replaces the last test, never a real banner), and every id an action
  writes with is `"test"` (`attractionId`, `entry`), so an app that ignored the
  marker would still change nothing real.

### `news` — a new post in the news export

`NewsPushService`, a fourth half of the five-minute job, for devices with
`news`. It reads `<NEWS_EXPORT_BASE>/news/<locale>.json` (the file the app
itself reads, so a post is announced when the app can open it) for each locale
that has readers. A slug first added to the Redis set `push:news:seen:<locale>`
(no TTL, so never evicted) is new; a missing set is filled silently, and a post
published more than 6 hours ago is never announced. `newsParks` narrows to posts
about those parks (geo paths, else park slugs). Quiet hours drop it.

```json
{
  "title": "📰 Europa-Park startet in den Winter",
  "subtitle": "Parky hat Neues für dich",
  "body": "<teaser>",
  "url": "/news/europa-park-wintersaison-2026",
  "tag": "news:europa-park-wintersaison-2026"
}
```

### `guides` — a new guide in the guides export

The same service and rules over `<NEWS_EXPORT_BASE>/guides/<locale>.json`
(`POST_FEEDS`), for devices with `guides`: its own Redis sets
`push:guides:seen:<locale>`, the same 6 hours on `publishedAt`, the same quiet
hours. A device gets guides only if `guides` is among its `kinds`; an app that
never sends the kind is unaffected.

- **Only the slug counts.** An edited guide (`updatedAt`) is not a new one, and
  a slug is never removed from the set, so a guide that leaves the export and
  returns stays quiet. The first run per locale fills the set with every guide
  there is and sends nothing.
- **`newsParks` does not narrow guides.** A guide is often about several parks
  or none; the news rule would keep those from a device narrowed to its
  favourites, and guides have their own switch.
- **A post and a guide in one run are two notifications**, with different
  tags, each to the devices of its kind; nothing is merged or capped across
  the two.

```json
{
  "title": "📖 Die Freizeitpark-Packliste",
  "subtitle": "Parky hat einen neuen Guide",
  "body": "<teaser>",
  "url": "/blog/freizeitpark-packliste",
  "tag": "guides:freizeitpark-packliste"
}
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

| rule  | what changes                                                                                                                                                                            | when read                        |
| ----- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| hours | earliest OPERATING opening / latest closing of the day moves, or the day turns `CLOSED` (or back)                                                                                       | every watched day                |
| shows | a planned performance is no longer in the day's `scheduled` times (nearest time named), or a scheduled show drops out of a still-scheduled day (cancelled)                              | today; a projection says nothing |
| rides | a planned, not yet ridden ride goes from running to not running or back (reopen-alert reading: any non-OPERATING standby row is down, only a fresh non-heartbeat OPERATING row is open) | today, while the park is open    |
| waits | the `/plan/day` forecast at the planned slot moves by ≥ 15 min **and** ≥ 30 %                                                                                                           | up to 7 days out                 |

Unknown on either side says nothing (a first sight is the baseline), except a
show first seen away from its planned time. A digest names at most three
changes and counts the rest: the first change is the title, whose plan and
which day the subtitle, the others the body, one per line. **Pacing**: the first digest of a day goes at once,
then at most one every 30 minutes, and one holding only wait changes at most
every 3 hours. A change not sent (pacing, quiet hours) stays pending against the
baseline, so a blip that reverts in between is never sent and a drift adds up.

**State and bounds.** The baseline lives in Redis
(`push:plan-change:<hash(tripId)>:<slug>:<date>`) until three days after the
date; an evicted one restarts in silence. At most 150 plan days per tick
(`MAX_PLAN_DAYS_PER_RUN`), 31 per trip, trips in random order, 60 s of wall
clock, each day isolated; nothing at all without an APNs key. A ride
first seen down is not remembered, so a ride opening in the morning is not
"running again"; `/plan/day` comes from its shared
quarter-hour cache and is only read for days with rides or shows within the
show/forecast reach.

```json
{
  "title": "🔄 Öffnungszeiten jetzt 09:00–17:00 Uhr (vorher 09:00–18:00)",
  "subtitle": "Dein Plan · Phantasialand, Sa., 17.10.",
  "body": "Taron ist gerade außer Betrieb",
  "url": "/trip-planner?park=europe%2Fgermany%2Fbruehl%2Fphantasialand&date=2026-10-17",
  "tag": "plan-change:phantasialand:2026-10-17",
  "kind": "plan-change"
}
```

### The device payload

`buildApnsRequest` (`src/push/apns.ts`) writes one request per message from the
table `APNS_KINDS`; `apns.spec.ts` pins every row. Limits are Apple's: the
payload at most 4 KB (4096 bytes; a longer one loses the end of its body, never
its title), the collapse id at most 64 bytes.

```json
{
  "aps": {
    "alert": {
      "title": "⏳ Taron: 15 Min.",
      "subtitle": "Phantasialand",
      "body": "Unter deiner Grenze von 20 Min."
    },
    "sound": "default",
    "thread-id": "phantasialand:2026-10-17",
    "interruption-level": "time-sensitive",
    "relevance-score": 0.9,
    "category": "RIDE_WAIT",
    "mutable-content": 1,
    "target-content-id": "/parks/europe/germany/bruehl/phantasialand/taron"
  },
  "attractionId": "3f2c…",
  "url": "/parks/europe/germany/bruehl/phantasialand/taron",
  "kind": "ride-wait",
  "image": "https://media.park.fan/taron/01/ab12cd34-1200.webp"
}
```

| kind           | level          | relevance | category       | priority | expires                             | thread          | photo     | custom keys                       |
| -------------- | -------------- | --------- | -------------- | -------- | ----------------------------------- | --------------- | --------- | --------------------------------- |
| `ride-wait`    | time-sensitive | 0.9       | `RIDE_WAIT`    | 10       | 10 min                              | `<park>:<date>` | the ride  | `attractionId`                    |
| `ride-reopen`  | time-sensitive | 0.8       | `RIDE_REOPEN`  | 10       | 20 min                              | `<park>:<date>` | the ride  | `attractionId`                    |
| `next-up`      | time-sensitive | 1.0       | `NEXT_UP`      | 10       | the block's start                   | `<park>:<date>` | the ride  | `entry`, `date`, `park`?, `ride`? |
| `show`         | time-sensitive | 0.8       | `SHOW`         | 10       | the performance's start             | `<park>:<date>` | none      |                                   |
| `rain-soon`    | time-sensitive | 0.7       | `RAIN_SOON`    | 10       | the rain's start                    | `<park>:<date>` | none      |                                   |
| `plan-change`  | active         | 0.6       | `PLAN_CHANGE`  | 10       | 1 h for today, 24 h for a later day | `<park>:<date>` | none      | `park`, `date`                    |
| `morning-plan` | active         | 0.5       | `MORNING_PLAN` | 10       | the park's opening                  | `<park>:<date>` | the park  |                                   |
| `news`         | passive        | 0.2       | `NEWS`         | 5        | 12 h                                | `news`          | the cover |                                   |
| `guides`       | passive        | 0.1       | `GUIDE`        | 5        | 12 h                                | `guides`        | the cover |                                   |

- **Level.** Time-sensitive breaks through a Focus and the scheduled summary, so
  only what the visitor asked for by name and what happens in the park within the
  hour gets it (Apple: "an event that's happening now or will happen within an
  hour", never marketing). A plan change can be about a day weeks away: `active`.
  News and guides never light the screen: `passive`, no `sound`, priority 5. The
  app needs the entitlement `com.apple.developer.usernotifications.time-sensitive`;
  a build without it shows these as ordinary notifications.
- **A device's share.** At most 8 time-sensitive notifications per device in 12
  hours (`TIME_SENSITIVE_CAP`, counted in the process's memory); the next ones are
  sent as `active`, never dropped.
- **Expiry** (`apns-expiration`) is when the message stops being true, at least a
  minute ahead so Apple stores it for a phone in a tunnel. A producer sets it with
  `PushMessage.expiresAtMs`; the column is the kind's own otherwise.
- **Thread** (`thread-id`): one stack per park and park day (`<parkSlug>:<yyyy-MM-dd>`,
  the park's date), so a visit's alerts, plan and weather sit together; news and
  guides stack by kind. `summary-arg` is not sent: Apple deprecated it with iOS 15.
- **Collapse id** is the tag: a wait alert replaces the same alert's older banner,
  a plan-change digest the same day's, a rain warning the same park day's.
- **Photo.** A producer names a ride's or park's geo path or a cover's image id
  (`PushMessage.photo`); `src/push/push-media.ts` resolves it against the media
  bucket's `index.json` (kept 30 minutes, a failed read retried after 5) to the
  first width from 1000 px up. A photo whose credit has to be shown
  (`attribution: true`) is not sent. With a photo the payload carries `image` and
  `mutable-content: 1`, and the app's Notification Service Extension attaches it;
  the files are WebP, which Apple does not list for attachments (JPEG, GIF, PNG,
  10 MB), so the extension converts. `PUSH_IMAGES=false` sends none.
- **Categories** name the action buttons the app registers; a build that registers
  none shows none. The custom keys are what the actions need: `attractionId` for
  "Alarm aus" (`DELETE /v1/push/ride-alerts`), `entry`/`park`/`ride`/`date` for
  the plan's actions.
- **An older app** ignores every key it does not know and keeps routing by `url`.

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
- Configuration: `VAPID_*`, `PUSH_ENDPOINT_HOSTS` and `NEWS_EXPORT_BASE` in
  `.env.example`; the APNs key is uploaded in the admin (`APNS_*` as a
  fallback).
