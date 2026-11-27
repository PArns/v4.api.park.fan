# `/plan/optimize`: the day optimizer

The single-day trip planner's optimizer runs here: park.fan's planner and Parky send the day as the
planner holds it and get the order with the least queueing back. It used to run in the browser
(`lib/planner/optimize.ts` and `fit.ts` in park.fan) and moved over unchanged, so both callers get
one answer and no forecast leaves the API to be searched over. Since then it has changed here only
(see [the schedule](#the-schedule)).

```
POST /v1/parks/:continent/:country/:city/:parkSlug/plan/optimize
```

- Behind a data key (`@RequiresApiKey()`, see [API keys](../api-keys.md)); park.fan's own frontend
  relays it with its server key.
- Answered `Cache-Control: private, no-store` (the route says `no-store`, the key guard makes every
  keyed answer private): one visitor's plan, never a shared copy.
- The server plans against its own `/plan/day` for `date` (the same Redis entry the
  [`/plan/day`](./README.md) route serves, kept 15 minutes) plus the rides a curated works window
  shuts that day. The client never uploads forecast data.
- Code: `src/planner/` (controller, service, DTOs); the engine is plain TypeScript in
  `src/planner/day-optimizer/`, run in worker threads.

## The request

| Field       | Notes                                                                                                                                                   |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `date`      | park-local `YYYY-MM-DD`; an impossible date is a **400**                                                                                                |
| `mode`      | `day`, `headliners` or `fit` (below)                                                                                                                    |
| `entries[]` | the day as it stands, at most 80, ids unique: `id`, `attractionSlug?`, `attractionName?`, `custom?`, `showSlug?`, `startMinute`, `done?`, `actualWait?` |
| `prefs`     | `riderHeightCm?`, `avoidWet?` (read by `headliners`), `earlyEntry?` (the visitor holds early entry that day)                                            |
| `nowMinute` | park-local minute the caller reads as now; used only when `date` is the park's today                                                                    |
| `add[]`     | `fit` only: slugs of rides being added, unique, at most 40                                                                                              |
| `choice`    | `fit` only: `dropped[]`, `droppedBlocks[]`, `shortBlocks[]`, `priority[]` (wish keys and block ids); absent is everything ticked                        |
| `levers`    | `fit` only: also measure what taking out or shortening each free block buys                                                                             |
| `preview`   | `day`/`headliners` only: skip the fit question, for a button that shows what a press would gain                                                         |
| `from`      | `{ attractionSlug }`: the ride the visitor stands at now; read on the park's today only ([below](#from-where-the-visitor-stands))                       |

An entry is park.fan's `PlannerEntry`: `startMinute` is park-local minutes since midnight and runs
past 1440 on a day past midnight (at most 2880). A free block carries
`custom: { label, icon, durationMinutes }`; a show carries `custom` and `showSlug`. The browser's
deprecated `hour` mirror is accepted and ignored. Any other field is a **400**, as are `add`,
`choice` or `levers` on `day`/`headliners`, and `preview` on `fit`. Unknown park: **404**.

### The clock

Past, today or future is the server's call, from the park's timezone. On today's date the plan
starts at `nowMinute` (snapped up to the quarter hour), or at the server's park-local minute when
none is sent; an entry whose start the clock has passed is fixed. A past date is refused before the
day is loaded.

### From where the visitor stands

Without `from` the optimizer knows no place: the first stop of the rest of the day costs no walk
and may start at the next quarter hour, wherever the visitor is. `from: { attractionSlug }` names
the ride they stand at ("Umplanen ab hier" in the app), and on the park's today:

- the visitor leaves that ride at the clock's minute (`nowMinute` as sent, unsnapped), and the
  first stop starts no earlier than that plus the walk to it, on foot at park pace with the detour
  factor, nothing added for leaving a station;
- that first walk is priced like every other (`0.2 × walk`), so the order begins near the visitor
  unless a queue elsewhere is worth the way; `before` and `after` both count it;
- a day whose first entry cannot be reached in time (the fast-walker floor) is not held as the
  incumbent, so the press moves it instead of answering `unchanged`;
- everything else is as without it: ticked-off entries, free blocks, shows, entries the clock has
  passed and rides in works stay where they are;
- the answer carries a [`receipt`](#receipt).

The slug is checked against the park's catalogue (retired rides excluded), not against
`/plan/day`: the ride a visitor stands at need not have a curve that day. A slug that is not a
ride of this park is a **400** (`from.attractionSlug: "…" is not a ride of this park`), and so is
any other field inside `from`: it takes a slug and never a position. A ride without coordinates is
walked from at the assumed three minutes. On a future date `from` is checked and then not read,
and the answer has no `receipt`.

Nothing of `from` is kept: the route stores no plan, no log line names the slug, and no cache key
holds it. The device works out the nearest ride from its own position; the server learns the ride.

What a re-plan on the day does not know: the forecast is `/plan/day`'s (the hourly model, cached
15 minutes), not the live queue; a ride that is down right now is planned like any other unless a
curated works window shuts it; and rain moves nothing (`avoidWet` only filters the headliners
`headliners` adds).

## The schedule

What a plan is made of, and what the search weighs:

- **Starts sit on a five-minute step** (`START_STEP_MIN`): a stop starts at the first step after
  the one before it is over and walked from. The floors stay on the quarter hour (`SNAP_MIN_FINE`):
  today's plan starts at the next quarter hour, and a ride's soft floor is snapped up to one.
- **A stop occupies its expected queue**, the band excluded, with no minimum; the ride itself and
  leaving the station are in the transfer (`EXIT_MIN` + `RIDE_DURATION_MIN`, then the walk).
- **The cost is `wait + 0.5 × idle + 0.2 × walk`** (`IDLE_WEIGHT`, `WALK_WEIGHT`), after the
  overflow tiers and before the clock. The walk is the minutes on foot between two stops that
  happen, at park pace with the detour factor. Walking is priced so a day stops crossing the park
  for a queue a minute shorter, and low enough that a queue five minutes shorter still beats a
  kilometre more of walking.

Until PAR-1364 every start was rounded up to the quarter hour, a queue occupied at least fifteen
minutes and walking cost nothing. On 23 park-days (six parks, four dates, every headliner) that
left 23.7 minutes between the end of one queue and the start of the next; it is 15.7 now, with 23 %
less walking and the last queue left about an hour earlier.

## The modes

- **`day`** re-orders the rides the day holds. Ticked-off rides, free blocks, shows, entries under
  way and rides in works stay where they are; only the rest move.
- **`headliners`** adds the park's curated headliners the day does not have and this party can
  ride (`riderHeightCm` against `minimumHeight`, `avoidWet` against `mayGetWet`), then plans
  everything. `add` names them, `skippedHeadliners` counts the ones the party ruled out.
- **`fit`** answers the fit assistant: the day with `add` on top and the visitor's `choice`.

`day` and `headliners` first ask what the planner asked before a press: does the day hold
everything? Where something has no slot before closing the answer is `needs-fit`, and `fit` holds
the assistant's opening answer, levers included. With `preview: true` that question is skipped.

## The answer

As on every public route, a field whose value is `null` is left out of the JSON
(`ExcludeNullInterceptor`): read an absent `refusal`, `before`, `plan`, `fit`, a stop's `entryId` or
`waitMinutes` as `null`. A stop without `entryId` is a ride being added.

| Field               | Notes                                                                                                             |
| ------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `status`            | `optimized`, `unchanged` (the day is already the plan), `needs-fit`, `fit` or `refused`                           |
| `refusal`           | with `refused`: `past-day`, `no-plan`, `no-hours`, `no-wait-times`, `no-rides`, `unknown-ride`; else `null`       |
| `clock`             | `{ phase }`, plus `nowMinute` on today: what the day was planned against                                          |
| `maxStops`          | 24: rides one search holds; more are reported in `plan.capped`, never silently dropped                            |
| `add`               | slugs offered on top of the entries                                                                               |
| `skippedHeadliners` | `headliners`: headliners this party cannot ride                                                                   |
| `movable`           | entries the optimizer may move                                                                                    |
| `before`, `after`   | `{ totalWaitMinutes, idleMinutes, walkMinutes, endMinute, overflow, clashes }` for the day as sent and as planned |
| `plan`              | `optimized`: `stops[]` in plan order plus the totals (`walkMinutes` among them) and `capped`                      |
| `entries`           | the day as the answer leaves it, in start order; an added ride has `id: null` and `added: true`                   |
| `fit`               | `needs-fit` and `fit`: the assistant's answer (below)                                                             |
| `receipt`           | only with `from` on the park's today: what the plan changes ([below](#receipt)); the key is absent otherwise      |

A stop is `{ entryId, attractionSlug, attractionName, startMinute, waitMinutes, fits }`:
`entryId: null` is a ride being added, `fits` says whether its queue is joined before closing. A
ride the optimizer adds is never filed past closing; an entry the visitor already had keeps its
overflow block rather than being deleted. park.fan writes `stops` with its own `applyPlan`, which
mints the ids for added rides; a caller without such a store can write `entries`.

`before` and `after` cover the same rides, so their difference is what a press saves; `clashes`
counts the places where the next thing starts before the one before it is over and walked from.
The waits are raw minutes, not rounded to five. A press can save walking and no queueing: the
search weighs both, so `after.totalWaitMinutes` may sit a few minutes above `before`'s when
`after.walkMinutes` is well below it.

### `receipt`

Present on every answer but `refused` when the day was planned from `from`; what a client shows
before it applies the plan.

| Field           | Notes                                                                                                                                             |
| --------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `from`          | `{ attractionSlug, attractionName }`: the start as the server read it                                                                             |
| `nowMinute`     | the minute the visitor leaves it                                                                                                                  |
| `first`         | `{ entryId?, attractionSlug, attractionName, walkMinutes, startMinute }`: the first ride of the plan and the walk to it; absent with no ride left |
| `moved[]`       | `{ entryId, attractionSlug, attractionName, fromMinute, toMinute }` for each entry whose start changes, in plan order                             |
| `pastClosing[]` | the same shape for each entry whose queue is no longer joined before closing: kept at `toMinute`, flagged, never dropped                          |

On `unchanged` `moved` is empty and `first` is the day's own next ride. On `needs-fit` and `fit`
the receipt describes `fit.stops`. A re-plan that should always answer with a plan, overflow
flagged, sends `preview: true`; without it a day that no longer fits answers `needs-fit`.

```jsonc
// POST /v1/parks/europe/germany/bruehl/phantasialand/plan/optimize   (14:03 in the park)
{
  "date": "2026-11-25",
  "mode": "day",
  "preview": true,
  "nowMinute": 843,
  "from": { "attractionSlug": "chiapas" },
  "entries": [
    {
      "id": "r1",
      "attractionSlug": "maus-au-chocolat",
      "startMinute": 600,
      "done": true,
      "actualWait": 20,
    },
    { "id": "r2", "attractionSlug": "black-mamba", "startMinute": 855 },
    {
      "id": "lunch",
      "custom": { "label": "Mittag", "icon": "food", "durationMinutes": 45 },
      "startMinute": 930,
    },
    { "id": "r3", "attractionSlug": "taron", "startMinute": 990 },
    { "id": "r4", "attractionSlug": "chiapas", "startMinute": 1050 },
  ],
}
```

```jsonc
{
  "date": "2026-11-25",
  "mode": "day",
  "status": "optimized",
  "clock": { "phase": "today", "nowMinute": 843 },
  "movable": 3,
  "before": {
    "totalWaitMinutes": 100,
    "idleMinutes": 90,
    "walkMinutes": 19,
    "endMinute": 1080,
    "overflow": 0,
    "clashes": 0,
  },
  "after": {
    "totalWaitMinutes": 100,
    "idleMinutes": 60,
    "walkMinutes": 19,
    "endMinute": 1055,
    "overflow": 0,
    "clashes": 0,
  },
  "plan": {
    "stops": [
      {
        "entryId": "r3",
        "attractionSlug": "taron",
        "attractionName": "Taron",
        "startMinute": 855,
        "waitMinutes": 45,
        "fits": true,
      },
      {
        "entryId": "r2",
        "attractionSlug": "black-mamba",
        "attractionName": "Black Mamba",
        "startMinute": 975,
        "waitMinutes": 30,
        "fits": true,
      },
      {
        "entryId": "r4",
        "attractionSlug": "chiapas",
        "attractionName": "Chiapas",
        "startMinute": 1025,
        "waitMinutes": 25,
        "fits": true,
      },
    ],
    // … the totals, as in `after`
  },
  // "entries": the whole day in start order, r1 and lunch where they were
  "receipt": {
    "from": { "attractionSlug": "chiapas", "attractionName": "Chiapas" },
    "nowMinute": 843,
    "first": {
      "entryId": "r3",
      "attractionSlug": "taron",
      "attractionName": "Taron",
      "walkMinutes": 5,
      "startMinute": 855,
    },
    "moved": [
      {
        "entryId": "r3",
        "attractionSlug": "taron",
        "attractionName": "Taron",
        "fromMinute": 990,
        "toMinute": 855,
      },
      {
        "entryId": "r2",
        "attractionSlug": "black-mamba",
        "attractionName": "Black Mamba",
        "fromMinute": 855,
        "toMinute": 975,
      },
      {
        "entryId": "r4",
        "attractionSlug": "chiapas",
        "attractionName": "Chiapas",
        "fromMinute": 1050,
        "toMinute": 1025,
      },
    ],
    "pastClosing": [],
  },
}
```

### `fit`

| Field                                          | Notes                                                                                                                                           |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `wishes[]`                                     | `{ key, entryId, attractionSlug, attractionName, headliner, replannable }`; key `e:<entry id>` or `a:<slug>`                                    |
| `blocks[]`                                     | the free blocks a lever may act on: `{ entryId, label, icon, durationMinutes }`                                                                 |
| `order[]`                                      | wish keys in the order the day is decided in: pinned first, then as offered, a second lap last                                                  |
| `wanted`                                       | wishes still ticked                                                                                                                             |
| `fitted[]`, `missed[]`                         | wish keys with and without a slot before closing                                                                                                |
| `pinnedMissed[]`                               | of `missed`, the pinned wishes: no slot even with every unpinned ride given up first; empty without a pin, absent from servers before PAR-1452  |
| `endMinute`, `totalWaitMinutes`, `idleMinutes` | the plan for this choice                                                                                                                        |
| `entries[]`, `stops[]`                         | the entries the day keeps under this choice, and the plan to lay over them                                                                      |
| `levers[]`, `applied[]`                        | with `levers` (always on `needs-fit`): what each block would buy, best first, and the levers already pulled as `kind:entryId`; `null` otherwise |

A lever is `{ kind, entryId?, label?, icon?, minutes?, fits, fitsNow, wanted, solves }` with `kind`
one of `shorten-block` (to 30 minutes), `drop-block`, `drop-all-blocks`. A lever that buys nothing
is not offered. This is the one answer that may remove an entry: a wish switched off in `choice`
is not in `fit.entries`, and only a list that names it put it there.

Two promises the answer keeps (PAR-1452, `fit.monotone.spec.ts`):

- **A pin is kept.** A wish key in `choice.priority` is pinned, and a pinned ride is given up only
  where no number of unpinned rides being left out makes room for it: its own overflow tier, above
  the headliners. Before, the order only decided _which_ ride fell out once the count was settled,
  so a pinned Taron went whenever losing it let one more small ride in. A pin the day has no minute
  for at all is in `missed` and named again in `pinnedMissed`.
- **Unticking never makes the rest fit worse** than the whole list's plan with those rides taken
  out: no fewer rides fitted, and no worse under the engine's order (tiers, then the cost). The
  search is a beam and not monotone in its input, so a smaller list could come out worse; with
  anything unticked the engine also plans the whole list (same blocks, same pins) and starts from
  that order too. It costs one more search per answer with an untick, levers included (a
  fourteen-ride day with levers: about 150 ms to about 350 ms). The promise is against the whole
  list, the one the assistant opens on, not against every larger choice in between.

## Limits and failure

The search is synchronous and takes about a tenth of a second on a full day, a fit answer with
levers several of those, so it runs in a small pool of worker threads (`PLAN_OPTIMIZE_WORKERS`,
by default two, or one on a two-core machine) with a queue of 64 and 20 seconds per job. Under
ts-jest and ts-node, where there is no compiled worker, it runs inline. A full queue or a job past its time answers
**503**; a caller offers a retry.

## Checks

`src/planner/day-optimizer/optimize.vectors.spec.ts` and `fit.vectors.spec.ts` are park.fan's
`test-planner-optimize.mjs` and `test-planner-fit.mjs`. PAR-1364 resized the fixtures whose
arithmetic assumed the quarter-hour step, so each still pins the one rule it was written for; the
rules themselves are unchanged. `browser-parity.spec.ts` replays 66 recorded cases (three real
`/plan/day` days, eighteen seeded synthetic ones) through the engine and the request layer and
expects the recorded orders, start minutes, scores and levers exactly. The inputs and the first
answers came from the browser engine (`__fixtures__/record-browser-parity.mjs` says how); a change
meant to move answers re-records them with `RECORD_PARITY=1 pnpm test -- browser-parity`, as
PAR-1364 did, and the fixture's diff is the change.

**Every order of eight rides, on every change to the optimizer.** `optimize.vectors.spec.ts` §16
enumerates all 40,320 orders of eight rides through the optimizer's own scorer and asserts the
heuristic found one of the optima: the check the claim "it finds the best order" rests on. It takes
about a minute, 18 % of the whole unit suite on its own, so `pnpm test` skips it (the run lists it
as skipped) and `pnpm test:planner-exhaustive` runs it, with the rest of
`src/planner/day-optimizer/`. Run that before you push any change there. CI does it for you: a
pull request or push whose diff touches `src/planner/day-optimizer/` runs the step "Day optimizer,
every order of eight rides" after the unit tests, and skips it, saying so, otherwise. A change
outside the folder that moves what the optimizer sees (the `/plan/day` payload it plans on, a
scorer weight imported from elsewhere) does not trigger it; run the script by hand then.
`BENCH=1 pnpm test:planner-exhaustive` adds the timing table.

`fit.monotone.spec.ts` holds the two fit promises above on a real Phantasialand day; its sweep over
every one or two rides unticked at five clocks runs with `PLANNER_EXHAUSTIVE=1`.

`start.spec.ts` and `plan-optimize.from.spec.ts` hold `from`: the order follows the walk, done and
started entries stay, the first start is at or after now plus the walk, the answer without `from`
is the same string as before, and the slug reaches no log line or cache key.

## Related

- [`/plan/day`](./README.md): the day the optimizer plans against
- [API keys](../api-keys.md)
- park.fan's
  [optimizer page](https://github.com/PArns/park.fan/blob/main/docs/features/trip-planner/optimizer.md)
  and its rules on a day that does not fit and on closing time
