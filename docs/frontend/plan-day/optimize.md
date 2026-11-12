# `/plan/optimize`: the day optimizer

The single-day trip planner's optimizer runs here: park.fan's planner and Parky send the day as the
planner holds it and get the order with the least queueing back. It used to run in the browser
(`lib/planner/optimize.ts` and `fit.ts` in park.fan); the engine moved over unchanged, so both
callers get one answer and no forecast leaves the API to be searched over.

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

| Field                | Notes                                                                                                                               |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `date`               | park-local `YYYY-MM-DD`; an impossible date is a **400**                                                                            |
| `mode`               | `day`, `headliners` or `fit` (below)                                                                                                |
| `entries[]`          | the day as it stands, at most 80, ids unique: `id`, `attractionSlug?`, `attractionName?`, `custom?`, `showSlug?`, `startMinute`, `done?`, `actualWait?` |
| `prefs`              | `riderHeightCm?`, `avoidWet?` (read by `headliners`), `earlyEntry?` (the visitor holds early entry that day)                         |
| `nowMinute`          | park-local minute the caller reads as now; used only when `date` is the park's today                                               |
| `add[]`              | `fit` only: slugs of rides being added, unique, at most 40                                                                          |
| `choice`             | `fit` only: `dropped[]`, `droppedBlocks[]`, `shortBlocks[]`, `priority[]` (wish keys and block ids); absent is everything ticked     |
| `levers`             | `fit` only: also measure what taking out or shortening each free block buys                                                          |
| `preview`            | `day`/`headliners` only: skip the fit question, for a button that shows what a press would gain                                     |

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

| Field               | Notes                                                                                                         |
| ------------------- | ------------------------------------------------------------------------------------------------------------- |
| `status`            | `optimized`, `unchanged` (the day is already the plan), `needs-fit`, `fit` or `refused`                       |
| `refusal`           | with `refused`: `past-day`, `no-plan`, `no-hours`, `no-wait-times`, `no-rides`, `unknown-ride`; else `null`   |
| `clock`             | `{ phase }`, plus `nowMinute` on today: what the day was planned against                                      |
| `maxStops`          | 24: rides one search holds; more are reported in `plan.capped`, never silently dropped                        |
| `add`               | slugs offered on top of the entries                                                                           |
| `skippedHeadliners` | `headliners`: headliners this party cannot ride                                                               |
| `movable`           | entries the optimizer may move                                                                                |
| `before`, `after`   | `{ totalWaitMinutes, idleMinutes, endMinute, overflow, clashes }` for the day as sent and as planned          |
| `plan`              | `optimized`: `stops[]` in plan order plus the totals and `capped`                                             |
| `entries`           | the day as the answer leaves it, in start order; an added ride has `id: null` and `added: true`               |
| `fit`               | `needs-fit` and `fit`: the assistant's answer (below)                                                         |

A stop is `{ entryId, attractionSlug, attractionName, startMinute, waitMinutes, fits }`:
`entryId: null` is a ride being added, `fits` says whether its queue is joined before closing. A
ride the optimizer adds is never filed past closing; an entry the visitor already had keeps its
overflow block rather than being deleted. park.fan writes `stops` with its own `applyPlan`, which
mints the ids for added rides; a caller without such a store can write `entries`.

`before` and `after` cover the same rides, so their difference is what a press saves; `clashes`
counts the places where the next thing starts before the one before it is over and walked from.
The waits are raw minutes, not rounded to five.

### `fit`

| Field                  | Notes                                                                                                   |
| ---------------------- | ------------------------------------------------------------------------------------------------------- |
| `wishes[]`             | `{ key, entryId, attractionSlug, attractionName, headliner, replannable }`; key `e:<entry id>` or `a:<slug>` |
| `blocks[]`             | the free blocks a lever may act on: `{ entryId, label, icon, durationMinutes }`                         |
| `order[]`              | wish keys in the order the day is decided in: pinned first, then as offered, a second lap last         |
| `wanted`               | wishes still ticked                                                                                     |
| `fitted[]`, `missed[]` | wish keys with and without a slot before closing                                                        |
| `endMinute`, `totalWaitMinutes`, `idleMinutes` | the plan for this choice                                                        |
| `entries[]`, `stops[]` | the entries the day keeps under this choice, and the plan to lay over them                              |
| `levers[]`, `applied[]`| with `levers` (always on `needs-fit`): what each block would buy, best first, and the levers already pulled as `kind:entryId`; `null` otherwise |

A lever is `{ kind, entryId?, label?, icon?, minutes?, fits, fitsNow, wanted, solves }` with `kind`
one of `shorten-block` (to 30 minutes), `drop-block`, `drop-all-blocks`. A lever that buys nothing
is not offered. This is the one answer that may remove an entry: a wish switched off in `choice`
is not in `fit.entries`, and only a list that names it put it there.

## Limits and failure

The search is synchronous and takes about a tenth of a second on a full day, a fit answer with
levers several of those, so it runs in a small pool of worker threads (`PLAN_OPTIMIZE_WORKERS`,
by default two, or one on a two-core machine) with a queue of 64 and 20 seconds per job. Under
ts-jest and ts-node, where there is no compiled worker, it runs inline. A full queue or a job past its time answers
**503**; a caller offers a retry.

## Parity with the browser

`src/planner/day-optimizer/optimize.vectors.spec.ts` and `fit.vectors.spec.ts` are park.fan's
`test-planner-optimize.mjs` and `test-planner-fit.mjs`, vectors unchanged.
`browser-parity.spec.ts` replays 66 recorded cases (three real `/plan/day` days, eighteen seeded
synthetic ones) through the port and the request layer and expects the browser engine's orders,
start minutes, scores and levers exactly; `__fixtures__/record-browser-parity.mjs` beside the
fixture says how it was recorded.

## Related

- [`/plan/day`](./README.md): the day the optimizer plans against
- [API keys](../api-keys.md)
- park.fan's
  [optimizer page](https://github.com/PArns/park.fan/blob/main/docs/features/trip-planner/optimizer.md)
  and its rules on a day that does not fit and on closing time
