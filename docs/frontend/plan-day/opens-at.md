# `/plan/day`: a ride that opens later than its park

`opensAt`, `opensAtConfidence` and `runsOnWeekdays` on each ride: when the ride
(not the gate) opens, how well that is known, and on which weekdays it runs at all.
Part of the [`/plan/day` contract](./README.md).

## `opensAt`

`context.openHour` is the gate, not when the rides start. Phantasialand opens at
09:00 and sixteen rides (Taron, F.L.Y., Winja's, Talocan, River Quest …) run from
10:00. Each ride carries `opensAt` (park-local `HH:mm`) and **its `hours` and
`slots` already begin there**, so a caller clamps nothing; the field is there to be
shown to somebody arriving at rope drop.

Absent means "opens with the park" or "we cannot tell" (fewer than five observed
openings, or a feed that never reports the transition). Both render the same way.
There is **no `closesAt`** and there will not be one: feeds leave rides OPERATING
for hours after a park shuts.

Source: `AnalyticsService.getRideOpeningTimes` (cached per park,
`park:ride-openings:v3:<parkId>`), type `src/common/types/ride-opening.type.ts`.

### The gap is seasonal, so the lookup is too

|         | park opens | Taron | F.L.Y. | Black Mamba |
| ------- | ---------- | ----- | ------ | ----------- |
| Apr–Aug | 09:00      | 10:10 | 10:10  | 08:10       |
| January | 11:00      | 11:00 | 11:00  | 11:05       |

In winter everything opens with the park, so neither the absolute time nor the
offset survives the season. Openings are stored **keyed by the park's own opening
time that day**; a park opening never watched yields no answer, never the nearest.

Detection, each rule a wrong answer first:

- **The transition, not the first `OPERATING` row.** `queue_data` is written on
  change plus an hourly heartbeat, so a ride left open overnight would report its
  06:15 heartbeat as an opening; a day counts only when the ride was seen CLOSED
  earlier that day.
- **The park's hour is the floor.** Half the rides report OPERATING before the
  gates (Black Mamba 08:10 against 09:00) — the feed carries the operator's system
  state.
- **Floored to the quarter hour.** A five-minute poller plus feed lag puts a 10:00
  ride at 10:10.

## `opensAtConfidence`

`high` from 40 observed days, `medium` from 20, `low` below
(`RIDE_OPENING_CONFIDENCE_THRESHOLDS`, the same boundaries as
`src/analytics/utils/rope-drop.util.ts`, which counts the same thing). Present
exactly when `opensAt` is. **Not a confidence interval; no minutes can be derived
from it.** `low` is a real answer on thin evidence (the floor for any answer is
five openings).

It exists because one ride's two keys are not equally known — a park opens at its
off-season hour on far fewer mornings (Black Mamba: 176 days at the 09:00 key,
23 at 11:00). Measured 2026-09-23 over 2,757 served times: 58.4 % `high`,
22.5 % `medium`, 19.0 % `low`. The park-level 90/30 convention in
`analytics.service.ts` would pin every winter opening at `low` forever.

## `runsOnWeekdays`

The opening time says when a ride starts on the days it runs, not that it runs.
Movie Park's Halloween mazes run Friday–Sunday evenings and their 17:00 was served
on a Monday. `runsOnWeekdays` lists the ISO weekdays (Monday 1 … Sunday 7) the ride
usually runs on, park timezone, from the last eight weeks of observed `OPERATING`
readings (no heartbeat, no reconciliation row).

- **The denominator is the park's open days, not the calendar.** A weekday counts
  when the ride ran on at least half (`RUN_WEEKDAYS_MIN_SHARE`) of the days the
  park was open on that weekday, counted from the ride's first operating day in the
  window. "Open" means any ride of the park observed `OPERATING` that day, because
  Movie Park's calendar does not mark its Halloween evenings. Half and not more: a
  weekday kept wrongly shows `opensAt` as before, one dropped wrongly tells a
  visitor the ride is not on the programme.
- **One open day is an anecdote.** A weekday the park was open on fewer than twice
  counts only for a ride that ran on ≥ 90 % of all the park's open days since its
  first run (`RUN_WEEKDAYS_EVERY_OPEN_DAY_SHARE`). A weekday the park was never open
  on is left out, except today's once any ride has run today and the ride runs
  whenever the park does.
- On a planned day whose weekday is not listed, `opensAt` and `opensAtConfidence`
  are absent and the curve draws the park's day.
- The field is absent when there is no pattern: under two weeks of history, no
  operating day in the last seven, or no weekday clearing its share. **Absent never
  means "runs every day".** On `/plan/day` the full list is served (all seven when
  it runs daily); the park payload carries it only when a weekday is left out
  (`partialRunWeekdays`).

Measured 2026-10-19 at Movie Park: the six mazes read `[5, 6, 7]`, Star Trek and Van
Helsing's Factory all seven days. The pure rule is `runWeekdays`
(`src/common/utils/run-weekdays.util.ts`); readings come from
`AnalyticsService.getRideRunWeekdays` (cached six hours).
