# `/plan/day`: a day that runs past midnight

How the response represents an operating day that crosses midnight
(`closeHour < openHour`), and the four places on the server that had to agree
about it. Open it before touching anything that walks a day's hours. Part of the
[`/plan/day` contract](./README.md).

## The contract

A park whose day crosses midnight publishes `closeHour < openHour`: La Ronde,
Six Flags Mexico and Six Flags Qiddiya City every day (`10 → 0`, `10 → 0`,
`16 → 0`), many parks on Halloween (Six Flags Magic Mountain `10 → 1`).
`context.openHour` / `closeHour` keep the operator's wall-clock pair, because that
pair is what renders as "16:00 – 01:00".

**`hours[].hour` does not wrap.** It continues past 23, so one operating day is
always one ascending series and `hours[0]` is its first hour; 24 is the midnight
after `date`, 25 is 01:00. `slots[].minute` unfolds the same way (1440 = that
midnight).

```jsonc
"context": { "openHour": 16, "closeHour": 1 },
"hours": [
  { "hour": 23, "wait": 35 },
  { "hour": 24, "wait": 30 },   // 00:00, the night after `date`
  { "hour": 25, "wait": 20 }    // 01:00, when the park closes
]
```

A caller that walks the window itself must unfold it the same way —
`closeHour < openHour ? closeHour + 24 : closeHour`, inclusive — and must never
test `hour > closeHour`, which for `16 → 1` is true of **every** hour. The rule
lives in one function on each side: `unfoldedCloseHour`
(`src/common/utils/day-shape.util.ts`) here, and its twin in park.fan's
`lib/planner/day-grid.ts`, whose frontend traps are in park.fan's
`docs/api/parks-open-past-midnight.md`.

Shows on such a day: a past-midnight performance belongs to the day whose
published window still covers it — see [shows](./shows.md#past-midnight-performances).

## Four places have to agree

A wrap day once answered `rides: []` (a `closeHour < openHour` guard). Every loop
that walks the day's hours has to unfold it, and two stores key the night by the
next date:

- **Hourly predictions** are timestamps, so 00:30 of a `16 → 1` day carries
  *tomorrow's* park-local date. Matching on the day's own date dropped exactly the
  hours that make the day unusual.
- **`attraction_hourly_history`** is keyed by park-local date, so an observed night
  is stored across two rows and is read back from both — otherwise the curve ends
  at 23:00 and `dayPeak` belongs to whichever half was busier. (The night's small
  hours sit in the next date's row, written the following night at 04:30.)
- **The historical shape** buckets by wall-clock hour, so its `0` bucket is moved
  to 24 before it is read against the day; left alone, midnight sorted in front of
  the morning.
- **The observed-hours fallback** takes the window as the widest silence on the
  24-hour clock, not min-to-max: a park measured at `[0, 16 … 23]` would otherwise
  read `0 → 23` and draw a queue at 03:00.
