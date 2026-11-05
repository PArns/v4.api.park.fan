# `/plan/day`: shows

The day's programme in `shows[]`: where each time comes from (`scheduled` vs
`projected`), how far ahead either reaches, past-midnight performances, coverage,
and show positions. Part of the [`/plan/day` contract](./README.md). Code:
`PlanDayService.buildShows`, `ShowsService` (`src/shows/shows.service.ts`).

```jsonc
{ "showSlug": "big-moments", "showName": "Big Moments", "latitude": 48.266, "longitude": 7.722,
  "times": ["12:30", "14:30", "17:45"], "source": "projected", "observedOn": "2026-08-29", "sampleDays": 7 }
```

| source      | what it is                                                                                                       |
| ----------- | ---------------------------------------------------------------------------------------------------------------- |
| `scheduled` | the operator's own times for that day. Today and past days, never a future one                                   |
| `projected` | the times the show ran at on the most recent day with the **same weekday**, with `observedOn` and `sampleDays`   |

Render `projected` differently from `scheduled`: it is what the show did, not a
promise. Entries are sorted by first time, on the unfolded day.

## No feed publishes showtimes ahead of the current day

Measured over all retained `show_live_data` (2025-12-24 → 2026-09-08, 15.2 M
showtime entries): 99.21 % fall on the snapshot's own operating day, 0.79 % on a
day already past (feed litter as old as 2022, which is why both SQL paths filter on
the showtime and not only on the snapshot), and 152 on a later date — 109 of those
past-midnight performances of the publishing day, 43 genuine. So for a future date
the choice is to project or to say nothing.

**Why the weekday.** Europa-Park's "Big Moments" runs twice on a Thursday and three
times on a Saturday; a weekday-blind projection drops the extra performance or
promises it on a Tuesday. **Why the most recent matching day**, not an average: a
union over weeks merges a summer and an autumn programme into a day that never
happened.

**Two guards** stop a projection becoming a claim: the show was seen on that weekday
more than once (`MIN_PATTERN_DAYS` = 2 — a one-off Thursday concert would otherwise
be promised every Thursday), and within the last 28 days **measured against today**,
not the date asked about (`MAX_PATTERN_AGE_DAYS`). Patterns come from a 56-day window
(`PATTERN_WINDOW_DAYS`, `rebuildSchedulePatterns`).

**No month filter.** A projection is not filtered by `shows.season_months`: no sync
field supplies it (on 2026-10-02 none of 1,414 seasonal shows carried months,
PAR-645). A show's season is the pattern window — a Halloween show drops out 28 days
after its last sighting. A `scheduled` time is never filtered.

## How far ahead showtimes are genuinely known

- **`scheduled` reaches zero days forward.** Backward it reaches as far as
  `show_live_data` is retained: `getShowtimesOnDate` anchors its snapshot window on
  the date asked about (Europa-Park, three months back, still returned 29 entries).
- **`projected` has no distance limit.** Both guards are independent of the target
  date, so a March answer is the same answer as next Saturday's, built from the same
  September observation. **`observedOn` is the only field carrying the
  uncertainty**; its age, not the distance from today, is the quality signal. Past a
  season boundary a summer programme is carried onto a spring date and nothing
  notices.
- **Coverage is the bigger limit.** Of 128 parks with shows, 46 have a weekday
  pattern and 45 can project. The rest return `[]` for every future date, which
  covers two cases the response cannot tell apart: a park that never published a
  showtime (81) and a seasonal park that fell out of the 56-day window (Aquatica
  Orlando, quiet since May). An empty `shows` therefore means "no projection
  available", not "this park has no shows".

## Past-midnight performances

A showtime belongs to the **previous** date when that date's published `OPERATING`
window still covers it (`ShowsService.operatingDaySql`, shared by
`getShowtimesOnDate` and the pattern rebuild), so a 00:30 performance is the day's
last, not the next day's first. It reads a published window, never the clock:

- Measured 2026-09-15 (PAR-51): 20 showtimes move, all at 00:00 (Disneyland Park
  Anaheim, Magic Kingdom). How many parks publish a wrap day moves with schedule
  syncs — quote such a count with its date.
- **Universal's late programme does not move**: no Universal park publishes an
  `OPERATING` day across midnight (Halloween Horror Nights is a `TICKETED_EVENT`), so
  its 00:30 performances stay on the following date. A snapshot taken today can
  therefore give *tomorrow* a lone `scheduled` 04:30 entry for a show whose real
  programme is unknown, and because `buildShows` prefers `scheduled` it suppresses
  that show's projection (109 entries in nine months).
- A blanket "before 06:00 belongs to yesterday" would be wrong the other way:
  Universal Studios Japan lists a daytime walkthrough at 01:00 local on 148 days.

`times` are wall-clock `HH:mm` strings; on a wrap day a 00:30 entry sorts after
23:00 (see [midnight](./midnight.md)).

## Where a show is

`latitude` / `longitude` (PAR-50) come from the `shows` row already loaded, through
the same `coord()` conversion as a ride's: numbers, and `null` for anything not a
finite number (a blank column would otherwise be `Number("") = 0`, the Gulf of
Guinea). **`null` means "position unknown"** — the normal case. A planner measuring
walks must treat such a show as a slot in time only and never substitute 0/0, the
park's position or the previous entry's. Test each field on its own. A straight line
is a lower bound on the walk.
