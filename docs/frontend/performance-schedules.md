# A ride that runs as performances (`performanceSchedules`)

The contract for a ride that starts at fixed times instead of running continuously: a 4D cinema, a
show walk-through, a theatre with a programme (PAR-1355). What the field holds, how a client picks
today's start times, and why a live CLOSED between two performances is no closure. On every ride of
the park payload and on the attraction detail response:

```jsonc
"performanceSchedules": [
  {
    "from": "2026-11-14",          // first park-local day, inclusive; absent: no start
    "to": "2027-01-24",            // last park-local day, inclusive; absent: no end
    "times": ["13:00", "14:00", "15:00", "16:00", "17:00"]  // park-local HH:MM, ascending
  }
]
```

Absent when no schedule is curated, which is the state of nearly every ride; never an empty list.
Sorted by `from`, an open start first. Schedules never share a day, so at most one covers a given
day. A schedule with neither bound holds every day.

## Why it exists

No feed carries it. Phantasialand's „Die 3 Mausketiere" runs continuously in summer and at
13:00, 14:00, 15:00, 16:00 and 17:00 during Wintertraum, as the sign at its entrance says.
ThemeParks.wiki reports the cinema as an `ATTRACTION` without showtimes, and it read CLOSED from
the last summer day on, through the first Wintertraum days. A wait time is the wrong answer for
such a ride in any case: what a visitor needs is when the next performance begins.

Curated by hand under `/admin/attractions/<id>` in the group „Vorstellungen", or through the MCP
`curate_attraction` tool (`curatedPerformanceSchedules`). See [curation](../admin/curation.md).

## Picking today's times

The payload carries every schedule and no "today", for the reason
[`worksPeriods`](./works-period.md) gives: it is cached, and a choice made when it was built lags
the day boundary by the cache lifetime. A client:

1. takes the park's today in the park's `timezone`, never the reader's clock;
2. picks the schedule with `from <= today <= to` (a missing bound is open);
3. finds no schedule: the ride runs as its live status says. This is the summer Mausketiere;
4. finds one: the next start is the first time after the park's current wall-clock time, and when
   none is left, there is no performance today.

A time belongs to the calendar day it is read on. A park whose day runs past midnight has no
performance after midnight in this field yet.

## Status

The API does not change `status`, `effectiveStatus` or `notRunToday` for such a ride: they stay the
feed's reading. A client showing today's schedule:

- shows the countdown while the park is `OPERATING` and the ride reads `OPERATING`, `CLOSED` or
  `UNKNOWN`. A CLOSED between two performances is the feed not knowing the programme;
- keeps the outage rendering for `DOWN` and the works rendering for `REFURBISHMENT` and a running
  [works period](./works-period.md);
- shows nothing of the schedule while the park is closed.

## Pinned by

`performance-schedule.util.spec.ts` (order, the day a schedule covers, an open schedule) and the
PAR-1355 cases in `admin-curation.service.spec.ts` (validation on write).
