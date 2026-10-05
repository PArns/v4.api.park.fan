# A ride that has not run yet today (`notRunToday`)

Added 2026-10-05 (PAR-157's pull request). On the park payload's attractions and
on the attraction detail response:

```json
"notRunToday": { "lastRunAt": "2026-10-04T16:00:00.000Z" }
```

## When it is present

All of these, decided by `AttractionOutageService.getNotRunToday` and
`NOT_RUN_TODAY_SQL` (`src/common/utils/not-run-today.sql.ts`):

- the ride's resolved status is `CLOSED`, and it has no `outage`;
- it is not out of season (`isCurrentlyInSeason !== false`) and not inside a
  curated works period — the page already says why such a ride is shut;
- its park is inside a published OPERATING window, and the day's first window
  opened at least `NOT_RUN_TODAY_GRACE_MINUTES` (15) ago, so the minutes in
  which rides open one poll at a time do not put the line on every ride;
- the ride has no observed OPERATING reading since the middle of the night:
  halfway between the previous operating day's last close and today's first
  opening. Not local midnight, not today's opening, and not the previous close
  itself. Feeds often read a ride OPERATING once more just after the close, and
  that reading belongs to yesterday. A ride that ran in an early-entry hour has
  run today, a ride that ran before a midday break has too, and a ride whose
  feed leaves it OPERATING overnight and flips it in the morning has not;
- `lastRunAt` falls on one of the six days before today, in the park's own
  calendar. Seven days back is today's weekday again, and „zuletzt Montag,
  17:00 Uhr" on a Monday reads as this afternoon. A ride that has not run for
  longer is closed for something longer than a day, and "not yet today" would
  promise an opening nobody announced. It gets no field.

## What `lastRunAt` is

The end of the ride's last OPERATING run — the reading that followed its last
OPERATING one — clipped to the close of the window that run fell in. Without
the clip, a feed that flips a ride to CLOSED only the next morning would make
the last run „Monday 09:30" in a sentence that also says it has not run today.
If the last OPERATING reading itself came after the close (an evening event),
that reading is the latest instant that can be vouched for.

## What it does not say

Why. A water ride on a cold day, a maintenance day, a ride with a later opening
time and a fault from yesterday evening all read the same from the feed. That is
why this is its own field and never a third outage `signal`: an outage claims
that something stopped the ride. The page renders it neutrally („Heute noch
nicht in Betrieb · Zuletzt in Betrieb: Sonntag, 18:00"), never in the outage
colour, and names the instant by weekday in the park's timezone, as the outage
line names its start.

## Pinned by

`test/e2e/not-run-today.e2e-spec.ts` on TimescaleDB (the close of the day it
last ran, an overnight-carried run clipped to the close, a ride that ran this
morning, one running now, one gone for eight days, the grace period and a shut
park) and the selection cases in `attraction-outage.service.spec.ts`.
