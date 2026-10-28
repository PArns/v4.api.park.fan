# Ride downtime: the closure-gap signal

The second outage signal, `closed_gap`, for parks whose feed never emits `DOWN`: what shape a fault
has, the filters that separate it from a scheduled closure, standing closures and the window rules.
Open it before changing `src/common/utils/closure-gap.sql.ts`. Index: [README](README.md).

## Why a second signal

The `DOWN` reconstruction ignores `CLOSED` while the park is open, because as a raw status it is mostly
genuine closures (534 of 602 anomalies in ML's deleted `unexpected_closure` class). That leaves the
[`never_reports` parks](publishing.md#park-regimes) with nothing. A fault has a shape a scheduled closure
lacks: the ride **ran earlier the same operating day**, shut **inside** opening hours, and (usually) came
back.

It runs **only in blind parks** (`blind_parks` CTE). Where the feed emits `DOWN`, faults appear as
`DOWN` (45.74 per 1,000 operating hours) and hardly ever as closure gaps (2.37), so adding it there
would mix two populations for a 5 % gain. In blind parks the `DOWN` rate is 0.00 and the gap rate 5.78.

It is not a report and is never worded as one: `attraction_outages.signal = 'closed_gap'` reaches the
page as „Steht seit 14:20 Uhr still", never „Störung gemeldet" or „behoben".

## The filters

The constants and the measurement behind each are in the header of `closure-gap.sql.ts`. Over 21 days,
25,759 raw `OPERATING → CLOSED` transitions with a same-day return became 3,618 gaps, with quartiles
close to `DOWN`'s 10/25/50 minutes and unlike a scheduled closure.

| Constant                         | Value | Removes                                                                                                   |
| -------------------------------- | ----: | --------------------------------------------------------------------------------------------------------- |
| `MAX_SIMULTANEOUS_CLOSERS`       |     2 | Park-wide events. 61.2 % of raw transitions are 5+ rides closing in one minute (Phantasialand at 18:10). Counted over the **park**, not over the ids passed in, so a single-ride call is correct. |
| `MAX_REGULAR_DAYS`               |     5 | A ride with its own shorter hours (Disneyland rides on their night pause).                               |
| `MAX_GAP_DAY_SHARE`              |   0.5 | A show or duty cycle that closes at a different time each day (71 rides gapped on ≥ 60 % of their days).  |
| `MAX_EARLY_END_SHARE`            |   0.5 | A ride whose day ends before the park's (Futuroscope's cinemas: 100 % of days; a real fault: 8 %).        |
| `MIN_GAP_MINUTES`                |    10 | One poll of noise (27.1 % of raw gaps are exactly one 5-minute cycle).                                    |
| `MIN_PARK_MINUTES_LEFT`          |    60 | A closure in the last hour of the closure's own window.                                                   |

Carried rows are excluded by `is_heartbeat` with the `"lastUpdated" = timestamp` fallback. Filtering by
`data_source` alone failed: the heartbeat copies the previous row's `data_source`, and one carried row
in an `OPERATING/CLOSED/OPERATING` triple dropped the gap, which silently cut every gap over ~65 minutes.

Free-flow rides (`open_with_park`) are excluded from both closure statements: the flag says their
`CLOSED` readings are not evidence, and the page showed such a ride as open by the flag and standing
still from the same rows (PAR-913).

## Standing closures (PAR-157)

A closure that does not come back the same day is kept as a **standing closure**: followed to the
ride's next status for at most `LIVE_LOOKBACK_HOURS` (26 h), on the operating clock, and stored as
`recovered`, `reclassified` (turned `REFURBISHMENT`, censored for this curve), `ongoing` or
`window_edge`. Before that, every stored gap was `recovered`, the population had no censoring, and a
recovery curve built on it could only err towards "back soon".

## Which window a closure belongs to (PAR-913)

- **`MIN_PARK_MINUTES_LEFT` is measured against the window the closure started in** (`run_window`), not
  today's close. Measured against today's close, a ride that shut after Friday's close stood all of
  Saturday as „Steht seit Freitag, 18:05 Uhr still".
- **A start before the opening counts from the opening.** `startedAt` is clipped to the window's
  opening, and the 10-minute floor and elapsed minutes count from there. A start after the day's last
  close finds no window and is dropped; `notRunToday` speaks for that ride the next morning.
- **Outside every window, "today" begins halfway through the night** (`night_mid`, the same instant
  `NOT_RUN_TODAY_SQL` uses), so a reading the feed left standing after the close cannot make the next
  day's fault.
- **The live statement returns nothing outside a published `OPERATING` window**: a ride reading
  `CLOSED` in a shut park is a shut park.

The nightly statement still requires the start inside a window, so pre-opening closures are shown live
but not stored for the curve (the conservative direction).

## Rides whose feed always reports 0 (measured, not filtered)

No closure line for any ride whose feed always reports a wait of 0 was measured and not applied: in
the blind parks 1,105 such rides read `OPERATING` on ≥ 80 % of open days and produced 777 of 4,140
stored closures in 60 days, and those parks publish status and nothing else. Rides reading
`OPERATING` on under 40 % of open days (Die 3 Mausketiere's shape) produced 74.

**Known limit:** whether those closures are real has no ground truth yet; the closed_gap hand check
(PAR-1095) decides whether a track-record gate on that share is needed.

Cases are pinned in `test/e2e/closure-standing.e2e-spec.ts` and `closure-gap-operating-day.e2e-spec.ts`.
