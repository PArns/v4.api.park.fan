# Ride downtime: how much longer a running outage lasts

The recovery estimate on a ride that is down right now (`outage.estimate`): why it is allowed when a
forecast of the next outage is not, the measured curve, its calibration, and the wall-clock window.
Open it before changing `DowntimeRecoveryService`, `downtime-estimate.util.ts` or
`operating-clock.util.ts`. Index: [README](README.md).

## Why this is not the refused forecast

[refused.md](refused.md) refuses "when will this ride break next". This answers "it is broken now — what
happened to the outages that got this far". The event is observed, not forecast, and that changes all
four objections:

| Objection to a forecast                                 | Why it does not reach this                                                                                                                       |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1. ~1,300 events per bin to separate 0.006 from 0.012    | The quantity is a recovery share of 0.15–0.54 on 5,900–128,000 intervals per bucket; at p = 0.15, n = 5,924 the standard error is 0.46 points. |
| 2. The numerator is not stationary                      | Still true, which is why every sentence says _gemeldet_, but no claim is made that a ride will break.                                          |
| 3. Censoring depends on the outcome                     | Counted in operating minutes on the stitched reconstruction, censoring is 15.6 %, not 68 %.                                                    |
| 4. Left truncation at an unknown point                  | Conditioning removes it: a spell that survived to T minutes is by construction not one that vanished between two polls. Read only at T ≥ 5.    |

## The curve: measured, never fitted

`downtime_recovery_curves`, rebuilt nightly by `RECOVERY_CURVE_SQL` (Kaplan-Meier over
`attraction_outages`, one curve per signal), at elapsed buckets `RECOVERY_ELAPSED_BUCKETS` (5, 10, 15,
20, 30, 45, 60, 90, 120, 180, 240 operating minutes) and horizons 30 and 60. Pooled over all parks
(2026-09-06):

| Elapsed (operating min) |       5 |     15 |     30 |     60 |    120 |   240 |
| ----------------------- | ------: | -----: | -----: | -----: | -----: | ----: |
| at risk                 | 128,412 | 97,054 | 62,109 | 34,190 | 14,957 | 5,924 |
| back within 30 min      |  54.2 % | 47.0 % | 37.0 % | 29.8 % | 18.3 % | 8.5 % |
| back within 60 min      |  70.5 % | 63.8 % | 55.8 % | 46.8 % | 29.1 % | 14.8 % |
| remaining, median       |      25 |     35 |     50 |     70 |    165 | 1,168 |

The hazard falls from 54 % to 8.5 %, so **no exponential**: it would tell someone at hour four that it is
nearly over. For the same reason the unconditional median (25 minutes) is never shown. Past about two
hours the upper quartile stops resolving and is stored as `remainingP75: null`, rendered as an open range.

**Calibration**: fitted on 120 days, scored on the next 60, mean absolute error 2.55 percentage points,
under 2 from T = 60 on. Percentages are rendered in steps of five because of it.

**Per park, never per ride.** P(recover within 30 | down 30) runs from 8.6 % to 52.4 % across 48 parks;
only 95 of 2,285 rides have 200 intervals. A park uses its own curve at a bucket with ≥ 300 intervals
(`MIN_PARK_CURVE_SAMPLE`), otherwise the pooled one, **decided per bucket**: a park whose curve stops at
30 minutes must not answer a four-hour outage with its 30-minute row (52 % instead of the measured
8.5 %). Below 200 intervals (`MIN_ESTIMATE_SAMPLE`) no estimate is given. A `closed_gap` is read only off
its own signal's curve.

The bucket is floored, never interpolated, and `remaining` is not shifted by the minutes already spent in
the bucket: the hazard falls, so an outage at 175 minutes has a longer remaining distribution than one at
120. The window therefore recedes inside a bucket and errs late; a finer grid is the fix, not
interpolation.

## The wall-clock window (`recoveryWindow`)

`remaining` is in operating minutes, which no reader may add to a clock: 120 operating minutes left in a
park that shuts in twenty end 100 minutes into tomorrow's opening. `estimate.recoveryWindow` is
`{ from, to }` in ISO 8601 UTC, from `remaining.p25` / `p75` walked forward over the park's flattened
`OPERATING` windows (`projectOperatingMinutes`, the forward twin of the elapsed sum; both read
`parkOpenWindowCtes()`).

- A pair, never one instant: the distribution is heavy-tailed.
- `to` is absent when there is no upper bound (no p75, or the calendar does not reach it).
- The whole window is absent when the park publishes no hours or the calendar does not reach p25. There
  is no wall-clock fallback.
- With the park shut, `from` is the next opening plus the quartile.
- The horizon is 14 days (`WINDOW_HORIZON_DAYS`): the largest p75 the curve resolves is 460 operating
  minutes, and 14 days covers a weekends-only park.
- The instants are absolute, so a cached copy (~15 min) stays internally consistent; a `from` in the past
  reads as "any moment now".

`ExcludeNullInterceptor` deletes null-valued keys outside `/v1/admin/*` and `?debug=true`, so
`recoveryWindow.to` is typed `string | undefined`. `remaining.p75` keeps `number | null` because clients
are coded against it; its description says the key is absent.

## Rendering

The park page's ride card shows the range only („meist noch 25 Min. bis 4:15 Std."); the ride page shows
the sentence, naming the condition („Von Störungen, die schon so lange dauern, waren rund 30 % binnen
einer Stunde wieder in Betrieb"). Frontend: `OutageEstimateNote`.

## The remaining caveat

The live statement (`trailingOutageWithElapsedSql`) ends a run on our synthetic `data_source` values but
does not read `is_heartbeat`, so a carried `DOWN` whose recovery never reached us keeps accumulating
elapsed minutes. The error is in the safe direction (a larger elapsed reads a lower recovery share).
