# A ride's live crowd level and best visit times

Applies to every ride in the park payload (`attractions[]`), the attraction detail
payload and the favorites card.

## `crowdLevel` is a measurement, never a forecast

A ride's `crowdLevel` is its **current live wait rated against its own baseline**
(ratio-vs-P50, the live regime in [Unified crowd levels](../rules/crowd-levels.md)).
It takes one of:

| Value | When |
| --- | --- |
| `very_low` … `extreme` | The ride's `effectiveStatus` is `OPERATING`, a live wait exists, the park is ratable and the ride has a baseline |
| `closed` | The ride's `effectiveStatus` is `CLOSED` |
| `unknown` | Anything else: no live wait (a `DOWN` ride — even one whose row still carries a posted number or the 0 ingestion writes for a non-`OPERATING` status — an `OPERATING` ride whose row carries no minutes, an `UNKNOWN` ride), no baseline, a thin park, or a park whose wait times we cannot read |

Before PAR-1029 a ride with no live wait took the model's forecast level for the
current UTC hour (at 14:50 that was the 14:00 row), or `very_low` when there was
none. For a broken-down ride that read as "walk on". It is now `unknown`, by the
rule in [An absent fact never becomes a confident one](../rules/absent-facts.md).

What the frontend must do with it:

- Render `unknown` as "no rating", never as a quiet tier.
- Leave `unknown` (and `closed`) rides **out of** any count, average or sort key
  built from ride crowd levels. Counting them as low is the bug this fixes.
- The park's own `analytics.statistics.crowdLevel` / `occupancy.crowdLevel` is a
  separate computation over live waits (`calculateParkOccupancy`); it never read
  the per-ride `crowdLevel`, so it is unaffected.
- Favorites used to send `null` where the other surfaces sent `unknown`; it
  sends `unknown` now too.

## `bestVisitTimes` never offers "now" for a ride that is not running

`bestVisitTimes` ranks today's 15-minute forecast slots. The current slot is
included only while the ride's `effectiveStatus` is `OPERATING`; for `DOWN`,
`CLOSED`, `REFURBISHMENT` or `UNKNOWN` it is dropped and the later slots stay,
since the ride may be back by then. An `OPERATING` ride's current slot is still
ranked on its live wait instead of the forecast.

The gate lives in `computeBestVisitTimes(predictions, closingTimeIso, rideOperating)`
(`src/common/utils/best-visit-times.util.ts`), which all three surfaces call.
