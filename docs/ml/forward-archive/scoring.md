# Forward archive — truth, daily scores and the admin board

How the daily `score` job (12:00 UTC, queue `forecast-archive-score`) builds the truth, what it
writes to `forecast_archive_scores`, and what `GET /v1/admin/forecast-archive` returns. The paired
model comparison written by the same job is in [model-comparison.md](model-comparison.md). Open
this page before reading or changing a forward-archive score.

## Truth

- **Slots (BENCH-SPEC).** 15-minute slots on the UTC quarter-hour grid. A slot's value is the
  STANDBY row in force at its midpoint (the last change-log row at or before it, ≤ 3 h old). It
  counts when that row is OPERATING with a wait ≥ 5 and the midpoint lies inside a published
  park-wide OPERATING window from `schedule_entries` for that date (windows are instants, so DST
  and midnight need no special case). A ride-day's level is the P90 of its truth slots (≥ 4).
- **Heartbeat rows are not truth** (PAR-1024): the heartbeat writer copies the last status and
  wait with a fresh timestamp, which resets the 3 h clock and keeps a frozen value alive while the
  feed is silent — persistence then scores 0 on exactly those slots. Truth reads
  `NOT COALESCE(is_heartbeat, false)` (`truthRowsSql`), the same rule as ml-bench. The live anchor
  still reads them: it is what the app shows. Other boards that read `queue_data` themselves (the
  `prediction_accuracy` performers) do not use this filter.
- **Crowd bucket (D6)** uses the calendar's own statistic on both sides: the denominator is the
  park's `typicalDayPeak` as stored at the origin (on the park-day row) — the median over 548 days
  of a day value: per headliner the P90 of the raw STANDBY, OPERATING, ≥ 10 min rows of the
  park-local date, averaged over headliners (`AnalyticsService.calculateTypicalDayPeak`). The
  numerator is the same day value for the target date (`calendarDayValue`), not the 15-minute
  truth. The ratio is bucketed with `determineCrowdLevel` ([crowd-level rules](../../rules/crowd-levels.md)).
- A `plan_day` hour is compared with each of its four quarter-hours, as served (a step).

## Scores

One row per `(target_date, region, use_case, lead, source, segment)` with **additive counters** in
`sums`, so any window pools by adding and divides once. A date is replaced as a whole (delete +
insert in one transaction); up to three missed dates are caught up. `region` ∈ EU / NA / ASIA /
OTHER and `ALL`; `segment` ∈ `all`, `busy` (ex-ante), `headliner`; `source` is the slot source,
`mixed` for a ride-day with several, or `all`. Key widths are `SCORE_KEY_WIDTHS` (`source` is
`varchar(32)`); a spec asserts every emitted label fits, because one over-long label fails the
whole date's insert.

| Use case | Lead | What |
| --- | --- | --- |
| UC1 | `h0-1`, `h1-2` | MAE, bias, band coverage — any origin |
| UC2 | `h2-6`, `h6-12`, `h12-24`, `h24-48` | same |
| UC2 | `d0` … `d2` (06:00 origin) | D3 best time and slot Spearman |
| UC3 | `d0` … `d7`, `d10`, `d14`, `d21`, `d30`, `d45`, `d60`, `d90` | slot MAE/bias; D3 per ride-day; dayPeak MAE and bias vs truth P90; D8 (below); park-day dayPeak rank Spearman and pairwise ordering (pairs ≥ 5 min apart) |
| UC3S | as UC3 | the composer shadow (slot MAE/bias, best-time regret, Spearman) |
| D1 | `h0-2` and the later hour's `h0-1` / `h1-2` × source | next-best ride, the frontend rule ported as `nextRideLater` (below) |
| D6 | UC3 leads | exact / ±1 bucket, busy-day (high+) recall and precision, `unknownPred`, cross-park ordering on a date, by source |
| D9 | UC3 leads, source = plan tier | rides that operated that the plan offered; offered rides that never operated; park-days with operating rides but an empty plan (incl. `capture_error`) |

- **D1** follows `park.fan/lib/planner/next-best-ride.ts`: "later" = the maximum of today's
  `/plan/day` hours whose start lies in [now + walk, now + 120 min] and before the close hour; a
  suggestion when later − live ≥ 10. No visitor position, so walk = 0. Precision = the ride's truth
  reached live + 10 within the 120 min; base rate = the same outcome among rides with a qualifying
  hour but a gap under 10. Rides with no qualifying hour are counted apart (`d1NoHour`).
- **D3 best time.** Regret (truth at the predicted best slot − true minimum) is the headline and is
  defined on every ride-day. The hit rates (true best among the predicted top 2; within ±30 min)
  count only ride-days with a clear best time — when the true minimum is shared by > 25 % of the
  slots the day counts as `rdFlat`.
- **D6 sources:** `predicted` (calendar `predictedCrowdLevel`), `plan_day` (the planner's
  `crowdLevel` where the calendar had a forecast), `plan_day_live` (d0, live-overridden),
  `fallback` (no calendar forecast — `unknown` since PAR-1028, counted in `unknownPred`; older rows
  hold the ML crowd or a placeholder `moderate`). Never scored as a forecast.
- **D8 bands.** On `park_hourly`: `truth ≤ pred + band` per slot (the q95 upper side; CatBoost's
  band covers 53–57 %, see [quantile serving](../quantile-serving-and-calibration.md)). On
  `plan_day`: truth P90 − dayPeak ≤ band per ride-day, also one-sided. Never pooled. Plus stated
  `expectedError` vs realised |dayPeak error| on the same ride-days, and the share of ride-days
  carrying each field.
- **Level-source split.** UC3 and D9 rows are also written under
  `source = level_<tft|catboost|climatology|mixed|none>`, but only for numbers that come from the
  level (composed / climatology hours and dayPeak); a measured hour is the hourly model's.
  `composed_h5_tft` counts as level-derived, `composed_h5` does not.
- **Unpaired board, rounding.** The per-source MAEs in `forecast_archive_scores`
  (`ScoreAccumulator`) are unrounded; the paired rows round both arms (see
  [model-comparison.md](model-comparison.md)). They are not the matrix's figures.
- **The heartbeat change is a step in this table.** Dropping heartbeats changed UC MAEs and the
  D-scores, and these rows carry no definition: the 14 replayed dates were rewritten, older dates
  keep the old truth, so a 28/56-day window mixes the two until it has moved past the deploy.

**Scoring cost:** per park and day five indexed reads (curves via `(park_id, target_date)`, park
days, schedule, attractions) plus one `queue_data` read of that park's STANDBY rows over the
park-local day and 3 h before the first window — ~1,000 small queries once a day.

## Admin board

`GET /v1/admin/forecast-archive?days=14&region=ALL` (admin auth, read-only, cached 2 h under a
version that every scoring run increments): pooled counters and derived metrics per use case ×
lead × source × segment over the last `days` scored dates, the last 24 h of archive rows per
surface (`surfaceName`) and origin kind, the first origin, and the `chronos` section
([chronos-shadow.md](chronos-shadow.md#paired-scoring-and-the-board)).

## Known limits

- **Known limit:** UC4 (the calendar month view) is not scored here; its daily levels out to d365
  are scored from `prediction_lead_snapshots`. D7 is in the paired table (`cal_rank`).
- **Known limit:** D1 assumes walk = 0, which can put "later" up to an hour early for distant
  rides.
- **Known limit:** the unpaired scores carry no confidence interval; the raw curves (kept 14 days
  past target) let the bench compute one.
- Open work: D4 optimiser regret, and D6 split by level source (`predictedCrowdLevel` mixes both
  models within a park-day), PAR-1143.
