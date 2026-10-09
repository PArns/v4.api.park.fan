# Forward archive of served intraday curves (PAR-831)

**What it answers:** how good was the curve a visitor was actually *served*, one
to ninety days before their visit? Nothing else in the database can say. Every
forecast table keeps only the freshest answer per target —
`wait_time_predictions` deletes the future rows on each run,
`prediction_accuracy` keeps the last prediction per slot — so by the time a day
can be scored, the 3-day-ahead curve of it is gone.
[`prediction_lead_snapshots`](./long-range-forecasting.md#6-what-is-still-unknown)
fixed this for the *daily* number; this archive does it for the *curve*, for
the surfaces BENCH-SPEC (PAR-826) measures: UC1, UC2, UC3 and decision metrics
D1, D3, D6, D8, D9 — and, for BENCH-SPEC's headline question ("Horizon": up to
which lead does a forecast stay useful?), a sparse set of long leads out to the
planner's +90 days.

Code: `src/parks/services/forecast-archive.service.ts` (capture, retention),
`src/parks/services/forecast-archive-scoring.service.ts` (truth, scoring, board),
`src/ml/utils/forecast-archive-scoring.util.ts` (the pure metrics),
`src/queues/processors/forecast-archive.processor.ts` (queue `forecast-archive`
for the capture, `forecast-archive-score` for the scoring — separate, so a long
scoring run never delays the hourly capture and costs Europe an origin).

## 1. What is captured, and from where

**As served, never re-implemented.** The archive calls the same methods the
endpoints call; a second implementation would measure itself, and the first time
it drifted the board would score a product nobody is shown.

| origin (park-local) | surface | method | resolution | content |
| --- | --- | --- | --- | --- |
| 06:00 daily | `park_hourly` | `MLService.getParkPredictions(park, "hourly")` | 15 min (native) | next 48 h, one row per ride × operating day (d0, d1, d2 partial) |
| 06:00 daily | `plan_day` | `PlanDayService.buildPlanDay(park, date)` | 60 min (native, served as a step) | d0 … d7, one row per ride |
| 06:00 daily | park-day row | `buildPlanDay` + `CalendarService.buildCalendarResponse` | per park-day | tier, `crowdLevel` (+ whether it was a fallback), `predictedCrowdLevel`, `typicalDayPeak` at the origin, hours, `ridesOffered`, `ridesUnavailable`, `accuracy`, `leadTimeMae`, level source |
| 07:00 (long leads) | `plan_day` + park-day row | same as the 06:00 planner | 60 min | **d10, d14, d21, d30, d45, d60, d90** only; leads count from the same park-local date. An hour after 06:00 so the Europe burst is spread over two hours |
| 10, 12, 14, 16, 18 | `park_hourly` | same as above | 15 min | next 4 h only (UC1 + short UC2), with the live anchor |
| 10, 12, 14, 16, 18 | `plan_day` (D1 row) | `buildPlanDay(park, today)` | 60 min | per planned ride: the plan hours of the 2-h look-ahead, the frontend's next-best-ride "later" value and the live anchor — feeds D1 only |

Intraday origins run only for parks whose 06:00 capture found a curve for today,
so a closed park costs nothing all day.

`getParkPredictions` is the **served** read: CatBoost hourly with the PCN
override and §7.7 persistence blend applied where PCN answered. Each slot records
its source in `sources` (`p` = pcn_blend, `c` = catboost); `/plan/day` hours
record `m` measured (the hour-mean of that same served curve), `k` composed,
`l` climatology, `o` observed. `-` is a slot the payload did not carry.

**Composer A/B (PAR-834).** With `PLAN_DAY_H5_COMPOSER=true` a planner ride that
carries `slots` is archived at **15 minutes** (`slot_minutes = 15`), exactly as
the frontend contract reads it: the slot where there is one, the hour's value for
an hour with no slot at all. Composed numbers are coded by their composer, so the
board scores the two side by side under their own source names: `k` composed
(old: hourly P50 stretched to the level), `h` `composed_h5` (H5, no level), `t`
`composed_h5_tft` (H5 × TFT level ÷ the ride's 56-day P90). `composed_h5_tft`
counts as level-derived for the `level_<x>` split; `composed_h5` does not — it
uses no level. Rides the flag leaves on the old composer stay hourly with `k`.

Per curve row: `waits` (served q50 per slot), `day_peak` and `expected_error`
(plan_day), `model_version` (CatBoost version without `+pcn`), `ride_q90_56d`
and `is_headliner`, plus:

- `bands` (park_hourly only): served `uncertaintyMinutes` per slot — CatBoost's
  q95 − q50, one-sided.
- `peak_band` (plan_day only): the ride's served `uncertaintyMinutes`. On the
  planner it is a ONE-sided upper half-width around **`dayPeak`** (the p95 of
  the signed residual `actual − predicted peak`, built in
  `forecast-accuracy.service.ts`), not a band around each hour, so it is scored
  per ride-day and never pooled with the per-slot band.
- `live_wait`, `live_age_min` (park_hourly and the D1 rows): the STANDBY reading
  **in force** at the origin — the last change-log row at or before it, no older
  than 3 h (the truth's own rule), kept only when OPERATING with a wait — and its
  age. A "row in the last 30 min" rule missed half the operating rides:
  `queue_data` writes only on change.
- `later_wait`, `later_at` (D1 rows): the frontend's next-best-ride value.
- `level_source` (plan_day, and on the park-day row): which model produced the
  day level the curve is built on — `tft`, `catboost`, `climatology` (a
  climatology day), on the park-day also `mixed` / `none`. The planner payload
  does not expose it, so it is reconstructed the way `PlanDayService.dayLevels`
  picks it: the `getServingDailyPredictions` row for that ride whose
  `predictedTime.slice(0, 10)` is the date, TFT rows carrying
  `modelVersion: "tft"`. Internal only, not in the API.

**Operating day.** A served 15-min slot before 04:00 park-local belongs to the
previous date, so a park open until 01:00 keeps its evening in one row and is
scored against the schedule window that holds it.

**Ex-ante busy** (`ride_q90_56d`) is the ride's q90 of **time-weighted** 15-min
waits inside the published windows over the 56 days before the origin —
BENCH-SPEC's busy segment (≥ 45 min), stored as the number so the threshold can
move. It is read from `attraction_hourly_history`, whose `slots` hold only the
quarter-hours that had a reading (a change log again), so each ride-day is
forward-filled (a value in force for at most 3 h) onto the quarter-hour grid of
that day's published OPERATING window, starting at the park's opening — not at
the first slot the rollup holds, which can be a stray 00:00 reading. A window
past midnight is cut at 23:59; a day without a published window contributes
nothing. Measured on production-shaped data: 228 ms for Europa-Park.

**Origin and schedule.** One hourly job at :05 UTC reads each park's clock, so
every timezone (including half-hour offsets) gets its origin within the hour.
Each park's `origin_at` is read from the clock **immediately before its own
capture** — one instant for the whole run overstated every later park's leads by
however long the parks before it took. A park × origin hour is marked done in
Redis only after its capture succeeded. The cron runs once an hour, so a park
whose capture failed misses that origin; the marker only stops a manual or late
re-run inside the hour from capturing a park twice. A park's curves and park-day
rows are written in **one transaction**, so a failure between the two cannot
leave curves behind that a re-capture would duplicate.

**A plan that throws is an empty plan.** If `buildPlanDay` fails (as it does for
Europa-Park while PAR-832 is open), the park-day row is still written, with
`ridesOffered = 0` and `rides_unavailable = capture_error`, so D9 counts the day
rather than dropping it from the denominator.

**Not archived:** the q80 crowd quantile (only `crowdLevel` is served).

## 2. Size and DB impact (measured 2026-10-09)

Inputs from production: 4,158 rides in 126 parks carry an hourly curve, 76.4
operating slots per ride on average within 48 h (max 192); `queue_data` writes
148 k STANDBY rows a day; 50 of the 211 parks are in Europe. Tuple sizes
measured with `pg_column_size` on literal rows: 592 B for a 76-slot row, 224 B
for 16 slots, 208 B for an 11-hour plan_day row.

| part | rows / day | written / day (incl. ~100 B of indexes per row) | lives | steady state |
| --- | --- | --- | --- | --- |
| park_hourly, 06:00 | ~10 k | ~4 MB | target + 14 d (~15 d) | ~60 MB |
| park_hourly, intraday | ≤ ~21 k | ≤ ~7 MB | ~14 d | ~95 MB |
| plan_day D1 rows, intraday | ≤ ~20 k | ≤ ~5 MB | ~14 d | ~70 MB |
| plan_day d0–d7 | ~30 k | ~10 MB | ~17.5 d avg | ~175 MB |
| plan_day d10–d90 | ~27 k | ~9 MB | lead + 14 d (Σ 368 row-days per ride) | ~460 MB |
| **curves total** | **~107 k** | **~35 MB** | | **~0.85 GB** |
| park days | ~3.2 k | ~0.8 MB | target + 180 d | ~140 MB |
| scores | ~1.5 k | ~0.5 MB | 400 d | ~200 MB |

Retention is by **target date** (14 days past it for curves), never by origin:
counted from the origin, a d90 row would be gone before its day is scored. The
steady state is dominated by long-lead rows waiting for their day — the price of
measuring d90 at all.

Writes are batched (≤ 1,000 rows per `INSERT … ON CONFLICT DO NOTHING`). The
heaviest capture hours are the two in which Europe's 50 parks reach 06:00 and
07:00: ~50 × 8 `buildPlanDay` calls (06:00) and ~50 × 7 (07:00, long leads), on
warm caches, three parks at a time under the shared `DbJobBudget`; the queue has
a 15-minute lock headroom. Each intraday origin adds one `buildPlanDay` (today)
per open park. Per capture the extra reads are one `getServingDailyPredictions`
(single-flight, the call `buildPlanDay` makes anyway), one `DISTINCT ON` over the
last 3 h of `queue_data` for the live anchor, and once a day per park the
forward-filled q90 over 56 days of the hourly rollup (cached).

**Not a hypertable, no foreign key.** At ~0.85 GB a plain table is fine, and
retention is an app-side `DELETE` in 20,000-row batches with `lock_timeout` 2 s
and `statement_timeout` 60 s — never a TimescaleDB policy, whose `drop_chunks`
locks every FK target ACCESS EXCLUSIVE ([runbook §0b](../troubleshooting/db-health-runbook.md)).
Every query the jobs run against `queue_data`, `attraction_hourly_history`,
`schedule_entries` and the archive itself goes through `queryWithLimits`
(`src/common/utils/statement-limits.util.ts`) with `statement_timeout`,
`lock_timeout` and `idle_in_transaction_session_timeout`.

**Scoring cost:** per park and day, five indexed reads (curves via
`(park_id, target_date)`, park days, schedule, attractions) plus one `queue_data`
read of that park's STANDBY rows over the whole park-local day (and the 3 h of
forward-fill before the first window) — yesterday's chunk, uncompressed,
~150 k rows across all parks. ~1,000 small queries once a day.

## 3. Truth

**Slots (BENCH-SPEC).** 15-min slots on the UTC quarter-hour grid (= the
park-local grid for every quarter-hour offset). A slot's value is the STANDBY
row in force at its midpoint — `queue_data` is a change log, so the last row at
or before it, no older than 3 h. It counts only when that row is OPERATING with a
wait ≥ 5 and the midpoint lies inside a published park-wide OPERATING window from
`schedule_entries` for that date (windows are instants, so DST and midnight
crossings need no special case). A ride-day's level is the P90 of its truth
slots (≥ 4).

**Crowd bucket (D6) — the calendar's own statistic, on both sides.** The
denominator is the park's `typicalDayPeak` **as stored at the origin** (on the
park-day row), and that baseline is the median, over 548 days, of a day value:
per headliner the P90 of the raw change-log rows that are STANDBY, OPERATING and
≥ 10 min on that park-local date — no window, no forward-fill — averaged over
the headliners (`AnalyticsService.calculateTypicalDayPeak`). The numerator is
that same day value for the target date (`calendarDayValue`), not the 15-min
truth: dividing a time-weighted P90 by a change-weighted baseline would shift
every bucket by the difference of the two statistics. The ratio is bucketed with
`determineCrowdLevel` ([crowd-level rules](../rules/crowd-levels.md)).

A plan_day hour is compared with each of its four quarter-hours (as served — a
step; BENCH-SPEC's interpolated variant is left to the bench runner).

## 4. Scores

Daily at 12:00 UTC, when the previous UTC date is over in every park timezone;
up to three missed dates are caught up. One row per
`(target_date, region, use_case, lead, source, segment)` in
`forecast_archive_scores`, with **additive counters** in `sums` (so any window
pools by adding, then divides once). A date is replaced as a whole (delete +
insert in one transaction). `region` ∈ EU / NA / ASIA / OTHER and an explicit
`ALL`. `segment` ∈ `all`, `busy` (ex-ante), `headliner`. `source` is the slot
source, `mixed` for a ride-day with several, or `all`. The key columns' widths
are `SCORE_KEY_WIDTHS` (`source` is `varchar(32)`); a spec asserts every label the
scorer can emit fits, because one over-long label fails the whole date's insert.

| use case | lead | what |
| --- | --- | --- |
| UC1 | `h0-1`, `h1-2` (slot lead after origin) | MAE, bias, band coverage — any origin |
| UC2 | `h2-6`, `h6-12`, `h12-24`, `h24-48` | same |
| D1 | `h0-2` (every judged origin) and the later hour's `h0-1` / `h1-2` × source | next-best-ride, **exactly the frontend's rule** (`park.fan/lib/planner/next-best-ride.ts`, ported as `nextRideLater`): "later" = the maximum of today's `/plan/day` hours whose START lies in [now + walk, now + 120 min] and that are before the unfolded close hour; a suggestion when later − live ≥ 10. The archive has no visitor position, so walk = 0. **Precision** = the ride's truth reached live + 10 within those 120 min; false-suggestion rate = 1 − precision; **base rate** = the same outcome among rides that had a qualifying hour but a gap under 10, so precision can be read against chance. Rides with no qualifying hour at all (closing soon, no curve) are counted apart (`d1NoHour`) and do not dilute the base rate |
| UC2 | `d0` … `d2` (06:00 origin) | D3 best time and slot Spearman (below) |
| UC3 | `d0` … `d7`, `d10`, `d14`, `d21`, `d30`, `d45`, `d60`, `d90` | slot MAE/bias; D3 per ride-day; dayPeak MAE and bias vs truth P90; **D8**: share of ride-days with truth P90 − dayPeak ≤ the served band (one-sided, as the band is), stated `expectedError` vs realised \|dayPeak error\| on the same ride-days, and the share of ride-days carrying each field; park-day dayPeak rank Spearman and pairwise ordering (pairs ≥ 5 min apart; D4's ordering half) |
| D6 | same leads; source `predicted` (calendar `predictedCrowdLevel`), `plan_day` (the planner's `crowdLevel` where the calendar had a forecast), `plan_day_live` (d0, where the planner's `crowdLevel` is the live-overridden one) or `fallback` (no calendar forecast: ML crowd or placeholder `moderate` — scored apart, never as a forecast) | exact / ±1 bucket accuracy, busy-day (high+) recall and precision, `unknownPred`, cross-park pairwise ordering on the same date |
| D9 | same leads, source = plan tier | rides that operated (≥ 1 truth slot) that the plan offered; offered rides that never operated; park-days with operating rides but an empty plan (including `capture_error`) |

**D3 best time.** Regret (truth at the predicted best slot − true minimum) is the
headline: it is defined on every ride-day. The hit rates — true best slot among
the predicted top 2, and within ±30 min of the predicted best — are counted only
on ride-days with a clear best time: when the true minimum is shared by more than
25 % of the slots (a ride at 5 min all day) any forecast would "hit", so those
ride-days are counted as `rdFlat` instead.

**Level-source split.** UC3 and D9 rows are also written under
`source = level_<tft|catboost|climatology|mixed|none>` so the board splits the
horizon curve by the model behind the day level (TFT ≤ 60 days, CatBoost
beyond) — but only for numbers that come from the level: composed / climatology
hours, and dayPeak. A `measured` hour is the hourly model's, whatever the day
level was.

**D8 bands.** On park_hourly the band is `truth ≤ pred + band` per slot — the q95
upper side as served; the CatBoost band is known to cover only 53–57 %
([quantile serving](./quantile-serving-and-calibration.md)). On plan_day it is
truth P90 − dayPeak ≤ band per ride-day — also one-sided, because the served
figure is the p95 of `actual − predicted peak`: an over-forecast is inside it
however large. The two are different statistics and are never pooled.

## 5. Admin board

`GET /v1/admin/forecast-archive?days=14&region=ALL` (admin auth, read-only,
cached 2 h). The cache key carries a version that every scoring run increments,
so all `days` × region variants are retired at once. Returns the pooled counters
and derived metrics per use case × lead × source × segment over the last `days`
scored dates, plus the last 24 h of archive rows per surface and origin kind and
the first origin.

## 6. When the first scores appear

The archive starts filling on the first capture hour after deploy. Day *D*
is scored at 12:00 UTC on *D + 1*:

- UC1, UC2 (`h…`, `d0`), D1, UC3 `d0`, D6/D9 `d0`: the day after deploy.
- `dN` needs a capture *N* days before the target, so lead *N* first reports on
  deploy + *N* + 1 days: d7 after 8 days, d14 after 15, d30 after 31, d60
  after 61, **d90 after 91 days**. A lead has a stable pooled number about two
  weeks after its first report (one row per ride per day per lead — ~4,000
  ride-days a day, plenty for the pooled board, thin per park).

## 7. Follow-ups (not built here)

- **D4** optimiser regret: needs the frontend's optimiser objective re-run on
  archived vs true curves; the dayPeak pairwise ordering half is scored here.
- **D7** day-comparison winner: needs the calendar's per-day levels for a whole
  month (UC4), which the park-day rows do not cover.
- **UC4** (daily per park out to d365) stays with `prediction_lead_snapshots`.
- D1 assumes the visitor is at the ride (walk = 0); a walk distribution would
  shift "later" by up to an hour for distant rides.
- D6 is not split by level source (the calendar's `predictedCrowdLevel` mixes
  both models within a park-day).
- Bootstrap CIs over park-days: the scores are pooled per region; the raw
  archive lets the bench runner recompute per park-day when a paired comparison
  needs a CI (raw curves live 14 days past their target date).
- Park merges: `forecast_archive_curves` rows move with a merged ride
  (`merge-dependencies.ts`); `park_id` is not reparented on a park merge — the
  rows age out 14 days after their target date.
