# Forward archive of served intraday curves (PAR-831)

**What it answers:** how good was the curve a visitor was actually *served*, one
to seven days before their visit? Nothing else in the database can say. Every
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
`src/queues/processors/forecast-archive.processor.ts` (queue `forecast-archive`).

## 1. What is captured, and from where

**As served, never re-implemented.** The archive calls the same methods the
endpoints call; a second implementation would measure itself, and the first time
it drifted the board would score a product nobody is shown.

| origin (park-local) | surface | method | resolution | content |
| --- | --- | --- | --- | --- |
| 06:00 daily | `park_hourly` | `MLService.getParkPredictions(park, "hourly")` | 15 min (native) | next 48 h, one row per ride × local date (d0, d1, d2 partial) |
| 06:00 daily | `plan_day` | `PlanDayService.buildPlanDay(park, date)` | 60 min (native, served as a step) | d0 … d7, one row per ride |
| 06:00 daily | park-day row | `buildPlanDay` + `CalendarService.buildCalendarResponse` | per park-day | tier, `crowdLevel`, `predictedCrowdLevel`, hours, `ridesOffered`, `ridesUnavailable`, `accuracy`, `leadTimeMae` |
| 07:00 (long leads) | `plan_day` + park-day row | same as the 06:00 planner | 60 min | **d10, d14, d21, d30, d45, d60, d90** only; leads count from the same park-local date. An hour after 06:00 so the Europe burst is spread over two hours |
| 10, 12, 14, 16, 18 | `park_hourly` | same as above | 15 min | next 4 h only (UC1 + short UC2) plus the live anchor; only parks whose 06:00 capture found a curve for today |

`getParkPredictions` is the **served** read: CatBoost hourly with the PCN
override and §7.7 persistence blend applied where PCN answered. Each slot records
its source in `sources` (`p` = pcn_blend, `c` = catboost); `/plan/day` hours
record `m` measured (the hour-mean of that same served curve), `k` composed,
`l` climatology, `o` observed. `-` is a slot the payload did not carry.

Per curve row: `waits` (served q50 per slot), `bands` (served
`uncertaintyMinutes` per slot — CatBoost's q95 − q50; on plan_day the ride's one
band repeated), `day_peak` and `expected_error` (plan_day), `model_version`
(CatBoost version without `+pcn`), `ride_q90_56d` and `is_headliner`, plus:

- `live_wait` (park_hourly): the ride's latest STANDBY reading at the origin,
  OPERATING and no older than 30 min — the anchor of D1.
- `level_source` (plan_day, and on the park-day row): which model produced the
  day level the curve is built on — `tft`, `catboost`, `climatology` (a
  climatology day), on the park-day also `mixed` / `none`. The planner payload
  does not expose it, so it is reconstructed the way `PlanDayService.dayLevels`
  picks it: the `getServingDailyPredictions` row for that ride whose
  `predictedTime.slice(0, 10)` is the date, TFT rows carrying
  `modelVersion: "tft"`. Internal only, not in the API.

**Ex-ante busy** (`ride_q90_56d`) is the ride's q90 of 15-min **average** waits
(`attraction_hourly_history.slots[].avgWait ≥ 5`) over the 56 days before the
origin — BENCH-SPEC's busy segment (≥ 45 min), read from the rollup instead of a
`queue_data` scan, and stored as the number so the threshold can move. It is not
filtered to published windows, which the spec's truth is; the difference is the
rollup's, and it is the same for every model.

**Schedule:** one hourly job at :05 UTC reads each park's clock, so every
timezone (including half-hour offsets) gets its origin within the hour. A Redis
`NX` marker makes each park × origin hour capture once, whatever re-runs.

**Not archived:** the q80 crowd quantile (only `crowdLevel` is served).

## 2. Size and DB impact (measured 2026-10-09)

Inputs from production: 4,158 rides in 126 parks carry an hourly curve, 76.4
operating slots per ride on average within 48 h (max 192); `queue_data` writes
148 k STANDBY rows a day. Tuple sizes measured with `pg_column_size` on literal
rows: 592 B for a 76-slot row, 224 B for 16 slots, 208 B for an 11-hour
plan_day row.

| part | rows / day | written / day (incl. ~100 B of indexes per row) | lives | steady state |
| --- | --- | --- | --- | --- |
| park_hourly, 06:00 | ~10 k | ~4 MB | target + 14 d (~15 d) | ~60 MB |
| park_hourly, intraday | ≤ ~21 k | ≤ ~7 MB | ~14 d | ~95 MB |
| plan_day d0–d7 | ~30 k | ~10 MB | ~17.5 d avg | ~175 MB |
| plan_day d10–d90 | ~27 k | ~9 MB | lead + 14 d (Σ 368 row-days per ride) | ~460 MB |
| **curves total** | **~87 k** | **~29 MB** | | **~0.8 GB** |
| park days | ~3.2 k | ~0.8 MB | target + 180 d | ~140 MB |
| scores | ~1.5 k | ~0.5 MB | 400 d | ~200 MB |

Retention is by **target date** (14 days past it for curves), never by origin:
counted from the origin, a d90 row would be gone before its day is scored. The
steady state is dominated by long-lead rows waiting for their day — the price of
measuring d90 at all.

Writes are batched (≤ 1,000 rows per `INSERT … ON CONFLICT DO NOTHING`). The
heaviest capture hours are the two in which Europe reaches 06:00 and 07:00: ~90
parks × 8 `buildPlanDay` calls (06:00) and ~90 × 7 (07:00, long leads), on warm
caches, three parks at a time under the shared `DbJobBudget`; the queue has a
15-minute lock headroom. Per capture the extra reads are one
`getServingDailyPredictions` (single-flight, the call `buildPlanDay` makes
anyway) and, for served curves, one `DISTINCT ON` over the last 30 min of
`queue_data` for the live anchor.

**Not a hypertable, no foreign key.** At ~0.8 GB a plain table is fine, and
retention is an app-side `DELETE` in 20,000-row batches with `lock_timeout` 2 s
and `statement_timeout` 60 s — never a TimescaleDB policy, whose `drop_chunks`
locks every FK target ACCESS EXCLUSIVE ([runbook §0b](../troubleshooting/db-health-runbook.md)).
Every query the jobs run against `queue_data`, `attraction_hourly_history` and
the archive itself sets `statement_timeout`/`lock_timeout` locally
(`withStatementLimits`).

**Scoring cost:** per park and day, five indexed reads (curves via
`(park_id, target_date)`, park days, schedule, attractions) plus one `queue_data`
read of that park's STANDBY rows from 3 h before the first window to the last
close — yesterday's chunk, uncompressed, ~150 k rows across all parks. ~1,000
small queries once a day.

## 3. Truth (BENCH-SPEC)

15-min slots on the UTC quarter-hour grid (= the park-local grid for every
quarter-hour offset). A slot's value is the STANDBY row in force at its
midpoint — `queue_data` is a change log, so the last row at or before it, no
older than 3 h. It counts only when that row is OPERATING with a wait ≥ 5 and the
midpoint lies inside a published park-wide OPERATING window from
`schedule_entries` for that date (windows are instants, so DST and midnight
crossings need no special case). A ride-day's level is the P90 of its truth
slots (≥ 4); a park-day's level is the mean of its headliners' levels, divided by
the park's `typicalDayPeak` and bucketed with `determineCrowdLevel` — the
calendar's regime ([crowd-level rules](../rules/crowd-levels.md)). The
`typicalDayPeak` is today's value, not the one at the origin; it moves slowly.

A plan_day hour is compared with each of its four quarter-hours (as served — a
step; BENCH-SPEC's interpolated variant is left to the bench runner).

## 4. Scores

Daily at 12:00 UTC, when the previous UTC date is over in every park timezone;
up to three missed dates are caught up. One row per
`(target_date, region, use_case, lead, source, segment)` in
`forecast_archive_scores`, with **additive counters** in `sums` (so any window
pools by adding, then divides once). `region` ∈ EU / NA / ASIA / OTHER and an
explicit `ALL`. `segment` ∈ `all`, `busy` (ex-ante), `headliner`. `source` is
the slot source, `mixed` for a ride-day with several, or `all`.

| use case | lead | what |
| --- | --- | --- |
| UC1 | `h0-1`, `h1-2` (slot lead after origin) | MAE, bias, band coverage — any origin |
| UC2 | `h2-6`, `h6-12`, `h12-24`, `h24-48` | same |
| D1 | `h0-2` (all origins with a live anchor) and the trigger slot's `h0-1` / `h1-2` × source | next-best-ride: a suggestion is made when live ≥ 10 min below the forecast anywhere in the next 120 min; **precision** = the ride's truth reached live + 10 within those 120 min; false-suggestion rate = 1 − precision; **base rate** = the same outcome among origins with no suggestion, so precision can be read against chance |
| UC2 | `d0` … `d2` (06:00 origin) | D3 best time: hit rate (true best slot among the predicted top-2), hit within ±30 min of the predicted best, **regret** (truth at predicted best − true minimum); slot Spearman |
| UC3 | `d0` … `d7`, `d10`, `d14`, `d21`, `d30`, `d45`, `d60`, `d90` | slot MAE/bias/band coverage; D3 per ride-day; dayPeak MAE and bias vs truth P90; **D8**: stated `expectedError` vs realised \|dayPeak error\| on the same ride-days, and the share of ride-days that carry a stated error at all; park-day dayPeak rank Spearman and pairwise ordering (pairs ≥ 5 min apart; D4's ordering half) |
| D6 | same leads, source `predicted` (calendar `predictedCrowdLevel`) or `plan_day` (`context.crowdLevel`) | exact / ±1 bucket accuracy, busy-day (high+) recall and precision, `unknownPred`, cross-park pairwise ordering on the same date |
| D9 | same leads, source = plan tier | rides that operated (≥ 1 truth slot) that the plan offered; offered rides that never operated; park-days with operating rides but an empty plan |

UC3 and D9 rows are also written under `source = level_<tft|catboost|climatology|mixed|none>`
— the model behind the day level — so the board splits the horizon curve by
level source (TFT covers ≤ 60 days, CatBoost beyond).

D8 band coverage is `truth ≤ pred + band` over slots with a band; it is the q95
upper side as served, and the CatBoost band is known to cover only 53–57 %
([quantile serving](./quantile-serving-and-calibration.md)). `bandFieldCoverage`
is the share of slots that carry a band at all.

## 5. Admin board

`GET /v1/admin/forecast-archive?days=14&region=ALL` (admin auth, read-only,
cached 2 h; scoring evicts it). Returns the pooled counters and derived metrics
per use case × lead × source × segment over the last `days` scored dates, plus
the last 24 h of archive rows per surface and origin kind and the first origin.

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
  month (UC4), which the 8-day park-day rows do not cover.
- **UC4** (daily per park out to d365) stays with `prediction_lead_snapshots`.
- D6 is not split by level source (the calendar's `predictedCrowdLevel` mixes
  both models within a park-day).
- Bootstrap CIs over park-days: the scores are pooled per region; the raw
  archive lets the bench runner recompute per park-day when a paired comparison
  needs a CI (raw curves live 14 days past their target date).
- Park merges: `forecast_archive_curves` rows move with a merged ride
  (`merge-dependencies.ts`); `park_id` is not reparented on a park merge — the
  rows age out 14 days after their target date.
