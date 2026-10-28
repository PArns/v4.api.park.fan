# Forward archive — what is captured, and what it costs

Which curves the hourly `capture` job archives at which park-local origin, the columns each row
carries, and the measured size and database impact. Open it before adding a surface or a column
to `forecast_archive_curves`, or when the capture job is slow.

## Captured as served, never re-implemented

The archive calls the same methods the endpoints call (`MLService.getParkPredictions`,
`PlanDayService.buildPlanDay`, `CalendarService.buildCalendarResponse`); a second implementation
would measure itself.

| Origin (park-local) | Surface (id) | Method | Resolution | Content |
| --- | --- | --- | --- | --- |
| 06:00 | `park_hourly` (1) | `getParkPredictions(park, "hourly")` | 15 min | next 48 h, one row per ride × operating day |
| 06:00 | `plan_day` (2) | `buildPlanDay(park, date)` | 60 min, served as a step (15 min under the H5 composer) | d0 … d7, one row per ride |
| 06:00 | park-day row | `buildPlanDay` + `buildCalendarResponse` | per park-day | tier, `crowdLevel` (+ whether it was a fallback), `predictedCrowdLevel`, `typicalDayPeak` at the origin, hours, `ridesOffered`, `ridesUnavailable`, `accuracy`, `leadTimeMae`, level source |
| 07:00 | `plan_day` + park-day row | as at 06:00 | 60 min | **d10, d14, d21, d30, d45, d60, d90** only (an hour later to spread Europe's burst) |
| 06:00 / 07:00 | `plan_day_shadow_peak` (3), `plan_day_shadow_h5` (4), `plan_day_shadow_routed` (5) | `buildPlanDayWithShadow` | 15 min | the composers side by side on the same quarter-hours, whatever `PLAN_DAY_H5_COMPOSER` says |
| 06:00 | `chronos_grid` (7, shadow) | nf-service `POST /chronos/forecast` `variant=grid` | 15 min | d0 … d7 — [chronos-shadow.md](chronos-shadow.md) |
| 10, 12, 14, 16, 18 | `park_hourly` (1) | as above | 15 min | next 4 h, with the live anchor |
| 10 … 18 | `plan_day` D1 row | `buildPlanDay(park, today)` | 60 min | per planned ride: the plan hours of the 2-h look-ahead, the frontend's next-best-ride "later" value and the live anchor — feeds D1 only |
| 10 … 18 | `chronos_live` (6, shadow) | nf-service `variant=live` | 15 min | next 4 h; also feeds the serving cache ([chronos-serving.md](chronos-serving.md)) |
| 10 … 18, only while `SERVE_CHRONOS_INTRADAY` is on | `park_hourly_incumbent` (8, not served) | `getParkPredictions(…, { chronos: false })` | 15 min | what `park_hourly` would show without Chronos-2 |

Intraday origins run only for parks whose 06:00 capture found a curve for today, so a closed park
costs nothing all day. The q80 crowd quantile is not archived (only `crowdLevel` is served).

**Source codes.** `getParkPredictions` is the served read: CatBoost hourly with the PCN override
and the persistence blend where PCN answered. Each slot records its source in `sources`: `p`
pcn_blend, `c` catboost, `s` chronos2_served (PAR-911). `/plan/day` hours record `m` measured
(the hour-mean of that served curve), `k` composed (old composer: hourly P50 stretched to the
level), `h` `composed_h5` (H5, no level), `t` `composed_h5_tft` (H5 × TFT level ÷ the ride's 56-day
P90), `l` climatology, `o` observed; `-` is a slot the payload did not carry. With
`PLAN_DAY_H5_COMPOSER=true` a planner ride carrying `slots` is archived at 15 minutes
(`slot_minutes = 15`) exactly as the frontend reads it.

**Composer shadow (PAR-834).** Surfaces 3–5 hold, per ride with both an old-composer curve and an
H5 profile, the old composer (its hour repeated over four quarters), plain H5, and H5 as the flag
would serve it (`h5LevelFor`'s routing — a level only on busy rides d0–d2 since PAR-901). Where
surface 5 is missing the scorer reads surface 4 under its name, so the three always cover the
same ride-days. They are scored only under use case `UC3S` (sources `shadow_peak_scaled`,
`shadow_h5`, `shadow_h5_routed`), never into `all` or `UC3`.

## Columns per curve row

`waits` (served q50 per slot), `day_peak` and `expected_error` (plan_day), `model_version`
(CatBoost version without `+pcn`), `ride_q90_56d`, `is_headliner`, and:

- `bands` (park_hourly): served `uncertaintyMinutes` per slot — CatBoost's q95 − q50, one-sided.
  PCN-served slots carry `null` since PAR-1029, so with `SERVE_PCN_INTRADAY` on the band
  population is the CatBoost- and Chronos-served slots only.
- `peak_band` (plan_day): the ride's served `uncertaintyMinutes` on the planner — a one-sided upper
  half-width around `dayPeak` (p95 of `actual − predicted peak`, from `forecast-accuracy.service.ts`),
  scored per ride-day and never pooled with the per-slot band.
- `live_wait`, `live_age_min` (park_hourly, D1 rows): the STANDBY reading in force at the origin
  (last change-log row at or before it, ≤ 3 h old, OPERATING with a wait) and its age. A "row in
  the last 30 min" rule missed half the operating rides, because `queue_data` writes on change only.
- `later_wait`, `later_at` (D1 rows): the frontend's next-best-ride value.
- `level_source` (plan_day and park-day row, internal only): `tft`, `catboost`, `climatology`
  (park-day also `mixed` / `none`), reconstructed the way `PlanDayService.dayLevels` picks it from
  the `getServingDailyPredictions` row whose `predictedTime.slice(0, 10)` is the date.
- `catboost_day_peak` (plan_day, PAR-1042): on a TFT-levelled ride-day, CatBoost's day level for the
  same ride-day — the `dayPeak` the planner would have shown had CatBoost levelled the day. Read from
  the same cached daily answer `getServingDailyPredictions` starts from; NULL on every other row.
- `driver_day_peak` (plan_day, PAR-1058): the driver-level shadow for the ride-day on every level
  source, from `driver_level_forecasts` written at or before the origin, rounded; NULL where the
  nightly job has no row (the capture never fails on it). See
  [model-comparison.md](model-comparison.md#the-shadow-levels).

**Operating day.** A served 15-minute slot before 04:00 park-local belongs to the previous date,
so a park open until 01:00 keeps its evening in one row.

**Ex-ante busy** (`ride_q90_56d`): the ride's q90 of time-weighted 15-minute waits inside the
published windows over the 56 days before the origin (BENCH-SPEC's busy segment is ≥ 45 min; the
number is stored so the threshold can move). Read from `attraction_hourly_history`, forward-filled
(≤ 3 h) per ride-day onto the quarter-hour grid of that day's published OPERATING window from the
park's opening; a window past midnight is cut at 23:59; a day without a published window
contributes nothing. 228 ms for Europa-Park.

## Origin, schedule and failure handling

- One hourly job at :05 UTC reads each park's clock, so every timezone (incl. half-hour offsets)
  gets its origin within the hour. Each park's `origin_at` is read immediately before its own
  capture.
- A park × origin hour is marked done in Redis only after its capture succeeded. A park whose
  capture failed misses that origin; the marker only stops a re-run inside the hour from capturing
  twice. A park's curves and park-day rows are written in one transaction.
- A plan that throws is an empty plan: the park-day row is still written with `ridesOffered = 0`
  and `rides_unavailable = capture_error`, so D9 counts the day.
- Three parks at a time under the shared `DbJobBudget`, 15-minute lock headroom. Writes are batched
  (≤ 1,000 rows per `INSERT … ON CONFLICT DO NOTHING`). Every query against `queue_data`,
  `attraction_hourly_history`, `schedule_entries` and the archive goes through `queryWithLimits`
  (`src/common/utils/statement-limits.util.ts`).

## Size and DB impact (measured 2026-10-09)

Inputs: 4,158 rides in 126 parks with an hourly curve, 76.4 operating slots per ride on average
within 48 h; 50 of the 211 parks in Europe. Tuple sizes: 592 B for a 76-slot row, 224 B for 16
slots, 208 B for an 11-hour plan_day row.

| Part | Rows / day | Written / day (incl. indexes) | Lives | Steady state |
| --- | --- | --- | --- | --- |
| park_hourly, 06:00 | ~10 k | ~4 MB | target + 14 d | ~60 MB |
| park_hourly, intraday | ≤ ~21 k | ≤ ~7 MB | ~14 d | ~95 MB |
| plan_day D1 rows | ≤ ~20 k | ≤ ~5 MB | ~14 d | ~70 MB |
| plan_day d0–d7 | ~30 k | ~10 MB | ~17.5 d avg | ~175 MB |
| plan_day d10–d90 | ~27 k | ~9 MB | lead + 14 d | ~460 MB |
| **curves total** | **~107 k** | **~35 MB** | | **~0.85 GB** |
| park days | ~3.2 k | ~0.8 MB | target + 180 d | ~140 MB |
| scores | ~1.5 k | ~0.5 MB | 400 d | ~200 MB |
| paired (model comparison) | ~21 k | — | 120 d | ~330 MB (estimate) |

Added since, estimated: the composer shadow ≤ ~120 k rows / ~43 MB a day (≤ ~1.5 GB steady state,
two thirds the d10–d90 rows waiting for their day; capping it at d30 would cut that to about a
third); Chronos-2 ≤ ~50 k rows / ~18 MB a day (+0.35 GB); surface 8 ~+5 k rows a day while
Chronos-2 is served; `catboost_day_peak` and `driver_day_peak` a few kB a day.

**Retention is by target date** (curves 14 days past it, `CURVE_RETENTION_AFTER_TARGET_DAYS`;
park days 180, scores 400, paired 120 days), never by origin — counted from the origin a d90 row
would be gone before its day is scored. Retention is an app-side `DELETE` in 20,000-row batches
with `lock_timeout` 2 s and `statement_timeout` 60 s, never a TimescaleDB policy, whose
`drop_chunks` locks every FK target ACCESS EXCLUSIVE
([TimescaleDB jobs and lock queues](../../troubleshooting/timescaledb-lock-queues.md)). Hence also no foreign key and no
hypertable.

**Load.** The heaviest hours are the two in which Europe's 50 parks reach 06:00 and 07:00
(~50 × 8 and ~50 × 7 `buildPlanDay` calls on warm caches). Each intraday origin adds one
`buildPlanDay` per open park. Per capture: one `getServingDailyPredictions` (single-flight, the
call `buildPlanDay` makes anyway; on the warm path a `parks` PK lookup plus a Redis GET), one
`DISTINCT ON` over the last 3 h of `queue_data` for the live anchor, and once a day per park the
56-day q90 (cached).

**Park merges:** curve rows move with a merged ride (`merge-dependencies.ts`); `park_id` is not
reparented on a park merge — the rows age out 14 days after their target date.
