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

| 06:00 daily | `chronos_grid` (surface 7, **shadow**) | nf-service `POST /chronos/forecast` `variant=grid` | 15 min | d0 … d7, Chronos-2 never served — see §8 |
| 10, 12, 14, 16, 18 | `chronos_live` (surface 6, **shadow**) | nf-service `POST /chronos/forecast` `variant=live` | 15 min | next 4 h, with the live anchor — see §8; also feeds the Chronos serving cache (§10) |
| 10, 12, 14, 16, 18, only while `SERVE_CHRONOS_INTRADAY` is on | `park_hourly_incumbent` (surface 8, not served) | `MLService.getParkPredictions(…, { chronos: false })` | 15 min | next 4 h: what `park_hourly` would show without Chronos, the live A/B's reference once Chronos is served — see §10.5 |

Intraday origins run only for parks whose 06:00 capture found a curve for today,
so a closed park costs nothing all day.

`getParkPredictions` is the **served** read: CatBoost hourly with the PCN
override and §7.7 persistence blend applied where PCN answered. Each slot records
its source in `sources` (`p` = pcn_blend, `c` = catboost, `s` = chronos2_served since PAR-911); `/plan/day` hours
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

**Composer shadow (PAR-834), whatever the flag says.** The 06:00 / 07:00 captures
call `PlanDayService.buildPlanDayWithShadow` and also write, per ride that has
both an old-composer curve and an H5 profile, the composers side by side on the
SAME quarter-hours: surface 3 `plan_day_shadow_peak` (old composer, its hour
repeated across four quarters, `k`), 4 `plan_day_shadow_h5` (plain H5, `h`),
5 `plan_day_shadow_routed` (H5 as the flag would serve it — `h5LevelFor`'s
routing, `t`, the same `h5LevelFor` call the served path makes). Surface 5 is
written only where a level was applied (busy rides, d0–d2 since PAR-901, d0–d6
before); where it is missing the scorer reads surface 4 under its name too, so the
three always cover the same ride-days. Scored under use case **`UC3S`**, sources
`shadow_peak_scaled` / `shadow_h5` / `shadow_h5_routed`, never into `all` and
never into `UC3` — slot MAE/bias and the ride-day shape metrics (best-time
regret, Spearman); no dayPeak or coverage, which do not depend on the composer.
Measured hours play no part: this compares composers, not the hourly model.

Added size (estimate from §2's measured row counts, not yet measured): at most
two shadow rows per planner ride and lead (~57 k planner rows a day) plus the
routed row for busy rides d0–d6 (~8 k; since PAR-901 only d0–d2, 3 of those 7
leads, so ~3.4 k) — **≤ ~120 k rows / ~43 MB a day**
(~0.35 KB per 15-minute row with its index entry), steady state **≤ ~1.5 GB**,
two thirds of it the d10–d90 rows waiting for their day. If that is too much,
capping the shadow at d30 drops the long-lead share (d45/d60/d90 hold most of the
row-days) to roughly a third.

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

**Heartbeat rows are not truth (PAR-1024).** The hourly heartbeat writer copies
the last status *and* wait with a fresh timestamp, which resets the 3 h staleness
clock and keeps a frozen value alive while the feed is silent — persistence then
scores 0 on exactly those slots. Truth reads `NOT COALESCE(is_heartbeat, false)`
(`truthRowsSql`), the same filter as ml-bench `--drop-heartbeats`; NULL is a row
written before the column existed. The live anchor (§2) still reads them: it is
what the app shows. Other boards that read `queue_data` themselves (the
`prediction_accuracy` performers) are not changed by this.

**Paired slot waits are compared as served (PAR-1024).** `PairedAccumulator`
puts both arms of a UC1/UC2/UC3 slot through `roundServedWait` (5-minute steps,
floor 10) — what any candidate would go through once served. Before, persistence
and Chronos-2 were scored raw against a rounded served curve and won partly by
rounding. That is a choice with a cost: persistence is judged as if it were the
served forecast, so a live 5 counts as 10, although the app shows the live wait
itself unrounded. `plan_day` is archived at 1-minute precision and is rounded
here like its candidates. D3 regret and PEAK stay unrounded, and so does the
unpaired board (`ScoreAccumulator`) — its per-source MAEs are not the matrix's.

**One definition per pooled window (PAR-1024).** `forecast_archive_paired.definition`
records the scoring definition a row was written under (`PAIRED_DEFINITION`, now
2; older rows read 1). The matrix, its `since` dates and the Chronos section read
the current definition only, so every live gate restarts at the deploy instead of
being met on rows scored the old way — except for what the **replay** restores:
curves live 14 days past their target date, and they are the forecasts as they
were made, so `scoreDue` rescores every date of that window whose paired rows
carry an older definition (oldest first, 5 per run, `scoreDate` is
delete-then-insert per date). Three daily runs after a deploy, the last 14 dates
are back under the new definition with no retraining; dates older than the curve
retention cannot be replayed and stay out of the matrix. A replayed date is written only
when every park scored (a failed park keeps the old rows and the date is retried
the next day). It is scored with today's headliner set, attraction list and
`schedule_entries`, not those of the target date. While the replay runs, `since`
is already the oldest replayed date with gaps after it, so the "frühestens" dates
read a few days late until it finishes.

**The unpaired board has a step at the deploy.** Dropping heartbeats changes
`forecast_archive_scores` too (UC MAEs and the D-scores, D6 included), and those
rows carry no definition: the replayed 14 dates are rewritten, everything older
keeps the old truth, and the board's 28/56-day window mixes the two until it has
moved past the deploy.

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

Since PAR-1029 a PCN-served slot carries `uncertaintyMinutes: null` (PCN publishes
no q0.95, and CatBoost's band described a different forecast), so with
`SERVE_PCN_INTRADAY` on the park_hourly band population (`nBand`) is the
CatBoost- and Chronos-served slots only — much smaller than before, and not the
same slots as the MAE population. The same null narrows `/plan/day`'s "widest
band of the day" (`plan-day.service.ts`, the max over the served hourly bands —
the fallback when the day-level row has no band) to the remaining CatBoost/Chronos
slots, and to null when PCN served them all.

## 5. Admin board

`GET /v1/admin/forecast-archive?days=14&region=ALL` (admin auth, read-only,
cached 2 h). The cache key carries a version that every scoring run increments,
so all `days` × region variants are retired at once. Returns the pooled counters
and derived metrics per use case × lead × source × segment over the last `days`
scored dates, plus the last 24 h of archive rows per surface (`surfaceName`) and
origin kind and the first origin, plus the `chronos` section described in §8.4.

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

## 8. Chronos-2 shadow (PAR-852)

**What it answers:** does the zero-shot foundation model that won parts of the
PAR-828 offline benchmark also win on live, leakage-free forecasts? The owner's
rule for replacing a model needs (1) an offline win, (2) a live confirmation in
this archive — ≥ 14 days and ≥ 10 parks per lead — and (3) a visible effect on a
user surface. This section is (2). **Nothing here is served to a user** — until
`SERVE_CHRONOS_INTRADAY` is switched on (§10, PAR-911): the only reader of
nf-service `/chronos/forecast` is the archive capture, and the curves it reads
are what serving would use.

### 8.1 Model and variants

`amazon/chronos-2`, **Apache-2.0** for code and weights (model card
`license: apache-2.0`; `chronos-forecasting==2.3.2`, the version the bench
scored; HF revision pinned in `nf-service/config.py`). Zero-shot, no training.
The two bench variants that won (`ml-bench/mlbench/models/foundation.py` on the
PAR-828 branch), ported without the bench harness to
`nf-service/chronos_shadow.py`:

| surface | bench name | origin | context | horizon asked | archived |
| --- | --- | --- | --- | --- | --- |
| 6 `chronos_live` | `chronos2` | 10, 12, 14, 16, 18 (intraday) | the park's last 8192 15-min slots inside PUBLISHED windows (~170 operating days), nights left out | every window slot of today + tomorrow (as the bench's intraday origins asked) | next 4 h |
| 7 `chronos_grid` | `chronos2_grid` | 06:00 (daily) | regular 15-min UTC grid, last 8192 steps (~85 days), NaN off-window | grid steps to the last window close of d7 (≤ 1024) | d0 … d7 |

A park is one multivariate task (every ride a target variate, covariates
shared). Rides forecast: those with a truth slot in the 14 days before the
origin (the bench forecast the rides it was asked about; this is the ex-ante
equivalent). Quantiles 0.5 / 0.8 / 0.95, clipped at 0 and made monotone; only
future slots inside a published window are returned — a closed hour gets no
value. `waits` = q50, `bands` = q95 − q50 (one-sided, like the served band);
the grid row's `dayPeak` is the P90 of its in-window q50 (the truth's ride-day
statistic), so `dayPeakMae` compares with the planner's.

### 8.2 Covariates and where each comes from

The bench's, unchanged — `chronos2_nocov` lost 0.39–0.82 min. No weather: it
was oracle-only (actuals) in the bench and worth < 0.12 min.

| covariate | production source |
| --- | --- |
| `ko_min`, `kc_min` | minutes since the published opening / until the published closing: `schedule_entries`, park-wide `OPERATING` rows, per service day earliest opening and latest closing (future days: as published at the origin) |
| `in_win` (grid only) | 1 inside such a window, else 0 |
| `tod` | park-local hour + minute / 60 (`parks.timezone`) |
| `is_weekend`, `dow_sin`, `dow_cos` | park-local date, pandas `dayofweek` (0 = Monday — not Postgres' 0 = Sunday) |
| `is_holiday_primary`, `is_school_holiday_primary`, `holiday_count_total`, `school_holiday_count_total`, `is_school_holiday_any` | `holidays` table via `holiday_features.park_day_holiday_features` — THE ml-service implementation (OR semantics, neighbour regions from `parks.influencingRegions`, Easter Sunday). Vendored into nf-service byte-identical; `tests/test_chronos_shadow.py` compares the files |
| `is_bridge_day` | derived from `is_holiday_primary` of the neighbouring calendar days (Mon before a Tue holiday, Fri after a Thu holiday, never the holiday itself) — the bench's definition |

**A missing covariate is recorded, not zero.** Each curve says whether its
day's holiday flags rested on data: `none` (no `holidays` row for the park's
country — every flag reads 0), `short` (the country's last row is before the
day), `full`. The archive keeps it in `model_version`
(`chronos-2@29ec376;hol=short`). Schedule covariates cannot be missing: a day
without a published window is not forecast at all.

### 8.3 Information cut and data reads

Truth (the context) is BENCH truth — the STANDBY reading in force at each slot
midpoint, OPERATING, ≥ 5, inside the published window — and enters only for
slots that **ended by the origin**; today's readings are read up to the origin
instant. Schedule and holidays are known-future inputs read at the origin.

nf-service caches each park's truth slots of complete past days as parquet on
the nf-models volume (`/app/models/chronos-truth`), so a warm capture reads one
new day of `queue_data` once a day plus today's rows. A gap of more than
`CHRONOS_INLINE_FILL_DAYS` (3) goes to ONE background thread that backfills park
after park in 30-day reads (`statement_timeout` 120 s, `lock_timeout` 5 s), and
the request answers `status: "warming"` at once. Cold start after deploy: the
context reaches ~343 days back (compressed) — one-off, then incremental.

### 8.4 Scoring and the board

The scorer treats surface 6 like `park_hourly` (UC1/UC2 slot leads, band
coverage) and surface 7 like `plan_day` slots (UC3 `d0` … `d7`, D3, Spearman,
dayPeak) under the sources `chronos2` / `chronos2_grid` — **never into `all`,
`level_*`, D1, D9 or the dayPeak ordering**, which judge what is served.

`scorePairedShadows` pairs each shadow slot with a reference **from the same
origin instant** where truth, shadow and reference all have a value:

| shadow | reference | what |
| --- | --- | --- |
| `chronos2` | `served` | `park_hourly` (CatBoost / PCN blend as served) — or, once Chronos is served, `park_hourly_incumbent` (§10.5) |
| `chronos2` | `persistence` | the live wait in force at the origin, held flat |
| `chronos2_grid` | `served` | `plan_day` (its hour value on each quarter-hour) |
| `chronos2_grid` | `h5` | the composer shadow's plain H5 curve (surface 4) |

The sums go per **park and day** to `forecast_archive_paired` (key: target date,
park, use case, lead, model, reference, segment; `n`, Σ|e| and Σe of both;
~8 k rows a day, 120 days, replaced with the date like the scores).

`GET /v1/admin/forecast-archive` returns `chronos`:

```jsonc
{
  "captureEnabled": true,                 // the API's CHRONOS_SHADOW_ENABLED
  "method": { "diff": "model MAE − reference MAE on identical slots, minutes; negative = Chronos better",
              "ci": 0.95, "bootstrap": "park-cluster (parks resampled with replacement)",
              "reps": 1000, "seed": 852, "minDays": 14, "minParks": 10 },
  "rows": [{
    "useCase": "UC1", "lead": "h0-1", "model": "chronos2", "surface": "chronos_live", "segment": "all",
    "mae": 9.1, "bias": -1.2, "n": 41234, "days": 14,   // own coverage, from the pooled scores
    "bandCoverage": 0.82, "dayPeakMae": null,
    "comparisons": [{
      "reference": "served", "n": 40110, "days": 14, "parks": 61,
      "modelMae": 9.0, "referenceMae": 10.4, "modelBias": -1.1, "referenceBias": -3.0,
      "diff": -1.4, "ciLow": -2.1, "ciHigh": -0.8,
      "enoughData": true, "verdict": "chronos_better"   // | reference_better | no_significant_difference | insufficient_data
    }]
  }]
}
```

The CI resamples **parks**, not park-days: days of one park are not
independent, and the bench (PAR-828) found the park-day interval too narrow.
`diff` pools first ((Σ|e_model| − Σ|e_ref|) / Σn), as the scores do. With
fewer than 2 parks there is no CI; with fewer than 10 parks reaching 14 days the verdict is
`insufficient_data` whatever the interval says. `rows` (§5) carry the same
shadow sources for every other metric (D3 regret, Spearman, …), unpaired.

### 8.5 Safety, cost and switches

- **Kill switches:** `CHRONOS_SHADOW_ENABLED=false` on the API (no call at all);
  `CHRONOS_ENABLED=false` on nf-service (503). Both default on.
- **Isolation:** the call starts first in each park's capture and overlaps the
  served reads; any error or timeout (45 s) returns nothing and the other
  surfaces are written as before. After 3 failures in one hourly run the
  remaining parks skip the call (a hung service costs a run ≤ 3 timeouts, well
  inside the 15-minute lock). A 503 is a skip, not a failure. One warning per
  run, never one per park.
- **TFT training:** `/train` unloads the weights first and the endpoint answers
  503 while a run is in progress — the GPU and the container's 14 GB are the
  training's. Origins at 03:00 UTC (06:00 in UTC+3) lose their shadow on those
  ~11 minutes.
- **Footprint:** weights 0.48 GB on the volume (downloaded at first use to
  `/app/models/hf-cache`); ~1.8 GB VRAM at peak on a 30-ride park (bench), capped
  at 25 % of the GPU per process (`CHRONOS_VRAM_FRACTION`); the image grows by
  ~18 MB of wheels (transformers, tokenizers, accelerate, safetensors, …, ~80 MB
  installed). One forward pass per park and origin: measured 2.9 s (live, 96
  steps) and 3.2 s (grid, 744 steps) for a 40-ride park with the full 8192-step
  context on a laptop CPU (Apple silicon, model resident); the bench saw ~5 s
  per 30-park batch on the RTX 5080. ~50 European parks at 06:00 add one pass
  each, overlapping the served reads of the same capture.
- **Archive size:** at most ~4 k rides × (1 grid row × 8 leads + 5 live rows) a
  day ≈ 50 k rows / ~18 MB (15-min rows, ~0.35 KB with index), living 14 days
  past their target ≈ +0.35 GB steady state.

### 8.6 When the first scores appear

The first capture hour after deploy starts the backfill; each park's first
shadow rows appear once its context is cached (usually the same day). Day *D* is
scored at 12:00 UTC on *D + 1*, so the first `chronos2` (UC1/UC2) and
`chronos2_grid` `d0` scores appear at the first 12:00 UTC scoring after a full
day of captures; `dN` after *N* + 1 days. The verdict needs 14 scored days.

## 9. Model comparison for the admin (PAR-855)

**What it answers:** for every question the app answers, does any model beat
**what is served today** for that question? One matrix, every model, live from
this archive plus the offline verdicts of the ml-bench runs — the owner's
switch rule made visible: (O) offline better over the whole year, (L) live
≥ 14 days and ≥ 10 parks with the CI below zero, (S) visible in the app.

Code: `src/ml/utils/forecast-archive-scoring.util.ts` (`scorePairedModels`),
`src/ml/utils/weekday-average.util.ts`, `src/ml/utils/model-comparison.util.ts`
(questions, state rule, notes, dates),
`src/parks/services/model-comparison.service.ts`, the contract in
`src/ml/dto/model-comparison.dto.ts`, the offline verdicts in
`src/ml/data/offline-model-verdicts.json`. Frontend: park.fan PAR-856.

The admin ML overview ([ml-overview.md](ml-overview.md), PAR-861) reads this
matrix's summary and derives its open decisions from named cells of it.

### 9.1 Generalised paired scoring

`scorePairedModels` runs inside the existing 12:00 UTC scoring next to
`scorePairedShadows` and writes into the same `forecast_archive_paired`. Every
row pairs one model with **`reference = served`** on identical truth slots (or
ride-days) of the **same origin instant**. No key it writes is one
`scorePairedShadows` writes (a spec asserts it), so PAR-852's Chronos rows are
unchanged; the Chronos board section now reads its two models only.

| model | served reference | use case × lead | origins |
| --- | --- | --- | --- |
| `persistence` (live wait at the origin, held flat) | `park_hourly` | UC1/UC2 slot leads | all |
| `shadow_h5`, `shadow_h5_routed`, `shadow_peak_scaled`, `chronos2_grid` | `park_hourly` | UC1/UC2 slot leads | 06:00 |
| `shadow_h5`, `shadow_h5_routed`, `shadow_peak_scaled`, `weekday_avg` | `plan_day` | UC3 dN | 06:00 / 07:00 |
| the same four + `chronos2_grid` | `plan_day` | `D3` dN: best-time regret per ride-day, both judged on the same slots (`n` = ride-days, Σ\|e\| = Σ regret) | 06:00 / 07:00 |
| `weekday_avg` | `plan_day` `dayPeak` | `PEAK` dN: \|day peak − truth P90\| per ride-day, `all` and `headliner` | 06:00 / 07:00 |
| `chronos2` | `park_hourly` | UC1/UC2 — PAR-852's row, read as is | intraday |

Segment `all` only (plus `headliner` for PEAK): the matrix shows nothing else.
The routed H5 curve falls back to plain H5 where it was not archived, as in the
scores.

**Weekday average (`weekday_avg`).** The ex-ante yardstick: per ride and
park-local quarter-hour, the mean over the **8 most recent same-weekday dates
strictly before the origin's park-local date** of that date's wait — nothing
at or after the origin's date, not even the morning the 06:00 origin has not
seen yet (the util applies the guard itself; a spec pins it). It needs ≥ 2
dates per quarter-hour. It is computed **at scoring time** from the
`attraction_hourly_history` rollup — the per-quarter-hour mean of OPERATING
STANDBY readings ≥ 5, i.e. a `queue_data` aggregate — forward-filled ≤ 3 h per
day like the truth. The read is bounded: exactly the dates D − 7k for
k = 1 … ⌊max lead / 7⌋ + 8 (≤ 20 dates for d90), one indexed query per park
and scoring date, measured **15 ms** on production for Europa-Park (~95 rides ×
8 dates). Its day peak is the P90 of its values over the served curve's span.

**Size.** ~165 rows per park-day (segment `all`), ~21 k a day over ~126 parks,
~2.5 M rows / ~330 MB at the 120-day retention (estimate, not measured). The
hourly capture is untouched: everything here happens in the scoring job.

### 9.2 Questions

| id | group | label | served | use case × leads | metric |
| --- | --- | --- | --- | --- | --- |
| `live_h0_1` | live | Wartezeit in der nächsten Stunde | `pcn` | UC1 h0-1 | slot MAE |
| `live_h1_2` | live | Wartezeit in 1 bis 2 Stunden | `pcn` | UC1 h1-2 | slot MAE |
| `today_h2_6` | today | Heute, 2 bis 6 Stunden später | `pcn` | UC2 h2-6 | slot MAE |
| `today_h6_12` | today | Heute, 6 bis 12 Stunden später | `pcn` | UC2 h6-12 | slot MAE |
| `plan_d0_1` | planner | Geplanter Tag ist heute oder morgen | `served` | UC3 d0, d1 | slot MAE |
| `plan_d2_5` | planner | Geplanter Tag ist in 2 bis 5 Tagen | `served` | UC3 d2–d5 | slot MAE |
| `plan_d6_60` | planner | Geplanter Tag ist in 6 Tagen bis 2 Monaten | `served` | UC3 d6, d7, d10, d14, d21, d30, d45, d60 | slot MAE |
| `plan_besttime` | planner | Beste Besuchszeit (bis 6 Wochen) | `served` | D3 d0–d7, d10, d14, d21, d30 | best-time regret |
| `cal_busier` | calendar | Welcher Tag ist voller? | `tft` | PEAK d0–d7, d10, d14, d21 (headliners) | day-peak error |
| `cal_far` | calendar | Kalendertage 1 bis 3 Monate voraus vergleichen | `tft` | PEAK d30, d45, d60, d90 (headliners) | day-peak error |

Every question also carries `leadLabel`, the forecast distance in plain German
("Prognose 2–5 Tage vorher, morgens um 06:05 gespeichert"). Every live metric
is minutes, lower is better (`metric.unit = "min"`). The calendar questions
read the planner's per-ride `dayPeak` — built on the same daily level the
calendar reads (TFT ≤ 60 days, CatBoost beyond) — against the truth P90; the
calendar's crowd level itself is not archived per model. **CatBoost** and
**Toto 2.0 / TiRex** have no archived curve, so their cells are offline only
(`checks.live = "open"`). Offline evidence for `cal_far` is a hit rate (+0.026
for TFT); it stays in the note, never in `offline.diff`, which would then be in
another unit than the live figure.

### 9.3 Cells

Per cell the question's leads × the column's archived model names are pooled
per park (Σ minutes, then divide once) and PAR-852's park-cluster bootstrap
(1,000 reps, per-cell seed) draws the 95 % CI of `diff = modelMae −
referenceMae`, **negative = better than served**.

State rule: the served model is `today`. A live comparison that meets the gate
(≥ 10 parks with ≥ 14 scored dates **each** — `gateDays`, the 10th-best park's
count; a union of dates over parks let one park × 14 days plus nine × 1 day pass,
PAR-1024) decides — CI entirely > 0 `worse`, else `unclear`, and CI entirely < 0
`better` **only when the offline verdict is `better` too** (owner rule: offline
AND live). A live win while the offline run is still going is `pending`; one
offline does not share is `unclear`; the note says which. The PAR-852 Chronos
section's `chronos_better` stays a live-only verdict and can read better where the
matrix says `unclear`. The "frühestens" dates assume the 10th-best park is scored
every day, so parks closing for the season make them optimistic. Below the gate the offline verdict speaks: offline `better` or
a run in progress → `pending` (waiting for live confirmation), offline `worse`
→ `worse`, otherwise `unclear`.

`checks`: `offline` yes / no / running / open from the verdict file; `live`
yes / no once the gate is met, `running` before, `open` when not archived;
`visible` = the served model, or a confirmed live win of ≥ 1 minute.
`expectedBy` (Europe/Berlin dates): for a live gate not met, lastDate + the
missing days + 1 (a date is scored at 12:00 UTC the next day) — or, when only
parks are missing, the next scoring; for an offline run in progress the file's
`expectedBy`; the earlier of the two. `live.since` is the first scored date of
that model × question over the whole table (cached per scoring run). `live` is
null with fewer than two parks (no CI). `note` is generated from the numbers
("1,3 Minuten besser als heute, sicher. 18 Tage, 34 Parks."); below the gate it
is the offline note plus the live figures so far.

**Offline verdicts** (`offline-model-verdicts.json`): per question × model
`{ state, diff?, ciLow?, ciHigh?, source, period?, note, expectedBy? }`, and
`servedNotes` for the `today` cells. A cell's state is always against the
**served** model of the question; a figure against `wt_med`, climatology or
persistence is quoted in the note, never in `diff`, and a split across the
question's leads (e.g. Chronos-2 worse at 15 min, better at 30 and 60) is
`pending` when the majority is a win, with the split spelled out. Seeded from the approved mockup (ml-bench
PR #445 full period, #447 driver_level, #450 Chronos-2). Only numbers a run
reported against the served reference are filled in — no CI was reported, so
none is in the file. Update it when a run lands; the service reads it at
start-up.

**The Chronos cells were filled on 2026-10-11** from the **mixed-weekday re-run**
of #450 (`--shard 0/5`, 47 origins spread over all seven weekdays). The run it
replaces used `--shard 0/7`, and a 7-day stride puts every origin on the same
weekday — which locks every *lead* to one *target weekday* (origin Thursday → d2
is always a Saturday). Its per-lead curve was therefore a per-weekday curve read
as a per-lead one, and the d2/d3 loss against the H5 profile it reported does not
survive mixed weekdays. Result: `plan_d0_1` `better`, `plan_d2_5` `unclear` (only
d2 is a win, d3–d5 a tie), `plan_besttime` `worse` (best-time regret never gets
under climatology), `live_h1_2` `better` and `live_h0_1` `worse` confirmed
unchanged, `today_h2_6` / `today_h6_12` `pending` — Chronos beats the H5 profile
there, but PCN is what is served and PCN is not in the offline benchmark, so only
the live archive can decide those two. `plan_d6_60` has no Chronos cell at all:
the archived `chronos_grid` surface stops at d7. Toto 2.0 / TiRex (`fm`) stay
`pending` until PAR-853 reports.

**Corrections of 2026-10-11 (PAR-855 follow-up), checked against
[foundation-models.md](foundation-models.md), [ml-bench.md](ml-bench.md) and
[level-drivers.md](level-drivers.md):** `live_h0_1` × Chronos `worse` →
`pending` (vs persistence +0.26 [+0.18, +0.36] at 15 min, −0.42 [−0.61, −0.21]
at 30, −1.26 [−1.60, −0.93] at 60; #450 full period, 33 origins, 30 parks);
`live_h0_1` × persistence `better` → `unclear` (the served PCN is blended to
this very value in the first hour, and no run compares the two); `plan_d6_60` ×
TFT×H5 `worse` → `unclear` (the +0.42 was against `wt_med`; against served it
is ahead by point estimate); the `plan_d0_1` × TFT×H5 `diff` (−0.3, against
`wt_med`, headline window only) removed. Notes that quoted the headline window
as the full period, or a lead outside the question (the 15-min figures in
`live_h1_2`), were corrected: `live_h1_2` × persistence / H5 / TFT×H5,
`today_h6_12` × persistence / TFT×H5, `plan_d0_1` × TFT×H5, `plan_d2_5` × H5,
`plan_d6_60` × H5, `plan_besttime` × H5, `cal_busier` × weekday average and its
served note. **Left for the owner:** `plan_besttime` × Chronos and `cal_far` ×
weekday average are judged against climatology / a hit rate rather than the
served model on the question's own metric (against served, both point
estimates favour the challenger, without a CI), and `plan_d0_1` × TFT×H5 keeps
`better` although the full-period all-ride figure is +0.09 (n.s.) — the routed
curve is not what the bench measured.

### 9.4 Endpoints

All admin-auth, read-only, every query under statement limits.

- `GET /v1/admin/model-comparison?days=28&region=ALL|EU|NA|ASIA|OTHER` —
  `ModelComparisonResponseDto`. Cached 2 h under the scoring run's board
  version (`forecast-archive:board:version`), so each scoring run retires every
  variant. One aggregate over the window's paired rows (`GROUP BY` use case,
  lead, model, segment, park).
- `GET /v1/admin/model-comparison/day/options?date=YYYY-MM-DD` — **complete**
  target dates with archived 06:00 curves (newest first, ≤ 30): a date is
  listed once it is over in the timezone of every park (`lastCompleteDate`, so
  the US west coast holds a date back until ~09:00 Berlin). `parks` / `rides`
  are the rides with a `park_hourly` or `plan_day` curve on `date` **and
  OPERATING truth that day** — a ride without any is left out, not flagged.
  Truth coverage per ride is the share of the operating window (published
  hours, else the park-local day) in which an OPERATING STANDBY reading ≥ 5 min
  was in force (≤ 3 h old): the truth rule, time-weighted. One pass over that
  day's change log of all archived rides, 0.6 s on production for 2026-10-10
  (5,645 rides, 356 k rows); cached 15 min. `parks` are ordered by data
  richness (rides with coverage ≥ 0.5, then rides with any truth, then name),
  `rides` headliners first, then by coverage. `suggested` (`{ date, parkId,
  attractionId }` or null) is the first ride in that order with coverage ≥ 0.5
  and curves from as many archived surfaces (morning origin) as any such ride.
  Without a `date`, the newest of the first three complete dates that has a
  suggestion is described; an explicit `date` is described as asked, even an
  incomplete one. Validated on 2026-10-10: Alton Towers / Spinball Whizzer
  (headliner, 99 % coverage, 5 surfaces; 40 of 42 rides with coverage ≥ 0.5);
  2,406 of 5,645 archived rides had any truth that day. The previous default,
  Adventureland Resort / Draken Falls (no OPERATING row, schedule `UNKNOWN`), is
  no longer listed. Origins `morning` / `d1` / `d3` / `d7` = the 06:00 capture
  of the day itself, one, three or seven days before.
- `GET /v1/admin/model-comparison/day?parkId&date&attractionId&origin=morning`
  — `slots` are park-local "HH:MM" quarter-hours of the operating window only
  (`dayViewSlots`): the first published opening (floored) to the last closing;
  without published hours the first to the last slot with OPERATING truth,
  padded by one hour (truth is then read over the park-local day); without
  truth either, the span where the served curve (`plan_day`, else
  `park_hourly`) has values. Never a fixed clock window. `truth` is the BENCH
  truth per slot (null = not operating / no reading), `truthCoverage` the
  share of slots with a truth value (0 = no measurements that day); `series` every archived surface of that
  origin (`pcn` = park_hourly as served, `served`, `peak`, `h5`, `tfth5` only
  where the routed curve was archived, `chronos`, `wkd` computed as above,
  `persist` when the origin had a live wait), hourly surfaces read as a step;
  `scores` the MAE of each over the slots with truth and a value, ascending.
  `origin.at` (instant) and `origin.localLabel` ("10.10. 06:05 Uhr",
  park-local; "keine Prognose gespeichert" without a capture), `openingHours`
  park-local or null. Cached 15 min.

## 10. Chronos-2 served on the live surface (PAR-911)

**Default OFF.** `SERVE_CHRONOS_INTRADAY=true` on the API serves Chronos-2's
`live` curve for the slots 45–120 minutes ahead; nothing else changes. Prepared
so that the switch, once §8's live A/B has confirmed the offline result
(PAR-828/PAR-869: Chronos-2 beats persistence from 30 min and the served
PCN/profiles at 30–120 min; persistence stays best at 15 min), is a flag flip.

### 10.1 Which model serves which lead

Lead = slot start − the current slot start for Chronos
(`src/ml/chronos-serving.ts`, `MLService.applyServedIntradayOverride`); the §7.7
persistence blend measures its own lead from the actual time (PAR-1026), which
moves α within a slot on the 1–3 h ramp only:

| lead | served with both flags on | Chronos only on (`SERVE_PCN_INTRADAY` off) |
| --- | --- | --- |
| 0 – 30 min | PCN + persistence blend (pure anchor for a ride whose OPERATING reading is ≥ 5 min) | CatBoost |
| 45 – 120 min | **Chronos-2** q50 | **Chronos-2** q50 |
| > 120 min | PCN + blend (anchor share decays to 0 by 3 h, for an anchor ≥ 40 min) | CatBoost |

A ride without a cached curve, a curve older than **90 min**, or a slot the
curve has no value for (outside the published window) keeps what the row
above would serve — the fallback is the incumbent, never an empty slot. There
is a seam at 120 → 135 min (Chronos → a blend that still holds half the
anchor at 2 h); the offline bench shows no lead where that ordering is wrong.

Per served slot, the same per-purpose quantile split CatBoost and PCN use
(docs/ml/quantile-serving-and-calibration.md):

- **wait** = q50, through `roundServedWait` (5-minute steps, ≥ 10 when positive);
- **`uncertaintyMinutes`** = q95 − q50, the one-sided upper half-width the
  CatBoost band is (Chronos's own quantile, not a measured residual);
- **`crowdLevel`** = q80 (clamped ≥ the shown wait) ÷ the row's carried P50
  baseline — the live **ratio-vs-P50** regime of docs/rules/crowd-levels.md,
  exactly what the PCN override does with its q0.8. No baseline → the row's
  crowd level is left as it was.
- `modelVersion` = `<catboost version>+chronos` (replacing `+pcn`).

### 10.2 Serving cache and cadence

The request path never calls nf-service. It reads one Redis `MGET` of
`ml:chronos-serve:<attractionId>` (JSON: `originAt`, `revision`, the curve's
segments with q50/q80/q95; TTL 3 h, the 90-min guard decides) for the rides of
the rows being served.

Writers, both on the hourly `forecast-archive` job (`:05`):

| park-local hour | who writes | GPU cost |
| --- | --- | --- |
| 10, 12, 14, 16, 18 | the intraday archive capture (§8): its `live` response is written to the cache **whatever the flag says**, so the cache can be inspected before the switch | none extra — the shadow call already happens |
| 11, 13, 15, 17 | `refreshChronosServing`: one `live` call (`keep_minutes` 240) per park whose daily capture marked it active, cache only, no archive rows; claimed per park and hour (`NX`) | **only while the flag is on** |

Why hourly and not more often: the archive's origins are 2-hourly, so a curve
would be up to ~2 h 15 min old at its last use — the 90-min guard would retire
it for half of every gap. Hourly keeps the newest curve ≤ ~80 min old (`:05` +
up to ~15 min of capture). Measured 2026-10-16/17: up to 64 parks at one
origin hour (~1.3 k rides), one forward pass each (~3 s on CPU for a 40-ride
park, §8.5); the odd hours roughly double the hourly Chronos calls, still a few
minutes of the shared RTX 5080 per hour, and a TFT training run (503) only
costs those hours' refresh (→ fallback to PCN). A 30-minute cadence would need
its own cron and double that again for a freshness gain the A/B has not
measured; not built. Hours 19+ are not refreshed because the A/B measured
origins 10–18 only: after ~19:30 a park falls back to PCN.

With the flag on, an intraday capture awaits the Chronos call **before** its
served read, so that origin's surface-1 row already shows the new curve (it
costs the capture the overlap §8.5 describes, ~3 s per park).

### 10.3 Where it applies

Every read path that applies the PCN override applies this one too, through
`applyServedIntradayOverride`: `getParkPredictions` (park page, `/plan/day`
measured hours, calendar today, favorites, the archive's surface 1),
`getBatchStoredPredictions`, `getAttractionPredictionsWithFallback`, and
`servedIntradayValues` (the badge recorder). `prediction-deviation` is not
touched: it compares the CURRENT slot, which is inside the ≤ 30 min stretch
Chronos never serves.

### 10.4 The served-accuracy badge (PAR-854)

`prediction_accuracy.served_source` gains the value `chronos` (varchar, no
migration), set wherever the served pipeline's Chronos step produced the row.
**Read this before taking the badge as a Chronos figure:** the recorder scores
each slot at the last write ≥ 2 h ahead (`SERVED_ACCURACY_MIN_LEAD_MS`), and
Chronos serves ≤ 120 min — so in practice the scored row is PCN/blend at 2 h
to 2 h 15 min, and `chronos` appears only where the generator runs on a slot
boundary. That is consistent (the badge measures what a reader sees at the
lead it is defined for), but it means the switch will barely move the badge.
Moving the scored lead into Chronos's window is a separate decision: at
60–90 min the fallback rides would be scored on a mostly-anchor blend, which
is the self-grading PAR-854 avoided.

### 10.5 The archive after the switch

- **Surface 1 (`park_hourly`)** keeps archiving what is served; its Chronos
  slots carry the source code `s` (`chronos2_served`), so `all` and the
  per-source rows show the served product as it is.
- **Surface 8 (`park_hourly_incumbent`)**, captured only while the flag is on:
  the same read with `{ chronos: false }`. `scorePairedShadows` pairs
  `chronos_live` with it under the reference `served` when present (else with
  `park_hourly`, which before the switch IS the incumbent) — otherwise, after
  the switch, Chronos would be compared with its own served copy and the
  board's `diff` would read ~0. The admin matrix (`reference = 'served'`)
  therefore continues as one series across the switch. Surface 8 is scored
  under the source `incumbent`, never into `all`. Size: one more 4-hour row
  per ride and intraday origin (~+5 k rows a day).
- The `persistence` reference is unchanged.

### 10.6 Switch-on checklist (planned ~2026-10-25)

1. §8.4 board: `chronos2` vs `served` on UC1 `h0-1` … UC2 2-hour leads,
   `verdict: chronos_better` with ≥ 14 days and ≥ 10 parks, and vs
   `persistence` not worse beyond 30 min.
2. Cache is warm: `redis-cli --scan --pattern 'ml:chronos-serve:*' | wc -l`
   during an intraday hour ≈ the rides of the parks at that origin, and a
   sample's `originAt` is the last even hour.
3. `GET /v1/admin/ml-overview` Chronos component not `failed`/`warming`.
4. Set `SERVE_CHRONOS_INTRADAY=true` on the API in Coolify, redeploy.
5. Next odd hour: the archive job log shows the refresh; the overview says
   "Live ausgeliefert"; `GET /v1/parks/<slug>` hourly forecast shows
   `modelVersion …+chronos` on 45–120 min slots.
6. Next day's scoring: surface 8 rows exist and `chronos2` vs `served` keeps
   its sign; surface 1 `chronos2_served` slots appear in the scores.

**Rollback:** `SERVE_CHRONOS_INTRADAY=false` (or unset) and redeploy. Every
read path is back to PCN/blend/CatBoost on the next request; nothing is
persisted that depends on the flag (the cache keys expire within 3 h and are
only read while the flag is on; surface 8 simply stops being written). The
shadow keeps running under `CHRONOS_SHADOW_ENABLED`, independent of this flag.
