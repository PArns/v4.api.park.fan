# ML overview for the admin (PAR-861)

`GET /v1/admin/ml-overview` answers three questions in this order: what is up
for decision, where is which model better, is everything running. The frontend
(park.fan PAR-862) replaces `/admin/ml` with it; the model comparison
([forward-archive.md §9](forward-archive.md#9-model-comparison-for-the-admin-par-855))
stays the detail page behind it.

Code: `src/admin/ml-overview.service.ts` (reads), `src/ml/utils/ml-overview.util.ts`
(pure: decision state, progress, earliest date, status thresholds),
`src/ml/dto/ml-overview.dto.ts` (contract), `src/ml/data/ml-decisions.json`
(the decisions). AdminAuthGuard, read-only, cached 5 min in Redis
(`ml-overview:v1`).

## 1. Decisions

Each entry in `ml-decisions.json` names the matrix cells it rests on
(`questionId` × `modelId` of the model comparison). The served (`today`) cells
in that list are kept for the link but ignored in the derivation. The matrix
is `ModelComparisonService.getMatrix(28, "ALL")` — the board's default, cached
2 h per scoring run, so the overview adds no aggregation of its own.

- **`ready`** — every cell `better`, with checks offline `yes` and live `yes`.
- **`waiting_live`** — at least one cell is better offline (check `yes`, or
  `running` = an offline run is still going) while its live check is `running`
  or `open`.
- **`open`** — otherwise: offline shows no sure win, or live already said no.
- **`later`** / **`owner`** — a static `state` in the file wins. `later` is dated
  after the latest `earliest` of its `dependsOn`; `owner` is the owner's call
  (no matrix cells, nothing to measure).

`progress` = min `live.days` and min `live.parks` over the decision's cells (0
when a cell has no live figure), against the gate 14 days / 10 parks.
`earliest` = today (Europe/Berlin) + the missing days, via `addIsoDays`; days
met but parks short = tomorrow (the next scoring); gate met or `ready` = null —
never before `decisionFloor`, the latest `cellFloorDate` of its cells
(2026-10-09 + the longest lead + 14: a lead with no row yet counts as 0 days,
and "today + 14" alone slid forward every day for the d60/d90 cells).

**`evaluateOn`** (PAR-1036, owner decision: one fixed evaluation date instead of
re-testing every day). Before that date a decision is never `ready` (a derived
`ready` reads `waiting_live`) and `earliest` is at least the date ("ausgewertet
am …"). From that date on (`evaluated: true`) the decision is judged on the
matrix of the window ending the day before (`getMatrix(28, "ALL", now, until)`,
cached per `until`), so its verdict no longer drifts. Each date in
`ml-decisions.json` is at least `decisionFloor` (a spec checks it); the
short-lead ones are 09.11.2026, two weeks after the paired-definition-3 deploy,
so they do not rest on the replay alone. **The owner changes a decision's date
by editing `evaluateOn` in `src/ml/data/ml-decisions.json`** (a later date
re-judges it on the newer window; removing the field returns to daily
derivation).
This is park-agnostic and can differ by a day from a cell's own `expectedBy`,
which counts from the window's last scored date.

Without a matrix (the comparison fails), the summary is zero and every derived
decision reads `open` with no progress; the page still renders.

## 1a. Frozen decision reports (PAR-1059)

The `evaluateOn` verdict above was still recomputed on every page build from
the paired rows of its window — rows that `forecast_archive_paired` deletes
after 120 days, so a verdict would have drifted again about three months
later. Now it is written down once.

**When.** `MlDecisionWatchService.runDaily`, called at the end of the
forward-archive `score` job (queue `forecast-archive-score`, 12:00 UTC), after
scoring and retention. For each decision without a static `state` whose
`evaluateOn` (Europe/Berlin) has arrived and that has no row for that date yet,
it reads `getMatrix(28, "ALL", now, evaluateOn − 1)`. It freezes only once that
window's last scored date is `evaluateOn − 1` (scored on `evaluateOn` itself at
12:00 UTC); if scoring is behind, it waits up to
`WINDOW_END_GRACE_DAYS` = 3 days and then freezes anyway with the
`window_end` check failed.

**Where.** `ml_decision_reports` — `decision_id`, `evaluate_on` (PK both),
`verdict`, `warning`, `report` (jsonb, `MlDecisionReportDto`), `created_at`.
Plain table, no FK, a handful of rows a year. Written with `INSERT … ON
CONFLICT DO NOTHING` and never updated: a second run, a retry or a parallel
worker changes nothing. Moving a decision's `evaluateOn` in
`ml-decisions.json` makes a new report for the new date; the old row stays and
is no longer shown.

**What.** Per non-served cell: state, `diff`, the gate's park × date CI and the
park-cluster CI, parks, days, `n`, the question's `jointRows` and the cell's
own rows. For the decision: the paired park × date cells of its
`forecast_archive_paired` keys (definition 3, the cells' leads and models) —
distinct parks and dates, the count per weekday (Mo … So), the share on a
public holiday, school break or bridge day at the park (`dayHolidayFlags`, the
calendar's rule; no other event source exists), the season (meteorological,
northern hemisphere), and `verdict` = `deriveDecisionState` on the frozen
cells.

**Mandatory checks** (`src/ml/utils/ml-decision-report.util.ts`):

| id | fails when | why |
| --- | --- | --- |
| `weekday_mix` | any weekday > `WEEKDAY_MAX_SHARE` = 25 % of the paired park × date cells (even = 14.3 %) | weekend-only parks in autumn make a window a Saturday/Sunday sample |
| `holiday_share` | > `HOLIDAY_MAX_SHARE` = 40 %, or unknown (no holiday data) | a window of school breaks is not a typical one |
| `autumn_only` | every paired date in September–November | the first evaluation dates (09.11.) see autumn only |
| `window_end` | the window does not end on `evaluateOn − 1` | scoring was behind when the report froze |

A failed check sets `warning: true` and a plain-German line in `warnings`; the
verdict itself is the matrix's and is not altered — a `ready` with a warning is
shown as such, never as a silent green.

**On the overview.** A decision with a stored report for its current
`evaluateOn` takes `state` from `report.verdict` and carries `report`
(`MlDecisionDto.report`, additive); its frozen window is no longer read. Before
the job ran on the day itself, the frozen-matrix path of §1 still applies.

## 2. Components

Every row is one bounded read, `SET LOCAL statement_timeout = 10 s`, plus at
most a 3 s service call. Any failing source degrades that row to `unknown`.
Status: `ok` if the newest run is younger than its threshold, `late` beyond,
`unknown` without any run; `failed` / `warming` from a service's own state win
over the age. Thresholds are `LATE_AFTER_MS` in the util, each the cadence plus
room for one slow run, never tighter than the gaps measured on production.

| id | source of `lastRunAt` | expected | late after | timing on prod (2026-10-11) |
| --- | --- | --- | --- | --- |
| `pcn` | `pcn_forecasts` newest `created_at`, last 2 days (`idx_pcn_forecasts_created_at`, backward) | alle 15 Min. (rund um die Uhr) | 45 min | 5.7 ms |
| `tft` | `tft_forecasts`: per ride, its newest forecast for the day after tomorrow (PK lookup, see below) + nf `/train/status` | täglich 03:00 UTC | 30 h | 20–28 ms warm, 372–728 ms cold |
| `catboost` | `ml_models` active `trainedAt` + ml-service `/train/status` | täglich 06:00 UTC | 30 h | 0.6 ms |
| `chronos` | archive surfaces 6/7 + the archive's last Chronos run summary + nf `/chronos/status` | stündlich zur Minute :05 UTC (je offenem Park alle 2 Std.) | 4 h | (shared with archive) |
| `h5` | archive surface 4 | stündlich zur Minute :05 UTC (je Park einmal am Tag) | 8 h | (shared) |
| `archive` | newest `origin_at` of any surface (one `GROUP BY surface` over 2 days, index-only on the PK) | stündlich zur Minute :05 UTC | 3 h | 34–46 ms |
| `scoring` | `forecast_archive_scores` newest `scored_at`, last 30 target dates | täglich 12:00 UTC | 26 h | 0.6 ms |

The `shape` row (and the decision `shape_service_off`) went with the
shape-service in PAR-870 (2026-10-12).

Measured gaps behind the thresholds (prod, last 2–3 days): PCN 15 min max;
archive `park_hourly` 2 h, `plan_day` 1 h, H5 6 h, the routed H5 7 h.

**TFT has no index on `created_at` or `forecast_date`** (3.4 GB, 23.7 M rows), so
`max(created_at)` is a 0.7 s sequential scan. The probe goes through the PK
instead: for every attraction, `(attraction_id, target_date = today+2)` backward
on `forecast_date`, `LIMIT 1` — the newest run that covered that day. A stopped
TFT leaves an older `created_at` there and the row reads `late`. `failed` when
nf-service reports a failed run that finished after it.

**PCN training (PAR-1053).** PCN keeps writing forecasts every 15 min from the
previous model when a training run fails, so the forecast age alone never showed
it. The row also reads pcn-service `GET /train/status` (3 s timeout; unreachable
→ ignored, the age decides as before): `status: "failed"` overrides to `failed`
with the error in the note, `is_training` adds "trainiert gerade". The status
file carries no finish time, so a failure stands until the next run replaces it.

**Feature drift (PAR-1050).** `detectFeatureDrift` returns
`summary.status: "no_data"` (counts all 0) when no feature could be compared —
which is production today: `prediction_accuracy.features` is never written. The
hourly alert check then keeps a low `feature_drift` alert "Feature Drift: No
Data" open instead of resolving it as healthy.

**Chronos warming.** `ChronosShadowService` counts calls / ok / warming per
archive run, and `ForecastArchiveService.captureDue` writes that summary to
Redis (`forecast-archive:chronos:last-run`, 2 days) whenever the run asked
Chronos anything. A summary younger than the 4 h threshold with `warming > 0`
reads `warming` ("lädt Historie (12 von 70 Parks liefern schon)"); with calls
but no answer and failures, `failed`. `CHRONOS_SHADOW_ENABLED=false` or nf
`enabled: false` is said in the note.

Every `expected` string names its clock in UTC (the frontend labels the
column "Zeiten in UTC"); per-park local origins are described as the UTC hourly
run that captures them. `lastRunAt` is always a full ISO instant — for the
scoring row the run's `scored_at`, with the scored target day in `note`.

PCN is `served` only when `SERVE_PCN_INTRADAY=true`, else `shadow` with a note.

## 2a. Drift watch (PAR-1059)

Owner decision: no outbound alerts for now ("vorerst keine Alarme"). The watch
is a status on the overview, `MlOverviewDto.drift` (absent until the first
run; Redis `ml-overview:drift:v1`, 3 days).

Computed daily in the same step as §1a (`computeDrift`, one query, 5 min
statement timeout) from `prediction_accuracy`, hourly rows, scored slots only
(`scoredSlotSql`, [accuracy-scoring-rule.md](accuracy-scoring-rule.md)), over
the last 63 days. Two windows: **recent** = the last 7 days, **previous** = the
28 before. Per model:

- `catboost` — the raw CatBoost answer (`absolute_error`) and its stored
  quantiles: coverage = P(actual ≤ `predicted_q80`) and P(actual ≤
  `predicted_q95`).
- `pcn`, `chronos` — the served answer (`served_absolute_error` where
  `served_source` is that model). No quantile of theirs is stored, so their
  coverage is null.

**Weekday median.** Per scored slot, the median actual of the same ride at the
same park-local weekday and hour 7, 14, 21 and 28 days before (≥ 2 of them),
from the table's own actuals. Ex ante for every slot, so both windows are
judged alike. `skill` = Σ model error ÷ Σ weekday-median error on the slots
that have one; below 1 beats the weekday median. The ratio, not the MAE, is
what is compared: a busier week raises both.

**Thresholds** (`DRIFT_THRESHOLDS`, `src/ml/utils/ml-drift-watch.util.ts`):

| | drift when recent vs previous |
| --- | --- |
| q0.8 coverage | moves more than 5 percentage points |
| q0.95 coverage | moves more than 3 points |
| skill | rises more than 10 % (0.80 → above 0.88) |
| floor | fewer than 1,000 scored slots in either window → `no_data`, no verdict |

The overall `status` is the worst model's (`drift` > `ok` > `no_data`); each
model lists the crossed thresholds in plain German under `reasons`.

## 3. Maintaining the decisions

Edit `src/ml/data/ml-decisions.json` when a decision is taken, added or
retired. A spec checks that every cell names a real question and model of the
matrix, that ids are unique and that `dependsOn` points backwards.
