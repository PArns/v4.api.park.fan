# ML overview for the admin (PAR-861)

`GET /v1/admin/ml-overview` answers three questions in this order: what is up for decision, where
is which model better, is everything running. The frontend shows it at `/admin/ml` (park.fan
PAR-862); the [model comparison](forward-archive/model-comparison.md) is the detail page behind it
(`/admin/ml/vergleich`). Open this page before editing `src/ml/data/ml-decisions.json`, a status
threshold, or the decision reports.

Code: `src/admin/ml-overview.service.ts` (reads), `src/ml/utils/ml-overview.util.ts` (pure:
decision state, progress, earliest date, status thresholds), `src/ml/dto/ml-overview.dto.ts`
(contract), `src/ml/data/ml-decisions.json` (the decisions),
`src/ml/utils/ml-decision-report.util.ts` (§1a), `src/ml/utils/ml-drift-watch.util.ts` (§2a).
AdminAuthGuard, read-only, cached 5 min in Redis (`ml-overview:v1`). The response also carries
`bandCalibration`, the current CatBoost hourly band factors (PAR-1060,
[quantile serving](quantile-serving-and-calibration.md#catboost-hourly-band-split-conformal-factor-par-1060)).

## 1. Decisions

Each entry in `ml-decisions.json` names the matrix cells it rests on (`questionId` × `modelId` of
the model comparison); served (`today`) cells are kept for the link but ignored in the derivation.
The matrix is `ModelComparisonService.getMatrix(28, "ALL")` — the board's default, cached 2 h per
scoring run.

- **`ready`** — every cell meets its expectation: by default `better`, with checks offline `yes` and
  live `yes`. A cell with **`expect: "not_better"`** (PAR-1042) instead needs live check `no` (the
  gate is met and the model does not beat what is served) — for switching a model off on cells that
  measure it against its replacement (`catboost_daily_off`'s `catboost` cells compare CatBoost with
  the served TFT). A `not_better` cell whose gate is not met counts towards `waiting_live`.
- **`waiting_live`** — at least one cell is better offline (check `yes`, or `running`) while its live
  check is `running` or `open`.
- **`open`** — otherwise: offline shows no sure win, or live already said no.
- **`later`** / **`owner`** — a static `state` in the file wins. `later` is dated after the latest
  `earliest` of its `dependsOn`; `owner` is the owner's call (no cells).

`progress` = min `live.days` and min `live.parks` over the decision's cells (0 without a live figure)
against the gate 14 days / 10 parks. `earliest` = today (Europe/Berlin) + the missing days
(`addIsoDays`); days met but parks short = tomorrow; gate met or `ready` = null — never before
`decisionFloor`, the latest `cellFloorDate` of its cells (2026-10-09 + the longest lead + 14).

**`evaluateOn`** (PAR-1036, owner decision: one fixed evaluation date instead of re-testing daily).
Before that date a decision is never `ready` (a derived `ready` reads `waiting_live`) and `earliest`
is at least the date. From that date on (`evaluated: true`) it is judged on the matrix of the window
ending the day before (`getMatrix(28, "ALL", now, until)`). Each date is at least `decisionFloor` (a
spec checks it); the short-lead ones are 2026-11-09, two weeks after the paired-definition-3 deploy.
**To re-judge a decision, edit its `evaluateOn`** (a later date re-judges on the newer window;
removing the field returns to daily derivation). Current decisions and dates:

| Decision | Cells (question × model) | `evaluateOn` |
| --- | --- | --- |
| `live_chronos` | `live_h0_1`, `live_h1_2` × `chronos` | 2026-11-09 |
| `planner_chronos` | `plan_d0_1`, `plan_d2_5` × `chronos` | 2026-11-09 |
| `planner_h5_slots` | `plan_*`, `plan_besttime` × `h5`, `tfth5` | 2026-12-22 |
| `calendar_tft_headliners` | `cal_rank`, `cal_busier`, `cal_far` × `tft`, `wkd` | 2026-12-22 |
| `calendar_tft_grid`, `calendar_tft_grid_drivers` | `cal_rank`, `cal_busier`, `cal_far` × `tft_grid` / `tft_grid_drivers` | 2027-01-08 |
| `catboost_daily_off` (state `later`) | `cal_rank`, `cal_busier`, `cal_far` × `catboost` (`not_better`), `cal_far_90` × `catboost`, `wkd` | 2027-01-21 |
| `calendar_driver_level` | `cal_rank`, `cal_busier`, `cal_far`, `cal_far_90` × `driver` | 2027-09-01 |

The overview date can differ by a day from a cell's own `expectedBy`, which counts from the window's
last scored date. Without a matrix (the comparison fails) the summary is zero and every derived
decision reads `open`; the page still renders.

## 1a. Frozen decision reports (PAR-1059)

The `evaluateOn` verdict is written down once, because `forecast_archive_paired` deletes its rows
after 120 days and a recomputed verdict would drift.

- **When.** `MlDecisionWatchService.runDaily`, at the end of the forward-archive `score` job (queue
  `forecast-archive-score`, 12:00 UTC), after scoring and retention. For each decision without a
  static `state` whose `evaluateOn` (Europe/Berlin) has arrived and that has no row for that date, it
  reads `getMatrix(28, "ALL", now, evaluateOn − 1)` and freezes once that window's last scored date
  is `evaluateOn − 1`; if scoring is behind it waits up to `WINDOW_END_GRACE_DAYS` = 3 days and then
  freezes with the `window_end` check failed.
- **Where.** `ml_decision_reports` — `decision_id`, `evaluate_on` (PK both), `verdict`, `warning`,
  `report` (jsonb, `MlDecisionReportDto`), `created_at`. Plain table, no FK. `INSERT … ON CONFLICT DO
  NOTHING`, never updated; moving a decision's `evaluateOn` makes a new report and the old row is no
  longer shown.
- **What.** Per non-served cell: state, `diff`, both CIs (park × date and park-cluster), parks, days,
  `n`, the question's `jointRows` and the cell's own rows. For the decision: its paired park × date
  cells (definition 3) — distinct parks and dates, count per weekday, the share on a public holiday,
  school break or bridge day at the park (`dayHolidayFlags`), the meteorological season, and
  `verdict` = `deriveDecisionState` on the frozen cells.

| Check | Fails when | Why |
| --- | --- | --- |
| `weekday_mix` | any weekday > `WEEKDAY_MAX_SHARE` = 25 % of the paired park × date cells | weekend-only parks in autumn make a window a weekend sample |
| `holiday_share` | > `HOLIDAY_MAX_SHARE` = 40 %, or unknown | a window of school breaks is not a typical one |
| `autumn_only` | every paired date in September–November | the first evaluation dates see autumn only |
| `window_end` | the window does not end on `evaluateOn − 1` | scoring was behind when the report froze |

A failed check sets `warning: true` and a plain-German line in `warnings`; the verdict itself is not
altered. A decision with a stored report for its current `evaluateOn` takes `state` from
`report.verdict` and carries `report` (`MlDecisionDto.report`).

## 2. Components

Every row is one bounded read (`SET LOCAL statement_timeout = 10 s`) plus at most a 3 s service call;
a failing source degrades that row to `unknown`. Status: `ok` if the newest run is younger than its
threshold, `late` beyond, `unknown` without a run; `failed` / `warming` from a service's own state win
over the age. Thresholds are `LATE_AFTER_MS` in the util — the cadence plus room for one slow run,
never tighter than the gaps measured on production.

| id | Source of `lastRunAt` | Expected | Late after | Timing on prod (2026-10-11) |
| --- | --- | --- | --- | --- |
| `pcn` | `pcn_forecasts` newest `created_at`, last 2 days (`idx_pcn_forecasts_created_at`) + pcn-service `/train/status` | every 15 min | 45 min | 5.7 ms |
| `tft` | `tft_forecasts`: per ride its newest forecast for the day after tomorrow (PK lookup) + nf `/train/status` | daily 03:00 UTC | 30 h | 20–28 ms warm, 372–728 ms cold |
| `catboost` | `ml_models` active `trainedAt` + ml-service `/train/status` | daily 06:00 UTC | 30 h | 0.6 ms |
| `chronos` | archive surfaces 6/7 + the last Chronos run summary + nf `/chronos/status` | hourly at :05 UTC (every 2 h per open park) | 4 h | shared with archive |
| `h5` | archive surface 4 | hourly at :05 UTC (once a day per park) | 8 h | shared |
| `archive` | newest `origin_at` of any surface (one `GROUP BY surface` over 2 days, index-only) | hourly at :05 UTC | 3 h | 34–46 ms |
| `scoring` | `forecast_archive_scores` newest `scored_at`, last 30 target dates | daily 12:00 UTC | 26 h | 0.6 ms |

- **TFT has no index on `created_at` or `forecast_date`** (3.4 GB, 23.7 M rows), so `max(created_at)`
  is a 0.7 s sequential scan. The probe goes through the PK: per attraction,
  `(attraction_id, target_date = today+2)` backward on `forecast_date`, `LIMIT 1`. A stopped TFT
  leaves an older `created_at` there and reads `late`; `failed` when nf-service reports a failed run
  that finished after it.
- **PCN training** (PAR-1053): PCN keeps writing forecasts from the previous model when training
  fails, so the row also reads pcn-service `GET /train/status` (unreachable → ignored):
  `status: "failed"` → `failed` with the error in the note, `is_training` adds "trainiert gerade".
  The status file has no finish time, so a failure stands until the next run replaces it.
- **Chronos warming.** `ChronosShadowService` counts calls / ok / warming per archive run and the
  capture writes that summary to Redis (`forecast-archive:chronos:last-run`, 2 days). Younger than 4 h
  with `warming > 0` → `warming` ("lädt Historie (12 von 70 Parks liefern schon)"); calls with no
  answer and failures → `failed`. `CHRONOS_SHADOW_ENABLED=false` or nf `enabled: false` is said in
  the note; with `SERVE_CHRONOS_INTRADAY` on the note says "Live ausgeliefert".
- PCN is `served` only when `SERVE_PCN_INTRADAY=true`, else `shadow`.
- **Feature drift** (PAR-1050): `detectFeatureDrift` returns `summary.status: "no_data"` when no
  feature could be compared — production today, because `prediction_accuracy.features` is never
  written — and the hourly alert check keeps a low "Feature Drift: No Data" alert open.
- Every `expected` string names its clock in UTC (the frontend labels the column "Zeiten in UTC");
  `lastRunAt` is a full ISO instant (for scoring the run's `scored_at`, the target day in `note`).

## 2a. Drift watch (PAR-1059)

Owner decision: no outbound alerts for now ("vorerst keine Alarme"). The watch is a status on the
overview, `MlOverviewDto.drift` (absent until the first run; Redis `ml-overview:drift:v1`, 3 days),
computed in the same daily step as §1a (`computeDrift`, one query, 5 min timeout) from
`prediction_accuracy` hourly rows, scored slots only (`scoredSlotSql`,
[accuracy-scoring-rule.md](accuracy-scoring-rule.md)), over the last 63 days: **recent** = the last 7
days, **previous** = the 28 before.

- `catboost` — the raw CatBoost answer (`absolute_error`) and its stored quantiles: coverage
  P(actual ≤ `predicted_q80`) and P(actual ≤ `predicted_q95`).
- `pcn`, `chronos` — the served answer (`served_absolute_error` where `served_source` is that model);
  no stored quantile, so no coverage.
- **Skill** = Σ model error ÷ Σ weekday-median error on the slots that have one (below 1 beats it).
  The weekday median is the median actual of the same ride at the same park-local weekday and hour
  7, 14, 21 and 28 days before (≥ 2), computed from the table's own actuals folded to one value per
  ride and local hour (unfolded, the self-join was 23 M rows and 61 s; folded 12 s on 2026-10-24).
  The ratio, not the MAE, is compared, because a busier week raises both.

| `DRIFT_THRESHOLDS` | Drift when recent vs previous |
| --- | --- |
| q0.8 coverage | moves more than 5 percentage points |
| q0.95 coverage | moves more than 3 points |
| skill | rises more than 10 % (0.80 → above 0.88) |
| floor | fewer than 1,000 scored slots in either window → `no_data` |

The overall `status` is the worst model's (`drift` > `ok` > `no_data`); each model lists the crossed
thresholds in plain German under `reasons`.

## 3. Maintaining the decisions

Edit `src/ml/data/ml-decisions.json` when a decision is taken, added or retired. A spec checks that
every cell names a real question and model of the matrix, that ids are unique, that `dependsOn`
points backwards and that every `evaluateOn` is at least the decision's floor.
