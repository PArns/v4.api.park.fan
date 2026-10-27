# Admin model comparison (PAR-855)

For every question the app answers, does any model beat **what is served today** for that
question? One matrix, every model, live from the forward archive plus the offline verdicts of the
ml-bench runs — the owner's switch rule made visible: (O) offline better over the whole year,
(L) live ≥ 14 days and ≥ 10 parks with the CI below zero, (S) visible in the app. Open this page
before changing the paired scoring, a question, a cell's state rule or the offline verdict file.
The admin ML overview ([ml-overview.md](../ml-overview.md)) derives its open decisions from named
cells of this matrix. Frontend: park.fan PAR-856.

Code: `scorePairedModels` in `src/ml/utils/forecast-archive-scoring.util.ts`,
`src/ml/utils/forecast-archive-paired.util.ts` (bootstraps), `src/ml/utils/weekday-average.util.ts`,
`src/ml/utils/model-comparison.util.ts` (questions, state rule, notes, dates),
`src/parks/services/model-comparison.service.ts`, contract `src/ml/dto/model-comparison.dto.ts`,
offline verdicts `src/ml/data/offline-model-verdicts.json`.

## Paired scoring

`scorePairedModels` runs inside the 12:00 UTC scoring next to `scorePairedShadows`
([chronos-shadow.md](chronos-shadow.md)) and writes into the same `forecast_archive_paired`
(key: target date, park, use case, lead, model, reference, segment; `n`, Σ|e| and Σe of both; kept
`PAIRED_RETENTION_DAYS` = 120 days). No key it writes is one `scorePairedShadows` writes (a spec
asserts it).

**One question, one set of rows** (PAR-1035, `PAIRED_DEFINITION` = 3). A slot or ride-day is
paired only where truth, the served reference **and every core candidate of the question**
(`PAIRED_QUESTION_MODELS`) have a value, under `reference = served_joint`, so every cell of a
question at a lead shares one reference MAE. (Before, each model was paired on its own coverage:
`live_h1_2` × H5 was then the rope-drop hour of the 7 parks open before 08:00, −9.1 min.)

| Use case × lead | Core candidates | Served reference | Origins |
| --- | --- | --- | --- |
| UC1 `h0-1` | persistence, `chronos2`, `shadow_h5` | `park_hourly` (`park_hourly_incumbent` once Chronos-2 is served) | 10–18 h |
| UC1 `h1-2`, UC2 `h2-6` | + `shadow_h5_routed` | same | 10–18 h (`h2-6` is therefore 2–4 h) |
| UC2 `h6-12` | `shadow_h5`, `shadow_h5_routed`, `chronos2_grid` | `park_hourly` | 06:00 |
| UC3 d0–d5 | `shadow_h5`, routed, `shadow_peak_scaled`, `weekday_avg`, `chronos2_grid` | `plan_day` | 06:00 |
| UC3 d6, d7, d10 … d60 | the same without Chronos-2 | `plan_day` | 06:00 / 07:00 |
| D3 d0–d7 / d10–d30 | as UC3 d0–d5 / without Chronos-2; regret of every candidate and of served on the same slots (`n` = ride-days, Σ\|e\| = Σ regret) | `plan_day` | 06:00 / 07:00 |
| PEAK dN | `weekday_avg`: \|peak − truth P90\| per ride-day, `all` and `headliner`; optional shadow levels below | `plan_day` `dayPeak`, under `served_tft` / `served_catboost` by the model behind the level (PAR-1037) | 06:00 / 07:00 |
| D7 dN | `weekday_avg` vs served `dayPeak`, headliners whose level is TFT's; shadow levels likewise, each on its own rows | `served_tft` | 06:00 / 07:00 |

- At a 10–18 h origin the daily curves (H5, routed, the old composer, `chronos2_grid`) are the ones
  **issued at or before it** (the 06:00 capture, which reads nothing after 06:00); only the lead-0
  curve counts. Persistence is the origin's live wait. The 06:00 origin feeds no `h0-1` … `h2-6`
  row, and `today_h6_12` has no persistence column (almost no park has a live wait at 06:00).
- **Core and optional candidates.** Only core candidates define the joint rows. The nf-service
  shadows (`chronos2`, `chronos2_grid`), the old composer (`shadow_peak_scaled`) and the shadow
  levels are **optional** (`PAIRED_OPTIONAL_MODELS`): written on the core rows where they have a
  value, so an nf-service outage costs their own cells and nothing else; each question's
  `coverage` (`jointRows` and rows per cell) shows the difference. A missing core candidate (a new
  ride with no weekday average, a ride without an H5 profile) removes that row from every cell of
  the question. For D3 an optional candidate counts only on ride-days where it has a value on every
  joint slot.
- **Direct comparisons** (`PAIRED_PAIRWISE`) on the same rows: `chronos2` / `chronos2_grid` vs
  `shadow_h5`, and `shadow_h5_routed` vs `shadow_h5`; shown as `comparisons`, never as a state.
- **Rounding** (PAR-1024): `PairedAccumulator` puts both arms of a UC1/UC2/UC3 slot through
  `roundServedWait` (5-minute steps, floor 5 since PAR-1027), which any candidate would go through
  once served — persistence and Chronos-2 had won partly by rounding before. Persistence is
  therefore judged as if it were the served forecast (a live 3 counts as 5). D3 regret and PEAK
  stay unrounded.
- **Definition and replay.** `forecast_archive_paired.definition` records the scoring definition
  of a row; the matrix reads the current one only, so every live gate restarts at a definition
  change — except what the replay restores: curves live 14 days past their target, so `scoreDue`
  rescores every date of that window whose rows carry an older definition (oldest first, 5 per
  run, delete-then-insert, written only when every park scored). Three daily runs after a deploy
  the last 14 dates are back; older dates stay out. A replayed date uses today's headliners,
  attractions and `schedule_entries`. New optional models (`catboost_shadow`, `tft_grid`,
  `tft_grid_drivers`, `driver_level`) did not bump the definition: no existing row changes meaning.

### Calendar questions: day peaks split by the level's model

TFT and CatBoost never level the same ride-day (the served daily read takes TFT where it has a row),
so neither can be paired with the other. PEAK and D7 rows are split by the model behind the served
level: the TFT-served questions compare with TFT's days, `cal_far_90` (d90, CatBoost-served beyond
TFT's 60 days) with CatBoost's.

### The shadow levels

All four are optional models paired with the served `dayPeak` on PEAK and D7, only on ride-days
where they have a value; D7 day pairs are formed within one model's rows (`d7AggregatesByModel`).

| Model | Source | Read | Paired against |
| --- | --- | --- | --- |
| `catboost_shadow` (PAR-1042) | CatBoost's level on TFT-levelled ride-days, captured as `catboost_day_peak` | capture column | `served_tft` — makes "switch the CatBoost daily level off" decidable live |
| `tft_grid` (PAR-1041) | the calendar-grid TFT trained nightly into `tft_forecasts_shadow` ([TFT grid shadows](../tft-grid-shadows.md#the-grid-as-a-nightly-shadow-par-1041)) | at scoring time (`shadowLevels`, PK-bounded); `shadowLevelAt` picks the freshest `forecast_date` ≤ 3 days before the origin's date written at or before the origin, rounded, floored at 0 | `served_tft` |
| `tft_grid_drivers` (PAR-1057) | the grid TFT with the level drivers, `tft_forecasts_shadow_drivers` | as `tft_grid` | `served_tft`; own column and decision (`calendar_tft_grid_drivers`) |
| `driver_level` (PAR-1058) | `ml-service/driver_level.py` → `driver_level_forecasts` ([level drivers](../level-drivers.md#8-the-production-shadow-par-1058)) | capture column `driver_day_peak` (the table keeps only the latest night) | every level source: PEAK under `served_tft` or `served_catboost`, D7 on TFT-levelled headliners; matrix column `driver` („Treiber-Niveau") |

Because these ride on the weekday average's rows, they are scored only where that average exists
— a quarter-hour needs `WEEKDAY_AVG_MIN_DAYS` = 2 earlier same-weekday dates — so a park's or
ride's first two weeks collect nothing. For `driver_level` a park whose 06:00 origin comes before
the night's 01:30 UTC run in its own date (Asia, the Americas) reads the level that run made at
lead L+1.

`MODEL_ARCHIVE_START` holds each later model's assumed first capture (`tft_grid`,
`tft_grid_drivers` 2026-10-26; `driver` 2026-10-27) — a shortcut: correct them to the first
`forecast_date` of the shadow tables after deploy. The `catboost_shadow` cells' "frühestens" dates
count from `ARCHIVE_START`, so they are optimistic by the time between 2026-10-09 and its deploy.

### Weekday average (`weekday_avg`)

The ex-ante yardstick: per ride and park-local quarter-hour, the mean over the **8 most recent
same-weekday dates strictly before the origin's park-local date** (the util applies the guard; a
spec pins it), ≥ 2 dates per quarter-hour. Computed at scoring time from `attraction_hourly_history`
(per-quarter-hour mean of OPERATING STANDBY readings ≥ 5), forward-filled ≤ 3 h per day like the
truth. The read is exactly the dates D − 7k for k = 1 … ⌊max lead / 7⌋ + 8 (≤ 20 dates for d90),
one indexed query per park and scoring date, 15 ms for Europa-Park. Its day peak is the P90 of its
values over the served curve's span.

### D7 rows

D7 rows are levels, not errors: per park, date and lead `n` headliners, `sae_model` = Σ model peak,
`sae_ref` = Σ served `dayPeak`, `se_model` = `se_ref` = Σ truth P90. The read path (`d7Aggregates`,
once per model) orders a park's dates within the window at each lead: two dates whose realised
level differs by ≥ 5 min are a pair, wrongly ordered when the forecast disagrees (a predicted tie
counts half). The cell is the share of wrongly ordered pairs in percent, model minus served, lower
is better (`metric.unit = "rate"`). Caveats: the headliner set is those with all three values on
that date, so a pair can flip on which rides were measured; parks open every day contribute more
pairs (they grow with the square of the dates), and a long lead with few pairs weighs as much as d1.

**Size** (estimate): ~165 rows per park-day, ~21 k a day over ~126 parks, ~2.5 M rows / ~330 MB at
120 days. Everything happens in the scoring job; the hourly capture is untouched.

## Questions

The questions, with their German labels, served model, use case and leads, are
`COMPARISON_QUESTIONS` in `src/ml/utils/model-comparison.util.ts`; read them there, not from a copy.
By group: `live_h0_1`, `live_h1_2` and `today_h2_6`, `today_h6_12` compare against PCN (UC1, UC2);
`plan_d0_1`, `plan_d2_5`, `plan_d6_60` (UC3) and `plan_besttime` (D3, best-time regret) against the
served curve; `cal_rank` (D7), `cal_busier` and `cal_far` (PEAK) against the TFT level of the
headliners, `cal_far_90` (PEAK d90) against CatBoost's. Every metric is minutes, lower is better,
except `cal_rank` (`rate`, percentage points). The calendar questions read the planner's per-ride `dayPeak` (built on the same daily
level the calendar reads); the calendar's crowd level itself is not archived per model. Toto 2.0 /
TiRex-2 (`fm`) have no archived curve, so their cells are offline only (`checks.live = "open"`).

## Cells and states

- **Pooling.** Per cell and lead the column's model rows are pooled per park (Σ minutes, divide
  once); **across the leads of a question every lead weighs the same** (PAR-1036 — pooled by `n`,
  `plan_d6_60` was de facto d6/d7).
- **CI.** `diff = modelMae − referenceMae`, negative = better than served. **The gate reads the
  two-way park × date block bootstrap** (`blockBootstrap(…, { twoWay: true })`: parks and dates
  resampled independently, each cell weighted by the product of multiplicities, so a day that moved
  every park is one draw). `live.ciLow/ciHigh` are that interval (`live.ci = "park_date"`); the
  park-cluster interval is shown as `parkCiLow/parkCiHigh`. 1,000 reps, per-cell seed. No
  multiplicity correction across the ~45 cells.
- **Gate.** ≥ 10 parks with ≥ 14 scored dates **each** (`gateDays` = the 10th-best park's count),
  and every lead of the cell (`cellLeads`) must meet it on its own; `days` / `parks` are the weakest
  lead's.
- **Floor dates.** "frühestens" never falls before `cellFloorDate` = `ARCHIVE_START` (2026-10-09) +
  the cell's longest lead + 14 (22.12.2026 for d60, 21.01.2027 for d90), or the model's own
  `MODEL_ARCHIVE_START`. The dates assume the 10th-best park is scored daily; parks closing for the
  season make them optimistic.
- **Fixed evaluation dates.** Decisions in the ML overview carry `evaluateOn` and are judged once
  on the window ending the day before (`getMatrix(…, until)`), then frozen in
  `ml_decision_reports` ([ml-overview.md §1a](../ml-overview.md#1a-frozen-decision-reports-par-1059)).
  The matrix page itself keeps showing the latest window.
- **State rule.** The served model is `today`. A live comparison that meets the gate decides: CI
  entirely > 0 → `worse`; CI entirely < 0 → `better` **only when the offline verdict is `better`
  against served too** (owner rule: offline AND live), a live win while the offline run is in
  progress → `pending`, else `unclear`. Below the gate the offline verdict speaks: `better` or a run
  in progress → `pending`, `worse` → `worse`, otherwise `unclear`.
- **`checks`**: `offline` yes / no / running / open; `live` yes / no once the gate is met, `running`
  before, `open` when not archived; `visible` = the served model, or a confirmed live win of ≥ 1 min
  (≥ 5 points on the `rate` question, `VISIBLE_MIN_DIFF_RATE` — about the same share of a ~30–40 %
  base rate as a minute is of a 7–9 min slot MAE).
- **`expectedBy`** (Europe/Berlin): for a live gate not met, last date + missing days + 1; when only
  parks are missing, the next scoring; for an offline run in progress the file's `expectedBy`; the
  earlier of the two. `live.since` = the model × question's first scored date. `live` is null with
  fewer than two parks. `note` is generated from the numbers ("1,3 Minuten besser als heute, sicher.
  18 Tage, 34 Parks.").

## Offline verdicts (`offline-model-verdicts.json`)

Per question × model: `{ state, reference, metric, runId, diff, ciLow, ciHigh, source, period?,
note, expectedBy? }`, plus `servedNotes` for the `today` cells; the service reads it at start-up and
the file's `_about` keeps its change log.

- **`reference`** (PAR-1038) is what the offline figure was measured against: `served` (the
  question's served model — ml-bench's `prod_served`, which exists from d3 only, or the
  [served arm](../ml-bench/served-arm.md)), `persistence`, `wt_med`, `clim`, `chronos2`,
  `hourly_profile`, or null. **An offline `better` counts only with `reference = served`**
  (`offlineBetterThanServed`); otherwise a live win reads `pending`, the offline check `open`. This
  keeps every PCN-served question (PCN is not in the bench) from turning `better` on a win over
  persistence. The DTO carries `reference`, `referenceIsServed`, `metric` and `runId` additively.
- `metric` (`slot_mae`, `besttime_regret`, `daypeak_mae`, `hit_rate`), `runId` and the three figures
  stay null until a run reports them against that reference; a figure against another baseline is
  quoted in the note, never in `diff`. A split across a question's leads is `pending` when the
  majority wins, with the split in the note.
- Generated, not typed, from now on: `python3 -m mlbench.served_verdicts … --write`
  ([served arm](../ml-bench/served-arm.md#pairing-and-verdicts)). `benchmark-only` models are
  refused there and by `src/ml/utils/benchmark-only-models.spec.ts`.
- The Chronos-2 cells come from the mixed-weekday re-run (`--shard 0/5`, 47 origins on all seven
  weekdays); a `--shard 0/7` run puts every origin on one weekday and turned a Saturday effect into
  a "d2" loss. `plan_d6_60` has no Chronos-2 cell (the archived grid stops at d7).
- **Open for the owner:** `plan_besttime` × Chronos-2 is judged against climatology rather than the
  served model on the question's own metric; `plan_d0_1` × TFT×H5 keeps `better` although the
  full-period all-ride figure is +0.09 (n.s.) and the routed curve is not what the bench measured.

## Endpoints

All admin-auth, read-only, every query under statement limits.

- `GET /v1/admin/model-comparison?days=28&region=ALL|EU|NA|ASIA|OTHER` — `ModelComparisonResponseDto`.
  Cached 2 h under the scoring run's board version (`forecast-archive:board:version`). One aggregate
  over the window's paired rows (`GROUP BY` use case, lead, model, segment, park).
- `GET /v1/admin/model-comparison/day/options?date=YYYY-MM-DD` — complete target dates with
  archived 06:00 curves (newest first, ≤ 30): a date is listed once it is over in every park's
  timezone (`lastCompleteDate`). `parks` / `rides` are the rides with a `park_hourly` or `plan_day`
  curve and OPERATING truth that day; truth coverage per ride is the time-weighted share of the
  operating window with an OPERATING STANDBY reading ≥ 5 in force (0.6 s for 5,645 rides). Parks
  ordered by data richness, rides headliners first. `suggested` is the first ride with coverage
  ≥ 0.5 and curves from as many surfaces as any such ride; without `date`, the newest of the first
  three complete dates that has one. Cached 15 min.
- `GET /v1/admin/model-comparison/day?parkId&date&attractionId&origin=morning|d1|d3|d7` — one
  ride-day: `slots` (park-local "HH:MM" quarter-hours of the operating window only, `dayViewSlots`:
  published hours, else the span of OPERATING truth ± 1 h, else the served curve's span), `truth`
  per slot, `truthCoverage`, `series` per archived surface (`pcn`, `served`, `peak`, `h5`, `tfth5`,
  `chronos`, `wkd`, `persist`; hourly surfaces as a step), `scores` (MAE of each, ascending),
  `origin.at` / `origin.localLabel`, `openingHours`. Origins `morning` / `d1` / `d3` / `d7` = the
  06:00 capture of the day itself, one, three or seven days before. Cached 15 min.
