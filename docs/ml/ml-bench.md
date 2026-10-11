# ml-bench — the offline forecasting benchmark (PAR-827)

> **One harness for every model comparison.** `ml-bench/` exports production
> read-only, builds the 15-minute truth once, runs a rolling-origin backtest of
> every baseline and every plug-in model under a strict information cut, and
> scores each use case and each frontend decision with park-day bootstrap CIs.
> The binding measurement spec is BENCH-SPEC (PAR-826); this page is how the
> harness implements it and how to add a model.

It is **not deployed** — not in either compose file, no service, no port. It runs
by hand on celestrial, inside its own container, with CPU and memory caps.

## Layout

```
ml-bench/
  Dockerfile            python 3.11 + torch cu128 (sm_120) + duckdb/pandas
  mlbench/export.py     read-only export (stdlib only — runs on the DB host)
  mlbench/build.py      raw CSV -> Parquet + 15-min truth + covariates + bench.duckdb
  mlbench/baselines.py  baselines 1–6 as DuckDB SQL, one origin at a time
  mlbench/runner.py     rolling origins, plug-in calls, per-origin aggregates
  mlbench/decisions.py  planner optimiser simulation (D4)
  mlbench/report.py     bootstrap, horizon curves, hand-over table, summary.md
  mlbench/models/       plug-in interface (base.py) + example adapter
  tests/                synthetic export; truth, info-cut, optimiser, plug-in parity
```

Data lives outside the repo: `/data/parkfan/ml-bench/exports/<id>/` on celestrial
(`raw/` CSV, `parquet/`, `bench.duckdb`, `manifest.json`). Results go to
`ml-bench/results/<run-id>/` — `summary.md`, `tables/*.csv` and `run-*.json`
(exact config, git SHA, export) are small and committed for reference runs;
`parts/` (per-origin aggregates, re-scorable with `report`) and `work/` are
git-ignored.

## The export

`python3 ml-bench/mlbench/export.py --out /data/parkfan/ml-bench/exports/<id> --repo <checkout>`
on celestrial. Every query is one `COPY (SELECT …) TO STDOUT` through
`scripts/prod-psql.sh` (statement/idle/lock timeouts), autocommit, strictly one
after another, resumable (finished files are skipped).

- **`queue_data` is exported as the raw STANDBY change log, one UTC day per
  query** — one day is exactly one hypertable chunk. Measured on 2026-10-09
  (`export-manifest.json`, committed): 16,559–212,454 rows per day (median
  ~150 k), **0.11–0.40 s per query** (30 of the 289 queries over 0.3 s), 289
  UTC days in ~3 minutes, 455 MB gzip. Building the 15-minute slots server-side
  would have cost production a window function over every ride of a park-month
  and returned 4–10× more rows; client-side it is a chunked DuckDB step.
- `tft_forecasts` / `catboost_daily_forecasts` by forecast-date month
  (no index on `forecast_date`; largest file 5,670,526 rows, slowest query
  4.43 s), leads 0–120 d.
- `parks` (timezone, regionCode, influencingRegions, lat/lng), `attractions`
  (+ `headliner_attractions`, coordinates, land), park-level `schedule_entries`,
  `holidays`, `weather_data`.
- **No weather forecast archive exists** (`weather_data` keeps only the current
  16-day forecast) — the covariates carry daily actuals flagged `ORACLE_actuals`,
  which baselines never read and plug-ins only get by opting in.
- **Schedule history is partial**: `schedule_entries` keeps the latest version
  of a day, but `updatedAt` says when it was last written. A window last written
  after an origin was not known at that origin (16.2–17.0 % of the scored slots
  at d1, 32.5–32.9 % at d30, 44 % at d60 — 14.6–15.4 % at d0 — in the 2026-10-09
  export; the range is across models because each has its own coverage; the
  median operator publishing horizon is 39 d), so the runner treats it as
  unknown — see "Protocol".
- `headliner_attractions` is the current table (548-day window), not ex-ante.
  It only decides which rides count as headliners (UC4 / D2 / D4 / D6).

## Truth

15-minute slots on a UTC grid (every park offset in use is a whole quarter hour).
A slot's value is the STANDBY reading **in force at the slot midpoint** — the
newest change-log row with `ts <= midpoint`, at most 3 h old. A slot belongs to
the service day whose published OPERATING window contains its start (windows
may cross midnight: `ws` then continues past 95). Truth = status OPERATING and
wait ≥ 5. The `slots` table keeps every status (D9 openness). Ride-day level =
P90 of the ride-day's truth slots (≥ 8 slots); park level = mean over headliners.
`tests/test_truth.py` pins the midpoint rule, the 3-hour cap, DST and midnight.

## Protocol

- Origin per park-local day at **06:00** (UC2/UC3/UC4) and intraday origins at
  08, 10 … 20 (UC1, UC2 rest-of-day). Evaluation from the first truth day + 56.
- **Information cut**: profiles read only slots that END before the park's
  origin; TFT / CatBoost daily levels are the latest rows with
  `created_at < origin` and, like production's serving read, at most 3 days
  stale; long-history baselines read a month snapshot cut at the first day of the
  origin's month; production's hourly profile is rebuilt as of the day before.
- **Opening hours as known at the origin**: a target day's published window is
  used only if its schedule row was last written before the origin; otherwise
  the forecast uses a window projected from the last 56 days (median opening and
  closing time per weekday type) — production's observed-hours fallback. Every
  opening-aligned forecast (H5's first and last hour, `prod_served`'s open hours,
  the plug-in grid) reads that window; the truth and the "first hour" segment of
  the scores stay on the published one. Scores are also split by schedule
  known / projected at the origin.
- `tests/test_runner.py::test_information_cut_baselines` and
  `::test_information_cut_plugins` (run for EVERY registered model, on the daily,
  the daily-LEVEL and the intraday path) poison truth, ride-day levels, hourly
  stats, TFT/CatBoost rows written after the origin, **both ends** of a window
  written after the origin and every covariate column, and assert no forecast
  moves. An ORACLE-weather model keeps the weather it is entitled to and
  everything else still has to hold.
- The two schedule-derived covariates (`sched_is_holiday`, `sched_is_bridge_day`)
  are built from every schedule row of the day, so they are cut at **their own**
  publication time, not at the OPERATING rows'. A day whose OPERATING row was
  published before the origin but whose holiday flag came from a later
  non-OPERATING row is delivered as NULL.
- Ex-ante busy = the ride's q90 over the 56 days before the origin ≥ 45 min.
- Lead grid: d0 (intraday + 06:00), d1–d7, d10, d14, d21, d30, d45, d60, d90 for
  slot curves; UC4 daily levels at every lead 1–90 plus 120/180/270/365.

## Baselines

| column | BENCH-SPEC | note |
|---|---|---|
| `persistence` | 1 | intraday only; the slot that ends at the origin |
| `snaive7` | 2 | same local slot 7 days earlier, only if that day is before the origin (≤ d6 from 06:00) |
| `wt_med` (+ `wt_q80/q95`) | 3 | 56 d, weekday/weekend ≥ 3 days per slot, else all days ≥ 4 |
| `clim` | reference fallback | same slot + weekday type + season over all history before the month snapshot, else all seasons (no ride has a previous year yet) |
| `h5` | 4 | opening-aligned first hour, close-aligned last hour, hourly median interpolated |
| `lvlh5_naive` / `lvlh5_tft` / `lvlh5_cbd` | 5a / 5b / — | H5 × level ÷ window median daily P90; first hour unscaled |
| `prod_served` / `prod_served_lin` | 6 | `/plan/day` composed tier rebuilt as of the origin: the `/stats/hourly` profile (one year ending yesterday; AVG over days of each hour's P50 with ≥ 2 samples; rides with ≥ 20 days; the park's hour axis keeps an hour measured on ≥ 10 days and ≥ 0.4 × the best hour's days and reported by ≥ 50 % of the rides; rounded to 5), `composeDayCurve` over the open hours known at the origin (midnight wrap), max-normalised to the served day level (TFT ≤ 3 days stale, else CatBoost daily), rounded to 5. `prod_served` = the hourly step the planner reads; `_lin` = the same curve interpolated between hour centres. d ≥ 3 only (d0–d2 serve CatBoost hourly, which is not stored per lead). The hourly P50 is approximated from the 15-min truth (`hour_stats`), not read from `queue_data_aggregates` |
| `oracle_level` / `oracle_shape` | decomposition | H5 × TRUE level / TRUE shape × naive level — never in the hand-over |

## Decision metrics (frontend: park.fan)

Every decision metric is PAIRED at the level it lives on — never only at the
park-day: slot by slot (MAE, first hour), ride-day by ride-day (best time,
dayPeak error, rope drop; a ride-day counts for a model only if the model covers
every truth slot of it, so all models are scored on the same slots), ride pair by
ride pair (dayPeak ordering), park-day by park-day (optimiser), day pair by day
pair (day comparison).

| ID | What is simulated |
|---|---|
| D1 | `next-best-ride.ts`: candidates = every (intraday origin, ride) with a live OPERATING wait, the same set for every model; suggest when the forecast maximum inside a FIXED lookahead (60 or 120 min) ≥ live + 10 (walk 0); precision = the TRUE maximum there is also ≥ live + 10; top-3 per park and origin; recall |
| D2 | `LIVE_WINDOW_MIN = 45`: bias / MAE at leads 15–45 min on headliners — `persistence` is what production serves there |
| D3 | best slot: hit if a true-minimum slot is in the predicted lowest two / within ±30 min; regret = true wait at predicted best − true minimum |
| D4 | `optimize.ts` objective Σwait + 0.5·Σidle on the park's top 4 rides (headliners first, ex-ante level); exact optimum over orders and 0–120 min delays (the beam search converges to it on 4 rides); regret = true cost of the forecast's plan − truth-optimal cost. Plus headliner dayPeak pairwise ordering |
| D5 | first-hour MAE (first 4 slots of the PUBLISHED window, also split by schedule known / projected); worth = peak ≥ 60 ∧ peak − opening wait ≥ 45 per ride-day, and the production rule on window medians (`prod_ropedrop_hist`) |
| D6 | park level ÷ typical-day peak → `determineCrowdLevel` ladder; exact (paired) / ±1 bucket, busy (high+) recall & precision, cross-park ordering on a date |
| D7 | `day-comparison.ts` rank = bucket + min(0.99, avg/120): winner accuracy on pairs with true gap ≥ 0.5 (paired, CI over origin-park), tie (< 0.1) rate; calendar star = rank ≤ month median − 0.5 among ≥ 4 days, precision / recall with CIs |
| D8 | **what production states**: `forecast-accuracy.service.ts` publishes the global MAE of TFT's predicted peak against the day's max hourly P90 by predicted band (quiet / mid / busy) × lead bucket (d1/d3/d7/d14/d30/d60) over the 45 days before today. It is rebuilt as of each origin and compared with the realised error of the forecasts served at that origin. Plus empirical q80/q95 coverage, field coverage, and (secondary) the trailing 28-day slot MAE |
| D9 | share of operating slots / headliner-days with a forecast per lead (incl. 90–365 d), forecast present vs ride operated. The baselines have no openness logic, so their D9 is the bar a model with an open/closed decision has to beat |

D4 limits: a fixed ride set (top 4), no overflow / headliner dropping (the
park-day is skipped if the rides cannot all start before closing), no fixed
blocks, opensAt floors, early entry or live corrections; the wait at a start is
the 15-min slot value (the frontend reads the hourly point today); truth gaps are
filled from the nearest slot of the same ride-day; walking from coordinates, else
3 / 8 min (`leg.ts`).

## Horizon outputs

`lead_availability.csv` (origin days per lead **for the pass that writes it** —
in a windowed pass the target day must also be inside the window, LOW-N < 30,
"not measurable yet" with the date it becomes measurable), `coverage_horizon.csv`
(TFT / CatBoost / weather / operator schedule), `cells.csv` (every metric × use
case × lead × segment × region × model: value with park-day CI, paired difference
vs the SELECTED reference with park-day AND park-cluster CIs, coverage, unit of
analysis and unit counts), `cells_all_refs.csv` (the same, against **every**
reference candidate), `ref_choice.csv` (candidates, their values, the selected
reference and why), `low_coverage_cells.csv`, `usable_horizon.csv` (with
`leads_tested` and `not_tested_because`) and `handover.csv` (with both CIs and a
`park_day_unit_agrees` flag).

- **A win = beating the whole naive ENVELOPE.** The model must be better than
  **every** reference candidate at that lead, each comparison paired on its own
  shared rows, and each with its **park-cluster** bootstrap CI (all days of a
  park resampled together) excluding 0, over at least **10 distinct bootstrap
  units** and with a strictly positive CI width. This replaces "beats the
  selected reference": a single selected reference is a choice made on the data,
  and every rule for making it errs in one direction — an argmin over
  own-coverage values picks on noise and understates models, a ladder tie-break
  picks the *simplest* candidate and overstates them. A conjunction over a FIXED
  candidate set is neither, and it is strictly harder than beating any one
  member, so **no verdict depends on which candidate is displayed as `ref`**.
  Days of one park are not independent, so the park-day CI is too narrow; both
  are in `cells.csv` (`diff_lo/hi` park-day, `diff_lo_park/hi_park` park-cluster)
  and both are printed in the hand-over table, which marks the cells where they
  disagree. BENCH-SPEC's literal unit is the park-day; the park-cluster deviation
  and its reason are **recorded in BENCH-SPEC.md** (change #1; the unit-count and
  width guards are change #2, the envelope is #5), as the spec requires. The unit-count and width guards exist because a single-unit paired
  set makes the resampled ratio weight-independent: the CI collapses to zero
  width and "excludes 0" for free.
- **Reference per lead** = BENCH-SPEC's ladder (persistence → snaive7 → wt_med →
  clim; for daily levels lvl_snaive7 → lvl_naive4 → lvl_wt56 → lvl_clim), among
  candidates covering ≥ 50 % of what the best-covered one covers. A candidate is
  moved off the ladder position only when another candidate beats it in a
  **paired** round robin with the park-cluster CI excluding 0; near-ties are
  broken by the ladder order, never by the point estimate. `ref_choice.csv`
  records the candidates, their own-coverage values and the reason for each
  choice.

  This is deliberately *not* "the best naive on the metric itself". That reading
  — an argmin over the candidates' own-coverage values — compares MAEs measured
  on different row sets, re-chosen at every lead, selected on the same data the
  models are scored against. On the 2026-10-09 run the `wt_med`/`clim` gaps are
  0.019–0.187 min against a park-cluster half-width of ±0.24, so the reference
  flipped `wt_med → clim → wt_med` along the lead axis on noise, and at d7 and
  d60 the harness's own paired test said the rejected candidate was better. The
  resulting "nothing beats the reference in the d7–d60 band" was a claim about
  the pointwise minimum of three naives, not about any of them.
- **Every model is also scored against each candidate individually**
  (`cells_all_refs.csv`). "No model beats the reference" is a statement about the
  best-of-N naive envelope; "`h5` beats the weekday median by 0.14 min" is a
  different statement, and both are published.
- **`cells.csv` has two win columns and they disagree. That is deliberate, and the
  disagreement is one-directional.** `wins` is the published criterion — beats
  **every** candidate. `wins_vs_ref` is the older, weaker question — beats the
  *displayed* `ref` only — and it is kept because BENCH-SPEC change #5 requires the
  per-reference statement to stay separable from the envelope one. They disagree on
  **233 cells over the two passes (154 in the headline window, 79 on the full
  period)**, and in **all 233** it is `wins_vs_ref = True` with `wins = False`: a
  cell can beat the candidate it is displayed against and still fail the
  conjunction, never the reverse. That one-directionality is the proof that the
  envelope is strictly the harder test. **If you are reading one column, read
  `wins`.**
- **`hardest_naive` is the worst point estimate, not necessarily the blocker, and
  the two coincide less often than is comfortable.** These shares are
  **scope-sensitive**, so the population is stated rather than implied:
  `cells.csv` rows with `region == "all"`, `wins == False`,
  `is_candidate == False`, the model neither an oracle nor `_owx`, the cell passing
  `tested()`, and `n_env >= 2`; blockers are the candidates in
  `cells_all_refs.csv` for that cell whose own comparison is not a win.

  | pass | N | `hardest_naive` is **among** the blockers | `hardest_naive` is the **sole** blocker |
  |---|---|---|---|
  | headline window | 1,077 | 97.8 % | **17.4 %** |
  | full period | 996 | 98.8 % | **18.8 %** |

  Widening the population to include reference candidates and cells that fail the
  gate moves the sole-blocker share to ~21–25 %, which is why the scope matters;
  the among-blockers share is stable either way. So the old reference is almost
  always *a* blocker and the sole blocker only about one time in five — which is
  the real proof that the envelope criterion does work, because a conjunction that
  were usually decided by one member would be the selected-reference rule under
  another name. "The hardest candidate is the one that decides" is a useful
  heuristic and a bad inference: read `cells_all_refs.csv` per candidate before
  naming a blocker. PAR-830 reported this mechanism holding on its own plug-in;
  **it does not transfer to these baselines at anything like 100 %**, and this PR
  does not claim it does.
- **Gates** for the hand-over and the usable horizon: enough units for the
  metric's own unit of analysis — ≥ 30 origin **days** for day-unit metrics,
  ≥ 30 park-**months** for UC4, whose unit is the park-month — and the model's
  paired rows ≥ 30 % of the reference's own. Winners are ranked by the PAIRED
  margin. Usable horizon = the largest lead, contiguous from the model's first
  scored lead, at which it wins; a lead where it is the reference, is untested or
  fails a gate breaks the run. `usable_horizon.csv` carries `leads_tested` and
  `not_tested_because`: **`leads_tested = 0` means nothing was tested, which is
  not the same as nothing winning** and must never be read as a loss.
- **KNOWN GAP: no table carries a weekday column, and that is why a two-day blind
  spot was possible.** PAR-828's subset took every 7th origin from a Thursday, so
  all 33 of its origins were Thursdays and **every planner lead was one fixed
  weekday** — d2 = Saturday, d3 = Sunday. Its "unexplained d2 hole" was the
  Saturday cell, and it withdrew a lead-indexed routing rule because of it. Four
  workstreams and three critic passes missed it for two days: `grep` for
  `dow|weekday|weekend` across all 26 output CSVs returns **zero hits**, so the
  aliasing was invisible in every artefact a reviewer reads first.

  Two guards now exist at run time: `run-*.json` records an `origin_weekdays`
  census (seven counts, Monday first) plus `origin_weekday_distinct`, and the
  runner **refuses** an origin set whose lead axis is aliased with day-of-week —
  any `--shard i/n` with `n` a multiple of 7, or any set of more than 7 origins
  landing on ≤ 2 distinct weekdays — unless `--allow-weekday-alias` is passed to
  state that this is one of several shards whose union is complete.
  `test_weekday_aliasing_is_refused_and_the_census_is_recorded` mutation-checks
  the refusal in both directions.

  **A `mae_by_weekday` split is the right fix and is NOT here.** It would mean
  regenerating tables that a critic has verified cell by cell and that three PRs
  now cite, and the verification is worth more than the split is today — so it is
  recorded as an owner follow-up rather than silently skipped. The run-time guards
  above stop the *aliased* case being constructed; they do not let you read a
  weekday effect out of these tables, and nothing in this review does.
- **`low_coverage_cells.csv`** lists every cell whose paired rows are under half
  the reference's. Those absolute values are not comparable with the rest of
  their column — at d60 `lvlh5_tft` reads 1.337 min on the first hour because
  TFT's horizon ends there and 2 % of the reference's slots remain.
- **Headline horizon curves come from a common target window**
  (`report --target-from … --target-to …`), which fixes the *truth population* so
  two leads are compared on the same days' outcomes. It does **not** remove the
  lead–season confound, and an earlier version of this page wrongly said it did:
  inside a fixed target window lead L's origins are `target − L`, so the d1 cells
  come from autumn origins and the d90 cells from summer ones. The full period
  has the mirror problem (lead L starts at first origin + L, and TFT / CatBoost
  only exist from late May, so lead mixes with *target* season). **Both passes are
  confounded and neither alone identifies the horizon curve** — use
  `--origin-from/--origin-to` to pin the origin window instead, and read the
  per-season tables before reading a lead boundary as a number.

## Adding a model

1. Copy `ml-bench/mlbench/models/example_level_h5.py`, rename the class and
   `name`, implement `predict` (and `fit` / `predict_daily` if useful). The
   example reproduces baseline 5a through the plug-in path and a test asserts
   the two score identically — keep that as the pattern.
2. Register it in `mlbench/models/__init__.py` (`REGISTRY`), or pass
   `--model package.module:Class`. A registered model is automatically covered by
   `test_information_cut_plugins` — run `pytest` before any run you report.

   **What the test fixture cannot check.** `tests/synth.py` builds **two** parks,
   so `n_units_park ≤ 2` and the `min_bootstrap_units = 10` guard makes
   `wins = True` unreachable in the suite. Any assertion about *winners* on that
   fixture is vacuous unless it lowers the floor, so prefer data-independent
   invariants — `test_handover_and_usable_horizon_share_one_gate` checks that
   `handover.n_tested` equals the count the same gate admits in `cells.csv`,
   which is what would have caught critic B4 in both tables at once.
3. What a plug-in gets (`mlbench/models/base.py`): DataFrames only, never a
   database connection and no object from which one is reachable —
   `origin.history.df(days)` (truth ending before the origin, materialised up
   front, so `days` is capped at `--history-days`, 56 by default; use `fit` for
   longer history), `train_panel` in `fit`, the horizon grid of the window known
   at the origin, and covariates without weather unless the **class** sets
   `uses_oracle_weather = True` (then it is scored as `<name>_owx`; setting it on
   the instance buys nothing). Rows outside the grid are dropped.

   **The history cap is part of the plug-in contract, not an implementation
   detail.** `df(days)` **raises** above `--history-days`; it does not return a
   short frame. A foundation model that wants a 343- or 400-day context therefore
   fails loudly at the first origin and has to be given the budget explicitly
   (`--history-days`, or a `fit(train_panel, cutoff)` that takes `train_days` of
   truth) instead of silently publishing numbers from a truncated context with
   nothing in the output to show it. PAR-828's Chronos-2 plug-ins hit this on the
   merged tree and PAR-829's `df(56)` passes only because it sits exactly on the
   inclusive boundary — so read the cap before reporting a long-context model, and
   widen it deliberately. Like `uses_oracle_weather`, an information budget is read
   off the **class**: an instance attribute must never be able to widen it.
4. Run (celestrial):

```bash
cd ~/claude-worktrees/<your-checkout>
docker build -t ml-bench:<tag> --build-arg GIT_SHA=<commit sha> \
  --build-context mlsvc=ml-service ml-bench
docker run --rm --gpus all ml-bench:<tag> gpu-check          # must list sm_120
IMAGE_ID=<output of: docker image inspect -f '{{.Id}}' ml-bench:<tag>>
docker run -d --name mlbench-<model> --gpus all --user 1000:1000 -e HOME=/tmp \
  -e MLBENCH_IMAGE_ID=$IMAGE_ID --cpus 6 --memory 8g --cpu-shares 256 --entrypoint nice \
  -v /data/parkfan/ml-bench:/data -v $PWD/ml-bench/results:/app/results \
  ml-bench:<tag> -n 10 python -m mlbench run --export /data/exports/20261009 \
  --out /app/results/<run-id> --model <name> --memory 5GB --threads 4 --reference
docker run --rm --user 1000:1000 -e HOME=/tmp -v /data/parkfan/ml-bench:/data \
  -v $PWD/ml-bench/results:/app/results ml-bench:<tag> report --run /app/results/<run-id> \
  --target-from 2026-08-15 --target-to 2026-10-07          # the headline (common window)
docker run --rm --user 1000:1000 -e HOME=/tmp -v /data/parkfan/ml-bench:/data \
  -v $PWD/ml-bench/results:/app/results ml-bench:<tag> report --run /app/results/<run-id>
```

`--from/--to` limit the origins, `--leads 0,1,3,7` the slot leads, `--shard i/n`
splits origins across containers. Every run re-scores the baselines, so the
model's numbers are always paired against the same reference on the same rows.
`--reference` refuses to start without a git SHA and an image id; every run
records both plus a sha256 of the `mlbench` sources in `run-*.json`.

Model families benchmarked so far get their own document: the foundation models
Chronos-2 and TimesFM 3.0 (PAR-828) plus TiRex-2 and Toto 2.0 (PAR-853) in
`docs/ml/foundation-models.md`.

## Results

Reference run: `ml-bench/results/20261009-baselines-v2/`. Export of 2026-10-09
(289 UTC days, 44.2 M change-log rows → 13,455,954 truth slots, 403,317
ride-days, 3,601 rides, 157 parks), 231 daily origins 2026-02-19 → 2026-10-07,
3 shards × (`--cpus 3 --memory 2560m`), 54.4 shard-minutes.

**Two provenance records, because two different commits are involved.** The
scoring sums in `parts/` come from git `ea9a8438`, sources sha256
`25ed0c3b26d3accd`, image `sha256:a1e54cb8fea6` — that is what `run-*.json`
describes. Every *verdict* on top of them (the reference choice, the envelope
win, the LOW-N gate, the hand-over winner) is recomputed by `report`, and the
published tables were produced by git `ca29858b`, sources sha256
`efd8088545a5852a`, image `sha256:8fb8722a15eb` — that is what
`report-<tables-dir>.json` describes (recorded change #7). `code_sha256` is
**measured** inside the writing process and is the field to trust; `git_sha` and
`image_id` are **asserted** by whoever launched the container and have both been
recorded wrong in this repository. Read the two lines at the top of a `summary*.md`
rather than assuming one SHA covers a table.

**The headline is the common target window 2026-08-15 … 2026-10-07**
(`summary_tables_2026-08-15_2026-10-07.md`, `tables_2026-08-15_2026-10-07/`). The
full period (`summary.md`, `tables/`) covers all 231 origins.

**What the common target window does and does not do.** It fixes the *truth
population*: every lead's cells are measured on the same 54 target days, so two
leads are compared on the same days' outcomes rather than on different months of
weather, holidays and crowding. That is a real and necessary improvement over the
full period, where lead L starts at first origin + L and TFT / CatBoost only exist
from late May.

It does **not** remove the lead–season confound, and the earlier wording on this
page, which said it did, was wrong. A lead-L cell inside a fixed target window is
scored from origins at `target − L`: the d1 cells come from **autumn** origins and
the d90 cells from **summer** origins. So the apparent decay along the lead axis
still contains an origin-season gradient. Fixing the target window pins when the
truth happened; for a horizon curve the confounded axis is when the forecast was
*made*. PAR-830 measured this independently on a different model — its
`driver_level` d1 level-gap share flips from +8.5 % on this window to −11.0 % on
the full period, and the model beats `h5` only in autumn.

**So both passes are confounded, in different ways, and neither alone identifies
the horizon curve.** The full period mixes lead with target season; the common
target window mixes lead with *origin* season. The honest statement is that lead
and season are not separable in 54 days of targets. `report` now takes
`--origin-from` / `--origin-to` so a future run can fix the origin window instead
(or as well); doing that properly on this data needs a longer history than the
289 days we have — a 54-day origin window and a 90-day lead grid need 144 days of
truth after the warm-up, which exists, but then the target days differ by lead
again. The resolution is more data, not a better window. Until then: read the
per-season tables, and treat every lead boundary as a *band*, not a point.

**Where the two passes disagree, neither is "the answer".** Neither is a general
serving verdict: the common window is 54 days of late summer and early autumn —
17 summer days and 37 autumn days, no Easter, no Pentecost, no peak summer, no
Christmas — and the effect that drives the TFT-level recommendations is
**seasonal inside that window**: see "The season split" below. Every hand-over and
usable-horizon row below therefore carries both passes, and the rows where they
disagree are marked.

Two more scope limits a reader needs before the tables:

- **The target itself changes character across the window.** `is_heartbeat` rows
  (a carried-forward status *and* wait, with a fresh `ts`, which resets the 3 h
  staleness clock) are not filtered out of the truth. Their share of OPERATING
  rows goes 0 % (until ~2026-09-06, when the column was not populated) → 20.6 %
  (09-10) → 33.1 % (10-05), and the share of truth slots that exist *only*
  because of a heartbeat goes 0.00 % (08-20) → 3.58 % (09-15) → 11.20 % (10-05).
  That is an unremoved confound of exactly the summer/autumn contrast below, and
  it is an open owner decision — see "Heartbeat contamination".
- The reference-selection rule changed after the first publication of these
  numbers (see "Horizon outputs"), so figures quoted from the first revision of
  PR #445 or from `20261009-baselines/` do not match this page.

How to read it: every MAE is on the model's own coverage, so a model counts as
better only through the **paired** difference against the per-lead reference,
with the **park-cluster** 95 % CI excluding 0 (`*` below). `lvlh5_tft` at d1 on
the full period reads 7.50 against `wt_med` 7.66, yet paired it is +0.09
[−0.04, +0.23] — not a win.

Lead availability in the headline window: **54 origin days at every lead up to
d177**, 51 at d180 (a target day needs its origin to exist, and the first origin
is 2026-02-19, so from d178 the early days of the window drop out). The gate is
30 origin days, so no *slot* lead is LOW-N. Two caveats that the earlier wording
("no lead d0–d180 is LOW-N") got wrong:

- **UC4's unit is the park-month, not the park-day.** The headline window spans
  3 calendar months, so UC4's `n_origin_days` is 3 — it counts month labels, not
  sample size, and the real sample is ~200 park-months. 130 of the 3,770 cells in
  `cells.csv` have `n_origin_days ≠ 54` and 79 of those are UC4. UC4 is gated on
  park-months instead; what remains limited is *calendar* coverage (3 months),
  which is a generalisability caveat, not LOW-N.
- Leads 270 and 365 are not measurable yet; they become measurable on
  2026-12-15 and 2027-03-20.

### UC2 / UC3 — 15-min MAE from the 06:00 origin (headline window)

All rides, then ex-ante busy rides. `ref` is the per-lead reference; the paired
differences and their park-cluster CIs are the numbers that decide.

| lead | `snaive7` | `wt_med` | `clim` | `h5` | `lvlh5_naive` | `lvlh5_tft` | `lvlh5_cbd` | `prod_served` | `oracle_level` | `oracle_shape` | ref | binding naive |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 8.44 | 8.00* | 8.24 | 7.88* | 8.12 | 7.74* | 10.17 | – | 5.32* | 6.96* | `wt_med` | `wt_med` |
| 1 | 8.44 | 8.11* | 8.28 | 7.98* | 8.15 | 7.86* | 10.21 | – | 5.34* | 6.96* | `wt_med` | `wt_med` |
| 2 | 8.44 | 8.15* | 8.30 | 8.02* | 8.16 | 7.94 | 10.24 | – | 5.35* | 6.96* | `wt_med` | `wt_med` |
| 3 | 8.44 | 8.19* | 8.31 | 8.06* | 8.18 | 8.03 | 10.25 | 8.52 | 5.35* | 6.96* | `wt_med` | `wt_med` |
| 4 | 8.44 | 8.23 | 8.32 | 8.09* | 8.18 | 8.11 | 10.23 | 8.61 | 5.35* | 6.96* | `snaive7` | `wt_med` |
| 5 | 8.44 | 8.26 | 8.33 | 8.12* | 8.19 | 8.13 | 10.29 | 8.62 | 5.36* | 6.96* | `snaive7` | `wt_med` |
| 6 | 8.44 | 8.33 | 8.35 | 8.20 | 8.22 | 8.27 | 10.77 | 8.73 | 5.39* | 6.96* | `snaive7` | `wt_med` |
| 7 | – | 8.45 | 8.38 | 8.32 | 8.65 | 8.51 | 10.92 | 9.01 | 5.43* | 7.59* | `wt_med` | `wt_med` |
| 10 | – | 8.57 | 8.37 | 8.44 | 8.70 | 8.67 | 11.03 | 9.22 | 5.45* | 7.59* | `wt_med` | `clim` |
| 14 | – | 8.73 | 8.37 | 8.60 | 8.99 | 8.97 | 11.20 | 9.55 | 5.49* | 8.00* | `wt_med` | `clim` |
| 21 | – | 8.84 | 8.48 | 8.71 | 9.17 | 9.09 | 11.19 | 9.80 | 5.52* | 8.22 | `wt_med` | `clim` |
| 30 | – | 8.84 | 8.62 | 8.70 | 9.24 | 9.31 | 11.14 | 10.19 | 5.51* | 8.29 | `wt_med` | `clim` |
| 45 | – | 8.82 | 8.55 | 8.69 | 9.23 | 9.30 | (10.93) | 10.09 | 5.57* | 8.31 | `wt_med` | `clim` |
| 60 | – | 8.73 | 8.54 | 8.63 | 9.08 | (4.26) | – | (4.30) | 5.59* | 8.15 | `wt_med` | `clim` |
| 90 | – | 8.42* | 8.58 | 8.33* | 8.93 | – | – | – | 5.46* | 7.90* | `wt_med` | `wt_med` |

Ex-ante busy rides:

| lead | `wt_med` | `clim` | `h5` | `lvlh5_naive` | `lvlh5_tft` | `prod_served` | ref | binding naive |
|---|---|---|---|---|---|---|---|---|
| 0 | 15.21 | 16.03 | 14.90* | 15.11 | 13.90* | – | `snaive7` | `wt_med` |
| 1 | 15.42 | 16.10 | 15.10* | 15.12* | 14.11* | – | `snaive7` | `wt_med` |
| 2 | 15.50 | 16.11 | 15.17 | 15.14* | 14.30* | – | `snaive7` | `wt_med` |
| 3 | 15.59 | 16.10 | 15.25 | 15.15* | 14.50* | 15.68 | `snaive7` | `wt_med` |
| 4 | 15.67 | 16.11 | 15.33 | 15.17* | 14.69* | 15.85 | `snaive7` | `wt_med` |
| 5 | 15.74 | 16.13 | 15.40 | 15.17* | 14.79* | 16.01 | `snaive7` | `wt_med` |
| 6 | 15.89 | 16.17 | 15.55 | 15.23* | 15.03* | 16.17 | `snaive7` | `wt_med` |
| 7 | 16.12 | 16.22 | 15.78 | 16.22 | 15.48* | 16.73 | `wt_med` | `wt_med` |
| 10 | 16.39 | 16.18 | 16.04 | 16.30 | 15.84* | 17.31 | `wt_med` | `clim` |
| 14 | 16.76 | 16.16 | 16.42 | 17.02 | 16.51 | 18.03 | `wt_med` | `clim` |
| 21 | 17.09 | 16.34 | 16.73 | 17.47 | 16.89 | 18.70 | `wt_med` | `clim` |
| 30 | 17.21 | 16.65 | 16.86 | 17.69 | 17.54 | 19.64 | `wt_med` | `clim` |
| 45 | 17.28 | 16.82 | 16.97 | 17.75 | 17.61 | 19.67 | `wt_med` | `clim` |
| 60 | 17.37 | 17.21 | 17.17 | 17.77 | (19.64) | (20.74) | `wt_med` | `wt_med` |
| 90 | 16.72 | 17.13 | 16.55* | 17.46 | – | – | `wt_med` | `wt_med` |

`ref` is the per-lead reference (BENCH-SPEC change #4: the ladder, moved off it
only by a significant *paired* loss). **`ref` is not the comparator a win is
measured against** — change #4 fixed the selection rule, and change #5 then made
the selection almost irrelevant: `*` means the model beats **every** naive
candidate at that lead, each paired on its own shared rows, with the park-cluster
CI excluding 0. Values in brackets rest on < 30 % of the reference's paired rows
and the gates keep them out of the hand-over.

"binding naive" is `hardest_naive` from the CSVs: the candidate against which the
model's **point estimate** is worst. It is *not* necessarily the candidate that
denies the win — that is whichever comparison's CI includes 0, and a candidate
with a larger point-estimate margin can have a much wider CI. `h5` in the 2–4 h
intraday band is the example: its worst point estimate is against `wt_med`
(−0.129 [−0.158, −0.102], significant), yet it does not win the band, because
against `persistence` it is −0.385 **[−0.823, +0.023]**. Read
`cells_all_refs.csv` per candidate before quoting a blocker.

Note what the reference column does **not** say any more: under the paired rule
`clim` is **never** the per-lead reference on this metric, at any lead. The
earlier "the reference switches to climatology at d7" was an artefact of the
unpaired argmin, and so was everything derived from it.

#### What each model does against each individual naive (BENCH-SPEC change #5)

"nothing beats the reference" and "nothing beats the weekday median" are
different statements, and conflating them is what misled the first revision of
PR #445. Both are published, per lead, in `cells_all_refs.csv`. `h5`, all rides:

| lead | vs `snaive7` | vs `wt_med` | vs `clim` | beats every naive? |
|---|---|---|---|---|
| 0 | -0.69 [-1.05, -0.36]\* | -0.13 [-0.16, -0.10]\* | -0.48 [-0.67, -0.30]\* | **yes** |
| 1 | -0.59 [-0.95, -0.24]\* | -0.14 [-0.17, -0.10]\* | -0.43 [-0.63, -0.25]\* | **yes** |
| 2 | -0.55 [-0.91, -0.19]\* | -0.14 [-0.17, -0.11]\* | -0.41 [-0.62, -0.22]\* | **yes** |
| 3 | -0.51 [-0.88, -0.16]\* | -0.14 [-0.17, -0.11]\* | -0.38 [-0.59, -0.19]\* | **yes** |
| 4 | -0.47 [-0.85, -0.12]\* | -0.14 [-0.17, -0.11]\* | -0.36 [-0.57, -0.16]\* | **yes** |
| 5 | -0.44 [-0.82, -0.08]\* | -0.14 [-0.17, -0.11]\* | -0.34 [-0.56, -0.14]\* | **yes** |
| 6 | -0.36 [-0.74, +0.00] | -0.14 [-0.17, -0.11]\* | -0.30 [-0.53, -0.09]\* | no |
| 7 | – | -0.14 [-0.17, -0.11]\* | -0.22 [-0.46, +0.00] | no |
| 10 | – | -0.14 [-0.17, -0.10]\* | -0.10 [-0.34, +0.14] | no |
| 14 | – | -0.14 [-0.17, -0.10]\* | +0.07 [-0.18, +0.34] | no |
| 21 | – | -0.14 [-0.18, -0.10]\* | +0.12 [-0.14, +0.39] | no |
| 30 | – | -0.13 [-0.16, -0.09]\* | +0.05 [-0.20, +0.33] | no |
| 45 | – | -0.10 [-0.13, -0.08]\* | +0.01 [-0.22, +0.25] | no |
| 60 | – | -0.07 [-0.10, -0.05]\* | -0.02 [-0.30, +0.25] | no |
| 90 | – | -0.06 [-0.08, -0.04]\* | -0.35 [-0.63, -0.09]\* | **yes** |

`h5`, ex-ante busy rides:

| lead | vs `snaive7` | vs `wt_med` | vs `clim` | beats every naive? |
|---|---|---|---|---|
| 0 | -1.13 [-1.97, -0.29]\* | -0.30 [-0.37, -0.24]\* | -0.97 [-1.38, -0.54]\* | **yes** |
| 1 | -0.92 [-1.78, -0.07]\* | -0.31 [-0.38, -0.25]\* | -0.87 [-1.31, -0.43]\* | **yes** |
| 2 | -0.84 [-1.71, +0.03] | -0.32 [-0.39, -0.25]\* | -0.81 [-1.26, -0.35]\* | no |
| 3 | -0.76 [-1.65, +0.13] | -0.32 [-0.39, -0.25]\* | -0.73 [-1.19, -0.27]\* | no |
| 4 | -0.69 [-1.57, +0.22] | -0.32 [-0.39, -0.26]\* | -0.67 [-1.16, -0.19]\* | no |
| 5 | -0.61 [-1.51, +0.30] | -0.32 [-0.39, -0.26]\* | -0.64 [-1.15, -0.13]\* | no |
| 6 | -0.45 [-1.38, +0.47] | -0.33 [-0.39, -0.26]\* | -0.56 [-1.10, -0.03]\* | no |
| 7 | – | -0.33 [-0.40, -0.25]\* | -0.41 [-0.97, +0.14] | no |
| 10 | – | -0.33 [-0.40, -0.26]\* | -0.13 [-0.72, +0.48] | no |
| 14 | – | -0.32 [-0.40, -0.25]\* | +0.21 [-0.42, +0.90] | no |
| 21 | – | -0.34 [-0.42, -0.26]\* | +0.33 [-0.35, +1.07] | no |
| 30 | – | -0.32 [-0.39, -0.25]\* | +0.22 [-0.46, +0.95] | no |
| 45 | – | -0.27 [-0.32, -0.22]\* | +0.06 [-0.58, +0.79] | no |
| 60 | – | -0.19 [-0.26, -0.13]\* | -0.22 [-0.91, +0.58] | no |
| 90 | – | -0.14 [-0.20, -0.09]\* | -0.78 [-1.54, -0.09]\* | **yes** |

`lvlh5_tft`, all rides:

| lead | vs `snaive7` | vs `wt_med` | vs `clim` | beats every naive? |
|---|---|---|---|---|
| 0 | -0.91 [-1.15, -0.66]\* | -0.31 [-0.54, -0.06]\* | -0.80 [-1.03, -0.56]\* | **yes** |
| 1 | -0.77 [-1.01, -0.53]\* | -0.28 [-0.53, -0.04]\* | -0.69 [-0.92, -0.47]\* | **yes** |
| 2 | -0.69 [-0.93, -0.45]\* | -0.24 [-0.48, +0.01] | -0.62 [-0.85, -0.40]\* | no |
| 3 | -0.59 [-0.83, -0.33]\* | -0.19 [-0.41, +0.06] | -0.54 [-0.74, -0.34]\* | no |
| 4 | -0.50 [-0.73, -0.28]\* | -0.14 [-0.38, +0.11] | -0.47 [-0.69, -0.26]\* | no |
| 5 | -0.47 [-0.71, -0.25]\* | -0.14 [-0.38, +0.09] | -0.48 [-0.69, -0.27]\* | no |
| 6 | -0.32 [-0.56, -0.09]\* | -0.08 [-0.34, +0.18] | -0.38 [-0.59, -0.18]\* | no |
| 7 | – | +0.04 [-0.20, +0.29] | -0.15 [-0.35, +0.04] | no |
| 10 | – | +0.08 [-0.15, +0.32] | +0.02 [-0.16, +0.21] | no |
| 14 | – | +0.22 [-0.04, +0.48] | +0.35 [+0.14, +0.57] | no |
| 21 | – | +0.25 [+0.05, +0.45] | +0.45 [+0.24, +0.70] | no |
| 30 | – | +0.48 [+0.27, +0.71] | +0.64 [+0.35, +0.92] | no |
| 45 | – | +0.51 [+0.30, +0.71] | +0.61 [+0.31, +0.92] | no |
| 60 | – | +0.05 [-1.15, +1.50] | +0.46 [-0.30, +2.11] | no |

`lvlh5_tft`, ex-ante busy rides:

| lead | vs `snaive7` | vs `wt_med` | vs `clim` | beats every naive? |
|---|---|---|---|---|
| 0 | -2.27 [-2.73, -1.80]\* | -1.36 [-1.91, -0.81]\* | -2.36 [-2.80, -1.84]\* | **yes** |
| 1 | -2.02 [-2.46, -1.56]\* | -1.34 [-1.89, -0.80]\* | -2.15 [-2.57, -1.67]\* | **yes** |
| 2 | -1.82 [-2.31, -1.31]\* | -1.22 [-1.81, -0.66]\* | -1.96 [-2.39, -1.44]\* | **yes** |
| 3 | -1.58 [-2.09, -1.03]\* | -1.09 [-1.64, -0.54]\* | -1.75 [-2.15, -1.30]\* | **yes** |
| 4 | -1.39 [-1.84, -0.91]\* | -0.99 [-1.55, -0.44]\* | -1.59 [-2.05, -1.07]\* | **yes** |
| 5 | -1.28 [-1.77, -0.78]\* | -0.95 [-1.52, -0.41]\* | -1.57 [-2.01, -1.06]\* | **yes** |
| 6 | -1.01 [-1.52, -0.45]\* | -0.86 [-1.49, -0.28]\* | -1.43 [-1.87, -0.97]\* | **yes** |
| 7 | – | -0.65 [-1.20, -0.12]\* | -1.00 [-1.40, -0.54]\* | **yes** |
| 10 | – | -0.56 [-1.11, -0.05]\* | -0.59 [-1.01, -0.15]\* | **yes** |
| 14 | – | -0.26 [-0.91, +0.33] | +0.09 [-0.37, +0.62] | no |
| 21 | – | -0.20 [-0.66, +0.25] | +0.35 [-0.19, +0.94] | no |
| 30 | – | +0.35 [-0.11, +0.83] | +0.83 [+0.12, +1.60] | no |
| 45 | – | +0.39 [-0.07, +0.86] | +0.69 [-0.06, +1.54] | no |
| 60 | – | +2.03 [-1.27, +5.85] | +6.88 [+5.48, +10.77] | no |

Read across those four tables rather than down one column:

- **`h5` beats the weekday median at EVERY lead d0–d90**, all rides (−0.14 …
  −0.06) and busy (−0.34 … −0.14), every CI excluding 0. The first revision's
  claim was right; it was never refuted, only re-referenced.
- **`h5` fails the envelope at d6 because of `snaive7`** (−0.36 [−0.74, +0.00] —
  the upper bound lands on zero), not because of climatology. From d7 the
  blocker is climatology: n.s. at d7 (−0.22 [−0.46, +0.00]) and d10 (−0.10
  [−0.34, +0.14]), then genuinely *worse* at d14–d45 (+0.07, +0.12, +0.05,
  +0.01, none significant), then better again at d90 (−0.35 [−0.63, −0.09]).
- **`lvlh5_tft` on busy rides beats all three naives at every lead d0–d10 in this
  window** — at d10 still −0.56 [−1.11, −0.05] against `wt_med` and −0.59
  [−1.01, −0.15] against `clim`. **But only d0–d2 reproduces on the full period**
  (there the `wt_med` comparison is already n.s. at d3, −0.267 [−0.553, +0.002]),
  so d0–d2 is what PAR-834's routing should rest on and d3–d10 is an autumn
  extension.
- **`lvlh5_tft` on all rides only survives d0–d1.** From d14 it is significantly
  worse than climatology (+0.35 … +0.61) and from d21 worse than the weekday
  median too.

At d60 `lvlh5_tft` and `prod_served` sit on 1.5 % of the slots (TFT's horizon
ends at 60 days and only a few parks still have a level) — the values in
brackets are not comparable and the gates drop them from the hand-over.

### Hand-over (input for the serving router)

**This table is the HEADLINE WINDOW only.** It is the per-lead detail; it is not
the serving decision. Read it together with "Which results survive BOTH passes"
below — 107 of these 296 rows name a different winner on the full period, and the
busy-ride band in particular is d0–d10 here and **d0–d2** there.

| Use case / segment | Leads | Winner | Margin vs the BINDING naive |
|---|---|---|---|
| UC1 live, 15–120 min (all and busy) | m015–m120 | `persistence` | nothing beats it at any lead; the best profile (`lvlh5_tft`) is still +0.64 [+0.30, +0.96] behind at 120 min, and +0.86 [+0.26, +1.47] on busy rides |
| UC2 intraday, 2–4 h | — | `lvlh5_tft` (all and busy) | −0.31 [−0.55, −0.06] (all) / −1.03 [−1.59, −0.46] (busy) |
| UC2 intraday, 4–8 h | — | **`lvlh5_tft`** (all and busy) | −0.30 [−0.53, −0.05] (all) / −1.34 [−1.89, −0.76] (busy) |
| UC2 intraday, 8 h+ | — | `h5` (all), `lvlh5_tft` (busy) | −0.18 [−0.23, −0.14] (all) / −0.70 [−1.11, −0.27] (busy) |
| UC2 / UC3, all rides | d0–d1 | **`lvlh5_tft`** | −0.31 … −0.28 |
| UC2 / UC3, all rides | d2–d5 | **`h5`** | −0.14 |
| UC2 / UC3, all rides | d6–d60 | nothing wins | `h5` is −0.14 vs `wt_med` throughout but n.s. vs `snaive7` (d6) / `clim` (d7–d60) |
| UC2 / UC3, all rides | d90 | `h5` | −0.06 |
| UC2 / UC3, ex-ante busy | d0–d10 | **`lvlh5_tft`** | −1.36 (d0) … −0.56 (d10) |
| UC2 / UC3, ex-ante busy | d14–d60 | nothing wins | — |
| UC2 / UC3, ex-ante busy | d90 | `h5` | −0.14 |
| UC2/UC3 within-ride-day Spearman | d0–d14 / d21–d45 / d90 | `h5` / **`prod_served_lin`** / `h5` | +0.016 … +0.012 / +0.015 … +0.026 / +0.015 |
| UC3 dayPeak abs error | d0–d2 / d3 | `lvlh5_tft` / `prod_served_lin` | −0.67 … −0.38 / −0.35; nothing wins from d4 |
| D3 best time (regret, top-2 hit) | d0–d45 | `h5` | −0.42 … −0.35 min regret, +0.012 … +0.013 hit rate |
| D4 optimiser regret | d1 / d3 | `h5` / `lvlh5_tft` | −1.35 / −1.96 min per park-day; **nothing wins from d7** |
| D4 dayPeak ordering | d0–d45 / d60–d90 | `lvlh5_tft` / `h5` | +0.048 … +0.029 / +0.021 … +0.022 |
| D5 first hour | d0–d7 / d90 | `lvlh5_tft` / `h5` | −0.44 … −0.32 / −0.08; nothing wins d10–d60 |
| D5 rope-drop worth | all | nothing wins | agreements 0.917–0.925 for every candidate and every model |
| D6 crowd bucket | d1–d6 / d7–d21 / d30+ | `lvl_snaive7` / `lvl_tft` / nothing | — / +0.023 … +0.037 / — |
| D7 day comparison | d1–7 / d8–30, d31–90 | `lvl_snaive7` / `lvl_tft` | — / +0.071, +0.058 |
| UC4 daily ranking | d1–d180 | **nothing wins — and this is now a measurement** | `lvl_tft` loses at every lead and is significantly *worse* at d30 (−0.081 [−0.144, −0.011]) |
| D1 next-best ride (0–60, 0–120 min) | — | `snaive7` | nothing wins; every forecast is −0.04 … −0.08 precision |
| D2 live window 0–45 min | w0-45 | `persistence` | 6.05 vs ≥ 11.90 for any forecast |

Naive candidates are excluded from the winner column. A candidate *can* beat the
envelope of the others — `wt_med` does on all rides at d1–d3 — and
`usable_horizon.csv` keeps that, because "climatology is the best naive here" is
a real finding. But it must not be ranked against a model: the two margins are
measured against different (and for a candidate, weaker) pools, so `wt_med`
beating `clim` by 0.35 would outrank `lvlh5_tft` beating `wt_med` by 0.31 and the
serving router would be told to keep the naive. The reference pool is the
fallback, not a contender.

### Usable horizon

Largest lead up to which a model beats the whole naive envelope, contiguous from
its first scored lead. `tested` is the number of leads that were a real test
(`leads_tested = 0` means **nothing was tested**, not "nothing won" — and
`not_tested_because` in the CSV says which gate fired).

| metric | model | unit | tested | usable horizon | max significant lead |
|---|---|---|---|---|---|
| UC3 MAE, all | `h5` | park-day | 14/14 | **d5** | d90 |
| UC3 MAE, all | `lvlh5_tft` | park-day | 12/13 | **d1** | d1 |
| UC3 MAE, all | `lvlh5_naive` / `lvlh5_cbd` / `prod_served` | park-day | 14 / 11 / 10 | — | — |
| UC3 MAE, busy | `lvlh5_tft` | park-day | 12/13 | **d10** | d10 |
| UC3 MAE, busy | `lvlh5_naive` | park-day | 14/14 | d6 | d6 |
| UC3 MAE, busy | `h5` | park-day | 14/14 | d1 | d90 |
| UC2 intraday MAE, all | `lvlh5_tft` / `h5` | park-day | 3/3 | h4–8 / — | h4–8 / h8+ |
| UC2 intraday MAE, busy | `lvlh5_tft` / `h5` | park-day | 3/3 | h8+ / — | h8+ / h8+ |
| D5 first-hour MAE | `h5` / `lvlh5_naive` / `lvlh5_tft` / `lvlh5_cbd` | park-day | 15 / 15 / 13 / 12 | d7 / d7 / d7 / d6 | d90 / d7 / d7 / d6 |
| D3 best-time regret / top-2 hit | `h5` | park-day | 15/15 | d45 | d90 / d45 |
| D4 optimiser regret | `h5` / `lvlh5_tft` | park-day | 5/5 | **d3** / — | d3 / d3 |
| D4 dayPeak ordering | `h5` / `lvlh5_tft` / `lvlh5_naive` / `prod_served_lin` | park-day | 15 / 13 / 15 / 10 | d90 / d45 / d30 / d45 | d90 / d45 / d90 / d45 |
| UC2/UC3 within-ride-day Spearman | `h5` | park-day | 15/15 | d45 | d90 |
| D6 crowd bucket | `lvl_tft` | park-day | 12/13 | — (loses d1–d6) | d21 |
| D7 day comparison | `lvl_tft` | park-origin | 3/3 | — (loses d1–7) | d31–90 |
| **UC4 Spearman** | `lvl_tft` | **park-month** | **12/13** | — | — |
| UC4 Spearman | `lvl_snaive7` / `lvl_naive4` / `lvl_clim` | park-month | 6 / 16 / 16 | d6 / — / — | d6 / d120 / d14 |

**UC4 is tested now, and that is a change of kind, not of degree.** Before
BENCH-SPEC change #3 every UC4 cell was gated out on `n_origin_days = 3` (three
month *labels*), `usable_horizon.csv` read `leads_tested = 0` for all six level
sources, and the PR quoted that emptiness as "no level source wins". The unit is
the park-**month**: there are **175–189 park-months per lead** at d1–d45, every
lead clears the 30-unit gate, and the answer is now a real one — `lvl_tft` loses
to the best naive daily ranking at every lead and is significantly worse at d30
(−0.081 [−0.144, −0.011]). "Tested and lost" is a much stronger basis for
"the crowd calendar stays naive" than "never tested" was.

A usable horizon shorter than the max significant lead means a lead in between
breaks the run. For `h5` on UC3 MAE all rides that lead is **d6**, and the
candidate that breaks it is `snaive7` (−0.36 [−0.74, +0.00]) — not climatology.
`h5`'s own margin against the weekday median is −0.14 at every lead from d0 to
d60; what it cannot do is separate from climatology between d7 and d60.

### The origin-constant pass: the d10 band is real for autumn origins, and not an origin-drift artefact

The third view is in (`tables_o2026-08-15_2026-10-07/`, `report-tables_o2026-08-15_2026-10-07.json`, git `53165b5f`, 699 s). It holds the **origin** window at 2026-08-15 … 10-07 and lets the targets drift, which is the only one of the three views that takes the origin-season gradient off the lead axis.

**First, its honest validity range.** The origin window is fixed, but a lead-L origin needs its target inside the truth, so the late — autumn — origins drop out as the lead grows:

| lead | origins scored | summer / autumn | headline window's mix, for comparison |
|---|---|---|---|
| d0 | 54 | 31 % / **69 %** | 31 % / 69 % |
| d3 | 51 | 33 % / **67 %** | 37 % / 63 % |
| d7 | 47 | 36 % / **64 %** | 44 % / 56 % |
| d10 | 44 | 39 % / **61 %** | 50 % / 50 % |
| d14 | 40 | 42 % / 58 % | 57 % / 43 % |
| d30 | 24 | 71 % / 29 % | 87 % / 13 % |
| d45 | 9 | 100 % / 0 % | 100 % / 0 % |

So it is a **good control over d0–d10** — the mix moves only 69 % → 61 % autumn, against the headline window's 69 % → 50 % — and it **degenerates past d14**, where it is majority summer on 24–40 origins. It cannot speak to long leads, and I am not using it there.

**Inside that range it answers the question.** `lvlh5_tft`, ex-ante busy, paired margins with both CIs:

| lead | vs `wt_med` (park-cluster) | (park-day) | vs `clim` (park-cluster) | beats envelope? |
|---|---|---|---|---|
| 0 | −1.361 [−1.908, −0.814] | [−1.594, −1.126] | −2.363 [−2.800, −1.835] | **yes** |
| 2 | −1.269 [−1.876, −0.678] | [−1.508, −1.016] | −1.967 [−2.427, −1.426] | **yes** |
| 3 | −1.151 [−1.735, −0.550] | [−1.390, −0.900] | −1.718 [−2.163, −1.224] | **yes** |
| 6 | −1.058 [−1.752, −0.430] | [−1.309, −0.802] | −1.388 [−1.866, −0.874] | **yes** |
| 7 | −0.897 [−1.494, −0.324] | [−1.150, −0.641] | −0.974 [−1.461, −0.452] | **yes** |
| **10** | **−0.988 [−1.640, −0.376]** | [−1.257, −0.705] | **−0.632 [−1.175, −0.036]** | **yes** |
| 14 | −0.708 [−1.479, **+0.045**] | [−1.011, −0.428] | **+0.201** [−0.439, +0.906] | no |

`paired_share` is 0.973–0.980 throughout, so this is not a coverage effect. The band ends **cleanly at d10** and the candidate that ends it is climatology.

**The three views side by side, ex-ante busy:**

| view | what it fixes | origin season | band |
|---|---|---|---|
| headline window | the target days | drifts 69 % → 50 % autumn over d1–d10 | **d0–d10** |
| full period | nothing | year-round, ~16 % autumn in the d3–d6 cells | **d0–d2** |
| **origin-constant** | the origin days | drifts 69 % → 61 % autumn over d0–d10 | **d0–d10** |

**What it decides.** The d10 boundary is **not** an artefact of origin-season drift along the lead axis: remove most of that drift and d10 reproduces, with every CI excluding 0 against both binding naives. So the full period's d0–d2 is not a correction of the headline window — **it is a year-round average that weights the season where the TFT level helps at about one sixth.** Both numbers are right about different populations, and the answer to "d0–d2 or ≤ d6?" is neither: **the band is d0–d10 for late-summer/autumn-shoulder origins and d0–d2 as a year-round floor.** It is season-conditional, not undecidable.

**Consequence for PR #446, which is the decision this was run for.** The shipped ≤ d6 scope is neither right nor wrong — it is **unconditioned**: too wide for a year-round rule, too narrow for the autumn shoulder. My earlier ask was "narrow to d0–d2 **or** gate it seasonally". This pass makes the seasonal effect credible but **does not make the gate shippable** — its other arm is unmeasured and its condition is not estimable from one partial autumn, so it is recorded as a **hypothesis** and the guidance is **narrow to flat d0–d2** (see "Why the seasonal gate is a HYPOTHESIS"). A flat ≤ d6 remains the one choice the evidence does not support, because it is wrong in both directions at once.

**`h5` shows the mirror, which is what makes the mechanism credible.** Its band is d0–d5 in *both* autumn-heavy views and **d0–d45 on the full period** — wider year-round, narrower in autumn. The reason is not `h5`: its own margin over the weekday median is season-neutral and significant in both halves (−0.199 autumn, −0.030 summer at d1). What moves is the naive it has to beat — **climatology beats the weekday median only in autumn**, so in autumn-heavy data `clim` is a stronger blocker and caps `h5` at d5. So the "d6–d60 dead band" is an autumn phenomenon, not a horizon limit, and the same seasonal fact explains both the TFT level's autumn gain and `h5`'s autumn cap.

**Reconciled with the heartbeat result — and the two are different kinds of result, so they must not be run together.** Dropping the carried-forward truth slots **extends** the band: all six hand-over flips gain significance, none loses, and `h5` all-rides goes d0–d5 → d0–d6. Fixing the origin window **extends nothing** — it returns the *same* d0–d10 band as the headline pass, and that identity is the whole point: it is what establishes that the headline's d10 was **not an origin-drift artefact**. The d2-versus-d10 gap is therefore **season weighting, not drift.** (An earlier phrasing of mine — "removing the origin-season drift extends the band from d2 to d10" — was wrong and is withdrawn; the origin pass changed no boundary, it removed an alternative explanation for one.) Net effect: **d0–d2 is a floor that no tested perturbation moves inward**, and both the open heartbeat decision and the season question can only widen the band from it, so acting on d0–d2 is one-sided-safe in both dimensions at once.

**Three things that strengthen this result, which I did not claim and the critic found.**

1. **The self-imposed d0–d10 bound is corroborated by the harness, independently of my judgement.** `tables_o2026-08-15_2026-10-07/lead_availability.csv` applies the ≥ 30-origin-day gate on its own and marks **d30 LOW-N (24 origin days)** and **d45 LOW-N (9)**, with d60 "not measurable yet" (0) — so the leads I excluded on season-mix grounds are the leads the gate excludes on sample-size grounds. Two independent reasons, same boundary.
2. **The residual drift runs CONSERVATIVE, so the published margin is a lower bound.** The autumn share still falls 69 % → 61 % across d0–d10, and what dilutes it is summer, where `lvlh5_tft` is *worse*. So the true autumn-origin margin at d10 is if anything **larger** than the −0.988 [−1.640, −0.376] published here. The bias in this pass points away from the conclusion it supports.
3. **This pass contains the cleanest defence of the envelope criterion (recorded change #4) on PAR-827's own data.** At d21 the `wt_med` comparison turns significant again — **−1.048 [−1.686, −0.455]** — while `clim` is **+0.349 [−0.485, +1.288]**. A single-reference rule pointed at `wt_med` would have published a d21 win; the envelope refuses it, because climatology denies it. That is exactly the failure mode change #4 exists to prevent, caught on live data rather than argued from first principles.

**The mechanism behind all of this is confirmed in three passes, and one step of it is interpretation rather than measurement.** `h5` beats the weekday median at **15/15 leads in the headline pass, 15/15 on the full period and 13/13 in the origin-constant pass** (13 leads are scored there), with mean margins of −0.125 / −0.083 / −0.170 — so the effect is **sign-and-significance invariant** across every view while being about **twice as large** in the autumn-heavy ones. And `clim − wt_med` is negative **only in autumn, at every lead** (d1 −0.135, d7 −0.302, d14 −0.565, d30 −0.702) against +0.16 … +1.23 in winter, spring and summer — a three-point dose-response across the three passes. That is why climatology caps `h5` in autumn and not elsewhere, and it is measured. **Extending the same common cause to the TFT level is plausible but NOT shown** — that the level source and climatology both gain in autumn is consistent with one driver, and this review does not demonstrate it. Marked as interpretation.

**Residual limit, stated because all three views share it:** every one of them is either ≥ 58 % autumn or a year-round average weighted toward summer. None is a holiday-rich or peak-summer window. "The autumn shoulder" is the only season for which this review has a measured band, and the year-round floor is an average, not a measurement of any other season.

### Why the seasonal gate is a HYPOTHESIS and not the recommendation

I proposed gating the busy routing on season. That is the right reading of the effect and the wrong thing to ship, for two reasons that are about identifiability rather than about the measurement.

**1. The "otherwise" arm was never separately measured.** The d10 arm comes from an origin-constant pass that is 61–69 % autumn. The d2 arm does **not** come from a non-autumn pass — it comes from the blended full period, which is itself still about one sixth autumn. So I have one measured arm and one blended average standing in for the other. **The cheap measurement that would fix this is one more origin-constant pass over the existing `parts/`**, with the origin window set to a non-autumn stretch (e.g. `--origin-from 2026-06-01 --origin-to 2026-07-31`): ~12 minutes of CPU, no re-run, and it would give the non-autumn arm directly instead of by subtraction.

**2. The gate's condition cannot be estimated from this data at all, which is the harder objection.** Truth begins 2025-12-24, so the export contains **exactly one partial autumn**. "Autumn" and "September–October 2026" are therefore the same rows, and nothing here can separate a seasonal effect from a one-off property of those particular weeks — a late-season event calendar, a park-closure pattern, a single operator's schedule change. Distinguishing them needs a second autumn, i.e. roughly **2027-09**. **A gate whose condition cannot be estimated is not implementable, however real the effect behind it is**, and shipping one would encode "Sep–Oct 2026" as a production rule under a seasonal name.

**So the serving guidance is: narrow PR #446 from flat ≤ d6 to flat d0–d2.** That is the floor **all five views agree on** — headline window, full period, origin-constant, and both heartbeat arms — and no tested perturbation moves it inward. **`d10`-for-autumn is recorded as a hypothesis**, with the non-autumn origin-constant pass named above as the test that would settle its other arm, and a second autumn (≈ 2027-09) as what would make the condition estimable. This is also the coordinator's hand-over position, deliberately, so the PR and the run summary say the same thing.

What does **not** change: flat ≤ d6 remains the one choice unsupported in either direction, because d3–d6 is n.s. year-round and d7–d10 is left on the table in autumn.

### Which results survive BOTH passes — read this before acting on anything

This is the section critic B2 asked for, and it changes the recommendation. Of the
296 hand-over rows present in both passes, **189 (64 %) name the same winner** and
**103 name the same non-naive model**. Those 103 are the only rows a year-round
serving change should rest on. Everything else is a one-pass result Of the 229 usable-horizon rows present in both passes, **53 report a different horizon** (32 of them for non-naive models)., and on this
data "one pass" almost always means "the autumn shoulder season".

| surface | robust in BOTH passes | headline only (autumn) | full period only |
|---|---|---|---|
| UC3 slot MAE, all rides | `h5` **d2–d5** and d90 | `lvlh5_tft` d0–d1 | `h5` to **d45** |
| UC3 slot MAE, ex-ante busy | `lvlh5_tft` **d0–d2**; `h5` d90 | `lvlh5_tft` **d3–d10** | `h5` to **d45** |
| UC2 intraday | `lvlh5_tft` busy h2–4 and h4–8; `h5` all h8+ | `lvlh5_tft` all h2–8, busy h8+ | `h5` all/busy h8+ |
| D5 first hour | **`lvlh5_tft` d0–d7** | — | to d14–d45 |
| D4 dayPeak ordering | **`lvlh5_tft` d0–d45** | — | — |
| D4 optimiser regret | `h5` d1, `lvlh5_tft` d3 | — | `h5` to d30 |
| D3 best time (regret + both hit rates) | **`h5` d0–d45** | — | to d60 |
| within-ride-day Spearman | `h5` d0–d14 and d90; **`prod_served_lin` d21–d45** | — | `h5` to d90 |
| UC3 dayPeak abs error | `lvlh5_tft` d0; `prod_served_lin` d3 | `lvlh5_tft` d1–d2 | to d5 |
| D6 crowd bucket | **nothing** | `lvl_tft` d7–d21 (+0.023 … +0.037) | — |
| D7 day comparison | **`lvl_tft` d31–90** (+0.026 [+0.007, +0.045]) | `lvl_tft` d8–30 (+0.071) | — |
| UC4 daily ranking | **nothing wins in either pass** | — | — |

**The load-bearing correction.** The busy-ride band is **d0–d2**, not d0–d10. On
the full period `lvlh5_tft` beats the weekday median on busy rides only to d2
(d3 −0.267 **[−0.553, +0.002]**, d7 +0.033, and significantly *worse* from d30 at
+0.420 [+0.116, +0.708]), while it stays ahead of climatology to d10. The d10
figure in the headline window is real for that window and **autumn-specific**;
widening PAR-834's routing to d10 would be a year-round change justified by 37
autumn days, which is exactly what critic B2 warned against.

**The mirror correction, in the models' favour.** `h5` is *stronger* on the full
period than in the window: it beats the whole envelope at **every lead d0–d45 and
again at d90**, on all rides and on busy rides, failing only at d60 (against
climatology, −0.165 [−0.394, +0.023]). Combined with "it beats the weekday median
at every lead in both passes", `h5` is the one profile result that is not
seasonal — and it is therefore the one to serve.

**The TFT *level* is autumn-specific almost everywhere it wins.** Its full-period
wins are confined to the opening hour (D5 d0–d7), dayPeak ordering (d0–d45), busy
slots to d2, and the d31–90 day comparison. D6's crowd-bucket win disappears
entirely (full period d7 +0.005 [−0.007, +0.018], d14 +0.007, d21 +0.006 — none
significant), and D7's d8–30 win collapses to **−0.0004 [−0.015, +0.015]**. Where
the level helps robustly is where the *ramp* matters, not the whole day.

**One UC4 nuance, stated so it is not over-read.** `lvl_tft` fails to beat the
best naive daily ranking in **both** passes, which is the robust finding. The
headline window additionally shows a *significant* loss at d30
(−0.081 [−0.144, −0.011]); the full period does not confirm it
(−0.036 [−0.077, +0.007]) on 264 park-months. So "the TFT level does not win UC4"
is robust; "it is significantly worse at d30" is a headline-window result only.

### What the numbers say

- **UC1 / D2 stay with production's live replacement.** `persistence` beats every
  naive and every profile at every lead to 2 h (2.46 min at 15 min, 7.58 at
  120 min) and owns the live window 0–45 min on headliners (6.05 vs ≥ 11.90). The
  best profile is still +0.64 [+0.30, +0.96] behind at 120 min on all rides and
  +0.86 [+0.26, +1.47] on busy ones.
- **The hand-over out of persistence happens inside the 2–4 h band, and it is
  now significant.** At 2–4 h `lvlh5_tft` beats `persistence` by −0.43
  [−0.77, −0.12] on all rides and −1.03 [−1.59, −0.46] on busy ones, while
  `persistence` is still level with the weekday median there (+0.22
  [−0.20, +0.67]). By 4–8 h `persistence` has collapsed to +3.02 [+2.37, +3.75].
  So the crossover is earlier than "either model is fine to 4 h": a level-scaled
  profile is already measurably better at 2–4 h.
- **The TFT level pays for itself on busy rides — robustly to d2, and to d10 in
  the autumn window only.** `lvlh5_tft` is −1.36 [−1.91, −0.81] at d0 and still
  −0.56 [−1.11, −0.05] vs `wt_med` and −0.59 [−1.01, −0.15] vs `clim` at d10 *in
  the headline window*. On the full period the `wt_med` comparison stops being
  significant at d3 (−0.267 **[−0.553, +0.002]**) and reverses by d30
  (+0.420 [+0.116, +0.708]), so **only d0–d2 survives both passes** — PAR-834's
  routing should rest on d0–d2, not d10. See "Which results survive BOTH passes".
  On *all* rides the model only survives d0–d1 even in the window, the full
  period gives it nothing, and from d14 it is significantly worse than
  climatology.
- **H5 beats the weekday median at every lead d0–d90 — that claim was never
  refuted.** −0.13 … −0.06 on all rides, −0.34 … −0.14 on busy, every CI
  excluding 0. What it cannot do is beat the *whole* envelope past d5: at d6 the
  blocker is `snaive7` (−0.36 [−0.74, +0.00]) and from d7 to d60 it is
  climatology — n.s. at d7 (−0.22 [−0.46, +0.00]) and d10 (−0.10 [−0.34, +0.14]),
  genuinely worse at d14–d45 (+0.07, +0.12, +0.05, +0.01, none significant),
  better again at d90 (−0.35 [−0.63, −0.09]). The previous wording — "the
  reference switches to climatology at d7" — described the discarded argmin rule,
  not this result.
- **So the honest statement of the long-lead gap** is not "nothing beats
  climatology". It is: *between d6 and d60 no model separates from the best naive
  at 95 % park-cluster confidence, and the naive that blocks it is `snaive7` at
  d6 and climatology from d7; climatology itself is only better than H5 by
  +0.01 … +0.12 min and never significantly.* That is the gap PAR-828 / 829 / 830
  are measuring against, and it is a narrower gap than the first revision said.
- **Level, not shape, is the error.** At d1 a perfect level takes H5 from 7.98 to
  **5.34**; a perfect shape on the naive level gives 6.96. It holds at every lead
  (d30: 5.51 vs 8.70). Nothing in the benchmark closes it.
- **The production composed tier (`prod_served`) loses at every lead from d3.**
  Against the per-lead reference: +0.41 [+0.14, +0.71] at d3, +0.70
  [+0.42, +1.01] at d7, +1.00 [+0.65, +1.36] at d14, +1.41 [+1.07, +1.76] at d30
  — and against the *hardest* naive it is worse still (+1.19 at d14, +1.60 at
  d30). Worst in the opening hour (6.21 against `h5` 4.32 and `lvlh5_tft` 4.33,
  +1.45 [+1.18, +1.73] vs the hardest naive) and on the optimiser. It never wins
  a slot-MAE row at any lead.
- **…with one genuine exception that the first revision missed.**
  `prod_served_lin` — the linearly interpolated variant — **wins the
  within-ride-day Spearman at d21–d45** (+0.015 … +0.026 vs climatology) and the
  dayPeak absolute error at d3, where no profile does. Production's composed
  curve is bad at *levels* and competitive at long-lead *ordering*. That does not
  change the replacement recommendation (the surfaces it loses are the ones users
  see) but "it never wins anything, at any lead, on any metric" was wrong.
- **The CatBoost-daily level makes things worse than no level.** `lvlh5_cbd`
  10.21 at d1 against `h5` 7.98, +2.12 [+1.62, +2.68] against `wt_med` — a
  P50-like level against a P90 target, on 84 % of the reference's rows.
- **UC4 / D6 / D7: nothing beats the naive level in the first week, and UC4 is
  now a measurement rather than an empty gate.** `lvl_snaive7` beats the other
  naives at d1–d6 (UC4 Spearman 0.33, crowd bucket exact 0.434, day comparison
  0.519). `lvl_tft` loses UC4 at **every** lead in **both** passes — on 175–189
  park-months in the window and 264–410 on the full period. In the window it wins
  the crowd bucket at d7–d21 (+0.023 … +0.037) and the day comparison at d8–30
  (+0.071) and d31–90 (+0.058), but **on the full period only the d31–90 day
  comparison survives** (+0.026 [+0.007, +0.045]): the crowd-bucket win vanishes
  (d7 +0.005 [−0.007, +0.018], d14 +0.007, d21 +0.006) and d8–30 collapses to
  −0.0004 [−0.015, +0.015]. The window's *significant* UC4 loss at d30
  (−0.081 [−0.144, −0.011]) is also not confirmed on the full period
  (−0.036 [−0.077, +0.007]), so "does not win UC4" is the robust claim and
  "significantly worse" is window-only. Within-month ranking is weak everywhere: the best source
  reaches Spearman 0.33 at d1 and only 0.20–0.24 from d14 (0.28 at d90, where
  `lvl_wt56` takes over), against 0.13–0.17 for `lvl_tft` over the same band.
- **D8 — and this number changed, because the table used to be the wrong
  window.** Until critic S13 the `d8tft` parquet was loaded with no window
  filter, so the "headline" production-calibration table was a byte-identical
  copy of the 231-origin one. On the real 54-day window production's published
  `expectedError` is **not** flat at 1.00–1.04: realised ÷ stated runs 1.03–1.08
  at d1 and then splits by band — busy 1.09 (d3), 1.11 (d7), 1.15 (d14),
  **1.18 (d30)**, 1.13 (d60), while quiet falls 1.02 (d3) → 0.95 (d14) → 0.88
  (d30) → **0.79 (d60)**. Production under-states the error it will make on busy
  rides and over-states it on quiet ones, and the spread widens with lead.
  `wt_med`'s empirical q80 covers 0.846 and q95 0.920 at d1 — q80 conservative,
  q95 short. `prod_served` has no interval at all.
- **D9: "has a forecast" is not an open/closed decision.** The weekday median
  produces a curve for 83 % of the ride-days a ride never operated on in the
  headline window (76 % over the full period), so coverage has to be scored
  separately from the wait error.

#### One reproducibility limit, measured

Two report passes over the same `parts/` that differ **only** in DuckDB thread
count (4 vs 2) agree on every published verdict: `usable_horizon.csv` is
byte-identical, `handover.csv` has no change of reference, winner or margin
beyond 1.8 × 10⁻¹⁵, and no cell changes `wins`. They do **not** agree bit for bit
on UC4's park-month metrics: the Spearman point estimate moves by ≤ 0.0019
(≈ 1 % relative), its park-cluster bounds by ≤ 0.0021 (≤ 0.020 on the single
gated one-park-month d60 cell), and the busy-day predicted set by ≤ 9 park-days
out of ~1,400. The park-month counts are identical and `iqr_true` differs only at
7 × 10⁻¹⁵, so the inputs are the same values in a different **row order**: the
`levels` query carries no `ORDER BY`, and two UC4 computations are tie-order
sensitive — `rank(method="first")` for the true top quartile, and the `p >= q75`
boundary when predictions tie. No slot-MAE, hand-over or usable-horizon number is
affected. **Follow-up:** make those two tie-breaks deterministic. It was not
landed with this pass on purpose — changing the UC4 computation after the pass
was produced would reintroduce exactly the code-vs-tables divergence that
recorded change #7 exists to prevent.

### The season split — why the two passes disagree

`mae_by_season.csv` splits the headline window into its **17 summer days**
(Aug 15–31) and **37 autumn days**, on the same cells. This is the reason the two
passes reverse, and it is a scope limit on every TFT-level recommendation. MAE in
minutes, all rides, from the 06:00 origin:

| lead | season | slots | `wt_med` | `clim` | `h5` | `lvlh5_tft` |
|---|---|---|---|---|---|---|
| 0 | summer | 1.24 M | 7.752 | 8.495 | **7.719** | 7.892 |
| 0 | autumn | 1.80 M | 8.176 | 8.076 | 7.983 | **7.629** |
| 3 | summer | 1.23 M | 7.923 | 8.494 | **7.887** | 8.172 |
| 3 | autumn | 1.79 M | 8.378 | 8.191 | 8.173 | **7.927** |
| 7 | summer | 1.23 M | 8.224 | 8.494 | **8.198** | 8.737 |
| 7 | autumn | 1.78 M | 8.605 | 8.304 | 8.396 | **8.355** |
| 30 | summer | 1.12 M | 8.661 | 9.241 | **8.622** | 9.115 |
| 30 | autumn | 1.76 M | 8.946 | **8.244** | 8.751 | 9.442 |

Three things follow, and they are the load-bearing reading of this whole page:

1. **The TFT level's advantage is autumn-only.** `lvlh5_tft` beats `wt_med` by
   0.45–0.55 min in autumn and is **worse** in summer at every lead shown (+0.14
   at d0, +0.25 at d3, +0.51 at d7). It is not a coverage artefact: its
   `paired_share` is 0.98 in this window. So "serve `lvlh5_tft`" is a statement
   about the autumn shoulder season on this evidence, not a year-round one.
2. **Climatology's advantage is autumn-only too**, and far larger (−0.70 at d30).
   That is why the old argmin reference handed the reference to `clim` from d7 in
   this window and never did so in the full period below d60 — the reference was
   tracking the season mix, not the lead.
3. **H5's advantage over the weekday median is the one effect that is NOT
   seasonal, and it is why `h5` is the thing to serve.** `h5` is better than
   `wt_med` in both halves of this window at every lead (summer −0.02 … −0.04,
   autumn −0.19 … −0.21) and in **autumn, spring and summer** on the full period
   (d1 −0.199 / −0.084 / −0.030; d7 −0.209 / −0.106 / −0.047; d30 −0.195 /
   −0.137 / −0.107). The one exception is **winter** (2 % of the data, 256 k
   slots at d0), where `h5` is **+0.036 min worse at d0, +0.029 at d1, +0.020 at
   d3 and +0.007 at d7** — a deficit that shrinks with lead and is an order of
   magnitude below the autumn gain.

   The contrast with the TFT *level* on the same table is the whole story: on the
   full period `lvlh5_tft` beats `wt_med` by −0.50 in autumn at d1 but is
   **+0.23 worse in summer**, and at d7 it is +0.62 worse in spring and +0.48
   worse in summer. That is the difference between a profile-shape improvement,
   which transfers across seasons, and a level-source improvement, which does
   not.

### Heartbeat contamination of the target — measured, owner decision open

`queue_data` carries `is_heartbeat` rows: when the feed drops a ride, production
writes the previous status **and wait** forward with a fresh `ts`. The harness
exports the column but does not filter on it, and because each heartbeat is a new
`ts` it **resets the 3 h staleness cap** the truth definition depends on — so a
ride that stopped reporting keeps producing truth with a frozen wait.

Measured by rebuilding the truth from the **same, untouched** raw export with the
filter on (`/data/parkfan/ml-bench/exports/20261010-nohb`, whose `raw/` is a
read-only symlink to `20261009/raw`):

| period | truth slots with heartbeats | without | dropped |
|---|---|---|---|
| 2025-12 … 2026-08 | 11.6 M | 11.6 M | **0.000 %** (the column was not populated) |
| 2026-09 | 1,383,904 | 1,344,445 | **2.851 %** |
| 2026-10 (1–9) | 463,637 | 432,999 | **6.608 %** |
| whole export | 13,455,954 | 13,385,859 | 0.521 % |
| **headline window, summer half** (Aug 15–31) | 1,239,702 | 1,239,703 | **0.000 %** |
| **headline window, autumn half** (Sep 1 – Oct 7) | 1,805,151 | 1,735,059 | **3.883 %** |
| 2026-10-05 alone | 55,866 | 48,724 | **12.78 %** |

So the target changes character **across the headline window**: its summer half is
completely clean and its autumn half is up to ~13 % carried-forward by the end.
Those slots are systematically flat. That is an unremoved confound of exactly the
summer/autumn contrast above — and it is *asymmetric in the same direction*, so
it cannot be waved away.

What it does **not** do, measured rather than assumed, is move the level: on the
56,324 ride-days both builds share in 2026-09-01 … 10-07 the mean ride-day P90 is
**24.7096 with heartbeats and 24.7340 without** — a 0.024 min difference, 0.1 %.
The carried-forward slots are flat but they sit near the day's own typical wait,
so they neither inflate nor deflate a P90 materially.

(Removing heartbeats is a near-subset operation, not an exact one: deleting a row
lengthens the *preceding* real row's forward fill, so 2 slots out of 13.5 M
survive under a different parent row. No slot's value changes, because a
heartbeat carries the same wait as the row before it.)

#### The scoring sensitivity, measured on a matched run pair

`build --drop-heartbeats` builds the other target, and two runs that differ in
**nothing else** score it: `results/20261010-sens-hb` (export `20261009`) and
`results/20261010-sens-nohb` (export `20261010-nohb`), both 68 daily origins
2026-08-01 … 10-07, leads 0–14, reported on the headline target window. Same raw
export, same runner, same report code — the `report-*.json` in each dir pins it.
The comparison is a committed script,
`results/20261009-baselines-v2/heartbeat_sensitivity.py`, so every number below
can be re-derived from the committed tables alone.

**1. How much of the target it is.** 2.23–2.29 % of the headline window's truth
slots exist only because of a heartbeat (64–68 k of ~2.9 M per lead).

**2. All of it is in the autumn half.** The column was not populated before
~2026-09-06, so the window's 17 summer days are **bit-identical** between the two
arms and its 37 autumn days carry 3.81–3.86 %:

| lead | season | share dropped | `wt_med` | `h5` | `lvlh5_tft` |
|---|---|---|---|---|---|
| d0 | summer | 0.00 % | 7.752 → 7.752 | 7.719 → 7.719 | 7.892 → 7.892 |
| d0 | autumn | 3.86 % | 8.176 → 8.364 (**+0.188**) | 7.983 → 8.165 (**+0.182**) | 7.629 → 7.760 (**+0.131**) |
| d3 | autumn | 3.84 % | 8.378 → 8.559 (+0.182) | 8.173 → 8.350 (+0.177) | 7.927 → 8.063 (+0.136) |
| d7 | autumn | 3.82 % | 8.605 → 8.787 (+0.182) | 8.396 → 8.574 (+0.178) | 8.355 → 8.495 (+0.140) |
| d14 | autumn | 3.81 % | 8.861 → 9.041 (+0.180) | 8.652 → 8.828 (+0.176) | 8.831 → 8.984 (+0.153) |

Every autumn MAE on this page is therefore **0.13–0.19 min optimistic**, and
every summer one is exact. The contamination sits on precisely the half of the
window that drives the season reversal above — the two confounds are stacked.

**3. It does not explain the season reversal.** That was the obvious worry and it
is false: `lvlh5_tft`'s advantage over `wt_med` in autumn *grows* slightly when
the slots are removed (−0.547 → −0.604 at d0) while its summer deficit is
unchanged (+0.140). The reversal is seasonal, not an artefact of carried-forward
truth.

**4. The bias has a direction: it flatters `persistence`.** A heartbeat slot
carries a *frozen* wait, which is exactly what `persistence` predicts — so the
contamination helps it, and the longer the horizon the more a real queue would
have moved while the frozen one did not. Dropping the slots costs `persistence`
+0.061 min at 15 min but **+0.275 min at 120 min**, against +0.101 → +0.108 for
`h5`. Its lead at 2 h shrinks by 0.111 min (17 %) against the best profile
`lvlh5_tft` (+0.636 → +0.525) and by 0.077 min (10 %) against `h5`
(+0.763 → +0.686), and two regional UC1 wins at m120 (EU all rides, Asia busy)
stop being wins. PAR-828 predicted
this sign from its Chronos-2 run before it was measured here; it means UC1
results on this target are **conservative for any model that is not persistence**.

**5. The hand-over bands DO change, and one of them is the band this PR asks the owner to act on.** My earlier "nothing load-bearing flips" was **false** and is withdrawn. Comparing the two arms' own `handover.csv` — both re-reported today on one report build, so this is not a provenance artefact — over the 213 rows present in both:

| quantity | differs between the arms |
|---|---|
| winners | **6 of 213** |
| references | 9 |
| envelope sets | 14 |
| largest `paired_margin` move | **0.912 min** (UC2 busy d0) |

The six winner flips, and every one gains significance when the carried-forward slots go:

| metric | seg | lead | heartbeats KEPT | heartbeats DROPPED |
|---|---|---|---|---|
| UC3 MAE | all | **d2** | `h5` −0.136963 (sig 1) | **`lvlh5_tft` −0.260504 (sig 2)** |
| UC3 MAE | all | **d6** | `snaive7`, nothing wins (sig 0) | **`h5` −0.383984 (sig 1)** |
| UC3 MAE, schedule known | all | d2 | `h5` (sig 1) | `lvlh5_tft` (sig 2) |
| UC3 dayPeak abs error | all | d4 | `snaive7`, nothing wins | `prod_served_lin` (sig 4) |
| UC3 dayPeak abs error | all | d5 | `snaive7`, nothing wins | `prod_served_lin` (sig 4) |
| UC3 dayPeak abs error | all | d6 | `snaive7`, nothing wins | `prod_served_lin` (sig 4) |

(The `sens-hb` arm reproduces the published headline hand-over exactly at both leads, so the arms are comparable and the published table is the heartbeats-kept one.)

**The consequence, and it is the most important caveat in this review: the serving recommendation is conditional on a truth definition the owner has not chosen yet.** The two leads that move are precisely the two the recommendation turns on — **d2**, where the narrowed PR #446 ask terminates, and **d6**, whose boundary this page already admits rests on `snaive7`'s upper bound landing on +0.0038. Under the heartbeats-dropped target the all-rides band *extends*: `lvlh5_tft` wins d2 outright and `h5` wins d6, so the dead band starts later than d6. BENCH-SPEC #6 records the heartbeat question as an **open owner decision**. So: **do not read "narrow #446 to d0–d2" as settled. The d2 boundary itself moves under the other truth definition, in the direction of giving the models more, and the choice between the two targets has to be made before that boundary is treated as a number.**

**And the risk is ONE-SIDED, which is more useful than "it is conditional".** All six flips go the same way — every one *gains* significance when the carried-forward slots are dropped (sig 1→2, 0→1, 1→2, 0→4, 0→4, 0→4); **not one loses it.** The UC3 all-rides served band is **d0–d5 with heartbeats kept and d0–d6 with them dropped** (`h5` −0.383984, sig 1 at d6), and `lvlh5_tft` at d2 goes from no-win to a win. So **the open heartbeat decision can only extend the served band, never narrow it.** The published recommendation is therefore the **conservative end**: if the owner later chooses the queue-based truth, every band in it widens and none shrinks. That tells the owner which way they can be wrong, and it means acting on the published bands is safe against the decision while *widening* on them is not. Nothing here tells the owner which target is right; it tells them the answer depends on it.

**This is a product decision, not a bug to fix silently.** Scoring against a
carried-forward wait measures agreement with a stale *display*; scoring without
it measures agreement with the queue. Both are legitimate targets — "what the app
showed" is what a user experienced. The published headline keeps them (the
deviation is recorded as BENCH-SPEC change #6); the alternative target is
published beside it. **The choice is not cosmetic**: it moves 6 of 213 hand-over
winners — including d2 and d6, the two leads the serving recommendation turns on —
and absolute autumn MAEs by ~0.18 min. Make the decision before acting on a lead
boundary. See
`~/ml-review-2026-10/RESULTS-PAR-827.md` for the recommendation.

### Sanity check against the 2026-10 review (33 sample parks)

Restricted to the review's 33 sample parks and its two target windows, a query
over `parts/slot` reproduces the review's own 15-min numbers:

| window, lead | ml-bench `wt_med` | review `t_c` | ml-bench `h5` | review `t_H5` | ml-bench `lvlh5_tft` | review TFT×H5 |
|---|---|---|---|---|---|---|
| 08-15 → 09-10, d1 | 7.04 | 7.05 | 6.95 | 6.96 | 7.05 | 7.06 |
| 08-15 → 09-10, d7 | 7.24 | 7.26 | 7.15 | 7.17 | 7.58 | 7.69 |
| 09-11 → 10-07, d1 | 7.22 | 7.24 | 7.06 | 7.09 | 7.11 | 7.13 |

Everything reproduces to within 0.03 except TFT×H5 at d7 (−0.11): the rope-drop
hour is left unscaled here, as BENCH-SPEC says, and the review scaled it. The
all-park population reads ~0.9 higher (8.11 vs 7.04 at d1) because it adds US
and Asian parks with longer queues, not because the method differs.

The query is committed as
`ml-bench/results/20261009-baselines-v2/sanity_33parks.sql` (self-contained, the
33 park ids inlined, the container command in its header) and **its output beside
it as `sanity_33parks.csv`**. It is not produced by `report`, so it is not in the
`tables_*/` dirs — an earlier version of this page said it was, which was wrong.
This is the only evidence on this page that the harness agrees with an
independent implementation, so it has to be reproducible from the repository: it
previously came from an ad-hoc query over the git-ignored `parts/` directory.
