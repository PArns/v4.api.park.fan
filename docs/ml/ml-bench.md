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

- **Wins** = the paired difference's **park-cluster** bootstrap CI (all days of
  a park resampled together) excludes 0, over at least **10 distinct bootstrap
  units** and with a strictly positive CI width. Days of one park are not
  independent, so the park-day CI is too narrow; both are in `cells.csv`
  (`diff_lo/hi` park-day, `diff_lo_park/hi_park` park-cluster) and both are
  printed in the hand-over table, which marks the cells where they disagree.
  BENCH-SPEC's literal unit is the park-day; the park-cluster deviation and its
  reason are **recorded in BENCH-SPEC.md** ("Recorded changes" 1–3), as the spec
  requires. The unit-count and width guards exist because a single-unit paired
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
- **Gates** for the hand-over and the usable horizon: enough units for the
  metric's own unit of analysis — ≥ 30 origin **days** for day-unit metrics,
  ≥ 30 park-**months** for UC4, whose unit is the park-month — and the model's
  paired rows ≥ 30 % of the reference's own. Winners are ranked by the PAIRED
  margin. Usable horizon = the largest lead, contiguous from the model's first
  scored lead, at which it wins; a lead where it is the reference, is untested or
  fails a gate breaks the run. `usable_horizon.csv` carries `leads_tested` and
  `not_tested_because`: **`leads_tested = 0` means nothing was tested, which is
  not the same as nothing winning** and must never be read as a loss.
- **`low_coverage_cells.csv`** lists every cell whose paired rows are under half
  the reference's. Those absolute values are not comparable with the rest of
  their column — at d60 `lvlh5_tft` reads 1.337 min on the first hour because
  TFT's horizon ends there and 2 % of the reference's slots remain.
- **Headline horizon curves come from a common target window**
  (`report --target-from … --target-to …`). On the full period, lead L starts at
  first origin + L and TFT / CatBoost only exist from late May, so lead would be
  confounded with season.

## Adding a model

1. Copy `ml-bench/mlbench/models/example_level_h5.py`, rename the class and
   `name`, implement `predict` (and `fit` / `predict_daily` if useful). The
   example reproduces baseline 5a through the plug-in path and a test asserts
   the two score identically — keep that as the pattern.
2. Register it in `mlbench/models/__init__.py` (`REGISTRY`), or pass
   `--model package.module:Class`. A registered model is automatically covered by
   `test_information_cut_plugins` — run `pytest` before any run you report.
3. What a plug-in gets (`mlbench/models/base.py`): DataFrames only, never a
   database connection and no object from which one is reachable —
   `origin.history.df(days)` (truth ending before the origin, materialised up
   front, so `days` is capped at `--history-days`, 56 by default; use `fit` for
   longer history), `train_panel` in `fit`, the horizon grid of the window known
   at the origin, and covariates without weather unless the **class** sets
   `uses_oracle_weather = True` (then it is scored as `<name>_owx`; setting it on
   the instance buys nothing). Rows outside the grid are dropped.
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

## Results

Reference run: `ml-bench/results/20261009-baselines-v2/`. Export of 2026-10-09
(289 UTC days, 44.2 M change-log rows → 13.5 M truth slots, 3,601 rides, 157
parks), 231 daily origins 2026-02-19 → 2026-10-07, git `ea9a8438`, sources
sha256 `25ed0c3b26d3accd`, image `sha256:a1e54cb8fea6`, 3 shards ×
(`--cpus 3 --memory 2560m`), 54.4 shard-minutes, two report passes (7 min
common-window, 14 min full period).

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

| lead | `snaive7` | `wt_med` | `clim` | `h5` | `lvlh5_naive` | `lvlh5_tft` | `lvlh5_cbd` | `prod_served` | `oracle_level` | `oracle_shape` | ref |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 8.44 | 8.00 | 8.24 | 7.88* | 8.12 | 7.74* | 10.17 | – | – | – | `wt_med` |
| 1 | 8.44 | 8.11 | 8.28 | 7.98* | 8.15 | 7.86* | 10.21 | – | 5.34* | 6.96* | `wt_med` |
| 3 | 8.44 | 8.19 | 8.31 | 8.06* | 8.18 | 8.03 | 10.25 | 8.52 | 5.35* | 6.96* | `wt_med` |
| 6 | 8.44 | 8.33 | 8.35 | 8.20* | 8.22 | 8.27 | 10.77 | 8.73 | 5.39* | 6.96* | `wt_med` |
| 7 | – | 8.45 | 8.38 | 8.32 | 8.65 | 8.51 | 10.92 | 9.01 | 5.43* | 7.59* | `clim` |
| 14 | – | 8.73 | 8.37 | 8.60 | 8.99 | 8.97 | 11.20 | 9.55 | 5.49* | 8.00* | `clim` |
| 30 | – | 8.84 | 8.62 | 8.70 | 9.24 | 9.31 | 11.14 | 10.19 | 5.51* | 8.29 | `clim` |
| 60 | – | 8.73 | 8.54 | 8.63 | 9.08 | (4.26) | – | (4.30) | 5.59* | 8.15 | `clim` |
| 90 | – | 8.42 | 8.58 | 8.33* | 8.93 | – | – | – | 5.46* | 7.90* | `wt_med` |

Ex-ante busy rides:

| lead | `wt_med` | `clim` | `h5` | `lvlh5_naive` | `lvlh5_tft` | `prod_served` | ref |
|---|---|---|---|---|---|---|---|
| 0 | 15.21 | 16.03 | 14.90* | 15.11 | 13.90* | – | `wt_med` |
| 1 | 15.42 | 16.10 | 15.10* | 15.12* | 14.11* | – | `wt_med` |
| 3 | 15.59 | 16.10 | 15.25* | 15.15* | 14.50* | 15.68 | `wt_med` |
| 6 | 15.89 | 16.17 | 15.55* | 15.23* | 15.03* | 16.17 | `wt_med` |
| 7 | 16.12 | 16.22 | 15.78* | 16.22 | 15.48* | 16.73 | `wt_med` |
| 10 | 16.39 | 16.18 | 16.04 | 16.30 | 15.84* | 17.31 | `clim` |
| 14 | 16.76 | 16.16 | 16.42 | 17.02 | 16.51 | 18.03 | `clim` |
| 30 | 17.21 | 16.65 | 16.86 | 17.69 | 17.54 | 19.64 | `clim` |
| 90 | 16.72 | 17.13 | 16.55* | 17.46 | – | – | `wt_med` |

Paired differences vs the reference, park-cluster 95 %:

| lead | `h5`, all | `lvlh5_tft`, all | `h5`, busy | `lvlh5_tft`, busy | `prod_served`, all |
|---|---|---|---|---|---|
| 0 | −0.13 [−0.16, −0.10] | −0.31 [−0.54, −0.08] | −0.30 [−0.37, −0.23] | −1.36 [−1.88, −0.85] | – |
| 1 | −0.14 [−0.17, −0.11] | −0.28 [−0.52, −0.05] | −0.31 [−0.38, −0.24] | −1.34 [−1.86, −0.82] | – |
| 3 | −0.14 [−0.17, −0.11] | −0.19 [−0.42, +0.04] | −0.32 [−0.39, −0.25] | −1.09 [−1.64, −0.58] | +0.41 [+0.13, +0.70] |
| 7 | −0.22 [−0.45, +0.01] | −0.15 [−0.35, +0.06] | −0.33 [−0.40, −0.25] | −0.65 [−1.18, −0.15] | +0.58 [+0.37, +0.79] |
| 10 | −0.10 [−0.33, +0.15] | +0.02 [−0.15, +0.21] | −0.13 [−0.71, +0.47] | −0.59 [−1.00, −0.15] | +0.83 [+0.60, +1.07] |
| 14 | +0.07 [−0.17, +0.34] | +0.35 [+0.15, +0.56] | +0.21 [−0.41, +0.90] | +0.09 [−0.38, +0.60] | +1.19 [+0.91, +1.49] |
| 30 | +0.05 [−0.19, +0.32] | +0.64 [+0.36, +0.94] | +0.22 [−0.44, +0.93] | +0.83 [+0.12, +1.70] | +1.60 [+1.22, +2.01] |
| 90 | −0.06 [−0.08, −0.04] | – | −0.14 [−0.20, −0.09] | – | – |

At d60 `lvlh5_tft` and `prod_served` sit on 1.5 % of the slots (TFT's horizon
ends at 60 days and only a few parks still have a level) — the values in
brackets are not comparable and the gates drop them from the hand-over.

### Hand-over (input for the serving router)

| Use case / segment | Leads | Winner | Margin vs reference |
|---|---|---|---|
| UC1 live, 15–120 min | all | `persistence` | nothing beats it; +0.64 [+0.30, +0.96] for the best profile even at 120 min |
| UC2 intraday, 2–4 h | — | `h5` / `lvlh5_tft` | −0.13 / −0.31 vs `wt_med`; `persistence` is already only +0.22 [−0.20, +0.67] |
| UC2 intraday, 4 h+ | — | `h5` (busy: `lvlh5_tft`) | `persistence` collapses to +3.02 [+2.43, +3.70] |
| UC2 / UC3, all rides | d0–d2 | **`lvlh5_tft`** | −0.31 … −0.24 |
| UC2 / UC3, all rides | d3–d6 | **`h5`** | −0.14 |
| UC2 / UC3, all rides | d7–d60 | `clim` (no model wins) | — |
| UC2 / UC3, all rides | d90 | `h5` | −0.06 |
| UC2 / UC3, busy rides | d0–d10 | **`lvlh5_tft`** | −1.36 at d0 … −0.59 at d10 |
| UC2 / UC3, busy rides | d14–d60 | `clim` (no model wins) | — |
| UC2 / UC3, busy rides | d90 | `h5` | −0.14 |
| D3 best time (regret, hit rate) | d0–d45 | `h5` | −0.54 … −0.32 min regret, +0.017 … +0.010 hit rate |
| D4 optimiser regret | d1 / d3–d7 | `h5` / `lvlh5_tft` | −1.3 / −2.0 … −1.3 min per park-day; nothing wins from d14 |
| D4 dayPeak ordering | d0–d45 | `lvlh5_tft` | +0.048 … +0.029 (`h5` wins d0–d90) |
| D5 first hour | d0–d7 | `lvlh5_tft` | −0.48 … −0.32 min; nothing wins d10–d60 |
| D5 rope-drop worth | all | `clim` (no model wins) | — |
| D6 crowd bucket | d1–d6 / d7–d21 | `lvl_snaive7` / `lvl_tft` | — / +0.034 … +0.030 |
| D7 day comparison | d1–7 / d8–90 | `lvl_snaive7` / `lvl_tft` | — / +0.071, +0.058 |
| UC4 daily ranking | all | `lvl_snaive7` (d1–6), `lvl_naive4` (d7–10), `lvl_clim` (d14–60) | no level source wins at any lead |
| D1 next-best ride | 0–120 min | `snaive7` | nothing wins; every forecast is −0.04 … −0.08 precision |
| D2 live window 0–45 min | — | `persistence` | 6.05 vs ≥ 11.90 for any forecast |

### Usable horizon

| metric | model | usable horizon | max significant lead |
|---|---|---|---|
| UC3 MAE, all | `h5` | d6 | d90 |
| UC3 MAE, all | `lvlh5_tft` | d2 | d2 |
| UC3 MAE, busy | `h5` | d7 | d90 |
| UC3 MAE, busy | `lvlh5_tft` | **d10** | d10 |
| UC2 intraday MAE, all / busy | `h5` | h8+ | h8+ |
| UC2 intraday MAE, all / busy | `lvlh5_tft` | h4–8 / h8+ | — |
| D5 first-hour MAE | `h5` / `lvlh5_naive` / `lvlh5_tft` | d7 | d90 / d7 / d7 |
| D3 best-time regret / hit rate | `h5` | d45 | d90 / d45 |
| D4 optimiser regret | `h5` | d7 | d7 |
| D4 dayPeak ordering | `h5` / `lvlh5_tft` | d90 / d45 | d90 / d45 |
| UC2/UC3 within-ride-day Spearman | `h5` | d45 | d90 |
| D6 crowd bucket | `lvl_tft` | — (loses d1–d6) | d21 |
| D7 day comparison | `lvl_tft` | — (loses d1–7) | d31–90 |
| UC4 Spearman | every level source | — | — |

A usable horizon shorter than the max significant lead means a lead in between
breaks the run — usually because the reference switches from `wt_med` to `clim`
at d7 and nothing beats climatology in the d7–d60 band.

### What the numbers say

- **UC1 / D2 stay with production's live replacement.** `persistence` wins every
  lead to 2 h (2.46 at 15 min, 7.58 at 120 min) and the live window 0–45 min on
  headliners (6.05 vs 11.90 for the best forecast). The hand-over happens in the
  2–4 h band, where `persistence` is already only +0.22 [−0.20, +0.67] against
  the weekday median and is +3.02 [+2.43, +3.70] by 4–8 h.
- **The TFT level pays for itself on busy rides, out to d10.** `lvlh5_tft` is
  −1.36 [−1.88, −0.85] at d0 and still −0.59 [−1.00, −0.15] at d10 on ex-ante
  busy rides; on all rides it only wins d0–d2 and H5 alone is better from d3.
- **H5 beats the weekday median wherever the weekday median is the reference**
  — d0–d6 (−0.13 … −0.14 all rides, −0.30 … −0.33 busy) and d90 (−0.06 / −0.14).
  But from d7 the best naive reference becomes **climatology**, and H5 does not
  beat *that* in the d7–d60 band (d7 −0.22 [−0.45, +0.01], d14 +0.07
  [−0.17, +0.34]), even though its raw MAE stays below the weekday median's at
  every lead. The planner's long leads are not yet beaten by anything we have.
- **Level, not shape, is the error.** At d1 a perfect level takes H5 from 7.98 to
  **5.34**; a perfect shape on the naive level gives 6.96. That holds at every
  lead (d30: 5.51 vs 8.70).
- **The production composed tier (`prod_served`) is worse than the reference at
  every lead from d3** (+0.41 [+0.13, +0.70] at d3 … +1.60 [+1.22, +2.01] at
  d30), worst in the opening hour (6.21 vs 4.33 for `lvlh5_tft`, +1.45
  [+1.17, +1.73]) and on the optimiser (+5.0 [+3.2, +6.6] min per park-day at
  d3): the hourly max-normalised step curve loses the rope-drop ramp. It has no
  interval at all (D8).
- **The CatBoost-daily level makes things worse than no level.** `lvlh5_cbd`
  10.21 at d1 against `h5` 7.98, +2.12 [+1.65, +2.63] against the reference — a
  P50-like level against a P90 target.
- **UC4 / D6 / D7: nothing beats the naive level in the first week.** The
  seasonal-naive level (`lvl_snaive7`) is the reference and unbeaten at d1–d6
  (UC4 Spearman 0.33, crowd bucket exact 0.434, day comparison 0.519). TFT's
  level only wins beyond that: crowd bucket +0.034 at d7 … +0.030 at d21, day
  comparison +0.071 (d8–30) and +0.058 (d31–90). Within-month ranking is weak
  everywhere (Spearman ≤ 0.33 at d1, 0.15–0.26 from d14).
- **D8 is calibrated at short leads.** Production's published `typicalError`
  (TFT peak MAE by band × lead bucket) lands at realised ÷ stated 1.00–1.04 for
  d1–d30 and goes unstable at d60 (1.20 busy, 0.78 quiet). `wt_med`'s empirical
  q80 covers 0.846 and q95 0.920 at d1 — q80 is conservative, q95 is short.
- **D9: "has a forecast" is not an open/closed decision.** The weekday median
  produces a curve for 83 % of the ride-days a ride never operated on in the
  headline window (76 % over the full period), so coverage has to be scored
  separately from the wait error.

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
   seasonal.** `h5` is better than `wt_med` in both halves of this window at
   every lead (summer −0.02 … −0.04, autumn −0.19 … −0.21) and in spring and
   summer on the full period. The one exception is **winter** (2 % of the data,
   256 k slots), where `h5` is 0.02–0.04 min *worse* at d0–d7. That is the
   difference between a profile-shape improvement, which transfers, and a
   level-source improvement, which does not.

### Heartbeat contamination of the target — open owner decision

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

**This is a product decision, not a bug to fix silently.** Scoring against a
carried-forward wait measures agreement with a stale *display*; scoring without
it measures agreement with the queue. Both are legitimate targets — "what the app
showed" is what a user experienced. `build --drop-heartbeats` builds the other
target so the two can be compared cell for cell; see
`~/ml-review-2026-10/RESULTS-PAR-827.md` for the measured sensitivity and the
recommendation, and BENCH-SPEC "Recorded changes" 6 for the deviation record.

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
33 park ids inlined, the container command in its header) and its output as
`sanity_33parks.csv` in both `tables_*/`. This is the only evidence on this page
that the harness agrees with an independent implementation, so it has to be
reproducible from the repository — it previously came from an ad-hoc query over
the git-ignored `parts/` directory.
