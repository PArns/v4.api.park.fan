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
  query** — one day is exactly one hypertable chunk. Measured on 2026-10-09:
  127–186 k rows per day, **0.2–0.3 s per query**, 290 days in ~2 minutes,
  ~400 MB gzip. Building the 15-minute slots server-side would have cost
  production a window function over every ride of a park-month and returned
  4–10× more rows; client-side it is a chunked DuckDB step.
- `tft_forecasts` / `catboost_daily_forecasts` by forecast-date month
  (no index on `forecast_date`; ≤ 5 M rows, ≤ 4.5 s per query), leads 0–120 d.
- `parks` (timezone, regionCode, influencingRegions, lat/lng), `attractions`
  (+ `headliner_attractions`, coordinates, land), park-level `schedule_entries`,
  `holidays`, `weather_data`.
- **No weather forecast archive exists** (`weather_data` keeps only the current
  16-day forecast) — the covariates carry daily actuals flagged `ORACLE_actuals`,
  which baselines never read and plug-ins only get by opting in.
- **Schedule history is partial**: `schedule_entries` keeps the latest version
  of a day, but `updatedAt` says when it was last written. A window last written
  after an origin was not known at that origin (15 % of the scored slots at d1,
  ~40 % at d30 in the 2026-10-09 export; the median operator publishing horizon
  is 39 d), so the runner treats it as unknown — see "Protocol".
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
  `::test_information_cut_plugins` (run for EVERY registered model) poison truth,
  ride-day levels, hourly stats, TFT/CatBoost rows written after the origin,
  windows written after the origin and the weather, and assert no forecast moves.
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

`tables/lead_availability.csv` (origin days per lead, LOW-N < 30, "not measurable
yet" with the date it becomes measurable), `coverage_horizon.csv` (TFT / CatBoost
/ weather / operator schedule), `cells.csv` (every metric × use case × lead ×
segment × region × model: value with park-day CI, paired difference vs the
reference with park-day AND park-cluster CIs, coverage), `usable_horizon.csv`
and `handover.csv`.

- **Wins** = the paired difference's **park-cluster** bootstrap CI (all days of
  a park resampled together) excludes 0. Days of one park are not independent,
  so the park-day CI is too narrow; both are in `cells.csv`.
- **Reference per lead** = the best naive candidate (persistence → snaive7 →
  wt_med → clim) on the metric itself, among candidates covering ≥ 50 % of what
  the best-covered one covers. Picking the best of several on the same data
  favours the reference a little (winner's curse), which makes a model's win
  conservative.
- **Gates** for the hand-over and the usable horizon: ≥ 30 origin days, and the
  model's paired rows ≥ 30 % of the reference's own. Winners are ranked by the
  PAIRED margin. Usable horizon = the largest lead, contiguous from the model's
  first scored lead, at which it wins; a lead where it is the reference, is
  untested or fails a gate breaks the run.
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
   database connection — `origin.history.df(days)` (truth ending before the
   origin), `train_panel` in `fit`, the horizon grid of the window known at the
   origin, and covariates without weather unless `uses_oracle_weather = True`
   (then it is scored as `<name>_owx`). Rows outside the grid are dropped.
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

> **Superseded — re-run pending.** The figures below come from the first run,
> before the PAR-827 review fixes: ride-day metrics were paired only per
> park-day, opening-aligned forecasts read windows published after the origin,
> and `prod_served` was not the production profile. They will be replaced by the
> re-run on the same export.

First full baseline run: `ml-bench/results/20261009-baselines/` (export of
2026-10-09: 289 UTC days, 44.2 M change-log rows → 13.5 M truth slots, 3,601
rides, 157 parks; 231 daily origins 2026-02-19 → 2026-10-07; 3 shards × 3 CPUs
× 2.5 GB, 24 min wall, report 9 min). What it says:

- **Read MAE with its paired difference, not alone.** Each model's MAE is on its
  own coverage; `lvlh5_tft` reads 7.50 at d1 against `wt_med` 7.65, yet on the
  slots both cover it is 0.09 min *worse* — TFT covers 68 % of the slots, the
  easier ones. The `*` / `diff` columns are the comparison.
- **UC1:** persistence wins every lead to 2 h (2.46 at 15 min, 7.48 at 120 min
  vs 7.52 for the best profile) — the crossover is right at 2 h.
- **UC2/UC3:** H5 is the only baseline significantly better than the weekday
  median at every lead d0–d90 (−0.07 … −0.20 min); on ex-ante busy rides
  TFT × H5 wins d0–d6 (−0.5 at d1) and is no longer significant from d7. Error grows slowly:
  H5 7.58 at d1 → 8.10 at d14 → 8.71 at d90. A perfect level would take d1 to
  5.43, a perfect shape to 6.54: the level is still the larger error.
- **Production-as-served (`prod_served`) is worse than the weekday median at
  every lead from d3** (+0.56 … +1.15 min) and worst in the first hour (5.7 vs
  3.9): the hourly max-normalised step curve loses the rope-drop ramp.
- **CatBoost-daily level (`lvlh5_cbd`) is ~2.6 min worse than no level at all**
  (bias −5.7: a P50-like level against a P90 target).
- **UC4 / D6 / D7:** the daily level is barely rankable within a month
  (Spearman ≤ 0.30 at d1, ~0.15 beyond d7); TFT's crowd bucket is right 40 %
  of the time at d1 (±1: 81 %), the day comparison picks the right day in
  ~45 % of clear pairs at d1–7, mostly because flat levels tie.

Leads 270 and 365 become measurable on 2026-12-15 and 2027-03-20.
