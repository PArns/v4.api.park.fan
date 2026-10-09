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
  16-day forecast) — the covariates carry daily actuals flagged `ORACLE_actuals`.
- **No schedule history exists** (`schedule_entries` keeps the latest version of
  a day) — every window is flagged `published_final`.
- `headliner_attractions` is the current table (548-day window), not ex-ante.

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
  `created_at < origin`; long-history baselines read a month snapshot cut at the
  first day of the origin's month. `tests/test_runner.py::test_information_cut`
  poisons everything after the origin and asserts no forecast moves.
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
| `prod_served` | 6 | `/plan/day` composed tier: 365-day hourly P50 profile, max-normalised to TFT (else CatBoost daily) level, rounded to 5; d ≥ 3 only (d0–d2 serve CatBoost hourly, which is not stored per lead) |
| `oracle_level` / `oracle_shape` | decomposition | H5 × TRUE level / TRUE shape × naive level — never in the hand-over |

## Decision metrics (frontend: park.fan)

| ID | What is simulated |
|---|---|
| D1 | `next-best-ride.ts`: suggest when max forecast in the next 120 min ≥ live + 10 (walk 0); precision = the TRUE max also ≥ live + 10; top-3 per park and origin; recall |
| D2 | `LIVE_WINDOW_MIN = 45`: bias / MAE at leads 15–45 min on headliners — `persistence` is what production serves there |
| D3 | best slot: hit if a true-minimum slot is in the predicted lowest two / within ±30 min; regret = true wait at predicted best − true minimum |
| D4 | `optimize.ts` objective Σwait + 0.5·Σidle on the park's top 4 rides (headliners first, ex-ante level); exact optimum over orders and 0–120 min delays (the beam search converges to it on 4 rides); regret = true cost of the forecast's plan − truth-optimal cost. Plus headliner dayPeak pairwise ordering |
| D5 | first-hour MAE; worth = peak ≥ 60 ∧ peak − opening wait ≥ 45 per ride-day, and the production rule on window medians (`prod_ropedrop_hist`) |
| D6 | park level ÷ typical-day peak → `determineCrowdLevel` ladder; exact / ±1 bucket, busy (high+) recall & precision, cross-park ordering on a date |
| D7 | `day-comparison.ts` rank = bucket + min(0.99, avg/120): winner accuracy on pairs with true gap ≥ 0.5, tie (< 0.1) rate; calendar star = rank ≤ month median − 0.5 among ≥ 4 days |
| D8 | coverage of q80/q95, field coverage (share of slots that HAVE an interval), stated typical error (trailing 28-day MAE at the lead, what `expectedError` reports) vs realised |
| D9 | share of operating slots / headliner-days with a forecast per lead (incl. 90–365 d), forecast present vs ride operated |

D4 simplifications: no overflow / headliner dropping (all 4 rides fit or the
park-day is skipped), no fixed blocks, opensAt floors or early entry; the wait at
a start is the 15-min slot value (the owner's 15-min requirement), truth gaps are
filled from the nearest slot of the same ride-day.

## Horizon outputs

`tables/lead_availability.csv` (origin days per lead, LOW-N < 30, "not measurable
yet" with the date it becomes measurable), `coverage_horizon.csv` (TFT / CatBoost
/ weather / operator schedule), `horizon_curve_mae.csv` + `decision_metrics.csv`
(metric vs lead with CIs and the paired difference vs the per-lead reference),
`usable_horizon.csv` (largest lead, contiguous from the shortest, where the paired
CI excludes 0), `handover.csv` (per use case × lead: the significant winner, else
the reference — the serving router's input). Reference per lead = the best naive
candidate (persistence → snaive7 → wt_med → clim) that covers ≥ 50 % of the rows.

## Adding a model

1. Copy `ml-bench/mlbench/models/example_level_h5.py`, rename the class and
   `name`, implement `predict` (and `fit` / `predict_daily` if useful). The
   example reproduces baseline 5a through the plug-in path and a test asserts
   the two score identically — keep that as the pattern.
2. Register it in `mlbench/models/__init__.py` (`REGISTRY`), or pass
   `--model package.module:Class`.
3. Run (celestrial):

```bash
cd ~/claude-worktrees/<your-checkout>
docker build -t ml-bench:<tag> --build-context mlsvc=ml-service ml-bench
docker run --rm --gpus all ml-bench:<tag> gpu-check          # must list sm_120
docker run -d --name mlbench-<model> --gpus all --cpus 6 --memory 8g --cpu-shares 256 \
  --entrypoint nice -v /data/parkfan/ml-bench:/data -v $PWD/ml-bench/results:/app/results \
  ml-bench:<tag> -n 10 python -m mlbench run --export /data/exports/20261009 \
  --out /app/results/<run-id> --model <name> --memory 5GB --threads 4
docker run --rm -v /data/parkfan/ml-bench:/data -v $PWD/ml-bench/results:/app/results \
  ml-bench:<tag> report --run /app/results/<run-id>
```

`--from/--to` limit the origins, `--leads 0,1,3,7` the slot leads, `--shard i/n`
splits origins across containers. Every run re-scores the baselines, so the
model's numbers are always paired against the same reference on the same rows.

## Results

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
