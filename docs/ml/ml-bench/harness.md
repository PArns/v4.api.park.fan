# ml-bench harness — export, truth, protocol and scoring rules

How `ml-bench/` turns a production export into paired, bootstrapped verdicts: what is exported,
how the 15-minute truth is built, the information cut every forecast obeys, the baselines, the
frontend decision metrics, and the rules that decide a "win" and a usable horizon. Open it
before you read a results table or change `export.py`, `build.py`, `runner.py` or `report.py`.

## Export

`python3 ml-bench/mlbench/export.py --out /data/parkfan/ml-bench/exports/<id> --repo <checkout>`
on celestrial. Standard library only. Every query is one `COPY (SELECT …) TO STDOUT` through
`scripts/prod-psql.sh` (statement, idle and lock timeouts), autocommit, strictly one after
another, resumable (finished files are skipped). `--tables` restricts the tables,
`--queue-from` / `--queue-to` the change-log days, `--forecast-from` the forecast months.

- **`queue_data`** is exported as the raw STANDBY change log, one UTC day (= one hypertable
  chunk) per query. Measured 2026-10-09 (`export-manifest.json` of `20261009-baselines-v2`):
  16,559–212,454 rows per day, 0.11–0.40 s per query, 289 days in ~3 min, 455 MB gzip.
  Building 15-minute slots server-side would have cost production a window function over
  every ride of a park-month and returned 4–10× more rows.
- **`tft_forecasts`** by forecast-date month (no index on `forecast_date`), leads 0–120 d.
  Production keeps only the last **120 days** of `forecast_date` (`TFT_FORECAST_RETENTION_DAYS`,
  PAR-1054), so an export's TFT origins start at most 120 days before it was taken. A backtest
  that needs older TFT origins must use an export taken before the first prune (e.g. `20261009`).
- **`catboost_daily_forecasts`** is no longer written and was dropped (PAR-870, 2026-10-12): new
  exports leave `cbd`, `lvlh5_cbd` and `lvl_cbd` empty; older exports still carry it.
- `parks` (timezone, regionCode, influencingRegions, coordinates), `attractions` (+ headliners,
  coordinates, land), park-level `schedule_entries`, `holidays`, `weather_data`.
- `forecast_archive_curves` (`--tables archive_curves`, `--archive-from`) for the
  [served arm](served-arm.md).

Known limits of the source data:

- **No weather forecast archive.** `weather_data` keeps only the current 16-day forecast, so
  covariates carry daily actuals flagged `ORACLE_actuals`. Baselines never read them; a plug-in
  gets them only by opting in and is then scored as `<name>_owx` (an upper bound, never a
  serving result).
- **Schedule history is partial.** `schedule_entries` keeps the latest version of a day and
  `updatedAt` says when it was last written. In the 2026-10-09 export a window last written after
  the origin covers 14.6–15.4 % of scored slots at d0, 16.2–17.0 % at d1, 32.5–32.9 % at d30 and
  44 % at d60; the median operator publishing horizon is 39 days. The runner treats such a window
  as unknown at the origin (see below).
- **`headliner_attractions`** is the current table (548-day window), not ex-ante. It only decides
  which rides count as headliners (UC4, D2, D4, D6).

## Truth

15-minute slots on a UTC grid (every park offset in use is a whole quarter hour). A slot's value
is the STANDBY reading **in force at the slot midpoint** — the newest change-log row with
`ts <= midpoint`, at most 3 h old. A slot belongs to the service day whose published OPERATING
window contains its start (a window crossing midnight continues past slot 95). Truth = status
OPERATING and wait ≥ 5; the `slots` table keeps every status for D9. Ride-day level = P90 of the
ride-day's truth slots (≥ 8 slots); park level = mean over headliners.

**Heartbeat rows are dropped by default** (PAR-1054): `build` filters `is_heartbeat` rows
(`drop_heartbeats=True`, recorded in `build_stats.json`); `--no-drop-heartbeats` builds the old
target. A heartbeat carries the previous status and wait forward with a fresh `ts`, which resets
the 3 h staleness cap and keeps a frozen wait alive. This is the same rule as the forward
archive's truth and the TFT panel. Builds before 2026-10-24 kept heartbeats — a `build_stats.json`
without the key, or with `false`, is the old target; compare figures within one target only.
What the choice changes is measured in [results-baselines.md](results-baselines.md#heartbeat-contamination-of-the-target).

`tests/test_truth.py` pins the midpoint rule, the 3-hour cap, DST, midnight and the heartbeat
filter.

## Protocol and information cut

- **Origins.** One per park-local day at 06:00 (UC2, UC3, UC4) and intraday origins at
  08, 10 … 20 (UC1, UC2 rest-of-day). Evaluation starts at the first truth day + 56.
- **Lead grid.** Slot curves: d0, d1–d7, d10, d14, d21, d30, d45, d60, d90 (`SLOT_LEADS`).
  Daily levels are scored at every lead 1–90 plus 120/180/270/365 and reported on `UC4_LEADS`;
  the D4 optimiser at 1/3/7/14/30.
- **Information cut.** Profiles read only slots that END before the park's origin. TFT daily
  levels are the latest rows with `created_at < origin` and, like production's serving read, at
  most 3 days stale. Long-history baselines read a month snapshot cut at the first day of the
  origin's month; production's hourly profile is rebuilt as of the day before.
- **Opening hours as known at the origin.** A target day's published window is used only if its
  schedule row was last written before the origin; otherwise the forecast uses a window projected
  from the last 56 days (median opening and closing per weekday type) — production's
  observed-hours fallback. Every opening-aligned forecast reads that window; the truth and the
  "first hour" segment stay on the published one. Scores are also split by schedule known /
  projected (`sk`).
- The schedule-derived covariates `sched_is_holiday` / `sched_is_bridge_day` are built from every
  schedule row of the day, so they are cut at their own publication time; a day whose holiday flag
  came from a later non-OPERATING row is delivered as NULL.
- **Ex-ante busy** = the ride's q90 over the 56 days before the origin ≥ 45 min (`busy_q90_min`).
- `tests/test_runner.py::test_information_cut_baselines` and `::test_information_cut_plugins`
  (run for every registered model, on the daily, daily-level and intraday paths, one pass per
  cut) poison truth, ride-day levels, hourly stats, forecast rows written after the origin, both
  ends of a window written after the origin and every covariate column, and assert no forecast
  moves. An ORACLE-weather model keeps only the weather it is entitled to.

## Baselines

| Column | BENCH-SPEC | Definition |
| --- | --- | --- |
| `persistence` | 1 | intraday only; the slot that ends at the origin |
| `snaive7` | 2 | same local slot 7 days earlier, only if that day is before the origin (≤ d6 from 06:00) |
| `wt_med` (+ `wt_q80/q95`) | 3 | 56 days, weekday/weekend ≥ 3 days per slot, else all days ≥ 4 |
| `clim` | reference fallback | same slot + weekday type + season over all history before the month snapshot, else all seasons |
| `h5` | 4 | opening-aligned first hour, close-aligned last hour, hourly median interpolated |
| `lvlh5_naive` / `lvlh5_tft` / `lvlh5_cbd` | 5a / 5b / — | H5 × level ÷ window median daily P90; first hour unscaled |
| `prod_served` / `prod_served_lin` | 6 | `/plan/day`'s composed tier rebuilt as of the origin (see below); d ≥ 3 only |
| `oracle_level` / `oracle_shape` | decomposition | H5 × TRUE level / TRUE shape × naive level — never in the hand-over |
| `lvl_snaive7`, `lvl_naive4`, `lvl_wt56`, `lvl_clim` | daily-level naives | the level references for UC4 / D6 / D7 |

`prod_served` rebuilds production's planner curve: the `/stats/hourly` profile (one year ending
yesterday; AVG over days of each hour's P50 with ≥ 2 samples; rides with ≥ 20 days; an hour kept
when measured on ≥ 10 days, ≥ 0.4 × the best hour's days and by ≥ 50 % of the rides; rounded to
5), `composeDayCurve` over the open hours known at the origin, max-normalised to the served day
level (TFT ≤ 3 days stale, else CatBoost daily), rounded to 5. `prod_served` is the hourly step
the planner reads, `_lin` the same curve interpolated between hour centres. d0–d2 are not covered
(production serves CatBoost hourly there, which is not stored per lead), and the hourly P50 is
approximated from the 15-minute truth rather than read from `queue_data_aggregates`.

## Decision metrics (frontend: park.fan)

Every decision metric is paired at the level it lives on: slot by slot (MAE, first hour),
ride-day by ride-day (best time, dayPeak error, rope drop — a ride-day counts for a model only if
the model covers every truth slot of it), ride pair (dayPeak ordering), park-day (optimiser) and
day pair (day comparison).

| ID | What is simulated |
| --- | --- |
| D1 | `lib/planner/next-best-ride.ts`: candidates = every (intraday origin, ride) with a live OPERATING wait; suggest when the forecast maximum inside a fixed lookahead (60 or 120 min) ≥ live + 10 (walk 0); precision = the true maximum is also ≥ live + 10; top-3 per park and origin; recall |
| D2 | `LIVE_WINDOW_MIN = 45`: bias / MAE at leads 15–45 min on headliners — `persistence` is what production serves there |
| D3 | best slot: hit if a true-minimum slot is in the predicted lowest two / within ±30 min; regret = true wait at the predicted best − true minimum |
| D4 | `lib/planner/optimize.ts` objective Σwait + 0.5·Σidle on the park's top 4 rides (headliners first, ex-ante level); exact optimum over orders and 0–120 min delays; regret = true cost of the forecast's plan − truth-optimal cost. Plus headliner dayPeak pairwise ordering |
| D5 | first-hour MAE (first 4 slots of the published window, split by schedule known / projected); rope-drop worth = peak ≥ 60 ∧ peak − opening wait ≥ 45 per ride-day, plus the production rule on window medians (`prod_ropedrop_hist`) |
| D6 | park level ÷ typical-day peak → `determineCrowdLevel` ladder; exact / ±1 bucket, busy (high+) recall and precision, cross-park ordering on a date |
| D7 | `lib/parks/day-comparison.ts` rank = bucket + min(0.99, avg/120): winner accuracy on pairs with true gap ≥ 0.5, tie (< 0.1) rate; calendar star = rank ≤ month median − 0.5 among ≥ 4 days, precision / recall |
| D8 | what production states: the `forecast-accuracy.service.ts` grid (predicted band × lead bucket over the 45 days before the origin) rebuilt as of each origin and compared with the realised error; plus empirical q80/q95 coverage, field coverage and the trailing 28-day slot MAE |
| D9 | share of operating slots / headliner-days with a forecast per lead (incl. 90–365 d); forecast present vs ride operated. Baselines have no openness logic, so their D9 is the bar a model with an open/closed decision has to beat |

D4 simplifications (`mlbench/decisions.py`): a fixed ride set (top 4), no overflow or headliner
dropping (the park-day is skipped if the rides cannot all start before closing), no fixed blocks,
`opensAt` floors, early entry or live corrections; the wait at a start is the 15-minute slot value
(the frontend reads the hourly point); truth gaps are filled from the nearest slot of the same
ride-day; walking from coordinates, else 3 / 8 min (`lib/planner/leg.ts`).

## Report outputs

`report --run <dir>` writes, per pass:

- `lead_availability.csv` — origin days per lead **for the pass that writes it** (LOW-N < 30,
  "not measurable yet" with the date). Read per-cell `n_origin_days` in `cells.csv` when a run
  used `--shard` or a park subset: older windowed passes copied the full-period table here.
- `coverage_horizon.csv` — TFT / weather / operator schedule reach.
- `cells.csv` — every metric × use case × lead × segment × region × model: value with park-day
  CI, paired difference vs the selected reference with park-day (`diff_lo/hi`) and park-cluster
  (`diff_lo_park/hi_park`) CIs, coverage, unit of analysis and unit counts.
- `cells_all_refs.csv` — the same against **every** reference candidate.
- `ref_choice.csv` — candidates, their values, the selected reference and why.
- `low_coverage_cells.csv` — cells whose paired rows are under half the reference's; their
  absolute values are not comparable with the rest of the column.
- `usable_horizon.csv` (with `leads_tested`, `not_tested_because`) and `handover.csv` (both CIs,
  `park_day_unit_agrees`).
- `mae_by_season.csv` (unpaired, each model on its own coverage) and the per-question tables.
- `report --vs <model>` adds `cells_vs_<model>.csv` from the paired sums `run --pair-with` wrote
  (`pn__a__b`, `psm__a__b`, `psr__a__b`). Pairs exist only **inside one run**; two runs share no
  paired column, so a difference of two runs' MAEs is not a paired comparison.

`--target-from/--target-to` fixes the target window, `--origin-from/--origin-to` the origin
window. Both are needed to read a horizon curve honestly (see "Confounds" below).

## When a model "wins"

- **A win = beating the whole naive envelope** (BENCH-SPEC change #5). The model must be better
  than **every** reference candidate at that lead, each comparison paired on its own shared rows,
  each with its park-cluster CI excluding 0, over ≥ 10 distinct bootstrap units
  (`min_bootstrap_units`) and with a strictly positive CI width (change #2). A single-unit paired
  set collapses the CI to zero width and "excludes 0" for free; the guards stop that.
- **Park-cluster, not park-day** (change #1): days of one park are not independent, so all days of
  a park are resampled together. BENCH-SPEC's literal unit is the park-day; both CIs are printed
  and the hand-over marks where they disagree.
- **Reference per lead** (change #4) = the ladder persistence → snaive7 → wt_med → clim (daily
  levels: lvl_snaive7 → lvl_naive4 → lvl_wt56 → lvl_clim) among candidates covering ≥ 50 % of
  the best-covered one; a candidate moves off its ladder position only when another beats it in a
  paired round robin with the park-cluster CI excluding 0. The older unpaired argmin compared
  MAEs on different row sets and flipped `wt_med → clim → wt_med` along the lead axis on gaps of
  0.019–0.187 min against a ±0.24 half-width. Under the envelope rule no verdict depends on which
  candidate is displayed as `ref`.
- **`wins` vs `wins_vs_ref`.** `wins` is the published criterion. `wins_vs_ref` (beats the
  displayed `ref` only) is kept because change #5 asks for it; on the reference run they disagree
  on 233 cells and always as `wins_vs_ref = True, wins = False` — read `wins`.
- **`hardest_naive` is the worst point estimate, not necessarily the blocker.** It is among the
  blocking candidates in ~98 % of non-winning cells but the *sole* blocker in only 17–19 % (headline
  window / full period). Read `cells_all_refs.csv` per candidate before naming what denies a win.
- **Gates.** Enough units for the metric's own unit of analysis — ≥ 30 origin days
  (`low_n_origin_days`) for day-unit metrics, ≥ 30 park-months for UC4 (change #3; a 3-month
  window has 3 month labels but ~200 park-months) — and the model's paired rows ≥ 30 % of the
  reference's own (`MIN_COVERAGE`). Winners are ranked by the paired margin. Naive candidates are
  excluded from the winner column: their margins are measured against a weaker pool.
- **Usable horizon** = the largest lead, contiguous from the model's first scored lead, at which
  it wins; a lead where it is the reference, untested or gated breaks the run.
  **`leads_tested = 0` means nothing was tested, not that nothing won.** A model that loses its
  first lead (Chronos-2 at 15 min) shows an empty usable horizon even when it wins a later band —
  read `max_significant_lead` too.
- **Provenance** (change #7). `run-*.json` records the scoring code, `report-<tables>.json` the
  code that produced the verdicts; they can differ. `code_sha256` (sha256 over every
  `mlbench/**/*.py`, measured inside the process) is the field to trust; `git_sha` and `image_id`
  are asserted by whoever launched the container and have been recorded wrong before.

## Confounds every reader needs

- **Lead and season are not separable on this history.** A common target window fixes the truth
  population, but lead L's origins are then `target − L`, so for the 2026-08-15…10-07 window d1
  comes from autumn origins and d90 from summer ones. The full period has the mirror problem
  (lead L starts at first origin + L). Use `--origin-from/--origin-to` as a third view, read
  `mae_by_season.csv`, and treat every lead boundary as a band.
- **Weekday aliasing.** No output table carries a weekday column. A `--shard i/n` with `n` a
  multiple of 7 puts every origin on one weekday, which locks every planner lead to one target
  weekday (PAR-828's first Chronos-2 run: all origins Thursdays, so "d2" was Saturday). The
  runner now records an `origin_weekdays` census and `origin_weekday_distinct` in `run-*.json`
  and **refuses** such an origin set (n multiple of 7, or > 7 origins on ≤ 2 weekdays) unless
  `--allow-weekday-alias` states that the shards' union is complete
  (`test_weekday_aliasing_is_refused_and_the_census_is_recorded`). A `mae_by_weekday` split is
  an open follow-up.

## Known defects

- **UC4 tie order.** Two report passes over the same `parts/` that differ only in DuckDB thread
  count agree on every verdict, but UC4's Spearman moves by ≤ 0.0019 and the busy-day set by ≤ 9
  park-days of ~1,400: the `levels` query has no `ORDER BY`, and `rank(method="first")` and the
  `p >= q75` boundary are tie-order sensitive. Making those deterministic is open.
- `tests/synth.py` builds two parks, so `min_bootstrap_units = 10` makes `wins = True`
  unreachable in the suite. Prefer data-independent invariants
  (`test_handover_and_usable_horizon_share_one_gate`), and give every new guard a case in which it
  can fail.
