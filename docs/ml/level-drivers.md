# What explains the daily level — level drivers and `driver_level` (PAR-830)

The multi-day forecast error is dominated by the daily **level** (a perfect level takes the d1
slot MAE from 7.98 to 5.34 — [ml-bench results](ml-bench/results-baselines.md#level-not-shape-is-the-error)).
This page measures how much of a day's deviation from the naive level is explainable by drivers
known in advance, which drivers matter, what the resulting level model (`driver_level`) does when
scored through ml-bench, and how it runs as a production shadow. Open it before adding a covariate
to TFT or CatBoost, or before acting on the `driver_level` shadow.

**Answer, in one line each:**

- Offline, drivers known at the origin explain **12–16 % of a ride-day's and 19–28 % of a
  park-day's** squared log deviation from the naive level; the rest is noise at this resolution.
- Opening hours relative to the recent days and changes of school-break state lead — and **TFT has
  neither** (§5).
- Through the harness, `driver_level` improves the **ranking of days** (UC4, D6, D7, D4 optimiser)
  over the full year, but **not the minutes** on a 15-minute slot (the autumn-window win reverses on
  the full period) and it **regresses best time (D3)**. Verdict: **shadow on four ranking surfaces,
  serve nothing** — running since PAR-1058 (§8).

Code: `ml-bench/mlbench/drivers.py` (the driver table, shared), `ml-bench/mlbench/driver_analysis.py`
(`python -m mlbench drivers`), `ml-bench/mlbench/models/driver_level.py` (the harness plug-in),
`ml-service/driver_level.py` (the production shadow). Results:
`ml-bench/results/20261009-par830-drivers/` (`drivers/tables/` offline, `tables*/` harness).

**Two bodies of evidence — never mix them in one sentence.** §1–§5 are the offline walk-forward
analysis (R², level MAE, UC4 over 385 park-months, richer feature set); §6–§7 are the ml-bench
harness run (slot MAE, D3–D9, UC4 over 62–607 park-months, a reduced model).

## 1. Method (offline)

`python -m mlbench drivers --export <export> --out <dir>` builds one row per ride-day × lead
(1/3/7/14/30/60/90), fits walk-forward models and writes R², level MAE, UC4 ranking, univariate
effects and permutation importance (CPU, ~4 min on 1.9 M rows). It reads `parquet/` and
`raw/schedule.csv.gz` (ticketed events, extra hours and `updated_us`).

- **Target** `y = log(true daily P90 / naive)`. Naive = baseline 5a: median of the last four
  same-weekday daily P90s inside the 56 days before the origin (≥ 2); a test pins it to the harness.
- **Driver groups** (`drivers.GROUPS`): `cal` — weekday, lead, region, headliner and the reference
  itself (log level, weeks it rests on, age); **no month / day of year** (with < 1 year of history
  every test month is unseen; those features made the model worse than naive). `hol` — public and
  school holidays of the own and neighbour regions (ml-service `holiday_features.py`, OR semantics),
  bridge days, day before/after a holiday, holiday contamination of the reference days. `sched` —
  published opening hours (length, open/close hour, ratio and difference to the reference days),
  ticketed events, extra hours, days since season opening / until closing, days since the park last
  opened — **only where the schedule row was written before the origin** (`drivers.add_asof`, from
  `updated_us`); `sched_final` (as finally published) is an upper bound. `wx` — daily weather and its
  difference to the reference days, **ORACLE** (actuals; no forecast archive).
- **Validation.** Walk-forward by origin month: the model for month M trains on ride-days whose
  target is before the 1st of M; one model per fold and feature set, pooled over leads with `L` as a
  feature. Origins April → October 2026 (Feb/Mar have < 20,000 training rows). Headline tables use a
  common target window (targets ≥ 2026-06-30); `*_all.csv` the full window.
- **Models.** Univariate effects with park-cluster CIs; a GAM-like additive model (splines + one-hot,
  ridge); gradient boosting (`HistGradientBoostingRegressor`, absolute loss) as the main model.
- **Metrics.** R² vs naive = `1 − Σ(y − ŷ)² / Σy²`; level MAE with paired park-day CIs, per ride and
  per park (headliner mean); UC4 = Spearman of the park level within park-month (≥ 8 days).

## 2. How much is explainable (offline)

R² vs naive, main model on `cal+hol+sched` (as-of):

| Lead | 1 | 3 | 7 | 14 | 30 | 60 | 90 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ride-day | 0.117 | 0.113 | 0.136 | 0.146 | 0.161 | 0.157 | 0.097 |
| park-day | 0.195 | 0.187 | 0.222 | 0.244 | 0.268 | 0.282 | 0.204 |
| park-day + weather (ORACLE) | 0.224 | 0.217 | 0.250 | 0.271 | 0.291 | 0.304 | 0.231 |
| park-day, schedules as finally published (upper) | 0.205 | 0.194 | 0.236 | 0.265 | 0.291 | 0.299 | 0.213 |

Level MAE (minutes), naive → main model, all CIs exclude 0:

| Lead | 1 | 3 | 7 | 14 | 30 | 60 | 90 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ride-day naive | 9.00 | 9.00 | 9.70 | 10.28 | 10.93 | 11.32 | 11.12 |
| ride-day Δ | −0.36 | −0.34 | −0.49 | −0.59 | −0.78 | −0.92 | −0.49 |
| park-day naive | 11.93 | 11.93 | 13.17 | 14.05 | 15.05 | 15.80 | 15.04 |
| park-day Δ | −0.88 | −0.82 | −1.06 | −1.23 | −1.62 | −1.92 | −1.14 |

That is −4…−8 % level MAE per ride and −7…−12 % per park; UC4 rises 0.27 → 0.34 at d1 and
0.11 → 0.25 at d60.

- **The rest is noise at this resolution.** A ride-day's log deviation has SD 0.56 (d1) – 0.66
  (d60); the headline model leaves 88 % / 80 % (ride / park) unexplained at d1 and 84 % / 72 % at
  d60, the ORACLE-weather model still 86 % / 78 % at d1. Averaging over headliners removes ride noise,
  which is why the park level is about twice as explainable.
- **The gain grows with lead** to d60 (the naive reference goes stale; drivers carry the calendar
  change). d90 is lower: fewer origins have training rows that far out.
- **The additive model is not enough:** similar variance explained at d7–d60 but worse level MAE
  than naive at d1–d7 and d90 — the drivers interact (a school break matters only where the park is
  open long hours).
- **Weather (ORACLE)** adds +0.02–0.03 park R², an upper bound, and only ≤ 16 days of forecast exist.
- **Where:** ride-day R² at d1–d60 is 0.15–0.21 in EU, 0.05–0.14 NA, 0.01–0.09 Asia (holiday data is
  richest for EU). By season it wins most in autumn (R² 0.22–0.31) and spring; **in summer it is
  slightly worse than naive at d1–d14** (+0.06–0.10 min, every CI excluding 0) — a stable regime where
  the reference is already right — n.s. at d30 and a win at d60/d90.

## 3. Which drivers

Mean deviation from naive with vs without the driver, d7, park-cluster 95 % CI
(`drivers/tables/univariate_effects.csv`):

| Driver | Effect | Share of ride-days |
| --- | --- | --- |
| opening hours ≥ 1 h shorter than the reference days | −28 % [−31, −24] | 14 % |
| opening hours ≥ 1 h longer | +21 % [+15, +27] | 18 % |
| public holiday (own region) | +21 % [+15, +28] | 4 % |
| any school holiday (own or neighbour) | +21 % [+17, +25] | 58 % |
| school break ended vs reference | −20 % [−24, −16] | 32 % |
| school break started vs reference | +19 % [+13, +25] | 26 % |
| first day after a closure of ≥ 2 days | −18 % [−24, −12] | 3 % |
| neighbour public holiday | +17 % [+12, +23] | 5 % |
| closes ≥ 2 h later than reference (evening event) | +14 % [+8, +22] | 6 % |
| day before / after a public holiday | +11 % / +10 % | 3 % each |
| bridge day | +31 % [+6, +64] | 0.06 % (8 parks) |
| rain ≥ 10 mm (ORACLE) | −12 % [−16, −9] | 8 % |
| max temp ≥ 32 °C (ORACLE) | −3 % [−6, 0] | 21 % |
| ticketed event (synced only since Sep 2026) | 0 % [−5, +6] | 2 % |
| first / last 7 days of the season | n.s. | 0.2 % (10 parks) |

Slow drivers grow with lead (school breaks +21 % at d7 → +32 % at d30): the naive reference has not
seen the break yet.

Permutation importance (`permutation_importance.csv`) ranks the **ORACLE-weather** fit
(`cal+hol+sched+wx`) on one fold, pooled over L ∈ {1, 7, 30}, so it has `wx` features and no `lead`
column: `log_ref` +0.0734 (mean reversion), `dow` +0.0119, `is_headliner` +0.0092, `r_hours_a`
(opening hours relative to the reference) +0.0088, `d_school` (change of school-break state) +0.0067,
`close_h_a` +0.0047, `hours_a` +0.0046, then weather (`d_precip` +0.0028). Group ablation (park R²,
d7): `cal+hol` 0.152, `cal+sched` 0.190, both 0.222 — schedules carry more than holidays and the two
add up.

## 4. Data limits

- **Schedules are only partly known at the origin.** `updated_us` says the OPERATING window was
  written before the origin for 84 % of ride-days at d1, 69 % at d30, 59 % at d60, 43 % at d90 (a
  lower bound: a re-sync rewrites `updated_us`). Operator publishing horizon median 39 days.
- **Ticketed events / extra hours exist only from September 2026** (ticketed on 33 parks / 2.0 % of
  rows, extra hours on 25 parks / 2.2 %): one Halloween season.
- **Season start/end** are rare (10 parks) and n.s.; the first season starts where recording starts.
- **No previous year**: month / day-of-year effects cannot be learnt until every month has one year
  of history, from **2026-12-23** — the cheapest next improvement.
- **Three offline `sched` drivers are only partly as-of.** `since_season_start`, `to_season_end` and
  `since_prev_open` use window functions over the whole `windows` table while `add_asof` NULLs a
  driver only on the target day's own `win_upd_us`, so a value can derive from neighbour rows
  published after the origin. The first two are n.s. and out of the top 16 of 37; `since_prev_open`
  backs the "first day after a closure" effect (−18 %). The harness plug-in is clean of it (§6).

## 5. Ranked covariates worth adding to TFT / CatBoost

TFT's `FUTR_EXOG` (nf-service `db.py`) carries `is_holiday_primary`, `is_holiday_neighbor`,
`is_school_holiday`, `is_bridge_day`, `days_until/since_holiday`, weekday one-hots, `season_code`,
`is_peak_season` and daily weather — **no schedule, opening-hours or operating-window column**.
CatBoost has `is_park_open`, `has_special_event`, `has_extra_hours` but no opening-hours length. Both
carry the school-break *state*, neither the *change*.

1. **Published opening hours relative to the recent same-weekday days** (length, closing hour, ratio
   and difference to the last four same weekdays) — the largest single driver (±20–30 %). Feed it
   **as-of** (NULL where not yet published), never the final version: the final version adds only
   +0.01–0.03 R².
2. **Change of school-break state against the reference window** (`d_school`, own + neighbour
   regions): −20 % when a break ended, +19 % when one started at d7, −23.5 % / +23.8 % at d30.
3. **First day after a closure** of ≥ 2 days (`since_prev_open`, parks closed Mon/Tue): −18 %.
4. **Late close / evening event** (close hour vs reference): +14 %.
5. **Weather forecast**: both models have it; ≤ +0.03 R² even as ORACLE, 16 days of reach.
6. Bridge days / day before-after holidays — real but rare; TFT already has `is_bridge_day` and
   `days_until/since_holiday`.

**Wired as a shadow (PAR-1057):** 1–4 are futr_exog of the grid TFT shadow with the time-weighted
daily P90 as target ([TFT grid shadows](tft-grid-shadows.md#the-par-830-drivers-in-the-grid-shadow-par-1057));
its live cells are `tft_grid_drivers` in the [model comparison](forward-archive/model-comparison.md).
The served TFT has none of them.

**Ticketed-event and extra-hours flags: do not wire them until a second season exists.** Ticketed
is n.s. at d7 (+0.3 % [−5.2, +5.8]) and significantly *negative* at d30 (−15.2 % [−24.3, −8.0]), and
`EXTRA_HOURS` — longer hours by definition — is −15.8 % [−28.3, −2.2]: the sign is an artefact of
when the flags started being stored (one synced Halloween against Feb–Aug), not an event effect.

**What success looks like** (measure on the full period, not a window): the grid TFT's level should
close most of the D7 d8–30 gap (`lvl_tft` +0.000 over `lvl_naive4` at paired share 0.68,
`lvl_driver_level` +0.068) and the D6 gap at d30 (`lvl_tft` +0.003, `lvl_driver_level` +0.032). At
d31–90 `lvl_tft` is not measurable at its current coverage (19 % of day pairs, below
`MIN_COVERAGE`). Expect nothing on full-period slot MAE. If the gaps do not close, the drivers'
value is in a separate level composer, and the shadow (§8) is its home.

## 6. The harness plug-in `driver_level`

`--model driver_level` is the same gradient-boosted model built only from the runner's DataFrames
(history + known-future covariates, accumulated across origins; `truth_history_days = 400`, refit
every 28 days, every origin below `settled_rows = 20000` training rows, at most 300,000 rows, seed
830). `driver_level` = naive level × H5 curve × exp(ŷ); `lvl_driver_level` / `driver_level_x_h5`
come from `predict_daily` and equal `driver_level` on every slot metric.

It is **weaker than the §2 model**: `FEATURES` drops `is_headliner` (the offline model's #3 feature;
the runner exposes no headliner flag to plug-ins), `since_season_start_a` / `to_season_end_a` (the
as-of caveat of §4), and `ticketed_a` / `extra_a` / `ref_ticketed` (§5). Its `since_prev_open_a` is
computed from windows rebuilt out of observed past truth, so no post-origin information enters. §7
is therefore a lower bound on what §2 priced.

## 7. Scored through the harness

Run `20261009-par830-drivers`: git `e0a4547a`, `code_sha256` `915dc784…`, export `20261009`, 231
origins 2026-02-19 … 10-07, 3 shards, 197 shard-minutes CPU, no GPU. Two passes: the headline
target window 2026-08-15 … 10-07 (`tables_2026-08-15_2026-10-07/`) and the full period (`tables/`).
The run contains PAR-827's coverage-parity fixes, and its baselines reproduce
`20261009-baselines-v2` on every slot and decision cell (3,688 of 3,770 shared `cells.csv` keys
agree to < 1e-12; the other 82 are UC4 / D7 `lvl_*` tie-order differences at identical `n`).

Two defects of the code this run was scored with (fixed at PAR-827's head since): its
`usable_horizon.csv` and `handover.csv` gate UC4 on month labels (`n_origin_days = 3`) instead of
park-months, so they print `leads_tested = 0` and name the reference as UC4 winner — read UC4 from
`cells.csv` (`n_park_days`, `wins`, `diff_lo_park/hi_park`); and it has no `cells_all_refs.csv`, so
margins against non-reference baselines below are differences of paired differences through the
lead's common reference (exact where coverage coincides; 92–96 % here) and, where marked, point
estimates without a CI.

### 7.1 Slot MAE: an autumn win that reverses over the year

In the headline window `driver_level` has the lowest UC2/UC3 slot MAE of every model at d1–d60 on
both segments (all rides vs `h5` −0.226 at d1 … −0.061 at d60; busy −0.985 … −0.940), with usable
horizon d10 (all) and d60 (busy) under the park-cluster rule. **Over the full period it has no
usable horizon on either segment** and is worse than `h5` at every lead. Paired margin, full period,
negative = `driver_level` better:

| vs | Segment | d1 | d3 | d7 | d14 | d30 | d45 | d60 | d90 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `h5` | all | +0.273 | +0.248 | +0.350 | +0.348 | +0.379 | +0.500 | +0.494 | +0.935 |
| `wt_med` | all | +0.206 | +0.176 | +0.270 | +0.260 | +0.280 | +0.396 | +0.401 | +0.839 |
| `clim` | all | −0.316 | −0.314 | −0.102 | +0.004 | +0.124 | +0.262 | +0.329 | +0.644 |
| `lvlh5_tft` | all | +0.118 | −0.002 | −0.054 | −0.186 | −0.181 | −0.090 | gated | — |
| `h5` | busy | +0.063 | +0.019 | +0.238 | +0.209 | +0.220 | +0.391 | +0.326 | +1.110 |
| `wt_med` | busy | −0.112 | −0.169 | +0.032 | −0.010 | −0.023 | +0.140 | +0.106 | +0.890 |
| `lvlh5_tft` | busy | +0.383 | +0.098 | −0.000 | −0.351 | −0.442 | −0.270 | gated | — |

Against the per-lead reference the all-rides deficit is significant under both bootstrap units at
every lead except d6 (+0.110 [−0.023, +0.248] park-cluster). `lvlh5_tft`'s paired share is only
0.38–0.69 at d1–d45 and 0.01 at d60 (gated).

**Why the passes disagree — season, not cold start.** `tables/mae_by_season.csv` (unpaired, read
the direction) against `h5`:

| Lead | Autumn | Spring | Summer | Winter |
| --- | --- | --- | --- | --- |
| d1 | −0.467 | +0.424 | +0.320 | +1.150 |
| d3 | −0.523 | +0.425 | +0.291 | +1.432 |
| d7 | −0.433 | +0.525 | +0.347 | +2.372 |
| d30 | −0.392 | +0.666 | +0.433 | — |

- Summer origins carry 4–6 months of history (far above `settled_rows`) and still lose +0.320 at d1
  while autumn origins win −0.467 — opposite signs no history-length effect produces.
- The offline analysis, which has no cold start, also finds summer worse at d1–d14 (§2).
- "Winter" *is* the cold start (targets 2026-02-19 … 28) and is 1.91 % of d1 slots; excluding winter
  and all of spring still leaves +0.143 (d1) … +0.242 (d30) vs `h5`. The cold start shows only in
  spring (≈ +0.17 min at d30, an estimate).
- In the headline window, lead and origin season move together (d1 origins are autumn, d60–d90
  origins summer), so its −0.36 → +0.23 gradient is partly the model getting worse at summer origins.

The model beats the naive level on slot MAE at autumn / season-transition origins and loses at
spring, summer and cold-start origins. One autumn has been observed.

### 7.2 What it makes worse

- **D3 best time (`bestVisitTimes`) regresses**, significant under both units in both passes, with
  no usable horizon while `h5` reaches d45 (window) / d60 (full period). Headline window: top-2 hit
  rate 0.790 / 0.786 / 0.771 at d1 / d7 / d30 against `h5` 0.819 / 0.819 / 0.820 (paired −0.034 /
  −0.038 / −0.054); regret +0.51 min (d1) and +0.86 min (d30) vs `h5`. Full period: −0.021 … −0.038
  vs `clim`, −0.035 … −0.047 vs `h5`. Cause: the first hour is deliberately not scaled (BENCH-SPEC
  baseline 5), so scaling the rest of the day moves the predicted minimum against the rope-drop ramp
  (`lvlh5_tft` has the same defect at about half the size). **No level plug-in may feed
  `bestVisitTimes`.**
- **d90 on all rides** is worse than naive in the window (+0.234 [+0.040, +0.408]) and everywhere on
  the full period.
- **Asia gains nothing** (d1 −0.02 [−0.35, +0.30], d30 +0.31 n.s.; EU d1 −0.54, NA d1 −0.32
  significant).
- **UC3 dayPeak absolute error** is no better than the reference at d1/d7 and worse at d60/d90.

### 7.3 Ranking the day — what the level is for

These survive both passes (full period quoted; `*` = CI excludes 0 under both units):

| Surface | Lead | `lvl_driver_level` vs reference | Notes |
| --- | --- | --- | --- |
| D7 day comparison (winner accuracy) | d8–30 / d31–90 | **+0.068 [+0.053, +0.082]\*** / **+0.094 [+0.075, +0.113]\*** | 223 / 200 origin days, paired share 1.0; window +0.148 / +0.117 |
| D6 crowd bucket (exact) | d30 / d90 | **+0.032 [+0.013, +0.050]\*** / **+0.024 [+0.004, +0.043]\*** | d7 only park-day-significant; d1 **−0.020\*** vs `lvl_snaive7`; **d120 −0.034 [−0.050, −0.017]\*** (window −0.045) |
| UC4 within-park-month Spearman | d1 / d7 / d30 / d60 | **+0.065 / +0.100 / +0.109 / +0.086\*** | 607 / 522 / 446 / 349 park-months; d90 +0.029 n.s.; **d120 −0.029 [−0.052, −0.007]\*** (window −0.066) |
| D4 optimiser regret (min per park-day) | d1 / d3 / d7 / d14 / d30 | −1.23 / −1.52 / −1.17 / −1.47 / −1.44\* | the largest margin of any model, ~1.3–1.9× `h5`'s (−0.66 … −1.14); usable to d30 like `h5` (d14 in the window, against `h5`'s d7) |

- At d1–d6 the bar is the seasonal naive (`lvl_snaive7`: D6 0.434, D7 0.519) and `driver_level`
  does not clear it.
- UC4 wins are contiguous d1–d60 under both units, so the corrected UC4 usable horizon is **d60**;
  `lvl_driver_level` is the only level source that beats the reference at any UC4 lead and is ahead of
  every level source at every lead on UC4 d1–d90, D6 d14–d90 and both D7 bands (point estimates
  against `lvl_tft` / `lvl_cbd`, which hold 0.003–0.68 of the rows).
- **Nothing beyond d90:** UC4 and D6 both turn into a significant loss at d120 — against the
  strongest naive, `lvl_naive4`, specifically (against `lvl_clim` it is still ahead on UC4). D6 at
  d180 is a loss on the park-day unit only.
- **dayPeak ordering is not this model's.** It wins at no lead in either pass (`lvlh5_tft` d0–d45,
  `lvlh5_naive` / `h5` d60–d90), and its margin is below `h5`'s at every scored lead d7–d90 except
  d10 (+0.0239 vs +0.0225). Its d90 horizon is a four-way tie with `h5`, `lvlh5_naive` and
  `driver_level_x_h5`. The planner optimiser drops the headliner with the lowest `dayPeak`, so
  routing `dayPeak` to it would pick the worst candidate.

The drivers improve the **relative ordering of days** — what the crowd calendar, the "Empfohlen"
star and the day comparison consume — while the absolute minutes improve only when the naive
reference is stale, i.e. at a season transition.

### 7.4 Coverage and the level gap

- **It abstains more** (only where a naive reference exists): it publishes for 63.7 % of
  never-operated ride-days against `h5`'s 85.0 % (d1, D9), at the price of covering 81.9 % of operated
  ride-days against 91.8 %; serving would need `h5` as fallback. Its level reaches 96.5 % of ride-days
  at d90, 92.6 % at d120, 78.5 % at d180 — reach is not skill (see d120).
- **Share of the d1 level gap closed** ((`h5` − `driver_level`) / (`h5` − `oracle_level`)): in the
  headline window 8.5 % on all rides (7.980 → 7.756 vs 5.338) and 15–17 % on busy rides; over the
  full period −11.0 % on all rides (it widens the gap) and +0.5 % on busy rides.

### 7.5 Recommendation

Shadow only, scoped to exactly four ranking surfaces, judged over a spring and a summer:

| Surface | May feed | Evidence |
| --- | --- | --- |
| D6 crowd bucket / `predictedCrowdLevel` | d14–d90 | +0.032 (d30), +0.024 (d90) |
| D7 day comparison / "Empfohlen" star | d8–30, d31–90 | +0.068, +0.094 |
| D4 optimiser regret (ride order) | d1–d30 | largest margin of any model |
| UC4 within-park-month ranking / crowd calendar | d1–d60 | +0.065 … +0.109 |

Excluded, each for a measured reason: the 15-minute slot curve (no usable horizon over the year),
`bestVisitTimes` / D3 (regression on a shipped surface), `dayPeak` (loses to `h5` and `lvlh5_tft`),
anything beyond d90 (UC4 and D6 lose at d120). It needs no GPU and no new service.

## 8. The production shadow (PAR-1058)

`ml-service/driver_level.py` is a pandas port of `drivers.py` and the plug-in's fit / predict (same
features in the same order, same `HistGradientBoostingRegressor` parameters, seed 830). At 01:30 UTC
the `forecast-archive` queue's `driver-level` job calls ml-service `POST /driver-level/run`, which
starts the job in its own process and answers **409** while a run is live (`start_single_flight`,
an `flock` held by the child, freed even by an OOM kill). It fits on ~400 days of ride-day P90s, at
most `MAX_TRAIN_ROWS` = 300,000 rows on two threads (CPU, minutes), and replaces
`driver_level_forecasts` with one level per ride for origin+1 … origin+91.

**Why 01:30 UTC:** its scan is one all-park `PERCENTILE_CONT` over 400 days of `queue_data` (the
query nf-service already runs nightly), so it sits between `generate-daily` (01:00, ~2 min) and the
02:00 `queue_data` readers (`calculate-percentiles`, `detect-feature-drift`, 02:30
`detect-seasonal`), well before the 03:00 TFT training; the cron carries `tz: "UTC"`.

**Differences from the bench** (`ml-service/tests/test_driver_level.py` reproduces the plug-in's
driver rows to 1e-12 and levels to 1e-9 on a fixture it generated): ride-day P90s are the raw
change-log P90 the served TFT trains on (nf-service `fetch_daily_peak_panel`), not 15-minute slot
truth; past opening windows are the published ones; unpublished future days get the runner's
projection; the training-row cap is a seeded sample instead of DuckDB `hash()`; school flags past a
location's stored school data become unknown once PAR-1049's `SchoolCoverage` is in ml-service.

The forward-archive capture copies the level into `forecast_archive_curves.driver_day_peak` and the
scorer pairs it as model `driver_level` on PEAK and D7 (`cal_rank`, `cal_busier`, `cal_far`,
`cal_far_90`; matrix column „Treiber-Niveau") — [model comparison](forward-archive/model-comparison.md#the-shadow-levels).
D3 and the 15-minute curve are not fed. The switch decision `calendar_driver_level` is judged on
**2027-09-01**, on a summer window (the bench saw it lose in spring and summer); spring needs a
manual read in late May 2027, because a 28-day window cannot hold both seasons and
`forecast_archive_paired` keeps 120 days.
