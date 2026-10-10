# What explains the daily level — driver analysis (PAR-830)

> **Question.** The multi-day error is dominated by the daily LEVEL (a perfect
> level takes the d1 slot MAE from 7.58 to 5.43). How much of a day's deviation
> from the naive level can be explained by drivers known in advance, and how much
> is noise? **Answer (2026-10-09 export):** with the drivers known at the origin,
> **12–16 % of the squared log deviation of a ride-day and 19–28 % of a park-day**
> is explainable out of sample; the rest is noise at this resolution. That cuts
> the level MAE by 4–8 % per ride and 7–12 % per park, and lifts the within-month
> day ranking (UC4) from 0.27 → 0.34 at d1 and 0.11 → 0.25 at d60.

Code: `ml-bench/mlbench/drivers.py` (the driver table, shared),
`ml-bench/mlbench/driver_analysis.py` (`python -m mlbench drivers`),
`ml-bench/mlbench/models/driver_level.py` (the harness plug-in). Results:
`ml-bench/results/20261009-par830-drivers/`.

## 1. Method

- **Target.** `y = log(true daily P90 / naive)`, per ride-day and lead `L`. Naive
  = baseline 5a: median of the last four same-weekday daily P90s inside the 56
  days before the origin `date − L` (≥ 2). A test pins this to the harness
  definition.
- **Drivers**, in four groups (`drivers.GROUPS`):
  - `cal` — weekday, lead, region, headliner, and the reference itself (log
    level, weeks it rests on, age). **No month / day of year**: with < 1 year of
    history every test month is one the model has never seen, and those features
    made the calendar model *worse* than naive (measured, first run).
  - `hol` — public / school holidays of the own region and the neighbour regions
    (ml-service `holiday_features.py`, OR semantics), bridge days, day
    before/after a holiday, and how many reference days were holidays (the
    reference's own contamination).
  - `sched` — published opening hours (length, open/close hour, ratio and
    difference to the reference days), ticketed events, extra hours, days since
    the season opened / until it closes, days since the park last opened —
    **only where the schedule row was written before the origin**
    (`drivers.add_asof`, from `schedule_entries.updated_us`). `sched_final` is the
    same as finally published: an upper bound, never the headline.
  - `wx` — daily weather and its difference to the reference days. **ORACLE**:
    the export has actuals only (no forecast archive), so it is always a
    separate row.
- **Validation.** Walk-forward by ORIGIN month: the model for the origins of
  month M is trained on ride-days whose target date is before the 1st of M.
  One model per fold and feature set, pooled over the leads 1/3/7/14/30/60/90
  with `L` as a feature, scored per lead. Origins April → October 2026
  (Feb/March have < 20 000 training rows).
- **Common target window.** Headline tables score every lead on the same target
  days (≥ 2026-06-30 = first scored origin + 90 d); `*_all.csv` has the full
  window.
- **Models.** (a) univariate effects with park-cluster bootstrap CIs, (b) a
  GAM-like additive model (cubic splines per driver + one-hot categoricals,
  ridge, shifted by the median training residual), (c) gradient boosting
  (sklearn `HistGradientBoostingRegressor`, absolute loss, CPU) as the upper
  bound.
- **Metrics.** R² vs naive = `1 − Σ(y − ŷ)² / Σy²` (the share of the squared log
  deviation from naive that is explained; 0 = naive), level MAE in minutes with
  paired park-day bootstrap CIs, at ride level and at park level (headliner
  mean), and UC4 = Spearman of the park level within park-month (≥ 8 days),
  paired against naive on the same days.

## 2. How much is explainable (common window, out of sample)

R² vs naive — main model = gradient boosting on `cal+hol+sched` (as-of):

| lead | 1 | 3 | 7 | 14 | 30 | 60 | 90 |
|---|---|---|---|---|---|---|---|
| ride-day | 0.117 | 0.113 | 0.136 | 0.146 | 0.161 | 0.157 | 0.097 |
| park-day (headliner mean) | 0.195 | 0.187 | 0.222 | 0.244 | 0.268 | 0.282 | 0.204 |
| park-day, + weather (ORACLE) | 0.224 | 0.217 | 0.250 | 0.271 | 0.291 | 0.304 | 0.231 |
| park-day, schedules as finally published (UPPER) | 0.205 | 0.194 | 0.236 | 0.265 | 0.291 | 0.299 | 0.213 |

Level MAE (minutes), naive → main model, all CIs exclude 0:

| lead | 1 | 3 | 7 | 14 | 30 | 60 | 90 |
|---|---|---|---|---|---|---|---|
| ride-day naive | 9.00 | 9.00 | 9.70 | 10.28 | 10.93 | 11.32 | 11.12 |
| ride-day Δ | −0.36 | −0.34 | −0.49 | −0.59 | −0.78 | −0.92 | −0.49 |
| park-day naive | 11.93 | 11.93 | 13.17 | 14.05 | 15.05 | 15.80 | 15.04 |
| park-day Δ | −0.88 | −0.82 | −1.06 | −1.23 | −1.62 | −1.92 | −1.14 |

- **The unexplained rest is noise at this resolution**: a ride-day's log
  deviation has SD 0.56 (d1) – 0.66 (d60); even the oracle-weather model leaves
  86 % of it (ride) and 78 % (park). Averaging over headliners removes ride noise,
  which is why the park level is roughly twice as explainable.
- **The gain grows with the lead** to d60: the naive reference gets staler (it is
  a different season) and the drivers carry the calendar change. d90 is lower,
  because fewer origins have training rows that far out.
- **The additive model is not enough.** The GAM explains about as much variance
  at d7–d60 but its level MAE is worse than naive at d1–d7 and d90: the drivers
  interact (a school break matters only where the park is open long hours).
- **Weather (ORACLE)** adds +0.02–0.03 park R² on top — an upper bound, since a
  forecast is worse than the actual; and only ≤ 16 days exist at all.
- **Where:** EU ride-day R² 0.15–0.21, NA 0.05–0.14, Asia 0.01–0.09 (holiday
  data is richest for EU). By season the model wins most in autumn (R² 0.22–0.31,
  school breaks ending, Halloween hours) and spring; in **summer it is slightly
  worse than naive at d1–d14** (+0.06–0.10 min): the regime is stable for weeks,
  the reference is already right, and the model adds variance.

## 3. Which drivers (univariate, `tables/univariate_effects.csv`)

Mean deviation from naive with vs without the driver (park-cluster 95 % CI), d7:

| driver | effect | share of ride-days |
|---|---|---|
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
| ticketed event (only synced since Sep 2026) | 0 % [−5, +6] | 2 % |
| first / last 7 days of the season | n.s. | 0.2 % (10 parks) |

Effects grow with the lead for the slow drivers (school breaks +21 % at d7 →
+32 % at d30) — the naive reference has not seen the break yet.

Permutation importance of the main model (`tables/permutation_importance.csv`):
the reference level (mean reversion — high references come down), weekday,
headliner, then **opening hours relative to the reference** (`r_hours`,
`close_h`, `hours`), the **change in school-break state** (`d_school`), then
weather (ORACLE). Group ablation (common window, park R², d7): `cal+hol` 0.152,
`cal+sched` 0.190, both 0.222 — schedules carry more than holidays and the two
add up.

## 4. Data limits found on the way

- **Schedules are only partly known at the origin.** `updated_us` says the
  OPERATING window was written before the origin for 84 % of ride-days at d1,
  69 % at d30, 59 % at d60 and 43 % at d90. A re-sync rewrites `updated_us`, so
  this is a lower bound — but the "as finally published" variant is an upper
  bound and is reported only as such. Operator horizon median 39 d.
- **Ticketed events / extra hours exist only from September 2026** (the sync
  stores them since then): their measured effect covers one Halloween season.
  The evening-event signal before that comes from the opening hours (late close).
- **Season start/end** are rare in the window (10 parks) and not significant;
  the first season in the data starts where our recording starts and is masked.
- **No previous year** — month / day-of-year effects cannot be learnt yet; from
  2026-12-23 every month has one year of history.

## 5. Ranked covariates worth adding to TFT / CatBoost

TFT `FUTR_EXOG` (nf-service `db.py`) has holidays, calendar and weather but
**no schedule information at all**; CatBoost has `is_park_open`,
`has_special_event`, `has_extra_hours` but no opening-hours length.

1. **Published opening hours relative to the recent days** (`hours`, ratio and
   difference to the same weekday's last 4 weeks, closing hour) — the largest
   single driver (±20–30 %), missing in both models. Known only within the
   operator horizon: feed it as-of (NULL when not yet published), never the
   final version.
2. **Change of school-break state vs the reference window** (`d_school`, own +
   neighbour regions) — both models have the state, not the change; the change is
   what moves the level against a recent-history baseline (−20 % / +19 %).
3. **First day after a closure** (`since_prev_open`, parks closed Mon/Tue) — −18 %.
4. **Evening event / late close** (close hour vs reference) — +14 %; ticketed
   event flags add nothing measurable yet (one season).
5. **Weather forecast** (rain ≥ 10 mm −12 %) — both models have it; the gain is
   ≤ +0.03 R² even as ORACLE and limited to 16 d. Archive the forecasts (PAR-831)
   before claiming more.
6. Bridge days / day before-after holidays — real but rare; TFT already has
   `is_bridge_day` and `days_until/since_holiday`.

**Achievable gain:** about 4–8 % level MAE per ride and 7–12 % per park at
d1–d60, ranking +0.06–0.15 Spearman within month. Section 6 shows what that
buys on the 15-min slots.

## 6. Scored through the harness (`driver_level`)

Run `20261009-par830-drivers` (git `e0a4547a`, export `20261009`, 231 origins
2026-02-19 … 2026-10-07, 3 shards, 197 shard-minutes CPU, no GPU). Numbers below
are the **headline window** `--target-from 2026-08-15 --target-to 2026-10-07`:
every lead is scored on the same 54 target days, so the horizon curve is not
confounded with season. Tables:
`ml-bench/results/20261009-par830-drivers/tables_2026-08-15_2026-10-07/`
(`summary_tables_2026-08-15_2026-10-07.md`, `cells.csv`, `handover.csv`,
`usable_horizon.csv`); full-window equivalents in `tables/` and `summary.md`.

**Provenance.** `git_sha` `e0a4547a`, `code_sha256` `915dc7843c0b0afe`, image
`sha256:7b5e872f614f`, `--reference` (the run refuses to start without a known
SHA and image id). `e0a4547a` has PAR-827's `ea9a8438`, `a1d20bf8`, `336b2b62`
and `689a7b84` as ancestors, and `mlbench/baselines.py`, `mlbench/runner.py` and
`mlbench/build.py` are byte-identical to `ea9a8438`. Those commits are the
**plug-in ↔ built-in coverage-parity fixes**: before them a plug-in forecast only
the origin-known window while the built-in baselines forecast the whole
published one, the plug-in grid could emit slot starts matching no truth slot,
and a slot at a projected closing time read as inside the window. This run has
them, so the cross-baseline comparison below is sound — a run without them is
not comparable to it. The `mlbench` sources of this branch still hash to that
same `code_sha256`, so rebasing onto the PAR-827 head changed no benchmark code
and the run needs no repeat.

The plug-in's baselines reproduce the PAR-827 `20261009-baselines-v2` run
**bit-identically** (`wt_med`, `clim`, `h5`, `lvlh5_tft`, `oracle_level`,
`oracle_shape`, `prod_served` agree to 1e-15 at every lead checked), so the
comparison below is against exactly the published baselines.

### 6.1 Hand-over — UC2/UC3 slot MAE (15-min slots, minutes)

Paired difference against the per-lead naive reference (`wt_med` at d1–d6,
`clim` from d7, `wt_med` again at d90), point [95 % **park-cluster** bootstrap];
`*` = that CI excludes 0 in the model's favour. All leads have 54 origin days —
none is LOW-N under the ≥ 30-origin-day gate. `persistence` is UC1-only and is
not scored here (BENCH-SPEC baseline 1); it loses to the profile beyond ~1 h.

All rides:

| model | d1 | d3 | d7 | d14 | d30 | d60 | d90 |
|---|---|---|---|---|---|---|---|
| `wt_med` | (ref) | (ref) | −0.09 [−0.32, +0.16] | +0.21 [−0.04, +0.49] | +0.18 [−0.07, +0.45] | +0.05 [−0.23, +0.32] | (ref) |
| `h5` | −0.14 [−0.17, −0.11]\* | −0.14 [−0.17, −0.11]\* | −0.23 [−0.45, +0.01] | +0.07 [−0.17, +0.34] | +0.05 [−0.19, +0.32] | −0.03 [−0.30, +0.24] | −0.06 [−0.08, −0.04]\* |
| `lvlh5_tft` | −0.29 [−0.52, −0.05]\* | −0.19 [−0.42, +0.04] | −0.15 [−0.35, +0.06] | +0.35 [+0.15, +0.56] | +0.64 [+0.36, +0.94] | gated (1.6 % cov.) | — |
| **`driver_level`** | **−0.36 [−0.55, −0.19]\*** | **−0.41 [−0.62, −0.23]\*** | **−0.44 [−0.66, −0.22]\*** | −0.11 [−0.33, +0.13] | −0.15 [−0.43, +0.15] | −0.09 [−0.45, +0.25] | +0.23 [+0.04, +0.41] |
| `oracle_level` | −2.79 [−3.18, −2.41]\* | −2.86 [−3.26, −2.47]\* | −3.09 [−3.52, −2.68]\* | −3.01 [−3.43, −2.59]\* | −3.17 [−3.61, −2.72]\* | −3.07 [−3.57, −2.61]\* | −2.97 [−3.44, −2.54]\* |

Ex-ante busy rides (ride q90 ≥ 45 min over the 56 days before the origin):

| model | d1 | d3 | d7 | d14 | d30 | d60 | d90 |
|---|---|---|---|---|---|---|---|
| `h5` | −0.31 [−0.38, −0.24]\* | −0.32 [−0.40, −0.25]\* | −0.33 [−0.40, −0.25]\* | +0.21 [−0.41, +0.90] | +0.22 [−0.44, +0.93] | −0.22 [−0.96, +0.57] | −0.14 [−0.21, −0.09]\* |
| `lvlh5_tft` | −1.34 [−1.86, −0.82]\* | −1.09 [−1.64, −0.58]\* | −0.65 [−1.18, −0.14]\* | +0.09 [−0.38, +0.61] | +0.83 [+0.12, +1.70] | gated | — |
| **`driver_level`** | **−1.30 [−1.75, −0.88]\*** | **−1.41 [−1.89, −0.97]\*** | **−1.24 [−1.78, −0.75]\*** | **−0.75 [−1.27, −0.15]\*** | **−0.82 [−1.48, −0.10]\*** | **−1.16 [−2.08, −0.27]\*** | −0.27 [−0.74, +0.21] |
| `oracle_level` | −6.27 [−6.94, −5.55]\* | −6.43 [−7.10, −5.69]\* | −6.83 [−7.54, −6.05]\* | −6.83 [−7.48, −6.08]\* | −7.18 [−7.94, −6.36]\* | −7.34 [−8.42, −6.38]\* | −6.87 [−7.80, −5.94]\* |

From d7 the per-lead reference is **climatology**, not the weekday median.
PAR-827's baselines-v2 hand-over shows **climatology itself winning every
all-rides cell from d7 to d60** (`significant_models` = 0), and on busy rides
only `lvlh5_tft` clearing it, at d7 and d10. `driver_level` is the **first model
in the benchmark to beat climatology at d7–d10 on all rides** (−0.44
[−0.66, −0.22] at d7, −0.39 at d10) and the **first to beat it at d14–d60 on
busy rides** (−0.75 … −1.16, all CIs excluding 0). At d14–d60 on all rides it
is better in the point estimate but its CI still includes 0, so that band stays
"climatology, nothing beats it" for the all-rides headline.

`lvlh5_tft` and `prod_served` at d45–d60 carry only 1.6 % of the reference's
paired slots (173 of 4 492 park-days), below the 30 % coverage gate — their
apparent 4.26 / 4.30 min are a coverage artefact and the harness correctly keeps
them out of the hand-over.

`driver_level` and `driver_level_x_h5` are numerically identical on every slot
metric: `predict` already returns the naive H5 curve scaled by exp(ŷ), which is
what the runner composes from `predict_daily`.

### 6.2 Usable horizon (`usable_horizon.csv`)

| metric | `h5` | `lvlh5_tft` | `driver_level` |
|---|---|---|---|
| UC2/UC3 slot MAE, all rides | d6 | d2 | **d10** |
| UC2/UC3 slot MAE, busy rides | d7 | d10 | **d60** |
| D4 optimiser regret | d7 | — (d7 single) | **d14** |
| D4 dayPeak pairwise ordering | **d90** | d45 | d30 (significant to d90) |
| D5 first-hour MAE (opening-aligned) | d7 | d7 | d7 |
| D3 best-time hit rate / regret | **d45** | — | — (worse than the reference) |
| D6 crowd bucket (level only) | n/a | d14 (`lvl_tft`) | significant d7–d90 |

Reading: usable horizon = largest lead up to which the model beats the per-lead
reference *contiguously* with the park-cluster CI excluding 0. `driver_level`
extends the usable horizon of the slot forecast from d6 to **d10** on all rides
and from d7 to **d60** on busy rides, and the optimiser's from d7 to d14.

### 6.3 What it does NOT improve

- **D3 best time (`bestVisitTimes`, ride overview) gets worse**: hit rate
  (top-2) 0.790 vs `clim` 0.818 at d1 and 0.771 vs 0.820 at d30 (−0.018 …
  −0.042, CIs exclude 0 against the model). A pure level scaler leaves the
  within-day ordering alone except that the first hour is deliberately *not*
  scaled, which shifts the rope-drop ramp against the rest of the day and makes
  the predicted minimum worse. `lvlh5_tft` has the same defect (−0.015 … −0.022).
  **Any level plug-in must therefore not drive the best-time surface.**
- **dayPeak absolute error** is no better than the reference at d1/d7 (−0.27
  [−0.56, +0.04], −0.36 [−0.72, +0.03]) and worse at d60/d90 (+0.42, +0.49).
- **d90 all rides is significantly worse than naive** (+0.23 [+0.04, +0.41]):
  the model has no month/day-of-year features (deliberately, §1) and at d90 the
  training rows that far out are the fewest.
- **Asia: nothing.** EU d1 −0.54 [−0.88, −0.26]\*, d7 −0.36 [−0.68, −0.08]\*;
  NA d1 −0.32 [−0.68, −0.05]\*, d30 −0.61 [−1.08, −0.19]\*; Asia d1 −0.02
  [−0.35, +0.30], d30 +0.31 [−0.36, +0.95]. The holiday and school-break data
  that carry the signal are richest for EU.
- **Summer confirms the offline finding.** `mae_by_season.csv` is
  `Σ|error| / Σn` per model, **each model on its own coverage — unpaired**, so
  read the direction, not the decimals (`driver_level` covers 82 % of operated
  ride-days against `h5`'s 92 %, §6.4a). Against `h5`: autumn −0.47 (d1) /
  −0.43 (d7) / −0.39 (d30), summer **+0.13 / +0.08 / +0.12**. The sign agrees
  with the paired offline analysis (§2: worse than naive in summer at d1–d14),
  which is why the finding stands: the gain comes from season transitions, and
  in a stable summer regime the naive reference is already right and the model
  only adds variance.

### 6.4 Ranking the day (UC4, D6, D7) — what the level is really for

`lvl_driver_level` is the level alone, scored on the daily products.

**UC4 is not measurable in the headline window.** The Spearman of the park
level within park-month needs ≥ 8 days of a park-month, and an 8-week target
window holds barely one month per park, so the metric rests on **3 origin
days** — one tenth of BENCH-SPEC's 30-origin-day gate. The harness gates it out
of the usable horizon, and this page does not quote a number from it: three
origin days cannot carry a UC4 claim, with or without a CI. The UC4 row of the
headline window reads **"not measurable in this window"**. The full-period pass
(`tables/cells.csv`, §6.7) is where the harness UC4 number comes from.

Keep the two UC4 sources apart. §2's figures — naive 0.272 → drivers 0.321 at
d1 and 0.11 → 0.25 at d60 over 385 park-months — come from **this issue's own
walk-forward analysis** (`python -m mlbench drivers`, `uc4_spearman_common.csv`),
not from the benchmark harness. They are the quotable offline numbers. The
harness's UC4 (`lvl_driver_level` in `cells.csv`) is a separate measurement on
the benchmark's own park-months and is reported only from the full period.

Not LOW-N, and the clearest result of the run:

| metric | lead bucket | `lvl_naive4` | `lvl_tft` | `lvl_cbd` | **`lvl_driver_level`** | origin days |
|---|---|---|---|---|---|---|
| D6 crowd bucket exact | d1 | 0.358 | 0.415 | 0.232 | 0.410 — loses to ref `lvl_snaive7` 0.434 | 54 |
| D6 crowd bucket exact | d7 | 0.322 (ref) | 0.355 \* | 0.206 | **0.373 (+0.052 [+0.031, +0.070])\*** | 54 |
| D6 crowd bucket exact | d30 | 0.255 (ref) | 0.270 | 0.185 | **0.326 (+0.071 [+0.040, +0.100])\*** | 54 |
| D6 crowd bucket exact | d90 | 0.280 (ref) | — | — | **0.338 (+0.058 [+0.027, +0.087])\*** | 54 |
| D7 day comparison winner | d1–7 | 0.432 | 0.450 | 0.225 | 0.507 (ref `lvl_snaive7` 0.519) | 58 |
| D7 day comparison winner | d8–30 | 0.333 (ref) | 0.405 \* | 0.154 | **0.481 (+0.148 [+0.113, +0.178])\*** | 75 |
| D7 day comparison winner | d31–90 | 0.332 (ref) | 0.369 \* | 0.134 | **0.449 (+0.117 [+0.083, +0.151])\*** | 112 |

At **d1–d6 the bar is seasonal naive** (`lvl_snaive7`: D6 0.434, D7 0.519) and
`lvl_driver_level` does not clear it — same weekday, seven days ago is simply a
very good short-lead level. From d7 the seasonal-naive reference expires and the
drivers rank days better than TFT and far better than CatBoost-daily at
every lead — exactly the surfaces (crowd calendar, "Empfohlen" star,
day comparison A vs B) the review found to be weak everywhere.

### 6.4a Coverage and openness (D9)

Two side effects worth knowing before anyone wires this up.

- **It abstains more, and that cuts the open/closed error.** The review's D9
  finding was that the weekday median publishes a forecast for ~84 % of
  ride-days that never operated. `driver_level` only forecasts where it has a
  naive reference (≥ 2 of the last four same-weekday P90s), so it publishes for
  **64 %** of never-operated ride-days instead of `h5`'s **85 %** — a 21-point
  improvement on a gap the review called real. The price is recall: it covers
  **82 %** of the ride-days that *did* operate, against `h5`'s **92 %**. Serving
  it would need `h5` as the fallback on the 10 % it drops.
- **The level reaches further than any other source.** `lvl_driver_level`
  carries a daily level for 96.5 % of ride-days at d90, 92.6 % at d120 and
  78.5 % at d180, matching `lvl_naive4`; `lvl_tft` stops after d60 and
  `lvl_cbd` earlier still. For the crowd calendar — which the frontend takes to
  12 months while `/predictions/yearly` stops at +182 d — the drivers are the
  only level source that exists at all beyond d60 *and* ranks days better than
  naive there (D6 +0.058 at d90, D7 +0.117 over d31–90).

### 6.5 Share of the d1 level gap closed

With `h5` as the served reference and `oracle_level` (the true daily P90 on the
H5 shape) as the ceiling, re-read from the PAR-827 **`20261009-baselines-v2`**
headline window (not the superseded `20261009-baselines`):

| segment | `h5` | `driver_level × h5` | `oracle_level` | gap closed |
|---|---|---|---|---|
| all rides | 7.980 | 7.756 | 5.338 | **8.5 %** |
| ex-ante busy rides | 15.096 | 14.189 | 9.160 | **16.5 %** |

`(h5 − driver_level×h5) / (h5 − oracle_level)`; computed both unpaired on the
values and paired through the common `wt_med` reference (8.47 % / 8.52 % for all
rides, 15.3 % / 16.5 % for busy) — the two agree, so the figure is not a
coverage artefact. **A level built only from drivers known in advance closes
roughly a twelfth of the level gap on all rides and a sixth on busy rides.** The
remaining ~85 % of the gap is what §2 measured as noise at this resolution plus
what a better level model (TFT with the covariates of §5, foundation models)
would have to find.

### 6.6 Recommendation

**Shadow only — do not serve.** `driver_level` **regresses `bestVisitTimes`
(D3), a surface the frontend ships today**, and it is **worse than `h5` in
summer at every lead**. Those two facts bound the recommendation to shadow by
themselves, regardless of how good the rest of the numbers are. Feed the
covariates into TFT in parallel; that is the half of this issue with the larger
pay-off.

For it:

- It is the only model in the run that beats the naive reference on slot MAE
  *and* extends the usable horizon (all rides d6 → d10, busy d7 → **d60**), and
  the only level source that wins D6/D7 at every lead from d8 to d90.
- Its d1–d7 slot margin (−0.36 … −0.44 on all rides) is 2.5–3× H5's and, unlike
  `lvlh5_tft`, it does not turn negative beyond d7 (`lvlh5_tft` +0.35 at d14,
  +0.64 at d30).
- It needs no GPU and no new service: 197 shard-minutes of CPU for 231 origins.
- It is the only level source that exists beyond d60 and still ranks days
  better than naive there, which is exactly the calendar's weak spot (§6.4a),
  and it publishes 21 points fewer forecasts for ride-days that never operate.

Against serving it now:

- **D3 best time regresses** (−0.018 … −0.042 hit rate). The ride overview is a
  surface the owner cares about; a level plug-in must be wired so it scales the
  day level only and never feeds `bestVisitTimes`.
- **Summer is worse than H5** at every lead (+0.08 … +0.13). The win is a
  season-transition win; one autumn is a single observation of the regime it is
  good at. Serving it unconditionally would trade summer error for autumn error.
- **Asia gains nothing**, and d90 on all rides is significantly worse than naive.
- The run spans one year of history, so the model has no month/day-of-year
  features at all; from 2026-12-23 it can learn them, which is also when the
  cheapest improvement arrives.

So: register `driver_level` as a **shadow** surface in the forward archive
(PAR-831, the same UC3S mechanism PAR-834 uses for `h5`/`h5_routed`), gated to
the leads and segments where it wins — busy rides to d60, all rides to d10 —
with the best-time surface explicitly excluded, and flip it only once the online
A/B has seen a spring and a summer. The level gain is worth more inside the
models than as a separate composer: the two top-ranked covariates of §5 belong
in the TFT feature set.

**Two covariates to add to TFT `FUTR_EXOG` first** (nf-service `db.py`; it has
holidays, calendar and weather but no schedule information at all):

1. **Published opening hours relative to the recent same-weekday days** — length,
   closing hour, ratio and difference against the last four same weekdays. The
   largest single driver (±20–30 %, §3), absent from TFT and CatBoost alike.
   Feed it **as-of** (NULL where the schedule row is not yet published), never
   the final version: §4 measures only 84 % of ride-days published at d1 and
   43 % at d90, and the as-finally-published variant is worth only +0.01–0.03 R²
   more, i.e. the as-of version keeps nearly all of the signal without the leak.
2. **Change of school-break state against the reference window** (`d_school`,
   own and neighbour regions) — both models carry the *state*, neither the
   *change*, and it is the change that moves the level against a
   recent-history baseline (−20 % when a break ended, +19 % when one started,
   growing to ±32 % by d30).

Then, in order: first day after a closure of ≥ 2 days (−18 %), late close /
evening event (+14 %), and the archived weather forecast once PAR-831 has one.
Ticketed-event flags are not worth wiring yet — no effect is measurable over the
single Halloween season that is synced (0 % [−5, +6]).
