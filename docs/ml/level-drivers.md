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

> **Two separate bodies of evidence — do not mix them.**
>
> | | §1–§5 (above) | §6 (this section) |
> |---|---|---|
> | What | this issue's own **walk-forward driver analysis** | the **ml-bench harness** run |
> | Command | `python -m mlbench drivers` | `python -m mlbench run --model driver_level` + `report` |
> | Unit | ride-day / park-day log deviation from the naive level | 15-min slot, ride-day, park-day, park-month |
> | Tables | `results/20261009-par830-drivers/drivers/tables/` | `.../tables_2026-08-15_2026-10-07/`, `.../tables/` |
> | Numbers | R² vs naive, level MAE in minutes, UC4 Spearman over 385 park-months | slot MAE, D3–D9 decision metrics, hand-over |
>
> The R² figures (0.12–0.16 ride-day, 0.19–0.28 park-day), the level-MAE cuts
> (−4…−8 % ride, −7…−12 % park) and the UC4 Spearman 0.27 → 0.34 at d1 /
> 0.11 → 0.25 at d60 are **offline** numbers from §2. They are *not* harness
> numbers and must never be quoted as such. Everything in §6 is the harness.

Run `20261009-par830-drivers` (git `e0a4547a`, export `20261009`, 231 origins
2026-02-19 … 2026-10-07, 3 shards, 197 shard-minutes CPU, no GPU). Numbers below
are the **headline window** `--target-from 2026-08-15 --target-to 2026-10-07`:
every lead is scored on the same 54 target days.

> **Read §6.7 before quoting anything from §6.1.** The second committed pass —
> all 231 origins — **reverses the slot-MAE result**: over the full period
> `driver_level` has **no usable horizon on UC2/UC3 slot MAE at all**, on either
> segment, and is worse than the reference at every lead on all rides. The
> ranking surfaces (D4, D6, D7) and the D3 regression survive both passes; the
> slot-MAE win does not. §6.1–§6.5 are therefore a statement about **autumn and
> season-transition origins**, not about the model in general. The common-target
> window also does *not* remove the season confound the way it was meant to:
> at fixed target dates, lead `L`'s origins are `target − L`, so the lead axis
> and the origin season move together (§6.7, point 3).

Tables:
`ml-bench/results/20261009-par830-drivers/tables_2026-08-15_2026-10-07/`
(`summary_tables_2026-08-15_2026-10-07.md`, `cells.csv`, `handover.csv`,
`usable_horizon.csv`); full-window equivalents in `tables/` and `summary.md`
(§6.7).

**Provenance.** `git_sha` `e0a4547a`, `code_sha256` `915dc7843c0b0afe`, image
`sha256:7b5e872f614f`, `--reference` (the run refuses to start without a known
SHA and image id). `e0a4547a` has PAR-827's `ea9a8438`, `a1d20bf8`, `336b2b62`
and `689a7b84` as ancestors, and `mlbench/baselines.py`, `mlbench/runner.py` and
`mlbench/build.py` are **byte-identical to `ea9a8438`**. Those commits are the
**plug-in ↔ built-in coverage-parity fixes**: before them a plug-in forecast only
the origin-known window while the built-in baselines forecast the whole
published one, the plug-in grid could emit slot starts matching no truth slot,
and a slot at a projected closing time read as inside the window. This run has
them, so the cross-baseline comparison below is sound — **a run without them is
not comparable to it**. The `mlbench` sources of this branch still hash to that
same `code_sha256`, so rebasing onto the PAR-827 head changed no benchmark code
and the run needs no repeat.

The plug-in's baselines reproduce the PAR-827 `20261009-baselines-v2` run
**bit-identically** (`wt_med`, `clim`, `h5`, `lvlh5_tft`, `oracle_level`,
`oracle_shape`, `prod_served` agree to 1e-15 at every lead checked), so the
comparison below is against exactly the published baselines.

### 6.1 Hand-over — UC2/UC3 slot MAE (15-min slots, minutes)

**Read the per-baseline table first, not the "vs reference" one.** The critic of
PR #445 (finding B1) established that the harness's per-lead reference is an
**unpaired argmin** over `snaive7` / `wt_med` / `clim`, each measured on its own
coverage, and that at d7 and d60 it picks the candidate the harness's own paired
test calls worse — on gaps of 0.02–0.19 min against a park-cluster CI half-width
of ≈ 0.24. PAR-827 is re-reporting under a paired reference selection. So no
conclusion on this page rests on "beats / loses to the reference". The primary
evidence is the **paired margin against each named baseline individually**,
which does not move when the reference choice does:

Paired margin of `driver_level` against each named baseline — UC2/UC3 slot MAE,
**all rides** (minutes per slot, negative = `driver_level` better). Each column
is a difference of two paired differences through that lead's common reference;
every model's paired share of the reference's rows is ≥ 97 % except where noted,
so the pairing is on effectively the same slots.

| vs baseline | d1 | d3 | d7 | d10 | d14 | d21 | d30 | d45 | d60 | d90 |
|---|---|---|---|---|---|---|---|---|---|---|
| `snaive7` | −0.823 | −0.792 | — | — | — | — | — | — | — | — |
| `wt_med` | −0.362 | −0.413 | −0.351 | −0.431 | −0.323 | −0.359 | −0.328 | −0.220 | −0.136 | **+0.234** |
| `clim` | −0.655 | −0.655 | −0.435 | −0.390 | −0.114 | −0.104 | −0.146 | −0.105 | −0.086 | −0.062 |
| `h5` | −0.226 | −0.274 | −0.210 | −0.293 | −0.186 | −0.222 | −0.199 | −0.114 | −0.061 | **+0.293** |
| `lvlh5_tft` (TFT×H5) | −0.077 | −0.227 | −0.283 | −0.411 | −0.464 | −0.557 | −0.782 | −0.714 | gated | — |
| `persistence` | not scored — see below | | | | | | | | | |

Same, **ex-ante busy rides** (ride q90 ≥ 45 min over the 56 days before origin):

| vs baseline | d1 | d3 | d7 | d10 | d14 | d21 | d30 | d45 | d60 | d90 |
|---|---|---|---|---|---|---|---|---|---|---|
| `snaive7` | −1.919 | −1.858 | — | — | — | — | — | — | — | — |
| `wt_med` | −1.298 | −1.410 | −1.235 | −1.514 | −1.299 | −1.403 | −1.363 | −1.270 | −1.135 | −0.267 |
| `clim` | −1.836 | −1.803 | −1.307 | −1.313 | −0.754 | −0.734 | −0.821 | −0.928 | −1.164 | −0.923 |
| `h5` | −0.985 | −1.088 | −0.909 | −1.179 | −0.967 | −1.064 | −1.036 | −0.989 | −0.940 | −0.124 |
| `lvlh5_tft` (TFT×H5) | **+0.037** | −0.319 | −0.583 | −0.722 | −0.846 | −1.081 | −1.654 | −1.620 | gated | — |

Reading it:

- **In this window** `driver_level` has the lowest slot MAE of every competing
  model at every lead d1 … d60, on both segments — regardless of which naive is
  called the reference. That is the part of the *windowed* result that survives
  B1. It does **not** survive the full-period pass (§6.7): there it is worse
  than `h5` and `wt_med` at every lead on all rides.
- **Two exceptions, both stated plainly.** At **d90 on all rides** it is worse
  than `wt_med` (+0.234) and worse than `h5` (+0.293). At **d1 on busy rides**
  `lvlh5_tft` is 0.037 min better — a tie, not a win for either.
- `snaive7` only exists to d7 (BENCH-SPEC baseline 2) and `lvlh5_tft` is gated
  out at d60 (paired share 1.6 % all / 0.2 % busy — a coverage artefact, see
  below), so those cells are blank rather than zero.
- **`persistence` is not comparable here and is not claimed to be beaten.** It
  is scored on UC1 only (BENCH-SPEC baseline 1) and `driver_level` has no
  intraday origin, so no paired comparison exists. For the record, persistence
  is the *best* model inside 2 h (UC1 MAE 5.63 at +60 min, 7.58 at +120 min, vs
  `wt_med` 8.01 / 8.18) and decays fast beyond it (UC2-intraday 8.88 at 2–4 h,
  11.96 at 4–8 h, 16.35 at 8 h+ vs `wt_med` 8.06 / 8.43 / 8.43). Nothing in §6
  is evidence about the live window.

**Against the harness's selected reference, with both bootstrap units.**
BENCH-SPEC's Protocol says "bootstrap 95 % CIs over **park-days**"; the harness
decides wins on the stricter **park-cluster** bootstrap (all days of a park
resampled together) and PAR-827's critic (B5) showed the choice flips
conclusions. Both are in `cells.csv` (`diff_lo/hi` = park-day,
`diff_lo_park/hi_park` = park-cluster), so both are printed here.
`*` = that CI excludes 0 in the model's favour. All cells have 54 origin days —
none is LOW-N under the ≥ 30-origin-day gate (but see §6.6).

All rides, `driver_level` vs the selected reference:

| lead | ref | diff | park-cluster CI | park-day CI | verdict |
|---|---|---|---|---|---|
| d1 | `wt_med` | −0.362 | [−0.554, −0.188]\* | [−0.437, −0.287]\* | win, both |
| d3 | `wt_med` | −0.413 | [−0.615, −0.229]\* | [−0.488, −0.337]\* | win, both |
| d7 | `clim` | −0.435 | [−0.659, −0.217]\* | [−0.520, −0.344]\* | win, both |
| d10 | `clim` | −0.390 | [−0.613, −0.168]\* | [−0.474, −0.299]\* | win, both |
| d14 | `clim` | −0.114 | [−0.333, +0.130] | [−0.202, −0.027]\* | **units disagree** |
| d21 | `clim` | −0.104 | [−0.341, +0.167] | [−0.195, −0.011]\* | **units disagree** |
| d30 | `clim` | −0.146 | [−0.425, +0.146] | [−0.245, −0.059]\* | **units disagree** |
| d45 | `clim` | −0.105 | [−0.403, +0.215] | [−0.211, −0.003]\* | **units disagree** |
| d60 | `clim` | −0.086 | [−0.453, +0.251] | [−0.197, +0.021] | no win, both |
| d90 | `wt_med` | +0.234 | [+0.040, +0.408] | [+0.157, +0.313] | **loses, both** |

Busy rides, `driver_level` vs the selected reference:

| lead | ref | diff | park-cluster CI | park-day CI | verdict |
|---|---|---|---|---|---|
| d1 | `wt_med` | −1.298 | [−1.752, −0.880]\* | [−1.478, −1.122]\* | win, both |
| d3 | `wt_med` | −1.410 | [−1.892, −0.968]\* | [−1.589, −1.221]\* | win, both |
| d7 | `wt_med` | −1.235 | [−1.776, −0.751]\* | [−1.432, −1.050]\* | win, both |
| d10 | `clim` | −1.313 | [−1.804, −0.788]\* | [−1.504, −1.101]\* | win, both |
| d14 | `clim` | −0.754 | [−1.271, −0.150]\* | [−0.951, −0.547]\* | win, both |
| d21 | `clim` | −0.734 | [−1.303, −0.092]\* | [−0.962, −0.521]\* | win, both |
| d30 | `clim` | −0.821 | [−1.481, −0.100]\* | [−1.056, −0.599]\* | win, both |
| d45 | `clim` | −0.928 | [−1.727, −0.144]\* | [−1.191, −0.672]\* | win, both |
| d60 | `clim` | −1.164 | [−2.077, −0.272]\* | [−1.449, −0.881]\* | win, both |
| d90 | `wt_med` | −0.267 | [−0.736, +0.206] | [−0.471, −0.062]\* | **units disagree** |

The busy-ride result is the robust one: it wins under both units at every lead
to d60. The all-rides result is **unit-dependent from d14 to d45** — a reviewer
who uses BENCH-SPEC's literal park-day unit sees a win there, a reviewer who
uses the harness's park-cluster rule does not. Do not quote an all-rides
boundary without saying which unit produced it.

For context, the same two CIs for the baselines at d7 all rides: `h5` −0.225
[−0.451, +0.007] cluster / [−0.295, −0.155]\* park-day; `wt_med` −0.085
[−0.315, +0.158] / [−0.155, −0.010]\*. That is B1 and B5 in one cell: the
chosen reference (`clim`) is paired-worse than the rejected `wt_med`, and
whether `h5` "beats climatology at d7" depends on the bootstrap unit.

`lvlh5_tft` and `prod_served` at d45–d60 carry only 1.6 % of the reference's
paired slots (173 of 4 492 park-days; 0.2 % on busy) — below the 30 % coverage
gate. Their apparent 4.26 / 19.64 min are a coverage artefact and the harness
correctly keeps them out of the hand-over.

`driver_level` and `driver_level_x_h5` are numerically identical on every slot
metric: `predict` already returns the naive H5 curve scaled by exp(ŷ), which is
what the runner composes from `predict_daily`.

### 6.2 Usable horizon, under both bootstrap units

Usable horizon = largest lead up to which the model beats the per-lead reference
**contiguously** with the CI excluding 0, after the ≥ 30-origin-day and ≥ 30 %
coverage gates. The left number is the harness's park-cluster rule (what
`usable_horizon.csv` prints); the right is BENCH-SPEC's literal park-day unit,
recomputed from the same `cells.csv` columns.

| metric | `h5` | `lvlh5_tft` | `driver_level` |
|---|---|---|---|
| UC2/UC3 slot MAE, all rides | d6 / d10 | d2 / d5 | **d10 / d45** |
| UC2/UC3 slot MAE, busy rides | d7 / d7 | d10 / d10 | **d60 / d90** |
| D4 optimiser regret | d7 / d7 | — / d7 | **d14 / d14** |
| D4 dayPeak pairwise ordering | **d90 / d90** | d45 / d45 | d30 / d90 (d60 cell is LOW-N, 26–27 origin days) |
| UC3 dayPeak abs error | — / — | d2 / d3 | — / d5 |
| D5 first-hour MAE (opening-aligned) | d7 / d90 | d7 / d14 | d7 / d90 |
| D3 best-time hit rate (top-2) | **d45 / d45** | — / — | **— / —** |
| D3 best-time regret | **d45 / d90** | — / d0 | **— / —** |
| D6 crowd bucket (level only) | n/a | d14 (`lvl_tft`) | significant d7–d90, loses at d180 |

Two things to take from this table:

1. **In this window** `driver_level` extends the slot-forecast horizon on busy
   rides from d7 to d60 (d90 under the park-day unit) and the optimiser's from
   d7 to d14, under either unit. **Over the full period the slot-MAE horizon is
   empty on both segments** while the optimiser's grows to d30 and the dayPeak
   ordering to d90 (§6.7a) — so of these three, only the two *ranking* horizons
   are properties of the model rather than of the window.
2. **D3 is the only row where `driver_level` has no horizon at all under either
   unit** while `h5` reaches d45. That is the regression, not a borderline
   reading (§6.3).

The D5 row is a warning about the harness, not a result: `driver_level` does not
scale the first hour at all, so it is identical to `h5` there by construction,
and the d7 ↔ d90 swing between the two units is the same artefact the PAR-827
critic found. Nothing about the rope drop is claimed here.

### 6.3 What it makes WORSE — read before the wins

- **D3 best time (`bestVisitTimes`, ride overview) regresses, and the frontend
  ships that surface today.** Paired, and significant under **both** bootstrap
  units, against **both** `clim` and `h5`:

  | metric | lead | `driver_level` | `clim` (ref) | `h5` | vs ref, cluster CI | vs `h5` (paired margin) |
  |---|---|---|---|---|---|---|
  | hit rate (top-2) | d1 | 0.790 | 0.818 | 0.819 | −0.018 [−0.026, −0.009]\* | **−0.034** |
  | hit rate (top-2) | d7 | 0.786 | 0.820 | 0.819 | −0.026 [−0.034, −0.016]\* | **−0.038** |
  | hit rate (top-2) | d30 | 0.771 | 0.820 | 0.820 | −0.042 [−0.054, −0.031]\* | **−0.054** |
  | regret (min) | d1 | 3.369 | 3.145 | 2.932 | −0.015 [−0.263, +0.197] | **+0.44 min** |
  | regret (min) | d30 | 3.783 | 3.100 | 3.005 | +0.533 [+0.230, +0.820]\* | **+0.78 min** |

  Cause: the first hour is deliberately *not* scaled (BENCH-SPEC baseline 5), so
  scaling the rest of the day shifts the rope-drop ramp against it and the
  predicted minimum lands in the wrong place. `lvlh5_tft` has the same defect
  but about half the size (−0.015 … −0.022 hit rate). **Any level plug-in must
  therefore be wired so it cannot feed `bestVisitTimes`.** On its own this bounds
  the recommendation to shadow.

- **Summer is worse than `h5` at every lead.** `mae_by_season.csv` is
  `Σ|error| / Σn` per model, **each model on its own coverage — unpaired**, so
  read the direction, not the decimals (`driver_level` covers 82 % of operated
  ride-days against `h5`'s 92 %, §6.4a). Against `h5`: autumn −0.47 (d1) /
  −0.43 (d7) / −0.39 (d30), summer **+0.13 / +0.08 / +0.12**. The sign agrees
  with the paired offline analysis (§2: slightly worse than naive in summer at
  d1–d14), which is why the finding stands rather than being dismissed as an
  unpaired artefact: the gain is a **season-transition** gain, and in a stable
  summer regime the naive reference is already right and the model only adds
  variance. We have observed exactly **one autumn**.

- **d90 on all rides is significantly worse than naive** (+0.234 [+0.040,
  +0.408] cluster, [+0.157, +0.313] park-day) and worse than `h5` by +0.293.
  The model has no month/day-of-year features (deliberately, §1) and the
  training rows that far out are the fewest.

- **At d180 the level ranks days *worse* than naive**: D6 crowd bucket exact
  0.238 vs `lvl_naive4` 0.262, −0.024 [−0.050, +0.004] cluster /
  [−0.043, −0.005]\* park-day, 51 origin days. The far-horizon claim in §6.4a
  therefore stops at d90.

- **UC3 dayPeak absolute error** is no better than the reference at d1/d7
  (−0.274 [−0.563, +0.041], −0.356 [−0.720, +0.031] cluster) and worse at
  d60/d90.

- **Asia gains nothing.** EU d1 −0.54 [−0.88, −0.26]\*, d7 −0.36
  [−0.68, −0.08]\*; NA d1 −0.32 [−0.68, −0.05]\*, d30 −0.61 [−1.08, −0.19]\*;
  Asia d1 −0.02 [−0.35, +0.30], d30 +0.31 [−0.36, +0.95]. The holiday and
  school-break data that carry the signal are richest for EU.

### 6.4 Ranking the day (D6, D7) — what the level is really for

`lvl_driver_level` is the level alone, scored on the daily products. Both CIs
given; `*` = the stated CI excludes 0.

| metric | lead | `lvl_naive4` | `lvl_snaive7` | `lvl_tft` | `lvl_cbd` | **`lvl_driver_level`** | margin vs ref (cluster / park-day) | origin days |
|---|---|---|---|---|---|---|---|---|
| D6 crowd bucket exact | d1 | 0.358 | **0.434** (ref) | 0.415 | 0.232 | 0.410 | −0.014 [−0.035, +0.007] / [−0.031, +0.004] — **tie** | 54 |
| D6 crowd bucket exact | d7 | 0.322 (ref) | — | 0.355 | 0.206 | **0.373** | +0.052 [+0.031, +0.070]\* / [+0.037, +0.065]\* | 54 |
| D6 crowd bucket exact | d30 | 0.255 (ref) | — | 0.270 | 0.185 | **0.326** | +0.071 [+0.040, +0.100]\* / [+0.054, +0.088]\* | 54 |
| D6 crowd bucket exact | d90 | 0.280 (ref) | — | — | — | **0.338** | +0.058 [+0.027, +0.087]\* / [+0.039, +0.075]\* | 54 |
| D6 crowd bucket exact | d180 | **0.262** (ref) | — | — | — | 0.238 | −0.024 [−0.050, +0.004] / [−0.043, −0.005]\* — **loses on park-day** | 51 |
| D7 day comparison winner | d1–7 | 0.432 | **0.519** (ref) | 0.450 | 0.225 | 0.507 | −0.013 [−0.038, +0.012] / [−0.022, −0.004]\* — **units disagree** | 58 |
| D7 day comparison winner | d8–30 | 0.333 (ref) | — | 0.405 | 0.154 | **0.481** | +0.148 [+0.113, +0.178]\* / [+0.141, +0.154]\* | 75 |
| D7 day comparison winner | d31–90 | 0.332 (ref) | — | 0.369 | 0.134 | **0.449** | +0.117 [+0.083, +0.151]\* / [+0.111, +0.122]\* | 112 |

At **d1–d6 the bar is seasonal naive** (`lvl_snaive7`: D6 0.434, D7 0.519) and
`lvl_driver_level` does **not** clear it — same weekday, seven days ago is simply
a very good short-lead level. It is a statistical tie on D6 d1 and, on D7 d1–7,
a tie under the park-cluster rule but a **significant loss** under BENCH-SPEC's
park-day unit. From d7 the seasonal-naive reference expires and the drivers rank
days better than TFT and far better than CatBoost-daily out to **d90** under both
units — exactly the surfaces (crowd calendar, "Empfohlen" star, day comparison
A vs B) the review found weak everywhere. At **d180 the advantage is gone**.

### 6.4a Coverage and openness (D9)

- **It abstains more, and that cuts the open/closed error.** The review's D9
  finding was that the weekday median publishes a forecast for ~84 % of
  ride-days that never operated. `driver_level` only forecasts where it has a
  naive reference (≥ 2 of the last four same-weekday P90s), so it publishes for
  **63.7 %** of never-operated ride-days against `h5`'s **85.0 %** (d1,
  `d9_openness.csv`) — a 21-point improvement on a gap the review called real.
  The price is recall: it covers **81.9 %** of the ride-days that *did* operate
  against `h5`'s **91.8 %**. Serving it would need `h5` as the fallback on the
  ~10 % it drops.
- **The level reaches further than any other source.** `lvl_driver_level`
  carries a daily level for 96.5 % of ride-days at d90, 92.6 % at d120 and
  78.5 % at d180, matching `lvl_naive4`; `lvl_tft` stops after d60 and `lvl_cbd`
  earlier still. **Reach is not the same as skill**: it ranks days better than
  naive to d90 and no better at d180 (§6.3), so the useful far-horizon band is
  d60 … d90.

### 6.5 Share of the d1 level gap closed

With `h5` as the served reference and `oracle_level` (the true daily P90 on the
H5 shape) as the ceiling, re-read cell by cell from the PAR-827
**`20261009-baselines-v2`** headline window (`tables_2026-08-15_2026-10-07/cells.csv`,
UC3 MAE, region `all`, lead 1 — not the superseded `20261009-baselines`):

| segment | `h5` | `driver_level × h5` | `oracle_level` | gap closed |
|---|---|---|---|---|
| all rides | 7.980 | 7.756 | 5.338 | **8.5 %** |
| ex-ante busy rides | 15.096 | 14.189 | 9.160 | **15–17 %** |

`(h5 − driver_level×h5) / (h5 − oracle_level)`, computed two ways: unpaired on
the values, and paired through the common `wt_med` reference. All rides:
**8.48 % / 8.52 %** — they agree, so the figure is not a coverage artefact.
Busy rides: **15.3 % / 16.5 %** — a 1.2-point spread, which is why the busy
figure is quoted as a range and not as "16.5 %". `oracle_shape` at d1 is 6.958
(all rides), i.e. the level is the larger of the two gaps, as §2's premise says.

**A level built only from drivers known in advance closes roughly a twelfth of
the d1 level gap on all rides and a sixth on busy rides.** The remaining ~85 %
is what §2 measured as noise at this resolution plus what a better level model
(TFT with the covariates of §5, foundation models) would have to find.

**The same figure over the full period, which also reverses** (`tables/cells.csv`,
same cell):

| segment | `h5` | `driver_level × h5` | `oracle_level` | gap closed |
|---|---|---|---|---|
| all rides | 7.586 | 7.824 | 5.424 | **−11.0 %** (unpaired) / −12.5 % (paired) |
| ex-ante busy rides | 14.551 | 14.527 | 9.682 | **+0.5 %** (unpaired) / −1.3 % (paired) |

Over all 231 origins the drivers **widen** the d1 level gap on all rides by
roughly a ninth and do nothing measurable on busy rides. The 8.5 % / 15–17 %
above is an **autumn-window** figure, like the rest of §6.1–§6.5, and §6.7 is
where the model's year-round behaviour is.

### 6.6 LOW-N marking, and one harness defect that affects this page

BENCH-SPEC marks a lead LOW-N below **30 origin days**. In the headline window
**150 of 4 440 cells** have an origin-day count other than 54:

| cells | origin days | affected |
|---|---|---|
| 88 | **3** | **UC4 Spearman within park-month, every lead — LOW-N** |
| 3 | **26–27** | **D4 dayPeak pairwise ordering, d60 — LOW-N** |
| 3 | 37 | UC3 dayPeak Spearman, d60 |
| 18 | 48 | D3 (×3), D5 rope-drop, UC2/UC3 slot Spearman, UC3 dayPeak abs err — all d60 |
| 4 | 51 | D6 crowd bucket, d180 |
| 8 | 53 | the same metrics at d45 |
| 19 | 58 / 75 / 112 | D7 day comparison (its unit is the day *pair*) |

Only UC4 and D4-dayPeak-ordering-at-d60 fall below the gate; both are marked at
every use. **Harness defect, same as PR #445's B4:** the windowed pass's
`lead_availability.csv` is a verbatim copy of the full-period table (231, 230,
… 51 origin days) rather than the windowed counts — it is **not** the source of
the 54, which comes from `cells.csv`'s per-cell `n_origin_days`. Do not read
`lead_availability.csv` out of a windowed pass. (Fix belongs to PAR-827; it does
not change any number here.)

### 6.6a UC4 is not measurable in the headline window at all

The UC4 unit is the park-**month** (`report.py:342` sets
`u = {"park_id": park, "date": mon}`) and the 8-week headline window spans three
months, so **every** UC4 cell at every lead rests on **3 origin days** — one
tenth of the gate. `usable_horizon.csv` consequently reports `leads_tested = 0`
for all six level sources. That is an **empty result because nothing was
tested**, and it is explicitly *not* evidence that a naive baseline won. The
headline UC4 row reads **"not measurable in this window"**. §6.7b reports the
full-period UC4, which is larger but still LOW-N.

### 6.7 Full-period pass — the slot-MAE win does NOT survive it

The second committed pass scores **all 231 origins** (2026-02-19 … 2026-10-07,
`tables/`, `summary.md`; 1 356 s in one 2 CPU / 4 GB container). It **reverses
the slot-MAE conclusion of §6.1**, and this page says so rather than letting the
favourable pass stand alone.

`driver_level` vs the per-lead reference, UC2/UC3 slot MAE, region `all`
(positive = **worse** than the reference):

| lead | all rides, diff | cluster CI | park-day CI | busy, diff | cluster CI | park-day CI | nOD |
|---|---|---|---|---|---|---|---|
| d1 | **+0.206** | [+0.093, +0.327] | [+0.164, +0.246] | −0.112 | [−0.414, +0.167] | [−0.214, −0.018] | 230 |
| d3 | **+0.176** | [+0.056, +0.303] | [+0.133, +0.218] | −0.169 | [−0.485, +0.122] | [−0.274, −0.069] | 228 |
| d7 | **+0.270** | [+0.127, +0.422] | [+0.225, +0.313] | +0.032 | [−0.342, +0.396] | [−0.075, +0.140] | 224 |
| d14 | **+0.260** | [+0.109, +0.413] | [+0.216, +0.303] | −0.010 | [−0.411, +0.360] | [−0.121, +0.093] | 217 |
| d30 | **+0.280** | [+0.117, +0.442] | [+0.228, +0.330] | −0.023 | [−0.439, +0.382] | [−0.151, +0.100] | 201 |
| d60 | **+0.329** | [+0.056, +0.592] | [+0.243, +0.408] | −0.091 | [−0.769, +0.517] | [−0.290, +0.106] | 171 |
| d90 | **+0.839** | [+0.631, +1.042] | [+0.767, +0.911] | **+0.890** | [+0.389, +1.375] | [+0.719, +1.071] | 141 |

Paired margin vs each named baseline over the full period (negative = better):

| vs | segment | d1 | d3 | d7 | d10 | d14 | d30 | d45 | d60 | d90 |
|---|---|---|---|---|---|---|---|---|---|---|
| `h5` | all | +0.273 | +0.248 | +0.350 | +0.300 | +0.348 | +0.379 | +0.500 | +0.494 | +0.935 |
| `wt_med` | all | +0.206 | +0.176 | +0.270 | +0.215 | +0.260 | +0.280 | +0.396 | +0.401 | +0.839 |
| `clim` | all | −0.316 | −0.314 | −0.102 | −0.108 | +0.004 | +0.124 | +0.262 | +0.329 | +0.644 |
| `h5` | busy | +0.063 | +0.019 | +0.238 | +0.139 | +0.209 | +0.220 | +0.391 | +0.326 | +1.110 |
| `wt_med` | busy | −0.112 | −0.169 | +0.032 | −0.075 | −0.010 | −0.023 | +0.140 | +0.106 | +0.890 |
| `clim` | busy | −1.118 | −1.089 | −0.652 | −0.668 | −0.475 | −0.314 | −0.157 | −0.091 | +0.552 |

`tables/usable_horizon.csv` consequently gives `driver_level` **no usable
horizon at all** on UC3 slot MAE, on either segment, while `h5` keeps **d45**.

**Why the two passes disagree — and why the full-period pass is the more honest
one for a serving decision.**

1. **The model is an autumn / season-transition model.** `tables/mae_by_season.csv`
   (unpaired, each model on its own coverage — read direction) against `h5`:

   | lead | autumn | spring | summer | winter |
   |---|---|---|---|---|
   | d1 | **−0.467** | +0.424 | +0.320 | +1.150 |
   | d7 | **−0.433** | +0.525 | +0.347 | +2.372 |
   | d30 | **−0.392** | +0.666 | +0.433 | — |

   The headline window is **8 weeks of late summer and autumn**. The full period
   is February to October. Averaged over the year the autumn gain does not pay
   for spring, summer and especially winter.

2. **The winter cells are the plug-in's cold start.** The first origins are
   2026-02-19 onward, and the plug-in trains only on rows the runner has
   accumulated, so the earliest origins have almost no training history — §1
   records that the offline analysis deliberately **dropped February and March
   for having < 20 000 training rows**, a cut the harness plug-in does not make.
   `+2.372` at d7 in winter is that, not a seasonal effect of winter as such.

3. **In the common-target-window pass the lead axis is itself confounded with
   origin season.** With targets fixed to 2026-08-15 … 2026-10-07, lead `L`'s
   origins are `target − L`:

   | lead | origin span | season of the origins |
   |---|---|---|
   | d1 | 2026-08-14 … 2026-10-06 | autumn |
   | d10 | 2026-08-05 … 2026-09-27 | late summer → autumn |
   | d30 | 2026-07-16 … 2026-09-07 | summer → early autumn |
   | d60 | 2026-06-16 … 2026-08-08 | summer |
   | d90 | 2026-05-17 … 2026-07-09 | summer |

   So §6.1's own gradient — −0.36 at d1 decaying to **+0.23 at d90** — is partly
   the model getting worse at *summer origins*, not at *long leads*. The two
   passes tell one consistent story once this is seen: **`driver_level` beats the
   naive reference on 15-min slot MAE at autumn origins and loses at spring,
   summer and cold-start origins.** It is not "the best model at every lead".

### 6.7a What survives both passes

| finding | headline window | full period | survives? |
|---|---|---|---|
| **D3 best time regresses** (hit rate top-2 vs `clim`) | −0.018 (d1) / −0.026 (d7) / −0.042 (d30) | −0.021 / −0.028 / −0.038, all \* under both units; vs `h5` −0.035 / −0.040 / −0.047 | **yes, and it is larger** |
| D3 usable horizon | none (`h5` d45) | none (`h5` d60) | **yes** |
| **Summer / spring worse than `h5`** | +0.13 / +0.08 / +0.12 | +0.32…+0.43 summer, +0.42…+0.67 spring, +1.15…+2.37 winter | **yes, and much larger** |
| **D7 day comparison d8–30 / d31–90** | +0.148\* / +0.117\* | **+0.068 [+0.053, +0.082]\* / +0.094 [+0.075, +0.113]\*** | **yes, smaller** |
| **D6 crowd bucket d30 / d90** | +0.071\* / +0.058\* | **+0.032 [+0.013, +0.050]\* / +0.024 [+0.004, +0.043]\*** | **yes, smaller** |
| D6 crowd bucket d7 | +0.052\* both units | +0.010, cluster [−0.005, +0.023] **n.s.**, park-day [+0.002, +0.018]\* | **weakens to unit-dependent** |
| D6 d1 vs `lvl_snaive7` | −0.014, tie | **−0.020 [−0.035, −0.007]\* — significantly worse** | **turns into a loss** |
| D7 d1–7 vs `lvl_snaive7` | −0.013, cluster n.s. | −0.007, cluster n.s. / park-day [−0.012, −0.002]\* | no change (still ≤ naive) |
| D6 d180 | −0.024, park-day \* worse | −0.024, park-day \* worse | **yes** |
| D4 dayPeak pairwise ordering | d30 / d90 | **d90 / d90** | **yes, stronger** |
| D4 optimiser regret | d14 | **d30** | **yes, stronger** |
| **UC2/UC3 slot MAE win** | d10 all / d60 busy | **no horizon on either segment** | **NO — this is the reversal** |

So the *ranking* surfaces (D6, D7, D4) survive the full period and the *slot
MAE* win does not. That is coherent: the drivers improve the **relative ordering
of days**, which is what a ranking metric measures and what the crowd calendar
needs, while the **absolute minutes** only improve when the naive reference is
stale — i.e. at a season transition.

### 6.7b UC4 from the full period — still LOW-N, so point estimates only

The full period raises UC4 from 3 origin days to **9 (d1, d7) / 8 (d30) / 7
(d60) / 6 (d90) / 5 (d120) / 3 (d180)** — better, but still far below
BENCH-SPEC's 30, so `tables/usable_horizon.csv` *again* reports
`leads_tested = 0` for every level source. **That is still "not tested", not
"nothing won".** The point estimates, marked **LOW-N** and not to be used as a
hand-over decision:

| lead | ref | `lvl_naive4` | `lvl_snaive7` | `lvl_clim` | `lvl_tft` | `lvl_cbd` | **`lvl_driver_level`** | margin vs ref (cluster / park-day) | park-month origin days |
|---|---|---|---|---|---|---|---|---|---|
| d1 | `lvl_snaive7` 0.278 | 0.252 | 0.278 | 0.175 | 0.299 | 0.184 | **0.339** | +0.065 [+0.041, +0.088]\* / [+0.042, +0.086]\* | 9 — LOW-N |
| d7 | `lvl_naive4` 0.172 | 0.172 | — | 0.131 | 0.194 | 0.081 | **0.272** | +0.100 [+0.077, +0.122]\* / [+0.079, +0.121]\* | 9 — LOW-N |
| d30 | `lvl_clim` 0.150 | 0.145 | — | 0.150 | 0.105 | 0.011 | **0.257** | +0.109 [+0.072, +0.146]\* / [+0.077, +0.143]\* | 8 — LOW-N |
| d60 | `lvl_naive4` 0.164 | 0.164 | — | 0.162 | 0.139 | — | **0.249** | +0.086 [+0.052, +0.119]\* / [+0.059, +0.112]\* | 7 — LOW-N |
| d90 | `lvl_wt56` 0.164 | 0.150 | — | 0.154 | — | — | 0.187 | +0.029 [−0.011, +0.070] / [−0.010, +0.066] — n.s. | 6 — LOW-N |
| d120 | `lvl_naive4` 0.206 | 0.206 | — | 0.133 | — | — | 0.172 | **−0.029 [−0.052, −0.007]\* — worse** | 5 — LOW-N |
| d180 | `lvl_naive4` 0.328 | 0.328 | — | 0.250 | — | — | 0.328 | +0.013 [−0.019, +0.048] — n.s. | 3 — LOW-N |

Read as a **weak, consistent** signal in the same direction as the offline
analysis: on the park-month ranking `lvl_driver_level` is the best of the six
level sources at d1–d60 and the only one above every naive there, it is a tie at
d90/d180, and it is **worse than naive at d120**. Nothing here is a hand-over
decision; UC4 becomes decidable once ~30 park-months per lead exist, which needs
roughly another six months of history (≈ **2027-04**) for the short leads.

**The quotable UC4 numbers remain the offline ones** (§2: 0.27 → 0.34 at d1,
0.11 → 0.25 at d60 over 385 park-months) because they rest on 385 park-months
rather than 6–9 origin days.

### 6.8 Recommendation

**Shadow only — do not serve.** Three measured facts bound this, before any win
is weighed:

1. **The 15-min slot-MAE win does not survive the full backtest period.** Over
   all 231 origins `driver_level` has **no usable horizon on UC2/UC3 slot MAE at
   all**, on either segment, and is worse than the per-lead reference at **every**
   lead on all rides (+0.206 at d1 … +0.839 at d90, CIs excluding 0 under both
   bootstrap units) and worse than `h5` everywhere (+0.273 … +0.935). The
   headline window's win is an **autumn / season-transition** result (§6.7).
2. **It regresses `bestVisitTimes` (D3), a surface the frontend actually
   ships** — hit rate −0.018 … −0.042 against `clim` and −0.034 … −0.054 against
   `h5` in the headline window, −0.021 … −0.038 / −0.035 … −0.047 over the full
   period, significant under both units in both passes, and with **no usable
   horizon at all** while `h5` reaches d45/d60 (§6.3, §6.7a).
3. **It is worse than `h5` outside autumn**: +0.08 … +0.13 in summer in the
   headline window; over the full period +0.32 … +0.43 in summer, +0.42 … +0.67
   in spring and +1.15 … +2.37 in winter (partly the plug-in's cold start). We
   have observed exactly **one** autumn, which is the one regime it suits.

A surface regression on a shipped product, plus a slot-MAE advantage that exists
only in the window it was measured in, is more than enough to keep this out of
serving. **So: shadow, gated, never driving best time** — and shadow it to
*learn whether the autumn effect repeats*, which is the open question.

**What does survive both passes, and is the real result:** the *ranking*
surfaces.

- **D7 day comparison**: +0.068 [+0.053, +0.082]\* at d8–30 and +0.094
  [+0.075, +0.113]\* at d31–90 over the full period, under both units, on 223 /
  200 origin days — the best of every level source, with `lvl_tft` at +0.000 /
  +0.026 and `lvl_cbd` deeply negative.
- **D6 crowd bucket**: +0.032 [+0.013, +0.050]\* at d30 and +0.024
  [+0.004, +0.043]\* at d90 over the full period. At d7 it weakens to
  unit-dependent (+0.010) and at d1 it is **significantly worse** than seasonal
  naive (−0.020\*); at d180 it is worse than naive.
- **D4**: dayPeak pairwise ordering usable to **d90** and optimiser regret to
  **d30** over the full period — both *longer* than in the headline window and
  longer than any other model's.
- It needs **no GPU and no new service** (197 shard-minutes of CPU for 231
  origins) and it publishes 21 points fewer forecasts for ride-days that never
  operate (§6.4a).

So the defensible reading is: **the drivers improve the relative ordering of
days — which is what the crowd calendar, the "Empfohlen" star and the day
comparison consume — and they do not reliably improve the absolute minutes on a
15-min slot.** That is coherent with §2: a level model that explains 12–28 % of
the deviation moves a rank much further than it moves a minute.

Against it, beyond the three facts above: **Asia gains nothing**, the
far-horizon ranking advantage stops at d90 (worse at d120 and d180), and the
headline window's all-rides d14–d45 band is win-or-not depending on the
bootstrap unit. The run spans one year of history, so the model has no
month/day-of-year features at all; from **2026-12-23** it can learn them, which
is also the cheapest next improvement and the most likely cure for the
spring/summer deficit.

**Concretely:** register `driver_level` as a **shadow** surface in the forward
archive (PAR-831, the same UC3S mechanism PAR-834 uses for `h5` / `h5_routed`),
**scoped to the daily-ranking surfaces** (`lvl_driver_level` feeding D6/D7-style
comparisons at d8–d90) rather than to the slot forecast, with `bestVisitTimes`
**explicitly excluded** and the slot curve not served at all. Flip nothing until
the archive has covered a **spring and a summer** — those are the seasons where
the full-period pass says it loses, and they are exactly what the headline
window could not see.

**The larger pay-off is inside the models, not in a new composer.** Validate or
refute this expectation: the two top-ranked covariates of §5 are **absent from
TFT's `FUTR_EXOG`**, and the offline analysis says they are worth more than a
separate level composer is. Verified against `nf-service/db.py`: `FUTR_EXOG`
carries `is_holiday_primary`, `is_holiday_neighbor`, `is_school_holiday`,
`is_bridge_day`, `days_until/since_holiday`, day-of-week one-hots, `season_code`,
`is_peak_season` and daily weather — and **no schedule, opening-hours or
operating-window column of any kind** (`grep -n 'schedule\|opening\|operating'
nf-service/db.py` is empty). The school-break *state* is there; the *change*
against the reference window is not.

1. **Published opening hours relative to the recent same-weekday days** —
   length, closing hour, ratio and difference against the last four same
   weekdays. The largest single driver (**±20–30 %** on the daily level, §3),
   absent from TFT and CatBoost alike. Feed it **as-of** (NULL where the
   schedule row is not yet published), never the final version: §4 measures only
   84 % of ride-days published at d1 and 43 % at d90, and the as-finally-published
   variant is worth only +0.01–0.03 R² more — the as-of version keeps nearly all
   the signal without the leak.
2. **Change of school-break state against the reference window** (`d_school`,
   own and neighbour regions) — both models carry the *state*, neither the
   *change*, and it is the change that moves the level against a recent-history
   baseline (**−20 %** when a break ended, **+19 %** when one started at d7,
   growing to **−23.5 % / +23.8 %** by d30). The holiday *state* reaches +31.6 %
   at d30, but both models already carry the state — the change is the gap.

Then, in order: **first day after a closure** of ≥ 2 days (**−18 %**), **late
close / evening event** (**+14 %**), and the archived weather forecast once
PAR-831 has one (ORACLE upper bound +0.02–0.03 park R², §2).

**Ticketed events: not worth wiring, and the one significant cell is a
confound.** At d7 the effect is **+0.3 % [−5.2, +5.8], not significant**
(5 663 rows). At d30 it is **−15.2 % [−24.3, −8.0]**, i.e. *significantly
negative* — the opposite sign from a crowd-drawing event, on 5 576 rows from a
single synced Halloween season, and ticketed-event days usually also carry a
shorter regular schedule, which the opening-hours driver already captures. Read
it as collinearity with the schedule, not as a driver, and do not wire the flag
until a second season exists. `EXTRA_HOURS` behaves the same way (−7.2 % n.s. at
d7, −15.8 % at d30).

**What success looks like**, so the expectation is falsifiable. Measure it on
the **full period**, not the headline window — that is the lesson of §6.7:

- **D7 day comparison, d8–30 and d31–90**: `lvl_tft` is at +0.000 and +0.026 over
  `lvl_naive4` today, `lvl_driver_level` at **+0.068** and **+0.094**. Adding the
  two covariates should close most of that gap. This is the primary target,
  because it is the finding that survives both passes.
- **D6 crowd bucket, d30 and d90**: `lvl_tft` +0.003 / n.a., `lvl_driver_level`
  **+0.032** / **+0.024**.
- **Slot MAE**: expect *nothing* on the full period — `driver_level` itself is
  worse than `h5` there at every lead, so a covariate that only reproduces it
  would not help. If the covariates *do* move full-period slot MAE, that is
  better than the composer achieved and worth knowing.

If the ranking gaps do not close, the drivers' value is in the composer after
all, and the shadow surface — scoped to ranking, as recommended above — is its
long-term home.
