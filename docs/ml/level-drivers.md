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

See `ml-bench/results/20261009-par830-drivers/summary.md` (harness run with the
`driver_level` plug-in: H5 × driver level, as-of schedules, no weather).
