# NeuralForecast (Nixtla) TFT — Feasibility Evaluation & PoC Plan

> Status: **research / evaluation only** (2026-05-23). No production code changed.
> Companion to [ML Challenger: busy-day / holiday under-prediction](./busy-day-prediction-challenger.md),
> which names NeuralForecast TFT as the recommended offline challenger and punts the
> deep dive here. Read that doc first for the diagnosis; this doc decides whether the
> challenger is worth a real PoC, designs the smallest experiment, and gives a verified
> code skeleton.

---

## 0. TL;DR recommendation

**Yes — run a small, scoped, offline PoC.** Not because TFT is a better general
wait-time model (CatBoost wins near-term and on normal days), but because it directly
attacks the one failure mode CatBoost can't fix by feature engineering: **known-future
holiday/event covariates getting crowded out by rolling-average features for far-future
days.** TFT separates past and future covariates by construction, so a holiday flag for a
date 60 days out is a first-class structural input rather than a feature competing with
9 redundant level features.

**Do NOT plan to replace CatBoost.** The realistic target is a *hybrid*: keep CatBoost for
near-term hourly, evaluate TFT (or NHITS, see §1.3) only on the **far-future daily-peak**
surface where the problem lives. Success metric is **busy-day / holiday MAE on a held-out
busy window** (Pfingsten / Wintertraum), NOT overall MAE.

Smallest experiment that proves/disproves the benefit: §5.

---

## 1. Fit assessment — is TFT genuinely better *for this problem*?

### 1.1 Why the architecture matches the failure mode

The diagnosed problem (challenger doc lines 8-31): for future rows CatBoost's
autoregressive features (`avg_wait_last_1h`, `rolling_avg_7/28/90d`, `park_occupancy_pct`,
~78% of importance) degrade into same-DOW-4-week historical proxies, so the model predicts
"a typical recent `<weekday>`" and has **no lever** to lift a specific holiday. The
calendar/holiday features sit at ~0% importance because the rolling features already encode
the recent *level* and dominate the splits.

TFT addresses this structurally:

- **Separate encoders for past vs. known-future inputs.** `futr_exog_list` (holidays,
  weekend, season, weather forecast, scheduled-hours length) is fed through a dedicated
  future-covariate path and is available for *every* horizon step, including day 365. It is
  not in competition with autoregressive lags the way a tabular feature column is — the
  variable-selection network gates past and future inputs independently.
- **Variable Selection Networks (VSN)** give per-input importance weights (an analogue to
  CatBoost feature importance), so we can *measure* whether holiday lift is being learned —
  the exact metric the challenger doc wants to move off 0%.
- **Global model** across thousands of attraction series (one model, `unique_id` =
  attraction) — same cross-series pooling CatBoost gets from the `attractionId` categorical,
  so new/sparse attractions still benefit.
- **Native multi-horizon** with future covariates — designed for "forecast the next H steps
  given known-future inputs", which is precisely the calendar/daily surface.

### 1.2 Where TFT will *not* help (be honest)

- **Near-term hourly (next 1-6h):** CatBoost's rolling features (`avg_wait_last_1h`,
  `wait_time_velocity`) are an unambiguous win here. The current live signal *is* the best
  predictor of the next hour. TFT will, at best, match this and more likely lose, because its
  edge (future covariates) is irrelevant when "the queue is 45 min right now" is the dominant
  signal. **Keep CatBoost for the hourly surface.**
- **Normal (non-holiday, non-event) future days:** the recent-average proxy CatBoost uses is
  actually *correct* for a typical Tuesday. TFT's win is concentrated on the tails (busy
  holidays/events), which is exactly why overall MAE is the wrong success metric — it would
  hide the gain under a sea of well-predicted quiet days.
- **Data hunger / cold parks:** TFT needs enough history per series; parks with a few weeks of
  data or highly irregular sampling will be weaker than CatBoost's tabular pooling.

### 1.3 Model choice within the library

| Model | Future exog | Probabilistic | Notes for us |
|-------|-------------|---------------|--------------|
| **TFT** | ✅ first-class | ✅ via `DistributionLoss` | Best interpretability (VSN). Heaviest to train. **Primary candidate.** |
| **NHITS** | ✅ | ✅ | Much cheaper/faster, strong long-horizon. **Run as the cheap baseline alongside TFT** — if NHITS already gets the holiday lift, we may not need TFT's cost. |
| **TSMixer / TSMixerx** | ✅ (x variant) | ✅ | MLP-mixer, fast; `TSMixerx` supports exog. Secondary. |
| **DeepAR** | ✅ | ✅ (native) | Autoregressive/probabilistic; good but less interpretable than TFT for our "is holiday learned?" question. |

**Plan: train TFT and NHITS on the same panel.** NHITS is the cost hedge; TFT is the
interpretability + expected-best-quality bet.

---

## 2. PoC design on OUR data

### 2.1 Two models, two resolutions (the multi-horizon split)

A single TFT with `h=8760` (a year of hourly) is not realistic. The honest design is
**two separate NeuralForecast models**, matching the two API surfaces:

| Surface | Resolution | `freq` | `h` (horizon) | `input_size` (context) | Where the problem lives |
|---------|-----------|--------|---------------|------------------------|-------------------------|
| Hourly (near-term) | hourly peak | `'h'` | 24-48 | ~168 (1 week) | CatBoost already fine — TFT optional/secondary here |
| **Daily (far-future)** | **daily peak** | `'D'` | **90-365** | ~365 (1 year) | **The failure mode. PoC focuses here.** |

The PoC should target the **daily-peak** model first — it is the surface that reads
`very_low` for Pfingstsonntag.

### 2.2 Panel shape

One row per (attraction, time bucket). Long format:

| column | meaning | source |
|--------|---------|--------|
| `unique_id` | attraction UUID | `queue_data.attractionId` |
| `ds` | bucket timestamp (park-local, see note) | resampled `queue_data.timestamp` |
| `y` | target wait time | see target def below |

**Target (`y`) definition — daily-peak model:** per attraction per local day, the
**P90 (or daily max-of-hourly-P90)** of `waitTime` over `OPERATING`/`STANDBY` samples. This
matches the API's peak-vs-peak crowd contract (model-overview §"Alignment with API"), so the
output is directly comparable to the existing `crowdLevel` math. For the hourly model, `y` =
hourly P90 (or hourly mean) per attraction.

**Filtering (reuse existing rules):** `status='OPERATING'`, `queueType='STANDBY'`,
`waitTime >= 5`, and the schedule JOIN that excludes closed days
(`schedule_entries` park-level, see model-overview §"Training Pipeline"). Resample to the
chosen `freq`; drop closed days (don't fill them with 0 — that teaches the model spurious
zeros). NeuralForecast tolerates gaps better when series are contiguous, so prefer dropping
closed dates over zero-filling.

> **Timezone (CRITICAL — repo rule):** all `ds` buckets must be in **park-local time**, not
> UTC. The historical-occupancy bug (model-overview §"Historical Occupancy Profile") was a
> UTC-vs-local shift; the same trap applies to `day_of_week`/`hour`/holiday alignment here.
> Build `ds` via `qd.timestamp AT TIME ZONE p.timezone` (JOIN attractions→parks for the tz;
> `queue_data` has no `parkId`).

### 2.3 Covariate split — `futr` / `hist` / `stat`

The leakage rule: a variable goes in `futr_exog_list` **iff** its value is genuinely known
for future dates. Holidays/calendar/scheduled-hours are known; live wait dynamics are not.
**Never put a column in both `hist_exog_list` and `futr_exog_list`** (the docs' leakage trap).

**`futr_exog_list` (known-future — the whole point of this experiment):**
- `is_holiday_primary`, `is_school_holiday`, `is_bridge_day` (local region)
- `is_holiday_neighbor_1/2/3` (NL/BE source-market holidays — see challenger doc line 95-101;
  these are among PHL's biggest drivers and must be future covariates)
- `holiday_count_total`, `days_until_holiday`, `days_since_holiday`
- `is_weekend`, `day_of_week` (cyclic sin/cos), `month`/`day_of_year` (cyclic), `season`,
  `is_peak_season`
- `scheduled_hours_length` (extended hours correlate with busy days — challenger doc line 87,
  currently NOT a feature; cheap to add and genuinely known-future)
- weather **forecast** fields (temperature, precipitation) — known-future for the forecast
  window; honest only out to ~14 days, so for daily `h=365` either restrict weather to the
  near horizon or accept it is climatology beyond the forecast window.
- (later) per-park event-window flag (Wintertraum/Halloween) — the highest-value missing
  signal (challenger doc line 81-86); needs an event-calendar source, so it is a follow-up,
  not PoC scope.

**`hist_exog_list` (past-only — TFT may use these for the encoder but they are NOT projected
forward):**
- realized `park_occupancy_pct`, realized weather. These help the encoder understand the
  recent regime without leaking into the future path.

**`stat_exog_list` (static per attraction):**
- `parkId`, region/country code, ride attributes (e.g. ride category once populated). Static
  covariates let the global model condition per-attraction without per-series retraining.

### 2.4 Train / val / test split

- **Train:** all attractions in scope, history up to a cutoff *before* the held-out busy
  window.
- **Validation:** chronological tail before the test window (NeuralForecast cross-validation
  via `nf.cross_validation(...)` with `n_windows`/`step_size` for tuning).
- **Test (the decisive one):** a **known busy window held out entirely** — e.g. **Pfingsten
  2025** and/or **Wintertraum 2024** — predicted as a true future forecast (the model never
  saw those dates). This is what proves holiday lift.

### 2.5 Loss & uncertainty

The API contract relies on uncertainty intervals (CatBoost VirtEnsembles → `confidence`,
bounds). TFT's parity is **`DistributionLoss(distribution='StudentT', level=[80, 90])`** (or
`MQLoss(quantiles=[...])`). This yields prediction + lower/upper bounds natively, mapping
cleanly onto the existing response shape (§4.2). StudentT is a good default for heavy-tailed
wait times; `level=[80,90]` mirrors the bands the frontend already consumes.

---

## 3. Runnable code skeleton (verified against current Nixtla docs)

> Verified 2026-05-23 against the NeuralForecast docs
> (`nixtlaverse.nixtla.io/neuralforecast/.../exogenous_variables`,
> `.../models.tft`) and PyPI (`neuralforecast` **3.1.8**, requires **Python >=3.10**).
> API verified: `futr_exog_list`/`hist_exog_list`/`stat_exog_list` on the model;
> `NeuralForecast(models=[...], freq=...)`; `nf.fit(df=, static_df=)`;
> `nf.predict(futr_df=)`; `futr_df` must have **exactly `h` rows per `unique_id`** with
> all `futr_exog_list` columns. `scaler_type` required when using exogenous variables.

```python
"""
Offline PoC: NeuralForecast TFT (+ NHITS baseline) for far-future daily-peak
wait-time forecasting with holidays/calendar as KNOWN-FUTURE covariates.

OFFLINE ONLY. Reads a CSV/parquet export of queue_data (no live DB in PoC).
Does NOT touch the production ml-service.
Requires Python >=3.10 and `pip install neuralforecast` (3.1.8 at time of writing).
"""

import pandas as pd
from neuralforecast import NeuralForecast
from neuralforecast.models import TFT, NHITS
from neuralforecast.losses.pytorch import DistributionLoss

# ----------------------------------------------------------------------------
# 1. PANEL  (built from an offline export; see §2.2 for SQL/resampling rules)
#    df: long format, park-local `ds`, daily-peak `y`.
#    Required base columns: unique_id, ds, y  +  all exog columns below.
# ----------------------------------------------------------------------------
# df = pd.read_parquet("phl_daily_peak_panel.parquet")
# df["ds"] = pd.to_datetime(df["ds"])   # park-local daily timestamps

FUTR_EXOG = [
    # calendar / holidays — KNOWN for future dates (the experiment's core)
    "is_holiday_primary", "is_school_holiday", "is_bridge_day",
    "is_holiday_neighbor_1", "is_holiday_neighbor_2", "is_holiday_neighbor_3",
    "holiday_count_total", "days_until_holiday", "days_since_holiday",
    "is_weekend", "dow_sin", "dow_cos", "doy_sin", "doy_cos",
    "season_code", "is_peak_season", "scheduled_hours_length",
    # weather forecast: honest only ~14d out; restrict horizon or treat as climatology
    "temp_forecast", "precip_forecast",
]
HIST_EXOG = [                     # past-only — NOT projected forward (no leakage)
    "park_occupancy_pct_realized", "temp_realized", "precip_realized",
]
STAT_EXOG = ["park_code", "region_code"]   # static per attraction

# static_df: one row per unique_id with the stat_exog columns
# static_df = df.groupby("unique_id")[STAT_EXOG].first().reset_index()

H = 90            # daily-peak horizon (extend to 365 once stable)
INPUT_SIZE = 365  # ~1 year of daily context

common = dict(
    h=H,
    input_size=INPUT_SIZE,
    futr_exog_list=FUTR_EXOG,
    hist_exog_list=HIST_EXOG,
    stat_exog_list=STAT_EXOG,
    scaler_type="robust",                       # REQUIRED with exog (docs)
    loss=DistributionLoss(distribution="StudentT", level=[80, 90]),  # uncertainty parity
    max_steps=500,                              # small for PoC; tune up later
)

models = [
    TFT(**common, hidden_size=64, learning_rate=1e-3),
    NHITS(**common, learning_rate=1e-3),        # cheap baseline; same panel
]

nf = NeuralForecast(models=models, freq="D")

# ----------------------------------------------------------------------------
# 2. FIT  (cutoff BEFORE the held-out busy window; static_df carries stat exog)
# ----------------------------------------------------------------------------
# train_df = df[df["ds"] < HOLDOUT_START]
# nf.fit(df=train_df, static_df=static_df)

# ----------------------------------------------------------------------------
# 3. FUTURE COVARIATES  — futr_df must have EXACTLY H rows per unique_id and
#    ALL FUTR_EXOG columns filled for the forecast dates (holidays computed
#    from the `holidays` table; weekend/season/dow derived from `ds`).
# ----------------------------------------------------------------------------
# futr_df = build_future_covariates(unique_ids, HOLDOUT_START, H)  # holidays etc.
# Y_hat = nf.predict(futr_df=futr_df)
#   -> columns: unique_id, ds, TFT, TFT-lo-90, TFT-hi-90, NHITS, NHITS-lo-90, ...
#   (point = median of the distribution; -lo-/-hi- = the requested levels)
#   NOTE: exact suffixes depend on the loss config — inspect Y_hat.columns post-fit
#   (DistributionLoss naming can differ from the conventional shape shown here).

# ----------------------------------------------------------------------------
# 4. EVALUATE on the busy window only (success metric = busy-day MAE, §5)
# ----------------------------------------------------------------------------
# merge Y_hat with the held-out actuals on (unique_id, ds), restrict to the
# Pfingsten/Wintertraum dates, compute MAE & bias per model, compare to
# CatBoost replayed on the SAME dates.
```

> `nf.cross_validation(df=..., static_df=..., n_windows=..., step_size=...)` gives an
> apples-to-apples backtest if you prefer rolling-origin evaluation over a single holdout.

---

## 4. Cost / effort, serving integration, risks

### 4.1 Dependencies, compute, training time

- **New deps:** `neuralforecast` 3.1.8 pulls in **PyTorch + PyTorch Lightning** — a large
  dependency tree, far heavier than the current pure CatBoost/numpy/pandas stack
  (requirements.txt). This is the single biggest cost.
- **Python version:** neuralforecast requires **Python >=3.10**. The PoC sandbox here only
  has Python 3.9.6, so the synthetic API smoke-test was **not run** (it was optional). A real
  PoC needs a 3.10+ venv. The production ml-service Dockerfile Python version must be checked
  before any integration (likely a base-image bump).
- **GPU:** TFT trains *much* faster on GPU. CPU training is feasible for the small PoC
  (one park, ~10-15 series, daily resolution) but a full multi-park hourly model would
  realistically want a GPU. NHITS is far cheaper and may run fine on CPU.
- **Training time:** PoC scale = minutes-to-low-hours on CPU. Full global hourly TFT =
  hours, GPU-preferred. Versus CatBoost's current ~minutes on CPU.

### 4.2 Serving integration — must coexist with CatBoost & match output shape

The current contract (`main.py` `PredictionResponse`): `predictedWaitTime` (int),
`confidence` (float), `crowdLevel`, `baseline`, `trend`, `modelVersion`. Mapping from TFT:

| Response field | From TFT output |
|----------------|-----------------|
| `predictedWaitTime` | point forecast (median of `DistributionLoss`), rounded to nearest 5 (existing rule) |
| `confidence` | derived from interval width: `lo-90`/`hi-90` → same shape as VirtEnsembles `mean ± std` |
| lower/upper bounds | `TFT-lo-90` / `TFT-hi-90` directly |
| `crowdLevel` | unchanged — computed downstream from `predictedWaitTime` vs the typical-day-peak baseline (P50 fallback). Note `predicted_peak` is the **median** of the forecast distribution of a daily-P90 target, i.e. E[daily-P90] — not the P90 of the forecast distribution. |
| `modelVersion` | new TFT version string; **must register in `ml_models`** like CatBoost |

**Integration shape:** the cleanest path keeps CatBoost as the live serving model and runs
TFT **offline-only** for the PoC (no serving). If the PoC wins, the production design is a
**router**: CatBoost for hourly + near-term daily, TFT for far-future daily-peak. TFT's
batch `nf.predict` is a poor fit for the current per-request `/predict` (single timestamp)
API — it would need a **precompute-and-cache** job (predict the daily horizon nightly, store
in Redis/DB), which is actually a clean fit for the calendar surface (already cached). This
is meaningful new infra, not a drop-in.

### 4.3 Risks

- **Big dependency surface** (PyTorch) → larger images, slower cold starts, more CVE/maint
  overhead. Quantify before committing.
- **Two-model maintenance** (CatBoost + TFT) → more retraining pipelines, version tracking,
  monitoring.
- **Reproducibility / nondeterminism** of neural training vs. CatBoost's stability.
- **Cold-start parks** weaker than CatBoost tabular pooling. Note that with `input_size=365`,
  attractions with <1 year of history are filtered out by NeuralForecast during `fit` — the
  global pooling helps the rest, but very new series get no daily-model coverage (CatBoost
  still serves them).
- **Weather-forecast covariate honesty** beyond ~14 days (handle as climatology or restrict).
- **Overhype risk:** if NHITS already captures the lift, TFT's cost may be unjustified — hence
  running both.

---

## 5. Recommendation & smallest decisive experiment

**Recommendation: run the offline PoC — scoped tight.** It is real R&D (new deps, a 3.10+
env, an export+panel pipeline), not a one-afternoon task, but it is the only way to prove the
architectural hypothesis the challenger doc raised, and the failure mode (far-future holiday
under-prediction) is genuinely costing user-visible accuracy (Pfingstsonntag → `very_low`).

**Smallest experiment that proves/disproves holiday lift:**

1. **Scope:** one park where the problem is diagnosed — **Phantasialand (PHL)** — ~10-15
   attractions (headliners incl. Taron), **daily-peak** resolution, ~2 years of history.
2. **Hold out** a known busy window entirely: **Pfingsten 2025** (and optionally
   **Wintertraum 2024**). Train on everything before it.
3. **Train TFT + NHITS** with the §2.3 covariate split (holidays as `futr_exog`).
4. **Replay CatBoost** (champion `v20260523_0600`) on the *same* held-out dates.
5. **Success metric — busy-day / holiday MAE & bias on the held-out window only** (NOT
   overall MAE). Secondary: does TFT's VSN assign non-trivial weight to the holiday covariates
   (vs CatBoost's ~0%)? Sanity: do named busy days predict high and quiet weekdays low?
6. **Decision rule:** if TFT (or NHITS) cuts busy-day MAE / bias materially vs CatBoost on the
   holdout **and** the holiday covariates carry real weight → proceed to a productionized
   far-future daily model behind a router + nightly precompute. If not → stay on CatBoost and
   pursue the in-place levers (NaN-dropout full level-blanking + sample weighting,
   per-park event-window feature) from the challenger doc.

This isolates the one thing that matters (can a future-covariate-aware model lift a specific
holiday CatBoost can't?) at the smallest scale that can answer it, with a metric that won't be
washed out by well-predicted quiet days.

---

## Appendix — sources verified 2026-05-23

- NeuralForecast docs — Exogenous Variables capability page (panel shape, `futr_df` =
  exactly `h` rows/series, `scaler_type` required with exog).
- NeuralForecast docs — TFT model page (`futr_exog_list`/`hist_exog_list`/`stat_exog_list`,
  `h`, `input_size`, `loss=DistributionLoss(...)`, `max_steps`).
- PyPI `neuralforecast` — latest **3.1.8**, `requires_python >=3.10`.
- Repo: `docs/ml/busy-day-prediction-challenger.md`, `docs/ml/model-overview.md`,
  `docs/ml/prediction-quality-issues.md`, `ml-service/main.py`, `ml-service/model.py`,
  `ml-service/requirements.txt`.

---

## FINAL DECISION (2026-05-24) — productionized split

Decided + wired after the headliner OOS backtest (BASE 2026-05-10, 14-day matured
holdout, ~201 headliners / ~1300 attraction-days vs realised daily P90 + CatBoost's
genuine forward daily preds).

### Division of labour
> **UPDATE 2026-06-10**: horizon extended **30 → 45 days** after the scheduled re-eval —
> headliner history matured to median 168 operating-day points and the h=45 backtest
> showed TFT at lead 31-45 still beats CatBoost at lead 1 on every segment (h=60 also
> passed; deferred to ~2026-08). Details: [clean-comparison doc §6.5]
> (./tft-vs-catboost-clean-comparison.md). The text below describes the original gate.

- **TFT → near-term daily (days 1–45, was 1–30), headliners** — serves the calendar's
  predicted crowd levels. It beats CatBoost ~2× on busy days (the failure mode that
  matters).
- **CatBoost → far daily (46–365) + intraday 15-min slots** — the long horizon (TFT
  can't reach it from ~5 months of history) and the intraday regime (lag/occupancy
  features dominate, no proxy handicap, already ~4.2 min MAE).

### Backtest evidence (studentt, all covariates)
| | TFT h=14 | TFT h=30 | CatBoost |
|---|---|---|---|
| ALL MAE / bias | 11.1 / −0.9 | **11.2 / +1.5** | 16.3 / −12.7 |
| busy≥40 MAE / bias | 16.0 / −7.6 | **15.1 / −5.3** | 29.7 / −29.1 |
| busy≥70 MAE / bias | 22.9 / −11.8 | **22.1 / −10.1** | 45.4 / −44.9 |

h=30 ≈ h=14 on accuracy (gate passed), so 30 chosen for a month of calendar coverage.

### Why h=30 (not longer)
Per-headliner daily history is short + gappy (median 72 operating-day points, p25=36;
input_size 90 > median). A window needs ~h real horizon points → 79% of series train
at h=30 vs 93% at h=14. A yearly horizon is impossible from ~5 months of history (TFT
is a sequence model — it must have *observed* the cycle). The ceiling rises as history
accumulates → **re-evaluate every few weeks** (next ~2026-06-14): re-run this backtest,
check whether more seasonal coverage supports a longer horizon, recalibrate the split.

### Rejected levers (measured, not assumed)
- **Weather + holiday-distance + day-of-week one-hot futr_exog**: no lift (ALL MAE
  11.1→11.1). The busy spikes are promo/event-driven and that signal is in NO feature
  (schedule has no event type). Kept in (neutral, cheap) but not the win.
- **Quantile loss (CatBoost-q0.8 analog)**: rejected. TFT's busy bias (~−8) is far
  milder than CatBoost's (−29), so a high quantile over-steers — lifts busy but
  over-inflates the dominant quiet bucket → ALL MAE rises monotonically (studentt 11.1
  → q0.7 13.0 → q0.8 15.6 → q0.9 24.9) AND would regress the calendar typical-day-peak
  calibration. **Loss = studentt.**
- **The single biggest real lever is data, not modelling**: capture structured
  promo/event/special-opening data — both models miss exactly the days that signal
  drives.

### Wiring (serving only)
- `MLService.getTftDailyPredictions(parkId, days=30)` — freshest `tft_forecasts` per
  (headliner, day), 3-day staleness guard (stalled nf-service → CatBoost fallback),
  cached per park-day.
- **Write side (PAR-814)**: `forecast_date` is the day the run that made the forecast STARTED
  (UTC), fixed by `train_runner` and recorded in `nf_training_status.json`; never the day
  a write happens. A night whose run times out or dies writes nothing, and the reader
  above falls back to the previous `forecast_date`.
- `MLService.getServingDailyPredictions(parkId, tftDays=30)` — TFT overrides CatBoost
  for covered ≤30d (attraction,day); CatBoost for the long tail + uncovered rows.
- Consumed by `CalendarService` (predicted crowd levels) and the yearly endpoint
  (`getParkPredictionsYearly`) → calendar + yearly share one source.
- The **prediction-generator writer** keeps `getParkPredictions("daily")` (pure
  CatBoost) so `wait_time_predictions` + the TFT-vs-CatBoost scoreboard stay fair.

### Known caveats (accepted, not masked)
1. **Day 30→31 scale step**: TFT target = daily P90 (bias −0.9); CatBoost daily =
   `DAILY_PEAK_HOURS=12,14,16` collapsed-to-max (bias −12.7, under-reads the peak). So
   days 1–30 read slightly higher than 31+. Not wrong (TFT is the accurate one); the
   transition is just visible. Masking it would only hide that CatBoost-far under-reads.
2. **Pre-existing formula inconsistency**: the calendar uses AVG-of-headliner-waits ÷
   typical-day-peak (the typical-day-peak refactor); the yearly endpoint
   (`aggregateDailyPredictions`) still uses P90-of-headliner-waits ÷ P50. Separate issue,
   not addressed by this wiring.
3. **Dropouts cannot be dropped**: CatBoost still serves far-daily (31–365), and the
   occupancy/rolling dropouts exist for exactly that far-future regime. An intraday-only
   CatBoost (no dropouts) is only possible if a separate seasonal baseline takes over
   far-daily — a larger future refactor, not done here.
</content>
</invoke>

## Calendar-day spacing, partial today, heartbeats (PAR-1033, 2026-10-24)

`nf-service/db.py` builds the daily panel from operating days only, while the model runs
with `freq="D"`. NeuralForecast reads rows by position, so `input_size=90` meant the last
90 *operating* days and step k the k-th operating day after the last one. For a park with
rest days or a closed season the spacing in training and serving was not calendar days,
and `build_future_frame` started every series at its own `last ds + 1` — a ride last seen
20 days ago got a frame that began in the past and reached only 40 days ahead.

**Unconditional (default build):**
- The panel excludes park-local today: `DATE(ts AT TIME ZONE tz) < DATE(NOW() AT TIME ZONE tz)`.
  Training runs 03:00 UTC, late morning in Tokyo and Shanghai, so a partial morning P90
  was the last context value and a training target. A wrong value, not a modelling choice.
- Heartbeat rows are excluded (`NOT COALESCE(qd.is_heartbeat, false)`, PAR-1039). They copy
  the last status and wait with a fresh timestamp; the forward archive's truth already drops
  them (`truthRowsSql`, PAR-1024), so the TFT target now matches what it is scored against.
  Also in `backtest_headliners.py` and `backtest_horizon.py`.
- `is_peak_season` uses the park's latitude: south of 15°S the months are the same rule six
  months on (12, 1, 2, 6). The tropics (Jakarta −6°, Singapore +1°) have no summer to shift
  and keep the months they had, as does a park without a latitude; their seasonality is
  carried by the school-holiday flags.

**Behind `NF_CALENDAR_GRID` (default `false` = previous behaviour):** it changes the
training data shape, so it is shadowed before it serves.
- `to_calendar_grid`: every day from a series' first observation to park-local yesterday;
  absent days are `y=0` with `available_mask=0` (NeuralForecast 3.1.8 keeps them out of the
  scaler, the loss and the window sampler). `y` is 0 rather than NaN because TFT's forward
  pass never reads the mask — it still READS those zeros as context. So the grid also adds
  `is_observed` (1 = observed, 0 = masked) as a `hist_exog`: the encoder can tell a closed
  day from a quiet one even past the schedule's publishing horizon, where `sched_closed` is 0.
- Every series therefore ends on park-local yesterday, so every future frame starts
  park-local today (the day the reader serves first: `target_date >= today`).
- A series last observed more than `NF_MAX_STALE_DAYS` (90 = `NF_INPUT_SIZE`) days ago is not
  forecast: past that its whole context window is masked. Its last TFT forecast keeps serving
  for up to 3 more days (the reader takes `forecast_date >= today - 3`, `src/ml/ml.service.ts`)
  before CatBoost takes over — no loss, since on the old panel its frame lay mostly in the past.
- Four more futr_exog columns (`FUTR_EXOG_GRID`): `sched_open` / `sched_closed` from the
  published park schedule (both 0 = unknown, never closed) and the distance to the nearest
  school holiday.

**Known limit of the schedule flags:** in history almost every day of a scheduled park has
an entry, in the future only the operator's publishing horizon does. Beyond it both flags are
0, a state the model rarely saw in training. Measure before trusting the far leads.

**Left as options, not changed:**
- The target is the P90 over *rows*; `queue_data` is a change log, so a stable afternoon
  weighs less than a busy morning. A time-weighted P90 like `rideQ90` in
  `src/parks/services/forecast-archive.service.ts` would match the forward archive's truth.
- Chunking by 10 parks in UUID order (`forecast.py`) mixes unrelated parks per model;
  chunking by region or timezone would give each chunk's TFT a coherent calendar.
- `backtest_horizon.py` feeds the realised weather into the future frame (oracle), so its
  MAE is optimistic against serving, which gets the forecast.

**Measurement plan before switching the flag on:** compare `cal_busier` / `cal_far` and
`leadTimeMae` with the flag off and on, overall, for parks with weekly rest days and for the
Asian parks (Tokyo, Shanghai), where partial-today and spacing hit hardest. `tft_forecasts`
cannot hold both variants side by side — its key is `(attraction_id, target_date,
forecast_date)`, `model_version` is not part of it — so a same-night shadow needs either a
shadow table or a run against a copy of the database; switching the flag for a fortnight
and comparing against the fortnight before is cheaper but confounded by the season.

### The grid as a nightly shadow (PAR-1041)

The measurement plan above is built: `tft_forecasts_shadow` is the shadow table.

- **nf-service.** With `NF_GRID_SHADOW` (default `true`) `train_runner.py` trains the grid
  variant a second time after the served run (`run_grid_shadow`): grid forced on through
  `forecast.calendar_grid(True)` whatever `NF_CALENDAR_GRID` says, model version
  `<served>-grid`, written under the served run's `forecast_date`. Skipped when
  `NF_CALENDAR_GRID` is already on (the served run is the grid then).
- **Sequencing.** Same process, after the served forecast is persisted, never concurrently.
  The `is_training` lock stays on through it (`phase: "grid_shadow"` in the status), so no
  second `/train` and no Chronos-2 call shares the GPU, and the NestJS `train-nf` job waits
  for both. A shadow failure (OOM included) is reported in `info.shadow.error` and logged by
  the job; it never fails the night.
- **Cost.** GPU time roughly doubles (~11 min served + the grid run, which reads more rows per
  series: closed days are masked rows now). Peak memory of one fit is unchanged: the same
  `NF_WINDOWS_BATCH_SIZE` / `NF_BATCH_SIZE` / park chunks, and the two fits never overlap.
  Both stay well inside the 90-min job and 120-min runner deadlines, and 03:00 UTC leaves
  three hours before the CatBoost training at 06:00. Kill switch: `NF_GRID_SHADOW=false`.
- **Table.** `CREATE TABLE IF NOT EXISTS tft_forecasts_shadow (LIKE tft_forecasts INCLUDING
  ALL)` in `nf-service/db.py` — same columns and key, cannot drift. Plain table, no FK,
  no retention, like `tft_forecasts`; it grows by about one served night's rows per night.
  Registered in the merge (`ATTRACTION_DEPENDENCIES`) and in the E2E schema.
- **Comparison.** The forward archive reads it at scoring time as model `tft_grid` on the
  calendar questions `cal_rank`, `cal_busier` and `cal_far` — see
  [forward-archive.md §9.1](forward-archive.md#91-generalised-paired-scoring).

### The PAR-830 drivers in the grid shadow (PAR-1057)

The shadow — never the served model — also gets the level drivers that
[level-drivers.md §5](level-drivers.md#5-ranked-covariates-worth-adding-to-tft--catboost)
ranks highest, and a different target. It is a **third nightly run, its own model**: after
the served run and the grid-only shadow, `run_grid_shadow(..., drivers=True)` trains it into
`tft_forecasts_shadow_drivers` (`LIKE tft_forecasts`, model version `<served>-grid-drivers`),
and the forward archive pairs it as `tft_grid_drivers` beside PAR-1041's `tft_grid`. One
comparison would otherwise mix three changes (grid, drivers, time-weighted target — and that
target is closer to the archive's `rideQ90` truth than the served P90-over-rows one), so the
grid-only shadow keeps running and both are read separately against the served TFT.
`NF_SHADOW_DRIVERS` (default `true`) switches only this run, `NF_GRID_SHADOW` only the
grid-only one; it still runs when `NF_CALENDAR_GRID` serves the grid (it differs from that).
Cost: one more fit, the night's `train-nf` job ~40–50 min instead of ~25, inside the 90-min
poll deadline. `forecast.calendar_grid(True, drivers=True)` is the only way in, `db.DRIVERS`
is not an env setting, and `futr_exog()` returns `DRIVER_EXOG` only with the grid AND the flag.
Memory: the target is read as raw change-log rows (`fetch_time_weighted_panel`, every status,
~3M rows per 10-park chunk over the 730-day window) and expanded to a 15-min grid, so ids and
status are categoricals (the groupbys pass `observed=True`), and each chunk logs
`time-weighted panel: <parks> parks, <readings> readings -> <ride-days> ride-days, RSS <GB>`.
Watch that line and `docker stats` on the first nights; expected ~1–3 GB peak per chunk in the
14 GB container.

| futr_exog | meaning | unknown |
|---|---|---|
| `hours_ratio` | opening hours ÷ their mean over the last four same weekdays (d−7 … d−28) with a published window | 1.0 |
| `close_vs_ref` | closing hour − those days' mean closing hour, hours (past midnight counts past 24) — the late close | 0.0 |
| `closed_days_before` | days without a published window since the previous one, cap 14; > 0 = first operating day after a closure. Only a gap between two published windows counts | 0 |
| `sched_known` | the day's OPERATING window was published as of the origin | — (flag) |
| `school_break_first_day` / `_last_day` / `_day_after` | change of the own region's school-break state (d−1 → d, d → d+1) | 0, with `school_holiday_known` (PAR-1049) |

All are NaN-free, as NeuralForecast requires of a futr_exog: an unknown value is its neutral
value plus a flag. Built in `db.schedule_drivers` and `add_calendar_covariates`.

**As of the origin — what is and is not possible.** `schedule_entries` keeps only the latest
version of a day, with `updatedAt` as the one hint of when it became known (ml-bench's
`updated_us` is that column exported). A futr_exog has **one value per day**, shared by every
training window whatever its lead, so a per-lead as-of (ml-bench's `add_asof`, which masks
per origin) cannot be expressed. What is built: `sched_asof` = the window's last write came
before the day began — for today and later, before now. In the future frame that is exactly
the schedule as published at the forecast origin; in the history it is "published by the
day before", i.e. a lead-1 as-of for every training day, and a re-sync that rewrote an
unchanged row reads as unknown (conservative). At long leads the history therefore knows
more windows than the forecast does (PAR-830 §4: 84 % known at d1, 59 % at d60);
`sched_known` is how the model can tell the two apart. The reference days and the closure
gap read every published window before the day — those days are past at the origin.

**Target.** The shadow trains on the **time-weighted** daily P90 (`db.time_weighted_p90`):
each quarter-hour of the published OPERATING window (cut at midnight; without one, the
span of the park's own OPERATING readings) takes the last reading before its end, of any
status, held for at most 3 h; slots that are OPERATING with a wait ≥ `NF_MIN_WAIT` enter the
P90 (≥ 3 slots per ride-day) — `rideQ90`'s rule in `forecast-archive.service.ts`. The served
target stays the P90 over change-log rows, which weighs a jittery busy hour above a calm
one. The archive's truth is built from 15-min slots too, so part of any `cal_busier` /
`cal_far` gain of the shadow can be target alignment rather than the drivers; `cal_rank` is
scale-free. Separating the two needs a second shadow — an owner decision, raised on the PAR-1057 pull request.

**Cost.** One more `queue_data` read per chunk (every status, not only OPERATING, and raw
rows rather than one aggregate per ride-day) and a 15-min grid in pandas — roughly
rides × days × 40 rows per chunk of 10 parks. The fit itself is unchanged: seven more
futr_exog columns, same `NF_WINDOWS_BATCH_SIZE`.
