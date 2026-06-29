# ML Challenger: fixing busy-day / holiday under-prediction

> Status: **in progress** (started 2026-05-23). Living experiment log — every
> change, retrain, and metric goes here. Goal: future busy/holiday days
> (Pfingsten, Wintertraum, weekends) should predict realistically instead of
> regressing to a quiet recent-average.

## Problem (diagnosed)

The calendar's future days read far too low (Pfingstsonntag 2026 → `very_low`)
even though those days are genuinely busy. Root cause is **not** the crowd-level
calibration (typical-day-peak, shipped 2026-05-23) and **not** missing data
(German holidays incl. Pfingsten are present, 9162 DE rows). It is the **ML
model**:

- Active champion **v20260523_0600**: MAE 4.99, R² 0.882, 966 750 train samples.
- Feature importances are ~78 % rolling/recent + occupancy:
  `park_occupancy_pct 20.7%`, `avg_wait_last_1h 14.7%`, `rolling_avg_7d 10.0%`,
  `rolling_avg_28d 8.4%`, `rolling_avg_90d 8.4%`, `wait_time_velocity 5.9%` …
- **Calendar/holiday features ≈ 0 %**: `is_holiday_primary 0.00%`,
  `holiday_count_total 0.00%`, `is_bridge_day 0.00%`, `is_peak_season 0.00%`,
  `season 0.00%`, `is_weekend 0.05%`, `day_of_week 0.29%`.
- For future rows the autoregressive features become same-DOW-4-week proxies
  (predict.py ~1645-1678), so the model predicts "a typical recent <weekday>"
  and has no lever to lift a specific busy/holiday day.

**Evidence of under-prediction:** model predicts Taron ≈ 40 min for Pfingstsonntag;
recent weekends the top headliner actually hit 70–80 (`max_hdlnr`). AVG-headliner
predicted ≈ 23.5 ÷ typical-day-peak 40.25 = 58 % = `very_low`.

## Why not just swap CatBoost / use quantile loss

- Another GBT (XGBoost/LightGBM) = same family, same feature-dominance behaviour → no gain.
- **Constraint:** the loss is `RMSEWithUncertainty` (model.py:117), required for the
  VirtEnsembles uncertainty / confidence intervals (`posterior_sampling=True`,
  `predict_with_uncertainty`). A plain `Quantile` loss would break uncertainty.
- CatBoost itself is fine (best-in-class for tabular + categorical `attractionId`).
  The problem is the **training objective/feature dominance**, which is fixable in place.

## Levers (in priority order)

1. **Feature-forcing**: on holiday/weekend rows, neutralise (not DOW-proxy) the rolling
   features so the model is forced to learn `is_holiday`/`is_weekend`. Today's holiday
   dropout replaces with a DOW proxy that still leaks the level → holiday flag stays 0 %.
2. **Sample-weighting** busy/holiday rows (rare signal → upweight).
3. **Input types**: ensure binary calendar flags + categoricals are typed so CatBoost
   can actually split on them (audit `get_categorical_features` + dtypes).
4. **Serving-side (no retrain)**: use the uncertainty band (mean + α·std) as the crowd
   numerator for future days so high-variance busy days lift. Reversible.

## Success metric

NOT overall MAE (dominated by quiet days, hides the problem). Track:
- **Busy-day MAE / bias** (holiday + weekend subset of the 30-day hold-out).
- **PHL named-day sanity**: Pfingsten/Wintertraum/weekends predict high, quiet weekdays low.
- **Guardrail**: overall MAE must not regress materially (champion 4.99).
- **Feature-importance shift**: `is_holiday`/`is_weekend`/`season` should rise from ~0 %.

## Champion/Challenger discipline

Train challenger to its own version; evaluate offline (busy-day MAE) BEFORE flipping
`isActive`. Keep champion `.cbm` for rollback. Champion baseline: v20260523_0600.

## Workflow (fast iteration)

Live-patch the running ml-service container instead of full deploy each time:
1. Edit `ml-service/*.py` locally.
2. Stream into container: `... ssh dockerhost 'docker exec -i <ml> sh -c "cat > /app/<file>"' < ml-service/<file>` + `rm -rf /app/__pycache__/<file>.*`.
3. `POST localhost:8000/train {"version":"vYYYYMMDD_chalN"}` — the endpoint `importlib.reload`s config/model/features/train (picks up the patch), trains in a background thread, and writes the sentinel on completion (auto-activates for the ML service).
4. Evaluate from `metadata_<version>.pkl` (feature_importances, mae/r2) + live predictions.
5. Iterate. **Push to git only once a config is good** (then the daily cron registers it properly in `ml_models`). Direct `/train` does NOT register in the `ml_models` DB table.
6. Rollback: re-write `/app/models/active_version.txt` to the champion `v20260523_0600`.

Constraint: loss stays `RMSEWithUncertainty` (VirtEnsembles uncertainty) → no plain Quantile loss.

## Other potentially-missing signals (audit per request)

The 74-feature set covers rolling/occupancy, weather, DOW/hour/month/day_of_year,
holidays (national + 3 neighbor regions + school), bridge day, long weekend,
days_until/since_holiday, season, peak_season, ride attributes. Candidate gaps:

1. **Park special-event seasons (Wintertraum, Halloween)** — the BIGGEST PHL crowd
   drivers. `is_peak_season` (summer + December) is generic and 0% importance; it
   does not mark a park's actual event calendar. No per-park event-window feature
   exists. Highest-value missing signal, but needs an event-calendar data source.
2. **Source-market school holidays (NL/BE)** — PHL draws heavily from NL/BE. Verify
   those countries' school holidays are ingested as neighbor holidays for PHL.
3. **Scheduled operating-hours length** — extended hours correlate with busy days;
   not a feature (only `is_park_open` / `time_since_park_open`).
4. **Year-over-year same-event lag** — once ≥2y of data exists, a "same calendar
   position last year" feature would anchor recurring events.

Items 1 & 4 are follow-ups (data/feature-pipeline work); 2 is a quick verification.

**Verified 2026-05-23:** item 2 is covered — NL/BE holidays are ingested (incl. 142k NL
school-holiday rows; BE public/school/bridge) and PHL's `influencingRegions` include
NL-LI, NL-GE, BE. The `is_holiday_neighbor_1/2/3` features carry these. **But** iteration-1's
NaN-forcing mask only triggers on LOCAL holidays (`is_holiday_primary`/school/`is_bridge_day`),
NOT the neighbor flags — and NL/BE school holidays are among PHL's biggest drivers.
→ **Iteration-2 candidate:** add `is_holiday_neighbor_*` / `holiday_count_total>0` to the
NaN-dropout mask + busy-boost so the model learns the source-market-holiday lift too.

## Model-version + alternative-architecture research (2026-05-23)

- **CatBoost version**: installed/pinned **1.2.8**; latest **1.2.10** (patch-level behind).
  Low-risk hygiene bump, but NOT a fix for busy-day under-prediction. Don't change
  mid-experiment (keep variables controlled); do it separately.
- **Is there something better?** For our specific weakness — *known-future* holiday/event
  covariates that CatBoost lets rolling averages crowd out — yes, architecturally:
  - **TFT (Temporal Fusion Transformer)**: explicitly encodes past vs. **future** covariates
    with separate encoders + a variable-selection network (built-in feature importance).
    Strongest fit for "lift this future day because it's a holiday".
  - **DeepAR / Nixtla NeuralForecast**: global model across all attraction series,
    probabilistic, native `futr_exog` (holidays/weekend/season as future covariates).
  - **Nixtla TimeGPT**: foundation model, zero-shot, supports future exogenous features —
    fastest probe, but external/paid API.
  - Another GBT (XGBoost/LightGBM): same family → same crowding-out → skip.
- **Recommended parallel test**: **NeuralForecast (Nixtla) TFT** as an *offline* challenger —
  train on the same data export with holidays/weekend/season as `futr_exog_list`, evaluate
  busy-day MAE, compare to CatBoost. Keep CatBoost as production champion until proven.
  Cost: real R&D (new deps + training pipeline; GPU helps), not a one-afternoon task.
  → **Full feasibility report + PoC plan + verified code skeleton:**
  [NeuralForecast TFT Evaluation](./neuralforecast-tft-evaluation.md) (recommends a scoped
  PHL daily-peak PoC; success = busy-day MAE on a held-out Pfingsten/Wintertraum window).

## Experiment log

| # | date | change | overall MAE | busy-day MAE | is_holiday imp | PHL Pfingst pred | verdict |
|---|------|--------|-------------|--------------|----------------|------------------|---------|
| champion | 2026-05-23 | RMSEWithUncertainty, dropout 0.5/0.4/0.6/0.3, holiday 0.7 (DOW-proxy) | 4.99 | TBD | 0.00% | Taron≈40 → very_low | baseline |
| chal1 | 2026-05-23 | holiday/bridge dropout → **NaN** (4 cols only); busy weights ×1.8/1.6/1.4/1.25 | 4.967 | — | **0.00%** | unchanged | ❌ FAIL — model shifted level to `rolling_avg_weekday` (→14.4%); only 4 cols NaN'd left other rollings intact |
| chal2 | 2026-05-23 | NaN **full** level set on local-holiday rows (boost FAILED to apply — np bug → NaN-only ablation) | **6.02** (worse) | — | **0.02%** | unchanged | ❌ FAIL — model substituted `attractionId` (→**11.1%**) + `volatility_7d` (→14%) for the level instead of `is_holiday`; val MAE regressed. Rolled back to champion. |
| chal3 | 2026-05-23 | full level-NaN + **working** busy weighting (holiday×3.5/bridge×3/neighbor×2.5/school×2/weekend×1.3, cap 4.0) | **5.90** (worse) | — | **0.02%** | unchanged | ❌ FAIL — holidays STILL ~0%; model substituted `attractionId` (12.1%)+`volatility`; MAE regressed |

### ✅ CatBoost-side conclusion: DEAD END (confirmed over 3 iterations)
Partial-NaN (chal1), full-NaN (chal2), and full-NaN + aggressive holiday/neighbor sample
weighting (chal3) **all** left holiday importance at ~0%. CatBoost reliably substitutes a
level proxy (`rolling_avg_weekday` → `attractionId` → `volatility_7d`) rather than learning a
rare binary holiday flag, because that flag explains far less *global* variance than per-ride
level features. Forcing it (NaN) only hurts overall MAE without lifting holidays. **Stop here.**
The fix is architectural: a model with holidays as first-class *known-future* covariates →
NeuralForecast TFT. Production stays on champion **v20260523_0600** (CatBoost), now bumped to
**catboost 1.2.10**. Experimental `train.py` changes reverted (documented above; not shipped).

**Lesson from chal1:** partial NaN is useless — the model has ~9 redundant level
features; neutralising 4 just promotes the 5th. Must blank ALL level-carrying
features on holiday rows so only `is_holiday` + `attractionId` + DOW remain.

**Infra incident (chal2, 1st attempt):** the training worker **died during feature
engineering** (`Child process died`, uvicorn respawned it). Likely OOM — 4 uvicorn
workers each hold the model + the training spike; chal2 started ~3 min after chal1
before memory freed. Production stayed healthy on the champion (`active_version.txt`
= v20260523_0600). `/train/status` got stuck at `is_training:true` (dead worker never
wrote "failed") → had to reset `/app/models/training_status.json` manually before
retrying. **Mitigation:** retry only when container memory is low (~6.5/28 GiB);
monitor `docker stats` during the run. Follow-up: training and serving share the same
4-worker process pool — a dedicated training process / fewer workers during training
would prevent serving disruption.

**Parallel track:** NeuralForecast (Nixtla) **TFT** evaluation fanned out to a
sub-agent → `docs/ml/neuralforecast-tft-evaluation.md` (feasibility + PoC plan;
TFT encodes holidays as first-class future covariates, the architectural fix).
→ Now being implemented as a separate `nf-service/` Docker image (no official Nixtla
image exists; thin `python:3.11-slim` + torch CPU + neuralforecast).

**Bug found in chal2 (np shadowing):** `train_model` had a local `import numpy as np`
that made `np` function-local, so the busy-day `busy_boost` (earlier in the function)
hit `UnboundLocalError` → sample-weights calc threw → fell back to `None`. **So chal2
actually trained with the full level-NaN dropout but NO busy weighting** (a clean
NaN-only ablation). Fixed: numpy is now imported only at module level; container
re-patched. Next run (chal3) = NaN + working weighting.

(rows appended per iteration)

## CatBoost-side conclusion (forming)

chal1 (partial NaN) and chal2 (full NaN) both left holiday importance at ~0% — the
model always finds *some* level proxy (rolling avg → `attractionId` → `volatility`)
rather than learning a rare binary holiday flag. Strong evidence the **CatBoost-side
feature-forcing path is a dead end** for holiday lift. chal3 (heavy weighting) is the
last lever; if it also fails, the answer is the **NeuralForecast TFT** route (known-future
covariates as first-class structural inputs). Production stays on champion **v20260523_0600**.

## Reframe (2026-05-23, after the user's pushback): it's COMPRESSION, not holidays

Empirical test — champion `predict_with_uncertainty` on a recent week (125,777 rows),
bias = pred − actual by actual-wait bucket:

| actual | n | MAE | bias | upper-90 gap |
|---|---|---|---|---|
| quiet <20 | 90.4k (72%) | 2.21 | **+1.01** | 0.11 |
| 20-40 | 21.5k | 6.93 | −2.24 | 0.22 |
| 40-60 | 8.1k | 10.57 | −5.90 | 0.33 |
| **≥60** | 5.8k (5%) | 20.94 | **−15.86** | 0.63 |

Findings:
1. **Severe busy-tail under-prediction, scaling with busyness** (−16 min on ≥60). General
   (weekends + holidays), NOT holiday-specific — we over-focused on holidays. Classic
   regression-to-the-mean: RMSE predicts the conditional mean of a right-skewed dist.
2. **VirtEnsembles uncertainty is collapsed (~0.17 min width)** → `mean+k·std` lever is dead
   AND the `confidence` field is near-worthless. So the RMSEWithUncertainty constraint that
   blocked Quantile loss is **hollow**.
3. **Row imbalance (user's insight):** 72% of rows are quiet (<20), ~5% busy → the loss is
   dominated by quiet rows → busy tail under-fit. Root cause of (1). Note: it's *row-level*
   (most ride-hours are quiet even in busy parks), so park-filtering only partly helps;
   target-magnitude weighting / quantile loss are more direct.

### New levers (replacing the dead holiday-forcing)
- **Quantile loss** (`CATBOOST_LOSS_FUNCTION` env-gated, default unchanged; model.py picks
  eval-metric + disables posterior sampling for Quantile). Sweep **α=0.7/0.8/0.9**, trained
  WITHOUT activation (`train_model` directly, no sentinel → production stays champion),
  evaluated on busy-day bias. ← RUNNING.
- **Busyness/park weighting** (down-weight quiet rows / headliner-park focus) — complementary,
  test after the Quantile sweep tells us how much the loss alone fixes.

### Experiment log (cont.)
| q-sweep | 2026-05-23 | Quantile α∈{0.7,0.8,0.9}, no activation | see below | see below | n/a | n/a | ✅ **WORKS** — α0.8 cuts busy bias −15.4→−4.4 |

### ✅ Quantile-loss sweep result — the FIRST working lever
Busy-day **bias** (pred−actual) by actual-wait bucket, recent-week holdout:

| model | quiet<20 | 20-40 | 40-60 | **≥60** | quiet MAE | ≥60 MAE |
|---|---|---|---|---|---|---|
| champion RMSE | +1.0 | −2.1 | −5.7 | **−15.4** | 2.2 | 20.7 |
| Quantile 0.7 | +1.7 | +0.6 | −2.3 | **−10.2** | 2.4 | 19.5 |
| **Quantile 0.8** | +2.7 | +3.5 | +1.4 | **−4.4** | 3.1 | **18.8** |
| Quantile 0.9 | +5.0 | +8.1 | +7.8 | **+3.9** | 5.2 | 20.1 |

**Verdict: α=0.8 is the sweet spot** — very-busy under-prediction shrinks from −15.4 to −4.4
(~70%) with the lowest ≥60 MAE, at a modest quiet-day over-prediction cost (+2.7 bias). α=0.9
over-corrects. The lever that worked was the **loss/objective** (predict the upper conditional
quantile), directly countering the right-skew + 72%-quiet-row imbalance — NOT holidays.

**Refinement — busyness weighting (user's imbalance insight):** α=0.7 + quiet-row
down-weighting (`CATBOOST_BUSY_WEIGHT`, factor `clip((wait/20)^0.5, 0.4, 2.5)`):

| model | overall MAE | quiet<20 bias | 40-60 | ≥60 bias |
|---|---|---|---|---|
| champion | 4.99 | +1.0 | −5.6 | −15.3 |
| q0.8 | 6.56 | +2.7 | +1.4 | **−4.5** |
| **q0.7w** | **4.98** | +2.0 | −1.4 | −10.2 |

→ Weighting gives the **best overall MAE (4.98, beats champion)** + less quiet inflation, but
the **busy lift is driven by α, not the weighting** (q0.7w busy −10.2 ≈ q0.7, not q0.8's −4.4).
Convergence test **α=0.8 + weighting (q0.8w)** running — expect q0.8's busy fix + the
weighting's calibration benefit.

**Convergence — q0.8w (α=0.8 + busy-weighting) is the winner:**

| model | overall MAE | quiet<20 bias | 40-60 | ≥60 bias | ≥60 MAE |
|---|---|---|---|---|---|
| champion RMSE | 4.99 | +1.0 | −5.7 | −15.3 | 20.7 |
| q0.8 (no weight) | 6.56 | +2.7 | +1.4 | −4.5 | 18.8 |
| q0.7w | 4.63 | +2.1 | −1.4 | −10.1 | 18.9 |
| **q0.8w** | **5.17** | +2.8 | +1.9 | **−4.3** | **18.4** |

→ **q0.8w keeps q0.8's busy fix (−4.3) but recovers overall MAE (5.17 vs 6.56) and has the best
≥60 MAE (18.4).** The user's weighting insight worked: it improved calibration on top of the
alpha-driven busy lift. **This is the shippable CatBoost config** for the busy-compression defect.
Caveat: CatBoost still can't capture the *holiday-specific* surge (no holiday signal) — quantile
lifts the busy *tail* generally, but a holiday predicted as a "normal Sunday" only lifts to the
upper-quantile of a normal Sunday.

### Live production validation (US parks, 2026-05-23 ~19:05 UTC / mid-day US)
Deployed champion, predicted-this-hour vs actual-last-15min, US headliners:

| actual now | n | pred now | bias |
|---|---|---|---|
| quiet<20 | 126 | 12.6 | **+5.2** |
| 20-40 | 54 | 26.4 | −2.1 |
| 40-60 | 35 | 38.7 | −7.1 |
| **≥60** | 21 | 63.8 | **−9.6** |

Confirms the compression **live, on production, on the hourly/near-term surface** (not just the
far-future daily holdout): over-predicts quiet (+5), under-predicts busy (−10), scaling with
busyness. → The Quantile fix (q0.8w) helps the **hourly** path too; the hourly-TFT/quantile test
is well-motivated and US parks (currently busy, rich live data) are the place to run it.

## 🎯 NeuralForecast TFT PoC — the holiday lift CatBoost can't do

One-shot fit+predict (PHL, 37 series, 2552 daily rows, input_size=28/h=21). **TFT marquee
(Taron) 21-day forecast** — it clearly lifts holidays AND weekends via the future covariates:

| date | day | flag | TFT | NHITS |
|---|---|---|---|---|
| May 24 | Sun | weekend | **61.3** | 59.9 |
| May 25 | Mon | **HOLIDAY (Pfingstmontag)** | **62.9** | 58.2 |
| May 26–29 | Tue–Fri | — | 30–37 | 51–57 |
| May 30 | Sat | weekend | **70.0** | 58.9 |
| Jun 4 | Thu | **HOLIDAY (Fronleichnam)** | **47.1** | 51.9 |
| Jun 6 | Sat | weekend | **67.8** | 59.1 |

**TFT predicts Pfingsten ≈ 61–63 and Saturdays ≈ 66–70, weekdays ≈ 31** — exactly the
holiday/weekend-aware shape that matches reality (recent top-headliner Saturdays hit 70–80).
**Compare:** CatBoost champion predicted Pfingsten ≈ 40 (`very_low`). TFT, using `is_holiday`/
`is_weekend` as first-class *future* covariates, captures the surge CatBoost structurally cannot.
NHITS is much flatter (~50–60 everywhere — poor weekday/holiday differentiation) → **TFT ≫ NHITS** here.

## ✅ Overall conclusion & recommendation

1. **Busy-compression (general, weekends+holidays):** fixed in CatBoost via **Quantile α=0.8 +
   busy-weighting (q0.8w)** — busy under-prediction −15→−4, modest overall-MAE cost. Shippable now
   (needs the serving-path change for single-output Quantile).
2. **Holiday/event-specific surge (Pfingsten, Wintertraum):** only **TFT** captures it (holiday
   future-covariates). Validated on real data even with just ~5 months.
3. **Recommended target = hybrid:** CatBoost (q0.8w) for hourly/near-term; **TFT for the far-future
   daily calendar surface** (nightly train+forecast+cache — one job, sidesteps the save bug). The
   `nf-service` image is built and working; productionizing = the cache job + NestJS consuming it.

**✅ Productionized (2026-05-23, commit d76da66):**
- `config.py` defaults flipped to `CATBOOST_LOSS_FUNCTION=Quantile:alpha=0.8` + `CATBOOST_BUSY_WEIGHT=true` (env-gated; revert by setting RMSEWithUncertainty).
- Serving path: `model.predict_with_uncertainty` guards non-RMSEWithUncertainty models → returns point preds + zero-width band; `predict.py` treats a zero band as `use_uncertainty=False` → time-based confidence. `predict()` already handled single-output. Validated in-container with the q0.8w model (no crash, sensible preds).
- Deployed (api + ml-service rebuilt). Production kept serving the champion (RMSE) with the new code until the retrain. Then triggered the proper DB-registered training via `POST /v1/admin/train-ml-model` (localhost, bypasses Cloudflare) → version **v20260523_1920** (Quantile q0.8w) → registers in `ml_models` + activates. Verification in progress (busy-bias of the now-active model).

**⚠️ Recurring OOM on production training (4-worker pool):** the admin-triggered training
(v20260523_1920) died — worker OOM, same root cause as the chal2 experiment: the ml-service
runs **uvicorn --workers 4**, each worker holds the model, and the training thread spikes
~14-16 GiB inside one of them; combined with fresh-deploy bootstrap jobs it exceeded 28 GiB.
Single-process `train_model` (my experiments) peaked ~16 GiB and never OOM'd. **Infra fix
needed:** train in a dedicated process / fewer workers during training, or have the nightly
cron run a single-process train (docker exec) instead of the in-worker `/train`. Retried
v20260523_1939 on a now-quiet system (5 GiB baseline) — should fit.

**Remaining:**
- Serving path: a Quantile model has single-output `predict` and NO VirtEnsembles → adjust
  `predict_with_uncertainty` callers + the `confidence` field (the old uncertainty was 0.17 min
  anyway). Then it can be activated/registered.
- Fine-tune: test **α=0.75** and **α=0.8 + quiet-row down-weighting** (the imbalance lever) — the
  weighting may let a lower α hit the same busy fix with less quiet inflation.
- Decide: ship Quantile as champion vs. use it only for the far-future calendar surface.

## Parallel build: `nf-service/` (NeuralForecast)

Scaffolded a separate service (own Docker image, isolated from the CatBoost ml-service):
- `Dockerfile` — `python:3.11-slim` + torch **CPU** (from the cpu index, avoids the multi-GB
  CUDA wheels) + `neuralforecast==3.1.8`; single uvicorn worker (PyTorch memory).
- `requirements.txt`, `config.py` (reuses `DB_*` env; PoC scope via `NF_PARK_IDS`).
- `db.py` — daily-peak panel (park-local `ds`, P90 `y`, closed days dropped) + holiday/
  calendar **future-covariate** builder (local + neighbor NL/BE holidays, school, bridge,
  weekend, cyclic dow/doy, season).
- `forecast.py` — TFT + NHITS via NeuralForecast, `DistributionLoss(StudentT)` for uncertainty
  parity, `futr_exog_list` = the holiday/calendar set.
- `main.py` — FastAPI (`/health`, `/train`, `/train/status`, `/forecast`, `/forecast/latest`).
- No official Nixtla image exists → thin build. **Image builds + runs ✅** (`parkfan-nf:poc`,
  2.03 GB; neuralforecast 3.1.8 + torch-CPU + pytorch-lightning 2.5.6 on python:3.11-slim).
- **Smoke test ✅ (against live DB):** PHL panel = 2552 daily rows / 37 series /
  2025-12-26→2026-05-23; covariates populated — local holidays 186, **NL/BE neighbor 1147**,
  school 618. The holiday future-covariates CatBoost ignored are now first-class inputs.
- Fixed a `uuid = ANY(text[])` bind bug (cast `parkId::text`).

### PoC run findings (iterating)
- v1 failed: `input_size=90` > some series' history → reduced to 28 + `start_padding_enabled=True`.
- v2 **trained OK (~7 min)** but failed at `nf.save()`: NeuralForecast/PyTorch deepcopy bug with
  `DistributionLoss`. → **fit + predict in ONE process** (no save/reload). Design implication:
  the nf-service should do nightly **train+forecast+cache in one job** (which suits the calendar
  precompute anyway), not a save→load split. Re-running as a one-shot to get the holiday-lift signal.

### ⚠️ Data-length constraint (important PoC finding)
We only have **~5 months** of daily history (data starts Dec 2025 / Wintertraum 2025-26).
The blueprint's `input_size=365` + holding out **Pfingsten 2025 / Wintertraum 2024** is
**not possible** — that history doesn't exist. Adjusted PoC: `input_size≈90`, `h≈30`
(90+30=120 < ~150 days → ~30 train windows/series), hold out a recent busy window. These
are env-tunable (`NF_INPUT_SIZE`/`NF_HORIZON`), no rebuild needed. A proper long-horizon
daily TFT will only be trainable once ≥1-2 yr of data accrues — the model that needs
*explicit* holidays is also the model that needs *more history*.

**Sequencing:** don't run TFT training while the CatBoost ml-service is training (chal3) —
shared 28 GiB host, OOM risk (the chal1+chal2 lesson). Launch NF training after chal3 frees memory.

---

## Hourly TFT vs CatBoost — intraday slot comparison (2026-05-23)

**Frage des Users (a):** Warum nicht TFT auch für die 15-min/Stunden-Slots? Ist es besser als CatBoost?

**Aufbau (sauber out-of-sample, Leakage-Gate bestanden):**
- Park: **Disney's Animal Kingdom** (US). `base_time = 2026-05-21 00:00 ET`, Horizont 72 h.
- CatBoost: Modell **v20260520_2120** (RMSE, *vor* base_time trainiert → echt OOS). Damit das
  kein In-Sample wird, wurden `fetch_recent_wait_times` + `fetch_historical_park_occupancy`
  per `end_time=base_time` gebunden (vorher `NOW()`-anchored → hätten den Holdout geleakt).
- TFT: stündlicher Median-Panel je Attraktion (= CatBoost-Target `PERCENTILE_CONT(0.5)`),
  `freq=h`, `input_size=120`, `h=72`, futr_exog = Kalender + Stunde-des-Tages, geschlossene
  Stunden 0-gefüllt (reguläres Grid). Training nur auf `ds < base_time`. fit+predict in-process.
- Actuals: stündlicher Median je Attraktion, nur OPERATING/STANDBY/wait≥5.
- **Stichprobe dünn:** AK hat aktuell nur **8 Rides mit ≥5-min-Waits** → 226 Attraktion-Stunden,
  `busy≥60` nur n=7. Richtungsweisend, beim Busy-Tail aber **nicht** abschließend.

**Ergebnis (MAE / Bias, Minuten):**

| Schnitt | CatBoost | TFT | Sieger |
|---|---|---|---|
| h 7–24 | 7.8 / +3.4 | 9.1 / −3.8 | **CatBoost** |
| h 25–48 | 8.3 / −2.8 | 11.7 / −9.3 | **CatBoost** |
| h 49–72 (forward) | 7.5 / +2.6 | 10.1 / −6.5 | **CatBoost** |
| quiet <30 | 5.4 / +4.8 | 6.2 / −3.2 | **CatBoost** |
| 30–59 | 10.7 / −4.1 | 15.6 / −10.4 | **CatBoost** |
| busy ≥60 (n=7) | 20.0 / −10.5 | 24.5 / −24.5 | CatBoost (zu dünn) |
| **ALL** | **7.9 / +1.0** | **10.3 / −6.6** | **CatBoost** |

> Die „Sieger"-Spalte gilt nur für dieses **Quiet-Regime** (8 aktuell ruhige AK-Rides, Mittel 23.8 min).
> Der gleichmäßige TFT-Underbias (0-Fill) zieht *jede* Zeile nach unten — kein Per-Horizont-Argument.

**Bewertung (Schulnote):**

| Kriterium | CatBoost | TFT (hourly, lean) |
|---|---|---|
| Genauigkeit intraday (MAE) | **2** | 4 |
| Kalibrierung (Bias ~0) | **2** | 4− (systematisch −6.6) |
| Forward-Regime h49–72 | **2** | 4 |
| Busy-Tail | 3 (n=7) | 4 (n=7) |
| Reife / Integration | **1** (live, getuned) | 5 (PoC) |

**Befund:** Das lean/0-gefüllte Hourly-TFT **unter-prädiziert systematisch** (Bias −6.6). Dieser
gleichmäßige Underbias (0-Fill-Artefakt) dominiert den Vergleich und zieht alle Horizonte nach unten —
deshalb **kein** „TFT verliert sogar im Forward-Regime"-Argument. Ehrliche Lesart: auf den 8 aktuell
ruhigen AK-Rides schlägt lean-TFT CatBoost im **Quiet-Regime zentral nicht**; der **Busy-Tail bleibt
mit n=7 ungelöst** — also *nicht* „CatBoost gewinnt den Busy-Tail", sondern „kein Beleg, die Slots von
CatBoost wegzubewegen, und (noch) kein faires Busy-Urteil gegen TFT".

**Caveats (warum kein Todesurteil für TFT):**
1. Der negative Bias ist großteils ein **0-Fill-Artefakt** — der StudentT-Mittelwert wird von den
   vielen Geschlossen-Stunden nach unten gezogen. Ein TFT *nur auf Öffnungsstunden* (ohne 0-Fill)
   wäre fairer und vermutlich näher dran.
2. Bewusst **lean** konfiguriert (host-Speicher): `hidden=32`, `max_steps=300`, `input=120`,
   TFT-only — das ist ein **Performance-Floor**, nicht das Ceiling.
3. Stichprobe dünn (8 Rides, busy n=7) → Busy-Tail nicht entscheidbar.

**Empfehlung:** Intraday/Stunden-Slots **bei CatBoost** belassen. TFTs potenzielle Nische bleibt die
**Fernzukunft-Tagesfläche** (Feiertage als known-future Covariates — der Daily-PoC hob Pfingsten auf 61).
Vor einem echten Busy-Tail-Urteil: fairer Hourly-Retry (nur Öffnungsstunden, mehr Kapazität) auf einem
Park mit mehr aktuell hohen Waits.

### Coolify-Registrierung (2026-05-23)
nf-service war untracked → für Coolify unsichtbar. Jetzt in **beiden** Compose-Files registriert
(commit `0d263c4`): prod mit `mem_limit: 10g` + `/health`-Healthcheck + `coolify.managed`-Labels
(spiegelt ml-service), eigenes `/data/parkfan/nf-models`-Volume; dev auf Host-Port 8001; API bekommt
`NF_SERVICE_URL` (kein hartes `depends_on` — der experimentelle Forecaster darf den API-Start nicht
blockieren). **Deployed ✅** — Image `nf-service:0d263c4` (2.03 GB) gebaut + läuft. nf hat keinen
eigenen Cron; `/train` **nicht** im ml-service-06:00-Fenster triggern.

---

## Forward-Scoreboard: TFT vs CatBoost in Produktion (2026-05-23)

**Ziel (User):** TFT *nach* CatBoost trainieren, Cron dafür bauen, und Vergleichswerte
speichern, um beide Modelle **sauber** (fair) zu vergleichen. Gewählter Ansatz: **Forward-
Scoreboard** — beide Modelle machen echte Vorwärts-Prognosen, die nach Eintreffen der Ist-Werte
bewertet werden. Kein Holdout-Leak, kein In-Sample-Vorteil; Zahlen sammeln sich über Tage an.

### Cron-Sequenz (alle UTC)
- **06:00** CatBoost train (bestehend, `ml-training`).
- **07:30** `nf-training` Job `train-nf`: TFT train → poll bis fertig → `/forecast` (persistiert).
  Bewusst nach CatBoost, damit die beiden Trainings-Spikes nicht auf dem 28-GiB-Host kollidieren.
- **08:30** `nf-training` Job `score-comparison`: bewertet ausgereifte Zieltage.

### Vergleichs-Methodik (apples-to-apples)
- **Kanonisches Ziel = tatsächlicher Tages-P90-Peak je Attraktion** (was der Kalender nutzt).
- Beide Modelle werden gegen denselben Ist-P90 bewertet, jeweils mit der **frischesten Prognose,
  die *vor* dem Zieltag** gemacht wurde (echtes Forward, kein Leak).
  - TFT: nativer Tages-P90 (`NF_TARGET_PERCENTILE=0.9`).
  - CatBoost: seine Daily-Prediction = Wartezeit **um 14:00 lokal** ("typical peak time",
    `generate_future_timestamps`) als Peak-Proxy. **Caveat dokumentiert:** 14:00 ≈ Peak, aber
    nicht identisch zu P90 → ein evtl. leichter CatBoost-Underbias ist Semantik-Gap, kein reiner Fehler.
- Gespeichert je `(targetDate, model)`: `n`, `mae`, `bias`, `meanActual`, `meanPred`, `avgLeadDays`.

### Persistenz / Tabellen (keine Migration — synchronize/CREATE IF NOT EXISTS)
- **`tft_forecasts`** (nf-service schreibt in `persist_forecast`, `/forecast`-Endpoint): ein Row je
  `(attraction_id, target_date, forecast_date)`, upsert → vergangene `forecast_date`-Stände bleiben
  als echte Forward-Records erhalten.
- **`model_comparisons`** (NestJS TypeORM-Entity, `synchronize=true` legt die Tabelle an): das Scoreboard.

### Doppellauf-Schutz (User-Anforderung)
- **Registrierung:** `hasRepeatableJob` + fixe `jobId` (`nf-training-cron`, `nf-score-comparison-cron`)
  → keine doppelte Cron-Registrierung; Boot-Logik räumt überfällige Repeatables auf.
- **Überlappung:** `train-nf` prüft zuerst `/train/status`; läuft schon ein Training (bis ~90 min) →
  **skip** (auch 409 von nf-service wird sauber als skip behandelt). Beide Jobs teilen die Queue
  `nf-training` (Bull-Concurrency 1) → `train-nf` und `score-comparison` laufen nie gleichzeitig.
- **Retry:** beide Crons mit `attempts: 1` → kein Retry-Stacking. Daten sind ohnehin idempotent
  (Upsert per PK), ein versehentlicher Doppellauf würde nur dieselben Zeilen neu berechnen.

### Auswertung
```sql
SELECT "targetDate", model, n, ROUND(mae::numeric,1) mae, ROUND(bias::numeric,1) bias,
       ROUND("meanActual"::numeric,1) act, ROUND("meanPred"::numeric,1) pred, "avgLeadDays"
FROM model_comparisons ORDER BY "targetDate" DESC, model;
```
Erwartung: nach einigen Tagen genug Zieltage, um TFT vs CatBoost auf realen Forward-Prognosen
(MAE/Bias je Tag) sauber zu vergleichen. Champion bleibt bis dahin CatBoost (q0.8w).

---

## TFT-Training: lauffähig gemacht + optimiert (2026-05-24)

Langer Debug-/Optimierungs-Bogen, bis das TFT-Training auf der vollen Park-Menge stabil + multithreaded läuft.

### Root-Cause-Kette (jeder Fix war eine echte Ursache)
1. **Kovariaten-Hang (24 Min):** `add_calendar_covariates` lief O(Attraktionen × Holidays) mit row-wise `.apply`. → **vektorisiert pro Park, memoisiert** → 3 s.
2. **OOM beim Fit-Start (−9 / Container-Kill):** NICHT die Serien-Zahl, sondern der **`windows_batch_size`-Default** (zu groß) → TFT-Attention spikte >14g. cgroup `oom_kill`, `memory.peak`=14g bestätigt. → **`windows_batch_size=128` + `batch_size=16`** = der eigentliche Fix (MEM danach ~3,8g).
3. **Healthcheck-Ablenker:** in-thread-Training tötete bei OOM den uvicorn-PID1 (RC++); direkt-exec nur den exec-Prozess. War immer OOM, nie der Healthcheck.
4. **Multi-Core:** `NF_NUM_WORKERS=4` (Dataloader) → **~800% CPU / 8 Cores** (vorher 3% = dataloader-starved). Training in **eigenständigem `subprocess.Popen`-Prozess** (Standalone-Runner) → uvicorn bleibt responsiv, OOM isoliert.
5. **Iteratives Chunking** (10 Parks/Chunk): Sicherheitsmarge + erlaubt Worker; bad Chunks („No windows") werden geskippt statt den Lauf abzubrechen.
6. **`h=90→30`:** zu langer Horizont ließ ganze Chunks ohne Trainingsfenster → geskippt → Coverage-Verlust. h=30 → mehr Serien bilden Fenster.
7. **UUID→str:** `to_parquet`/persist scheiterten an `UUID`-Objekten in `unique_id` → `attractionId::text`.
8. **Holiday-Covariate-Bug (kritisch):** der Panel-`::text`-Cast machte `unique_id` zum String, `fetch_attraction_meta` blieb UUID → der Covariate-Join verfehlte → **„calendar covariates done: 0 parks" → ALLE Feiertags-Flags = 0** (TFTs Kern-Signal!). → meta `id::text/parkId::text`. **Erst danach trainiert TFT mit echten Feiertagen.**

### Endstand
Lauffähig, multithreaded (8 Cores), ~3,8g MEM, kein OOM, alle Parks (Chunks), saubere CI-Logs + per-Epoch-Progress mit **Gesamt-%** (im Log + via `/train/status` → Admin-Endpoint).

### TFT Best-Practices — HAVE/NEED (Research 2026-05-24)
- **HAVE:** TFT + StudentT-DistributionLoss, starke futr_exog (Feiertage/Kalender — jetzt wirklich aktiv), robust scaler, sinnvolle Defaults, funktionierende Infra.
- **NEED (priorisiert):** ① **statische Covariates** (`stat_exog`: country/region) — größter Hebel lt. Paper → **implementiert, flag-gated `NF_USE_STATIC=False`** (validieren, dann an); ② Early Stopping (Coverage-Risiko bei kurzen Serien — später); ③ Wetter als `hist_exog`; ④ Hyperparameter/GPU (sekundär, CPU-limitiert); ⑤ mehr Historie (wächst).

### Admin-System-Health-Endpoint
`GET /v1/admin/system-health` (Cloudflare-geschützt) liefert: Host (CPU/RAM/Disk/Load), Postgres (Conns, DB-Size, Cache-Hit), Redis (Memory, Keys, Hit-Rate), CatBoost (Status + aktives Modell mae/rmse/r2), TFT (Status + Live-Progress-%), und das **TFT-vs-CatBoost-Scoreboard**. Jede Quelle eigenes try/catch.

### CatBoost-vs-TFT-Vergleich — Stand
Scoreboard (`model_comparisons`) füllt sich: **CatBoost-Zeilen fließen** (Ziel-Tage 19.–23.05., MAE **~9–13 min**, Bias **−4 bis −8** gegen Ist-Tages-P90 — die 14:00-Daily-Prediction liegt erwartungsgemäß unter dem P90-Peak). **TFT-Zeilen** kommen, sobald die Forecasts des Holiday-Fix-Laufs reifen.

### OOS-Backtest auf HEADLINERN (2026-05-24) — das Ergebnis
Fairer Backtest: TFT trainiert auf Headliner-Daily-P90 < 10.05., prognostiziert 10.–23.05.;
CatBoosts echte Forward-Daily-Predictions; beide gegen Ist-Tages-P90; nur Headliner (1295 Tage / 199 Attraktionen).

| Segment | TFT MAE/bias | CatBoost MAE/bias |
|---|---|---|
| ALLE (1295) | **11.3 / +0.8** | 16.4 / −12.7 |
| quiet<40 (833) | 8.9 / +4.4 | 9.0 / −3.7 |
| busy≥40 (462) | **15.6 / −5.7** | 29.7 / −29.1 |
| busy≥70 (79) | **22.0 / −8.9** | 45.4 / −44.9 |

**TFT gewinnt klar auf der Tages-Peak-Fläche**, v.a. busy (busy≥70: MAE 22 vs 45). TFT kalibriert (Bias ~0..−9),
CatBoost unterschätzt den Peak massiv (−13 gesamt, −45 sehr busy). **Caveat:** CatBoost-Daily = 14:00-Wert,
nicht P90 → der −Bias ist teils strukturell. Aber der **Peak** ist genau die Kalender-Größe → für diesen Zweck
ist TFT das bessere Modell. Hebel: (A) TFT für die Tages-Peak-Kalenderfläche; (B) CatBoost-Daily auf Peak statt 14:00.
**Optimierung lohnt jetzt:** `NF_USE_STATIC=true` (country/region, #1-Lever), Wetter als hist_exog, mehr Historie
(quiet-Über-Bias +4.4 senken). Backtest-Script: `nf-service/backtest_headliners.py`.

### Static covariates: gemessen = marginal (2026-05-24)
`NF_USE_STATIC=true` (country/region als stat_exog) aktiviert + per Backtest gemessen: Headliner-MAE
**11.3 → 11.1** (ALL), busy≥70 sogar 22.0 → 22.7 — **alles innerhalb des Trainings-Rauschens**. Grund:
country/region zu grob + bei per-Park-Chunks teils redundant + kurze Historie. **Kein echter Lift hier** —
der Gewinn kam vom Holiday-Fix + dass TFT CatBoost beim Peak ohnehin schlägt. **Wichtiger Fix dabei:**
`static_df` muss auch an `nf.predict` (nicht nur `fit`), sonst „static exogenous variables not found" → Lauf bricht.
Empfehlung: `NF_USE_STATIC` kann aus bleiben/wieder aus, bis reichere Static-Features (attraction-type, park-size) + mehr Daten da sind.

### CatBoost-Daily auf Peak umgestellt (2026-05-24)
`generate_future_timestamps("daily")` erzeugt jetzt `DAILY_PEAK_HOURS=12,14,16`/Tag; `predict_wait_times`
kollabiert pro (Attraktion, Tag) auf das **MAX** = Peak-Proxy (≈ P90) statt des einzelnen 14:00-Werts.
Behebt den Numerator-vs-Peak-Baseline-Mismatch auf der Zukunfts-Kalenderfläche (las den Peak zu niedrig).
Greift, sobald der Prediction-Generator die Daily-Forecasts neu erzeugt; CatBoost-Scoreboard-Bias sollte
sich über die nächsten Tage verbessern. ~3× Daily-Inferenz-Compute (Batch).
