# ML Prediction Quality Issues & Fixes

> **Discovered**: 2026-03-08 | **Fixed in training run**: 2026-03-09 06:00

---

## 1. Critical Bug: 5-Minute Predictions (`TotalUncertainty` vs `VirtEnsembles`)

### Symptom
All predictions returned `5 min` regardless of actual wait time (e.g. 40 min real wait → 5 min prediction).

### Root Cause
`predict_with_uncertainty` in `ml-service/model.py` used `prediction_type="TotalUncertainty"`, which returns **uncertainty scalars** `[knowledge_uncertainty, data_uncertainty]` (shape `(n, 2)`), not per-ensemble predictions.

`np.mean(axis=1)` averaged the two uncertainty values (~2.77), and `round_to_nearest_5(2.77)` → **5 min**.

### Fix (`ml-service/model.py`)
Switch to `prediction_type="VirtEnsembles"` (shape `(n, 10, 1)`) and take **median + std**:
```python
virtual_preds = self.model.virtual_ensembles_predict(
    X_ordered, prediction_type="VirtEnsembles", virtual_ensembles_count=10,
)
virtual_preds = virtual_preds.squeeze(axis=2)   # (n, 10)
predictions = np.median(virtual_preds, axis=1)   # median more robust than mean at n=10
uncertainty = np.std(virtual_preds, axis=1)      # std more stable than p5/p95 at n=10
lower_bound = np.maximum(predictions - uncertainty, 0)
upper_bound = predictions + uncertainty
predictions = np.maximum(predictions, 0)
```

**Why median over mean?** At `n=10` virtual ensembles, `np.percentile(5)` and `np.percentile(95)` collapse to the min/max, making bounds useless. `median ± std` is more statistically stable.

---

## 2. NoneType Crash in `fetch_holidays`

### Symptom
ML service crashed with `TypeError: '<' not supported between instances of 'NoneType' and 'str'` when `sorted(country_codes)` was called.

### Root Cause
Some parks have `None` in their `country_codes` list (e.g. parks with missing country metadata). `sorted()` cannot compare `None` with strings.

### Fix (`ml-service/db.py`)
```python
# Filter None before sorting
country_codes = [c for c in country_codes if c is not None]
cache_key = f"{','.join(sorted(country_codes))}:{start_date.date()}:{end_date.date()}"
```

---

## 3. Underprediction on Busy Weekend Days

### Symptom
On weekends, Efteling rides with 40 min actual waits were predicted at 15-20 min (after fix #1).

### Root Cause: `volatility_7d` Dominates Feature Importance

Feature importance analysis of model `v20260302_0600`:
```
volatility_7d                : 32.91%   ← dominant
rolling_avg_7d               : 15.23%
avg_wait_same_hour_last_week :  9.84%
trend_7d                     :  7.12%
is_weekend                   :  0.01%   ← effectively ignored
avg_wait_last_1h             :  0.00%   ← completely ignored
```

`volatility_7d` was computed over **all 7 days mixed together**. A ride with stable weekdays (10 min avg) and busy weekends (35 min avg) shows high volatility, but the model could not *use* that to predict higher wait times on weekends — it only knew "this ride is volatile" without knowing *when*.

Additionally, the volatility cap was set to `40 min std`, allowing extremely high values that drown out other signals.

### Fixes

#### 3a. Split volatility into weekday/weekend (`ml-service/features.py`)
New features in `calculate_trend_volatility`:
- `volatility_weekday` — log1p-dampened std of weekday-only observations in the 7d window
- `volatility_weekend` — log1p-dampened std of weekend-only observations in the 7d window

The model can now learn: "high `volatility_weekend` + is_weekend → predict higher wait".

#### 3b. Add rolling weekday/weekend averages (`ml-service/features.py` + `predict.py` SQL)
New features:
- `rolling_avg_weekday` — 7d rolling mean of weekday observations only
- `rolling_avg_weekend` — 7d rolling mean of weekend observations only

For Efteling Python on a Sunday: `rolling_avg_weekend ≈ 28 min` vs `rolling_avg_7d ≈ 17 min`.

SQL (in `fetch_recent_wait_times`):
```sql
AVG(CASE WHEN day_of_week BETWEEN 1 AND 5 THEN avg_wait END) OVER (...) as rolling_avg_weekday,
AVG(CASE WHEN day_of_week IN (0, 6) THEN avg_wait END) OVER (...) as rolling_avg_weekend
```
> Note: Postgres `EXTRACT(DOW ...)` returns 0=Sunday, 6=Saturday.

#### 3c. Add `avg_wait_same_dow_4w` (`ml-service/features.py` + `predict.py`)
Average of the last 4 same-day-of-week observations (−1w, −2w, −3w, −4w at the same hour).

More stable than a single 1-week lag; provides a representative "normal" for this hour+day-of-week combination. Computed via existing `merge_lag` helper.

#### 3d. Lower `VOLATILITY_CAP_STD_MINUTES` from 40 → 15 (`ml-service/config.py`)
The cap was too high (40 min), allowing `volatility_7d` to saturate and dominate. Lowering to 15 min compresses the dynamic range so temporal features (is_weekend, avg_wait_last_1h, holiday flags) contribute meaningfully.

---

## Impact Summary

| Feature | Status |
|---------|--------|
| `TotalUncertainty` → `VirtEnsembles` fix | **Live** (no retraining needed) |
| `fetch_holidays` NoneType crash | **Live** (no retraining needed) |
| `volatility_weekday` / `volatility_weekend` | Active in training pipeline; **effective from next training run (2026-03-09)** |
| `rolling_avg_weekday` / `rolling_avg_weekend` | Active in training + inference; **effective from next training run** |
| `avg_wait_same_dow_4w` | Active in training + inference; **effective from next training run** |
| `VOLATILITY_CAP_STD_MINUTES` 40 → 15 | **Live** in inference; **effective in training from next run** |

---

## Verification

Test attraction: **Danse Macabre** (Efteling, NL) — `attractionId: 6c9146ef-66a4-4620-9d21-1688ed94a4d3`

Weekend data confirmed the bug:
```
Hour  | Weekday avg | Weekend avg
11:00 |  8 min      |  22 min
14:00 | 14 min      |  35 min
16:00 | 12 min      |  31 min
```

Before fixes: all predictions ≈ 5 min (TotalUncertainty bug).
After fix #1: predictions ≈ 15-20 min (closer but weekday-biased).
After training with new features: expected to track weekend peaks correctly.
