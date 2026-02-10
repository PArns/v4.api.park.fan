# ML Service Performance

## Request flow (why 2–5 s shows as `*_phase1_ml_ms`)

1. **NestJS** (`getParkPredictions` / `getAttractionPredictionsWithFallback`):
   - Checks Redis `ml:park:{parkId}:{type}:{today}` (or attraction cache). **Cache hit → no Python call.**
   - On cache miss: builds payload (attraction IDs, weather, current/recent wait times, feature context, P50 baseline) and **POSTs to ML service** with 5 s timeout.

2. **Python** (`POST /predict` in `main.py`):
   - `predict_wait_times()` in `predict.py`:
     - **`create_prediction_features()`** – most of the time:
       - `fetch_parks_metadata()` – **cached 5 min** in process.
       - Timezone conversion, time features, weather (from request or **DB** `weather_data`).
       - **`fetch_holidays()`** – DB.
       - **Schedule query** – `schedule_entries` for park/date range.
       - **`fetch_recent_wait_times(attraction_ids, lookback_days)`** – **heavy**: 1 year (default 365 d, configurable) of `queue_data` aggregated by attraction, date, hour, day_of_week. Often the **slowest single step** for parks with many attractions. Uses partial index `idx_queue_data_ml_recent` (OPERATING + STANDBY).
       - Row-by-row loops for holiday and schedule logic (O(rows); can be slow for large feature DataFrames).
     - **`model.predict_with_uncertainty(features_df)`** – CatBoost inference (usually fast).
     - Format results and apply schedule filtering.

So the 2–5 s are mostly **feature building in Python** (DB + loops), not the HTTP hop or model inference.

## Existing optimizations

| Layer | What |
|-------|------|
| **NestJS** | Redis cache for full prediction response (2 h hourly, 6 h daily). Cache key includes `today` in park TZ so daily rollover invalidates. |
| **Python** | `fetch_parks_metadata(use_cache=True)` with 5 min in-process TTL. |
| **DB** | Single connection pool in ML service (`pool_size=10`). |

## Where to optimize (without changing behaviour)

1. **`fetch_recent_wait_times(lookback_days)`**
   - **Done:** Lookback is configurable via `PREDICTION_LOOKBACK_DAYS` (default 365). Reduces data volume vs 730 days.
   - **Done:** Partial index `idx_queue_data_ml_recent` on `queue_data (attractionId, timestamp)` WHERE status = 'OPERATING' AND queueType = 'STANDBY' speeds up the range query.
   - Optional: **materialized view or pre-aggregate** updated by a job (e.g. daily) and query that instead of raw `queue_data` for inference.

2. **Row-by-row loops in `create_prediction_features`**
   - Holiday and schedule logic use `for idx, row in df.iterrows()` and `df.at[idx, ...]`. For large DataFrames (many attractions × many timestamps) this is slow. Options: vectorize where possible, or merge with pre-built lookup DataFrames instead of per-row lookup.

3. **Schedule query**
   - Already scoped by `park_ids` and date range. Ensure index on `schedule_entries (parkId, date)` (or similar) so this stays fast.

4. **Observability**
   - **Done:** When DB phase in `create_prediction_features` exceeds 500 ms, Python logs `create_prediction_features db: holidays=…ms schedule=…ms recent_wait=…ms lookback=…d`. Total features and model time are already logged when features > 1 s or model > 500 ms.

## Cache hit rate

- **Park/attraction detail** requests that hit Redis for `ml:park:...` or the attraction prediction cache **do not call the ML service**; `*_phase1_ml_ms` is then 0 or very small (e.g. health check).
- If almost every request shows 2–5 s in `*_phase1_ml_ms`, cache hit rate is low (e.g. many different parks/attractions, or TTL too short). Improving NestJS-side cache warmup or TTL can reduce load and latency without touching the ML service.

## Summary

| Step | Typical cost | Notes |
|------|----------------|--------|
| NestJS → Python HTTP | &lt; 50 ms | 5 s timeout; not the bottleneck. |
| `create_prediction_features` | **1.5–4 s** | DB (recent wait times, holidays, schedule) + Python loops. |
| `model.predict_with_uncertainty` | &lt; 500 ms | CatBoost on feature matrix. |
| Schedule filter + response | &lt; 100 ms | In-memory. |

Focus: **DB queries and aggregation in `create_prediction_features`** (especially `fetch_recent_wait_times`) and **row-wise logic**; then **NestJS cache hit rate**.

---

## Optimization options

| Measure | Status | Effect |
|---------|--------|--------|
| **Parks metadata cache** | ✅ In place | `fetch_parks_metadata` 5‑min in-process cache. |
| **NestJS ML response cache** | ✅ In place | Redis `ml:park:...` / attraction predictions; cache hit = no Python call. |
| **DB queries in parallel** | ✅ Done | Holidays, schedule, `fetch_recent_wait_times` run in a `ThreadPoolExecutor`; latency ≈ max of the three instead of sum. |
| **Reduce lookback days** | ✅ In place | `PREDICTION_LOOKBACK_DAYS=365` (default); halves data volume vs 730. |
| **Index on `queue_data`** | ✅ In place | `idx_queue_data_ml_recent` partial index (OPERATING + STANDBY) for `fetch_recent_wait_times`. |
| **Hourly cache TTL** | ✅ 2 h | NestJS Redis TTL for hourly predictions increased to 2 h for better hit rate. |
| **Vectorize row loops** | Optional | Replace holiday/schedule `iterrows()` logic with merge/lookup DataFrames. |
| **Pre-aggregate / NestJS feeding** | Optional | NestJS could send precomputed lag features (avg 24h, 7d, …); Python could then skip `fetch_recent_wait_times`. |
