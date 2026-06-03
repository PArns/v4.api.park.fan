# TODO

## ML hourly_agg cache — post-deploy verification & follow-up

**Context:** `fetch_recent_wait_times` (`ml-service/predict.py`, the `WITH hourly_agg ...` query)
is the #1 steady-state DB load. Root cause: `base_time=datetime.now()` (microseconds) flowed
into the cache key → ~0% hit rate. Fix shipped: bucket `end_time` to the cache-TTL window +
raise TTL 2→15 min + evict expired entries on write.

**Baseline (pre-fix, measured 2026-06-03, ~133 min window):**

| fingerprint | calls/min | ms/min (DB time) | mean_ms |
|-------------|-----------|------------------|---------|
| 48c290bd    | 26.1      | 7196             | 275.2   |
| 0f0d8d65    | 3.6       | 1355             | 373.4   |
| **combined**| **~29.7** | **~8551** (≈14% of one core) | — |

### Verification protocol (run AFTER deploy)
- [ ] Confirm new ml-service container is live (Coolify redeploy done — module-global cache
      only resets on a fresh process, so the fix is NOT active until redeploy).
- [ ] `SELECT pg_stat_statements_reset();` on celestrial Postgres.
- [ ] Let it run ~30–60 min (cover ≥2 of the 15-min prediction crons + on-demand traffic).
- [ ] Re-run the baseline query (calls/min + ms/min for `query LIKE 'WITH hourly_agg%'`) and
      compare against the table above. Expect the on-demand/repeat-park calls to collapse.

Baseline SQL: `pg_stat_statements` joined with `pg_stat_statements_info`, normalize
`calls` and `total_exec_time` by `EXTRACT(EPOCH FROM now()-stats_reset)/60`.

### Decision gate — per-attraction caching (only if still hot)
If `hourly_agg` is still a top load after the fix, the remaining cost is the **single-attraction
path** `getAttractionPredictions` (`src/ml/ml.service.ts:975`, `attractionIds: [attractionId]`,
attraction-detail pages) which uses a per-single-attraction key and does NOT reuse the
park-level fetch (`predictForPark` → `activeAttractionIds`).
- [ ] If hot: cache the query result **split by attractionId** (safe — window functions are
      `PARTITION BY "attractionId"`, so each attraction's rolling values are independent).
      On read, assemble from per-attraction cache; query only the missing IDs. Then single-
      attraction and park-level paths share entries.
- [ ] If not hot: close this out, no further work.
