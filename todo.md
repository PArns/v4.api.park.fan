# TODO

## PCN/Shape shadow boards — one-time reset + post-deploy steps (PR #79)

**Context:** the pre-fix shadow scorers overwrote matured board days with ever-smaller
rolling-window slices (visible live: lead-bucket N sums > the "all" row). The full-day
contract fix (PR #79, [review §8](docs/ml/pcn-intraday-review.md)) makes new writes
correct, but the already-persisted matured rows are irreparably degraded and must be
dropped once. Yesterday + today regenerate within the next hourly score run.

- [ ] **After PR #79 is deployed**, run once against prod Postgres:

  ```sql
  DELETE FROM pcn_intraday_comparisons WHERE target_date < CURRENT_DATE;
  DELETE FROM shape_comparisons       WHERE target_date < CURRENT_DATE;
  ```

- [ ] Verify the next scored days are consistent: per (segment, date), the lead-bucket
      `n` values must sum exactly to the `all` row, and daily N should jump ~10×+
      (full days instead of the last hour).
- [ ] **Optional but recommended BEFORE the first post-deploy forecast/score run:**
      pre-create the new indexes without blocking writes (the in-code
      `CREATE INDEX IF NOT EXISTS` is non-concurrent — the first build over the
      accumulated backlog would block the producer for minutes):

  ```sql
  CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_pcn_forecasts_created_at
      ON pcn_forecasts (created_at);
  CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_shape_forecasts_created_at
      ON shape_forecasts (created_at);
  ```

- [ ] Expect the first `/score` run to take longer once: the new retention prune
      (`pcn_forecasts` 14d / `shape_forecasts` 30d) deletes the accumulated backlog
      (order 10⁸ rows). If it times out, pre-delete manually in batches, e.g.
      `DELETE FROM pcn_forecasts WHERE created_at < now() - interval '14 days'`
      with a `LIMIT`-style loop (`ctid` batching) or during a quiet window.
- [ ] Let the boards mature **1–2 weeks**, then re-confirm the PCN swap win
      (busy/headliner, no quiet inflation) and re-judge Shape offline-vs-live —
      no Shape producer swap before that.
- [ ] After the next nightly PCN retrain (08:30 UTC): models pick up the new
      DOW channels (11 channels); spot-check `/status` + board that nothing regressed.
- [ ] When boards are clean: run the receptive-field bake-off
      (`run_bakeoff.py --layers 8` vs `--layers 2`, busy MAE/bias) — flip
      `PCN_GWN_LAYERS` only on a busy-segment win (review §5a).

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
path** `getAttractionPredictions` (`src/ml/ml.service.ts`, `attractionIds: [attractionId]`,
attraction-detail pages) which uses a per-single-attraction key and does NOT reuse the
park-level fetch (`predictForPark` → `activeAttractionIds`).
- [ ] If hot: cache the query result **split by attractionId** (safe — window functions are
      `PARTITION BY "attractionId"`, so each attraction's rolling values are independent).
      On read, assemble from per-attraction cache; query only the missing IDs. Then single-
      attraction and park-level paths share entries.
- [ ] If not hot: close this out, no further work.

---

## Remaining refactorings from the 2026-06 codebase review

The low-risk findings were fixed in PR #68 (N+1 batching, parallel weather sync,
shared live-data/pagination/cache-key helpers, dead TTL constants, luxon removal,
ML consistency cleanup, `safeJsonParse`, broken merge/repair cache invalidation).
What follows is the deliberately deferred rest — each item carries behavioral risk
and should be its own PR.

### 1. Tests for ShowsService / RestaurantsService (do this FIRST)

**Why first:** both services have **zero** spec files, and they are the precondition
for item 2 — refactoring untested sync code is how regressions ship.

**How:**
- Mirror the existing patterns in `src/attractions/services/attraction-integration.service.spec.ts`
  and `src/parks/parks.service.spec.ts` (repository mocks via `getRepositoryToken`,
  Redis mock as plain object).
- Priority coverage, in order:
  1. `shouldSaveShowLiveData` / `shouldSaveDiningAvailability` (delta-save contract:
     status change, showtimes/waitTime change, operating-hours change, day rollover
     via `hasDateChangedInTimezone`),
  2. `findBatchCurrentStatusByShows` (stale-showtime skip: OPERATING + lastUpdated > 48h → null;
     `projectShowtimesToToday` projection),
  3. `findTodayOperatingDataByPark` (timezone filter — feed rows across a midnight boundary),
  4. `syncShows` / `syncRestaurants` (upsert behaviour, slug uniqueness, wiki-only park filter).
- Effort: ~1 day. No production code changes needed.

### 2. Generic entity sync (attractions/shows/restaurants)

**Current state:** `syncAttractions`, `syncShows`, `syncRestaurants` share the
walk-parks → fetch-children → filter-type → prefetch-existing → upsert skeleton
(~80 duplicated lines), but differ on purpose:
- attractions: also syncs from Queue-Times (`qt-`) and Wartezeiten (`wz-`) sources,
- shows: batches updates (`toUpdate[]` + `Promise.all`) and inserts separately,
- restaurants: optional `deep` mode (per-entity `getEntity()` with fallback),
  prefetch via `In(apiExternalIds)`.

**How:** template-method base class, NOT full unification:
```ts
abstract class ThemeParksEntitySync<TEntity, TChild> {
  // template: park loop + isThemeParksWikiId() skip + children fetch + prefetch maps
  protected abstract filterChildren(children: EntityChild[]): TChild[];
  protected abstract mapChild(child: TChild, parkId: string): Partial<TEntity>;
  protected abstract persist(toInsert: ..., toUpdate: ...): Promise<void>; // strategies stay per-entity
}
```
Attractions keep their qt-/wz- branches OUTSIDE the template (only the wiki branch
moves in). Don't force `deep` into the template — keep it a restaurants-only hook.
**Prereq:** item 1. Effort: ~1–2 days incl. test updates.

### 3. Queue processor batch-loop helper (NOT a base class)

**Current state:** ~21 processors in `src/queues/processors/` repeat
logger + batch loop (`BATCH_SIZE = 5`) + success/failure counters + duration log.
Redis done-markers and error semantics vary too much for inheritance.

**How:** extract only the uniform part into `src/queues/utils/batch-runner.util.ts`:
```ts
export async function runInBatches<T>(items: T[], batchSize: number,
  worker: (item: T) => Promise<void>,
): Promise<{ succeeded: number; failed: number }>
```
Adopt it opportunistically when touching a processor; don't do a big-bang rewrite.
Effort: helper ~1h, adoption incremental.

### 4. `any`-sweep in analytics.service.ts

**Current state:** ~12 `any`s in `src/analytics/analytics.service.ts`, mostly
untyped raw-SQL rows (`getRawMany()` results sorted/mapped with `(a: any, b: any)`).
Smaller offenders: `park-merge.service.ts`, `conflict-resolver.service.ts`,
`open-meteo.client.ts`, `file-logger.util.ts`.

**How:** for each raw query, declare a row type next to it (same pattern as the
`ExistingScheduleRow` type added to `parks.service.ts` in PR #68) and type the
`getRawMany<Row>()` call. Verify each field against the actual SELECT — numeric
columns come back as **strings** from pg (`parseFloat` sites are the tell).
Effort: ~0.5–1 day; mechanical but needs care with pg string-typing.

### 5. Search index bounding (only when the warning fires)

**Current state:** the 4 `loadXxxIndexFromDb` methods in `search.service.ts` are
unbounded full-table reads. There is no active/deleted flag to filter on, so a
naive LIMIT would silently drop entities from search. PR #68 added a size log +
a warning at >16 MB serialized.

**How (when the warning appears in logs):**
- preferred: add an `isSearchable`/popularity-derived flag and filter on it,
- or: split the Redis index into per-continent keys and lazy-load,
- or: switch serialization to msgpack/gzip (last resort, complexity for ~2–3x).

### 6. Python `features.py` vectorization (nice-to-have)

**Current state:** per-park `groupby` loops with `df.loc[idx, ...]` assignments for
timezone-local features (`ml-service/features.py` ~lines 51–114). Affects nightly
training wall-time only, not request latency.

**How:** group rows by timezone (not park), convert once per unique tz via
`df["timestamp"].dt.tz_convert(tz)`, assign back via `.loc[mask]`. Validate by
comparing feature output on a fixed dataset before/after (`verify_features.py`
exists for exactly this).

### 7. Decide: PoC scripts in nf-service

`nf-service/poc_eval.py` and `poc_eval_hourly.py` are standalone eval tools in the
same spirit as `backtest_*.py` / ml-service's `verify_*.py`. Kept for now.
Either document them in a README line each, or delete them. Owner call.
