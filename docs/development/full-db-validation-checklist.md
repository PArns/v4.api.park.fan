# Full-DB Validation Checklist (post-deploy / staging)

> Why this exists: the P50/P90 + caching work (PR on `claude/brave-cray-vkcbq2`)
> was implemented against `pnpm build` + unit tests only — the dev/CI container
> has **no populated database** (Postgres isn't running, repos are mocked). The
> new baselines **self-calibrate at runtime**, so the formulas are structurally
> correct without data, but *bucket plausibility* and the new SQL can only be
> verified against real parks. Run this against staging/prod (read-only) after
> deploy.

Conventions: `$PARK` = a park UUID (use Phantasialand & Europa-Park — dense,
seasonal), `$ATTR` = a headliner attraction id (text). Redis examples use
`redis-cli`. **Read-only**: every SQL here is a `SELECT`; the Redis writes are
done by the app — only `GET`/`KEYS`/`TTL` to inspect.

---

## A. Crowd-level calibration invariants (most important)

The whole regime rests on **"a statistically typical day ≈ 100% = moderate"**.
Validate the invariant, not a fixed distribution (real levels depend on
conditions — promos, holidays).

### A1. Park calendar — typical-day-peak (baseline sanity)
```sql
-- typical-day-peak should sit between P50 and the pooled P90.
SELECT "parkId", "p50Baseline", "typicalDayPeak"
FROM park_p50_baselines WHERE "parkId" = '$PARK';
-- + pooled P90 for context
SELECT "p90Baseline" FROM park_p90_baselines WHERE "parkId" = '$PARK';
```
**Pass:** `p50 < typicalDayPeak < pooledP90` (for PHL the doc measured
≈ 30 / 40.3 / 51.6). If `typicalDayPeak` is null → the baseline cron hasn't run.

### A2. Per-attraction calendar (L8 — NEW, highest-priority check)
The per-attraction calendar now divides by `getAttractionTypicalDayPeak`, not P50.
Confirm the inflation is gone:
```sql
-- For a headliner, compare the OLD denominator (P50) with the NEW one
-- (median of daily peaks). The ratio is roughly the per-normal-day inflation
-- the old code applied.
WITH per_day AS (
  SELECT (qda.hour AT TIME ZONE (SELECT timezone FROM parks WHERE id = a."parkId"))::date AS day,
         PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qda.p90) AS day_peak
  FROM queue_data_aggregates qda
  JOIN attractions a ON a.id::text = qda."attractionId"
  WHERE qda."attractionId" = '$ATTR'
    AND qda.hour >= now() - interval '548 days'
    AND qda."sampleCount" >= 2
  GROUP BY day
)
SELECT
  (SELECT "p50Baseline" FROM attraction_p50_baselines WHERE "attractionId" = '$ATTR') AS old_p50_denominator,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY day_peak) AS new_typical_day_peak,
  COUNT(*) AS sample_days
FROM per_day;
```
**Pass:** `new_typical_day_peak > old_p50_denominator` (typically 1.3–2×) and
`sample_days ≥ 20`. Then hit the ride page and confirm a known **normal** day reads
`moderate` (not `high`/`very_high`) and a **known busy** day reads high+.
*Regression to catch:* if normal days still read elevated, the consumer didn't pick
up the new denominator.

### A3. Yearly vs monthly calendar agreement (H1)
Open the **same future date** in the monthly calendar and the yearly view.
**Pass:** identical `crowdLevel`. (Both now = AVG predicted headliner waits ÷
typical-day-peak.) Before the fix the yearly view was a "wall of red".

### A4. historical-stats matches the calendar scale (M2)
`/v1/parks/.../historical-stats` is now headliner-only + self-consistent.
**Pass:** a statistically typical month/weekday reads `moderate`; the busiest
season (Wintertraum/summer) reads high/very_high/extreme; the relative ordering
matches the monthly calendar for the same park. (It won't be byte-identical — it
uses the hourly pre-aggregation, not raw queue_data — but must be on the same
scale.)

---

## B. The new SQL — run it against real parks

These queries were never executed against data. Run them and sanity-check shape.

### B1. `getAttractionTypicalDayPeak` (analytics.service.ts)
Same query as A2's CTE. **Pass:** returns one row, non-negative
`typical_day_peak`, `sample_days` plausible for a busy ride (hundreds over 548d).
Spot-check a low-traffic ride returns `sample_days < 20` ⇒ the app caches `0`
(rating skipped) — verify `redis-cli GET attraction:typicalpeak:$ATTR` = `0` with a
short TTL, and a busy ride caches a real value with 24h TTL.

### B2. historical-stats headliner day-values (park-historical-stats.service.ts)
```sql
-- The per-day day_value the service aggregates by month/weekday.
WITH per_attraction_day AS (
  SELECT (qda.hour AT TIME ZONE 'Europe/Berlin')::date AS day, qda."attractionId" AS aid,
         PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY qda.p90) AS day_peak
  FROM queue_data_aggregates qda
  WHERE qda."parkId" = '$PARK'
    AND qda.hour >= now() - interval '2 years'
    AND qda."sampleCount" >= 2
    AND qda."attractionId" = ANY(
      SELECT "attractionId"::text FROM headliner_attractions WHERE "parkId" = '$PARK')
  GROUP BY day, aid
)
SELECT COUNT(*) AS operating_days,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY day_value) AS typical_day_peak
FROM (SELECT day, AVG(day_peak) AS day_value FROM per_attraction_day GROUP BY day) d;
```
**Pass:** `operating_days` ≈ the park's real operating days in the window;
`typical_day_peak` close to A1's `park_p50_baselines.typicalDayPeak` (same concept,
slightly different source). Verify the **headliner fallback**: a park with **no**
`headliner_attractions` rows still returns day-values (the `$6 IS NULL` branch).

---

## C. Caching

### C1. Park merge invalidation (HIGH-1)
Pick a test merge (or a park that was merged) and confirm no stale per-park keys
survive:
```
redis-cli --scan --pattern 'park:*:$PARK'         # statistics/p50/p90/typicalpeak/...
redis-cli --scan --pattern 'analytics:crowdlevel:park:$PARK:*'
redis-cli --scan --pattern 'park:historical-stats:v2:$PARK:*'
redis-cli GET attraction:integrated:$MIGRATED_ATTR   # should be (nil) right after merge
```
**Pass:** all gone immediately after the merge (not just after TTL).

### C2. Discovery geo structure busted on merge (HIGH-4)
`redis-cli GET discovery:geo:structure:v4` → should be `(nil)` right after a
merge/repair (then repopulated on next read). Confirm a newly merged/renamed park
appears in `/v1/discovery/*` without waiting 24h.

### C3. `/nearby` coordinate index (HIGH-2) — and that the RESULT stays per-user
```
redis-cli GET location:parkcoords:v1 | head -c 200    # the shared static index
redis-cli TTL location:parkcoords:v1                  # ~600s
```
**Pass:** the index key exists (one shared blob). Then call `/nearby` from two
different coordinates → **different** park orderings (the per-user result is NOT
cached). Confirm `/nearby` output is unchanged vs the previous deploy for a fixed
coordinate (refactor is behaviour-preserving). Verify no `location:*` key encodes a
lat/lng.

### C4. Single-flight (HIGH-5 subset)
Cold-start (or `redis-cli DEL ml:park:$PARK:daily:$TODAY`) then fire ~10 concurrent
requests to the calendar/yearly endpoint. **Pass:** the Python `/predict` service
logs **one** inference for that park, not ten.

### C5. `ml:last-accuracy-check` is written (MEDIUM-1)
After the accuracy compare cron runs:
`redis-cli GET ml:last-accuracy-check` → JSON with a **recent** `completedAt` and a
real `newComparisonsAdded`. **Pass:** the ML dashboard's "last accuracy check" is
no longer a fabricated "just now / 0".

---

## D. ML served-quantile monotonicity (model.py fix)
On a sample of real predictions, confirm `q0.5 ≤ q0.8 ≤ q0.95` holds per row
(`predict_quantiles`). Quick check: log/inspect a `/predict` batch and assert the
crowd signal (q0.8) is never below the displayed wait (q0.5), and the uncertainty
band (`q0.95 − q0.5`) is ≥ 0 for every row.

---

## E. Plausibility (cross-cutting)
For a dense seasonal park (PHL/Europa-Park), eyeball that crowd levels follow
**real** occupancy:
- Wintertraum (Dec/Jan), Easter, Pfingsten, summer Saturdays → high / very_high / extreme.
- Quiet off-season weekdays → low / very_low.
- A statistically typical day → moderate.

Success = levels track real conditions, **not** a fixed target shape. If an entire
surface skews one way (all red / all green), suspect a wrong denominator (peak÷P50
inflation) or a missing baseline.

---

## Known follow-ups (not blocking)
- **Single-flight** on `getParkPredictions("daily")` + discovery
  `getGeoStructure`/`getLiveStats` (need a method extraction first).
- **`analytics:headliners`** (6h TTL) isn't evicted when the headliner set is
  recomputed — stale up to 6h (bounded; merge/repair do evict it).
- **`stats.service.ts` percentiles** are still nearest-rank (M3, deferred — moot
  for historical-stats now; only the raw `/stats` endpoints read `park_daily_stats`).
- **Per-attraction typical-day-peak** is sourced from `queue_data_aggregates`
  (hourly) while the calendar numerator uses `attraction_hourly_history` slot-P90s
  — minor method/in-hours-filter drift; revisit if per-ride levels look off in E.
