/**
 * The SELECT behind every row of `queue_data_aggregates`, shared by
 * `calculate-percentiles` and `backfill-percentiles` so the two cannot drift.
 *
 * `queue_data` stores changes, not samples (`isSignificantChange`), plus one
 * heartbeat row when an hour passed without a change. A ride whose posted wait
 * holds still for most of an hour therefore has one or two rows in it, and the
 * old `HAVING COUNT(*) >= 3` dropped exactly those hours. The value a guest saw
 * in such an hour is the one posted before it began, so each hour that has a
 * reading also takes the last STANDBY row before its start as an extra sample
 * (carry-forward).
 *
 * The anchor counts only when that row is OPERATING with a wait time: a
 * CLOSED/DOWN row before the hour is the newest fact, and nothing is carried
 * over it. It is looked up no further back than `ANCHOR_LOOKBACK`; the heartbeat
 * writes a row once the newest one is an hour old, so three hours is already slack.
 * The two extra bounds on `p.timestamp` in the LATERAL are implied by the window
 * (`$1 <= hour < $2`) but are constants, so chunk exclusion happens at plan time
 * instead of once per row (281 chunk nodes and 10.3 s for one day become 3 and 0.3 s).
 *
 * `HAVING COUNT(*) >= 2` is not the old threshold in disguise: one real row and
 * its anchor is a full hour, a single unanchored row (the first reading after
 * opening) is not, and readers already discard hours below two samples
 * (`MIN_SAMPLES_PER_HOUR`).
 *
 * Parameters: `$1` window start (inclusive), `$2` window end (exclusive).
 */
export const ANCHOR_LOOKBACK = "3 hours";

/**
 * The `id` of a `queue_data_aggregates` row, as SQL over its natural key.
 *
 * The PK is `(id, hour)`, so a deterministic id is what makes that PK enforce
 * one row per `(attractionId, hour)` and lets `ON CONFLICT (id, hour)` fire at
 * all. Before June 2026 this was `gen_random_uuid()`, the conflict target never
 * matched, and every re-run, retry and backfill inserted a duplicate row —
 * double-counting `sampleCount` and skewing every percentile.
 *
 * A function rather than a copy per statement, because the id is derived in two
 * unrelated places: the rollup below writes it, and an attraction merge has to
 * recompute it when it reparents a bucket (`ATTRACTION_DEPENDENCIES`). A second
 * derivation of this formula that drifts produces exactly the duplicate rows the
 * formula exists to prevent, and it would drift silently — a wrong hash is a
 * valid uuid.
 *
 * `hourExpr` is rendered with `::text`, so its value depends on the session
 * `TimeZone`. Every writer reaches this table through the application pool, so
 * every one of them renders it the same way; a reader comparing ids from a psql
 * session has to set `TimeZone` to the pool's (UTC) to get the same bytes.
 *
 * `attractionIdExpr` must be the value the row's `"attractionId"` column ends up
 * holding, byte for byte — the column is `text`, so the hash is over the stored
 * text and not over a canonicalized uuid.
 */
export function aggregateIdSql(
  attractionIdExpr: string,
  hourExpr: string,
): string {
  return `md5(${attractionIdExpr} || '|' || ${hourExpr}::text)::uuid`;
}

export const HOURLY_AGGREGATE_SELECT = `
  WITH observed AS (
    SELECT
      qd."attractionId",
      date_trunc('hour', qd.timestamp) AS hour,
      qd."waitTime"
    FROM queue_data qd
    WHERE qd.timestamp >= $1
      AND qd.timestamp < $2
      AND qd.status = 'OPERATING'
      AND qd."waitTime" IS NOT NULL
      AND qd."queueType" = 'STANDBY'
  ),
  anchors AS (
    SELECT h."attractionId", h.hour, prev."waitTime"
    FROM (SELECT DISTINCT "attractionId", hour FROM observed) h
    CROSS JOIN LATERAL (
      SELECT p.status, p."waitTime"
      FROM queue_data p
      WHERE p."attractionId" = h."attractionId"
        AND p."queueType" = 'STANDBY'
        AND p.timestamp < h.hour
        AND p.timestamp >= h.hour - INTERVAL '${ANCHOR_LOOKBACK}'
        AND p.timestamp >= $1 - INTERVAL '${ANCHOR_LOOKBACK}'
        AND p.timestamp < $2
      ORDER BY p.timestamp DESC
      LIMIT 1
    ) prev
    WHERE prev.status = 'OPERATING'
      AND prev."waitTime" IS NOT NULL
  ),
  samples AS (
    SELECT * FROM observed
    UNION ALL
    SELECT * FROM anchors
  )
  SELECT
    -- DETERMINISTIC id derived from the natural key (attractionId, hour);
    -- see aggregateIdSql above for why, and for the one other place that
    -- derives it.
    ${aggregateIdSql('s."attractionId"', "s.hour")} as id,
    s.hour as hour,
    s."attractionId",
    a."parkId",
    percentile_cont(0.25) WITHIN GROUP (ORDER BY s."waitTime") as p25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY s."waitTime") as p50,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY s."waitTime") as p75,
    percentile_cont(0.90) WITHIN GROUP (ORDER BY s."waitTime") as p90,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY s."waitTime") as p95,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY s."waitTime") as p99,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY s."waitTime") -
      percentile_cont(0.25) WITHIN GROUP (ORDER BY s."waitTime") as iqr,
    STDDEV(s."waitTime") as "stdDev",
    AVG(s."waitTime") as mean,
    COUNT(*) as "sampleCount",
    NOW() as "createdAt",
    NOW() as "updatedAt"
  FROM samples s
  INNER JOIN attractions a ON a.id = s."attractionId"
  GROUP BY s.hour, s."attractionId", a."parkId"
  HAVING COUNT(*) >= 2
`;

/**
 * The nightly upsert of `calculate-percentiles`, as one statement.
 *
 * Exported rather than written at the call site so a test can issue the real
 * thing: the whole point of the deterministic id is that `ON CONFLICT (id, hour)`
 * fires on a bucket that already exists, and a test that re-states the clause
 * proves its own copy fires instead. The merge path recomputing the id
 * (`moveAggregateBuckets`) is checked against this statement for that reason.
 *
 * Parameters as in `HOURLY_AGGREGATE_SELECT`: `$1` window start, `$2` window end.
 */
export const HOURLY_AGGREGATE_UPSERT = `
  INSERT INTO queue_data_aggregates (
    id, hour, "attractionId", "parkId",
    p25, p50, p75, p90, p95, p99,
    iqr, "stdDev", mean, "sampleCount",
    "createdAt", "updatedAt"
  )
  ${HOURLY_AGGREGATE_SELECT}
  ON CONFLICT (id, hour) DO UPDATE SET
    p25 = EXCLUDED.p25,
    p50 = EXCLUDED.p50,
    p75 = EXCLUDED.p75,
    p90 = EXCLUDED.p90,
    p95 = EXCLUDED.p95,
    p99 = EXCLUDED.p99,
    iqr = EXCLUDED.iqr,
    "stdDev" = EXCLUDED."stdDev",
    mean = EXCLUDED.mean,
    "sampleCount" = EXCLUDED."sampleCount",
    "updatedAt" = NOW()
`;
