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
    -- DETERMINISTIC id derived from the natural key (attractionId, hour).
    -- The PK is (id, hour), so a stable id makes that PK enforce one row
    -- per (attractionId, hour) and lets ON CONFLICT (id, hour) actually
    -- fire. Previously this was gen_random_uuid(), so the conflict target
    -- never matched and every re-run/retry/backfill inserted duplicate
    -- rows — double-counting sampleCount and skewing the percentiles.
    md5(s."attractionId" || '|' || s.hour::text)::uuid as id,
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
