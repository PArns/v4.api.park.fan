import {
  HEARTBEAT_SOURCE,
  MAX_CARRIED_HEARTBEATS,
  OUTAGE_QUEUE_TYPE,
  outageRunBreaks,
} from "../../common/utils/outage-rows.sql";
import {
  overlapMinutesSql,
  parkOpenWindowCtes,
} from "../../common/utils/park-open-window.sql";
import { RECONCILIATION_SOURCE } from "../../common/utils/source-absent-status.util";
import { attractionIsCuratedOutOfService } from "../../attractions/utils/curated-out-of-service.util";

/**
 * `queue_data` into outage intervals and exposure minutes.
 *
 * Two statements over one set of shared CTEs. Everything hard about this problem
 * lives in the shared half: `queue_data` is a change log, not a sample, so a row
 * is a STATE that holds until the next one — and three of our own writers put
 * rows in it that look like readings and are not.
 *
 * ## Two mistakes this shape exists to avoid
 *
 * **Minutes are summed over SEGMENTS, never over an interval's outer bounds.**
 * Measuring `[first DOWN, last DOWN]` against the opening hours invents downtime
 * out of silence: a ride whose first reading of the day lands at 11:00 in a park
 * that opened at 09:00 would be credited with two hours nobody observed. Each
 * capped segment is clipped to the windows on its own.
 *
 * **An overnight gap is not a lost sight.** A run stitched across a night has a
 * hole in it by construction — the park is shut and the safety-net heartbeat is
 * off by design. Censoring on "was there a gap" would make every multi-day
 * outage unusable. What matters is whether park hours INSIDE the interval went
 * unobserved, which is `spell_open_minutes - operating_minutes`.
 *
 * ## Bump this when the rules change
 *
 * A recomputation under new rules is not the world changing. Same device as the
 * content-change detector's `FINGERPRINT_VERSION`.
 */
export const OUTAGE_FINGERPRINT_VERSION = 1;

/**
 * How long one row may speak for.
 *
 * `writeHourlyHeartbeats` writes when the newest STANDBY row is over 60 minutes
 * old and the sync runs every 5, so an observed ride's rows sit at most ~65
 * minutes apart. Past 70 the silence is one of the four indistinguishable causes
 * and none of them is evidence: the time becomes `unobserved` and leaves the
 * denominator. The failure mode is always LESS exposure, never invented exposure.
 */
export const MAX_SEGMENT_MINUTES = 70;

/** Longer than this, wall-clock, and it is a works period rather than a breakdown. */
export const MAX_OUTAGE_DAYS = 7;

/** Below this an interval is inside the poll's own noise and is not an outage. */
export const MIN_OUTAGE_OPERATING_MINUTES = 5;

/**
 * Unobserved park minutes inside an interval before its duration is censored.
 *
 * Ten minutes is two polls. Below that the hole is the poll's own jitter; above
 * it, park hours passed with no reading and the length is a lower bound.
 */
export const MAX_UNOBSERVED_MINUTES_IN_SPELL = 10;

/**
 * A run may be stitched to the next across a gap no longer than this, provided
 * nothing between them says the ride ran.
 *
 * This is what keeps a multi-day outage ONE row. Twenty hours covers the longest
 * night in the catalogue while staying short enough that a ride which quietly
 * recovered and broke again the next afternoon is two events.
 */
export const OUTAGE_STITCH_HOURS = 20;

/**
 * The shared CTEs: park windows, tracked rides, and every reading turned into a
 * capped segment carrying a state.
 *
 * Parameters: `$1` uuid[] park filter or NULL, `$2` scan_start, `$3` win_end,
 * `$4` as_of.
 */
function sharedCtes(): string {
  return `
${parkOpenWindowCtes()},
  tracked AS (
    SELECT a.id AS aid, a."parkId" AS pid, z.tz
      FROM attractions a
      JOIN park_tz z ON z.park_id = a."parkId"
     WHERE a.retired_at IS NULL
       -- A free-flow ride has no queue and its source reports it CLOSED all day.
       -- It cannot produce a meaningful outage and would only add noise.
       AND COALESCE(a.open_with_park, FALSE) = FALSE
       -- A merge reparents the loser's history with no dedupe, so afterwards two
       -- interleaved series sit on top of each other and flap between OPERATING
       -- and DOWN at the same instant. Skip the ride while its merge is inside
       -- the window rather than manufacture outages out of the seam.
       AND (a.last_merged_at IS NULL OR a.last_merged_at < $2::timestamptz)
  ),
  rows_raw AS (
    SELECT t.aid, t.pid, t.tz,
           qd.timestamp AS ts,
           qd.status,
           COALESCE(qd.data_source, '') AS src,
           -- NULL means "written before the column existed"; fall back to the
           -- old heuristic rather than promoting a year of carried rows to
           -- observations.
           COALESCE(
             qd.is_heartbeat,
             qd."lastUpdated" IS NOT NULL AND qd."lastUpdated" = qd.timestamp
           ) AS is_hb,
           ${outageRunBreaks("qd")} AS breaks
      FROM tracked t
      JOIN queue_data qd ON qd."attractionId" = t.aid
     WHERE qd."queueType" = '${OUTAGE_QUEUE_TYPE}'
       AND qd.timestamp >= $2::timestamptz
       AND qd.timestamp <  $3::timestamptz
  ),
  seg AS (
    SELECT aid, pid, tz, ts, status, src, is_hb, breaks,
           -- A row speaks until the next one, capped. The cap is what keeps an
           -- ingestion gap out of the denominator.
           LEAST(
             COALESCE(LEAD(ts) OVER (PARTITION BY aid ORDER BY ts), $4::timestamptz),
             ts + INTERVAL '${MAX_SEGMENT_MINUTES} minutes'
           ) AS seg_end,
           CASE
             WHEN src = '${RECONCILIATION_SOURCE}' THEN 'ABSENT'
             WHEN src = '${HEARTBEAT_SOURCE}' AND status = 'CLOSED' THEN 'ABSENT'
             ELSE status::text
           END AS st,
           -- Consecutive carried heartbeats: rows since the last observed one.
           -- Reset by any real reading, so a single heartbeat between two
           -- observations costs nothing.
           SUM(CASE WHEN is_hb THEN 0 ELSE 1 END) OVER (
             PARTITION BY aid ORDER BY ts ROWS UNBOUNDED PRECEDING
           ) AS obs_grp
      FROM rows_raw
  ),
  seg_c AS (
    SELECT *,
           CASE WHEN is_hb
                THEN ROW_NUMBER() OVER (PARTITION BY aid, obs_grp ORDER BY ts) - 1
                ELSE 0 END AS carried
      FROM seg
  )`;
}

/**
 * STATEMENT 1 — outage intervals, one row per interval.
 *
 * Groups consecutive non-breaking rows into runs, stitches runs across a night
 * when nothing between them says the ride ran, measures each segment against the
 * park's flattened windows, and decides how the interval ended.
 *
 * Three things it gets right that a naive `LAG`/`LEAD` grouping does not:
 *
 * 1. **The ending reading belongs to the run it ends.** The group id counts
 *    breaking rows STRICTLY BEFORE the current one. Counting the current row
 *    pushes every terminator into the next group and every outage comes back
 *    open-ended.
 * 2. **A multi-day outage stays one row.** The stitch joins the evening's run to
 *    the next morning's, and `operatingDays` says how many days it touched.
 * 3. **Carried heartbeats stop being evidence.** Past `MAX_CARRIED_HEARTBEATS`
 *    in a row the state is `unconfirmed` rather than extended.
 */
export const OUTAGE_INTERVALS_SQL = `
WITH ${sharedCtes()},
  marked AS (
    SELECT *,
           -- STRICTLY BEFORE. See point 1 above.
           COALESCE(
             SUM(CASE WHEN breaks THEN 1 ELSE 0 END) OVER (
               PARTITION BY aid ORDER BY ts
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
             ), 0
           ) AS grp,
           -- How many times this ride has been SEEN RUNNING so far.
           --
           -- The stitch below needs to know whether the ride ran again between
           -- two DOWN runs, and end_state cannot answer that. A breaking row
           -- carries the group of the run it closes, so in
           -- DOWN,DOWN,CLOSED,OPERATING,DOWN,DOWN the CLOSED closes the first
           -- run and the OPERATING lands in a group of its own with no run in
           -- it — invisible. The stitch then sees prev_end_state = CLOSED,
           -- treats the seam as one interrupted outage and merges two separate
           -- ones: measured on Cedar Point over 7 days, 24 of 330 run pairs
           -- inside the 20-hour window.
           --
           -- A running count survives that, because it does not care which
           -- group the sighting fell into.
           SUM(CASE WHEN breaks AND status = 'OPERATING' THEN 1 ELSE 0 END) OVER (
             PARTITION BY aid ORDER BY ts ROWS UNBOUNDED PRECEDING
           ) AS ops_seen
      FROM seg_c
  ),
  run_rows AS (
    SELECT * FROM marked WHERE NOT breaks
  ),
  run_ends AS (
    -- At most one per group by construction: the group id increments after each
    -- breaking row, so a run and its terminator share exactly one id.
    SELECT aid, grp, ts AS ended_at, st AS end_state FROM marked WHERE breaks
  ),
  runs AS (
    SELECT r.aid, r.pid, r.tz, r.grp,
           MIN(r.ts)                          AS started_at,
           MAX(r.seg_end)                     AS down_until,
           MIN(r.ops_seen)                    AS ops_at_start,
           MAX(r.ops_seen)                    AS ops_at_end,
           COUNT(*)::int                      AS rows_in_run,
           COUNT(*) FILTER (WHERE r.is_hb)::int AS hb_rows,
           MAX(r.carried)::int                AS max_carried
      FROM run_rows r
     GROUP BY r.aid, r.pid, r.tz, r.grp
  ),
  runs_e AS (
    SELECT runs.*, e.ended_at, e.end_state
      FROM runs
      LEFT JOIN run_ends e ON e.aid = runs.aid AND e.grp = runs.grp
  ),
  stitched AS (
    SELECT *,
           SUM(CASE WHEN prev_until IS NULL
                     OR started_at > prev_until + INTERVAL '${OUTAGE_STITCH_HOURS} hours'
                     OR prev_end_state = 'OPERATING'
                     -- The ride was seen running between the two runs, wherever
                     -- that sighting fell. Two outages, not one interrupted.
                     OR ops_at_start > prev_ops_at_end
                    THEN 1 ELSE 0 END)
             OVER (PARTITION BY aid ORDER BY started_at ROWS UNBOUNDED PRECEDING) AS spell
      FROM (
        SELECT r.*,
               LAG(down_until) OVER (PARTITION BY aid ORDER BY started_at) AS prev_until,
               LAG(end_state)   OVER (PARTITION BY aid ORDER BY started_at) AS prev_end_state,
               LAG(ops_at_end)  OVER (PARTITION BY aid ORDER BY started_at) AS prev_ops_at_end
          FROM runs_e r
      ) s
  ),
  spells AS (
    SELECT aid, pid, tz, spell,
           MIN(started_at)                                   AS started_at,
           MAX(down_until)                                   AS down_until,
           -- The LAST run's terminator is the spell's. An earlier run's end was
           -- stitched over precisely because it did not say the ride ran.
           (ARRAY_AGG(ended_at  ORDER BY started_at DESC))[1] AS ended_at,
           (ARRAY_AGG(end_state ORDER BY started_at DESC))[1] AS end_state,
           SUM(rows_in_run)::int                             AS rows_in_spell,
           SUM(hb_rows)::int                                 AS heartbeat_rows,
           MAX(max_carried)::int                             AS max_carried
      FROM stitched
     GROUP BY aid, pid, tz, spell
  ),
  spell_rows AS (
    SELECT s.spell, r.*
      FROM run_rows r
      JOIN (SELECT DISTINCT aid, grp, spell FROM stitched) s
        ON s.aid = r.aid AND s.grp = r.grp
  ),
  -- Minutes per SEGMENT, clipped to the windows. Never over the spell's outer
  -- bounds: that would credit the ride with park hours nobody observed.
  seg_minutes AS (
    SELECT sr.aid, sr.spell,
           SUM(${overlapMinutesSql("sr.ts", "sr.seg_end", "w.opens_at", "w.closes_at")})
             AS operating_minutes,
           SUM(CASE WHEN sr.is_hb THEN 0
                    ELSE ${overlapMinutesSql("sr.ts", "sr.seg_end", "w.opens_at", "w.closes_at")}
               END) AS observed_minutes,
           COUNT(DISTINCT w.op_day) AS operating_days
      FROM spell_rows sr
      JOIN win w ON w.park_id = sr.pid
                AND sr.ts < w.closes_at
                AND sr.seg_end > w.opens_at
     GROUP BY sr.aid, sr.spell
  ),
  measured AS (
    SELECT s.*,
           COALESCE(sm.operating_minutes, 0) AS operating_minutes,
           COALESCE(sm.observed_minutes, 0)  AS observed_minutes,
           COALESCE(sm.operating_days, 0)    AS operating_days,
           -- Park minutes the interval SPANS. The difference against
           -- operating_minutes is how much of it went unobserved during
           -- opening hours, which is the only gap that censors anything.
           COALESCE((
             SELECT SUM(${overlapMinutesSql("s.started_at", "s.down_until", "w.opens_at", "w.closes_at")})
               FROM win w WHERE w.park_id = s.pid
           ), 0) AS spell_open_minutes,
           EXTRACT(EPOCH FROM (s.down_until - s.started_at)) / 60.0 AS wall_minutes
      FROM spells s
      LEFT JOIN seg_minutes sm ON sm.aid = s.aid AND sm.spell = s.spell
  ),
  classified AS (
    SELECT m.*,
           (m.spell_open_minutes - m.operating_minutes
              > ${MAX_UNOBSERVED_MINUTES_IN_SPELL}) AS lost_sight,
           (m.ended_at IS NOT NULL)                 AS has_end
      FROM measured m
  )
SELECT c.aid                                   AS "attractionId",
       c.pid                                   AS "parkId",
       c.started_at                            AS "startedAt",
       c.ended_at                              AS "endedAt",
       ROUND(c.operating_minutes)::int         AS "operatingMinutes",
       ROUND(c.observed_minutes)::int          AS "observedOperatingMinutes",
       ROUND(c.wall_minutes)::int              AS "wallMinutes",
       GREATEST(c.operating_days, 1)::smallint AS "operatingDays",
       CASE
         WHEN c.max_carried > ${MAX_CARRIED_HEARTBEATS}          THEN 'unconfirmed'
         WHEN c.lost_sight                                        THEN 'gap'
         WHEN NOT c.has_end
          AND c.down_until >= $4::timestamptz - INTERVAL '${MAX_SEGMENT_MINUTES} minutes'
                                                                  THEN 'ongoing'
         WHEN NOT c.has_end                                       THEN 'window_edge'
         WHEN c.end_state = 'OPERATING'                           THEN 'recovered'
         WHEN c.end_state = 'REFURBISHMENT'                       THEN 'reclassified'
         WHEN c.end_state = 'ABSENT'                              THEN 'source_absent'
         ELSE 'closed'
       END                                     AS "endReason",
       (c.started_at <= $2::timestamptz + INTERVAL '${MAX_SEGMENT_MINUTES} minutes')
                                               AS "startCensored",
       (c.wall_minutes > ${MAX_OUTAGE_DAYS} * 24 * 60) AS "likelyWorksPeriod",
       LEAST(c.rows_in_spell, 32767)::smallint  AS "rowsInSpell",
       LEAST(c.heartbeat_rows, 32767)::smallint AS "heartbeatRows",
       -- The operating day this interval STARTED on, so the per-day start count
       -- keys against the exposure table exactly. Not the calendar date of
       -- started_at: a park closing at 02:00 puts a 00:30 outage on the
       -- PREVIOUS operating day, and a calendar key would file the start under a
       -- day the exposure table has no row for.
       (SELECT MIN(w.op_day) FROM win w
         WHERE w.park_id = c.pid
           AND w.opens_at < c.down_until
           AND w.closes_at > c.started_at)    AS "startOpDay"
  FROM classified c
 WHERE c.operating_minutes >= ${MIN_OUTAGE_OPERATING_MINUTES}
   AND NOT EXISTS (
     SELECT 1 FROM attractions a
      WHERE a.id = c.aid
        AND ${attractionIsCuratedOutOfService("a", "(c.started_at AT TIME ZONE c.tz)::date")}
   )
 ORDER BY c.pid, c.aid, c.started_at
`;

/**
 * STATEMENT 2 — exposure, one row per (ride, park operating day).
 *
 * The same segments, bucketed by state and clipped to the flattened windows.
 * Minutes only; `outageStarts` is written from statement 1 in the same
 * transaction so a multi-day outage increments exactly one day.
 *
 * `unobserved` is the ceiling minus everything accounted for, which is what makes
 * the invariant checkable: a park hour with no reading behind it is named rather
 * than quietly folded into "closed".
 */
export const OUTAGE_EXPOSURE_SQL = `
WITH ${sharedCtes()},
  seg_win AS (
    SELECT s.aid, s.pid, w.op_day, s.st, s.is_hb,
           ${overlapMinutesSql("s.ts", "s.seg_end", "w.opens_at", "w.closes_at")} AS mins
      FROM seg_c s
      JOIN win w ON w.park_id = s.pid
                AND s.ts < w.closes_at
                AND s.seg_end > w.opens_at
  ),
  park_open AS (
    SELECT park_id, op_day,
           SUM(EXTRACT(EPOCH FROM (closes_at - opens_at)) / 60.0) AS park_open_minutes
      FROM win
     GROUP BY park_id, op_day
  )
SELECT t.aid                             AS "attractionId",
       t.pid                             AS "parkId",
       po.op_day                         AS "opDay",
       ROUND(po.park_open_minutes)::int  AS "parkOpenMinutes",
       ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st = 'OPERATING'), 0))::int
         AS "operatingMinutes",
       ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st = 'DOWN'), 0))::int
         AS "downMinutes",
       ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st = 'DOWN' AND NOT sw.is_hb), 0))::int
         AS "downMinutesObserved",
       ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st = 'CLOSED'), 0))::int
         AS "closedMinutes",
       ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st = 'REFURBISHMENT'), 0))::int
         AS "refurbishmentMinutes",
       ROUND(COALESCE(SUM(sw.mins) FILTER (WHERE sw.st = 'ABSENT'), 0))::int
         AS "absentMinutes",
       GREATEST(ROUND(po.park_open_minutes - COALESCE(SUM(sw.mins), 0))::int, 0)
         AS "unobservedMinutes"
  FROM tracked t
  JOIN park_open po ON po.park_id = t.pid
  LEFT JOIN seg_win sw ON sw.aid = t.aid AND sw.op_day = po.op_day
 GROUP BY t.aid, t.pid, po.op_day, po.park_open_minutes
 ORDER BY t.pid, t.aid, po.op_day
`;

/**
 * Where the scan has to start so an open interval is not born at the edge.
 *
 * From the DATA, never from a calendar: an outage that is still running, or that
 * began before the requested window, has to be re-read from its own beginning or
 * it comes back truncated with a wrong duration. Anything the scan still touches
 * at its own start is marked `startCensored` and leaves the duration set while
 * counting as one event.
 *
 * Parameters: `$1` the requested window start, `$2` uuid[] park filter or NULL.
 */
export const OUTAGE_SCAN_START_SQL = `
  SELECT GREATEST(
           LEAST(
             COALESCE(MIN(o.started_at), $1::timestamptz),
             $1::timestamptz
           ) - INTERVAL '1 day',
           -- Floor. Without it the scan walks back to the oldest interval that
           -- is still open, forever: an interval ending in \ongoing\ or
           -- \window_edge\ stores ended_at = NULL, is deleted by the next run
           -- and written again identically, so one stuck spell pins the scan to
           -- its own start and DEFAULT_WINDOW_DAYS bounds nothing. That is the
           -- ~7-minute, multi-GB-spill case the 30-day default exists to avoid.
           --
           -- It also keeps the closure signal's regularity and cycle filters
           -- meaningful: both are shares over the scanned window, so a window
           -- that silently grows from 30 days to five months turns
           -- MAX_REGULAR_DAYS from "1 day in 6" into "1 in 36" and makes the
           -- stored history depend on when it was computed.
           --
           -- A spell older than the floor keeps its stored row untouched, which
           -- is the right trade: a wrong duration on one long-running outage
           -- beats an unbounded nightly scan.
           $3::timestamptz
         ) AS scan_start
    FROM attraction_outages o
   WHERE ($2::uuid[] IS NULL OR o."parkId" = ANY($2::uuid[]))
     AND (o.ended_at IS NULL OR o.started_at >= $1::timestamptz)
`;
