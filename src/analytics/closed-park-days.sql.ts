/**
 * Whether a calendar day counts as a measured day for a park. This file is the
 * only statement of the rule in the repo, in two shapes: the CTE pair below,
 * used by the three aggregate queries in `park-historical-stats.service.ts`,
 * and {@link closedParkDayExists} for a query that spans every park at once.
 *
 * The two shapes exist because the park arrives differently. The aggregate
 * queries bind one park id and one timezone as parameters, so their day
 * expression can say `AT TIME ZONE $2`. The ML accuracy query
 * (`PARK_DAY_IS_CLOSED_SQL` in `src/ml/services/prediction-accuracy.service.ts`)
 * runs over every park in one statement and reads both the park id and the
 * timezone off joined columns, so it cannot read a CTE keyed on a bound id.
 * Restructuring it into the park-bound shape would mean one query per park
 * instead of one, for a figure the ML dashboard and the alert service both poll
 * hourly. It gets its own fragment here instead, so the rule still lives in one
 * file.
 *
 * `queue_data_aggregates` keeps only OPERATING rows, but some feeds report
 * rides as OPERATING with a wait of 0 while the park is shut, around the clock.
 * Europa-Park had 25,418 such headliner rows on the 28 CLOSED days of February
 * 2026, and `byMonth` printed February as 28 sample days with a median of 0
 * (PAR-692). Over one year, 78,425 of the park's 252,893 aggregate rows sit on
 * days the schedule calls CLOSED — 31 % (PAR-698).
 *
 * The test is the one `/calendar` shows: a day is shut when it has a park-level
 * CLOSED entry and no park-level OPERATING entry. A single ride's CLOSED entry
 * says nothing about the park, and an OPERATING entry beside the CLOSED one
 * means the park opened after all.
 *
 * The fragments are deliberately exported as text rather than a query builder:
 * the callers are raw SQL with different shapes — one filters a grouped day
 * column, others filter aggregate rows directly, one hangs off a query builder's
 * `andWhere` — and the only thing they must share is the rule, not the query
 * around it.
 *
 * `schedule_entries.date` is a PARK-LOCAL date, so every caller hands in a
 * park-local day expression. Comparing it against a UTC day asks the calendar
 * about the wrong date in both directions, and measurably: the ML accuracy
 * query did exactly that until PAR-707, and against production over 30 days,
 * 334 of its rows landed on the neighbouring day. At `America/New_York` (−4)
 * Sesame Place's 20:00–23:45 rows carried the NEXT UTC date (270 rows, charged
 * to a shut day on a date the park's own calendar calls OPERATING); at
 * `Asia/Riyadh` (+3) Six Flags Qiddiya City's 00:00–00:15 rows carried the
 * PREVIOUS one (64 rows, the other way). So it is not only a negative-offset
 * problem — every park off UTC is wrong in its own edge-of-day hours.
 *
 * What the day expression means is the PARK-LOCAL CALENDAR day, and that is a
 * deliberate approximation shared by all callers rather than a claim about the
 * operating day. The two part company at both ends of the day, in opposite
 * directions, and production has an example of each. Sesame Place closed at
 * 18:00 on 2026-09-07, so its 20:00–23:45 rows are after closing time on a day
 * the calendar calls OPERATING, and they now count. Six Flags Qiddiya City's
 * operating day 2026-10-04 ran 16:00 → 00:00, so its 00:00–00:15 rows are the
 * closing edge of that operating day while the calendar day they fall on
 * (2026-10-05) is shut, and they now do not count. Both are the right answer to
 * "which calendar day is this?" and neither is an answer to "which operating
 * day is this?". Clipping the rule to operating hours would change what it
 * decides, for every caller at once, so it is tracked separately (PAR-722) and
 * is not what PAR-707 did.
 */

import { observedReadingsSql } from "../common/utils/closure-gap.sql";
import {
  MEASURED_OPERATION_DEAD_HOURS_FROM,
  MEASURED_OPERATION_DEAD_HOURS_TO,
  MEASURED_OPERATION_MAX_BLOCK_HOURS,
  MEASURED_OPERATION_MIN_BLOCK_HOURS,
  MEASURED_OPERATION_MIN_DISTINCT_WAITS,
} from "../parks/utils/measured-operation.gate";

/**
 * True when measured ride activity says the park operated on that day anyway.
 *
 * This is the one place the measured-operation verdict is expressed, so the
 * calendar and the four statistics callers cannot drift apart on it — which is
 * the whole point: since PAR-697 a day the operator calls shut may be published
 * as an operating day, and a calendar that reopens a day while the statistics
 * still drop it is the split PAR-692 closed. The thresholds live in
 * `src/parks/utils/measured-operation.gate.ts` together with what each of them
 * was measured to reject.
 *
 * ## Two conjuncts, and the order is load-bearing
 *
 * The first reads `queue_data_aggregates`, hourly and pre-computed, on the
 * `("parkId", hour)` index. The second reads raw `queue_data` for one
 * park-local day, which is the only place `is_heartbeat` exists — the rollups
 * carry no such column, so a feed frozen on an OPERATING reading writes itself
 * a block that clears every other condition (`writeHourlyHeartbeats` copies
 * `status` and `waitTime` forward hourly for up to 24 h).
 *
 * The second conjunct is affordable **only** because the first one almost never
 * lets it run. Against production, 6 070 park-level CLOSED days carry 1 227
 * with any activity and 199 that clear the first conjunct; those 199 are what
 * reaches the raw table, and 193 of them survive it. Putting the cheap test
 * second would hand the hypertable all 6 070.
 *
 * ## Both halves are bounded by a timestamp range, never by a day cast
 *
 * `(hour AT TIME ZONE tz)::date = <day>` is the obvious way to write "this
 * park-local day" and it is the expensive one: the cast is not sargable, so
 * each evaluation scans every aggregate row the park owns instead of one day's
 * chunk. Measured on this fragment inside {@link closedParkDaysCte} against
 * production, that difference is the whole cost of the rule — 8 ms for the old
 * rule, 4 523–8 346 ms with the day cast, 135–161 ms with the range bounds
 * below, over Europa-Park, Walibi Holland and Chimelong Paradise. The verdicts
 * are identical; only the plan changes.
 *
 * The bounds hang on the outer row, so TimescaleDB re-decides chunk exclusion
 * per evaluation (G-128) — but it excludes to a single day each time, which is
 * what makes the per-day shape affordable at all, and it is the only shape the
 * cross-park caller can use: {@link closedParkDayExists} sits in a query
 * builder's `andWhere` and has no CTE to hang a set-based pass on.
 *
 * ## Why `p50 > 0` and not `sampleCount`
 *
 * The question is whether rides were queueing, not whether the aggregator wrote
 * a row. `queue_data_aggregates` keeps only OPERATING rows, but a feed reporting
 * OPERATING with a wait of 0 around the clock produces rows all the same — that
 * is PAR-692's 25 418 Europa-Park rows on 28 shut February days.
 *
 * @param parkIdExpr SQL yielding the park id. Compared as text against
 *   `queue_data_aggregates."parkId"` and cast back to `uuid` for `attractions`,
 *   so a `uuid` column and a text placeholder both work.
 *   A compile-time constant from our own SQL, never user input.
 * @param parkLocalDayExpr SQL yielding a PARK-LOCAL `date`.
 *   A compile-time constant from our own SQL, never user input.
 * @param parkTimezoneExpr SQL yielding the park's IANA timezone.
 *   A compile-time constant from our own SQL, never user input.
 */
export function measuredOperationDayExists(
  parkIdExpr: string,
  parkLocalDayExpr: string,
  parkTimezoneExpr: string,
): string {
  return `(EXISTS (
            SELECT 1
            FROM queue_data_aggregates mo_agg
            WHERE mo_agg."parkId" = (${parkIdExpr})::text
              AND mo_agg.hour
                  >= ((${parkLocalDayExpr})::text || ' 00:00')::timestamp
                     AT TIME ZONE ${parkTimezoneExpr}
              AND mo_agg.hour
                  <  (((${parkLocalDayExpr}) + 1)::text || ' 00:00')::timestamp
                     AT TIME ZONE ${parkTimezoneExpr}
              AND mo_agg.p50 > 0
            HAVING COUNT(DISTINCT mo_agg.p50)
                     >= ${MEASURED_OPERATION_MIN_DISTINCT_WAITS}
               AND COUNT(*) FILTER (
                     WHERE EXTRACT(HOUR FROM mo_agg.hour AT TIME ZONE ${parkTimezoneExpr})
                           BETWEEN ${MEASURED_OPERATION_DEAD_HOURS_FROM}
                               AND ${MEASURED_OPERATION_DEAD_HOURS_TO}
                   ) = 0
               AND (MAX(mo_agg.hour) - MIN(mo_agg.hour))
                   BETWEEN INTERVAL '${MEASURED_OPERATION_MIN_BLOCK_HOURS} hours'
                       AND INTERVAL '${MEASURED_OPERATION_MAX_BLOCK_HOURS} hours'
          )
          AND EXISTS (
            SELECT 1
            FROM queue_data mo_raw
            JOIN attractions mo_a
              ON mo_a.id = mo_raw."attractionId"
            WHERE mo_a."parkId" = (${parkIdExpr})::text::uuid
              AND mo_raw.timestamp
                  >= ((${parkLocalDayExpr})::text || ' 00:00')::timestamp
                     AT TIME ZONE ${parkTimezoneExpr}
              AND mo_raw.timestamp
                  <  (((${parkLocalDayExpr}) + 1)::text || ' 00:00')::timestamp
                     AT TIME ZONE ${parkTimezoneExpr}
              AND mo_raw.status = 'OPERATING'
              AND mo_raw."queueType" = 'STANDBY'
              AND mo_raw."waitTime" IS NOT NULL
              AND mo_raw."waitTime" >= 5
              AND ${observedReadingsSql("mo_raw")}
            HAVING COUNT(DISTINCT mo_raw."waitTime")
                     >= ${MEASURED_OPERATION_MIN_DISTINCT_WAITS}
               AND COUNT(*) FILTER (
                     WHERE EXTRACT(HOUR FROM mo_raw.timestamp AT TIME ZONE ${parkTimezoneExpr})
                           BETWEEN ${MEASURED_OPERATION_DEAD_HOURS_FROM}
                               AND ${MEASURED_OPERATION_DEAD_HOURS_TO}
                   ) = 0
               AND (MAX(mo_raw.timestamp) - MIN(mo_raw.timestamp))
                   BETWEEN INTERVAL '${MEASURED_OPERATION_MIN_BLOCK_HOURS} hours'
                       AND INTERVAL '${MEASURED_OPERATION_MAX_BLOCK_HOURS} hours'
          ))`;
}

/**
 * A CTE named `closed_park_days`, listing the park's shut days.
 *
 * Unbounded by date on purpose: a park carries a few hundred park-level
 * schedule rows in total (Europa-Park: 741), so the partial index on
 * `("parkId", date) WHERE "attractionId" IS NULL` reads the lot in one scan,
 * and leaving the window out keeps the fragment free of placeholders the
 * callers would have to renumber.
 *
 * The park id is cast `::text::uuid` rather than straight to `uuid`, and the
 * detour is load-bearing. `queue_data_aggregates."parkId"` is a text column,
 * so every caller also compares the same placeholder against text. Postgres
 * infers one type per parameter: a bare `$1::uuid` here, in a CTE that is
 * textually first, pins the parameter to `uuid` and the aggregate comparison
 * further down then fails with `operator does not exist: text = uuid`. Casting
 * through text pins it to text and leaves the comparison a constant, so the
 * partial index on `("parkId", date)` is still used.
 *
 * @param parkIdParam The caller's placeholder holding the park id, e.g. `"$1"`.
 *   A compile-time constant from our own SQL, never user input.
 * @param parkTimezoneParam The caller's placeholder holding the park's
 *   timezone, e.g. `"$2"` — every caller already binds one for its own day
 *   expression. Needed by {@link measuredOperationDayExists}. A compile-time
 *   constant from our own SQL, never user input.
 */
export function closedParkDaysCte(
  parkIdParam: string,
  parkTimezoneParam: string,
): string {
  return `closed_park_days AS (
     SELECT se.date AS day
     FROM schedule_entries se
     WHERE se."parkId" = ${parkIdParam}::text::uuid
       AND se."attractionId" IS NULL
       AND se."scheduleType" = 'CLOSED'
       AND NOT EXISTS (
             SELECT 1 FROM schedule_entries operating_day
             WHERE operating_day."parkId" = se."parkId"
               AND operating_day."attractionId" IS NULL
               AND operating_day.date = se.date
               AND operating_day."scheduleType" = 'OPERATING'
           )
       AND NOT ${measuredOperationDayExists(
         parkIdParam,
         "se.date",
         parkTimezoneParam,
       )}
   )`;
}

/**
 * The predicate that keeps a day in. Needs {@link closedParkDaysCte} in the
 * same statement.
 *
 * `NOT EXISTS` rather than `NOT IN`: the latter answers UNKNOWN for every row
 * as soon as one day is NULL, which would drop the whole park instead of one
 * day.
 *
 * @param parkLocalDayExpr SQL yielding a park-local `date`, e.g. `"pad.day"` or
 *   `'(qda.hour AT TIME ZONE $2)::date'`. A compile-time constant from our own
 *   SQL, never user input.
 */
export function isNotAClosedParkDay(parkLocalDayExpr: string): string {
  return `NOT EXISTS (
            SELECT 1 FROM closed_park_days
            WHERE closed_park_days.day = ${parkLocalDayExpr}
          )`;
}

/**
 * The same rule as a self-contained `EXISTS`, for a query that spans every park
 * and therefore cannot bind the park as a parameter. True when the given park's
 * given day is shut. Needs no CTE, so it drops into a query builder's
 * `andWhere`; negate it at the call site to keep a day in.
 *
 * `EXISTS` rather than a join on purpose: event rows (`TICKETED_EVENT` and the
 * like) can share a day with the status row, and a join would then count every
 * outer row once per schedule row of its day (PAR-640).
 *
 * @param parkIdExpr SQL yielding the park's `uuid`, e.g. `'a."parkId"'`.
 *   A compile-time constant from our own SQL, never user input.
 * @param parkLocalDayExpr SQL yielding a PARK-LOCAL `date` — a `timestamptz`
 *   needs `AT TIME ZONE` against the park's own timezone column first, e.g.
 *   `'(pa.target_time AT TIME ZONE p.timezone)::date'`. `DATE(x)` on its own
 *   reads the session timezone (UTC in production) and is the bug this
 *   parameter exists to prevent. A compile-time constant from our own SQL,
 *   never user input.
 * @param parkTimezoneExpr SQL yielding the park's IANA timezone, e.g.
 *   `'p.timezone'` — the same joined column `parkLocalDayExpr` already needs.
 *   Required by {@link measuredOperationDayExists}. A compile-time constant
 *   from our own SQL, never user input.
 */
export function closedParkDayExists(
  parkIdExpr: string,
  parkLocalDayExpr: string,
  parkTimezoneExpr: string,
): string {
  return `EXISTS (
            SELECT 1 FROM schedule_entries se
            WHERE se."parkId" = ${parkIdExpr}
              AND se.date = ${parkLocalDayExpr}
              AND se."attractionId" IS NULL
              AND se."scheduleType" = 'CLOSED'
              AND NOT EXISTS (
                    SELECT 1 FROM schedule_entries operating_day
                    WHERE operating_day."parkId" = se."parkId"
                      AND operating_day."attractionId" IS NULL
                      AND operating_day.date = se.date
                      AND operating_day."scheduleType" = 'OPERATING'
                  )
              AND NOT ${measuredOperationDayExists(
                parkIdExpr,
                "se.date",
                parkTimezoneExpr,
              )}
          )`;
}
