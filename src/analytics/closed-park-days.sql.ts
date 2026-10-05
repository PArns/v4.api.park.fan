/**
 * The one place that decides whether a calendar day counts as a measured day
 * for a park, shared by every aggregate query so the three cannot drift.
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
 * Both halves are deliberately exported as text rather than a query builder:
 * the three callers are raw SQL with different shapes — one filters a grouped
 * day column, the others filter aggregate rows directly — and the only thing
 * they must share is the rule, not the query around it.
 *
 * `schedule_entries.date` is a PARK-LOCAL date, so every caller hands in a
 * park-local day expression. Comparing it against a UTC day would be wrong for
 * every park off UTC and silently wrong for parks with a negative offset, where
 * an evening hour already belongs to the next UTC date.
 */

/**
 * A CTE named `closed_park_days`, listing the park's shut days.
 *
 * Unbounded by date on purpose: a park carries a few hundred park-level
 * schedule rows in total (Europa-Park: 741), so the partial index on
 * `("parkId", date) WHERE "attractionId" IS NULL` reads the lot in one scan,
 * and leaving the window out keeps the fragment free of placeholders the
 * callers would have to renumber.
 *
 * @param parkIdParam The caller's placeholder holding the park id, e.g. `"$1"`.
 *   A compile-time constant from our own SQL, never user input.
 */
export function closedParkDaysCte(parkIdParam: string): string {
  return `closed_park_days AS (
     SELECT se.date AS day
     FROM schedule_entries se
     WHERE se."parkId" = ${parkIdParam}::uuid
       AND se."attractionId" IS NULL
       AND se."scheduleType" = 'CLOSED'
       AND NOT EXISTS (
             SELECT 1 FROM schedule_entries operating_day
             WHERE operating_day."parkId" = se."parkId"
               AND operating_day."attractionId" IS NULL
               AND operating_day.date = se.date
               AND operating_day."scheduleType" = 'OPERATING'
           )
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
