/**
 * Whether a calendar day counts as a measured day for a park, shared by the
 * three aggregate queries in `park-historical-stats.service.ts` so they cannot
 * drift apart.
 *
 * It is NOT the only statement of this rule in the repo. `PARK_DAY_IS_CLOSED_SQL`
 * in `src/ml/services/prediction-accuracy.service.ts` says the same thing for
 * the ML accuracy figures, as a correlated `EXISTS` keyed on an outer park
 * column rather than on a bound park id — it spans every park in one query and
 * cannot read the CTE below without being restructured. It also compares
 * `se.date = DATE(pa.target_time)`, a UTC day against this park-local column.
 * Folding the two together changes published accuracy numbers and needs its own
 * before/after, so it is a separate ticket, not a drive-by.
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
 */
export function closedParkDaysCte(parkIdParam: string): string {
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
