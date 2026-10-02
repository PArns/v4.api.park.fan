import { ScheduleType } from "../entities/schedule-entry.entity";

/**
 * The two deduplication statements `ParksService` runs over `schedule_entries`,
 * built once instead of copied per scope.
 *
 * `schedule_entries` holds park-level rows (`attractionId IS NULL`, the park's
 * opening hours) and per-ride rows in the same table. **The subject of a row is
 * (park, day, ride)** — the same rule `migrateScheduleEntries` states for the
 * merge path, where two rows are the same statement only if date, type and ride
 * all agree. Leave the ride out of the key and a park-wide sweep keeps one row
 * per park and day, which is either the opening hours or a single ride,
 * whichever `updatedAt` happens to favour.
 *
 * The ride is nullable, and that is why the cross-type filter is an `EXISTS`
 * rather than the row-wise `IN` it replaced: `(parkId, date, attractionId) IN
 * (SELECT …)` evaluates to NULL for every park-level row, so the pre-filter
 * would silently exclude exactly the rows the priority rule is written for.
 * `IS NOT DISTINCT FROM` compares the three columns as written — the same
 * operator, for the same reason, as in `migrateScheduleEntries`.
 *
 * Both builders take a scope rather than a parameter list so the global and the
 * per-park caller share one derivation. The park id travels as `$1`, never
 * interpolated.
 */

/** `"global"` sweeps every park; `"park"` binds the park id to `$1`. */
export type ScheduleDedupScope = "global" | "park";

/**
 * Phase 1 — rows that state the same thing twice: identical park, day, ride and
 * type. Keeps the most recently written one.
 */
export function sameTypeDuplicateSql(scope: ScheduleDedupScope): string {
  const parkFilter = scope === "park" ? `WHERE "parkId" = $1::uuid` : "";

  return `
      DELETE FROM schedule_entries
      WHERE id IN (
        SELECT id FROM (
          SELECT id,
                 ROW_NUMBER() OVER (
                   PARTITION BY "parkId", date, "attractionId", "scheduleType"
                   ORDER BY "updatedAt" DESC
                 ) as rn
          FROM schedule_entries
          ${parkFilter}
        ) sub
        WHERE rn > 1
      )
    `;
}

/**
 * The day-status types phase 2 arbitrates between. Every other `ScheduleType`
 * is an addition to the day and is never removed by phase 2.
 */
export const CONFLICT_TYPES = [
  ScheduleType.OPERATING,
  ScheduleType.CLOSED,
  ScheduleType.UNKNOWN,
] as const;

/**
 * Phase 2 — one subject holding several types for one day.
 * Priority: OPERATING > API-provided CLOSED > Gap-filled CLOSED > UNKNOWN.
 *
 * Runs after phase 1, so each (park, day, ride, type) is already down to one
 * row and the ranking only has to choose between the types.
 *
 * **Only the three day-status types take part** (`CONFLICT_TYPES`): OPERATING,
 * CLOSED and UNKNOWN each say what the day *is*, so two of them on one subject
 * contradict each other and one has to go. The other five — TICKETED_EVENT,
 * PRIVATE_EVENT, EXTRA_HOURS, MAINTENANCE and INFO — say what *else* happens
 * that day. A Halloween Horror Nights evening (TICKETED_EVENT, see
 * `ShowsService`) is neither an opening day nor a closure, and it belongs next
 * to the OPERATING row, not in place of it. They are left out of both the
 * ranking and the `EXISTS` pre-filter, so this phase never deletes them and
 * they never decide which day-status row survives. PAR-276.
 *
 * Before PAR-276 they were ranked `ELSE 4`, below UNKNOWN, and a day that also
 * carried an OPERATING row lost them on every pass; the schedule sync wrote
 * them back, and the next cleanup deleted them again.
 *
 * Duplicates among those five are still phase 1's job (same park, day, ride
 * and type).
 */
export function crossTypeConflictSql(scope: ScheduleDedupScope): string {
  const parkFilter = scope === "park" ? `AND e."parkId" = $1::uuid` : "";
  const types = CONFLICT_TYPES.map((t) => `'${t}'`).join(", ");

  return `
      WITH ranked AS (
        SELECT e.id,
               ROW_NUMBER() OVER (
                 PARTITION BY e."parkId", e.date, e."attractionId"
                 ORDER BY
                   CASE
                     WHEN e."scheduleType" = 'OPERATING' THEN 0
                     WHEN e."scheduleType" = 'CLOSED' AND e.description != 'Gap-filled' THEN 1
                     WHEN e."scheduleType" = 'CLOSED' THEN 2
                     WHEN e."scheduleType" = 'UNKNOWN' THEN 3
                   END,
                   e."updatedAt" DESC
               ) as rn
        FROM schedule_entries e
        WHERE e."scheduleType" IN (${types})
          AND EXISTS (
            SELECT 1
            FROM schedule_entries other
            WHERE other."parkId" = e."parkId"
              AND other.date = e.date
              AND other."attractionId" IS NOT DISTINCT FROM e."attractionId"
              AND other."scheduleType" IN (${types})
              AND other."scheduleType" <> e."scheduleType"
          )
        ${parkFilter}
      )
      DELETE FROM schedule_entries
      WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
    `;
}
