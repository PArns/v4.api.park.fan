/**
 * The SQL counterpart of the question `isParkOpen()` asks first: **does this park's
 * schedule feed say anything about today?**
 *
 * `isParkOpen()` (common/utils/status-calculator.util) never sees a park's whole
 * schedule history — every caller hands it a bounded slice: today plus yesterday in
 * `ParksService.getBatchParkStatus`, the next 16 days in `ParkIntegrationService`. So
 * its "the park has a schedule, trust it and ignore the rides" branch really means
 * "the park published hours *for the days in question*".
 *
 * The two SQL re-implementations of that rule asked a different question — `EXISTS a
 * row with scheduleType = 'OPERATING'`, unbounded — which is "has this park EVER
 * published hours". Once true, always true, so a park whose feed went silent could
 * never reach the ride-based fallback again and was reported closed for as long as
 * the feed stayed down.
 *
 * Energylandia is what surfaced it: real hours through 2026-07-24, nothing but
 * `UNKNOWN` rows after that. The park page called it OPERATING (its 16-day window
 * held no OPERATING row, so it fell through to live ride data — 45 rides running,
 * Ø 45 min), while the country page, the card overlay and the homepage world panel
 * all called it closed off rows from May and July.
 *
 * ## What counts as "speaking for today"
 *
 * - **Park-level rows only.** Attraction-level rows (`attractionId IS NOT NULL`)
 *   describe one ride's hours, not the park's. Every other park-level query filters
 *   them out; the partial index `idx_schedule_park_date_no_attraction` exists for it.
 * - **Decisive types only.** `OPERATING` and `CLOSED` are statements about the day.
 *   `UNKNOWN` is the row the sync writes when it has a date but no hours — by the
 *   API's own contract (docs/frontend/calendar-schedule-status.md) that is "we don't
 *   know", so it must not silence the ride fallback. Callers that only care whether
 *   hours were published pass `['OPERATING']`.
 * - **Dated within a day of today.** `schedule_entries.date` is the park-local date
 *   while `CURRENT_DATE` is the server's, so the window has to be ±1 day to cover
 *   every timezone — UTC+14 is already on tomorrow's date when UTC-11 is still on
 *   yesterday's.
 */

/**
 * Half-width of the window, in days. ±1 covers the full UTC-11..UTC+14 spread
 * between a park's local date and the server's `CURRENT_DATE`.
 */
export const SCHEDULE_TODAY_WINDOW_DAYS = 1;

/** Row types that are a statement about the day rather than an absence of one. */
export const DECISIVE_SCHEDULE_TYPES = ["OPERATING", "CLOSED"] as const;

/**
 * Builds the WHERE-predicate for "this park-level schedule row speaks for today".
 *
 * @param alias - Table alias the `schedule_entries` row is bound to in the caller's SQL.
 * @param types - Row types that count. Defaults to {@link DECISIVE_SCHEDULE_TYPES};
 *                pass `['OPERATING']` to ask the narrower "were hours published".
 *
 * The caller supplies the `parkId` correlation — this is only the row filter, so it
 * drops into a CTE's WHERE and a correlated NOT EXISTS alike.
 */
export function scheduleRowSpeaksForToday(
  alias: string,
  types: readonly string[] = DECISIVE_SCHEDULE_TYPES,
): string {
  const typeList = types.map((t) => `'${t}'`).join(", ");
  return `${alias}."attractionId" IS NULL
          AND ${alias}."scheduleType" IN (${typeList})
          AND ${alias}.date BETWEEN CURRENT_DATE - ${SCHEDULE_TODAY_WINDOW_DAYS}
                                AND CURRENT_DATE + ${SCHEDULE_TODAY_WINDOW_DAYS}`;
}
