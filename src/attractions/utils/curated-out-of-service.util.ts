import { getCurrentDateInTimezone } from "../../common/utils/date.util";

/**
 * The hand-written "this ride is in a works period from … to …" window.
 *
 * The feed cannot tell a breakdown from a rebuild. ThemeParks.wiki passes
 * `REFURBISHMENT` through with no start, no end and no announcement, and it does
 * not use it for every closure that is really planned work — a long rebuild seen
 * from outside looks exactly like a ride that keeps failing. Without this the
 * only guard is the reconstruction's seven-day bound, which lets a six-day
 * rebuild through as an outage and says nothing at all about the live line.
 *
 * So an editor may state the window under `/admin/attractions/<id>`, and inside
 * it nothing is reported: no live "Störung gemeldet seit", and no reconstructed
 * outage once that exists. It is a statement about the ride, not a correction of
 * a synced column, so it has no upstream half and no two-writers problem.
 *
 * ## Which clock
 *
 * The park's. An editor writing "16 January to 3 March" means the park's
 * calendar, and a UTC comparison would open and close the window at the wrong
 * moment for every park west of Greenwich. Both bounds are **inclusive**: a
 * window that ends on the 3rd covers the 3rd.
 *
 * ## Half-open windows
 *
 * Either bound may stand alone. `from` with no `to` is the usual case while work
 * is running and nobody has been told when it ends. `to` with no `from` is a
 * window that was already open when somebody wrote it down. Both empty is the
 * normal state and means nothing is claimed.
 */
export interface CuratedOutOfServiceSource {
  curatedOutOfServiceFrom?: string | null;
  curatedOutOfServiceTo?: string | null;
}

/**
 * Whether a curated works period covers the given park-local day.
 *
 * @param source - The attraction's two curated date columns.
 * @param timezone - The park's IANA timezone.
 * @param onDate - Park-local `YYYY-MM-DD` to test. Defaults to the park's today.
 */
export function isCuratedOutOfService(
  source: CuratedOutOfServiceSource,
  timezone: string,
  onDate?: string,
): boolean {
  const from = source.curatedOutOfServiceFrom ?? null;
  const to = source.curatedOutOfServiceTo ?? null;
  if (!from && !to) return false;

  const day = onDate ?? safeToday(timezone);
  if (!day) return false;

  // Both columns are `date`, so a lexicographic compare on YYYY-MM-DD is a
  // calendar compare. No Date objects: constructing one would drag the server's
  // offset back into a park-local question.
  if (from && day < from) return false;
  if (to && day > to) return false;
  return true;
}

/**
 * SQL twin of {@link isCuratedOutOfService}, for the reconstruction and for any
 * catalogue-wide query that never loads a row.
 *
 * Hand-maintained beside its TypeScript half in the pattern
 * `attractionIsOutOfSeason()` set, and changed with it or not at all.
 *
 * Unlike that predicate this one takes the day as a parameter rather than
 * reading `NOW()`. A works period is a fact about a date range, so asking it
 * about a past day is meaningful and has to give the answer that day had.
 * `attractionIsOutOfSeason()` computes against `EXTRACT(MONTH FROM NOW())`,
 * which is why it may not be used on history at all: applied to 90 days it
 * deletes the July of a ride that leaves season in September.
 *
 * @param alias - Table alias the `attractions` row carries in the caller's SQL.
 * @param dayExpr - SQL expression yielding the park-local `date` to test.
 */
export function attractionIsCuratedOutOfService(
  alias: string,
  dayExpr: string,
): string {
  return `(
           (${alias}.curated_out_of_service_from IS NOT NULL
            OR ${alias}.curated_out_of_service_to IS NOT NULL)
           AND (${alias}.curated_out_of_service_from IS NULL
                OR ${dayExpr} >= ${alias}.curated_out_of_service_from)
           AND (${alias}.curated_out_of_service_to IS NULL
                OR ${dayExpr} <= ${alias}.curated_out_of_service_to)
         )`;
}

/**
 * A bad timezone must not cost the caller its whole response.
 *
 * This runs inside the park page's ride loop, so an unguarded throw would take
 * every ride's status with it for the sake of one malformed column.
 */
function safeToday(timezone: string): string | null {
  try {
    return getCurrentDateInTimezone(timezone);
  } catch {
    return null;
  }
}
