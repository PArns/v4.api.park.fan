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
 * The same window as it leaves the API, plus how firm its end is.
 *
 * Served as `worksPeriod`, next to and never inside `outage`: an outage is a
 * fault somebody is reporting right now, this is planned work somebody wrote
 * down in advance, and a client that folds them together tells a visitor the
 * ride broke when it is being rebuilt. The two live side by side on purpose.
 *
 * The keys drop the `curated` prefix the columns carry. That prefix is a
 * statement about where the value comes from, which is the storage layer's
 * question — no field this API serves has ever carried it, and a reader of the
 * ride page has no use for the distinction.
 */
export interface WorksPeriod {
  /** First park-local day, inclusive, or null for a window with no start. */
  from: string | null;
  /** Last park-local day, inclusive, or null while nobody knows when it ends. */
  to: string | null;
  /** Whether `to` is our estimate rather than a date the park published. */
  toUncertain: boolean;
}

export interface WorksPeriodSource extends CuratedOutOfServiceSource {
  curatedOutOfServiceToUncertain?: boolean | null;
}

/**
 * The curated works period for the API, or null when nothing is curated.
 *
 * Null rather than a block of nulls: "no works period" is the state of nearly
 * every ride in the catalogue, and an object saying so on each of them is
 * payload on every park page for a fact that is almost always absent.
 *
 * Two normalisations, both so a client cannot render a half-statement:
 *
 * - `toUncertain` is false whenever `to` is null. An adverb needs a date to
 *   qualify, and the pair (`to: null`, `toUncertain: true`) is a curation
 *   leftover from clearing the end date and not clearing the flag.
 * - `null` on the column reads as false here. Three states are what the EDITOR
 *   needs — "nobody has looked" is worth seeing in the form — but a page has
 *   two renderings, "until 3 March" and "probably until 3 March", and an
 *   unchecked date gets the plain one.
 *
 * This deliberately says nothing about whether the window covers today.
 * `isCuratedOutOfService()` answers that, it needs the park's timezone, and a
 * client comparing these dates against the reader's own clock would open the
 * window at the wrong moment for every park outside the reader's zone.
 */
export function resolveWorksPeriod(
  source: WorksPeriodSource,
): WorksPeriod | null {
  const from = source.curatedOutOfServiceFrom ?? null;
  const to = source.curatedOutOfServiceTo ?? null;
  if (!from && !to) return null;

  return {
    from,
    to,
    toUncertain: to !== null && source.curatedOutOfServiceToUncertain === true,
  };
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
