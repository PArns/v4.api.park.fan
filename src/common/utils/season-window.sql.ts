import { PARK_SEASON_BINDING_KINDS } from "../../parks/entities/park-season.entity";

/**
 * The SQL counterpart of `isCurrentlyInSeason(resolveCuratedFacts(a)) === false`.
 *
 * The TypeScript pair (`attractions/utils/curated-attraction-facts.util`) answers
 * this for anything that has an entity in hand. The park cards do not: their
 * "12/45 geöffnet" comes out of one CTE over every park in the catalogue
 * (`LIVE_STATS_SQL`), which never loads a row. Without a predicate here the two
 * surfaces disagree on the same sentence — Phantasialand's ice rink drops out of
 * the park page's counter in August and stays in the card's.
 *
 * ## The rule, and why it is shaped like this
 *
 * Seasonality resolves as a **pair**. A curated `is_seasonal` or a curated month
 * list takes the whole statement over, `season_out_since` included: the column is
 * the detector's note about when a ride was last seen running, and a human who
 * has written the season down is not to be contradicted by it.
 *
 * A park season that binds the ride (`seasonBindsAttraction`) sits between the
 * two: below a curated `false` or curated months, above everything the detector
 * wrote. Bound, the ride is seasonal and in season exactly on the season's days.
 *
 * Then three answers, and only the first two close a ride:
 *
 * - **Bound, or months on file** → a calendar question. Not one of the
 *   season's days, or not this month: not in season.
 * - **No months, but a `season_out_since`** → the detector flagged the ride for
 *   being shut on days the park was demonstrably open, and wrote down the last
 *   day it ran. That is "not now" without a claim about which months it does run.
 * - **Seasonal, and nothing else known** → unknown, which must NOT collapse into
 *   "closed". It would hide a ride we have simply not understood yet.
 *
 * ## Which clock
 *
 * `NOW()` read as the park's wall clock (`parks.timezone`, UTC when unset),
 * exactly like its twin, which reads the month in the park's timezone. Around a
 * month boundary the server's month is the wrong one for a park far from UTC
 * (PAR-1109); what matters as much is that the two agree, so they are changed
 * together or not at all.
 */

/**
 * Whether a `park_seasons` row binds an `attractions` row: same park, the ride
 * in `attraction_ids`, a binding kind, not cancelled. The one definition both
 * `Attraction.seasonBindings` and the predicates below read.
 *
 * `jsonb_build_array(<uuid>)` renders the id as the lowercase string the
 * season stores, so the uuid column is never cast to text.
 *
 * @param season - Alias of the `park_seasons` row.
 * @param alias - Alias of the `attractions` row.
 */
export function seasonBindsAttraction(season: string, alias: string): string {
  const kinds = PARK_SEASON_BINDING_KINDS.map((kind) => `'${kind}'`).join(", ");
  return `(${season}.park_id = ${alias}."parkId"
           AND ${season}.attraction_ids @> jsonb_build_array(${alias}.id)
           AND ${season}.kind IN (${kinds})
           AND ${season}.status <> 'cancelled')`;
}

/**
 * The seasons binding an attraction, as the jsonb list `SeasonBinding[]`
 * reads, ordered by start. `Attraction.seasonBindings` is this subquery.
 *
 * @param alias - Alias of the `attractions` row.
 */
export function attractionSeasonBindingsSql(alias: string): string {
  return `SELECT COALESCE(jsonb_agg(jsonb_build_object(
             'id', bound_season.id,
             'kind', bound_season.kind,
             'name', bound_season.name,
             'status', bound_season.status,
             'startDate', bound_season.start_date,
             'endDate', bound_season.end_date,
             'dates', bound_season.dates
           ) ORDER BY bound_season.start_date), '[]'::jsonb)
           FROM park_seasons bound_season
          WHERE ${seasonBindsAttraction("bound_season", alias)}`;
}

/** The SQL pieces of the precedence, shared by the predicates below. */
function seasonParts(alias: string) {
  const curatedMonths = `COALESCE(${alias}.curated_season_months, '[]'::jsonb)`;
  const hasCuratedMonths = `jsonb_array_length(${curatedMonths}) > 0`;
  // A curated statement exists as soon as EITHER half is filled in.
  const curated = `(${alias}.curated_is_seasonal IS NOT NULL OR ${hasCuratedMonths})`;
  // The bound season decides unless the ride's own curation overrides it: a
  // curated `false` or curated months. A curated `true` alone does not, it
  // agrees and leaves the window to the season.
  const bound = `(NOT (${alias}.curated_is_seasonal IS FALSE OR ${hasCuratedMonths})
           AND EXISTS (SELECT 1 FROM park_seasons bound_season
                        WHERE ${seasonBindsAttraction("bound_season", alias)}))`;
  // The flag as it resolved before bindings existed, for the branch where no
  // season binds the ride.
  const unboundIsSeasonal = `CASE
             WHEN ${curated}
               THEN COALESCE(${alias}.curated_is_seasonal, ${hasCuratedMonths})
             ELSE COALESCE(${alias}.is_seasonal, FALSE)
           END`;
  return { hasCuratedMonths, curated, bound, unboundIsSeasonal };
}

/**
 * The resolved `isSeasonal` of `resolveCuratedFacts`: curated first, then a
 * bound season, then the detector.
 *
 * @param alias - Alias of the `attractions` row.
 */
export function attractionIsSeasonal(alias: string): string {
  const { curated, bound, unboundIsSeasonal } = seasonParts(alias);
  // A curated statement answers first; past it, a binding makes the ride
  // seasonal whatever the detector wrote.
  return `(CASE
             WHEN NOT ${curated} AND ${bound} THEN TRUE
             ELSE ${unboundIsSeasonal}
           END)`;
}

/**
 * Whether a bound park season decides this ride's season, i.e. the resolved
 * `seasonBindings` is not empty.
 *
 * @param alias - Alias of the `attractions` row.
 */
export function attractionIsSeasonBound(alias: string): string {
  return seasonParts(alias).bound;
}

/**
 * Builds the boolean expression "this attraction's season says it cannot be open".
 *
 * Drops into a SELECT list or a WHERE alike. The caller supplies the alias the
 * `attractions` row is bound to; nothing else is correlated.
 *
 * @param alias - Table alias the `attractions` row carries in the caller's SQL.
 */
export function attractionIsOutOfSeason(alias: string): string {
  const { hasCuratedMonths, curated, bound, unboundIsSeasonal } =
    seasonParts(alias);

  // Curated months win where they exist; otherwise the detector's, which a
  // curated `is_seasonal: true` with no months of its own still leans on.
  const months = `COALESCE(
             CASE WHEN ${hasCuratedMonths} THEN ${alias}.curated_season_months
                  ELSE ${alias}.season_months END,
             '[]'::jsonb
           )`;

  // The park's wall clock, not the server's. A PK lookup per row; correlated
  // so no caller has to join `parks` for it.
  const parkNow = `NOW() AT TIME ZONE COALESCE(
             (SELECT p.timezone FROM parks p WHERE p.id = ${alias}."parkId"),
             'UTC'
           )`;
  const parkMonth = `EXTRACT(MONTH FROM ${parkNow})::int`;
  const parkToday = `(${parkNow})::date`;

  // One of the bound seasons runs today: inside its bounds, and on its list of
  // days when it has one.
  const boundToday = `EXISTS (SELECT 1 FROM park_seasons bound_season
                        WHERE ${seasonBindsAttraction("bound_season", alias)}
                          AND bound_season.start_date <= ${parkToday}
                          AND bound_season.end_date >= ${parkToday}
                          AND (bound_season.dates IS NULL
                               OR bound_season.dates @> to_jsonb((${parkToday})::text)))`;

  return `(CASE
           WHEN ${bound} THEN NOT ${boundToday}
           ELSE (${unboundIsSeasonal})
             AND CASE
                   WHEN jsonb_array_length(${months}) > 0
                     THEN NOT (${months} @> to_jsonb(${parkMonth}))
                   ELSE NOT ${curated} AND ${alias}.season_out_since IS NOT NULL
                 END
         END)`;
}
