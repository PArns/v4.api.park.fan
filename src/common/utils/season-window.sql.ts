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
 * Then three answers, and only the first two close a ride:
 *
 * - **Months on file** → a calendar question. Not this month, not in season.
 * - **No months, but a `season_out_since`** → the detector flagged the ride for
 *   being shut on days the park was demonstrably open, and wrote down the last
 *   day it ran. That is "not now" without a claim about which months it does run.
 * - **Seasonal, and nothing else known** → unknown, which must NOT collapse into
 *   "closed". It would hide a ride we have simply not understood yet.
 *
 * ## Which clock
 *
 * `NOW()`, the server's, exactly like the `new Date()` its twin defaults to.
 * Park-local would be more correct for both and is wrong for at most a few hours
 * a year at a month boundary; what matters far more is that the two agree, so
 * they are changed together or not at all.
 */

/**
 * Builds the boolean expression "this attraction's season says it cannot be open".
 *
 * Drops into a SELECT list or a WHERE alike. The caller supplies the alias the
 * `attractions` row is bound to; nothing else is correlated.
 *
 * @param alias - Table alias the `attractions` row carries in the caller's SQL.
 */
export function attractionIsOutOfSeason(alias: string): string {
  const curatedMonths = `COALESCE(${alias}.curated_season_months, '[]'::jsonb)`;
  const hasCuratedMonths = `jsonb_array_length(${curatedMonths}) > 0`;
  // A curated statement exists as soon as EITHER half is filled in.
  const curated = `(${alias}.curated_is_seasonal IS NOT NULL OR ${hasCuratedMonths})`;

  const isSeasonal = `CASE
             WHEN ${curated}
               THEN COALESCE(${alias}.curated_is_seasonal, ${hasCuratedMonths})
             ELSE COALESCE(${alias}.is_seasonal, FALSE)
           END`;

  // Curated months win where they exist; otherwise the detector's, which a
  // curated `is_seasonal: true` with no months of its own still leans on.
  const months = `COALESCE(
             CASE WHEN ${hasCuratedMonths} THEN ${alias}.curated_season_months
                  ELSE ${alias}.season_months END,
             '[]'::jsonb
           )`;

  return `(
           (${isSeasonal})
           AND CASE
                 WHEN jsonb_array_length(${months}) > 0
                   THEN NOT (${months} @> to_jsonb(EXTRACT(MONTH FROM NOW())::int))
                 ELSE NOT ${curated} AND ${alias}.season_out_since IS NOT NULL
               END
         )`;
}
