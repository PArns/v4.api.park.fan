/**
 * Merges the hand-curated attraction facts over the synced ones.
 *
 * Every fact here lives in two columns: the one a sync owns and overwrites on
 * every run, and the one only a human writes. A correction put in the synced
 * cell survives until the next poll, so it has to sit beside it — and the read
 * side has to know that.
 *
 * It is a function rather than a handful of `??` expressions because those
 * expressions were already copied into both DTO mappers, and a rule kept in two
 * places is a rule that drifts. The free-flow status flag drifted exactly that
 * way and shipped a bug. Every new curated column belongs in here, not at the
 * call site.
 */

export interface CuratedFactsSource {
  name?: string | null;
  curatedName?: string | null;
  landName?: string | null;
  curatedLandName?: string | null;
  attractionType?: string | null;
  curatedAttractionType?: string | null;
  minimumHeight?: number | null;
  curatedMinimumHeight?: number | null;
  minimumHeightUnit?: "cm" | "in" | null;
  maximumHeight?: number | null;
  curatedMaximumHeight?: number | null;
  mayGetWet?: boolean | null;
  curatedMayGetWet?: boolean | null;
  isSeasonal?: boolean | null;
  curatedIsSeasonal?: boolean | null;
  seasonMonths?: number[] | null;
  curatedSeasonMonths?: number[] | null;
  /** The last day the detector saw this ride OPERATING. See the entity. */
  seasonOutSince?: string | null;
}

export interface ResolvedCuratedFacts {
  name: string;
  landName: string | null;
  attractionType: string | null;
  minimumHeight: number | null;
  minimumHeightUnit: "cm" | "in" | null;
  maximumHeight: number | null;
  mayGetWet: boolean | null;
  isSeasonal: boolean;
  seasonMonths: number[] | null;
  /**
   * The last day this ride was seen running, when the detector flagged it.
   *
   * Null for a curated seasonality — a human writing "this is seasonal" makes
   * no claim about when it last ran, and the detector's date belongs to the
   * detector's verdict.
   */
  seasonOutSince: string | null;

  /**
   * Whether the resolved seasonality came from a human.
   *
   * The detector rewrites its own columns nightly, so "a person disagreed with
   * the detector" is a fact worth keeping visible — the admin renders it as an
   * override badge, and it is how somebody notices that a curation is now
   * stale because the ride reopened.
   */
  seasonalityCurated: boolean;
}

/**
 * A curated height of 0 means "no minimum/maximum at all", not a 0 cm limit.
 *
 * It is not really a sentinel — a 0 cm minimum excludes nobody — and it is how
 * a correction says "upstream's number is wrong and the truth is none".
 * Phantasialand's Winni Splash is the worked example: the wiki publishes 100,
 * while the park's own Nutzungsbedingungen say children under 1.00 m may play
 * *when accompanied*, which is no minimum at all.
 */
function resolveHeight(
  curated: number | null | undefined,
  synced: number | null | undefined,
): number | null {
  if (curated !== null && curated !== undefined) {
    return curated > 0 ? curated : null;
  }
  return synced ?? null;
}

/** Trimmed, or null when the curated string is absent or only whitespace. */
function cleaned(value: string | null | undefined): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/**
 * Every hand-written column on an attraction row, as physical column names.
 *
 * The park side has had this since the merge learned to carry curation across
 * (`CURATED_PARK_COLUMNS`); this is the same list for the other half, and it
 * exists for the same reason a list beats a hand-typed WHERE clause: the
 * "uncurated" filter in the admin covered four of fifteen park columns for a
 * while, so parks with a website, an address and a phone number kept being
 * handed to the next editor as untouched.
 */
export const ATTRACTION_CURATED_DB_COLUMNS: readonly string[] = [
  "curated_name",
  "curated_land_name",
  "curated_attraction_type",
  "curated_minimum_height",
  "curated_maximum_height",
  "curated_may_get_wet",
  "curated_is_seasonal",
  "curated_season_months",
  // Not `curated_`-prefixed because no sync owns them, but written by hand and
  // by hand only — a ride carrying one has been looked at.
  "has_fast_pass",
  "fast_pass_name",
  "fast_pass_price",
];

/*
 * Three hand-editable columns are deliberately NOT on that list:
 * `has_single_rider` (seeded from every attraction that ever reported a
 * SINGLE_RIDER queue), `rcdb_id` (a Wikidata import) and `open_with_park`
 * (NOT NULL, so every row holds a value). They were filled in bulk rather than
 * by an editor, and counting them would report thousands of rides as curated
 * that nobody has ever looked at.
 */

export function resolveCuratedFacts(
  attraction: CuratedFactsSource,
): ResolvedCuratedFacts {
  const minimumHeight = resolveHeight(
    attraction.curatedMinimumHeight,
    attraction.minimumHeight,
  );

  // Seasonality is resolved as a pair rather than field by field, because the
  // two halves are one statement. A curated `false` ("the detector is wrong,
  // this ride is not seasonal") must take the months down with it, or the API
  // serves a not-seasonal ride carrying a list of the months it operates in.
  const curatedIsSeasonal = attraction.curatedIsSeasonal;
  const curatedMonths = attraction.curatedSeasonMonths;
  const seasonalityCurated =
    (curatedIsSeasonal !== null && curatedIsSeasonal !== undefined) ||
    (Array.isArray(curatedMonths) && curatedMonths.length > 0);

  const isSeasonal = seasonalityCurated
    ? (curatedIsSeasonal ??
      (Array.isArray(curatedMonths) && curatedMonths.length > 0))
    : (attraction.isSeasonal ?? false);

  const seasonMonths = !isSeasonal
    ? null
    : Array.isArray(curatedMonths) && curatedMonths.length > 0
      ? curatedMonths
      : (attraction.seasonMonths ?? null);

  return {
    seasonOutSince: seasonalityCurated
      ? null
      : (attraction.seasonOutSince ?? null),
    // `name` is the one field with no meaningful null: a ride always has a
    // name, and the curated column only ever replaces it.
    name: cleaned(attraction.curatedName) ?? attraction.name ?? "",
    landName:
      cleaned(attraction.curatedLandName) ?? attraction.landName ?? null,
    attractionType:
      cleaned(attraction.curatedAttractionType) ??
      attraction.attractionType ??
      null,
    minimumHeight,
    // The unit only describes a number. Carrying "cm" next to a null height
    // would leave a ride page rendering a bare unit. When curation supplies a
    // height the sync never saw, nothing recorded a unit either — and every
    // curated figure so far is the metric one off the park's own sign.
    minimumHeightUnit:
      minimumHeight === null ? null : (attraction.minimumHeightUnit ?? "cm"),
    maximumHeight: resolveHeight(
      attraction.curatedMaximumHeight,
      attraction.maximumHeight,
    ),
    mayGetWet: attraction.curatedMayGetWet ?? attraction.mayGetWet ?? null,
    isSeasonal,
    seasonMonths,
    seasonalityCurated,
  };
}

/**
 * Whether a resolved seasonality says the ride is running this month.
 *
 * Three answers, and the third is the point. With months, it is a calendar
 * question. Without months but with a `seasonOutSince`, it is a different
 * question with a different source: the detector flagged this ride because it
 * has been fully closed on days the park was demonstrably open, and it wrote
 * down when it last ran. That says "not now" without claiming to know which
 * months it does run — and "not now" is what a ride list needs in order to
 * stop showing an ice rink in August.
 *
 * Null stays for the case it was written for: seasonal, and nothing else known.
 * That must not collapse into "not running", which would hide a ride we have
 * simply not understood yet.
 */
export function isCurrentlyInSeason(
  facts: Pick<
    ResolvedCuratedFacts,
    "isSeasonal" | "seasonMonths" | "seasonOutSince"
  >,
  now: Date = new Date(),
): boolean | null {
  if (!facts.isSeasonal) return null;
  if (facts.seasonMonths && facts.seasonMonths.length > 0) {
    return facts.seasonMonths.includes(now.getMonth() + 1);
  }
  return facts.seasonOutSince ? false : null;
}
