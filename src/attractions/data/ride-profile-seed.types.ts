/**
 * Shape of the curated ride-profile seed.
 *
 * Every id in `elements`, `types` and `manufacturerTermId` is a **glossary term
 * id** from the frontend glossary (`park.fan` → `lib/glossary/data.ts`). The
 * allowlist in `glossary-term-ids.ts` is checked against the seed by
 * `ride-profile-seed.spec.ts`, so a typo fails CI instead of silently
 * rendering a dead link on a ride page.
 */
/**
 * A ride's measurements, metric, as curated by hand.
 *
 * Metric only, and no unit suffixes to get wrong: a value entered here is
 * km/h, metres or seconds by the field name, and an American ride's published
 * feet and miles per hour are converted once, here, rather than at every
 * reader.
 *
 * Curate what the sources agree on and leave the rest out — an omitted field
 * falls through to the Wikidata import, a wrong one overrules it.
 */
export interface RideProfileSeedStats {
  /** Top speed in km/h. */
  topSpeedKmh?: number;
  /** Highest point above the ground in metres. */
  heightM?: number;
  /** Track length in metres. */
  lengthM?: number;
  /** Ride duration in seconds, station to station as the park counts it. */
  durationSeconds?: number;
}

export interface RideProfileSeedEntry {
  /** Park location key — park slugs are not globally unique. */
  citySlug: string;
  parkSlug: string;
  attractionSlug: string;

  /**
   * Track figures **in ride order**. Repeats are allowed and meaningful: a
   * layout that hits two corkscrews in a row lists `corkscrew` twice.
   * Omit entirely for rides without track figures (dark rides, flat rides).
   */
  elements?: string[];

  /** What kind of ride this is — `coasters` / `attractions` glossary terms. */
  types?: string[];

  /** Display name of the builder, e.g. "Bolliger & Mabillard". */
  manufacturer?: string;

  /** Glossary term id for the builder, when the glossary covers it. */
  manufacturerTermId?: string;

  /** The builder's own model name, e.g. "Blitz Coaster". */
  model?: string;

  /** Year the ride opened to the public. */
  openedYear?: number;

  /** Inversions as the park/manufacturer publishes them. */
  inversions?: number;

  /**
   * Speed, height, length and duration. Merged with the Wikidata import on
   * read, with these values winning field by field.
   */
  stats?: RideProfileSeedStats;
}
