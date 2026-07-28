/**
 * Shape of the curated ride-profile seed.
 *
 * Every id in `elements`, `types` and `manufacturerTermId` is a **glossary term
 * id** from the frontend glossary (`park.fan` → `lib/glossary/data.ts`). The
 * allowlist in `glossary-term-ids.ts` is checked against the seed by
 * `ride-profile-seed.spec.ts`, so a typo fails CI instead of silently
 * rendering a dead link on a ride page.
 */
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
}
