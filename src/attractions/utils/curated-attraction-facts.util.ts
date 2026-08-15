/**
 * Merges the hand-curated attraction facts over the synced ones.
 *
 * Height and the wet flag each live in two columns: the one the ThemeParks.wiki
 * detail sync owns, and the one only a human writes. The sync overwrites its
 * own cell on every run, so a correction has to sit beside it rather than in
 * it — and the read side has to know that.
 *
 * It is a function rather than two `??` expressions because those `??`
 * expressions were already copied into both DTO mappers, and a rule kept in two
 * places is a rule that drifts. The free-flow status flag drifted exactly that
 * way and shipped a bug.
 */

export interface CuratedFactsSource {
  minimumHeight?: number | null;
  curatedMinimumHeight?: number | null;
  minimumHeightUnit?: "cm" | "in" | null;
  mayGetWet?: boolean | null;
  curatedMayGetWet?: boolean | null;
}

export interface ResolvedCuratedFacts {
  minimumHeight: number | null;
  minimumHeightUnit: "cm" | "in" | null;
  mayGetWet: boolean | null;
}

export function resolveCuratedFacts(
  attraction: CuratedFactsSource,
): ResolvedCuratedFacts {
  const curated = attraction.curatedMinimumHeight;

  // 0 is a curated "no minimum height", not a 0 cm limit — it is how a
  // correction overrides an upstream number with nothing at all.
  const minimumHeight =
    curated !== null && curated !== undefined
      ? curated > 0
        ? curated
        : null
      : (attraction.minimumHeight ?? null);

  return {
    minimumHeight,
    // The unit only describes a number. Carrying "cm" next to a null height
    // would leave a ride page rendering a bare unit. When curation supplies a
    // height the sync never saw, nothing recorded a unit either — and every
    // curated figure so far is the metric one off the park's own sign.
    minimumHeightUnit:
      minimumHeight === null ? null : (attraction.minimumHeightUnit ?? "cm"),
    mayGetWet: attraction.curatedMayGetWet ?? attraction.mayGetWet ?? null,
  };
}
