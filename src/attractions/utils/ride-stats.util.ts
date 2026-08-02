import type {
  RideMeasurements,
  RideStats,
} from "../entities/attraction-ride-profile.entity";

const FIELDS = [
  "topSpeedKmh",
  "heightM",
  "lengthM",
  "durationSeconds",
] as const satisfies readonly (keyof RideMeasurements)[];

/**
 * Merge the hand-curated measurements with the ones imported from Wikidata.
 *
 * Field by field, not whole-record: a ride can have a curated top speed —
 * because Wikidata states none — and an imported duration, and dropping either
 * one to keep the record from a single source would serve less than we know.
 *
 * Curated wins where both exist. That is not a claim that hand work beats
 * automation in general, it is that these particular values were cross-checked
 * against several sources before being written, while the import takes whatever
 * the entity happens to say — including, in a handful of cases we found, a
 * ride's length entered as its height.
 *
 * `source` describes the result, so a consumer can tell a curated number from
 * an imported one, and `sourceId` is dropped when no imported value survived
 * the merge — pointing at a Wikidata entity none of the numbers came from
 * would be a false citation.
 */
export function mergeRideStats(
  curated: RideMeasurements | null | undefined,
  imported: RideStats | null | undefined,
): RideStats | null {
  if (!curated && !imported) return null;

  const merged: RideStats = {
    topSpeedKmh: null,
    heightM: null,
    lengthM: null,
    durationSeconds: null,
    source: "curated",
    sourceId: null,
  };

  let usedCurated = false;
  let usedImported = false;

  for (const field of FIELDS) {
    const own = curated?.[field];
    const theirs = imported?.[field];
    if (typeof own === "number") {
      merged[field] = own;
      usedCurated = true;
    } else if (typeof theirs === "number") {
      merged[field] = theirs;
      usedImported = true;
    }
  }

  if (!usedCurated && !usedImported) return null;

  merged.source =
    usedCurated && usedImported
      ? "mixed"
      : usedCurated
        ? "curated"
        : "wikidata";
  merged.sourceId = usedImported ? (imported?.sourceId ?? null) : null;

  return merged;
}
