import type { ParkWithAttractionsDto } from "../parks/dto/park-with-attractions.dto";
import { roundToNearest5Minutes } from "../common/utils/wait-time.utils";
import type { NearbyShortWaitDto } from "./dto/nearby-response.dto";

/** At most this many rides in `shortWaits`, across all nearby parks. */
export const SHORT_WAITS_LIMIT = 5;

/** At most this many from one park, so the nearest park cannot fill the list alone. */
export const SHORT_WAITS_PER_PARK = 3;

/**
 * A ride whose usual wait is below this has no queue worth skipping: „5 instead
 * of 10" is noise on a card that says where a wait is worth taking.
 */
export const SHORT_WAIT_MIN_TYPICAL = 15;

/** The live wait has to be at least this many minutes below the usual one. */
export const SHORT_WAIT_MIN_SAVING = 10;

/**
 * The two ratings `getComparisonText` gives a wait at or below 60 % of the
 * baseline, so the list agrees with the badge the park page shows for the ride.
 */
const SHORT_COMPARISONS = new Set(["much_lower", "lower"]);

/** A nearby park with its cached live payload, as `findNearbyParks` has it. */
export interface ShortWaitCandidatePark {
  /** The park's `park:integrated` payload. */
  integrated: ParkWithAttractionsDto;
  /** The curated park name, the one the park card shows. */
  name: string;
  /** Distance from the user in meters, rounded. */
  distance: number;
}

/**
 * The rides in the open nearby parks whose live wait is well below their usual
 * one, the biggest gap first.
 *
 * Reads only the cached park payloads, whose `baseline` and `comparison` the
 * park page uses too; a park without one is skipped rather than queried, so
 * the endpoint costs no extra round trip.
 */
export function pickShortWaits(
  parks: ShortWaitCandidatePark[],
): NearbyShortWaitDto[] {
  const candidates: NearbyShortWaitDto[] = [];

  for (const { integrated, name, distance } of parks) {
    if (integrated.status !== "OPERATING") continue;
    if (integrated.liveWaitTimes?.available === false) continue;

    for (const attraction of integrated.attractions ?? []) {
      if (attraction.effectiveStatus !== "OPERATING") continue;
      if (attraction.isCurrentlyInSeason === false) continue;
      if (
        !attraction.comparison ||
        !SHORT_COMPARISONS.has(attraction.comparison)
      ) {
        continue;
      }
      const rawWait = attraction.queues?.[0]?.waitTime;
      const baseline = attraction.baseline;
      if (typeof rawWait !== "number" || typeof baseline !== "number") continue;

      const waitTime = roundToNearest5Minutes(rawWait);
      // The baseline is a median, not a posted wait, so the walk-on exception of
      // roundToNearest5Minutes does not apply to it.
      const typicalWait = Math.round(baseline / 5) * 5;
      if (typicalWait < SHORT_WAIT_MIN_TYPICAL) continue;
      if (typicalWait - waitTime < SHORT_WAIT_MIN_SAVING) continue;

      candidates.push({
        id: attraction.id,
        name: attraction.name,
        slug: attraction.slug,
        url: attraction.url ?? null,
        waitTime,
        typicalWait,
        isHeadliner: attraction.isHeadliner === true,
        park: {
          id: integrated.id,
          name,
          slug: integrated.slug,
          distance,
          url: integrated.url ?? null,
        },
      });
    }
  }

  candidates.sort(
    (a, b) =>
      b.typicalWait - b.waitTime - (a.typicalWait - a.waitTime) ||
      Number(b.isHeadliner) - Number(a.isHeadliner) ||
      a.park.distance - b.park.distance,
  );

  const perPark = new Map<string, number>();
  const picked: NearbyShortWaitDto[] = [];
  for (const ride of candidates) {
    const taken = perPark.get(ride.park.id) ?? 0;
    if (taken >= SHORT_WAITS_PER_PARK) continue;
    perPark.set(ride.park.id, taken + 1);
    picked.push(ride);
    if (picked.length === SHORT_WAITS_LIMIT) break;
  }
  return picked;
}
