import { mapRideProfile, type RideProfileDto } from "../dto/ride-profile.dto";
import type { Attraction } from "../entities/attraction.entity";
import type { AttractionRideProfile } from "../entities/attraction-ride-profile.entity";
import { resolveCuratedFacts } from "./curated-attraction-facts.util";
import {
  chooseNameDuplicateWinner,
  nameDuplicateKey,
} from "../../common/utils/name-duplicate.util";

/**
 * The track figures that are launches. park.fan's ride page counts the same
 * two (`LAUNCH_ELEMENT_IDS` in the frontend's `lib/glossary/ride-profile.ts`):
 * a lift hill or a first drop also gets a train going, but it is not a launch,
 * and counting it would make every coaster a multi-launch.
 */
export const LAUNCH_ELEMENT_IDS: ReadonlySet<string> = new Set([
  "launch",
  "swing-launch",
]);

/**
 * A ride's curated profile, flattened for a reader with no use for nulls: a
 * field is there when park.fan knows it and absent when it does not.
 */
export interface RideFacts {
  /** Glossary term ids of the ride type, e.g. `launch-coaster`. */
  types?: string[];
  /** Glossary term ids of the track figures, in ride order. */
  elements?: string[];
  launches?: number;
  inversions?: number;
  manufacturer?: string;
  model?: string;
  /** The year it opened to the public. */
  openedYear?: number;
  topSpeedKmh?: number;
  heightM?: number;
  lengthM?: number;
  durationSeconds?: number;
}

/**
 * The facts of a curated profile, or null for a ride nobody has profiled.
 *
 * Launches are counted off the track figures, and only when figures were
 * curated at all: an empty list means "not written down", so it gives no
 * count rather than a confident zero.
 */
export function rideFacts(
  profile: RideProfileDto | null | undefined,
): RideFacts | null {
  if (!profile) return null;
  const elements = profile.elements ?? [];
  const facts: RideFacts = {
    types: profile.types?.length ? profile.types : undefined,
    elements: elements.length > 0 ? elements : undefined,
    launches:
      elements.length > 0
        ? elements.filter((id) => LAUNCH_ELEMENT_IDS.has(id)).length
        : undefined,
    inversions: profile.inversions ?? undefined,
    manufacturer: profile.manufacturer ?? undefined,
    model: profile.model ?? undefined,
    openedYear: profile.openedYear ?? undefined,
    topSpeedKmh: profile.stats?.topSpeedKmh ?? undefined,
    heightM: profile.stats?.heightM ?? undefined,
    lengthM: profile.stats?.lengthM ?? undefined,
    durationSeconds: profile.stats?.durationSeconds ?? undefined,
  };
  const known = Object.fromEntries(
    Object.entries(facts).filter(([, value]) => value !== undefined),
  ) as RideFacts;
  return Object.keys(known).length > 0 ? known : null;
}

/** What a park's rides can be ranked by. */
export const RIDE_RANKINGS = [
  "topSpeed",
  "height",
  "length",
  "duration",
  "inversions",
  "launches",
  "openedYear",
  "minimumHeight",
] as const;

export type RideRanking = (typeof RIDE_RANKINGS)[number];

/** One ride of a park with what is known about it, for {@link compareRides}. */
export interface ComparedRide extends Omit<RideFacts, "elements" | "model"> {
  id: string;
  name: string;
  /** Minimum rider height in cm; absent when park.fan does not know one. */
  minimumHeightCm?: number;
  maximumHeightCm?: number;
  /** "in" where the park signs the height in inches. */
  heightUnit?: "in";
}

/**
 * A park's attractions as rows to compare, one per name as the park page
 * serves them (`chooseNameDuplicateWinner`), with the curated heights and the
 * profile's facts. The track figures and the model stay out: a ranking never
 * needs them, and `get_attraction` has them for the one ride asked about.
 */
export function comparedRides(
  attractions: readonly Attraction[],
  profiles: ReadonlyMap<string, AttractionRideProfile>,
): ComparedRide[] {
  const groups = new Map<
    string,
    Array<{ slug: string; name: string; attraction: Attraction }>
  >();
  for (const attraction of attractions) {
    const name = nameDuplicateKey(resolveCuratedFacts(attraction).name);
    if (!name) continue;
    const row = { slug: attraction.slug, name, attraction };
    groups.set(name, [...(groups.get(name) ?? []), row]);
  }
  return [...groups.values()].map((rows) => {
    const { attraction } = chooseNameDuplicateWinner(rows)!;
    const facts = resolveCuratedFacts(attraction);
    const profile = rideFacts(mapRideProfile(profiles.get(attraction.id)));
    const { elements: _elements, model: _model, ...known } = profile ?? {};
    return {
      id: attraction.id,
      name: facts.name,
      ...(facts.minimumHeight !== null && {
        minimumHeightCm: facts.minimumHeight,
      }),
      ...(facts.maximumHeight !== null && {
        maximumHeightCm: facts.maximumHeight,
      }),
      ...(facts.minimumHeightUnit === "in" && { heightUnit: "in" as const }),
      ...known,
    };
  });
}

const VALUE: Record<RideRanking, (ride: ComparedRide) => number | undefined> = {
  topSpeed: (ride) => ride.topSpeedKmh,
  height: (ride) => ride.heightM,
  length: (ride) => ride.lengthM,
  duration: (ride) => ride.durationSeconds,
  inversions: (ride) => ride.inversions,
  launches: (ride) => ride.launches,
  openedYear: (ride) => ride.openedYear,
  minimumHeight: (ride) => ride.minimumHeightCm,
};

/** The result of {@link compareRides}. */
export interface RideComparison {
  /** The first `limit` rides, in order. */
  rides: ComparedRide[];
  /** Rides that had the value and passed the height filter. */
  ranked: number;
  /** Rides left out because park.fan does not know the value ranked by. */
  withoutValue: number;
  /** With a rider height: rides left out because their minimum height is unknown. */
  heightUnknown?: number;
  /** With a rider height: rides the rider is too small or too tall for. */
  notAllowed?: number;
}

/**
 * A park's rides ranked by one fact, highest first unless `order` is "asc".
 *
 * With `riderHeightCm` only the rides a rider that tall may ride are ranked:
 * the minimum height must be known and met, and a maximum, where there is one,
 * not exceeded. A ride with no known minimum is counted apart rather than
 * passed, because an unknown limit is not the same as none.
 */
export function compareRides(
  rides: readonly ComparedRide[],
  params: {
    by: RideRanking;
    order?: "asc" | "desc";
    riderHeightCm?: number;
    limit: number;
  },
): RideComparison {
  const rider = params.riderHeightCm;
  let heightUnknown = 0;
  let notAllowed = 0;
  const allowed =
    rider === undefined
      ? rides
      : rides.filter((ride) => {
          if (ride.minimumHeightCm === undefined) {
            heightUnknown++;
            return false;
          }
          const fits =
            ride.minimumHeightCm <= rider &&
            (ride.maximumHeightCm === undefined ||
              ride.maximumHeightCm >= rider);
          if (!fits) notAllowed++;
          return fits;
        });

  const value = VALUE[params.by];
  const withValue = allowed.filter((ride) => value(ride) !== undefined);
  const sign = params.order === "asc" ? 1 : -1;
  const ranked = [...withValue].sort(
    (a, b) =>
      sign * ((value(a) as number) - (value(b) as number)) ||
      a.name.localeCompare(b.name),
  );

  return {
    rides: ranked.slice(0, params.limit),
    ranked: ranked.length,
    withoutValue: allowed.length - withValue.length,
    ...(rider !== undefined && { heightUnknown, notAllowed }),
  };
}
