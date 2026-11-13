/**
 * The region's legs: OSRM's drive between car parks (`travel-matrix.generated.ts`)
 * with the curated parts laid over it — the transfer from a car park that is
 * not the gate, the walk or shuttle that replaces a 0 km drive between parks
 * sharing a garage, and the resort's own transport as alternatives. Pure.
 */
import {
  hotelPoint,
  type RegionConfig,
  type TransportAlternative,
  type TransportMode,
} from "./regions";
import {
  TRAVEL_MATRICES,
  type GeneratedTravelMatrix,
} from "./travel-matrix.generated";

/** Parks closer than this (door to door) may share a hop day. */
export const HOP_MAX_MINUTES = 30;

export interface TripLeg {
  from: string;
  to: string;
  km?: number;
  minutes: number;
  mode: "drive" | TransportMode;
  /** Where the drive ends short of the gate (`ttc`, `garage_shuttle`). */
  via?: string;
  /** Curated minutes from that car park to the gate, both ends together. */
  transferMinutes?: number;
  alternatives?: TransportAlternative[];
}

const pairKey = (a: string, b: string): string => [a, b].sort().join("|");

/** The region's travel matrix with its source fields. */
export function regionMatrix(region: RegionConfig): GeneratedTravelMatrix {
  const matrix = TRAVEL_MATRICES[region.slug];
  if (!matrix) {
    throw new Error(`No travel matrix for region ${region.slug}`);
  }
  return matrix;
}

/** Every leg of the region, curated parts applied, valid both ways. */
export function buildLegs(region: RegionConfig): TripLeg[] {
  const transfers = new Map(
    region.parks
      .filter((p) => p.transfer)
      .map((p) => [p.slug, p.transfer as { via: string; minutes: number }]),
  );
  const curated = new Map(region.legs.map((l) => [pairKey(l.from, l.to), l]));
  return regionMatrix(region).legs.map((leg) => {
    const extra = curated.get(pairKey(leg.from, leg.to));
    const alternatives = extra?.alternatives.length
      ? { alternatives: extra.alternatives }
      : {};
    if (extra?.replacesDrive) {
      return {
        from: leg.from,
        to: leg.to,
        ...(extra.replacesDrive.km != null && { km: extra.replacesDrive.km }),
        minutes: extra.replacesDrive.minutes,
        mode: extra.replacesDrive.mode,
        ...alternatives,
      };
    }
    const ends = [leg.from, leg.to]
      .map((end) => transfers.get(end))
      .filter((t): t is { via: string; minutes: number } => !!t);
    return {
      from: leg.from,
      to: leg.to,
      km: leg.km,
      minutes: leg.minutes,
      mode: "drive" as const,
      ...(ends.length > 0 && {
        via: ends.map((t) => t.via).join(","),
        transferMinutes: ends.reduce((s, t) => s + t.minutes, 0),
      }),
      ...alternatives,
    };
  });
}

/** The leg between two points, either way round, or undefined. */
export function findLeg(
  legs: TripLeg[],
  a: string,
  b: string,
): TripLeg | undefined {
  return legs.find(
    (l) => (l.from === a && l.to === b) || (l.from === b && l.to === a),
  );
}

/** Door-to-door minutes of a leg: the faster of the drive (with its transfers) and any timed alternative. */
export function doorToDoorMinutes(leg: TripLeg): number {
  const drive = leg.minutes + (leg.transferMinutes ?? 0);
  const timed = (leg.alternatives ?? [])
    .map((a) => a.minutes)
    .filter((m): m is number => m != null);
  return Math.min(drive, ...timed);
}

/**
 * Whether one hopper ticket covers both parks: the same set of the region's
 * `hopperSets` (Disney's four theme parks; Universal Studios and Islands of
 * Adventure, never Epic Universe).
 */
export function sameHopperSet(
  region: RegionConfig,
  a: string,
  b: string,
): boolean {
  return (
    a !== b && region.hopperSets.some((s) => s.includes(a) && s.includes(b))
  );
}

/**
 * The parks that may share a hop day: theme parks one hopper ticket covers
 * ({@link sameHopperSet}) whose leg is under {@link HOP_MAX_MINUTES} door to
 * door. Water parks and Discovery Cove take a whole day and never hop.
 */
export function hopPairs(
  region: RegionConfig,
  legs: TripLeg[],
  slugs: string[],
): Array<[string, string]> {
  const kindOf = new Map(region.groups.map((g) => [g.key, g.kind]));
  const parks = region.parks.filter(
    (p) =>
      slugs.includes(p.slug) &&
      p.type === "theme_park" &&
      kindOf.get(p.group) === "resort",
  );
  const pairs: Array<[string, string]> = [];
  for (let i = 0; i < parks.length; i++) {
    for (let j = i + 1; j < parks.length; j++) {
      if (!sameHopperSet(region, parks[i].slug, parks[j].slug)) continue;
      const leg = findLeg(legs, parks[i].slug, parks[j].slug);
      if (leg && doorToDoorMinutes(leg) < HOP_MAX_MINUTES) {
        pairs.push([parks[i].slug, parks[j].slug]);
      }
    }
  }
  return pairs;
}

/** The leg from a hotel area to a park, in the plan's shape. */
export function legFromHotel(
  legs: TripLeg[],
  area: string,
  park: string,
): TripLeg | undefined {
  const leg = findLeg(legs, hotelPoint(area), park);
  if (!leg) return undefined;
  return { ...leg, from: hotelPoint(area), to: park };
}
