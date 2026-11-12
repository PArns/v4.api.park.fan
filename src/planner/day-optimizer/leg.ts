import { calculateHaversineDistance } from "../../common/utils/distance.util";
import { RIDE_DURATION_MIN } from "./day-grid";
import type { OptimizerDay, OptimizerRide, PlannerEntry } from "./types";

/**
 * What it takes to get from one stop to the next. Ported from park.fan's `lib/planner/leg.ts`,
 * which grades the drawn legs with the same numbers. A straight line is a lower bound on a walk
 * and nothing more, so the floor is the only figure that may call a plan impossible; the ceiling
 * is an assumption and is what a plan is built against.
 */

/** Out of the station, through the shop, onto the path. A judgement. */
export const EXIT_MIN = 3;

/** A brisk walker, metres per minute; used only for the floor. */
export const WALK_FAST_M_PER_MIN = 100;

/** About 4 km/h, park pace; used only for the ceiling. */
export const WALK_PARK_M_PER_MIN = 67;

/** Assumed worst path-to-straight-line ratio. A judgement, measured for no park. */
export const DETOUR_MAX = 1.6;

/** Assumed ceiling with no coordinates, same land. A judgement. */
export const SAME_LAND_CEIL_MIN = 3;

/** The same, across lands. A judgement. */
export const CROSS_LAND_CEIL_MIN = 8;

/** Where an end of a leg stands: a ride, or a show with coordinates. */
export type LegPlace = Pick<OptimizerRide, "latitude" | "longitude" | "land">;

/** Which ends of a transfer are not rides: leaving one costs no station and no ride. */
export interface TransferEnds {
  fromBlock?: boolean;
  toBlock?: boolean;
}

/** The geometry of a transfer: distance and both bounds in minutes. */
export interface Transfer {
  metres: number | null;
  crossesLand: boolean;
  floorMinutes: number;
  ceilingMinutes: number;
  walkMinutes: number;
}

function coordsOf(ride: LegPlace | null | undefined): [number, number] | null {
  const lat = ride?.latitude;
  const lng = ride?.longitude;
  if (typeof lat !== "number" || typeof lng !== "number") return null;
  return [lat, lng];
}

/**
 * Where an entry is: the ride's or the show's coordinates from `/plan/day`, or `null`. A free block
 * and a show without a valid position have no place, since a guess may never feed the floor.
 */
export function entryPlace(
  day: OptimizerDay | null | undefined,
  entry: Pick<PlannerEntry, "attractionSlug" | "showSlug">,
): LegPlace | null {
  if (entry.attractionSlug) {
    return (
      day?.rides.find((ride) => ride.attractionSlug === entry.attractionSlug) ??
      null
    );
  }
  if (entry.showSlug) {
    const show = day?.shows?.find((s) => s.showSlug === entry.showSlug);
    const lat = show?.latitude;
    const lng = show?.longitude;
    if (
      typeof lat === "number" &&
      typeof lng === "number" &&
      Number.isFinite(lat) &&
      Number.isFinite(lng) &&
      Math.abs(lat) <= 90 &&
      Math.abs(lng) <= 180
    ) {
      return { latitude: lat, longitude: lng, land: null };
    }
  }
  return null;
}

/** True for an entry that is not a ride: a show, or a free block. */
export function isBlockEntry(
  entry: Pick<PlannerEntry, "attractionSlug">,
): boolean {
  return !entry.attractionSlug;
}

/** How long it takes to get from one stop to the next, the geometry alone. */
export function transferBetween(
  from: LegPlace | null | undefined,
  to: LegPlace | null | undefined,
  rideSeconds?: number | null,
  ends: TransferEnds = {},
): Transfer {
  if (ends.fromBlock && !from) {
    return {
      metres: null,
      crossesLand: false,
      floorMinutes: 0,
      ceilingMinutes: 0,
      walkMinutes: 0,
    };
  }

  const a = coordsOf(from);
  const b = coordsOf(to);
  const metres =
    a && b
      ? calculateHaversineDistance(
          { latitude: a[0], longitude: a[1] },
          { latitude: b[0], longitude: b[1] },
        )
      : null;

  const fromLand = from?.land ?? null;
  const toLand = to?.land ?? null;
  const crossesLand = Boolean(fromLand && toLand && fromLand !== toLand);

  const rideMin =
    typeof rideSeconds === "number" && rideSeconds > 0
      ? Math.ceil(rideSeconds / 60)
      : RIDE_DURATION_MIN;

  // No coordinates: the floor's walk is zero, so a guess never calls a plan impossible.
  const walkFloorMin =
    metres === null ? 0 : Math.ceil(metres / WALK_FAST_M_PER_MIN);
  const walkCeilMin =
    metres === null
      ? crossesLand
        ? CROSS_LAND_CEIL_MIN
        : SAME_LAND_CEIL_MIN
      : Math.ceil((metres * DETOUR_MAX) / WALK_PARK_M_PER_MIN);

  const leaving = ends.fromBlock ? 0 : EXIT_MIN + rideMin;

  if (ends.toBlock && !to) {
    return {
      metres: null,
      crossesLand: false,
      floorMinutes: leaving,
      ceilingMinutes: leaving,
      walkMinutes: 0,
    };
  }

  return {
    metres,
    crossesLand,
    floorMinutes: leaving + walkFloorMin,
    ceilingMinutes: leaving + walkCeilMin,
    walkMinutes: walkCeilMin,
  };
}
