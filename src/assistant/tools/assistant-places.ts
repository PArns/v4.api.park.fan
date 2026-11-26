import { calculateHaversineDistance } from "../../common/utils/distance.util";
import { isCanonicalUuid } from "../../common/utils/uuid-param.util";
import { McpToolError } from "../../mcp/mcp-tool";
import { nameKey } from "../../mcp/tools/park-ref";
import {
  DETOUR_MAX,
  WALK_PARK_M_PER_MIN,
} from "../../planner/day-optimizer/leg";

/** What a place in a park is: Parky names a ride, a show or a place to eat the same way. */
export type PlaceKind = "ride" | "show" | "restaurant";

/** A ride, show or restaurant of one park, with its position when the source gave one. */
export interface ParkPlace {
  id: string;
  name: string;
  kind: PlaceKind;
  latitude: number | null;
  longitude: number | null;
}

/** A row as the entities hold it: TypeORM reads a decimal column as a string. */
interface PlaceRow {
  id: string;
  name: string;
  latitude?: number | string | null;
  longitude?: number | string | null;
}

/** The park's rides, shows and restaurants, as places with parsed coordinates. */
export function parkPlaces(park: {
  attractions?: PlaceRow[];
  shows?: PlaceRow[];
  restaurants?: PlaceRow[];
}): ParkPlace[] {
  const of = (rows: PlaceRow[] | undefined, kind: PlaceKind) =>
    (rows ?? []).map((row) => ({
      id: row.id,
      name: row.name,
      kind,
      latitude: coordinate(row.latitude, 90),
      longitude: coordinate(row.longitude, 180),
    }));
  return [
    ...of(park.attractions, "ride"),
    ...of(park.shows, "show"),
    ...of(park.restaurants, "restaurant"),
  ];
}

/** A finite coordinate within its bound, else null; 0/0 is a source's placeholder, not a place. */
function coordinate(
  value: number | string | null | undefined,
  bound: number,
): number | null {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) && n !== 0 && Math.abs(n) <= bound ? n : null;
}

/** Several places answer to one name: handed back so the model can pick by id. */
export interface AmbiguousPlace {
  ambiguous: true;
  hint: string;
  places: Array<{ id: string; name: string; kind: PlaceKind }>;
}

/**
 * The place `reference` names: its id, the one whose name is the reference, or the only one
 * whose name contains it ("Chiapas" for "Chiapas - DIE Wasserbahn") or is contained in it.
 * Several equal candidates come back to pick from; none throws for the model.
 */
export function findPlace(
  places: readonly ParkPlace[],
  reference: string,
  parkName: string,
): ParkPlace | AmbiguousPlace {
  const ref = reference.trim();
  if (isCanonicalUuid(ref.toLowerCase())) {
    const byId = places.find((p) => p.id === ref.toLowerCase());
    if (byId) return byId;
  }
  const key = nameKey(ref);
  if (key.length < 2) throw new McpToolError(`"${reference}" names no place`);
  const exact = places.filter((p) => nameKey(p.name) === key);
  const hits =
    exact.length > 0
      ? exact
      : places.filter((p) => {
          const name = nameKey(p.name);
          return name.includes(key) || (name.length >= 4 && key.includes(name));
        });
  if (hits.length === 1) return hits[0];
  if (hits.length === 0) {
    throw new McpToolError(
      `No ride, show or restaurant "${reference}" at ${parkName}; try find_rides or find_restaurants`,
    );
  }
  return {
    ambiguous: true,
    hint: "Several places match; call again with one id",
    places: hits.slice(0, 8).map(({ id, name, kind }) => ({ id, name, kind })),
  };
}

/** True for the result of {@link findPlace} that names several places. */
export function isAmbiguousPlace(
  found: ParkPlace | AmbiguousPlace,
): found is AmbiguousPlace {
  return "ambiguous" in found;
}

/** The walk between two places, or null when either has no position. */
export interface Walk {
  /** Straight line, rounded to 10 m: a finer figure would claim a precision the map lacks. */
  metres: number;
  /** At park pace along paths up to {@link DETOUR_MAX} times the straight line, as the day planner plans. */
  walkMinutes: number;
}

/** The walk from `from` to `to`, as the day planner reckons a transfer's walk. */
export function walkBetween(from: ParkPlace, to: ParkPlace): Walk | null {
  if (
    from.latitude === null ||
    from.longitude === null ||
    to.latitude === null ||
    to.longitude === null
  ) {
    return null;
  }
  const metres = calculateHaversineDistance(
    { latitude: from.latitude, longitude: from.longitude },
    { latitude: to.latitude, longitude: to.longitude },
  );
  return {
    metres: Math.round(metres / 10) * 10,
    walkMinutes: Math.max(
      1,
      Math.ceil((metres * DETOUR_MAX) / WALK_PARK_M_PER_MIN),
    ),
  };
}

/**
 * `items` nearest to `anchor` first, each with its walk; those without a position last, in
 * their order, with a null walk. The anchor itself, when it is one of them, is left out.
 */
export function byWalkFrom<T extends { id: string }>(
  anchor: ParkPlace,
  items: readonly T[],
  placeOf: (item: T) => ParkPlace | undefined,
): Array<{ item: T; walk: Walk | null }> {
  const rows = items
    .filter((item) => item.id !== anchor.id)
    .map((item) => {
      const place = placeOf(item);
      return { item, walk: place ? walkBetween(anchor, place) : null };
    });
  const placed = rows.filter((row) => row.walk !== null);
  placed.sort((a, b) => (a.walk?.metres ?? 0) - (b.walk?.metres ?? 0));
  return [...placed, ...rows.filter((row) => row.walk === null)];
}
