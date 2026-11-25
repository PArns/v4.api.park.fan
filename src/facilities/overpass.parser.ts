import { normalizeForMatching } from "../common/utils/slug.util";
import type {
  FacilityType,
  OsmElementType,
  ParsedFacility,
} from "./facility.types";

/**
 * The Overpass queries and what is kept of their answers. Pure functions: the
 * filter rules live here and nowhere else, and this file is the "method of
 * making the alterations" ODbL 4.6 b asks to be published.
 * docs/frontend/facilities.md lists the rules in words.
 */

export interface OverpassElement {
  type: OsmElementType;
  id: number;
  lat?: number;
  lon?: number;
  center?: { lat: number; lon: number };
  bounds?: { minlat: number; minlon: number; maxlat: number; maxlon: number };
  tags?: Record<string, string>;
  version?: number;
  timestamp?: string;
}

export interface OverpassResponse {
  osm3s?: { timestamp_osm_base?: string };
  elements?: OverpassElement[];
  /** Set when the query timed out or ran out of memory: the answer is partial. */
  remark?: string;
}

/** How far around the park's coordinate a boundary may lie, in metres. */
const BOUNDARY_SEARCH_M = 2000;

/** Two points of one type closer than this are one facility mapped twice. */
export const DUPLICATE_DISTANCE_M = 15;

const BOUNDARY_FILTER = '["tourism"~"^(theme_park|zoo)$"]';

/**
 * The park outlines near a coordinate: everything tagged as a theme park, zoo
 * or water park that contains the point or lies within two kilometres of it.
 */
export function buildBoundaryQuery(lat: number, lon: number): string {
  const at = `${lat},${lon}`;
  const near = `(around:${BOUNDARY_SEARCH_M},${at})`;
  return `[out:json][timeout:25];
is_in(${at})->.c;
(area.c${BOUNDARY_FILTER};area.c["leisure"="water_park"];)->.ca;
(
  way(pivot.ca);rel(pivot.ca);
  way${BOUNDARY_FILTER}${near};rel${BOUNDARY_FILTER}${near};
  way["leisure"="water_park"]${near};rel["leisure"="water_park"]${near};
);
out tags bb;`;
}

const NAME_KEYS = [
  "name",
  "name:en",
  "official_name",
  "alt_name",
  "short_name",
];

/**
 * The outline that is this park: its name must match one of ours once
 * normalised. An equal name beats one that only contains the other
 * ("PortAventura Park" over "PortAventura Caribe Aquatic Park"), and of
 * several equally good the smallest wins, the park inside its resort. Null
 * when no name matches: a park is never cut with an outline chosen by position
 * alone, because the neighbour's toilets would be served as ours.
 */
export function pickBoundary(
  elements: readonly OverpassElement[],
  parkNames: readonly (string | null | undefined)[],
): OverpassElement | null {
  const ours = parkNames.map(normalizeForMatching).filter((n) => n.length > 2);
  /** 2: a name is equal, 1: one contains the other, 0: no match. */
  const rank = (element: OverpassElement): number =>
    Math.max(
      0,
      ...NAME_KEYS.flatMap((key) => (element.tags?.[key] ?? "").split(";"))
        .map(normalizeForMatching)
        .filter((theirs) => theirs.length > 2)
        .flatMap((theirs) =>
          ours.map((name) =>
            name === theirs
              ? 2
              : name.includes(theirs) || theirs.includes(name)
                ? 1
                : 0,
          ),
        ),
    );
  const area = ({ bounds }: OverpassElement) =>
    (bounds!.maxlat - bounds!.minlat) * (bounds!.maxlon - bounds!.minlon);
  return (
    elements
      .filter((e) => e.type !== "node" && e.bounds && rank(e) > 0)
      .sort((a, b) => rank(b) - rank(a) || area(a) - area(b))[0] ?? null
  );
}

/** The OSM tags that make an element one of our types. */
const TAG_FILTERS = [
  '["amenity"~"^(toilets|drinking_water|nursing_room|baby_care|first_aid|locker|lockers|luggage_locker|atm)$"]',
  '["emergency"~"^(first_aid|defibrillator)$"]',
  '["healthcare"="first_aid"]',
];

/** Everything of our types inside one outline, with version and timestamp. */
export function buildFacilitiesQuery(boundary: {
  type: OsmElementType;
  id: number | string;
}): string {
  const set = boundary.type === "relation" ? "rel" : "way";
  return `[out:json][timeout:60];
${set}(${boundary.id});map_to_area->.a;
(
${TAG_FILTERS.map((filter) => `  nwr${filter}(area.a);`).join("\n")}
);
out meta center;`;
}

function facilityType(tags: Record<string, string>): FacilityType | null {
  switch (tags.amenity) {
    case "toilets":
      return "toilets";
    case "drinking_water":
      return "drinking_water";
    case "nursing_room":
    case "baby_care":
      return "baby_care";
    case "first_aid":
      return "first_aid";
    case "locker":
    case "lockers":
    case "luggage_locker":
      return "lockers";
    case "atm":
      return "atm";
  }
  if (tags.emergency === "defibrillator") return "defibrillator";
  if (tags.emergency === "first_aid" || tags.healthcare === "first_aid") {
    return "first_aid";
  }
  return null;
}

/** `access` values that mean a visitor cannot use it. */
const NO_VISITOR_ACCESS = new Set([
  "private",
  "no",
  "staff",
  "employees",
  "delivery",
]);

const yesNo = (value: string | undefined): boolean | null =>
  value === "yes" ? true : value === "no" ? false : null;

function wheelchair(
  tags: Record<string, string>,
): ParsedFacility["wheelchair"] {
  const value = tags.wheelchair ?? tags["toilets:wheelchair"];
  if (value === "yes" || value === "designated") return "yes";
  return value === "limited" || value === "no" ? value : null;
}

/** How much of what we show an element carries: the richer twin is kept. */
const detail = (f: ParsedFacility): number =>
  (f.name ? 1 : 0) +
  (f.wheelchair ? 1 : 0) +
  (f.changingTable === null ? 0 : 1) +
  (f.fee === null ? 0 : 1);

function metres(a: ParsedFacility, b: ParsedFacility): number {
  const rad = Math.PI / 180;
  const x =
    (b.longitude - a.longitude) *
    rad *
    Math.cos(((a.latitude + b.latitude) / 2) * rad);
  const y = (b.latitude - a.latitude) * rad;
  return Math.sqrt(x * x + y * y) * 6_371_000;
}

/**
 * The facilities of an Overpass answer, after the filter rules:
 *
 * 1. only elements of a known type that carry a position, a version and a
 *    timestamp (the query asks for `meta`; one without is not trusted);
 * 2. nothing a visitor cannot use: `access` private, no, staff, employees,
 *    delivery. `customers` stays: inside the gate the customer is the visitor;
 * 3. no drinking water marked `drinking_water=no` or `drinking_water:legal=no`;
 * 4. of two of one type within 15 m, one: a toilet block mapped as a building
 *    and again as a node inside it. The one with more of the tags we show
 *    stays, a node before an outline, the lower id last so the choice is stable.
 *
 * A way or relation is placed at Overpass' `center`.
 */
export function parseFacilities(
  elements: readonly OverpassElement[],
): ParsedFacility[] {
  const kept: ParsedFacility[] = [];
  for (const element of elements) {
    const tags = element.tags ?? {};
    const type = facilityType(tags);
    const latitude = element.lat ?? element.center?.lat;
    const longitude = element.lon ?? element.center?.lon;
    const osmTimestamp = new Date(element.timestamp ?? NaN);
    if (
      !type ||
      typeof latitude !== "number" ||
      typeof longitude !== "number" ||
      typeof element.version !== "number" ||
      Number.isNaN(osmTimestamp.getTime())
    ) {
      continue;
    }
    if (NO_VISITOR_ACCESS.has(tags.access ?? "")) continue;
    if (
      type === "drinking_water" &&
      (tags.drinking_water === "no" || tags["drinking_water:legal"] === "no")
    ) {
      continue;
    }
    kept.push({
      type,
      latitude,
      longitude,
      name: tags.name?.trim() || null,
      wheelchair: wheelchair(tags),
      changingTable: yesNo(tags.changing_table),
      fee: yesNo(tags.fee),
      osmType: element.type,
      osmId: String(element.id),
      osmVersion: element.version,
      osmTimestamp,
    });
  }

  kept.sort(
    (a, b) =>
      detail(b) - detail(a) ||
      Number(b.osmType === "node") - Number(a.osmType === "node") ||
      Number(a.osmId) - Number(b.osmId),
  );
  const unique: ParsedFacility[] = [];
  for (const candidate of kept) {
    const twin = unique.some(
      (other) =>
        other.type === candidate.type &&
        metres(other, candidate) < DUPLICATE_DISTANCE_M,
    );
    if (!twin) unique.push(candidate);
  }
  return unique;
}
