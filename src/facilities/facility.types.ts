/**
 * Visitor facilities inside a park, read from OpenStreetMap.
 * docs/frontend/facilities.md holds the contract and the licence duties.
 */

/**
 * The types the import keeps and the public route serves: the one list. OSM
 * maps first aid, lockers, baby-care rooms and ATMs too thinly inside parks
 * (0 to 4 per park) for "the nearest one" to be true, so they are not read.
 *
 * `sight` is something to look at rather than to use: a named figure bin
 * (Efteling's Holle Bolle Gijs) or a named artwork. It is a map layer, not a
 * "nearest one": a park that maps none simply has no sight in its list.
 */
export const FACILITY_TYPES = ["toilets", "drinking_water", "sight"] as const;
export type FacilityType = (typeof FACILITY_TYPES)[number];

export type OsmElementType = "node" | "way" | "relation";

/** One facility as the parser returns it and the table stores it. */
export interface ParsedFacility {
  type: FacilityType;
  latitude: number;
  longitude: number;
  name: string | null;
  /** `yes` | `limited` | `no`, or null when OSM does not say. */
  wheelchair: "yes" | "limited" | "no" | null;
  /** Null when OSM does not say: an absent tag is not a "no". */
  changingTable: boolean | null;
  fee: boolean | null;
  osmType: OsmElementType;
  osmId: string;
  osmVersion: number;
  osmTimestamp: Date;
}

/** A parsed facility with where it lies relative to the park's outline. */
export interface ImportedFacility extends ParsedFacility {
  /** False for a building whose centre falls outside the outline it touches. */
  insideOutline: boolean;
}

/** An outline as closed rings of `[longitude, latitude]`. */
export type OutlineRings = [number, number][][];

/**
 * The notice every answer carries. ODbL 4.2 wants the licence named with the
 * data, the OSMF attribution guidelines want "OpenStreetMap" linked to the
 * copyright page.
 */
export const OSM_ATTRIBUTION = {
  text: "© OpenStreetMap contributors",
  url: "https://www.openstreetmap.org/copyright",
  license: "ODbL-1.0",
  licenseUrl: "https://opendatacommons.org/licenses/odbl/1-0/",
} as const;

/**
 * Off unless `FACILITIES_ENABLED` is `true`: the public route answers 404
 * until someone turns it on. Import and review in the admin work either way,
 * so parks can be prepared before the switch.
 */
export function facilitiesEnabled(): boolean {
  return (process.env.FACILITIES_ENABLED ?? "").toLowerCase() === "true";
}
