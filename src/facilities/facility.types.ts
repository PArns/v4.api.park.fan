/**
 * Visitor facilities inside a park, read from OpenStreetMap.
 * docs/frontend/facilities.md holds the contract and the licence duties.
 */

/** What the import keeps. The order is the order a client lists them in. */
export const FACILITY_TYPES = [
  "toilets",
  "drinking_water",
  "baby_care",
  "first_aid",
  "lockers",
  "atm",
  "defibrillator",
] as const;
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
