import type { ParsedFacility } from "./facility.types";
import type { ParkFacilityOsm } from "./entities/park-facility-osm.entity";

/** What an import does to a park's stored rows. */
export interface FacilityMergePlan {
  insert: ParsedFacility[];
  /** `visible`: something a client shows changed, not only the OSM version. */
  update: { id: string; facility: ParsedFacility; visible: boolean }[];
  /** Rows whose OSM element is gone from the answer. */
  removeIds: string[];
  unchanged: number;
}

const key = (f: { osmType: string; osmId: string }) =>
  `${f.osmType}/${f.osmId}`;

const SHOWN = [
  "type",
  "latitude",
  "longitude",
  "name",
  "wheelchair",
  "changingTable",
  "fee",
] as const;

/**
 * Compares a park's stored rows with a fresh answer, by OSM element. Importing
 * the same answer twice plans nothing. The fields are compared and not only
 * the version: a way's centre moves when its nodes do, and the way's version
 * stays. `hidden` is not part of a facility, so an update never touches it.
 */
export function planFacilityMerge(
  existing: readonly ParkFacilityOsm[],
  incoming: readonly ParsedFacility[],
): FacilityMergePlan {
  const stored = new Map(existing.map((row) => [key(row), row]));
  const plan: FacilityMergePlan = {
    insert: [],
    update: [],
    removeIds: [],
    unchanged: 0,
  };
  const seen = new Set<string>();
  for (const facility of incoming) {
    const k = key(facility);
    if (seen.has(k)) continue;
    seen.add(k);
    const row = stored.get(k);
    if (!row) plan.insert.push(facility);
    else {
      const visible = SHOWN.some((field) => row[field] !== facility[field]);
      if (
        visible ||
        row.osmVersion !== facility.osmVersion ||
        new Date(row.osmTimestamp).getTime() !== facility.osmTimestamp.getTime()
      ) {
        plan.update.push({ id: row.id, facility, visible });
      } else plan.unchanged += 1;
    }
  }
  plan.removeIds = existing
    .filter((row) => !seen.has(key(row)))
    .map((row) => row.id);
  return plan;
}
