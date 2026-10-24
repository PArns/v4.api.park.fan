import { HolidayInput } from "../../common/types/holiday-input.type";
import { KasiHoliday } from "./kasi.client";
import { KrRegion, resolveKrRegion } from "./kr-regions";

export const NEIS_PREFIX = "neis:KR:";
export const KASI_PREFIX = "kasi:KR:";

/**
 * Which regions to import, and under which stored region codes. A park's
 * region is written as the geocoder stored it ("KR-Gyeonggi-do") and as the
 * ISO code ("KR-41") the influence config uses, so both lookups find a row.
 * Codes that name no Korean region come back in `unknown`.
 */
export function krImportTargets(regionCodes: string[]): {
  targets: Map<KrRegion, Set<string>>;
  unknown: string[];
} {
  const targets = new Map<KrRegion, Set<string>>();
  const unknown: string[] = [];
  for (const code of regionCodes) {
    const region = resolveKrRegion(code);
    if (!region) {
      unknown.push(code);
      continue;
    }
    const set = targets.get(region) ?? new Set<string>();
    set.add(`KR-${region.iso}`).add(`KR-${code.replace(/^KR-/i, "")}`);
    targets.set(region, set);
  }
  return { targets, unknown };
}

function vacationName(event: string): string {
  if (event.includes("여름")) return "Summer vacation";
  if (event.includes("겨울")) return "Winter vacation";
  if (event.includes("봄")) return "Spring vacation";
  return "School vacation";
}

/** School rows for one region's majority vacation days, under each target code. */
export function neisHolidayRows(
  regionCodes: Iterable<string>,
  vacations: Map<string, string>,
): HolidayInput[] {
  const rows: HolidayInput[] = [];
  for (const region of regionCodes) {
    for (const [day, event] of vacations) {
      rows.push({
        externalId: `${NEIS_PREFIX}${region}:${day}`,
        date: new Date(`${day}T00:00:00.000Z`),
        name: vacationName(event),
        localName: event,
        country: "KR",
        region,
        holidayType: "school",
        isNationwide: false,
      });
    }
  }
  return rows;
}

export function kasiHolidayRows(holidays: KasiHoliday[]): HolidayInput[] {
  return holidays.map((h) => ({
    externalId: `${KASI_PREFIX}${h.date}:${h.name}`,
    date: new Date(`${h.date}T00:00:00.000Z`),
    name: h.name,
    localName: h.name,
    country: "KR",
    holidayType: "public",
    isNationwide: true,
  }));
}
