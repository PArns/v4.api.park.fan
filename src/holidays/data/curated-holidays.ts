import { HolidayInput } from "../../common/types/holiday-input.type";
import { addIsoDays } from "../../common/utils/date.util";
import { AU_CURATED_HOLIDAYS } from "./curated-holidays-au";
import { CN_CURATED_HOLIDAYS } from "./curated-holidays-cn";
import { SG_CURATED_HOLIDAYS } from "./curated-holidays-sg";
import { CuratedHolidaySet } from "./curated-holidays.types";

/** Every hand-curated holiday file, one per country (PAR-1065). */
export const CURATED_HOLIDAY_SETS: CuratedHolidaySet[] = [
  AU_CURATED_HOLIDAYS,
  CN_CURATED_HOLIDAYS,
  SG_CURATED_HOLIDAYS,
];

/** The externalId prefix of one country's curated rows. */
export const curatedPrefix = (country: string): string => `curated:${country}:`;

/**
 * One row per day of every entry, in the form the sync upserts. The id holds
 * region, type and day, so re-running the sync rewrites the same rows.
 */
export function expandCuratedHolidays(set: CuratedHolidaySet): HolidayInput[] {
  const rows: HolidayInput[] = [];
  for (const e of set.entries) {
    for (let day = e.from; day <= e.to; day = addIsoDays(day, 1)) {
      rows.push({
        externalId: `${curatedPrefix(set.country)}${e.region ?? "national"}:${e.type}:${day}`,
        date: new Date(`${day}T00:00:00.000Z`),
        name: e.name,
        localName: e.localName ?? e.name,
        country: set.country,
        region: e.region ?? undefined,
        holidayType: e.type,
        isNationwide: e.region === null,
      });
    }
  }
  return rows;
}
