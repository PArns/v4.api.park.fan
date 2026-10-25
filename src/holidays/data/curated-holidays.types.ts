import type { HolidayType } from "../entities/holiday.entity";

/**
 * One hand-copied holiday period, read off the publisher's own page.
 *
 * `from`/`to` are park-local calendar days, both inclusive. `region` is the
 * stored `holidays.region` form (`AU-QLD`), or null for the whole country.
 * Every entry names the page it was copied from and the day it was read, so a
 * reviewer can check it and the next yearly update knows where to look —
 * docs/rules/absent-facts.md: facts about real places come from primary
 * sources, never from recall. Nothing is extrapolated: a year the publisher
 * has not announced has no entry, and past the last entry the data is
 * unknown (docs/architecture/school-holiday-coverage.md).
 */
export interface CuratedHoliday {
  from: string;
  to: string;
  type: Extract<HolidayType, "public" | "school" | "working_day">;
  region: string | null;
  name: string;
  localName?: string;
  sourceUrl: string;
  retrievedAt: string;
}

export interface CuratedHolidaySet {
  country: string;
  entries: CuratedHoliday[];
}
