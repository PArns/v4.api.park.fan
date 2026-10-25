import { CuratedHoliday, CuratedHolidaySet } from "./curated-holidays.types";

/**
 * Great Britain — school holidays from ONE local authority for England
 * (PAR-1072): Staffordshire County Council, where Alton Towers is. Neither
 * Nager.Date nor OpenHolidays has GB school data, and English term dates are
 * set per local authority (and academies may set their own).
 *
 * All seven GB parks are in England and match holidays by `GB-ENG` (stored
 * `ENG`, Flamingo Land's `England` is aliased), so England carries one
 * council's calendar. That is a simplification: Thorpe Park (Surrey) has a
 * two-week October half term in 2026 and its summer starts 2027-07-29.
 * Staffordshire was chosen because it publishes three years ahead and its
 * 2026-27 year is the same day for day as Windsor & Maidenhead's (LEGOLAND).
 * Two councils under one key would be unioned, which overstates every break
 * that differs, so no second one is added. Scotland and Wales have no rows and read as
 * unknown (`REGION_ONLY_SCHOOL_COVERAGE`).
 *
 * Listed: every holiday the page names, the May Day bank holiday and the
 * fixed inset day the council sets (pupils are off; the other four inset days
 * are each school's choice and are not listed). A bank holiday next to a
 * holiday (Good Friday 2027, 3 January 2028) is part of the range, as are
 * weekends inside it; the ones around it are not. 2025-26 entries were read
 * from the page's Wayback captures of 2025-09-10 and 2026-01-27; the live page
 * (updated 8 October 2026) starts with autumn 2026. Next update: when the
 * council adds 2029-30.
 */
const RETRIEVED = "2026-10-25";
const STAFFS =
  "https://www.staffordshire.gov.uk/schools-and-learning/school-term-dates";

const eng = (from: string, to: string, name: string): CuratedHoliday => ({
  from,
  to,
  type: "school",
  region: "GB-ENG",
  name: `${name} (Staffordshire)`,
  sourceUrl: STAFFS,
  retrievedAt: RETRIEVED,
});

export const GB_CURATED_HOLIDAYS: CuratedHolidaySet = {
  country: "GB",
  entries: [
    // end of 2024-25 (Wayback 2025-09-10)
    eng("2025-05-05", "2025-05-05", "May Day bank holiday"),
    eng("2025-05-26", "2025-05-30", "May half term"),
    // holiday 22 July – 29 August, then the fixed inset day 1 September
    eng("2025-07-22", "2025-09-01", "Summer holiday"),
    // 2025-26
    eng("2025-10-27", "2025-10-31", "October half term"),
    eng("2025-12-22", "2026-01-02", "Christmas holiday"),
    eng("2026-02-16", "2026-02-20", "February half term"),
    eng("2026-03-30", "2026-04-10", "Easter holiday"),
    eng("2026-05-04", "2026-05-04", "May Day bank holiday"),
    eng("2026-05-25", "2026-05-29", "May half term"),
    // holiday 21 July – 31 August, inset day 1 September
    eng("2026-07-21", "2026-09-01", "Summer holiday"),
    // 2026-27
    eng("2026-10-26", "2026-10-30", "October half term"),
    eng("2026-12-21", "2027-01-01", "Christmas holiday"),
    eng("2027-02-15", "2027-02-19", "February half term"),
    // term ends Thursday 25 March, Good Friday 26 March
    eng("2027-03-26", "2027-04-09", "Easter holiday"),
    eng("2027-05-03", "2027-05-03", "May Day bank holiday"),
    eng("2027-05-31", "2027-06-04", "May half term"),
    // holiday 22 July – 1 September, inset day 2 September
    eng("2027-07-22", "2027-09-02", "Summer holiday"),
    // 2027-28
    eng("2027-10-25", "2027-10-29", "October half term"),
    // holiday 20–31 December, term starts Tuesday 4 January (3 January is
    // the New Year bank holiday)
    eng("2027-12-20", "2028-01-03", "Christmas holiday"),
    eng("2028-02-14", "2028-02-18", "February half term"),
    eng("2028-04-10", "2028-04-21", "Easter holiday"),
    eng("2028-05-01", "2028-05-01", "May Day bank holiday"),
    eng("2028-05-29", "2028-06-02", "May half term"),
    // holiday 24 July – 1 September, inset day Monday 4 September
    eng("2028-07-24", "2028-09-04", "Summer holiday"),
    // 2028-29
    eng("2028-10-23", "2028-10-27", "October half term"),
    eng("2028-12-22", "2029-01-05", "Christmas holiday"),
    eng("2029-02-12", "2029-02-16", "February half term"),
    eng("2029-03-30", "2029-04-13", "Easter holiday"),
    eng("2029-05-07", "2029-05-07", "May Day bank holiday"),
    eng("2029-05-28", "2029-06-01", "May half term"),
    // the page's last date; 2029-30's first day is not published
    eng("2029-07-26", "2029-08-31", "Summer holiday"),
  ],
};
