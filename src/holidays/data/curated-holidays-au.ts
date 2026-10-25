import { CuratedHoliday, CuratedHolidaySet } from "./curated-holidays.types";

/**
 * Australia, Queensland only — state school holidays and student-free
 * (staff professional development) days, from the Department of Education.
 * Our AU parks are all on the Gold Coast; other states are not curated.
 *
 * The end of the 2029/30 summer break is "to be published when approved by
 * the Minister", so that entry stops on 2029-12-31 — the page does say the
 * break runs into January 2030, but not to which day.
 * Next update: when the department publishes 2030.
 */
const QLD_2026 = "https://education.qld.gov.au/about-us/calendar/term-dates";
const QLD_FUTURE =
  "https://education.qld.gov.au/about-us/calendar/future-dates";
const RETRIEVED = "2026-10-24";

const qld = (
  from: string,
  to: string,
  name: string,
  sourceUrl: string,
): CuratedHoliday => ({
  from,
  to,
  type: "school",
  region: "AU-QLD",
  name,
  sourceUrl,
  retrievedAt: RETRIEVED,
});

const PD = "Student-free day (staff professional development)";

export const AU_CURATED_HOLIDAYS: CuratedHolidaySet = {
  country: "AU",
  entries: [
    // 2026
    qld("2026-01-22", "2026-01-23", PD, QLD_2026),
    qld("2026-04-03", "2026-04-19", "Term 1 school holidays", QLD_2026),
    qld("2026-04-16", "2026-04-17", PD, QLD_2026),
    qld("2026-06-27", "2026-07-12", "Term 2 school holidays", QLD_2026),
    qld("2026-09-04", "2026-09-04", PD, QLD_2026),
    qld("2026-09-19", "2026-10-05", "Term 3 school holidays", QLD_2026),
    qld("2026-12-12", "2027-01-26", "Summer school holidays", QLD_2026),
    // 2027
    qld("2027-01-21", "2027-01-22", PD, QLD_FUTURE),
    qld("2027-01-25", "2027-01-25", PD, QLD_FUTURE),
    qld("2027-03-26", "2027-04-11", "Term 1 school holidays", QLD_FUTURE),
    qld("2027-04-08", "2027-04-09", PD, QLD_FUTURE),
    qld("2027-06-26", "2027-07-11", "Term 2 school holidays", QLD_FUTURE),
    qld("2027-09-03", "2027-09-03", PD, QLD_FUTURE),
    qld("2027-09-18", "2027-10-04", "Term 3 school holidays", QLD_FUTURE),
    qld("2027-12-11", "2028-01-23", "Summer school holidays", QLD_FUTURE),
    // 2028
    qld("2028-01-20", "2028-01-21", PD, QLD_FUTURE),
    qld("2028-04-01", "2028-04-17", "Term 1 school holidays", QLD_FUTURE),
    qld("2028-04-12", "2028-04-13", PD, QLD_FUTURE),
    qld("2028-06-24", "2028-07-09", "Term 2 school holidays", QLD_FUTURE),
    qld("2028-09-01", "2028-09-01", PD, QLD_FUTURE),
    qld("2028-09-16", "2028-10-02", "Term 3 school holidays", QLD_FUTURE),
    qld("2028-12-09", "2029-01-21", "Summer school holidays", QLD_FUTURE),
    // 2029
    qld("2029-01-18", "2029-01-19", PD, QLD_FUTURE),
    qld("2029-03-30", "2029-04-15", "Term 1 school holidays", QLD_FUTURE),
    qld("2029-04-12", "2029-04-13", PD, QLD_FUTURE),
    qld("2029-06-23", "2029-07-08", "Term 2 school holidays", QLD_FUTURE),
    qld("2029-08-31", "2029-08-31", PD, QLD_FUTURE),
    qld("2029-09-15", "2029-10-01", "Term 3 school holidays", QLD_FUTURE),
    qld("2029-12-08", "2029-12-31", "Summer school holidays", QLD_FUTURE),
  ],
};
