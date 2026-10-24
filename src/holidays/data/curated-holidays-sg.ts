import { CuratedHoliday, CuratedHolidaySet } from "./curated-holidays.types";

/**
 * Singapore — MOE school vacations and scheduled school holidays for
 * MOE Kindergarten, primary and secondary schools (the JC/MI calendar differs
 * only at the end of the year and is left out).
 *
 * MOE publishes the next year in July. Public holidays come from Nager.Date;
 * only the days MOE itself closes schools on are listed here.
 * Next update: the "School Terms and Holidays for 2028" press release.
 */
const SG_2026 =
  "https://www.moe.gov.sg/news/press-releases/20250730-school-terms-and-holidays-for-2026";
const SG_2027 =
  "https://www.moe.gov.sg/news/press-releases/20260714-school-terms-and-holidays-for-2027";
const RETRIEVED = "2026-10-24";

const school = (
  from: string,
  to: string,
  name: string,
  sourceUrl: string,
): CuratedHoliday => ({
  from,
  to,
  type: "school",
  region: null,
  name,
  sourceUrl,
  retrievedAt: RETRIEVED,
});

export const SG_CURATED_HOLIDAYS: CuratedHolidaySet = {
  country: "SG",
  entries: [
    // 2026
    school("2026-03-14", "2026-03-22", "March school holidays", SG_2026),
    school(
      "2026-03-23",
      "2026-03-23",
      "Day off in lieu for schools (Hari Raya Puasa)",
      SG_2026,
    ),
    school("2026-05-30", "2026-06-28", "June school holidays", SG_2026),
    school("2026-07-06", "2026-07-06", "Youth Day school holiday", SG_2026),
    school("2026-09-04", "2026-09-04", "Teachers' Day", SG_2026),
    school("2026-09-05", "2026-09-13", "September school holidays", SG_2026),
    school(
      "2026-10-02",
      "2026-10-02",
      "Children's Day (primary schools only)",
      SG_2026,
    ),
    school("2026-11-21", "2026-12-31", "Year-end school holidays", SG_2026),
    // 2027
    school(
      "2027-02-09",
      "2027-02-09",
      "Day off in lieu for schools (Chinese New Year)",
      SG_2027,
    ),
    school("2027-03-13", "2027-03-21", "March school holidays", SG_2027),
    school(
      "2027-05-03",
      "2027-05-03",
      "Day off in lieu for schools (Labour Day)",
      SG_2027,
    ),
    school("2027-05-29", "2027-06-27", "June school holidays", SG_2027),
    school("2027-07-05", "2027-07-05", "Youth Day school holiday", SG_2027),
    school("2027-08-10", "2027-08-10", "Day after National Day", SG_2027),
    school("2027-09-03", "2027-09-03", "Teachers' Day", SG_2027),
    school("2027-09-04", "2027-09-12", "September school holidays", SG_2027),
    school(
      "2027-10-08",
      "2027-10-08",
      "Children's Day (primary schools only)",
      SG_2027,
    ),
    school("2027-11-20", "2027-12-31", "Year-end school holidays", SG_2027),
  ],
};
