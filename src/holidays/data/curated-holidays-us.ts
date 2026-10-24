import { CuratedHoliday, CuratedHolidaySet } from "./curated-holidays.types";

/**
 * United States — school days off from ONE school district per state, the one
 * nearest most of our parks there (PAR-1071, PAR-1074). Neither Nager.Date nor
 * OpenHolidays has US school data, and US school calendars are set per
 * district, not per state.
 *
 * Parks match holidays by state (`regionCode` FL/CA/TX → `US-FL`/…), so a
 * state carries one district's calendar for all its parks. That is a
 * simplification: Busch Gardens Tampa and LEGOLAND Florida get Orange County's
 * calendar too, and Kings Island gets Sandusky's. Two districts under one key
 * would be unioned, which overstates every break that differs, so a second
 * district is not added. A state with
 * no entry here reads as unknown, never as another state's calendar
 * (`REGION_ONLY_SCHOOL_COVERAGE`).
 *
 * Every day students have no school is listed: the breaks, the summer (last
 * day of school → first day of the next year) and the single student
 * holidays. A published bad-weather make-up day is listed as the holiday it
 * is planned to be. The 2025-26 year is included so the model's training
 * history does not read last school year as "no break". Interior weekends are part of a range, outer ones are not
 * (the weekend feature carries them). Next update: when a district publishes
 * its next school year.
 */
const RETRIEVED = "2026-10-25";

/** One district's entries: `name` gets the district appended. */
const district =
  (region: string, districtName: string) =>
  (
    from: string,
    to: string,
    name: string,
    sourceUrl: string,
  ): CuratedHoliday => ({
    from,
    to,
    type: "school",
    region,
    name: `${name} (${districtName})`,
    sourceUrl,
    retrievedAt: RETRIEVED,
  });

// Florida — Orange County Public Schools (Disney, Universal, SeaWorld: 13 of
// our 17 FL parks). The ocps.net links redirect to the district's PDFs; the
// historical page links 2024-25 and 2025-26.
const OCPS_HISTORY = "https://www.ocps.net/110681_3";
const OCPS_2026 = "https://www.ocps.net/110680_3";
const OCPS_2027 = "https://www.ocps.net/school-calendar-2027-28";
const OCPS_2028 = "https://www.ocps.net/school-calendar-2028-29";
const fl = district("US-FL", "Orange County Public Schools");

// Texas — Northside ISD, San Antonio (SeaWorld, Aquatica, Six Flags Fiesta
// Texas; Schlitterbahn New Braunfels nearby). Arlington ISD (Six Flags Over
// Texas) has published 2026-27 only, see the doc.
const NISD_FILES = "https://www.nisd.net/sites/default/files/attachments/file";
const NISD_2025 = `${NISD_FILES}/2025/2025-2026-nisd-calendar.pdf`;
const NISD_2026 = `${NISD_FILES}/2025/2026-2027-nisd-calendar.pdf`;
const NISD_2027 = `${NISD_FILES}/2025/2027-2028-nisd-calendar.pdf`;
const tx = district("US-TX", "Northside ISD");

// California — Los Angeles Unified (Universal Studios Hollywood; the LA
// basin is the main catchment for Disneyland, Knott's and Magic Mountain).
// The district page links the 2025-26, 2026-27 and 2027-28 PDFs (media.edlio.net, LAUSD's site
// host); "Board Approved", adoption tentative pending union negotiations.
const LAUSD =
  "https://www.lausd.org/apps/pages/index.jsp?uREC_ID=4432518&type=d&pREC_ID=2672183";
const ca = district("US-CA", "Los Angeles Unified");

// Pennsylvania — Derry Township School District, Hershey (Hersheypark). The
// district's "Year at a Glance" page shows 2026-27; 2025-26 and the end of
// 2024-25 come from its event calendar (calendar-view, month by month), which
// also shows the snow make-up day it used (2026-04-02). Days off for some
// grades only (K-5 conferences, 6th-grade conferences) are not listed.
const DTSD_EVENTS = "https://www.hershey.k12.pa.us/calendar/calendar-view";
const DTSD_2026 = "https://www.hershey.k12.pa.us/calendar/accessible-calendar";
const pa = district("US-PA", "Derry Township School District");

// Ohio — Sandusky City Schools (Cedar Point, Cedar Point Shores). The district
// publishes its calendars as Google Sheets linked from scs-k12.net; the
// 2024-25 and 2025-26 links were read from Wayback captures of its homepage,
// the sheets themselves directly. A day counts as off when the sheet colours
// it "No School" or "Professional Learning Day - No School".
const SCS_HOME = "https://www.scs-k12.net/";
const SCS_2026 =
  "https://www.scs-k12.net/protected/ArticleView.aspx?iid=6I3UUB0&dasi=3GGI";
const oh = district("US-OH", "Sandusky City Schools");

// Missouri — North Kansas City Schools (Worlds of Fun, Oceans of Fun). The
// page links the board-approved 2026-27 and 2027-28 PDFs; the 2024-25 and
// 2025-26 PDFs (still on the district's file host) were linked from it in
// 2025 (Wayback captures).
const NKC = "https://www.nkcschools.org/calendar3";
const mo = district("US-MO", "North Kansas City Schools");

// Virginia — Williamsburg-James City County Public Schools (Busch Gardens
// Williamsburg, Water Country USA). Days off for PK-5 only are not listed.
const WJCC_FILES = "https://wjccschools.org/wp-content/uploads";
const WJCC_2024 = `${WJCC_FILES}/2023/10/2024-25-Academic-Calendar.pdf`;
const WJCC_2025 = `${WJCC_FILES}/2021/07/2025-26-Academic-Calendar.pdf`;
const WJCC_2026 = `${WJCC_FILES}/2025/10/2026-27-Academic-Calendar.pdf`;
const WJCC_2027 = `${WJCC_FILES}/2021/07/2027-28-Academic-Calendar-4-1-26.pdf`;
const va = district("US-VA", "Williamsburg-James City County Public Schools");

export const US_CURATED_HOLIDAYS: CuratedHolidaySet = {
  country: "US",
  entries: [
    // Florida: 2024-25 ended 2025-05-28, 2025-26 starts 2025-08-11
    fl("2025-05-29", "2025-08-10", "Summer break", OCPS_HISTORY),
    // Florida 2025-26 (last day 2026-05-27)
    fl("2025-09-01", "2025-09-01", "Labor Day", OCPS_HISTORY),
    fl("2025-10-13", "2025-10-13", "Student holiday", OCPS_HISTORY),
    fl("2025-11-24", "2025-11-28", "Thanksgiving break", OCPS_HISTORY),
    fl("2025-12-22", "2026-01-05", "Winter break", OCPS_HISTORY),
    fl("2026-01-19", "2026-01-19", "Martin Luther King Jr. Day", OCPS_HISTORY),
    fl("2026-02-16", "2026-02-16", "Presidents' Day", OCPS_HISTORY),
    fl("2026-03-13", "2026-03-20", "Spring break", OCPS_HISTORY),
    fl("2026-04-24", "2026-04-24", "Student holiday", OCPS_HISTORY),
    fl("2026-05-25", "2026-05-25", "Memorial Day", OCPS_HISTORY),
    fl("2026-05-28", "2026-08-10", "Summer break", OCPS_2026),
    // Florida 2026-27 (first day 2026-08-11, last day 2027-05-26)
    fl("2026-09-07", "2026-09-07", "Labor Day", OCPS_2026),
    fl("2026-10-12", "2026-10-12", "Student holiday", OCPS_2026),
    fl("2026-11-23", "2026-11-27", "Thanksgiving break", OCPS_2026),
    fl("2026-12-21", "2027-01-04", "Winter break", OCPS_2026),
    fl("2027-01-18", "2027-01-18", "Martin Luther King Jr. Day", OCPS_2026),
    fl("2027-02-15", "2027-02-15", "Presidents' Day", OCPS_2026),
    fl("2027-03-12", "2027-03-19", "Spring break", OCPS_2026),
    fl("2027-04-23", "2027-04-23", "Student holiday", OCPS_2026),
    fl("2027-05-27", "2027-08-09", "Summer break", OCPS_2027),
    // Florida 2027-28 (first day 2027-08-10, last day 2028-05-24)
    fl("2027-09-06", "2027-09-06", "Labor Day", OCPS_2027),
    fl("2027-10-11", "2027-10-11", "Student holiday", OCPS_2027),
    fl("2027-11-22", "2027-11-26", "Thanksgiving break", OCPS_2027),
    fl("2027-12-20", "2028-01-03", "Winter break", OCPS_2027),
    fl("2028-01-17", "2028-01-17", "Martin Luther King Jr. Day", OCPS_2027),
    fl("2028-02-21", "2028-02-21", "Presidents' Day", OCPS_2027),
    fl("2028-03-10", "2028-03-17", "Spring break", OCPS_2027),
    fl("2028-04-21", "2028-04-21", "Student holiday", OCPS_2027),
    // ends the day before 2028-29's first day (2028-08-10)
    fl("2028-05-25", "2028-08-09", "Summer break", OCPS_2028),

    // Texas 2025-26 (first day 2025-08-11, last day 2026-05-21). The 2024-25
    // calendar is no longer on nisd.net, so summer 2025 is not listed.
    tx("2025-09-01", "2025-09-01", "Labor Day", NISD_2025),
    tx("2025-10-13", "2025-10-13", "Student holiday", NISD_2025),
    tx("2025-11-04", "2025-11-04", "Student holiday", NISD_2025),
    tx("2025-11-24", "2025-11-28", "Thanksgiving break", NISD_2025),
    tx("2025-12-22", "2026-01-05", "Winter break", NISD_2025),
    tx("2026-01-19", "2026-01-19", "Martin Luther King Jr. Day", NISD_2025),
    // a bad-weather make-up day, a student holiday unless needed
    tx("2026-02-16", "2026-02-16", "Student holiday", NISD_2025),
    tx("2026-03-09", "2026-03-16", "Spring break", NISD_2025),
    tx("2026-04-03", "2026-04-06", "Holiday", NISD_2025),
    tx("2026-04-24", "2026-04-24", "Holiday", NISD_2025),
    tx("2026-05-22", "2026-08-09", "Summer break", NISD_2026),
    // Texas 2026-27 (first day 2026-08-10, last day 2027-05-26)
    tx("2026-09-07", "2026-09-07", "Labor Day", NISD_2026),
    tx("2026-10-09", "2026-10-13", "Fall student holidays", NISD_2026),
    tx("2026-11-03", "2026-11-03", "Student holiday", NISD_2026),
    tx("2026-11-23", "2026-11-27", "Thanksgiving break", NISD_2026),
    tx("2026-12-21", "2027-01-04", "Winter break", NISD_2026),
    tx("2027-01-18", "2027-01-18", "Martin Luther King Jr. Day", NISD_2026),
    // 02-16 is a bad-weather make-up day, a student holiday unless needed
    tx("2027-02-12", "2027-02-16", "February holidays", NISD_2026),
    tx("2027-03-08", "2027-03-15", "Spring break", NISD_2026),
    tx("2027-03-26", "2027-03-29", "Holiday", NISD_2026),
    tx("2027-04-23", "2027-04-23", "Holiday", NISD_2026),
    tx("2027-05-27", "2027-08-08", "Summer break", NISD_2027),
    // Texas 2027-28 (first day 2027-08-09, last day 2028-05-24)
    tx("2027-09-06", "2027-09-06", "Labor Day", NISD_2027),
    tx("2027-10-08", "2027-10-12", "Fall student holidays", NISD_2027),
    tx("2027-11-02", "2027-11-02", "Student holiday", NISD_2027),
    tx("2027-11-22", "2027-11-26", "Thanksgiving break", NISD_2027),
    tx("2027-12-20", "2028-01-03", "Winter break", NISD_2027),
    tx("2028-01-17", "2028-01-17", "Martin Luther King Jr. Day", NISD_2027),
    // 02-22 is a bad-weather make-up day, a student holiday unless needed
    tx("2028-02-18", "2028-02-22", "February holidays", NISD_2027),
    tx("2028-03-06", "2028-03-13", "Spring break", NISD_2027),
    tx("2028-04-14", "2028-04-17", "Holiday", NISD_2027),
    tx("2028-04-28", "2028-04-28", "Holiday", NISD_2027),
    // 2028-29 is not published: the calendar runs through July 2028, so the
    // summer stops on the last day it shows and later days read as unknown.
    tx("2028-05-25", "2028-07-31", "Summer break", NISD_2027),

    // California 2025-26 (first day 2025-08-14, last day 2026-06-10). The
    // 2024-25 calendar was not read; the 2025-26 one shows July 2025 without
    // instruction, so summer 2025 starts on 07-01.
    ca("2025-07-01", "2025-08-13", "Summer break", LAUSD),
    ca("2025-08-29", "2025-09-01", "Admission Day and Labor Day", LAUSD),
    ca("2025-09-23", "2025-09-23", "Unassigned day", LAUSD),
    ca("2025-10-02", "2025-10-02", "Unassigned day", LAUSD),
    ca("2025-11-11", "2025-11-11", "Veterans Day", LAUSD),
    ca("2025-11-24", "2025-11-28", "Thanksgiving break", LAUSD),
    ca("2025-12-22", "2026-01-09", "Winter recess", LAUSD),
    ca("2026-01-19", "2026-01-19", "Martin Luther King Jr. Day", LAUSD),
    ca("2026-02-16", "2026-02-16", "Presidents' Day", LAUSD),
    ca("2026-03-27", "2026-04-03", "Farm Workers Day and spring recess", LAUSD),
    ca("2026-04-24", "2026-04-24", "Armenian Genocide Remembrance Day", LAUSD),
    ca("2026-05-25", "2026-05-25", "Memorial Day", LAUSD),
    ca("2026-06-11", "2026-08-11", "Summer break", LAUSD),
    // California 2026-27 (first day 2026-08-12, last day 2027-06-04)
    ca("2026-09-04", "2026-09-07", "Admission Day and Labor Day", LAUSD),
    ca("2026-09-21", "2026-09-21", "Unassigned day", LAUSD),
    ca("2026-11-11", "2026-11-11", "Veterans Day", LAUSD),
    ca("2026-11-23", "2026-11-27", "Thanksgiving break", LAUSD),
    ca("2026-12-21", "2027-01-08", "Winter recess", LAUSD),
    ca("2027-01-18", "2027-01-18", "Martin Luther King Jr. Day", LAUSD),
    ca("2027-02-15", "2027-02-15", "Presidents' Day", LAUSD),
    ca("2027-03-22", "2027-03-26", "Spring recess", LAUSD),
    ca("2027-03-31", "2027-03-31", "Farm Workers Day", LAUSD),
    ca("2027-05-31", "2027-05-31", "Memorial Day", LAUSD),
    ca("2027-06-05", "2027-08-11", "Summer break", LAUSD),
    // California 2027-28 (first day 2027-08-12, last day 2028-06-07)
    ca("2027-09-03", "2027-09-06", "Admission Day and Labor Day", LAUSD),
    ca("2027-09-21", "2027-09-21", "Unassigned day", LAUSD),
    ca("2027-10-11", "2027-10-11", "Unassigned day", LAUSD),
    ca("2027-11-11", "2027-11-11", "Veterans Day", LAUSD),
    ca("2027-11-22", "2027-11-26", "Thanksgiving break", LAUSD),
    ca("2027-12-20", "2028-01-07", "Winter recess", LAUSD),
    ca("2028-01-17", "2028-01-17", "Martin Luther King Jr. Day", LAUSD),
    ca("2028-02-21", "2028-02-21", "Presidents' Day", LAUSD),
    ca("2028-03-31", "2028-03-31", "Farm Workers Day", LAUSD),
    ca("2028-04-10", "2028-04-14", "Spring recess", LAUSD),
    ca("2028-04-24", "2028-04-24", "Armenian Genocide Remembrance Day", LAUSD),
    ca("2028-05-29", "2028-05-29", "Memorial Day", LAUSD),
    // 2028-29 is not published: the calendar ends with June 2028.
    ca("2028-06-08", "2028-06-30", "Summer break", LAUSD),

    // Pennsylvania 2025-26 (first day 2025-08-25, last day 2026-06-04). The
    // 2024-25 year ended 2025-06-05 (district event calendar).
    pa("2025-06-06", "2025-08-24", "Summer break", DTSD_EVENTS),
    pa("2025-08-29", "2025-09-01", "Labor Day recess", DTSD_EVENTS),
    pa("2025-10-02", "2025-10-02", "No school", DTSD_EVENTS),
    pa("2025-10-13", "2025-10-13", "Columbus Day", DTSD_EVENTS),
    pa("2025-11-26", "2025-11-28", "Thanksgiving recess", DTSD_EVENTS),
    pa("2025-12-24", "2026-01-02", "Winter recess", DTSD_EVENTS),
    pa("2026-01-16", "2026-01-19", "Teacher workday and MLK Day", DTSD_EVENTS),
    pa("2026-02-09", "2026-02-09", "Staff in-service", DTSD_EVENTS),
    pa("2026-02-16", "2026-02-16", "Presidents' Day", DTSD_EVENTS),
    pa("2026-03-13", "2026-03-13", "Conferences / in-service", DTSD_EVENTS),
    pa("2026-03-20", "2026-03-20", "Staff in-service", DTSD_EVENTS),
    // 04-02 was planned as recess and used as a snow make-up day
    pa("2026-04-03", "2026-04-06", "Spring recess", DTSD_EVENTS),
    pa("2026-05-25", "2026-05-25", "Memorial Day", DTSD_EVENTS),
    pa("2026-06-05", "2026-08-23", "Summer break", DTSD_2026),
    // Pennsylvania 2026-27 (first day 2026-08-24, last day 2027-06-03)
    pa("2026-09-04", "2026-09-07", "Labor Day recess", DTSD_2026),
    pa("2026-09-21", "2026-09-21", "No school", DTSD_2026),
    pa("2026-10-12", "2026-10-12", "Columbus Day", DTSD_2026),
    pa("2026-11-25", "2026-11-27", "Thanksgiving recess", DTSD_2026),
    pa("2026-12-23", "2027-01-01", "Winter recess", DTSD_2026),
    pa("2027-01-15", "2027-01-18", "Teacher workday and MLK Day", DTSD_2026),
    pa("2027-02-15", "2027-02-15", "Presidents' Day", DTSD_2026),
    pa("2027-03-19", "2027-03-19", "Staff in-service", DTSD_2026),
    // 03-25 is the planned snow make-up day
    pa("2027-03-25", "2027-03-29", "Spring recess", DTSD_2026),
    pa("2027-04-19", "2027-04-19", "Staff in-service", DTSD_2026),
    pa("2027-05-31", "2027-05-31", "Memorial Day", DTSD_2026),
    // 2027-28 is not published: the calendar ends with June 2027.
    pa("2027-06-04", "2027-06-30", "Summer break", DTSD_2026),

    // Ohio 2025-26 (first day 2025-09-02, last day 2026-06-05). The 2024-25
    // year ended 2025-06-05.
    oh("2025-06-06", "2025-09-01", "Summer break", SCS_HOME),
    oh("2025-10-10", "2025-10-10", "NEOEA Day", SCS_HOME),
    oh("2025-11-03", "2025-11-03", "Professional learning day", SCS_HOME),
    oh("2025-11-26", "2025-11-28", "Fall break", SCS_HOME),
    oh("2025-12-22", "2026-01-02", "Winter break", SCS_HOME),
    oh("2026-01-16", "2026-01-19", "Semester grades day and MLK Day", SCS_HOME),
    oh("2026-02-16", "2026-02-16", "Presidents' Day", SCS_HOME),
    oh("2026-03-02", "2026-03-02", "Professional learning day", SCS_HOME),
    oh("2026-04-03", "2026-04-10", "Good Friday and spring break", SCS_HOME),
    oh("2026-05-25", "2026-05-25", "Memorial Day", SCS_HOME),
    oh("2026-06-06", "2026-08-25", "Summer break", SCS_2026),
    // Ohio 2026-27 (first day 2026-08-26, last day 2027-06-03)
    oh("2026-09-07", "2026-09-07", "Labor Day", SCS_2026),
    oh("2026-10-05", "2026-10-05", "Professional learning day", SCS_2026),
    oh("2026-10-09", "2026-10-09", "NEOEA Day", SCS_2026),
    oh("2026-11-25", "2026-11-27", "Fall break", SCS_2026),
    oh("2026-11-30", "2026-11-30", "Professional learning day", SCS_2026),
    oh("2026-12-21", "2027-01-01", "Winter break", SCS_2026),
    oh("2027-01-15", "2027-01-18", "Semester grades day and MLK Day", SCS_2026),
    oh("2027-02-15", "2027-02-15", "Presidents' Day", SCS_2026),
    oh("2027-03-26", "2027-04-02", "Good Friday and spring break", SCS_2026),
    oh("2027-05-31", "2027-05-31", "Memorial Day", SCS_2026),
    // 2027-28 is not published: the calendar ends with June 2027.
    oh("2027-06-04", "2027-06-30", "Summer break", SCS_2026),

    // Missouri 2025-26 (first day 2025-08-19, last day 2026-05-22). The
    // 2024-25 year ended 2025-05-29 (revised calendar).
    mo("2025-05-30", "2025-08-18", "Summer break", NKC),
    mo("2025-09-01", "2025-09-01", "Labor Day", NKC),
    mo("2025-09-19", "2025-09-19", "Professional development", NKC),
    mo("2025-10-03", "2025-10-06", "No school", NKC),
    mo("2025-11-24", "2025-11-28", "Thanksgiving break", NKC),
    mo("2025-12-22", "2026-01-05", "Winter break", NKC),
    mo("2026-01-19", "2026-01-19", "Martin Luther King Jr. Day", NKC),
    mo("2026-02-16", "2026-02-16", "Presidents' Day", NKC),
    mo("2026-03-13", "2026-03-13", "Professional development", NKC),
    mo("2026-03-30", "2026-04-03", "Spring break", NKC),
    mo("2026-04-17", "2026-04-17", "Professional development", NKC),
    // 05-26..05-28 are make-up days if needed
    mo("2026-05-23", "2026-08-24", "Summer break", NKC),
    // Missouri 2026-27 (first day 2026-08-25, last day 2027-05-28)
    mo("2026-09-07", "2026-09-07", "Labor Day", NKC),
    mo("2026-09-25", "2026-09-25", "Professional development", NKC),
    mo("2026-10-16", "2026-10-19", "No school", NKC),
    mo("2026-11-23", "2026-11-27", "Thanksgiving break", NKC),
    mo("2026-12-21", "2027-01-04", "Winter break", NKC),
    mo("2027-01-18", "2027-01-18", "Martin Luther King Jr. Day", NKC),
    mo("2027-02-15", "2027-02-15", "Presidents' Day", NKC),
    mo("2027-03-12", "2027-03-12", "Teacher workday", NKC),
    mo("2027-03-29", "2027-04-02", "Spring break", NKC),
    mo("2027-04-19", "2027-04-19", "Professional development", NKC),
    // 06-01..06-04 are make-up days if needed
    mo("2027-05-29", "2027-08-23", "Summer break", NKC),
    // Missouri 2027-28 (first day 2027-08-24, last day 2028-05-26)
    mo("2027-09-06", "2027-09-06", "Labor Day", NKC),
    mo("2027-09-24", "2027-09-24", "Professional development", NKC),
    mo("2027-10-15", "2027-10-18", "No school", NKC),
    mo("2027-11-22", "2027-11-26", "Thanksgiving break", NKC),
    mo("2027-12-20", "2028-01-03", "Winter break", NKC),
    mo("2028-01-17", "2028-01-17", "Martin Luther King Jr. Day", NKC),
    mo("2028-02-21", "2028-02-21", "Presidents' Day", NKC),
    mo("2028-03-10", "2028-03-10", "Teacher workday", NKC),
    mo("2028-03-27", "2028-03-31", "Spring break", NKC),
    mo("2028-04-17", "2028-04-17", "Professional development", NKC),
    // 2028-29 is not published: the calendar ends with June 2028. 05-30..06-02
    // are make-up days if needed.
    mo("2028-05-27", "2028-06-30", "Summer break", NKC),

    // Virginia 2025-26 (first day 2025-08-25, last day 2026-06-10). The
    // 2024-25 year ended 2025-06-11.
    va("2025-06-12", "2025-08-24", "Summer break", WJCC_2024),
    va(
      "2025-08-29",
      "2025-09-01",
      "Student/teacher holiday and Labor Day",
      WJCC_2025,
    ),
    va("2025-10-03", "2025-10-03", "Student holiday", WJCC_2025),
    va(
      "2025-11-03",
      "2025-11-04",
      "Student holiday (family conferences)",
      WJCC_2025,
    ),
    va("2025-11-26", "2025-11-28", "Thanksgiving", WJCC_2025),
    va("2025-12-22", "2026-01-02", "Winter break", WJCC_2025),
    va("2026-01-19", "2026-01-19", "Martin Luther King Jr. Day", WJCC_2025),
    va("2026-01-26", "2026-01-26", "Student holiday", WJCC_2025),
    va("2026-02-16", "2026-02-16", "Presidents' Day", WJCC_2025),
    va("2026-04-06", "2026-04-10", "Spring break", WJCC_2025),
    va("2026-05-25", "2026-05-25", "Memorial Day", WJCC_2025),
    va("2026-06-11", "2026-08-23", "Summer break", WJCC_2026),
    // Virginia 2026-27 (first day 2026-08-24, last day 2027-06-09)
    va(
      "2026-09-04",
      "2026-09-07",
      "Student & teacher holiday and Labor Day",
      WJCC_2026,
    ),
    va("2026-10-12", "2026-10-12", "Student holiday", WJCC_2026),
    va(
      "2026-11-02",
      "2026-11-03",
      "Student holiday (family conferences)",
      WJCC_2026,
    ),
    va("2026-11-25", "2026-11-27", "Thanksgiving break", WJCC_2026),
    va("2026-12-21", "2027-01-01", "Winter break", WJCC_2026),
    va("2027-01-18", "2027-01-18", "Martin Luther King Jr. Day", WJCC_2026),
    va("2027-01-25", "2027-01-25", "Student holiday", WJCC_2026),
    va("2027-02-15", "2027-02-15", "Presidents' Day", WJCC_2026),
    va("2027-03-05", "2027-03-05", "Student holiday", WJCC_2026),
    va("2027-04-05", "2027-04-09", "Spring break", WJCC_2026),
    va("2027-05-31", "2027-05-31", "Memorial Day", WJCC_2026),
    va("2027-06-10", "2027-08-22", "Summer break", WJCC_2027),
    // Virginia 2027-28 (first day 2027-08-23, last day 2028-06-08)
    va(
      "2027-09-03",
      "2027-09-06",
      "Student & teacher holiday and Labor Day",
      WJCC_2027,
    ),
    va("2027-10-11", "2027-10-11", "Student holiday", WJCC_2027),
    va(
      "2027-11-01",
      "2027-11-02",
      "Student holiday (family conferences)",
      WJCC_2027,
    ),
    va("2027-11-24", "2027-11-26", "Thanksgiving break", WJCC_2027),
    va("2027-12-20", "2027-12-31", "Winter break", WJCC_2027),
    va("2028-01-17", "2028-01-17", "Martin Luther King Jr. Day", WJCC_2027),
    va("2028-01-27", "2028-01-28", "Student holiday", WJCC_2027),
    va("2028-02-21", "2028-02-21", "Presidents' Day", WJCC_2027),
    va("2028-03-07", "2028-03-07", "Student holiday", WJCC_2027),
    va("2028-04-03", "2028-04-07", "Spring break", WJCC_2027),
    va("2028-05-29", "2028-05-29", "Memorial Day", WJCC_2027),
    // 2028-29 is not published: the calendar ends with June 2028.
    va("2028-06-09", "2028-06-30", "Summer break", WJCC_2027),
  ],
};
