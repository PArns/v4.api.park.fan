import { CuratedHoliday, CuratedHolidaySet } from "./curated-holidays.types";

/**
 * Japan — school breaks (長期休業日) of public elementary and junior high
 * schools from the municipal board of education of the city a park is in
 * (PAR-1072). Nager.Date has Japan's public holidays only. Breaks are set by
 * each municipality's school regulations (学校管理規則).
 *
 * Two of four prefectures are covered:
 * - `JP-12` Chiba — Urayasu City (Tokyo Disneyland / DisneySea): only the
 *   summer break 2026, which the board's October 2025 minutes state for
 *   令和8年度. The fixed dates of its school regulation (第18条の2) could not
 *   be read independently, so they are not listed.
 * - `JP-23` Aichi — Nagoya City (LEGOLAND Japan), from the city's published
 *   school year (年間スケジュール), year by year.
 * Osaka (USJ, `27`) and Yamanashi (Fuji-Q, `Yamanashi`) are left out: no
 * dates published for a year, see the doc. They read as unknown
 * (`REGION_ONLY_SCHOOL_COVERAGE`).
 *
 * Spring break is the days between the last day of one year (修了式) and the
 * first of the next (始業式); where elementary and junior high start on
 * different days, only the days both are off are listed. Weekends inside a
 * range are part of it, the ones at its edges are not. Nothing past
 * 2027-03-31 (Nagoya) / 2026-08-31 (Urayasu): neither city has published
 * 令和9年度 dates.
 */
const RETRIEVED = "2026-10-25";

// Urayasu: the board of education's October 2025 minutes.
const URAYASU_2026 =
  "https://www.city.urayasu.lg.jp/_res/projects/default_project/_page_/001/045/711/202510teireikai.pdf";
const NAGOYA =
  "https://www.city.nagoya.jp/kodomo/schools/1027693/1016802/1016803/1016804.html";

const city =
  (region: string, cityName: string, cityLocal: string) =>
  (
    from: string,
    to: string,
    name: string,
    localName: string,
    sourceUrl: string,
  ): CuratedHoliday => ({
    from,
    to,
    type: "school",
    region,
    name: `${name} (${cityName})`,
    localName: `${localName}（${cityLocal}）`,
    sourceUrl,
    retrievedAt: RETRIEVED,
  });

const urayasu = city("JP-12", "Urayasu", "浦安市");
const nagoya = city("JP-23", "Nagoya", "名古屋市");

export const JP_CURATED_HOLIDAYS: CuratedHolidaySet = {
  country: "JP",
  entries: [
    // Urayasu 令和8年度: the summer break the board set for this year. The
    // regulation's other breaks are not listed (see the doc).
    urayasu(
      "2026-07-21",
      "2026-08-31",
      "Summer break",
      "夏季休業日",
      URAYASU_2026,
    ),

    // Nagoya 令和7年度 (the page starts with winter 2025)
    nagoya("2025-12-24", "2026-01-06", "Winter break", "冬休み", NAGOYA),
    // 修了式 03-24; 始業式 junior high 04-06 (Mon), elementary 04-07; the
    // weekend before is not part of the range
    nagoya("2026-03-25", "2026-04-03", "Spring break", "春休み", NAGOYA),
    // 令和8年度
    nagoya("2026-07-21", "2026-08-31", "Summer break", "夏休み", NAGOYA),
    nagoya("2026-12-24", "2027-01-06", "Winter break", "冬休み", NAGOYA),
    // 修了式 2027-03-24; 令和9年度 is not published
    nagoya("2027-03-25", "2027-03-31", "Spring break", "春休み", NAGOYA),
  ],
};
