import { CuratedHoliday, CuratedHolidaySet } from "./curated-holidays.types";

/**
 * China — the State Council General Office's notice on the holiday
 * arrangements (国务院办公厅关于…年部分节假日安排的通知), as released in full
 * by Xinhua ("受权发布", authorised release).
 *
 * Each block is the whole run of days off (放假调休), not just the festival
 * day Nager.Date carries. The weekend days declared working days (上班) are
 * `working_day` rows — school and work run on them, so they are the opposite
 * of a holiday and no holiday reader counts them.
 *
 * School holidays are set per province and are not curated here.
 * The 2027 notice was not published on 2026-10-24 (it usually appears in
 * early November); add it from the notice once it is out — never before.
 */
const CN_2026 =
  "https://www.news.cn/politics/20251104/88bcffd88ae249e58c699b8772548e3d/c.html";
const RETRIEVED = "2026-10-24";

const off = (
  from: string,
  to: string,
  name: string,
  localName: string,
): CuratedHoliday => ({
  from,
  to,
  type: "public",
  region: null,
  name,
  localName,
  sourceUrl: CN_2026,
  retrievedAt: RETRIEVED,
});

const work = (date: string, forHoliday: string): CuratedHoliday => ({
  from: date,
  to: date,
  type: "working_day",
  region: null,
  name: `Working day (make-up for ${forHoliday})`,
  localName: "调休上班",
  sourceUrl: CN_2026,
  retrievedAt: RETRIEVED,
});

export const CN_CURATED_HOLIDAYS: CuratedHolidaySet = {
  country: "CN",
  entries: [
    off("2026-01-01", "2026-01-03", "New Year's Day holiday", "元旦"),
    work("2026-01-04", "New Year's Day"),
    work("2026-02-14", "Spring Festival"),
    off("2026-02-15", "2026-02-23", "Spring Festival holiday", "春节"),
    work("2026-02-28", "Spring Festival"),
    off("2026-04-04", "2026-04-06", "Qingming Festival holiday", "清明节"),
    off("2026-05-01", "2026-05-05", "Labour Day holiday", "劳动节"),
    work("2026-05-09", "Labour Day"),
    off("2026-06-19", "2026-06-21", "Dragon Boat Festival holiday", "端午节"),
    work("2026-09-20", "National Day"),
    off("2026-09-25", "2026-09-27", "Mid-Autumn Festival holiday", "中秋节"),
    off("2026-10-01", "2026-10-07", "National Day holiday", "国庆节"),
    work("2026-10-10", "National Day"),
  ],
};
