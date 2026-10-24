import {
  buildSchoolCoverage,
  isLocalHoliday,
  schoolCoverageThrough,
} from "../../common/utils/holiday.utils";
import { HolidayInput } from "../../common/types/holiday-input.type";
import {
  krImportTargets,
  neisHolidayRows,
} from "../../external-apis/korea/korea-holidays";
import {
  CURATED_HOLIDAY_SETS,
  expandCuratedHolidays,
} from "./curated-holidays";

/**
 * The parks' `regionCode` values as production stores them (read 2026-10-24),
 * literally. Every imported row must reach the park through the same match
 * the readers use (`isLocalHoliday`, the coverage), whatever the format.
 */
const PARKS: [slug: string, country: string, regionCode: string | null][] = [
  ["caribbean-bay", "KR", "41"],
  ["everland", "KR", "41"],
  ["legoland-korea", "KR", "42"],
  ["lotte-world-adventure", "KR", "11"],
  ["universal-studios-singapore", "SG", ""],
  ["dreamworld", "AU", "QLD"],
  ["cn-zhejiang", "CN", "Zhejiang"],
  ["cn-jiangsu", "CN", "Jiangsu"],
  ["cn-gansu-sheng", "CN", "Gan Su Sheng"],
  ["cn-gansu", "CN", "Gansu"],
  ["cn-44", "CN", "44"],
  ["cn-cn-44", "CN", "CN-44"],
  ["cn-31", "CN", "31"],
  ["cn-11", "CN", "11"],
  ["cn-tianjin", "CN", "Tianjin"],
];

const toRow = (r: HolidayInput) => ({
  country: r.country,
  region: r.region ?? null,
  isNationwide: r.isNationwide,
});

const curated = (country: string) =>
  expandCuratedHolidays(
    CURATED_HOLIDAY_SETS.find((s) => s.country === country)!,
  );

describe("imported rows reach the parks' production region codes (PAR-1065)", () => {
  const kr = (() => {
    const { targets, unknown } = krImportTargets(
      PARKS.filter(([, c]) => c === "KR").map(([, , r]) => r!),
    );
    expect(unknown).toEqual([]);
    return [...targets.values()].flatMap((codes) =>
      neisHolidayRows(codes, new Map([["2026-07-21", "여름방학"]])),
    );
  })();

  const rowsFor = (country: string): HolidayInput[] =>
    country === "KR" ? kr : curated(country);

  it.each(PARKS)("%s (%s, %p) gets its rows", (_slug, country, regionCode) => {
    const park = { countryCode: country, regionCode };
    const rows = rowsFor(country);
    expect(rows.some((r) => isLocalHoliday(toRow(r), park))).toBe(true);
  });

  it.each(PARKS.filter(([, c]) => c !== "CN"))(
    "%s (%s, %p) has school coverage",
    (_slug, country, regionCode) => {
      const coverage = buildSchoolCoverage(
        rowsFor(country)
          .filter((r) => r.holidayType === "school")
          .map((r) => ({
            country: r.country,
            region: r.region ?? null,
            through: r.date.toISOString().slice(0, 10),
          })),
      );
      expect(
        schoolCoverageThrough(coverage, country, regionCode),
      ).not.toBeNull();
    },
  );

  it("writes KR rows under the ISO form the readers build from 41/42/11", () => {
    expect([...new Set(kr.map((r) => r.region))].sort()).toEqual([
      "KR-11",
      "KR-41",
      "KR-42",
    ]);
  });

  it("keeps every CN row national, so no province string has to match", () => {
    for (const r of curated("CN")) {
      expect(r.region).toBeUndefined();
      expect(r.isNationwide).toBe(true);
    }
  });
});
