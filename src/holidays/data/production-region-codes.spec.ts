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
  // GB (PAR-1072, read 2026-10-25): England, one stores the full name.
  ["alton-towers", "GB", "ENG"],
  ["flamingo-land", "GB", "England"],
  // JP (PAR-1072): ISO numbers; USJ (27) and Fuji-Q (Yamanashi) have no rows.
  ["tokyo-disneyland", "JP", "12"],
  ["tokyo-disneysea", "JP", "12"],
  ["legoland-japan", "JP", "23"],
  ["cn-zhejiang", "CN", "Zhejiang"],
  ["cn-jiangsu", "CN", "Jiangsu"],
  ["cn-gansu-sheng", "CN", "Gan Su Sheng"],
  ["cn-gansu", "CN", "Gansu"],
  ["cn-44", "CN", "44"],
  ["cn-cn-44", "CN", "CN-44"],
  ["cn-31", "CN", "31"],
  ["cn-11", "CN", "11"],
  ["cn-tianjin", "CN", "Tianjin"],
  // US parks store the bare state code (PAR-1071); curated rows are `US-FL`/….
  ["magic-kingdom-park", "US", "FL"],
  ["busch-gardens-tampa", "US", "FL"],
  ["six-flags-fiesta-texas", "US", "TX"],
  ["six-flags-over-texas", "US", "TX"],
  ["disneyland-park", "US", "CA"],
  ["universal-studios-hollywood", "US", "CA"],
  // PAR-1074 (read 2026-10-25)
  ["hersheypark", "US", "PA"],
  ["kennywood", "US", "PA"],
  ["cedar-point", "US", "OH"],
  ["kings-island", "US", "OH"],
  ["worlds-of-fun", "US", "MO"],
  ["silver-dollar-city", "US", "MO"],
  ["busch-gardens-williamsburg", "US", "VA"],
  ["kings-dominion", "US", "VA"],
  ["dollywood", "US", "TN"],
  ["carowinds", "US", "NC"],
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

  it("leaves a US state without a curated district unknown (hurricane-harbor-chicago, IL)", () => {
    const coverage = buildSchoolCoverage(
      curated("US").map((r) => ({
        country: r.country,
        region: r.region ?? null,
        through: r.date.toISOString().slice(0, 10),
      })),
    );
    expect(schoolCoverageThrough(coverage, "US", "IL")).toBeNull();
    expect(
      curated("US").some((r) =>
        isLocalHoliday(toRow(r), { countryCode: "US", regionCode: "IL" }),
      ),
    ).toBe(false);
  });

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
