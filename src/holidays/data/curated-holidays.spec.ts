import {
  buildSchoolCoverage,
  schoolCoverageThrough,
} from "../../common/utils/holiday.utils";
import { isRealIsoDate } from "../../common/utils/date.util";
import {
  CURATED_HOLIDAY_SETS,
  expandCuratedHolidays,
} from "./curated-holidays";

/**
 * The curated files are hand-copied facts about real places
 * (docs/rules/absent-facts.md), so every entry must say where it was read and
 * the source must be the publisher itself — a mis-typed range or a page from
 * an aggregator is exactly what this guards against.
 */
const OFFICIAL_HOSTS: Record<string, string[]> = {
  SG: ["www.moe.gov.sg"],
  AU: ["education.qld.gov.au"],
  // Xinhua's authorised release ("受权发布") of the State Council notice.
  CN: ["www.news.cn", "www.gov.cn"],
  // The school districts themselves (PAR-1071).
  US: ["www.ocps.net", "www.nisd.net"],
};

describe("curated holiday files (PAR-1065)", () => {
  it("has one file per country and an official source list for each", () => {
    const countries = CURATED_HOLIDAY_SETS.map((s) => s.country);
    expect(new Set(countries).size).toBe(countries.length);
    for (const c of countries) expect(OFFICIAL_HOSTS[c]).toBeDefined();
  });

  describe.each(CURATED_HOLIDAY_SETS.map((s) => [s.country, s] as const))(
    "%s",
    (country, set) => {
      it.each(set.entries.map((e) => [`${e.from}..${e.to} ${e.name}`, e]))(
        "%s is a well-formed entry from an official page",
        (_label, e) => {
          expect(isRealIsoDate(e.from)).toBe(true);
          expect(isRealIsoDate(e.to)).toBe(true);
          expect(e.from <= e.to).toBe(true);
          expect(["public", "school", "working_day"]).toContain(e.type);
          expect(e.name.trim()).not.toBe("");
          if (e.region !== null) {
            expect(e.region.startsWith(`${country}-`)).toBe(true);
          }
          const url = new URL(e.sourceUrl);
          expect(url.protocol).toBe("https:");
          expect(OFFICIAL_HOSTS[country]).toContain(url.host);
          expect(isRealIsoDate(e.retrievedAt)).toBe(true);
        },
      );

      it("expands to one row per day with a stable, prefixed id", () => {
        const rows = expandCuratedHolidays(set);
        const days = set.entries.reduce(
          (n, e) =>
            n +
            (Date.parse(`${e.to}T00:00:00Z`) -
              Date.parse(`${e.from}T00:00:00Z`)) /
              86_400_000 +
            1,
          0,
        );
        expect(rows).toHaveLength(days);
        for (const r of rows) {
          expect(r.externalId.startsWith(`curated:${country}:`)).toBe(true);
          expect(r.country).toBe(country);
          expect(r.isNationwide).toBe(r.region === undefined);
        }
      });
    },
  );

  it("never stores China's make-up working days as holidays", () => {
    const cn = CURATED_HOLIDAY_SETS.find((s) => s.country === "CN")!;
    const rows = expandCuratedHolidays(cn);
    const work = rows.filter((r) => r.holidayType === "working_day");
    expect(work.map((r) => r.date.toISOString().slice(0, 10))).toEqual([
      "2026-01-04",
      "2026-02-14",
      "2026-02-28",
      "2026-05-09",
      "2026-09-20",
      "2026-10-10",
    ]);
    // Every one of them is a Saturday or Sunday — that is what 调休 means.
    for (const r of work) expect([0, 6]).toContain(r.date.getUTCDay());
    const off = new Set(
      rows
        .filter((r) => r.holidayType === "public")
        .map((r) => r.date.toISOString().slice(0, 10)),
    );
    for (const r of work) {
      expect(off.has(r.date.toISOString().slice(0, 10))).toBe(false);
    }
  });

  it("covers school holidays exactly as far as the publishers announced them", () => {
    const rows = CURATED_HOLIDAY_SETS.flatMap(expandCuratedHolidays)
      .filter((r) => r.holidayType === "school")
      .map((r) => ({
        country: r.country,
        region: r.region ?? null,
        through: r.date.toISOString().slice(0, 10),
      }));
    const coverage = buildSchoolCoverage(rows);

    // Queensland: published through 2029; the 2029/30 summer break's end is
    // not, so the data stops on the last day the page vouches for.
    expect(schoolCoverageThrough(coverage, "AU", "QLD")).toBe("2029-12-31");
    // Singapore: MOE has announced 2027, not 2028.
    expect(schoolCoverageThrough(coverage, "SG", null)).toBe("2027-12-31");
    // China: provinces set school holidays; none are curated, so unknown.
    expect(schoolCoverageThrough(coverage, "CN", "31")).toBeNull();
    // US, one district per state (PAR-1071): Orange County has published
    // 2028-29, so the 2028 summer ends the day before it starts; Northside
    // ISD's 2027-28 calendar runs through July 2028.
    expect(schoolCoverageThrough(coverage, "US", "FL")).toBe("2028-08-09");
    expect(schoolCoverageThrough(coverage, "US", "TX")).toBe("2028-07-31");
    expect(schoolCoverageThrough(coverage, "US", "OH")).toBeNull();
  });
});
