import {
  attractionIsCuratedOutOfService,
  isCuratedOutOfService,
} from "./curated-out-of-service.util";

/** Collapses the SQL's formatting so assertions can match on wording alone. */
const flat = (sql: string) => sql.replace(/\s+/g, " ");

describe("isCuratedOutOfService", () => {
  const TZ = "Europe/Berlin";

  it("claims nothing when neither bound is set", () => {
    // The normal state for every ride in the catalogue. It must not read as
    // "in service" either — the caller asks a different question for that.
    expect(isCuratedOutOfService({}, TZ, "2026-09-06")).toBe(false);
    expect(
      isCuratedOutOfService(
        { curatedOutOfServiceFrom: null, curatedOutOfServiceTo: null },
        TZ,
        "2026-09-06",
      ),
    ).toBe(false);
  });

  it("includes both bounds", () => {
    const window = {
      curatedOutOfServiceFrom: "2026-01-16",
      curatedOutOfServiceTo: "2026-03-03",
    };
    expect(isCuratedOutOfService(window, TZ, "2026-01-16")).toBe(true);
    expect(isCuratedOutOfService(window, TZ, "2026-03-03")).toBe(true);
    expect(isCuratedOutOfService(window, TZ, "2026-02-10")).toBe(true);
    expect(isCuratedOutOfService(window, TZ, "2026-01-15")).toBe(false);
    expect(isCuratedOutOfService(window, TZ, "2026-03-04")).toBe(false);
  });

  it("runs forever from a start with no end", () => {
    // The ordinary state while work is happening and nobody has been told when
    // it finishes.
    const open = { curatedOutOfServiceFrom: "2026-01-16" };
    expect(isCuratedOutOfService(open, TZ, "2026-01-16")).toBe(true);
    expect(isCuratedOutOfService(open, TZ, "2031-01-01")).toBe(true);
    expect(isCuratedOutOfService(open, TZ, "2026-01-15")).toBe(false);
  });

  it("reaches back forever from an end with no start", () => {
    const open = { curatedOutOfServiceTo: "2026-03-03" };
    expect(isCuratedOutOfService(open, TZ, "2026-03-03")).toBe(true);
    expect(isCuratedOutOfService(open, TZ, "2020-01-01")).toBe(true);
    expect(isCuratedOutOfService(open, TZ, "2026-03-04")).toBe(false);
  });

  it("answers about the park's day, not the server's", () => {
    // A window opening on the 16th opens at Tokyo's midnight for a Tokyo park.
    // Anchoring the test on an explicit park-local day is the whole contract:
    // the caller resolves the day in the park's zone and hands it over.
    const window = { curatedOutOfServiceFrom: "2026-01-16" };
    expect(isCuratedOutOfService(window, "Asia/Tokyo", "2026-01-15")).toBe(
      false,
    );
    expect(isCuratedOutOfService(window, "Asia/Tokyo", "2026-01-16")).toBe(
      true,
    );
  });

  it("refuses rather than throws on a broken timezone", () => {
    // This runs inside the park page's ride loop. An unguarded throw would cost
    // every ride on the page its status for one malformed column.
    expect(() =>
      isCuratedOutOfService(
        { curatedOutOfServiceFrom: "2026-01-16" },
        "Not/AZone",
      ),
    ).not.toThrow();
  });
});

describe("attractionIsCuratedOutOfService", () => {
  const sql = flat(attractionIsCuratedOutOfService("a", "d.op_day"));

  it("is the twin of the TypeScript rule, bound for bound", () => {
    expect(sql).toContain("a.curated_out_of_service_from IS NOT NULL");
    expect(sql).toContain("a.curated_out_of_service_to IS NOT NULL");
    expect(sql).toContain("d.op_day >= a.curated_out_of_service_from");
    expect(sql).toContain("d.op_day <= a.curated_out_of_service_to");
  });

  it("takes the day as a parameter instead of reading NOW()", () => {
    // attractionIsOutOfSeason() computes against EXTRACT(MONTH FROM NOW()),
    // which is exactly why it may not be applied to history: over 90 days it
    // deletes the July of a ride that leaves season in September. A works
    // period is a fact about a date range, so a past day must get the answer
    // that day had.
    expect(sql).not.toContain("NOW()");
  });

  it("claims nothing when neither column is set", () => {
    // The leading IS NOT NULL pair is what stops an all-null row from
    // satisfying both open-ended branches and reading as permanently shut.
    expect(sql).toMatch(
      /\(\s*\(a\.curated_out_of_service_from IS NOT NULL OR a\.curated_out_of_service_to IS NOT NULL\)/,
    );
  });
});
