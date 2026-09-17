import {
  attractionIsCuratedOutOfService,
  isCuratedOutOfService,
  resolveWorksPeriod,
  worksPeriodEndsBeforeItBegins,
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

describe("resolveWorksPeriod", () => {
  it("is null when neither bound is curated", () => {
    // The state of nearly every ride in the catalogue. A block of nulls here
    // would ride along on every attraction of every park payload for a fact
    // that is almost always absent.
    expect(resolveWorksPeriod({})).toBeNull();
    expect(
      resolveWorksPeriod({
        curatedOutOfServiceFrom: null,
        curatedOutOfServiceTo: null,
        curatedOutOfServiceToUncertain: true,
      }),
    ).toBeNull();
  });

  it("carries both bounds and the estimate flag", () => {
    expect(
      resolveWorksPeriod({
        curatedOutOfServiceFrom: "2026-01-16",
        curatedOutOfServiceTo: "2026-03-03",
        curatedOutOfServiceToUncertain: true,
      }),
    ).toEqual({ from: "2026-01-16", to: "2026-03-03", toUncertain: true });
  });

  it("keeps a window that only has a start", () => {
    // The usual case while work is running: nobody has been told when it ends.
    expect(
      resolveWorksPeriod({ curatedOutOfServiceFrom: "2026-01-16" }),
    ).toEqual({ from: "2026-01-16", to: null, toUncertain: false });
  });

  it("keeps a window that only has an end", () => {
    expect(resolveWorksPeriod({ curatedOutOfServiceTo: "2026-03-03" })).toEqual(
      {
        from: null,
        to: "2026-03-03",
        toUncertain: false,
      },
    );
  });

  it("drops an estimate flag that has no end date to qualify", () => {
    // What clearing the end date and not the checkbox leaves behind. Served as
    // true it would tell a client to hedge a date that is not there.
    expect(
      resolveWorksPeriod({
        curatedOutOfServiceFrom: "2026-01-16",
        curatedOutOfServiceToUncertain: true,
      }),
    ).toEqual({ from: "2026-01-16", to: null, toUncertain: false });
  });

  it("reads an uncurated flag as a plain date, not as a hedge", () => {
    // The column has three states because the EDITOR needs them — "nobody has
    // looked" is worth seeing in the form. A page has two renderings, so null
    // and false both get the plain one.
    for (const flag of [null, undefined, false]) {
      expect(
        resolveWorksPeriod({
          curatedOutOfServiceFrom: "2026-01-16",
          curatedOutOfServiceTo: "2026-03-03",
          curatedOutOfServiceToUncertain: flag,
        })?.toUncertain,
      ).toBe(false);
    }
  });
});

/**
 * The rule both writers of a works period read (PAR-297).
 *
 * `AdminCurationService` refuses the pair a curator types, and
 * `AttractionMergeService` refuses to carry one onto the row that survives a
 * merge. Written twice it would drift, and the half that drifted would be the
 * one nobody types into.
 */
describe("worksPeriodEndsBeforeItBegins", () => {
  it("names the pair a window can never apply on", () => {
    expect(worksPeriodEndsBeforeItBegins("2026-03-01", "2026-01-20")).toBe(
      true,
    );
  });

  it("leaves a window that ends on the day it starts alone", () => {
    // Both bounds inclusive, so a one-day works period is a works period.
    expect(worksPeriodEndsBeforeItBegins("2026-03-01", "2026-03-01")).toBe(
      false,
    );
  });

  it("calls no half-open window inverted", () => {
    // There is nothing for the missing bound to precede, and both halves are
    // ordinary states — a start with no end while work runs, an end with no
    // start for a window that was already open when somebody wrote it down.
    expect(worksPeriodEndsBeforeItBegins("2026-03-01", null)).toBe(false);
    expect(worksPeriodEndsBeforeItBegins(null, "2026-01-20")).toBe(false);
    expect(worksPeriodEndsBeforeItBegins(null, null)).toBe(false);
    expect(worksPeriodEndsBeforeItBegins(undefined, undefined)).toBe(false);
  });

  it("describes a window that covers no day at all", () => {
    // The reason the merge refuses to carry one, written as an assertion
    // rather than as a sentence in a test comment: an inverted window
    // suppresses nothing, it is served as a `worksPeriod` running backwards.
    const inverted = {
      curatedOutOfServiceFrom: "2026-03-01",
      curatedOutOfServiceTo: "2026-01-20",
    };
    for (const day of [
      "2026-01-19",
      "2026-01-20",
      "2026-02-10",
      "2026-03-01",
    ]) {
      expect(isCuratedOutOfService(inverted, "Europe/Berlin", day)).toBe(false);
    }
    expect(resolveWorksPeriod(inverted)).toEqual({
      from: "2026-03-01",
      to: "2026-01-20",
      toUncertain: false,
    });
  });
});
