import {
  CLOSURE_GAP_INTERVALS_SQL,
  CURRENT_CLOSURE_GAP_SQL,
  LIVE_LOOKBACK_HOURS,
  MAX_GAP_HOURS,
} from "./closure-gap.sql";

/**
 * The two statements share a definition and must not drift apart.
 *
 * They already did once: the live `cycle` CTE claimed in its comment to
 * recognise "the same triple the nightly statement recognises" and did not — it
 * was missing both the 12-hour ceiling and the same-day check, so an ordinary
 * overnight close counted as a gap day. Nothing caught it, because SQL in a
 * template literal is a string to the compiler and the query's only caller
 * swallows failures to protect the page.
 *
 * These are shape assertions, not a parser. They cost nothing and they pin the
 * clauses that were actually lost.
 */
describe("closure-gap statements", () => {
  const both = [
    ["historical", CLOSURE_GAP_INTERVALS_SQL],
    ["live", CURRENT_CLOSURE_GAP_SQL],
  ] as const;

  it.each(both)(
    "%s recognises a gap as a same-park-local-day return",
    (_n, sql) => {
      expect(sql).toMatch(/AT TIME ZONE [^)]+\)::date\s*\n?\s*=\s*\(/);
    },
  );

  it.each(both)("%s bounds how far a gap may reach", (_n, sql) => {
    expect(sql).toContain(`INTERVAL '${MAX_GAP_HOURS} hours'`);
  });

  it("the live lookback covers a whole operating day, not the gap ceiling", () => {
    // These wanted opposite things and shared a constant: the ceiling is kept
    // short to exclude an overnight reopening, the lookback must span the day
    // or the morning falls out of the window. 18 blind parks had 399 operating
    // days over 12 hours, the longest 24.
    expect(LIVE_LOOKBACK_HOURS).toBeGreaterThan(24);
    expect(CURRENT_CLOSURE_GAP_SQL).toContain(
      `INTERVAL '${LIVE_LOOKBACK_HOURS} hours'`,
    );
  });

  it("every window function referenced in the live cycle CTE is selected", () => {
    // The same-day fix referenced `next_ts` in a subquery that did not select
    // it. `npm run build` is blind to that; production would have swallowed it.
    const cycle = CURRENT_CLOSURE_GAP_SQL.slice(
      CURRENT_CLOSURE_GAP_SQL.indexOf("cycle AS ("),
    );
    if (cycle.includes("f.next_ts")) {
      expect(cycle).toMatch(/lead\(qd\.timestamp\)\s+OVER w AS next_ts/);
    }
  });

  it("the nightly statement honours the curated works period", () => {
    // The DOWN reconstruction excludes it and the guarantee carries no signal
    // qualifier — "inside it nothing is reported". A ride mid-rebuild cycles
    // OPERATING/CLOSED during testing, which is exactly the shape this
    // statement recognises, so without the exclusion it would fill the whole
    // declared period with inferred outages.
    expect(CLOSURE_GAP_INTERVALS_SQL).toContain("curated_out_of_service_from");
    expect(CLOSURE_GAP_INTERVALS_SQL).toContain("curated_out_of_service_to");
  });
});
