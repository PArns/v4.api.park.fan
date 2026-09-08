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

  it("the live regime test is an EXISTS on the parameter, never a join", () => {
    // This is the whole performance fix, and it is invisible to the compiler.
    //
    // It used to be a CTE brought in with CROSS JOIN. A join is not a guard:
    // the planner has no reason to evaluate that arm first, so every historical
    // CTE ran to completion and the test was applied last — 6 s per call, 79 %
    // of the database, for parks that provably had no row to give. As an
    // uncorrelated EXISTS it becomes an InitPlan and PostgreSQL emits a
    // One-Time Filter that never demands the subtree: Europa-Park 939.8 ms to
    // 2.3 ms, 132 nodes never executed.
    //
    // Correlating it with anything from the rows above would silently undo
    // that, so the assertion pins both halves: it is an EXISTS, and it reads
    // nothing but $4.
    expect(CURRENT_CLOSURE_GAP_SQL).not.toContain("CROSS JOIN blind");
    const exists = CURRENT_CLOSURE_GAP_SQL.match(
      /EXISTS\s*\(\s*SELECT 1\s+FROM park_downtime_coverage c\s+WHERE([\s\S]*?)\)/,
    );
    expect(exists).not.toBeNull();
    const body = exists![1];
    expect(body).toContain('c."parkId" = $4::uuid');
    expect(body).toContain("c.regime = 'never_reports'");
    // Nothing from the outer query may leak in, or it stops being an InitPlan.
    expect(body).not.toMatch(/\bs\.|\bpo\.|\bsm\.|\bcy\.|\bac\.|\bee\./);
  });

  it.each(both)("%s cannot divide a gap share by zero", (_n, sql) => {
    // `active_days` is COALESCEd to 0 in the nightly CTE and can be 0 in the
    // live one, and the day-floor test beside it is NOT a guard — SQL does not
    // promise to evaluate OR left to right. In the live statement one raised
    // error costs EVERY ride in the park its closure line, because
    // addClosureGaps catches and returns.
    expect(sql).toMatch(
      /gap_days,?\s*0?\)?\s*\n?\s*\/ NULLIF\(\s*\w*\.?active_days, 0\)/,
    );
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
