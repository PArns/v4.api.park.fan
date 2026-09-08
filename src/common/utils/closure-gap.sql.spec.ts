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

  it.each(both)("%s hoists the day close out of the row loop", (_n, sql) => {
    // The 21.5-s-of-24-s regression, and it is one edit away from returning.
    // early_end used to resolve the park's closing time through a LATERAL, so
    // every queue_data row it read carried its own schedule lookup: 28 485
    // executions of one bitmap index scan at Alton Towers, for a table with at
    // most 23 rows to offer. Neither statement has any business running a
    // correlated subquery per reading.
    expect(sql).not.toContain("JOIN LATERAL");
  });

  /**
   * One CTE's body, with `--` comments removed first.
   *
   * Two earlier attempts were vacuous in the same way. Slicing to the next
   * `"),\n"` landed on the terminator only because nothing happened to be
   * formatted that way; counting parentheses over the raw text breaks on a lone
   * `)` inside one of the prose comments these CTEs are wrapped in. Both
   * truncate the body silently, and a `not.toContain` on a truncated body
   * passes on text it never read — which is exactly the failure these
   * assertions exist to catch.
   */
  const cteBody = (sql: string, name: string): string => {
    const bare = sql.replace(/--[^\n]*/g, "");
    const head = `${name} AS (`;
    const start = bare.indexOf(head);
    expect(start).toBeGreaterThan(-1);
    let depth = 0;
    for (let i = start + head.length - 1; i < bare.length; i++) {
      if (bare[i] === "(") depth++;
      else if (bare[i] === ")" && --depth === 0) {
        const body = bare.slice(start, i);
        // The extraction itself must not be vacuous.
        expect(body).toContain("FROM");
        expect(body.length).toBeGreaterThan(head.length + 60);
        return body;
      }
    }
    throw new Error(`unterminated CTE ${name}`);
  };

  it("the live historical CTEs judge the rides in a closure, not the park", () => {
    // cycle, active and early_end are read only through LEFT JOINs against
    // run_start, but all three used to be handed $1 — the whole roster, which
    // park_closers needs and they do not. A 96-ride park scanned 21 days of
    // queue_data 96 times over. Reverting any one of them to $1 is silent.
    //
    for (const cte of ["cycle", "active", "early_end"]) {
      const body = cteBody(CURRENT_CLOSURE_GAP_SQL, cte);
      expect(body).toContain("ARRAY(SELECT aid FROM run_start)");
      expect(body).not.toContain("ANY($1::uuid[])");
    }
  });

  it("the nightly denominator is restricted to the parks it serves", () => {
    // The third of the three defects in that CTE, and the only one with no
    // visible symptom: without the join it computes active_days for every
    // attraction in the database in order to use it for the blind ones. Every
    // assertion here stays green if it is deleted, the output is unchanged, and
    // the nightly job is simply slower.
    const body = cteBody(CLOSURE_GAP_INTERVALS_SQL, "active");
    expect(body).toContain("JOIN blind_parks");
    // Through the exposure table's own parkId, which is what its index covers,
    // and not through attractions — that join also drops exposure rows whose
    // attraction has since been merged away.
    expect(body).toContain('b.pid = e."parkId"');
    expect(body).not.toContain("JOIN attractions");
  });

  it.each(both)(
    "%s bounds the duty-cycle denominator on both sides",
    (_n, sql) => {
      // The nightly active CTE had a lower bound and no upper one, so a replay
      // counted every operating day from the window start to today — the leak
      // `reference_sql_rule_replay_windowing` already names. On an ordinary
      // incremental run the same omission made active_days 0–2 for every ride,
      // permanently under MIN_DAYS_FOR_CYCLE_TEST, so the filter never fired.
      const body = cteBody(sql, "active");
      expect(body).toMatch(/op_day\s*>=/);
      expect(body).toMatch(/op_day\s*<=/);
      // Park-local on both sides, because op_day is. A bare ::date takes the
      // session zone and disagrees for 56 of the 91 blind parks.
      expect(body).not.toMatch(/\$\d::timestamptz\)::date/);
      expect(body).toContain("AT TIME ZONE");
      // And no fixed offset in front of the numerator's edge. One day of slack
      // moved the nightly statement from 330 intervals over 112 rides to 370
      // over 120 — all dilution, in the direction that publishes a timetable
      // as a fault.
      expect(body).not.toMatch(/op_day\s*>=[^\n]*::date\s*-\s*\d/);
    },
  );

  it("both pseudoconstant gates are EXISTS, not joins", () => {
    // park_open is the same argument as the regime check and the costlier one:
    // a shut park is where run_start IS the roster, so the three historical
    // CTEs are at their most expensive exactly when the CROSS JOIN is about to
    // throw the result away. Alton Towers 586.4 ms to 10.2, Phantasialand
    // 707.6 to 3.8, both measured after closing time.
    expect(CURRENT_CLOSURE_GAP_SQL).toContain(
      "EXISTS (SELECT 1 FROM park_open)",
    );
    // Cheapest first: the regime probe is one primary-key lookup and rejects
    // the 122 parks that make 70 % of the calls, park_open is a schedule scan.
    // Both are pseudoconstants, so this only orders the InitPlans — but an
    // expensive test written in front of a cheap one is the shape this whole
    // change removes.
    expect(
      CURRENT_CLOSURE_GAP_SQL.indexOf("FROM park_downtime_coverage c"),
    ).toBeLessThan(
      CURRENT_CLOSURE_GAP_SQL.indexOf("EXISTS (SELECT 1 FROM park_open)"),
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
